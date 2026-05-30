"""Slack Bot for Team TODO - adds tasks to Google Sheets via mentions."""
import os
import re
import json
import base64
import threading
import urllib.request
from datetime import datetime, timedelta
import gspread
from google.oauth2.service_account import Credentials
from slack_bolt import App
from slack_bolt.adapter.socket_mode import SocketModeHandler
from http.server import HTTPServer, BaseHTTPRequestHandler
import difflib

SHEET_ID = '1Vc6qkfGUjTtGBCyCzSKe0kIpX2mr0Qvtf6_u7B4JYZE'

# Slack setup
app = App(token=os.environ['SLACK_BOT_TOKEN'])

# Google Sheets setup
scopes = ['https://www.googleapis.com/auth/spreadsheets']
creds_json = json.loads(base64.b64decode(os.environ['GOOGLE_CREDENTIALS']).decode())
creds = Credentials.from_service_account_info(creds_json, scopes=scopes)
gc = gspread.authorize(creds)
sh = gc.open_by_key(SHEET_ID)
ws = sh.sheet1

# Thread lock for gspread operations (gspread is not thread-safe)
_sheet_lock = threading.Lock()

# Refreshable gspread client
_gc_lock = threading.Lock()
_gc_state = {'client': gc, 'refreshed_at': datetime.now()}
GC_REFRESH_INTERVAL = timedelta(minutes=30)


def _get_gc():
    """Get a gspread client, re-authorizing if stale."""
    global gc, sh, ws
    with _gc_lock:
        now = datetime.now()
        if now - _gc_state['refreshed_at'] > GC_REFRESH_INTERVAL:
            gc = gspread.authorize(creds)
            sh = gc.open_by_key(SHEET_ID)
            ws = sh.sheet1
            _gc_state['client'] = gc
            _gc_state['refreshed_at'] = now
            print(f'[gspread] Re-authorized at {now}')
        return _gc_state['client']


def get_tasks_ws():
    """Get a fresh worksheet reference for the tasks sheet."""
    return _get_gc().open_by_key(SHEET_ID).sheet1

NAME_MAP = {
    'rena': 'rena',
    'ayano': 'Ayano Yo',
    'chihiro': 'MorishimaChihiro',
    'kanako': 'OshinoKanako',
    'satsuki': 'IihoshiSatsuki',
    'midori': 'Midori Fukihara',
    'tatsuya': 'Tatsuya Eguchi',
}

# ballOwner値 → 分報チャンネル名のマッピング（逆引き）
BUNPO_CHANNEL = {}
for _key, _val in NAME_MAP.items():
    BUNPO_CHANNEL[_val] = f'分報_{_key}'
    BUNPO_CHANNEL[_key] = f'分報_{_key}'

# Slack送信者ごとのデフォルトプロジェクト（案件未指定時に適用）
DEFAULT_PROJECT = {
    'rena': '中国銀行',
    'kanako': 'Stock point',
}

# NAME_MAPキー → Slack user ID
_name_to_slack_id = {
    'ayano': 'U01FRMDAV7C',
    'midori': 'U01HKPTH73P',
    'kanako': 'U048WF9GFHD',
    'rena': 'U02M0EKD1DJ',
    'tatsuya': 'U6RA29B50',
    'satsuki': 'U0A2Y6AM9V4',
    'chihiro': 'U0ABCPF9XKQ',
}
# Slack user ID → NAME_MAPキーのキャッシュ（逆引き）
_slack_name_cache = {v: k for k, v in _name_to_slack_id.items()}


def resolve_name(name):
    return NAME_MAP.get(name.lower(), name)


def build_slack_id_cache():
    """起動時にSlack users.listから未登録メンバーのIDを補完"""
    try:
        res = app.client.users_list()
        for u in res.get('members', []):
            if u.get('is_bot') or u.get('deleted'):
                continue
            display = (u.get('profile', {}).get('display_name', '') or
                       u.get('real_name', '')).lower()
            for key in NAME_MAP:
                if key not in _name_to_slack_id and key in display:
                    _slack_name_cache[u['id']] = key
                    _name_to_slack_id[key] = u['id']
                    break
    except Exception as e:
        print(f"[build_slack_id_cache] {e}")


def slack_user_to_key(user_id):
    """Slack user IDからNAME_MAPのキーを返す"""
    if user_id in _slack_name_cache:
        return _slack_name_cache[user_id]
    try:
        info = app.client.users_info(user=user_id)
        display = (info['user'].get('profile', {}).get('display_name', '') or
                   info['user'].get('real_name', '')).lower()
        for key in NAME_MAP:
            if key in display:
                _slack_name_cache[user_id] = key
                _name_to_slack_id[key] = user_id
                return key
    except Exception:
        pass
    _slack_name_cache[user_id] = None
    return None


def next_id(dataset):
    prefix = 'w' if dataset == 'work' else 'a'
    ids = [r[0] for r in ws.get_all_values()[1:] if r[0].startswith(prefix)]
    nums = [int(i[1:]) for i in ids if i[1:].isdigit()]
    return f"{prefix}{max(nums, default=0) + 1}"


def parse_task(text):
    # Remove mentions
    text = re.sub(r'<@[A-Z0-9]+>', '', text).strip()
    # Unwrap Slack auto-links: <http://tenki.jp|tenki.jp> -> tenki.jp
    text = re.sub(r'<[^|>]+\|([^>]+)>', r'\1', text)
    text = re.sub(r'<([^>]+)>', r'\1', text)

    task = {
        'project': '', 'name': '', 'assignees': '',
        'date': '', 'dateStart': '', 'dateEnd': '',
        'status': 'todo', 'stars': '0', 'hearts': '0',
        'ballOwner': '', 'notes': '', 'dataset': 'work',
    }

    # Extract fields from bullet-point lines or inline keywords
    bullet = r'[•\-＊\*]\s*'

    # Process line by line to cleanly separate fields from task name
    lines = text.split('\n')
    remaining_lines = []

    for line in lines:
        stripped = line.strip()

        m = re.match(bullet + r'タスク[:：]\s*(.+)', stripped)
        if m:
            task['name'] = m.group(1).strip()
            continue

        m = re.match(bullet + r'案件[:：]\s*(.+)', stripped)
        if m:
            task['project'] = m.group(1).strip()
            continue

        m = re.match(bullet + r'担当[:：]\s*([^\s]+)', stripped)
        if m:
            names = [resolve_name(n.strip()) for n in m.group(1).split(',')]
            task['assignees'] = ','.join(names)
            continue

        m = re.match(bullet + r'期限[:：]\s*(\d{4}[-/]\d{1,2}[-/]\d{1,2})', stripped)
        if m:
            task['date'] = m.group(1).replace('/', '-')
            continue

        m = re.match(bullet + r'開始[:：]\s*(\d{4}[-/]\d{1,2}[-/]\d{1,2})', stripped)
        if m:
            task['dateStart'] = m.group(1).replace('/', '-')
            continue

        m = re.match(bullet + r'終了[:：]\s*(\d{4}[-/]\d{1,2}[-/]\d{1,2})', stripped)
        if m:
            task['dateEnd'] = m.group(1).replace('/', '-')
            continue

        m = re.match(bullet + r'ボール[:：]\s*([^\s]+)', stripped)
        if m:
            task['ballOwner'] = resolve_name(m.group(1).strip())
            continue

        m = re.match(bullet + r'カテゴリ[:：]\s*(アプリ|app)', stripped, re.I)
        if m:
            task['dataset'] = 'app'
            continue

        remaining_lines.append(stripped)

    # Also check inline keywords in remaining text
    text = ' '.join(remaining_lines).strip()

    m = re.search(r'タスク[:：]\s*([^\s]+(?:\s+[^\s:：]+)*)', text)
    if m and not task['name']:
        task['name'] = m.group(1).strip()
        text = text[:m.start()] + text[m.end():]

    m = re.search(r'担当[:：]\s*([^\s]+)', text)
    if m and not task['assignees']:
        names = [resolve_name(n.strip()) for n in m.group(1).split(',')]
        task['assignees'] = ','.join(names)
        text = text[:m.start()] + text[m.end():]

    m = re.search(r'期限[:：]\s*(\d{4}[-/]\d{1,2}[-/]\d{1,2})', text)
    if m and not task['date']:
        task['date'] = m.group(1).replace('/', '-')
        text = text[:m.start()] + text[m.end():]

    m = re.search(r'ボール[:：]\s*([^\s]+)', text)
    if m and not task['ballOwner']:
        task['ballOwner'] = resolve_name(m.group(1).strip())
        text = text[:m.start()] + text[m.end():]

    if re.search(r'(アプリ|app)\s*$', text, re.I):
        task['dataset'] = 'app'
        text = re.sub(r'\s*(アプリ|app)\s*$', '', text, flags=re.I)

    text = text.strip()

    # タスク名が明示指定済みなら残りテキストは案件名の推定のみ
    if task['name']:
        if not task['project'] and text:
            task['project'] = text.split(None, 1)[0]
    else:
        # If project was not set via 案件: field, try first-word convention
        if not task['project']:
            parts = text.split(None, 1)
            if len(parts) >= 2:
                task['project'] = parts[0]
                task['name'] = parts[1].strip()
            elif len(parts) == 1:
                task['name'] = parts[0]
        else:
            task['name'] = text

    return task


@app.event("app_mention")
def handle_mention(event, say):
    text = event.get('text', '')

    if 'help' in text.lower() or 'ヘルプ' in text:
        say(
            text=(
                "*TODO Bot の使い方*\n"
                "`@todo-bot [案件名] タスク名 担当:name 期限:YYYY-MM-DD`\n\n"
                "例:\n"
                "• `@todo-bot リブセンス Meta配信セット 担当:rena,ayano 期限:2026-03-10`\n"
                "• `@todo-bot 青天気 バナーリサイズ 担当:rena`\n"
                "• `@todo-bot アプリ UI改善 担当:rena app`\n\n"
                "オプション:\n"
                "• `担当:` 担当者（カンマ区切り）\n"
                "• `期限:` Fix Day\n"
                "• `開始:` 作業開始日\n"
                "• `終了:` 作業終了日\n"
                "• 末尾に `app` でアプリPJTに追加"
            ),
            thread_ts=event.get('ts'),
        )
        return

    # ルーティンタスク生成コマンド
    clean = re.sub(r'<@[A-Z0-9]+>', '', text).strip().lower()
    if clean in ('routine', 'routines', 'ルーティン'):
        try:
            count = generate_routines()
            if count > 0:
                say(text=f":arrows_counterclockwise: ルーティンタスクを {count} 件追加しました（{_monday_of_week()} 週）",
                    thread_ts=event.get('ts'))
            else:
                say(text=f":white_check_mark: 今週（{_monday_of_week()}）のルーティンは生成済みです",
                    thread_ts=event.get('ts'))
        except Exception as e:
            say(text=f":x: ルーティン生成エラー: {e}", thread_ts=event.get('ts'))
        return

    task = parse_task(text)

    if not task['name']:
        say(text="タスク名がわかりませんでした。\n`@todo-bot [案件名] タスク名 担当:name 期限:YYYY-MM-DD`", thread_ts=event.get('ts'))
        return

    # 案件未指定なら送信者のデフォルトプロジェクトを適用
    if not task['project']:
        sender = slack_user_to_key(event.get('user', ''))
        if sender and sender in DEFAULT_PROJECT:
            task['project'] = DEFAULT_PROJECT[sender]

    # AI案件名正規化
    original_project = task['project']
    task['project'] = normalize_project(task['project'])

    tid = next_id(task['dataset'])
    group = '既存案件' if task['dataset'] == 'work' else ''

    row = [tid, task['name'], task['project'], task['status'],
           task['date'], task['dateStart'], task['dateEnd'],
           task['assignees'], task['stars'], task['hearts'],
           task['ballOwner'], task['notes'], group, task['dataset']]

    ws.append_row(row)

    normalized_note = ''
    if original_project and original_project != task['project']:
        normalized_note = f"\n(案件名: {original_project} → {task['project']} に統一しました)"

    say(
        text=f":white_check_mark: *{task['name']}* を追加しました{normalized_note}",
        thread_ts=event.get('ts'),
    )


# Column index map (0-based)
COL_MAP = {
    'id': 1, 'name': 2, 'project': 3, 'status': 4,
    'date': 5, 'dateStart': 6, 'dateEnd': 7,
    'assignees': 8, 'stars': 9, 'hearts': 10,
    'ballOwner': 11, 'notes': 12, 'group': 13, 'dataset': 14,
}


def find_row_by_id(task_id):
    """Find the row number (1-based) for a given task ID."""
    rows = ws.get_all_values()
    for i, r in enumerate(rows):
        if r and r[0] == task_id:
            return i + 1  # 1-based
    return None


def update_cell(task_id, field, value):
    """Update a single cell in the sheet by task ID and field name."""
    row_num = find_row_by_id(task_id)
    if not row_num:
        return False
    col_num = COL_MAP.get(field)
    if not col_num:
        return False
    ws.update_cell(row_num, col_num, value)
    return True


# Settings sheet (project colors etc.)
def get_settings_ws():
    try:
        return sh.worksheet('settings')
    except gspread.exceptions.WorksheetNotFound:
        sws = sh.add_worksheet(title='settings', rows=100, cols=2)
        sws.update_cell(1, 1, 'key')
        sws.update_cell(1, 2, 'value')
        return sws


def get_proj_colors():
    sws = get_settings_ws()
    rows = sws.get_all_values()
    colors = {}
    for r in rows[1:]:
        if len(r) >= 2 and r[0].startswith('projcolor:'):
            colors[r[0][len('projcolor:'):]] = r[1]
    return colors


def get_project_aliases():
    """settingsシートから alias:正式名 = 別名1, 別名2 形式のマッピングを取得"""
    sws = get_settings_ws()
    rows = sws.get_all_values()
    aliases = {}  # {別名: 正式名}
    for r in rows[1:]:
        if len(r) >= 2 and r[0].startswith('alias:'):
            canonical = r[0][len('alias:'):]
            for alt in r[1].split(','):
                alt = alt.strip()
                if alt:
                    aliases[alt] = canonical
            aliases[canonical] = canonical  # 正式名自身もマッピング
    return aliases


def get_existing_projects():
    """シートから既存の案件名一覧を取得（重複除去）"""
    rows = ws.get_all_values()[1:]
    projects = set()
    for r in rows:
        if len(r) >= 3 and r[2].strip():
            projects.add(r[2].strip())
    return sorted(projects)


def normalize_project(raw_name):
    """案件名をエイリアステーブル + あいまい一致で正規化する"""
    if not raw_name:
        return raw_name

    # Step 1: エイリアステーブルで完全一致
    aliases = get_project_aliases()
    if raw_name in aliases:
        result = aliases[raw_name]
        if result != raw_name:
            print(f"[normalize] alias hit: '{raw_name}' -> '{result}'")
        return result

    # Step 2: 既存案件リストとあいまい一致
    existing = get_existing_projects()
    if not existing or raw_name in existing:
        return raw_name

    matches = difflib.get_close_matches(raw_name, existing, n=1, cutoff=0.6)
    if matches:
        print(f"[normalize] fuzzy: '{raw_name}' -> '{matches[0]}'")
        return matches[0]

    print(f"[normalize] new project: '{raw_name}'")
    return raw_name


def get_clients_ws():
    """clientsシートを取得（なければ作成）"""
    try:
        return sh.worksheet('clients')
    except gspread.exceptions.WorksheetNotFound:
        cws = sh.add_worksheet(title='clients', rows=200, cols=2)
        cws.append_row(['id', 'name'])
        return cws


def get_projects_ws():
    """projectsシートを取得（なければ作成）"""
    try:
        return sh.worksheet('projects')
    except gspread.exceptions.WorksheetNotFound:
        pws = sh.add_worksheet(title='projects', rows=500, cols=3)
        pws.append_row(['id', 'name', 'clientId'])
        return pws


def get_routines_ws():
    """routinesシートを取得（なければ作成）"""
    try:
        return sh.worksheet('routines')
    except gspread.exceptions.WorksheetNotFound:
        rws = sh.add_worksheet(title='routines', rows=100, cols=10)
        headers = ['name', 'project', 'assignees', 'ballOwner',
                   'stars', 'hearts', 'frequency', 'group', 'dataset', 'notes']
        rws.append_row(headers)
        return rws


def _monday_of_week(dt=None):
    """指定日の週の月曜日をYYYY-MM-DD文字列で返す"""
    if dt is None:
        dt = datetime.now()
    monday = dt - timedelta(days=dt.weekday())
    return monday.strftime('%Y-%m-%d')


def generate_routines(force=False):
    """routinesシートからタスクを生成する。同じ週に既に生成済みならスキップ。"""
    rws = get_routines_ws()
    routines = rws.get_all_records()
    if not routines:
        print("[routines] no routines defined")
        return 0

    week_key = _monday_of_week()

    # 既に今週生成済みか確認（notesに週キーを埋め込む）
    marker = f'routine:{week_key}'
    existing_notes = [r[COL_MAP['notes'] - 1] for r in ws.get_all_values()[1:]
                      if len(r) >= COL_MAP['notes']]
    if not force and marker in existing_notes:
        print(f"[routines] already generated for week {week_key}")
        return 0

    count = 0
    for r in routines:
        freq = r.get('frequency', 'weekly').strip().lower()
        now = datetime.now()

        # frequency判定
        if freq == 'monthly' and now.day > 7:
            continue  # 月初の週のみ生成
        elif freq == 'biweekly':
            week_num = now.isocalendar()[1]
            if week_num % 2 != 0:
                continue  # 偶数週のみ

        dataset = r.get('dataset', 'work').strip() or 'work'
        tid = next_id(dataset)
        group = r.get('group', '既存案件').strip() or '既存案件'

        row = [
            tid,
            r.get('name', ''),
            r.get('project', ''),
            'todo',
            '',  # date
            '',  # dateStart
            '',  # dateEnd
            r.get('assignees', ''),
            str(r.get('stars', 0)),
            str(r.get('hearts', 0)),
            r.get('ballOwner', ''),
            marker,  # notesにマーカーを埋め込み
            group,
            dataset,
        ]
        ws.append_row(row)
        count += 1
        print(f"[routines] added: {r.get('name')} ({tid})")

    print(f"[routines] generated {count} tasks for week {week_key}")
    return count


def start_routine_scheduler():
    """毎週月曜 09:00 JST にルーティンタスクを自動生成するスケジューラ"""
    def _run():
        while True:
            now = datetime.now()
            # 次の月曜 09:00 を計算
            days_until_monday = (7 - now.weekday()) % 7
            if days_until_monday == 0 and now.hour >= 9:
                days_until_monday = 7
            next_monday = now.replace(hour=9, minute=0, second=0, microsecond=0) + timedelta(days=days_until_monday)
            wait_seconds = (next_monday - now).total_seconds()
            print(f"[routine_scheduler] next run: {next_monday} (in {wait_seconds:.0f}s)")
            threading.Event().wait(wait_seconds)
            try:
                generate_routines()
            except Exception as e:
                print(f"[routine_scheduler] ERROR: {e}")

    threading.Thread(target=_run, daemon=True).start()


def set_proj_color(project, color_id):
    sws = get_settings_ws()
    rows = sws.get_all_values()
    key = f'projcolor:{project}'
    for i, r in enumerate(rows):
        if r and r[0] == key:
            sws.update_cell(i + 1, 2, color_id)
            return
    sws.append_row([key, color_id])


# チャンネル名 → ID のキャッシュ
_channel_id_cache = {}


def _build_channel_cache():
    """全チャンネルをページネーション付きで取得しキャッシュに格納"""
    try:
        cursor = None
        while True:
            kwargs = dict(types='public_channel,private_channel', limit=200)
            if cursor:
                kwargs['cursor'] = cursor
            res = app.client.conversations_list(**kwargs)
            for ch in res['channels']:
                _channel_id_cache[ch['name']] = ch['id']
            cursor = res.get('response_metadata', {}).get('next_cursor')
            if not cursor:
                break
        print(f"[channel_cache] {len(_channel_id_cache)} channels cached")
    except Exception as e:
        print(f"[channel_cache] ERROR: {e}")


def notify_bunpo(channel_name, task_label, from_owner, new_owner_key):
    """分報チャンネルにボール移動の通知を送信（メンション付き）"""
    try:
        # キャッシュが空なら全チャンネルを取得
        if not _channel_id_cache:
            _build_channel_cache()
        ch_id = _channel_id_cache.get(channel_name)
        if not ch_id:
            print(f"[notify_bunpo] channel '{channel_name}' not found. Available: {[k for k in _channel_id_cache if '分報' in k]}")
            return
        # メンション用のSlack user IDを取得
        slack_id = _name_to_slack_id.get(new_owner_key)
        mention = f"<@{slack_id}>" if slack_id else new_owner_key
        app.client.chat_postMessage(
            channel=ch_id,
            text=f":basketball: {mention} *{task_label}* のボールが回ってきました（{from_owner} →）",
        )
        print(f"[notify_bunpo] OK: {channel_name} <- {task_label}")
    except Exception as e:
        print(f"[notify_bunpo] ERROR {channel_name}: {e}")


# API + Health check server
class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == '/api/tasks':
            try:
                acquired = _sheet_lock.acquire(timeout=15)
                if not acquired:
                    print('[API /api/tasks] lock timeout')
                    self._json(503, {'error': 'lock timeout'})
                    return
                try:
                    task_ws = get_tasks_ws()
                    rows = task_ws.get_all_values()
                finally:
                    _sheet_lock.release()
                if len(rows) <= 1:
                    self._json(200, [])
                    return
                hdr = rows[0]
                tasks = []
                for r in rows[1:]:
                    if len(r) >= 2 and r[0]:
                        t = {}
                        for j, col in enumerate(hdr):
                            t[col] = r[j] if j < len(r) else ''
                        tasks.append(t)
                self._json(200, tasks)
            except Exception as e:
                print(f'[API /api/tasks] ERROR: {e}')
                self._json(500, {'error': str(e)})
            return
        if self.path == '/api/clients':
            try:
                acquired = _sheet_lock.acquire(timeout=15)
                if not acquired:
                    self._json(503, {'error': 'lock timeout'})
                    return
                try:
                    cws = get_clients_ws()
                    rows = cws.get_all_values()
                finally:
                    _sheet_lock.release()
                if len(rows) <= 1:
                    self._json(200, [])
                    return
                hdr = rows[0]
                clients = []
                for r in rows[1:]:
                    if len(r) >= 2 and r[0]:
                        clients.append({'id': r[0], 'name': r[1] if len(r) > 1 else ''})
                self._json(200, clients)
            except Exception as e:
                print(f'[API /api/clients] ERROR: {e}')
                self._json(500, {'error': str(e)})
            return
        if self.path == '/api/projects':
            try:
                acquired = _sheet_lock.acquire(timeout=15)
                if not acquired:
                    self._json(503, {'error': 'lock timeout'})
                    return
                try:
                    pws = get_projects_ws()
                    rows = pws.get_all_values()
                finally:
                    _sheet_lock.release()
                if len(rows) <= 1:
                    self._json(200, [])
                    return
                projects = []
                for r in rows[1:]:
                    if len(r) >= 2 and r[0]:
                        projects.append({'id': r[0], 'name': r[1] if len(r) > 1 else '', 'clientId': r[2] if len(r) > 2 else ''})
                self._json(200, projects)
            except Exception as e:
                print(f'[API /api/projects] ERROR: {e}')
                self._json(500, {'error': str(e)})
            return
        if self.path == '/api/projcolors':
            try:
                self._json(200, get_proj_colors())
            except Exception as e:
                self._json(500, {'error': str(e)})
            return
        if self.path == '/api/debug':
            # キャッシュが空なら再構築を試みてエラーを返す
            cache_error = None
            if not _channel_id_cache:
                try:
                    _build_channel_cache()
                except Exception as e:
                    cache_error = str(e)
            bunpo_channels = [k for k in _channel_id_cache if '分報' in k or 'bunpo' in k or 'funho' in k]
            # Slack API直接テスト
            slack_test = None
            try:
                test_res = app.client.conversations_list(types='public_channel,private_channel', limit=5)
                slack_test = {
                    'ok': test_res.get('ok'),
                    'channel_count': len(test_res.get('channels', [])),
                    'channels': [c['name'] for c in test_res.get('channels', [])],
                }
            except Exception as e:
                slack_test = {'error': str(e)}
            # gspreadの疎通テスト
            gspread_test = None
            try:
                ws_test = get_tasks_ws()
                row_count = len(ws_test.get_all_values())
                gspread_test = {'ok': True, 'rows': row_count}
            except Exception as e:
                gspread_test = {'ok': False, 'error': str(e)}
            self._json(200, {
                'version': '2026-05-29-b',
                'bunpo_channel_map': BUNPO_CHANNEL,
                'channel_cache_size': len(_channel_id_cache),
                'bunpo_in_cache': {ch: _channel_id_cache[ch] for ch in bunpo_channels},
                'all_channels_sample': list(_channel_id_cache.keys())[:50],
                'cache_rebuild_error': cache_error,
                'slack_api_test': slack_test,
                'gspread_test': gspread_test,
            })
            return
        if self.path == '/api/routines':
            try:
                force = 'force' in (self.path + '?' + (self.headers.get('X-Force', '') or ''))
                count = generate_routines(force=force)
                self._json(200, {'ok': True, 'generated': count, 'week': _monday_of_week()})
            except Exception as e:
                self._json(500, {'error': str(e)})
            return
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b'OK')

    def do_POST(self):
        if self.path == '/api/clients':
            length = int(self.headers.get('Content-Length', 0))
            body = json.loads(self.rfile.read(length))
            action = body.get('action', 'add')
            try:
                cws = get_clients_ws()
                if action == 'add':
                    cid = body.get('id', 'c' + str(int(datetime.now().timestamp() * 1000)))
                    name = body.get('name', '')
                    if not name:
                        self._json(400, {'error': 'name required'})
                        return
                    cws.append_row([cid, name])
                    self._json(200, {'ok': True, 'id': cid})
                elif action == 'delete':
                    cid = body.get('id')
                    rows = cws.get_all_values()
                    for i, r in enumerate(rows):
                        if r and r[0] == cid:
                            cws.delete_rows(i + 1)
                            break
                    self._json(200, {'ok': True})
                elif action == 'rename':
                    cid = body.get('id')
                    new_name = body.get('name', '')
                    rows = cws.get_all_values()
                    for i, r in enumerate(rows):
                        if r and r[0] == cid:
                            cws.update_cell(i + 1, 2, new_name)
                            break
                    self._json(200, {'ok': True})
                elif action == 'sync':
                    # Bulk sync: replace all data
                    clients = body.get('clients', [])
                    cws.clear()
                    cws.append_row(['id', 'name'])
                    if clients:
                        cws.append_rows([[c['id'], c['name']] for c in clients])
                    self._json(200, {'ok': True, 'count': len(clients)})
                else:
                    self._json(400, {'error': 'unknown action'})
            except Exception as e:
                self._json(500, {'error': str(e)})
        elif self.path == '/api/projects':
            length = int(self.headers.get('Content-Length', 0))
            body = json.loads(self.rfile.read(length))
            action = body.get('action', 'add')
            try:
                pws = get_projects_ws()
                if action == 'add':
                    pid = body.get('id', 'p' + str(int(datetime.now().timestamp() * 1000)))
                    name = body.get('name', '')
                    client_id = body.get('clientId', '')
                    if not name:
                        self._json(400, {'error': 'name required'})
                        return
                    pws.append_row([pid, name, client_id])
                    self._json(200, {'ok': True, 'id': pid})
                elif action == 'delete':
                    pid = body.get('id')
                    rows = pws.get_all_values()
                    for i, r in enumerate(rows):
                        if r and r[0] == pid:
                            pws.delete_rows(i + 1)
                            break
                    self._json(200, {'ok': True})
                elif action == 'rename':
                    pid = body.get('id')
                    new_name = body.get('name', '')
                    rows = pws.get_all_values()
                    for i, r in enumerate(rows):
                        if r and r[0] == pid:
                            pws.update_cell(i + 1, 2, new_name)
                            break
                    self._json(200, {'ok': True})
                elif action == 'sync':
                    projects = body.get('projects', [])
                    pws.clear()
                    pws.append_row(['id', 'name', 'clientId'])
                    if projects:
                        pws.append_rows([[p['id'], p['name'], p.get('clientId', '')] for p in projects])
                    self._json(200, {'ok': True, 'count': len(projects)})
                else:
                    self._json(400, {'error': 'unknown action'})
            except Exception as e:
                self._json(500, {'error': str(e)})
        elif self.path == '/api/update':
            length = int(self.headers.get('Content-Length', 0))
            body = json.loads(self.rfile.read(length))
            task_id = body.get('id')
            updates = body.get('updates', {})
            if not task_id or not updates:
                self._json(400, {'error': 'id and updates required'})
                return
            try:
                with _sheet_lock:
                    task_ws = get_tasks_ws()
                    rows = task_ws.get_all_values()
                    row_num = None
                    for i, r in enumerate(rows):
                        if r and r[0] == task_id:
                            row_num = i + 1
                            break
                    if not row_num:
                        self._json(404, {'error': f'task {task_id} not found'})
                        return
                    # ballOwner変更検知用: 更新前の値を取得
                    old_ball = ''
                    if 'ballOwner' in updates:
                        row_data = task_ws.row_values(row_num)
                        old_ball = (row_data[COL_MAP['ballOwner'] - 1] if len(row_data) >= COL_MAP['ballOwner'] else '').strip()
                    for field, value in updates.items():
                        col_num = COL_MAP.get(field)
                        if col_num:
                            task_ws.update_cell(row_num, col_num, str(value))
                # ballOwner変更時に分報チャンネルへ通知
                new_ball = updates.get('ballOwner', '').strip()
                print(f"[api/update] task={task_id} ballOwner in updates={'ballOwner' in updates} old='{old_ball}' new='{new_ball}'")
                if 'ballOwner' in updates and new_ball != old_ball and new_ball:
                    print(f"[ball_change] '{old_ball}' -> '{new_ball}' (task {task_id})")
                    channel_name = BUNPO_CHANNEL.get(new_ball)
                    if channel_name:
                        with _sheet_lock:
                            task_ws = get_tasks_ws()
                            row_data = task_ws.row_values(row_num)
                        task_name = row_data[COL_MAP['name'] - 1] if len(row_data) >= COL_MAP['name'] else task_id
                        project = row_data[COL_MAP['project'] - 1] if len(row_data) >= COL_MAP['project'] else ''
                        label = f"[{project}] {task_name}" if project else task_name
                        owner_key = channel_name.replace('分報_', '')
                        notify_bunpo(channel_name, label, old_ball or '(未設定)', owner_key)
                    else:
                        print(f"[ball_change] no channel mapping for '{new_ball}'. Known: {list(BUNPO_CHANNEL.keys())}")
                elif 'ballOwner' in updates:
                    print(f"[ball_change] SKIPPED: same value or empty (old='{old_ball}' new='{new_ball}')")
                self._json(200, {'ok': True, 'row': row_num, 'updates': updates})
            except Exception as e:
                print(f'[API /api/update] ERROR: {e}')
                self._json(500, {'error': str(e)})
        elif self.path == '/api/projcolors':
            length = int(self.headers.get('Content-Length', 0))
            body = json.loads(self.rfile.read(length))
            project = body.get('project')
            color_id = body.get('colorId')
            if not project or not color_id:
                self._json(400, {'error': 'project and colorId required'})
                return
            try:
                set_proj_color(project, color_id)
                self._json(200, {'ok': True})
            except Exception as e:
                self._json(500, {'error': str(e)})
        elif self.path == '/api/create':
            length = int(self.headers.get('Content-Length', 0))
            body = json.loads(self.rfile.read(length))
            task = body.get('task', {})
            if not task.get('id') or not task.get('name'):
                self._json(400, {'error': 'task.id and task.name required'})
                return
            try:
                row = [
                    task.get('id', ''),
                    task.get('name', ''),
                    task.get('project', ''),
                    task.get('status', 'todo'),
                    task.get('date', ''),
                    task.get('dateStart', ''),
                    task.get('dateEnd', ''),
                    ','.join(task.get('assignees', [])) if isinstance(task.get('assignees'), list) else task.get('assignees', ''),
                    str(task.get('stars', 0)),
                    str(task.get('hearts', 0)),
                    task.get('ballOwner', ''),
                    task.get('notes', ''),
                    task.get('group', ''),
                    task.get('dataset', 'work'),
                ]
                with _sheet_lock:
                    task_ws = get_tasks_ws()
                    task_ws.append_row(row)
                self._json(200, {'ok': True, 'id': task['id']})
            except Exception as e:
                print(f'[API /api/create] ERROR: {e}')
                self._json(500, {'error': str(e)})
        elif self.path == '/api/delete':
            length = int(self.headers.get('Content-Length', 0))
            body = json.loads(self.rfile.read(length))
            task_id = body.get('id')
            if not task_id:
                self._json(400, {'error': 'id required'})
                return
            try:
                with _sheet_lock:
                    task_ws = get_tasks_ws()
                    rows = task_ws.get_all_values()
                    row_num = None
                    for i, r in enumerate(rows):
                        if r and r[0] == task_id:
                            row_num = i + 1
                            break
                    if not row_num:
                        self._json(404, {'error': f'task {task_id} not found'})
                        return
                    task_ws.delete_rows(row_num)
                self._json(200, {'ok': True, 'deleted': task_id})
            except Exception as e:
                print(f'[API /api/delete] ERROR: {e}')
                self._json(500, {'error': str(e)})
        else:
            self._json(404, {'error': 'not found'})

    def do_OPTIONS(self):
        self.send_response(200)
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'POST, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type')
        self.end_headers()

    def _json(self, code, data):
        self.send_response(code)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.end_headers()
        self.wfile.write(json.dumps(data).encode())

    def log_message(self, *args):
        pass

def start_health_server():
    port = int(os.environ.get('PORT', 10000))
    HTTPServer(('0.0.0.0', port), HealthHandler).serve_forever()


def keep_alive():
    """Self-ping every 5 minutes to prevent Render free tier from sleeping."""
    url = os.environ.get('RENDER_EXTERNAL_URL', 'https://todo-bot-u0rs.onrender.com')
    while True:
        threading.Event().wait(300)  # 5 minutes
        try:
            urllib.request.urlopen(url, timeout=10)
        except Exception:
            pass


if __name__ == '__main__':
    threading.Thread(target=start_health_server, daemon=True).start()
    threading.Thread(target=keep_alive, daemon=True).start()
    start_routine_scheduler()
    build_slack_id_cache()
    _build_channel_cache()
    handler = SocketModeHandler(app, os.environ['SLACK_APP_TOKEN'])
    handler.start()
