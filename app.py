"""Slack Bot for Team TODO - adds tasks to Google Sheets via mentions."""
import os
import re
import json
import base64
import threading
import urllib.request
import gspread
from google.oauth2.service_account import Credentials
from slack_bolt import App
from slack_bolt.adapter.socket_mode import SocketModeHandler
from http.server import HTTPServer, BaseHTTPRequestHandler

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

    task = parse_task(text)

    if not task['name']:
        say(text="タスク名がわかりませんでした。\n`@todo-bot [案件名] タスク名 担当:name 期限:YYYY-MM-DD`", thread_ts=event.get('ts'))
        return

    # 案件未指定なら送信者のデフォルトプロジェクトを適用
    if not task['project']:
        sender = slack_user_to_key(event.get('user', ''))
        if sender and sender in DEFAULT_PROJECT:
            task['project'] = DEFAULT_PROJECT[sender]

    tid = next_id(task['dataset'])
    group = '既存案件' if task['dataset'] == 'work' else ''

    row = [tid, task['name'], task['project'], task['status'],
           task['date'], task['dateStart'], task['dateEnd'],
           task['assignees'], task['stars'], task['hearts'],
           task['ballOwner'], task['notes'], group, task['dataset']]

    ws.append_row(row)

    say(
        text=f":white_check_mark: *{task['name']}* を追加しました",
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
        if self.path == '/api/projcolors':
            try:
                self._json(200, get_proj_colors())
            except Exception as e:
                self._json(500, {'error': str(e)})
            return
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b'OK')

    def do_POST(self):
        if self.path == '/api/update':
            length = int(self.headers.get('Content-Length', 0))
            body = json.loads(self.rfile.read(length))
            task_id = body.get('id')
            updates = body.get('updates', {})
            if not task_id or not updates:
                self._json(400, {'error': 'id and updates required'})
                return
            try:
                row_num = find_row_by_id(task_id)
                if not row_num:
                    self._json(404, {'error': f'task {task_id} not found'})
                    return
                # ballOwner変更検知用: 更新前の値を取得
                old_ball = ''
                if 'ballOwner' in updates:
                    row_data = ws.row_values(row_num)
                    old_ball = (row_data[COL_MAP['ballOwner'] - 1] if len(row_data) >= COL_MAP['ballOwner'] else '').strip()
                for field, value in updates.items():
                    col_num = COL_MAP.get(field)
                    if col_num:
                        ws.update_cell(row_num, col_num, str(value))
                # ballOwner変更時に分報チャンネルへ通知
                new_ball = updates.get('ballOwner', '').strip()
                if 'ballOwner' in updates and new_ball != old_ball and new_ball:
                    print(f"[ball_change] '{old_ball}' -> '{new_ball}' (task {task_id})")
                    channel_name = BUNPO_CHANNEL.get(new_ball)
                    if channel_name:
                        row_data = ws.row_values(row_num)
                        task_name = row_data[COL_MAP['name'] - 1] if len(row_data) >= COL_MAP['name'] else task_id
                        project = row_data[COL_MAP['project'] - 1] if len(row_data) >= COL_MAP['project'] else ''
                        label = f"[{project}] {task_name}" if project else task_name
                        owner_key = channel_name.replace('分報_', '')
                        notify_bunpo(channel_name, label, old_ball or '(未設定)', owner_key)
                    else:
                        print(f"[ball_change] no channel mapping for '{new_ball}'. Known: {list(BUNPO_CHANNEL.keys())}")
                self._json(200, {'ok': True, 'row': row_num, 'updates': updates})
            except Exception as e:
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
        elif self.path == '/api/delete':
            length = int(self.headers.get('Content-Length', 0))
            body = json.loads(self.rfile.read(length))
            task_id = body.get('id')
            if not task_id:
                self._json(400, {'error': 'id required'})
                return
            try:
                row_num = find_row_by_id(task_id)
                if not row_num:
                    self._json(404, {'error': f'task {task_id} not found'})
                    return
                ws.delete_rows(row_num)
                self._json(200, {'ok': True, 'deleted': task_id})
            except Exception as e:
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
    build_slack_id_cache()
    _build_channel_cache()
    handler = SocketModeHandler(app, os.environ['SLACK_APP_TOKEN'])
    handler.start()
