"""Slack Bot for Team TODO - adds tasks to Google Sheets via mentions."""
import os
import re
import json
import threading
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
creds_json = json.loads(os.environ['GOOGLE_CREDENTIALS'])
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


def resolve_name(name):
    return NAME_MAP.get(name.lower(), name)


def next_id(dataset):
    prefix = 'w' if dataset == 'work' else 'a'
    ids = [r[0] for r in ws.get_all_values()[1:] if r[0].startswith(prefix)]
    nums = [int(i[1:]) for i in ids if i[1:].isdigit()]
    return f"{prefix}{max(nums, default=0) + 1}"


def parse_task(text):
    text = re.sub(r'<@[A-Z0-9]+>', '', text).strip()

    task = {
        'project': '', 'name': '', 'assignees': '',
        'date': '', 'dateStart': '', 'dateEnd': '',
        'status': 'todo', 'stars': '0', 'hearts': '0',
        'ballOwner': '', 'notes': '', 'dataset': 'work',
    }

    m = re.search(r'担当[:：]\s*([^\s]+)', text)
    if m:
        names = [resolve_name(n.strip()) for n in m.group(1).split(',')]
        task['assignees'] = ','.join(names)
        text = text[:m.start()] + text[m.end():]

    m = re.search(r'期限[:：]\s*(\d{4}[-/]\d{1,2}[-/]\d{1,2})', text)
    if m:
        task['date'] = m.group(1).replace('/', '-')
        text = text[:m.start()] + text[m.end():]

    m = re.search(r'開始[:：]\s*(\d{4}[-/]\d{1,2}[-/]\d{1,2})', text)
    if m:
        task['dateStart'] = m.group(1).replace('/', '-')
        text = text[:m.start()] + text[m.end():]

    m = re.search(r'終了[:：]\s*(\d{4}[-/]\d{1,2}[-/]\d{1,2})', text)
    if m:
        task['dateEnd'] = m.group(1).replace('/', '-')
        text = text[:m.start()] + text[m.end():]

    if re.search(r'(アプリ|app)\s*$', text, re.I):
        task['dataset'] = 'app'
        text = re.sub(r'\s*(アプリ|app)\s*$', '', text, flags=re.I)

    text = text.strip()
    parts = text.split(None, 1)
    if len(parts) >= 2:
        task['project'] = parts[0]
        task['name'] = parts[1].strip()
    elif len(parts) == 1:
        task['name'] = parts[0]

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

    tid = next_id(task['dataset'])
    group = '既存案件' if task['dataset'] == 'work' else ''

    row = [tid, task['name'], task['project'], task['status'],
           task['date'], task['dateStart'], task['dateEnd'],
           task['assignees'], task['stars'], task['hearts'],
           task['ballOwner'], task['notes'], group, task['dataset']]

    ws.append_row(row)

    ds_label = 'アプリPJT' if task['dataset'] == 'app' else '既存案件'
    assignee_str = task['assignees'] or '未設定'
    date_str = task['date'] or '未設定'

    say(
        text=(
            f"*TODO追加しました* :white_check_mark:\n"
            f"• タスク: {task['name']}\n"
            f"• 案件: {task['project'] or '未設定'}\n"
            f"• 担当: {assignee_str}\n"
            f"• 期限: {date_str}\n"
            f"• カテゴリ: {ds_label}\n"
            f"_アプリで同期ボタンを押すと反映されます_"
        ),
        thread_ts=event.get('ts'),
    )


# Health check server to keep Render awake
class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b'OK')
    def log_message(self, *args):
        pass

def start_health_server():
    port = int(os.environ.get('PORT', 10000))
    HTTPServer(('0.0.0.0', port), HealthHandler).serve_forever()


if __name__ == '__main__':
    threading.Thread(target=start_health_server, daemon=True).start()
    handler = SocketModeHandler(app, os.environ['SLACK_APP_TOKEN'])
    handler.start()
