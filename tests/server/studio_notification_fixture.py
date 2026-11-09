import json
import os
from pathlib import Path
import socket
import sqlite3
import sys

assert os.environ.get('STUDIO_TEST_ONLY') == '1', 'test-only marker required'
root = Path(os.environ['STUDIO_TEST_ROOT'])
assert root.is_absolute() and root == root.resolve(strict=True) and root != Path(root.anchor)
assert Path(os.environ['HERMES_STUDIO_NOTIFICATION_DB']).resolve(strict=True).is_relative_to(root)
os.environ['HOME'] = str(root / 'home')
os.environ['TMPDIR'] = str(root)
mode = sys.argv[1] if len(sys.argv) > 1 else 'serve'
if mode == 'seed':
    import tempfile
    root = Path(tempfile.mkdtemp(prefix='receipt-', dir=root))
home = root / 'core-home'
home.mkdir(exist_ok=True)
os.environ['HERMES_HOME'] = str(home)
os.environ['HERMES_AGENT_BRIDGE_BASE_HOME'] = str(home)
os.environ['HERMES_AGENT_BRIDGE_WORKER_PROFILE'] = 'default'
from hermes_cli import kanban_db as kb
from hermes_cli import kanban_db_notify as notify
from hermes_cli.active_sessions import try_acquire_active_session, release_active_session, transfer_active_session
from bridge_server import BridgeServer
from bridge_transport import _read_json_request, _write_json_response

source = root / 'source.db'
if not source.exists():
    with sqlite3.connect(source) as conn:
        conn.executescript(kb.SCHEMA_SQL)
        conn.execute("INSERT INTO tasks(id,title,status,created_at) VALUES('task','synthetic','done',1)")
        conn.execute("INSERT INTO task_events(id,task_id,kind,payload,created_at) VALUES(8,'task','completed','{\"summary\":\"synthetic local result\"}',1)")
        conn.execute("INSERT INTO kanban_notify_subs(task_id,platform,chat_id,thread_id,last_event_id,notifier_profile,delivery_mode,delivery_metadata,created_at) VALUES('task','tui','session-one','',3,'default','notify+wake','{\"studio_auto_continue\":\"v1\"}',1)")
kb.list_boards = lambda **kwargs: [{'slug': 'default', 'db_path': str(source)}]

if mode == 'hold':
    lease, refusal = try_acquire_active_session(session_id='session-one', surface='tui', config={},
        metadata={'live_session_id': 'synthetic-core-owner'}, registry_home=home)
    assert lease and not refusal
    print('held', flush=True)
    sys.stdin.readline()
    release_active_session(lease)
    sys.exit(0)

server = BridgeServer('synthetic')
if mode == 'seed':
    from bridge_compatibility import guard
    guard({'action': 'background_poll'})
    item = json.load(sys.stdin)
    sid = item['session_id']
    admission = server.pool._admit_session(sid, item['profile'])
    stat = source.stat()
    claim = {'source_event': item.get('event', {}), 'db_identity': (stat.st_dev, stat.st_ino),
             'old_cursor': 0, 'new_cursor': item['identity'][2]}
    try:
        with server.pool._notification_fence(sid, admission) as fence:
            server.pool._studio_notifications.accept(item, claim, fence)
    finally:
        server.pool._release_session_admission(sid)
    print('seeded exact fixture receipt', flush=True)
    sys.exit(0)
if mode == 'crash':
    def crash(*args):
        os._exit(71)
    server.pool._ack_studio_notification = crash
    server.handle({'action': 'background_poll', 'session_ids': ['session-one']})
    raise AssertionError('must crash after real save')

listener = socket.socket()
listener.bind(('127.0.0.1', 0))
listener.listen()
print(json.dumps({'endpoint': 'tcp://127.0.0.1:' + str(listener.getsockname()[1])}), flush=True)
try:
    while True:
        conn, _ = listener.accept()
        with conn:
            request = _read_json_request(conn)
            if request.get('action') == 'fixture_stop':
                _write_json_response(conn, {'ok': True})
                break
            try:
                response = server.handle(request)
                _write_json_response(conn, {'ok': True, **response})
            except Exception as exc:
                _write_json_response(conn, {'ok': False, 'error': str(exc)})
finally:
    listener.close()
    server.pool.destroy_all()
