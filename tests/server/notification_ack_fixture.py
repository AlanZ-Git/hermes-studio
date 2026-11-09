from contextlib import contextmanager
from pathlib import Path
import hashlib
import json
import os
import sqlite3
import tempfile
import threading

assert os.environ.get('STUDIO_TEST_ONLY') == '1', 'test-only marker required'
root = Path(os.environ['STUDIO_TEST_ROOT'])
assert root.is_absolute() and root == root.resolve(strict=True) and root != Path(root.anchor)
template = Path(os.environ['HERMES_STUDIO_NOTIFICATION_DB']).resolve(strict=True)
assert template.is_relative_to(root)
os.environ['HOME'] = str(root / 'home')
os.environ['TMPDIR'] = str(root)
root = Path(tempfile.mkdtemp(prefix='exact-ack-', dir=root))
os.environ['HERMES_HOME'] = str(root)
os.environ['HERMES_AGENT_BRIDGE_BASE_HOME'] = str(root)
from hermes_cli import kanban_db as kb
from hermes_cli.active_sessions import transfer_active_session, release_active_session
from bridge_pool import AgentPool
import bridge_studio_notifications as adapter

results = []
for case in ['normal', 'cursor-2', 'cursor-4', 'cursor-8', 'cursor-10', 'event-inside-ack',
             'source-inode-inside-ack', 'target-inode', 'receipt-digest', 'receipt-source',
             'schema-missing', 'schema-incompatible', 'transfer', 'release']:
    home = root / case
    home.mkdir()
    os.environ['HERMES_HOME'] = str(home)
    os.environ['HERMES_AGENT_BRIDGE_BASE_HOME'] = str(home)
    os.environ['HERMES_AGENT_BRIDGE_WORKER_PROFILE'] = 'default'
    path, target = home / 'source.db', home / 'studio.db'
    with sqlite3.connect(template) as original, sqlite3.connect(target) as copy:
        original.backup(copy)
    os.environ['HERMES_STUDIO_NOTIFICATION_DB'] = str(target)
    with sqlite3.connect(path) as db:
        db.executescript(kb.SCHEMA_SQL)
        db.execute("INSERT INTO tasks(id,title,status,created_at) VALUES('task','synthetic','done',1)")
        db.execute("INSERT INTO task_events(id,task_id,kind,payload,created_at) VALUES(8,'task','completed','{}',1)")
        db.execute("INSERT INTO kanban_notify_subs(task_id,platform,chat_id,thread_id,last_event_id,notifier_profile,delivery_mode,delivery_metadata,created_at) VALUES('task','tui','session','',3,'default','notify+wake','{\"studio_auto_continue\":\"v1\"}',1)")
    kb.list_boards = lambda **kwargs: [{'slug': 'default', 'db_path': str(path)}]
    if case.startswith('schema-'):
        with sqlite3.connect(target) as db:
            if case == 'schema-missing':
                db.execute('DROP TABLE kanban_notification_receipts')
            else:
                db.execute('ALTER TABLE kanban_notification_receipts ADD COLUMN incompatible TEXT')
    pool = AgentPool()
    original_ack = pool._ack_studio_notification
    transaction = adapter.source_transaction
    attempted, finished = threading.Event(), threading.Event()
    mutation = []
    threads = []
    def before_ack(claim, item, fence):
        if case.startswith('cursor-'):
            with sqlite3.connect(path) as db:
                db.execute('UPDATE kanban_notify_subs SET last_event_id=?', (int(case.split('-')[1]),))
        elif case in {'target-inode', 'receipt-digest', 'receipt-source'}:
            if case == 'target-inode':
                target.rename(home / 'saved-target.db')
                with sqlite3.connect(template) as original, sqlite3.connect(target) as copy:
                    original.backup(copy)
            else:
                with sqlite3.connect(target) as db:
                    db.execute('UPDATE kanban_notification_receipts SET ' + ('source_digest' if case == 'receipt-digest' else 'source_event') + "='changed'")
        elif case in {'transfer', 'release'}:
            admission = pool._session_admissions['session']
            def change_owner():
                attempted.set()
                if case == 'transfer':
                    mutation.append(transfer_active_session(admission.owner_lease, session_id='transferred'))
                else:
                    release_active_session(admission.owner_lease)
                    mutation.append(admission.owner_lease.released)
                finished.set()
            thread = threading.Thread(target=change_owner)
            threads.append(thread)
            thread.start()
            assert attempted.wait(2)
            assert not finished.wait(0.05), 'owner mutation crossed the live receive fence'
        return original_ack(claim, item, fence)
    pool._ack_studio_notification = before_ack
    @contextmanager
    def inside_ack(conn):
        with transaction(conn):
            if case == 'event-inside-ack':
                conn.execute("UPDATE task_events SET payload='changed' WHERE id=8")
            elif case == 'source-inode-inside-ack':
                path.rename(home / 'original-source.db')
                with sqlite3.connect(path) as replacement:
                    replacement.execute('CREATE TABLE replacement(x)')
            yield
    if case in {'event-inside-ack', 'source-inode-inside-ack'}:
        def ack_with_hook(claim, item, fence):
            adapter.source_transaction = inside_ack
            try:
                return original_ack(claim, item, fence)
            finally:
                adapter.source_transaction = transaction
        pool._ack_studio_notification = ack_with_hook
    if case in {'transfer', 'release'}:
        pool._sessions['session'] = pool._admit_session('session', 'default')
    notifications = pool._claim_kanban_notifications({'session'})
    for thread in threads:
        thread.join(2)
        assert not thread.is_alive() and finished.is_set()
    if threads:
        assert mutation == [True], (case, mutation)
        pool._release_session_admission('session')
    original_path = home / 'original-source.db' if case == 'source-inode-inside-ack' else path
    with sqlite3.connect(original_path) as db:
        cursor = db.execute('SELECT last_event_id FROM kanban_notify_subs').fetchone()[0]
    success = case in {'normal', 'transfer', 'release'}
    assert len(notifications) == int(success), (case, notifications)
    expected_cursor = 8 if success else int(case.split('-')[1]) if case.startswith('cursor-') else 3
    assert cursor == expected_cursor, (case, cursor)
    with sqlite3.connect(home / 'saved-target.db' if case == 'target-inode' else target) as db:
        count = db.execute("SELECT COUNT(*) FROM messages WHERE session_id='session'").fetchone()[0]
        stats = db.execute("SELECT message_count FROM sessions WHERE id='session'").fetchone()[0]
        assert count == stats == (0 if case.startswith('schema-') else 1), (case, count, stats)
    if case == 'event-inside-ack':
        pool._ack_studio_notification = original_ack
        assert len(pool._claim_kanban_notifications({'session'})) == 1
        with sqlite3.connect(path) as db:
            assert db.execute('SELECT last_event_id FROM kanban_notify_subs').fetchone()[0] == 8
        with sqlite3.connect(target) as db:
            assert db.execute('SELECT COUNT(*) FROM messages').fetchone()[0] == 1
    results.append(case)
print('Exact source ACK: normal CAS; current-tree controls: ' + ', '.join(results))
