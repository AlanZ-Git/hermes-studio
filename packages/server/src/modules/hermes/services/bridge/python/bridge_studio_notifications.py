from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import time
from pathlib import Path
from contextlib import contextmanager


@contextmanager
def source_transaction(conn):
    conn.execute('PRAGMA busy_timeout=200')
    conn.execute('BEGIN IMMEDIATE')
    try:
        yield
        conn.commit()
    except BaseException:
        conn.rollback()
        raise


def canonical(value):
    return json.dumps(value, ensure_ascii=False, separators=(',', ':'), sort_keys=True)


class StudioNotificationStore:
    def __init__(self):
        raw = os.environ.get('HERMES_STUDIO_NOTIFICATION_DB')
        self.path = Path(raw).resolve(strict=True) if raw else None
        self.file_id = self._file_id() if self.path else None

    def _file_id(self):
        stat = self.path.stat()
        return [stat.st_dev, stat.st_ino]

    def configuration(self):
        if self.path is None:
            return None
        self.check()
        return [str(self.path), *self.file_id]

    def check(self):
        if self.path is None or self._file_id() != self.file_id:
            raise RuntimeError('Studio receiving database unavailable or replaced')

    def binding(self, session_id):
        self.check()
        return canonical([str(self.path), 'studio-sqlite-v1:' + ':'.join(map(str, self.file_id)), session_id])

    def open(self):
        self.check()
        conn = sqlite3.connect(self.path.as_uri() + '?mode=rw', uri=True, timeout=0.2)
        conn.row_factory = sqlite3.Row
        conn.execute('PRAGMA foreign_keys=ON')
        try:
            self.check()
            expected = {'delivery_id': 'TEXT', 'session_id': 'TEXT', 'profile': 'TEXT',
                'identity': 'TEXT', 'notification': 'TEXT', 'message_id': 'INTEGER',
                'consumer_state': 'TEXT', 'run_marker': 'TEXT', 'error': 'TEXT',
                'reference_state': 'TEXT', 'reference_error': 'TEXT', 'target_binding': 'TEXT',
                'source_digest': 'TEXT', 'source_event': 'TEXT', 'source_file': 'TEXT',
                'old_cursor': 'INTEGER', 'new_cursor': 'INTEGER'}
            columns = {r['name']: r for r in conn.execute('PRAGMA table_info(kanban_notification_receipts)')}
            required = {'session_id', 'profile', 'identity', 'notification', 'message_id', 'consumer_state', 'reference_state'}
            if (set(columns) != set(expected) or any(columns[k]['type'].upper() != v for k, v in expected.items())
                    or columns['delivery_id']['pk'] != 1
                    or any(columns[k]['notnull'] != int(k in required) or columns[k]['pk'] != int(k == 'delivery_id') for k in columns)
                    or columns['reference_state']['dflt_value'] != "'unchecked'"):
                raise RuntimeError('Incompatible Studio Kanban receipt schema')
            if not any(r['table'] == 'sessions' and r['from'] == 'session_id' and r['to'] == 'id' and r['on_delete'] == 'CASCADE'
                       for r in conn.execute('PRAGMA foreign_key_list(kanban_notification_receipts)')):
                raise RuntimeError('Incompatible Studio Kanban receipt binding')
            for table, required in {'sessions': {'id', 'profile', 'message_count', 'last_active', 'started_at'},
                                    'messages': {'id', 'session_id', 'role', 'content', 'run_marker', 'finish_reason', 'timestamp', 'display_role'}}.items():
                if not required <= {r['name'] for r in conn.execute('PRAGMA table_info(' + table + ')')}:
                    raise RuntimeError('Incompatible Studio message schema')
            return conn
        except BaseException:
            conn.close()
            raise

    def accept(self, notification, claim, fence):
        fence.check()
        conn = self.open()
        try:
            conn.execute('BEGIN IMMEDIATE')
            sid = notification['session_id']
            session = conn.execute('SELECT profile FROM sessions WHERE id=?', (sid,)).fetchone()
            if not session or (session['profile'] or 'default') != notification['profile']:
                raise RuntimeError('Studio notification target changed')
            binding = self.binding(sid)
            event_json = canonical(claim['source_event'])
            digest = hashlib.sha256(event_json.encode()).hexdigest()
            expected = {'session_id': sid, 'profile': notification['profile'],
                'identity': canonical(notification['identity']), 'target_binding': binding,
                'source_digest': digest, 'source_event': event_json,
                'source_file': canonical(claim['db_identity']), 'old_cursor': claim['old_cursor'], 'new_cursor': claim['new_cursor']}
            previous = conn.execute('SELECT * FROM kanban_notification_receipts WHERE delivery_id=?', (notification['delegation_id'],)).fetchone()
            if previous:
                if any(previous[k] != v for k, v in expected.items()) or json.loads(previous['notification'])['event'] != notification['event']:
                    raise RuntimeError('Studio notification receipt conflict')
            else:
                wake = notification.get('delivery_mode') in {'wake', 'notify+wake'} and not notification.get('execution_stopped') and (
                    notification.get('studio_auto_continue') != 'v1' or notification.get('wake_eligible') is True)
                message_id = conn.execute("INSERT INTO messages (session_id,role,content,run_marker,finish_reason,timestamp) VALUES (?,'assistant',?,?,'notification',?)",
                    (sid, notification['message'], notification['delegation_id'], int(time.time()))).lastrowid
                values = {'delivery_id': notification['delegation_id'], **expected, 'notification': canonical(notification),
                          'message_id': message_id, 'consumer_state': 'pending' if wake else 'consumed', 'reference_state': 'visible'}
                conn.execute('INSERT INTO kanban_notification_receipts (' + ','.join(values) + ') VALUES (' + ','.join('?' for _ in values) + ')', tuple(values.values()))
                conn.execute('UPDATE sessions SET message_count=(SELECT COUNT(*) FROM messages WHERE session_id=?), last_active=COALESCE((SELECT MAX(timestamp) FROM messages WHERE session_id=?),started_at) WHERE id=?', (sid, sid, sid))
            self.check()
            fence.check()
            conn.commit()
        except BaseException:
            conn.rollback()
            raise
        finally:
            conn.close()

    def verify(self, delivery_id, identity, fence):
        fence.check()
        conn = self.open()
        try:
            row = conn.execute('SELECT * FROM kanban_notification_receipts WHERE delivery_id=?', (delivery_id,)).fetchone()
            if (not row or json.loads(row['identity']) != identity
                    or row['target_binding'] != self.binding(identity[6])
                    or not row['source_event'] or not row['source_file']
                    or row['source_digest'] != hashlib.sha256(row['source_event'].encode()).hexdigest()
                    or row['new_cursor'] != identity[2] or row['old_cursor'] is None):
                raise RuntimeError('Studio notification receipt provenance unavailable')
            return dict(row)
        finally:
            conn.close()
