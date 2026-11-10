import argparse
import ast
from contextlib import contextmanager
from dataclasses import dataclass, field
import hashlib
import json
import os
from pathlib import Path
import queue
import sys
import tempfile
import threading
import time
import traceback
import types
import unittest
import uuid
from typing import Any, Callable


parser = argparse.ArgumentParser()
parser.add_argument('--source', type=Path, default=Path(__file__).resolve().parents[2] / 'packages/server/src/modules/hermes/services/bridge/python/bridge_pool.py')
parser.add_argument('--owner-source', type=Path)
args, remaining = parser.parse_known_args()


def module(name, **values):
    result = types.ModuleType(name)
    result.__dict__.update(values)
    sys.modules[name] = result
    return result


class SyntheticOwners:
    def __init__(self):
        self.entries = {}
        self.lock = threading.RLock()

    def acquire(self, *, session_id, metadata, registry_home, **kwargs):
        with self.lock:
            key = (str(registry_home), session_id)
            if key in self.entries:
                return None, 'SESSION_NOT_OWNED'
            lease = types.SimpleNamespace(key=key, live=metadata['live_session_id'], released=False)
            self.entries[key] = lease
            return lease, None

    def release(self, lease):
        with self.lock:
            if self.entries.get(lease.key) is lease:
                del self.entries[lease.key]
            lease.released = True

    @contextmanager
    def fence(self, *, lease, logical_session_id, live_session_id, profile_home, resolve_target):
        with self.lock:
            def check():
                if lease.released or self.entries.get(lease.key) is not lease or lease.live != live_session_id:
                    raise RuntimeError('owner changed')
            check()
            yield types.SimpleNamespace(check=check)


class MemoryStore:
    def __init__(self):
        self.path = Path(os.environ['HERMES_HOME']) / 'receiver'
        self.path.touch(exist_ok=True)
        self.receipts = {}
        self.order = []
        self.fail_accept = False

    def binding(self, sid):
        return 'synthetic-binding'

    def check(self):
        pass

    def accept(self, item, claim, fence):
        fence.check()
        if self.fail_accept:
            raise RuntimeError('synthetic receive failure')
        self.order.append('accept')
        self.receipts[item['delegation_id']] = {
            'source_event': canonical(claim['source_event']),
            'source_file': canonical(claim['db_identity']),
            'old_cursor': claim['old_cursor'], 'new_cursor': claim['new_cursor'],
        }

    def verify(self, delivery, identity, fence):
        fence.check()
        self.order.append('verify')
        return self.receipts[delivery]


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'))


@contextmanager
def transaction(conn):
    yield conn


module('bridge_studio_notifications', StudioNotificationStore=MemoryStore,
       canonical=canonical, source_transaction=transaction)
module('hermes_cli')
module('tools')
background = []
module('tools.async_delegation', list_async_delegations=lambda: background,
       claim_event_delivery=lambda *a: None, complete_completion_delivery=lambda *a: None)
module('tools.process_registry', process_registry=types.SimpleNamespace(completion_queue=queue.Queue()),
       format_process_notification=lambda event: '')
module('tools.mcp_tool_lifecycle', shutdown_mcp_servers=lambda: None)


METHODS = {
    '__init__', '_admit_session', '_release_session_admission', '_release_idle_admission',
    '_release_after_run_exit', '_notification_fence', '_close_model_run_auth',
    'get_or_create', 'start_chat', '_run_chat', '_run_chat_owned',
    'poll_background', '_poll_background', '_claim_kanban_notifications',
    '_claim_kanban_notifications_locked', '_ack_studio_notification',
    'background_notification_status', 'has_active_background_for_session',
}


def load_pool():
    tree = ast.parse(args.source.read_text())
    nodes = [ast.ImportFrom(module='__future__', names=[ast.alias(name='annotations')], level=0)]
    for node in tree.body:
        if isinstance(node, ast.Assign) and any(isinstance(n, ast.Name) and n.id in {'_KANBAN_AUDIT_KINDS', '_STUDIO_KANBAN_CANDIDATES'} for n in node.targets):
            nodes.append(node)
        if isinstance(node, ast.ClassDef) and node.name in {'AgentSession', 'RunRecord', 'SessionDbHolder'}:
            nodes.append(node)
        if isinstance(node, ast.ClassDef) and node.name == 'AgentPool':
            node.body = [n for n in node.body if not isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef)) or n.name in METHODS]
            nodes.append(node)
    scope = dict(globals(), ModelRunAuth=types.SimpleNamespace(parse=lambda value, *a: value),
                 _profile_home=lambda p: Path(os.environ['HERMES_HOME']),
                 _base_hermes_home=lambda: Path(os.environ['HERMES_HOME']),
                 _load_cfg=lambda: {}, _jsonable=lambda value: value,
                 _bind_session_workspace_cwd=lambda *a: False)
    exec(compile(ast.fix_missing_locations(ast.Module(body=nodes, type_ignores=[])), str(args.source), 'exec'), scope)
    return scope['AgentPool'], scope['AgentSession'], scope['RunRecord']


class NotificationLeaseTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.Pool, cls.Session, cls.Record = load_pool()

    def setUp(self):
        self.root = Path(tempfile.mkdtemp(prefix='notification-lease-', dir=os.environ['STUDIO_TEST_ROOT']))
        for key in ('HOME', 'HERMES_HOME', 'HERMES_AGENT_BRIDGE_BASE_HOME'):
            os.environ[key] = str(self.root)
        os.environ.pop('HERMES_WEB_UI_MODEL_RUN_AUTH', None)
        os.environ['HERMES_AGENT_BRIDGE_WORKER_PROFILE'] = 'default'
        background.clear()
        if args.owner_source:
            owner = module('hermes_cli.active_sessions')
            tree = ast.parse(args.owner_source.read_text())
            tree.body = [n for n in tree.body if not isinstance(n, ast.ImportFrom) or n.module not in {'hermes_constants', 'utils'}]
            def atomic_write(path, value, **kwargs):
                path.parent.mkdir(parents=True, exist_ok=True)
                temporary = path.with_suffix('.synthetic')
                temporary.write_text(json.dumps(value, **kwargs))
                temporary.replace(path)
            owner.__dict__.update(get_default_hermes_root=lambda: self.root,
                                  get_hermes_home=lambda: self.root,
                                  named_profile_is_live=lambda p: False, atomic_json_write=atomic_write)
            exec(compile(tree, str(args.owner_source), 'exec'), owner.__dict__)
            owner._own_start_time = lambda: 1.0
        else:
            owners = SyntheticOwners()
            owner = module('hermes_cli.active_sessions', try_acquire_active_session=owners.acquire,
                           release_active_session=owners.release, notification_owner_fence=owners.fence)
        self.owner = owner
        self.pool = self.Pool()
        self.pool._claim_kanban_notifications_locked = lambda ids, fence: []
        self.pool._background_notification_status = lambda *a, **kw: {'current': True}
        self.pool._studio_notifications.verify = lambda *a: {'synthetic': True}

    def resident(self, pool=None):
        pool = pool or self.pool
        session = self.Session('session', types.SimpleNamespace(_hermes_bridge_model_run_auth=None))
        pool._sessions['session'] = session
        return session

    def claim(self, pool=None):
        return (pool or self.pool)._claim_kanban_notifications({'session'})

    def status(self):
        return self.pool.background_notification_status('delivery', 'claim', identity=['synthetic'], session_id='session')

    def next_owner(self):
        next_pool = self.Pool()
        with next_pool._lock:
            next_pool._admit_session('session', 'default')
            next_pool._release_session_admission('session')

    def test_normal_terminal_watcher_poll_and_two_continuations(self):
        old_pools = []
        for generation in range(3):
            pool = self.Pool()
            binding = types.SimpleNamespace(run_marker=f'normal-{generation}')
            exited = threading.Event()
            allow_exit = threading.Event()
            entered = threading.Event()
            pool._model_run_auth = binding
            def get_session(sid, *a, **kw):
                session = pool._session_admissions[sid]
                session.agent = types.SimpleNamespace(_hermes_bridge_model_run_auth=binding)
                pool._sessions[sid] = session
                return session
            pool._get_or_create = get_session
            pool._install_usage_hook = lambda: None
            pool._install_boundary_interrupt = lambda session: None
            pool._bridge_context_ready_event = lambda *a: None
            pool._claim_kanban_notifications_locked = lambda ids, fence: []
            def body(session, record, *a):
                record.status = 'complete'
                record.result = {'message': 'synthetic normal result'}
                session.running = False
                pool._close_model_run_auth(binding)
                entered.set()
                if not allow_exit.wait(3):
                    raise RuntimeError('worker gate timed out')
            pool._run_chat_body = body
            watcher = pool._release_after_run_exit
            def watch(sid, thread):
                watcher(sid, thread)
                exited.set()
            pool._release_after_run_exit = watch
            record = pool.start_chat('session', 'hello', model_run_auth=binding)
            self.assertTrue(entered.wait(2))
            self.assertTrue(pool._model_run_auth_closed)
            self.assertIn('session', pool._session_admissions, 'live terminal worker must retain lease')
            allow_exit.set()
            self.assertTrue(exited.wait(2), 'actual exit watcher must finish its join/release')
            self.assertFalse(record.worker_thread.is_alive())
            self.assertFalse(record.finalizing)
            self.assertEqual(record.status, 'complete')
            self.assertNotIn('session', pool._session_admissions)
            self.assertIn('session', pool._sessions)
            old_pools.append(pool)
            for previous in old_pools:
                for _ in range(2):
                    self.assertEqual(previous.poll_background()['notifications'], [])
                    self.assertNotIn('session', previous._session_admissions,
                                     'closed resident empty poll retained owner lease')
                previous._studio_notifications.verify = lambda *a: {}
                previous._background_notification_status = lambda *a, **kw: {'current': True}
                self.assertTrue(previous.background_notification_status('d', 'c', identity=['synthetic'], session_id='session')['current'])
                self.assertNotIn('session', previous._session_admissions)
            self.next_owner()

    def test_idle_resident_status_and_exception_cleanup(self):
        self.resident()
        for operation in (self.claim, self.status):
            operation()
            self.assertNotIn('session', self.pool._session_admissions)
            self.next_owner()
        def fail(*a, **kw):
            raise RuntimeError('synthetic fence or receive failure')
        for failing_seam in ('_notification_fence', '_claim_kanban_notifications_locked'):
            original = getattr(self.pool, failing_seam)
            setattr(self.pool, failing_seam, fail)
            self.claim()
            self.assertNotIn('session', self.pool._session_admissions)
            setattr(self.pool, failing_seam, original)
        self.pool._studio_notifications.verify = fail
        with self.assertRaises(RuntimeError):
            self.status()
        self.next_owner()

    def test_existing_nonresident_admission_is_not_temporary(self):
        admission = self.pool._admit_session('session', 'default')
        self.claim()
        self.status()
        self.assertIs(self.pool._session_admissions.get('session'), admission,
                      'notification cleanup released an existing start reservation')
        self.pool._release_session_admission('session')

    def test_activity_guards_and_competing_writer(self):
        session = self.resident()
        self.pool._model_run_auth_closed = True
        gate = threading.Event()
        thread = threading.Thread(target=lambda: gate.wait(3))
        thread.start()
        try:
            guards = ('running', 'thread', 'background', 'recovery')
            for guard in guards:
                with self.subTest(guard=guard):
                    session.running = guard == 'running'
                    self.pool._runs.clear()
                    if guard == 'thread':
                        self.pool._runs['r'] = self.Record('r', 'session', worker_thread=thread)
                    if guard == 'background':
                        background.append({'status': 'finalizing', 'origin_ui_session_id': 'session'})
                    if guard == 'recovery':
                        record = self.Record('r', 'session')
                        record.recovery_lifecycle = types.SimpleNamespace(observe=lambda: {'outcome': 'unknown'})
                        self.pool._runs['r'] = record
                    self.claim()
                    admission = self.pool._session_admissions['session']
                    self.status()
                    self.assertIs(self.pool._session_admissions.get('session'), admission)
                    contender = self.Pool()
                    contender._claim_kanban_notifications_locked = lambda *a: self.fail('competing writer crossed fence')
                    self.assertEqual(self.claim(contender), [])
                    self.assertNotIn('session', contender._session_admissions)
                    with self.assertRaises(RuntimeError):
                        contender._admit_session('session', 'default')
                    session.running = False
                    background.clear()
                    if guard == 'recovery':
                        record.recovery_lifecycle.observe = lambda: {'outcome': 'exited_proven'}
                    elif guard == 'thread':
                        gate.set()
                        thread.join(2)
                        self.assertFalse(thread.is_alive())
                    else:
                        self.pool._runs.clear()
                    if guard in {'running', 'background'}:
                        self.status()
                    else:
                        self.claim()
                    self.assertNotIn('session', self.pool._session_admissions,
                                     'closed generation retained a previously guarded idle lease')
                    self.next_owner()
        finally:
            gate.set()
            thread.join(2)

    def test_cleanup_serialized_with_concurrent_start_admission(self):
        self.resident()
        for operation in (self.claim, self.status):
            cleanup_entered, start_attempted, start_admitted = (threading.Event() for _ in range(3))
            original = self.pool._release_idle_admission
            errors = []
            def release(sid):
                cleanup_entered.set()
                if not start_attempted.wait(2):
                    errors.append('start not attempted')
                if start_admitted.wait(0.05):
                    errors.append('start entered before notification cleanup finished')
                original(sid)
            self.pool._release_idle_admission = release
            def start():
                if not cleanup_entered.wait(2):
                    errors.append('cleanup did not run')
                    return
                start_attempted.set()
                with self.pool._lock:
                    self.pool._admit_session('session', 'default')
                    self.pool._sessions['session'].running = True
                    start_admitted.set()
            thread = threading.Thread(target=start)
            thread.start()
            operation()
            thread.join(3)
            self.assertFalse(thread.is_alive())
            self.assertEqual(errors, [])
            self.assertTrue(start_admitted.is_set())
            admission = self.pool._session_admissions['session']
            self.status()
            self.assertIs(self.pool._session_admissions.get('session'), admission)
            self.assertFalse(admission.owner_lease.released)
            self.pool._sessions['session'].running = False
            original('session')
            self.pool._release_idle_admission = original

    def test_owner_fence_blocks_release_until_receive_finishes(self):
        admission = self.pool._admit_session('session', 'default')
        attempted, finished = threading.Event(), threading.Event()
        def release():
            attempted.set()
            self.owner.release_active_session(admission.owner_lease)
            finished.set()
        with self.pool._notification_fence('session', admission) as fence:
            thread = threading.Thread(target=release)
            thread.start()
            self.assertTrue(attempted.wait(2))
            self.assertFalse(finished.wait(0.05), 'owner mutation crossed live notification fence')
            fence.check()
        thread.join(2)
        self.assertFalse(thread.is_alive())
        self.assertTrue(finished.is_set())
        self.pool._session_admissions.clear()

    def test_actual_claim_fence_receive_before_ack_and_failure(self):
        self.resident()
        self.pool._claim_kanban_notifications_locked = types.MethodType(self.Pool._claim_kanban_notifications_locked, self.pool)
        self.pool._studio_notifications = MemoryStore()
        store = self.pool._studio_notifications
        source = self.root / 'synthetic-board'
        source.touch()
        event = {'id': 8, 'task_id': 'task', 'kind': 'completed', 'payload': '{}', 'run_id': None}
        sub = {'task_id': 'task', 'platform': 'tui', 'chat_id': 'session', 'thread_id': '', 'last_event_id': 3,
               'notifier_profile': 'default', 'delivery_metadata': {}, 'delivery_mode': 'notify'}
        task = types.SimpleNamespace(notification_policy='direct', status='done')
        cursor = [3]
        class Connection:
            def execute(self, sql, *a):
                return self
            def fetchone(self):
                return dict(event)
            def close(self):
                pass
        self.pool._open_kanban_claim_db = lambda claim: Connection()
        self.pool._format_kanban_notification = lambda *a: 'synthetic completion'
        module('hermes_cli.kanban_db', DEFAULT_BOARD='default',
               list_boards=lambda **kw: [{'slug': 'default', 'db_path': str(source)}], get_task=lambda *a: task)
        module('hermes_cli.kanban_db_connect')
        def cas(conn, key, new, old):
            self.assertEqual(store.order[-2:], ['accept', 'verify'])
            self.assertEqual(cursor[0], old)
            cursor[0] = new
            store.order.append('ack')
        module('hermes_cli.kanban_db_notify', list_notify_subs=lambda conn: [sub],
               unseen_events_for_sub=lambda *a, **kw: (None, [types.SimpleNamespace(**event)]),
               _sub_key=lambda *a: a, _notify_cursor=lambda *a: cursor[0], _cas_cursor=cas)
        module('hermes_cli.kanban_db_notification_delivery', _binding=lambda *a, **kw: True)
        module('hermes_cli.kanban_stop', execution_stopped=lambda *a: False)
        self.assertEqual(len(self.claim()), 1)
        self.assertEqual(cursor[0], 8)
        self.assertEqual(store.order, ['accept', 'verify', 'ack'])
        self.assertNotIn('session', self.pool._session_admissions)
        cursor[0] = 3
        store.order.clear()
        store.fail_accept = True
        self.assertEqual(self.claim(), [])
        self.assertEqual(cursor[0], 3)
        self.assertEqual(store.order, [])
        self.assertNotIn('session', self.pool._session_admissions)
        admission = self.pool._admit_session('session', 'default')
        self.owner.release_active_session(admission.owner_lease)
        store.fail_accept = False
        self.assertEqual(self.claim(), [], 'released lease must not enter receive/ACK')
        self.assertEqual(cursor[0], 3)
        self.pool._session_admissions.clear()


if __name__ == '__main__':
    if not os.environ.get('STUDIO_TEST_ROOT'):
        raise SystemExit('STUDIO_TEST_ROOT must name an existing synthetic test directory')
    print(f'Bridge source: {args.source.resolve()}; owner source: {args.owner_source or "synthetic portable fixture"}', flush=True)
    unittest.main(argv=[sys.argv[0], *remaining], verbosity=2)
