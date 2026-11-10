import ast
import importlib.util
from pathlib import Path
import threading
import types

from notification_lease_fixture import *

METHODS.update({'estimate_context'})
# load_pool resolves METHODS in the imported module, which is the same set.
BRIDGE = args.source.resolve().parent
# The candidate may be a frozen standalone Python file; broker/auth stay bound
# to this checked-out source, not a parent path inferred from the payload.
REPO_BRIDGE = Path(__file__).resolve().parents[2] / 'packages/server/src/modules/hermes/services/bridge/python'
for name in ('bridge_model_run_auth', 'bridge_compatibility'):
    spec = importlib.util.spec_from_file_location(name, REPO_BRIDGE / (name + '.py'))
    assert spec is not None and spec.loader is not None
    imported = importlib.util.module_from_spec(spec)
    sys.modules[name] = imported
    spec.loader.exec_module(imported)
Auth = sys.modules['bridge_model_run_auth'].ModelRunAuth


def broker_class():
    tree = ast.parse((REPO_BRIDGE / 'bridge_broker.py').read_text())
    node = next(n for n in tree.body if isinstance(n, ast.ClassDef) and n.name == 'BridgeBroker')
    node.body = [n for n in node.body if isinstance(n, ast.FunctionDef) and n.name in {'handle', '_normalize_profile'}]
    scope = dict(globals(), ModelRunAuth=Auth)
    exec(compile(ast.fix_missing_locations(ast.Module(body=[ast.ImportFrom(module='__future__', names=[ast.alias(name='annotations')], level=0), node], type_ignores=[])), str(REPO_BRIDGE / 'bridge_broker.py'), 'exec'), scope)
    return scope['BridgeBroker']

class PreflightTests(NotificationLeaseTests):
    def setUp(self):
        super().setUp()
        self.binding = types.SimpleNamespace(run_marker='prepare-generation')
        self.pool._model_run_auth = self.binding
        def create(sid, *a, **kw):
            if kw.get('model_run_auth') != self.binding and (a[-1] if a else None) != self.binding:
                raise ValueError('binding mismatch')
            session = self.pool._sessions.get(sid) or self.pool._session_admissions[sid]
            session.agent = types.SimpleNamespace(_hermes_bridge_model_run_auth=self.binding)
            self.pool._sessions[sid] = session
            return session
        self.pool._get_or_create = create
        self.pool._estimate_context_info = lambda *a: {'token_count': 10}
        self.pool._install_usage_hook = lambda: None
        self.pool._install_boundary_interrupt = lambda session: None
        self.pool._bridge_context_ready_event = lambda *a: None
        self.pool._run_chat_body = lambda session, record, *a: self.run_body(session, record)
        self.entered, self.exit = threading.Event(), threading.Event()

    def run_body(self, session, record):
        self.entered.set()
        if not self.exit.wait(3):
            raise RuntimeError('fixture run timed out')
        session.running = False
        record.status = 'complete'
        self.pool._close_model_run_auth(self.binding)

    def tearDown(self):
        self.exit.set()
        for pool in (self.pool,):
            for record in pool._runs.values():
                if record.worker_thread:
                    record.worker_thread.join(3)
            with pool._lock:
                pool._release_idle_admission('session')

    def estimate(self):
        return self.pool.estimate_context('session', model_run_auth=self.binding)

    def maintenance(self):
        p = self.Pool()
        p._studio_notifications.verify = lambda *a: {}
        p._background_notification_status = lambda *a, **kw: {'current': True}
        return p

    def verify(self, maintenance):
        return maintenance.background_notification_status('delivery', 'claim',
            identity=['synthetic'], session_id='session')

    def test_prepare_second_verify_then_real_start_chat(self):
        maintenance = self.maintenance()
        self.assertTrue(self.verify(maintenance)['current'])
        self.assertEqual(self.estimate()['token_count'], 10)
        # Exact incident order: estimate on bound worker, status on maintenance worker.
        self.assertTrue(self.verify(maintenance)['current'])
        record = self.pool.start_chat('session', 'notification', model_run_auth=self.binding)
        self.assertTrue(self.entered.wait(2))
        self.assertIn('session', self.pool._session_admissions)
        with self.assertRaises(RuntimeError):
            self.verify(maintenance)
        self.exit.set()
        record.worker_thread.join(2)

    def test_competing_generation_wins_gap_then_chat_is_refused(self):
        self.estimate()
        contender = self.Pool()
        contender._admit_session('session', 'default')
        try:
            with self.assertRaises(RuntimeError):
                self.pool.start_chat('session', 'notification', model_run_auth=self.binding)
            self.assertFalse(self.entered.is_set(), 'no model dispatch after losing ownership')
            self.assertNotIn('session', self.pool._session_admissions)
        finally:
            contender._release_session_admission('session')

    def test_prepare_no_dispatch_does_not_reserve_session(self):
        self.estimate()
        self.assertNotIn('session', self.pool._session_admissions)
        self.next_owner()
        self.assertFalse(self.pool._model_run_chat_started)

    def test_estimate_exception_and_cancel_release(self):
        for error in (RuntimeError('estimate failed'), KeyboardInterrupt('cancelled')):
            with self.subTest(error=type(error).__name__):
                def fail(*a): raise error
                self.pool._estimate_context_info = fail
                with self.assertRaises(type(error)):
                    self.estimate()
                self.assertNotIn('session', self.pool._session_admissions)
                self.next_owner()

    def test_partial_creation_failure_releases(self):
        create = self.pool._get_or_create
        def fail(*a, **kw):
            create(*a, **kw)
            raise RuntimeError('failed after cache insertion')
        self.pool._get_or_create = fail
        with self.assertRaises(RuntimeError): self.estimate()
        self.assertNotIn('session', self.pool._session_admissions)
        self.next_owner()

    def test_actual_broker_routes_prepare_verify_dispatch_and_override_refusal(self):
        # Actual broker.handle -> substituted transport -> actual pool methods.
        # No IPC/server, DB notification verification and model body are synthetic.
        auth = Auth(1, 'fixture-owner', 'default', 'fixture-generation', str(self.root / 'token-source'))
        self.binding = auth
        self.pool._model_run_auth = auth
        maintenance = self.maintenance()
        routes = []
        B = broker_class()
        broker = B.__new__(B)
        broker._stop = threading.Event()
        broker._recovery = types.SimpleNamespace(check_dispatch=lambda req: None)
        def forward(profile, req, key):
            routes.append((req['action'], key))
            if key == 'default':
                return maintenance.background_notification_status('delivery', 'claim',
                    identity=req['notification_identity'], session_id=req['session_id'])
            self.assertEqual(key, auth.worker_key())
            if req['action'] == 'context_estimate':
                return self.pool.estimate_context(req['session_id'], model_run_auth=auth)
            return self.pool.start_chat(req['session_id'], 'notification', model_run_auth=auth)
        broker._forward = forward
        status = {'action':'background_notification_status','profile':'default','session_id':'session',
                  'notification_identity':['db','task',1,None,'default','tui','session','']}
        prepare = {'action':'context_estimate','profile':'default','session_id':'session','model_run_auth':auth.descriptor()}
        self.assertTrue(broker.handle(status)['current'])
        broker.handle(prepare)
        self.assertTrue(broker.handle(status)['current'])
        for changed in ({'model_run_auth':auth.descriptor()}, {'worker_key':auth.worker_key()},
                        {'profile':'other'}):
            with self.assertRaises(ValueError): broker.handle({**status, **changed})
        for changed in ({'worker_key':'foreign'}, {'profile':'other'}, {'model_run_auth':None}):
            with self.assertRaises(ValueError): broker.handle({**prepare, **changed})
        record = broker.handle({**prepare, 'action':'chat'})
        self.assertTrue(self.entered.wait(2))
        with self.assertRaises(RuntimeError): broker.handle(status)
        self.assertEqual(routes[:4], [('background_notification_status','default'),
            ('context_estimate',auth.worker_key()),('background_notification_status','default'),('chat',auth.worker_key())])
        self.exit.set(); record.worker_thread.join(2)

    def test_workspace_bind_and_clear_failure_release(self):
        scope = self.Pool.estimate_context.__globals__
        old_bind = scope['_bind_session_workspace_cwd']
        old_clear = scope.get('_clear_session_workspace_cwd')
        def fail(*a): raise RuntimeError('workspace failure')
        try:
            scope['_bind_session_workspace_cwd'] = fail
            with self.assertRaises(RuntimeError): self.estimate()
            self.next_owner()
            scope['_bind_session_workspace_cwd'] = lambda *a: True
            scope['_clear_session_workspace_cwd'] = fail
            with self.assertRaises(RuntimeError): self.estimate()
            self.next_owner()
        finally:
            scope['_bind_session_workspace_cwd'] = old_bind
            scope['_clear_session_workspace_cwd'] = old_clear

    def test_existing_reservation_is_not_released(self):
        admission = self.pool._admit_session('session', 'default')
        self.estimate()
        self.assertIs(admission, self.pool._session_admissions['session'])
        with self.assertRaises(RuntimeError): self.next_owner()

    def test_chat_started_reservation_is_not_released(self):
        self.pool._model_run_chat_started = True
        self.estimate()
        self.assertIn('session', self.pool._session_admissions)
        with self.assertRaises(RuntimeError): self.next_owner()

    def test_active_guards_during_new_estimate_admission(self):
        session = self.resident()
        for guard in ('running', 'background', 'recovery', 'thread'):
            with self.subTest(guard=guard):
                gate = threading.Event()
                thread = threading.Thread(target=lambda: gate.wait(3))
                session.running = guard == 'running'
                if guard == 'background': background.append({'status':'running', 'parent_session_id':'session'})
                if guard == 'recovery':
                    record = self.Record('r','session')
                    record.recovery_lifecycle = types.SimpleNamespace(observe=lambda: {'outcome':'unknown'})
                    self.pool._runs['r'] = record
                if guard == 'thread':
                    thread.start()
                    self.pool._runs['r'] = self.Record('r','session',worker_thread=thread)
                try:
                    self.estimate()
                    self.assertIn('session', self.pool._session_admissions)
                    with self.assertRaises(RuntimeError): self.next_owner()
                finally:
                    session.running = False
                    background.clear()
                    gate.set()
                    if thread.ident: thread.join(2)
                    self.pool._runs.clear()
                    self.pool._release_idle_admission('session')

    def test_estimate_holds_lease_and_serializes_same_worker_start(self):
        entered, release, attempted, finished = (threading.Event() for _ in range(4))
        failures = []
        def info(*a):
            entered.set()
            if not release.wait(3): raise RuntimeError('estimate gate timed out')
            return {'token_count':10}
        self.pool._estimate_context_info = info
        def estimate():
            try: self.estimate()
            except BaseException as ex: failures.append(repr(ex))
        estimator = threading.Thread(target=estimate)
        estimator.start()
        self.assertTrue(entered.wait(2))
        with self.assertRaises(RuntimeError): self.next_owner()
        def start():
            attempted.set()
            try: self.pool.start_chat('session','hello',model_run_auth=self.binding)
            except BaseException as ex: failures.append(repr(ex))
            finally: finished.set()
        starter = threading.Thread(target=start)
        starter.start()
        try:
            self.assertTrue(attempted.wait(2))
            self.assertFalse(finished.wait(.05), 'chat may not enter estimate cleanup window')
        finally:
            release.set()
            estimator.join(3); starter.join(3)
        self.assertFalse(estimator.is_alive()); self.assertFalse(starter.is_alive())
        self.assertEqual(failures, [])
        self.assertTrue(self.entered.wait(2))
        self.assertIn('session', self.pool._session_admissions)

if __name__ == '__main__':
    print(f'Preflight actual-method fixture source: {args.source}; owner: {args.owner_source}', flush=True)
    suite = unittest.defaultTestLoader.loadTestsFromTestCase(NotificationLeaseTests)
    for name in sorted(PreflightTests.__dict__):
        if name.startswith('test_'):
            suite.addTest(PreflightTests(name))
    outcome = unittest.TextTestRunner(verbosity=2).run(suite)
    sys.exit(not outcome.wasSuccessful())
