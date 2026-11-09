from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace
import os
import sqlite3
import tempfile
import threading

assert os.environ.get('STUDIO_TEST_ONLY') == '1', 'test-only marker required'
root = Path(os.environ['STUDIO_TEST_ROOT'])
assert root.is_absolute() and root == root.resolve(strict=True) and root != Path(root.anchor)
os.environ['HOME'] = str(root / 'home')
os.environ['TMPDIR'] = str(root)
root = Path(tempfile.mkdtemp(dir=root, prefix='recovery-resources-'))
os.environ['HERMES_HOME'] = str(root)
os.environ['HERMES_AGENT_BRIDGE_BASE_HOME'] = str(root)

from agent.recovery_lifecycle import RecoveryLifecycle, request_thread
from bridge_loops import LoopStore
from bridge_model_run_auth import ModelRunAuth
from bridge_pool import AgentPool
from bridge_recovery_coordinator import RecoveryCoordinator
broker = SimpleNamespace(hermes_home=str(root), _lock=threading.RLock(), _workers={})
@contextmanager
def admit(profile, key):
    yield broker._workers.get(key)
broker._admit = admit
coordinator = RecoveryCoordinator(broker)

def pending(profile, sid):
    home = root if profile == 'default' else root / 'profiles' / profile
    home.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(home / 'state.db') as db:
        db.execute('CREATE TABLE IF NOT EXISTS state_meta(key TEXT PRIMARY KEY,value TEXT)')
    store = LoopStore(home)
    store.command(sid, '30s inspect --times 3', profile)
    with store.transaction() as db:
        state, meta = store.read(db, sid)
        state.status = 'paused'
        meta['recovery'] = {'id': sid, 'generation': meta['generation'], 'status': 'pending', 'attempts': 0}
        store.save(db, sid, state, meta)
    return {'profile': profile, 'session_id': sid, 'recovery_id': sid, 'expected_generation': meta['generation']}

a = pending('default', 'a')
b = pending('alpha', 'b')
c = pending('beta', 'c')
def claim(req):
    result = coordinator.operate({**req, 'op': 'recovery_claim'})
    if result.get('authorized'):
        req['reservation_id'] = result['reservation_id']
    return result
assert claim(a)['authorized'] and claim(b)['authorized']
assert claim(c) == {'authorized': False, 'reason': 'recovery_capacity_held'}
assert len(coordinator.all()) == 2
second = RecoveryCoordinator(broker)
try:
    second.all()
except RuntimeError:
    pass
else:
    raise AssertionError('second broker must not acquire independent capacity')
assert coordinator.control(a, cancel=True)['outcome'] == 'not_dispatched_proven'
assert claim(c)['authorized']

auth = ModelRunAuth(1, 'fixture-owner', 'alpha', 'recovery_v1_original', str(root / 'synthetic-token-reference'))
assert coordinator.operate({**b, 'op': 'recovery_bind', 'recovery_auth': auth.descriptor()})['authorized']
assert coordinator.operate({**b, 'op': 'recovery_inject'})['authorized']
coordinator.check_dispatch({'session_id': 'b', 'profile': 'alpha', 'model_run_auth': auth.descriptor()})
assert coordinator.control(b)['outcome'] == 'unknown'
assert len(coordinator.all()) == 2

ready, release = threading.Event(), threading.Event()
request_threads = []
tracker = None
def owner():
    with tracker.scope():
        thread = request_thread(object(), lambda: (ready.set(), release.wait(10)), daemon=True)
        request_threads.append(thread)
        thread.start()
    tracker.finish_owner(True)
thread = threading.Thread(target=owner)
tracker = RecoveryLifecycle(thread)
thread.start()
assert ready.wait(10)
thread.join(10)
assert not thread.is_alive() and tracker.observe()['outcome'] == 'running'
pool = AgentPool()
pool._lock = threading.RLock()
pool._recovery_runs = {'original-run': ('b', auth, tracker)}
calls = []
class Worker:
    model_run_auth = auth
    running = True
    def request(self, req, timeout=None, *, allow_start=True):
        assert allow_start is False
        assert req['model_run_auth'] == auth.descriptor()
        assert req['session_id'] == 'b'
        calls.append(req)
        if req['action'] == 'observe_recovery_exit':
            return pool.observe_recovery_exit(req['session_id'], req.get('expected_run_id') or '', req['model_run_auth'])
        assert req['action'] == 'request_boundary_interrupt' and req['expected_run_id'] == 'original-run'
        return {'accepted': True}
broker._workers[auth.worker_key()] = Worker()
assert coordinator.control(b, cancel=True)['outcome'] == 'running'
assert calls[-1]['expected_run_id'] == 'original-run'
assert len(coordinator.all()) == 2

store = coordinator.store('alpha')
with store.transaction() as db:
    state, meta = store.read(db, 'b')
    original = meta.pop('recovery')
    meta.setdefault('recovery_history', []).append(original)
    meta['generation'] += 1
    state.status = 'done'
    store.save(db, 'b', state, meta)
release.set()
for request in request_threads:
    request.join(10)
assert tracker.observe()['outcome'] == 'exited_proven'
assert coordinator.control(b)['outcome'] == 'exited_proven'
with store.transaction() as db:
    state, meta = store.read(db, 'b')
    assert state.status == 'done' and meta['generation'] == b['expected_generation']+1
assert len(coordinator.all()) == 1
coordinator.close()
restarted = RecoveryCoordinator(broker)
assert len(restarted.all()) == 1
assert restarted.control(c, cancel=True)['outcome'] == 'not_dispatched_proven'
assert restarted.all() == []
restarted.close()

coordinator = RecoveryCoordinator(broker)
d = pending('default', 'durable-exit')
assert claim(d)['authorized']
auth = ModelRunAuth(1, 'fixture-owner', 'default', 'recovery_v1_durable', str(root / 'private-reference'))
assert coordinator.operate({**d, 'op': 'recovery_bind', 'recovery_auth': auth.descriptor()})['authorized']
assert coordinator.operate({**d, 'op': 'recovery_inject'})['authorized']
coordinator.check_dispatch({'session_id': d['session_id'], 'profile': 'default', 'model_run_auth': auth.descriptor()})
try:
    coordinator.check_dispatch({'session_id': d['session_id'], 'profile': 'default', 'model_run_auth': auth.descriptor()})
except ValueError:
    pass
else:
    raise AssertionError('one reservation must not authorize two dispatches')
from bridge_recovery_resources import persist_owner_exit
store = coordinator.store('default')
wrong = ModelRunAuth(1, 'other-owner', 'default', auth.run_marker, auth.token_path)
assert not persist_owner_exit(store, d['session_id'], wrong, 'durable-run', {'outcome': 'exited_proven'})
ready, release = threading.Event(), threading.Event()
request_threads = []
thread = threading.Thread(target=owner)
tracker = RecoveryLifecycle(thread)
thread.start()
assert ready.wait(10)
thread.join(10)
old_home = os.environ['HERMES_HOME']
os.environ['HERMES_HOME'] = str(root)
publisher = threading.Thread(target=pool._persist_recovery_exit,
    args=(d['session_id'], 'durable-run', auth, tracker), daemon=True)
try:
    publisher.start()
    assert len(coordinator.all()) == 1
    coordinator.close()
    release.set()
    for request in request_threads:
        request.join(10)
    publisher.join(10)
    assert not publisher.is_alive() and tracker.exit_persisted is True
finally:
    release.set()
    os.environ['HERMES_HOME'] = old_home
restarted = RecoveryCoordinator(broker)
assert restarted.all() == []
with store.transaction() as db:
    state, meta = store.read(db, d['session_id'])
    assert meta['recovery']['resource']['run_id'] == 'durable-run'
    assert meta['recovery']['resource']['outcome'] == 'exited_proven'
restarted.close()

bad_root = root / 'bad-enumeration'
bad_root.mkdir()
(bad_root / 'profiles').write_text('not a profile directory')
bad = RecoveryCoordinator(SimpleNamespace(hermes_home=str(bad_root)))
try:
    bad.all()
except RuntimeError:
    pass
else:
    raise AssertionError('incomplete profile inventory must fail closed')
finally:
    bad.close()
from bridge_broker import BridgeBroker
from bridge_server import BridgeServer
from bridge_transport import WorkerProcess
import json
import socketserver
broker = BridgeBroker('tcp://127.0.0.1:1', hermes_home=str(root))
coordinator = broker._recovery
e = pending('default', 'transport')
assert claim(e)['authorized']
auth = ModelRunAuth(1, 'fixture-owner', 'default', 'recovery_v1_transport', str(root / 'transport-reference'))
assert coordinator.operate({**e, 'op': 'recovery_bind', 'recovery_auth': auth.descriptor()})['authorized']
assert coordinator.operate({**e, 'op': 'recovery_inject'})['authorized']
coordinator.check_dispatch({'session_id': e['session_id'], 'profile': 'default', 'model_run_auth': auth.descriptor()})
ready, release = threading.Event(), threading.Event()
request_threads = []
thread = threading.Thread(target=owner)
tracker = RecoveryLifecycle(thread)
thread.start()
assert ready.wait(10)
thread.join(10)
pool._recovery_runs = {'old-run': (e['session_id'], auth, tracker)}
pool._model_run_auth = auth
pool._retired = False
session = SimpleNamespace(lock=threading.RLock(), boundary_supported=True, current_run_id='new-business-run',
    running=True, boundary_pending_run_id=None, boundary_reached_run_id=None)
pool._sessions = {e['session_id']: session}
server = object.__new__(BridgeServer)
server.pool = pool
server._admission_lock = threading.RLock()
wire_calls = []
class RPC(socketserver.StreamRequestHandler):
    def handle(self):
        req = json.loads(self.rfile.readline())
        wire_calls.append(req)
        try:
            result = {'ok': True, **server.handle(req)}
        except Exception as exc:
            result = {'ok': False, 'error': type(exc).__name__}
        self.wfile.write((json.dumps(result) + '\n').encode())
wire = socketserver.ThreadingTCPServer(('127.0.0.1', 0), RPC)
wire_thread = threading.Thread(target=wire.serve_forever, daemon=True)
wire_thread.start()
worker = WorkerProcess(auth.worker_key(), 'default', f'tcp://127.0.0.1:{wire.server_address[1]}', None, str(root))
worker.bind_model_run_auth(auth)
worker.process = SimpleNamespace(poll=lambda: None)
worker.start = lambda: (_ for _ in ()).throw(AssertionError('cleanup must never start a worker'))
broker._workers[auth.worker_key()] = worker
broker._session_worker_key[e['session_id']] = 'new-route'
broker._session_profile[e['session_id']] = 'default'
try:
    result = broker.handle({**e, 'action': 'abort_recovery'})
    assert result['outcome'] == 'running'
    assert wire_calls[-1]['action'] == 'request_boundary_interrupt'
    assert wire_calls[-1]['expected_run_id'] == 'old-run'
    assert session.boundary_pending_run_id is None and session.current_run_id == 'new-business-run'
    assert broker._session_worker_key[e['session_id']] == 'new-route'
    before = len(wire_calls)
    try:
        broker.handle({**e, 'action': 'abort_recovery', 'model_run_auth': wrong.descriptor()})
    except ValueError:
        pass
    else:
        raise AssertionError('mismatched cleanup auth must be refused before transport')
    assert len(wire_calls) == before
    snapshot = broker.handle({'action': 'recovery_quiesce'})
    assert len(snapshot['resources']) == 1
    f = pending('beta', 'after-quiesce')
    assert claim(f) == {'authorized': False, 'reason': 'recovery_admission_closed'}
    release.set()
    for request in request_threads:
        request.join(10)
    assert broker.handle({**e, 'action': 'observe_recovery_exit'})['outcome'] == 'exited_proven'
    assert coordinator.all() == []
    del broker._workers[auth.worker_key()]
    worker.process = None
    try:
        worker.request({'action': 'observe_recovery_exit'}, allow_start=False)
    except RuntimeError:
        pass
    else:
        raise AssertionError('missing original worker must never restart')
finally:
    release.set()
    wire.shutdown()
    wire.server_close()
    wire_thread.join(10)
    coordinator.close()
from bridge_pool import AgentSession
from hermes_cli.active_sessions import try_acquire_active_session, release_active_session
from unittest.mock import patch
import time


def wait_until(predicate):
    deadline = time.monotonic() + 5
    while not predicate():
        assert time.monotonic() < deadline, 'owned lifecycle did not settle'
        threading.Event().wait(0.01)


with patch.dict(os.environ, {'HERMES_HOME': str(root), 'HERMES_AGENT_BRIDGE_BASE_HOME': str(root)}), \
        patch('bridge_pool._load_cfg', return_value={}), \
        patch('bridge_pool._refresh_worker_profile_env'), \
        patch('bridge_pool._bind_session_workspace_cwd', return_value=False):
    for recovery in (False, True):
        sid = 'lease-recovery' if recovery else 'lease-ordinary'
        binding = ModelRunAuth(1, 'fixture-owner', 'default',
            'recovery_v1_lease' if recovery else 'ordinary-lease', str(root / 'lease-reference'))
        admitted = AgentPool()
        admitted._model_run_auth = binding
        synthetic_agent = SimpleNamespace(_hermes_bridge_model_run_auth=binding)
        owned = AgentSession(session_id=sid, agent=synthetic_agent, config={'profile': 'default'})
        admitted._sessions[sid] = owned
        admitted._install_usage_hook = lambda: None
        admitted._install_boundary_interrupt = lambda session: None
        admitted._bridge_context_ready_event = lambda *args: None
        entered, leave = threading.Event(), threading.Event()
        children = []
        reservation = pending('default', sid) if recovery else None
        if recovery:
            coordinator = RecoveryCoordinator(broker)
            assert claim(reservation)['authorized']
            assert coordinator.operate({**reservation, 'op': 'recovery_bind', 'recovery_auth': binding.descriptor()})['authorized']
            assert coordinator.operate({**reservation, 'op': 'recovery_inject'})['authorized']
            coordinator.check_dispatch({'session_id': sid, 'profile': 'default', 'model_run_auth': binding.descriptor()})

        def synthetic_body(session, record, *args, **kwargs):
            if recovery:
                child = request_thread(object(), lambda: (entered.set(), leave.wait(10)), daemon=True)
                children.append(child)
                child.start()
            else:
                entered.set()
                assert leave.wait(10)
            with session.lock:
                record.status = 'complete'
                record.result = {'completed': True}
                session.running = False
                session.current_run_id = None

        admitted._run_chat_body = synthetic_body
        try:
            record = admitted.start_chat(sid, 'synthetic local request', profile='default', model_run_auth=binding.descriptor())
            assert entered.wait(5)
            assert owned.owner_lease is not None
            assert admitted.get_or_create(sid, profile='default', model_run_auth=binding.descriptor()) is owned
            contender = AgentPool()
            try:
                contender._admit_session(sid, 'default')
            except RuntimeError:
                pass
            else:
                raise AssertionError('another live session stole the ordinary/recovery owner lease')
            with admitted._lock:
                admitted._release_idle_admission(sid)
            assert sid in admitted._session_admissions
            if recovery:
                record.worker_thread.join(5)
                assert not record.worker_thread.is_alive()
                assert admitted.observe_recovery_exit(sid, record.run_id, binding.descriptor())['outcome'] == 'running'
                assert sid in admitted._session_admissions
            leave.set()
            record.worker_thread.join(5)
            assert not record.worker_thread.is_alive()
            for child in children:
                child.join(5)
                assert not child.is_alive()
            wait_until(lambda: sid not in admitted._session_admissions)
            lease, refusal = try_acquire_active_session(session_id=sid, surface='studio', config={},
                metadata={'live_session_id': 'next-live-session'}, registry_home=root)
            assert lease is not None and not refusal, (sid, refusal)
            release_active_session(lease)
            if recovery:
                wait_until(lambda: record.recovery_lifecycle.exit_persisted)
                assert coordinator.all() == []
                coordinator.close()
        finally:
            leave.set()
            for child in children:
                child.join(5)
            for thread in admitted._run_threads:
                thread.join(5)
            with admitted._lock:
                admitted._release_idle_admission(sid)

print('W6 owner lease admits ordinary/recovery chat, excludes competing live sessions, retains provider-thread ownership and releases only after owned exit')
print('W6 cap2/global-profile/unique-owner/unknown/late-start/original-cancel/thread-leftover/history/restart/durable-owner-proof/duplicate-dispatch/actual-transport/new-run-isolation/quiesce controls passed')
