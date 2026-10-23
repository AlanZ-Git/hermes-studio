import { execFileSync } from 'node:child_process'
import { it } from 'vitest'

it('retires idle workers only after positive worker drain and protects in-flight admission', () => {
  execFileSync(process.platform === 'win32' ? 'python' : 'python3', ['-c', String.raw`
import sys, types
from pathlib import Path
sys.path.insert(0, str(Path('packages/server/src/modules/hermes/services/bridge/python').resolve()))
import bridge_transport as transport
from bridge_broker import BridgeBroker
clock = [4000]
transport.time.monotonic = lambda: clock[0]
worker = transport.WorkerProcess('w', 'default', 'unused', None, None)
worker.process = types.SimpleNamespace(poll=lambda: None, pid=42)
worker.start = lambda: None
worker.last_used_at = 0
response = {}
requests = []
def send(endpoint, payload, timeout):
    requests.append(payload)
    return response
transport._send_bridge_request = send
broker = BridgeBroker('unused')
broker._last_gc = 0
broker._workers['w'] = worker
stopped = []
worker.stop = lambda: stopped.append(True)
for action in ('status', 'background_poll', 'get_result', 'get_output', 'list'):
    worker.request({'action': action})
    assert worker.last_used_at == 0
# The broker owns admission, while the worker atomically knows active runs,
# finalizers and background claims. A missing/negative drain ACK is not idle.
broker._inflight['w'] = 1
broker._gc_idle_workers()
assert not stopped and not any(r['action'] == 'prepare_retire' for r in requests)
broker._inflight.clear()
for response in ({}, {'retired': False, 'running_count': 1},
                 {'retired': False, 'pending_count': 1}):
    broker._last_gc = 0
    broker._gc_idle_workers()
    assert not stopped and broker._workers['w'] is worker
response = {'retired': True}
broker._last_gc = 0
broker._gc_idle_workers()
assert stopped == [True] and 'w' not in broker._workers
assert requests[-1]['action'] == 'prepare_retire'
`], { cwd: process.cwd(), encoding: 'utf8', timeout: 10_000 })
})
