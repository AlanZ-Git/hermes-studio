import { execFileSync } from 'node:child_process'
import { describe, it } from 'vitest'

describe('bridge immutable model run routing', () => {
  it('requires a complete binding and rejects mismatched worker identities', () => {
    execFileSync('python3', ['-c', String.raw`
import sys
from pathlib import Path
sys.path.insert(0, str(Path('packages/server/src/modules/hermes/services/bridge/python').resolve()))
from bridge_broker import BridgeBroker
broker = BridgeBroker('ipc:///tmp/unused-binding-test.sock')
broker._forward = lambda *args, **kwargs: {'ok': True}
for request in (
    {'action':'chat', 'session_id':'synthetic', 'profile':'alpha'},
    {'action':'context_estimate', 'session_id':'synthetic', 'profile':'alpha'},
    {'action':'chat', 'session_id':'synthetic', 'profile':'alpha', 'model_run_auth':{'version':1, 'owner_id':'1','profile':'beta','run_marker':'r','token_path':'/tmp/synthetic-source.json'}},
):
    try:
        broker.handle(request)
    except (ValueError, KeyError):
        pass
    else:
        raise AssertionError('unbound or mismatched model execution admitted')
`], { encoding: 'utf8', timeout: 10_000 })
  })
})
