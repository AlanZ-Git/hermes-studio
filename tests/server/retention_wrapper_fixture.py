import copy
import importlib.util
import json
import os
from pathlib import Path
import tempfile
import sys

assert os.environ.get('STUDIO_TEST_ONLY') == '1', 'test-only marker required'
root = Path(os.environ['STUDIO_TEST_ROOT'])
assert root.is_absolute() and root == root.resolve(strict=True) and root != Path(root.anchor)
os.environ['HOME'] = str(root / 'home')
os.environ['TMPDIR'] = str(root)
os.environ['HERMES_HOME'] = str(root / 'core-home')
os.environ['HERMES_AGENT_BRIDGE_BASE_HOME'] = os.environ['HERMES_HOME']

source_root = Path(__file__).resolve().parents[2]
source = source_root / ('dist/server/agent-bridge/python/notification_retention.py' if '--packaged' in sys.argv else 'scripts/notification_retention.py')
spec = importlib.util.spec_from_file_location('retention', source)
r = importlib.util.module_from_spec(spec)
spec.loader.exec_module(r)

def candidate():
    return {'obsolete': True, 'worker': {'broker_generation': 'broker', 'worker_key': 'original', 'profile': 'default',
        'pid_start': '123', 'pid': 999999, 'socket_identity': [1, 2]},
        'snapshot': {'delegation_id': 'delegation', 'claim_id': 'claim', 'session_id': 'session', 'profile': 'default',
            'worker_generation': 'worker', 'identity': {'event_sha256': 'a'*64, 'result_sha256': 'b'*64,
                'database': 'c'*64, 'session_id': 'session', 'origin_session': 'origin', 'origin_ui_session_id': ''}}}

def page(items, next=None):
    return {'schema_version': r.PAGE_SCHEMA, 'candidates': items, 'next': next}

for bad in [page([candidate()], ''), page([candidate()], 2), page([candidate()], False),
            page([candidate(), {'obsolete': 'true'}]), {'schema_version': 'wrong', 'candidates': [candidate()], 'next': None},
            page([{'obsolete': True}])]:
    case = Path(tempfile.mkdtemp(dir=root, prefix='bad-page-'))
    result = r.scan(case, request=lambda req: bad, now=1000)
    assert result['candidate_count'] == 0 and result['partial'] is True, result
    calls = []
    applied = r.mechanical(case, request=lambda req: calls.append(req), now=1001)
    assert applied['reconciled'] == 0 and calls == []

case = Path(tempfile.mkdtemp(dir=root, prefix='prior-page-'))
pages = iter([page([candidate(), {'obsolete': False, 'reason': 'protected'}], 'first'), page([candidate()], 'first')])
calls = []
result = r.scan(case, request=lambda req: (calls.append(req), next(pages))[1], now=1000)
assert result['candidate_count'] == 2 and result['partial'] and len(calls) == 2, result

def reconcile(req):
    intents = list((case / 'notification-retention').glob('*-intent.json'))
    assert intents and json.loads(intents[0].read_text())['before'] == req['expected']
    return {'reconciled': False, 'reason': 'worker_changed'}

applied = r.mechanical(case, request=reconcile, now=1001)
assert applied == {'status': 'partial_checked', 'reconciled': 0, 'count': 2}, applied
saved = json.loads((case / 'notification-retention-latest.json').read_text())
for key, value in [('partial', False), ('errors', []), ('stop_reason', 'complete'), ('candidates', [candidate()])]:
    tampered = {**saved, key: value}
    (case / 'notification-retention-latest.json').write_text(json.dumps(tampered))
    calls = []
    assert r.mechanical(case, request=lambda req: calls.append(req), now=1001)['status'] == 'failed_closed'
    assert not calls
(case / 'notification-retention-latest.json').write_text(json.dumps(saved))
assert r.mechanical(case, request=lambda req: None, now=1000+r.MAX_AGE+1)['status'] == 'failed_closed'

case = Path(tempfile.mkdtemp(dir=root, prefix='empty-page-'))
assert r.scan(case, request=lambda req: page([]), now=1000)['partial'] is False
assert r.mechanical(case, request=lambda req: None, now=1001)['status'] == 'checked'
case = Path(tempfile.mkdtemp(dir=root, prefix='page-limit-'))
calls = []
def endless(req):
    calls.append(req)
    return page([{'obsolete': False}], f'{len(calls):04}')
assert r.scan(case, request=endless, now=1000)['stop_reason'] == 'worker_limit'
assert len(calls) == 8
case = Path(tempfile.mkdtemp(dir=root, prefix='cli-'))
original_scan = r.scan
try:
    r.scan = lambda output: original_scan(output, request=lambda req: page([]), now=1000)
    assert r.main(['scan', '--root', str(case)]) == 0
finally:
    r.scan = original_scan
assert json.loads((case / 'notification-retention-latest.json').read_text())['candidates'] == []
print('B34 whole-page/partial/tamper/expiry/protected/limit/intent/CLI controls passed from', source)
