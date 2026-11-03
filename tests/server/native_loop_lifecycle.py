"""Isolated SQLite lifecycle checks; no live sessions or model execution."""
import json,sqlite3,tempfile,time
from pathlib import Path
from unittest.mock import patch
import bridge_loops
from hermes_cli import loops
results=[]
def fixture(claim=True):
 home=Path(tempfile.mkdtemp(prefix='loop-lifecycle-'))
 with sqlite3.connect(home/'state.db') as db:db.execute('CREATE TABLE state_meta(key TEXT PRIMARY KEY,value TEXT)')
 store=bridge_loops.LoopStore(home);store.command('fixture','30s synthetic --times 3','default')
 with store.transaction() as db:
  state,meta=store.read(db,'fixture');state.next_due_at=0;store.save(db,'fixture',state,meta)
 r={'generation':1}
 if claim:
  with patch.object(loops,'goal_blocks_loop_tick',return_value=False):r=store.operation({'op':'claim','session_id':'fixture','profile':'default','generation':1,'tick_id':'tick'})
 req={'session_id':'fixture','profile':'default','tick_id':'tick','generation':r['generation']}
 return store,req
for control in ['pause','stop']:
 s,r=fixture();s.operation({**r,'op':'inject'});s.operation({**r,'op':'started','run_id':'native'})
 s.command('fixture',control,'default')
 late=s.operation({**r,'op':'complete','run_id':'native','response':'done'})
 assert late['loop_state']['status']==('paused' if control=='pause' else 'cleared'),late
 assert late['loop_state']['ticks_fired']==1
 assert s.operation({'op':'recover','session_id':'fixture'})['loop_state']['status']==late['loop_state']['status']
 results.append(control+' beats late completion and restart')
for op in ['unknown','interrupted','failed']:
 s,r=fixture();s.operation({**r,'op':'inject'});q=s.operation({**r,'op':op})
 assert q['loop_state']['status']=='paused' and q['loop_state']['ticks_fired']==1
 assert not s.operation({'op':'list'})['loops']
 s.operation({'op':'recover','session_id':'fixture'})
 with patch.object(loops,'goal_blocks_loop_tick',return_value=False):assert not s.operation({**r,'op':'claim'})['claimed']
 results.append(op+' retains count and no replay after reopen')
for phase in ['claimed','injecting','started']:
 s,r=fixture()
 if phase!='claimed':s.operation({**r,'op':'inject'})
 if phase=='started':s.operation({**r,'op':'started','run_id':'native'})
 q=s.operation({'op':'recover','session_id':'fixture'})
 assert q['loop_state']['ticks_fired']==1
 assert q['recovery']['status']==('released' if phase=='claimed' else 'pending')
 if phase!='claimed':
  rr={'session_id':'fixture','recovery_id':q['recovery']['id'],'expected_generation':q['generation']}
  s.operation({**rr,'op':'recovery_claim'});s.operation({**rr,'op':'recovery_inject'})
  with s.transaction() as db:
   state,meta=s.read(db,'fixture');meta['recovery']['deadline_at']=time.time()-1;s.save(db,'fixture',state,meta)
  q=s.operation({**rr,'op':'recovery_commit'})
  assert q.get('stale') and q['loop_state']['status']=='paused'
  q=s.operation({'op':'recover','session_id':'fixture'});assert q['recovery']['status']=='failed'
 results.append(phase+' restart, timeout and no stale commit')
def snapshot(s):
 with s.transaction() as db:
  state,meta=s.read(db,'fixture');return state.to_json(),json.dumps(meta,sort_keys=True)

for legacy in [False,True]:
 s,r=fixture()
 if legacy:
  with s.transaction() as db:
   state,meta=s.read(db,'fixture');meta['tick'].pop('profile');s.save(db,'fixture',state,meta)
 before=time.time();q=s.operation({**r,'op':'reconcile_claim'})
 assert q['claim_resolution']=='claim-only-retired' and q['current_tick'] is None
 assert q['loop_state']['ticks_fired']==1 and not q['loop_state']['awaiting_response']
 assert q['loop_state']['next_due_at']>=before+30
 saved=snapshot(s)
 assert s.operation({**r,'op':'reconcile_claim'})['claim_resolution']=='claim-only-retired'
 for op in ['claim','inject','started']:
  late=s.operation({**r,'op':op,'run_id':'late'})
  assert not late.get('claimed') and not late.get('authorized')
 assert snapshot(s)==saved
 results.append(('legacy' if legacy else 'new')+' claim retirement, durable fence and duplicate receipts')

s,r=fixture();new=s.command('fixture','1m newer --times 2','default')
saved_state=new['loop_state'];q=s.operation({**r,'op':'reconcile_claim'})
assert q['loop_state']==saved_state and q['generation']==new['generation']
assert q['current_tick'] is None
with s.transaction() as db:
 state,meta=s.read(db,'fixture');assert meta['retired_ticks'][-1]['generation']==r['generation']
results.append('new set preserved byte-for-byte when old claimed generation retires')

for phase in ['injecting','started']:
 s,r=fixture();s.operation({**r,'op':'inject'})
 if phase=='started':s.operation({**r,'op':'started','run_id':'native'})
 q=s.operation({**r,'op':'reconcile_claim'})
 assert q['claim_resolution']=='unknown' and q['current_tick']['phase']=='unknown'
 assert q['loop_state']['ticks_fired']==1 and q['loop_state']['status']=='paused'
 assert 'dispatch-unknown' in q['loop_state']['paused_reason']
 before=snapshot(s)
 for op in ['claim','inject','started','reconcile_claim']:
  s.operation({**r,'op':op,'run_id':'late'})
 assert snapshot(s)==before
 assert not s.command('fixture','resume','default')['ok']
 results.append(phase+' without terminal proof remains unknown and cannot replay')

s,r=fixture();before=snapshot(s)
for change in [{'profile':'other'},{'session_id':'other'},{'generation':0},{'generation':2},{'tick_id':'other'}]:
 q=s.operation({**r,**change,'op':'reconcile_claim'});assert q.get('stale'),q
 assert snapshot(s)==before
for change in [{'generation':0},{'generation':2},{'generation':None},{'profile':'other'}]:
 q=s.operation({**r,**change,'op':'claim'});assert not q['claimed']
 assert snapshot(s)==before
results.append('session profile tick old/future generation mismatches cannot write')

s,r=fixture()
with s.transaction() as db:
 state,meta=s.read(db,'fixture');state.times=1;s.save(db,'fixture',state,meta)
q=s.operation({**r,'op':'reconcile_claim'})
assert q['loop_state']['ticks_fired']==1 and q['loop_state']['status']=='paused'
assert 'exhausted' in q['loop_state']['paused_reason']
assert not s.command('fixture','resume','default')['ok']
results.append('claim-only exhaustion retains count and denies resume without new explicit control')

s,r=fixture();s.operation({**r,'op':'inject'});s.operation({**r,'op':'started','run_id':'native'})
before=snapshot(s)
for op,extra in [('claim',{}),('inject',{}),('started',{'run_id':'native'}),('started',{'run_id':'other'})]:
 s.operation({**r,'op':op,**extra})
assert snapshot(s)==before
results.append('duplicate live claim inject and started receipts never regress started phase or run id')

s,r=fixture(False);new=s.command('fixture','1m newer --times 2','default');before=snapshot(s)
q=s.operation({**r,'op':'reconcile_claim'})
assert q['claim_resolution']=='generation-fenced' and snapshot(s)==before
assert not s.operation({**r,'op':'claim'})['claimed'] and snapshot(s)==before
results.append('uncommitted old generation is read-only fenced by generation CAS after new set')

s,r=fixture();q=s.operation({**r,'op':'abandon','not_started':True})
assert q['loop_state']['ticks_fired']==1 and q['loop_state']['status']=='paused'
results.append('positive local abandon never refunds count')

import threading
from contextlib import contextmanager
s,r=fixture(False);arrived=threading.Event();release=threading.Event();late=[]
delayed=bridge_loops.LoopStore(s.home);transaction=delayed.transaction
@contextmanager
def before_commit():
 arrived.set();assert release.wait(5)
 with transaction() as db:yield db
delayed.transaction=before_commit
def late_claim():late.append(delayed.operation({**r,'op':'claim'}))
thread=threading.Thread(target=late_claim)
with patch.object(loops,'goal_blocks_loop_tick',return_value=False):
 thread.start();assert arrived.wait(5)
 q=s.operation({**r,'op':'reconcile_claim'})
 assert q['claim_resolution']=='not-committed-fenced'
 saved=snapshot(s);release.set();thread.join(5);assert not thread.is_alive()
assert not late[0]['claimed'] and snapshot(s)==saved
assert s.operation({**r,'op':'reconcile_claim'})['claim_resolution']=='not-committed-fenced'
assert q['loop_state']['ticks_fired']==0
results.append('concurrent delayed precommit request fenced durably before writer admission')

for status,attempts,expected in [('claimed',1,'pending'),('claimed',2,'failed'),('injecting',1,'failed'),('started',1,'failed')]:
 s,r=fixture();s.command('fixture','pause','default')
 with s.transaction() as db:
  state,meta=s.read(db,'fixture')
  meta['recovery']={'id':'saved-recovery','generation':meta['generation'],'status':status,'attempts':attempts}
  s.save(db,'fixture',state,meta)
  before_state=state.to_json();before_tick=dict(meta['tick']);generation=meta['generation']
 q=s.operation({'op':'recover','session_id':'fixture','recovery_only':True})
 assert q['recovery']['id']=='saved-recovery' and q['recovery']['status']==expected,q
 assert q['current_tick']==before_tick and q['generation']==generation
 with s.transaction() as db:
  state,meta=s.read(db,'fixture')
  assert state.to_json()==before_state and 'claim_fences' not in meta and 'recovery_history' not in meta
 saved=snapshot(s)
 s.operation({'op':'recover','session_id':'fixture','recovery_only':True})
 assert snapshot(s)==saved
results.append('recovery-only startup settles interrupted recovery statuses idempotently without touching business tick or count')

s,r=fixture(False)
before=time.time();q=s.operation({**r,'op':'reconcile_claim'})
assert q['claim_resolution']=='not-committed-fenced'
assert before < q['loop_state']['next_due_at'] <= before+3, q
assert q['loop_state']['ticks_fired']==0
saved=snapshot(s)
assert s.operation({**r,'op':'reconcile_claim'})['loop_state']==q['loop_state']
assert snapshot(s)==saved
results.append('uncommitted attempt retries shortly without consuming a round or extending duplicate backoff')

s,r=fixture(False)
for attempt in range(1,5):
 req={**r,'tick_id':f'failure-{attempt}','op':'reconcile_claim'}
 before=time.time();q=s.operation(req)
 assert q['startup_failure']['attempts']==attempt
 assert q['loop_state']['ticks_fired']==0
 if attempt<4:
  assert before+2**attempt <= q['loop_state']['next_due_at'] <= time.time()+2**attempt
 else:
  assert q['loop_state']['status']=='paused' and 'startup-failed' in q['loop_state']['paused_reason']
 saved=snapshot(s);s.operation(req);assert snapshot(s)==saved
reopened=bridge_loops.LoopStore(s.home)
with reopened.transaction() as db:
 state,meta=reopened.read(db,'fixture')
 assert state.status=='paused' and meta['startup_failure']['attempts']==4
assert not reopened.operation({'op':'list'})['loops']
assert reopened.command('fixture','resume','default')['loop_state']['status']=='active'
results.append('consecutive precommit failures persist bounded backoff then visible pause across reopen')

for control in ['pause','stop','30s newer --times 2']:
 s,r=fixture(False);s.command('fixture',control,'default');saved=snapshot(s)
 q=s.operation({**r,'op':'reconcile_claim'})
 assert q['claim_resolution']=='generation-fenced' and snapshot(s)==saved
results.append('pause stop and new-set invalidate uncommitted retries without rewriting new state')

s,r=fixture(False);s.operation({**r,'op':'reconcile_claim'})
with s.transaction() as db:
 state,meta=s.read(db,'fixture');state.next_due_at=0;s.save(db,'fixture',state,meta)
with patch.object(loops,'goal_blocks_loop_tick',return_value=False):
 r={**r,'tick_id':'retry'};assert s.operation({**r,'op':'claim'})['claimed']
q=s.operation({**r,'op':'inject'});assert q['authorized'] and q['startup_failure'] is None
assert q['loop_state']['ticks_fired']==1
results.append('successful retry injection resets failure streak and consumes exactly one round')

print(json.dumps({'checks':results,'count':len(results),'bridge_file':bridge_loops.__file__,'core_file':loops.__file__},indent=2))
