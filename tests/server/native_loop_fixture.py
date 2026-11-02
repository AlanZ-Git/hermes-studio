"""Test-only JSON RPC for real LoopStore, private per-case SQLite only."""
import json,sqlite3,sys,time
from pathlib import Path
from unittest.mock import patch
import bridge_loops
request=json.loads(sys.argv[1]); home=Path(request.pop('_home'))
if '_now' in request:
 now=request.pop('_now');time.time=lambda:now
home.mkdir(parents=True,exist_ok=True)
with sqlite3.connect(home/'state.db') as db:
 db.execute('CREATE TABLE IF NOT EXISTS state_meta(key TEXT PRIMARY KEY,value TEXT)')
store=bridge_loops.LoopStore(home)
action=request.pop('action',None)
sid=request.get('session_id','session-1')
if action=='fixture_proof':
 import threading
 from bridge_pool import AgentPool,RunRecord
 pool=object.__new__(AgentPool);pool._lock=threading.RLock()
 pool._runs={request['run_id']:RunRecord(run_id=request['run_id'],session_id=sid,status='complete',deltas=['verified response'],result={'completed':True})}
 result=pool.get_output(request['run_id'],cursor=1)
elif action=='fixture_command':
 result=store.command(sid,request['command'],request.get('profile','default'))
elif action=='fixture_due':
 with store.transaction() as db:
  state,meta=store.read(db,sid);state.next_due_at=0;store.save(db,sid,state,meta)
 result={'ok':True}
elif action=='fixture_legacy_claim':
 for _ in range(4):store.command(sid,'30s legacy --times 3','default')
 with store.transaction() as db:
  state,meta=store.read(db,sid);assert meta['generation']==4
  state.next_due_at=time.time()
  mgr=store.manager(sid,state);assert mgr.fire_tick()
  meta={'generation':4,'tick':{'id':'legacy-gen4','generation':4,'phase':'claimed'}}
  store.save(db,sid,mgr.state,meta)
 result=store.result(mgr.state,meta)
elif action=='fixture_legacy_set':
 result=store.command(sid,request['command'],'default')
 # The deployed OLD command replaced native state without snapshotting old count.
 with store.transaction() as db:
  state,meta=store.read(db,sid)
  meta['tick'].pop('ticks_fired',None)
  store.save(db,sid,state,meta)
 result=store.result(state,meta)
elif action=='fixture_snapshot':
 with store.transaction() as db:
  state,meta=store.read(db,sid)
  result={'native_bytes':db.execute('SELECT value FROM state_meta WHERE key=?',('loop:'+sid,)).fetchone()[0],'meta':meta}
elif action=='fixture_status':
 with store.transaction() as db:
  state,meta=store.read(db,sid);result=store.result(state,meta)
elif action=='loop_list': result=store.operation({'op':'list','include_inflight':request.get('include_inflight',False)})
elif action=='loop_recover':
 listed=store.operation({'op':'list','include_inflight':True})
 for item in listed['loops']:
  if request.get('recovery_only') is True and not item.get('recovery'):continue
  store.operation({'op':'recover','session_id':item['session_id'],'recovery_only':request.get('recovery_only') is True})
 result={'ok':True}
else:
 with patch('hermes_cli.loops.goal_blocks_loop_tick',return_value=False):result=store.operation(request)
print(json.dumps(result))
