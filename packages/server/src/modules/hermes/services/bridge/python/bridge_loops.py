"""Native loop lifecycle, atomic native state + minimal generation/tick metadata.
No model/session worker is loaded by list/recovery. SQLite is the shared writer gate.
"""
from __future__ import annotations
import json,sqlite3,time,uuid
from pathlib import Path
from contextlib import contextmanager
import bridge_recovery as recovery

META='webui-loop:'

class LoopStore:
    def __init__(self, home):
        self.home=Path(home)
        self.path=self.home/'state.db'

    @contextmanager
    def transaction(self):
        if not self.path.exists():
            from hermes_cli.loops import _get_session_db
            db=_get_session_db()
            if db is None or Path(db.db_path)!=self.path:
                raise RuntimeError('Loop database unavailable')
        db=sqlite3.connect(self.path,timeout=10)
        try:
            db.execute('BEGIN IMMEDIATE')
            yield db
            db.commit()
        except BaseException:
            db.rollback();raise
        finally:db.close()

    def read(self, db, sid):
        from hermes_cli.loops import LoopState
        def get(key):
            row=db.execute('SELECT value FROM state_meta WHERE key=?',(key,)).fetchone()
            return row[0] if row else None
        raw=get('loop:'+sid);meta=get(META+sid)
        return LoopState.from_json(raw) if raw else None,json.loads(meta) if meta else {'generation':0,'tick':None}

    def save(self,db,sid,state,meta):
        for key,value in [('loop:'+sid,state.to_json() if state else None),(META+sid,json.dumps(meta))]:
            if value is None:continue
            db.execute('INSERT INTO state_meta(key,value) VALUES (?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value',(key,value))
            if db.execute('SELECT value FROM state_meta WHERE key=?',(key,)).fetchone()[0]!=value:
                raise RuntimeError('Loop state persistence readback failed')

    def manager(self,sid,state):
        from hermes_cli.loops import LoopManager
        class StagedManager(LoopManager):
            def __init__(self):self.session_id=sid;self._state=state
            def _save(self):return self._state
        return StagedManager()

    def command(self,sid,arg,profile):
        from hermes_cli.loops import dispatch_loop_command
        with self.transaction() as db:
            state,meta=self.read(db,sid);mgr=self.manager(sid,state)
            before=state.to_json() if state else None
            action=arg.strip().lower()
            if action=='resume' and (recovery.unresolved(meta) or recovery.exhausted(state)):
                return {'session_id':sid,'handled':True,'ok':False,'message':'/loop: unresolved recovery or exhausted count; resume denied',**self.result(state,meta)}
            result=dispatch_loop_command(mgr,arg,route={'platform':'agent_bridge','chat_id':sid,'profile':profile})
            # clear() hides the state but its cleared record must remain in native storage.
            state=mgr.state or (state if arg.lower() in {'stop','clear','cancel'} else None)
            if result['created'] or (state.to_json() if state else None)!=before or action in {'pause','stop','clear','cancel'}:
                meta['generation']+=1
                if meta.get('recovery') and recovery.unresolved(meta):
                    meta['recovery'].update(status='held' if action=='pause' else 'superseded',result='new user control invalidated recovery',finished_at=time.time())
            self.save(db,sid,state,meta)
            output=result['output']
            running=meta.get('tick')
            if running:output+='\nCurrent wakeup still running; pause/stop only prevent subsequent wakeups. Use the existing abort action to interrupt.'
            if meta.get('recovery'):output+='\nRecovery: '+json.dumps(meta['recovery'],ensure_ascii=False)
            return {'session_id':sid,'handled':True,'ok':not output.startswith('/loop:'),'command':'loop','action':'loop','type':'loop','message':output,'created':result['created'],**self.result(state,meta),'scheduler_enabled':True}

    def operation(self,req):
        from hermes_cli.loops import goal_blocks_loop_tick
        op=req['op'];sid=req.get('session_id','')
        if op=='list':
            if not self.path.exists():return {'loops':[]}
            with sqlite3.connect(f'file:{self.path}?mode=ro',uri=True) as db:
                rows=db.execute("SELECT key,value FROM state_meta WHERE key LIKE 'webui-loop:%'").fetchall()
                out=[]
                for key,_ in rows:
                    s,m=self.read(db,key[len(META):])
                    if s and (s.status=='active' or recovery.unresolved(m) or req.get('include_inflight') and m.get('tick')):out.append({'session_id':key[len(META):],'route':s.route,'next_due_at':s.next_due_at,'awaiting_response':s.awaiting_response,'current_tick':m.get('tick'),'generation':m['generation'],'recovery':m.get('recovery')})
                return {'loops':out}
        # Native --until may call a judge: evaluate outside the writer transaction,
        # then exact-snapshot CAS below. Controls never wait for the model/judge.
        candidate=None;snapshot=None;decision=None
        if op=='complete':
            with self.transaction() as db:
                state,meta=self.read(db,sid)
                snapshot=(state.to_json() if state else None,json.dumps(meta,sort_keys=True))
                if not self.matches(meta,req):return self.result(state,meta,stale=True)
            if meta['generation']==req['generation']:
                mgr=self.manager(sid,state);decision=mgr.complete_tick(req.get('response',''));candidate=mgr.state
        with self.transaction() as db:
            state,meta=self.read(db,sid);mgr=self.manager(sid,state)
            if op=='recover':
                recovery.boot(self,mgr,meta)
            elif op.startswith('recovery_'):
                return recovery.operation(self,db,mgr,meta,req)
            elif op=='claim':
                if meta.get('tick') or recovery.unresolved(meta) or recovery.exhausted(state) or not mgr.is_due() or goal_blocks_loop_tick(sid):return self.result(state,meta,claimed=False)
                prompt=mgr.fire_tick()
                meta['tick']={'id':req['tick_id'],'generation':meta['generation'],'phase':'claimed'}
                self.save(db,sid,mgr.state,meta)
                return self.result(mgr.state,meta,claimed=True,prompt=prompt)
            elif op == 'inject':
                authorized = self.matches(meta,req) and meta['generation']==req['generation'] and state and state.status=='active'
                if authorized:
                    meta['tick']['phase']='injecting'
                    self.save(db,sid,state,meta)
                return self.result(state,meta,authorized=bool(authorized))
            elif op in {'complete','unknown','abandon','started','interrupted','failed'}:
                if not self.matches(meta,req):return self.result(state,meta,stale=True)
                if op=='abandon' and (req.get('not_started') is not True
                                      or meta['tick'].get('phase') not in {'claimed','injecting'}
                                      or meta['tick'].get('run_id')):
                    raise ValueError('abandon requires positive no-dispatch proof and no started run')
                if op=='started':meta['tick']['phase']='started';meta['tick']['run_id']=req['run_id']
                else:
                    if op=='complete' and meta['generation']==req['generation'] and snapshot!=(state.to_json() if state else None,json.dumps(meta,sort_keys=True)):
                        return self.result(state,meta,stale=True)
                    if meta['generation']==req['generation']:
                        if op=='complete':mgr=self.manager(sid,candidate)
                        elif op=='abandon':mgr.abandon_tick();mgr.pause('injection-not-executed: manual resume required')
                        elif op=='interrupted':mgr.pause('user-interrupted: count retained; manually resume when ready')
                        elif op=='failed':mgr.pause('execution-failed: count retained; inspect command/run error then manually resume')
                        else:mgr.pause('execution-unknown: count retained; inspect result then manually resume')
                    # New explicit controls own the new generation. Preserve an
                    # old uncertain receipt for audit, never mutate the new loop.
                    elif op in {'unknown','interrupted','failed'}:
                        meta.setdefault('retired_ticks',[]).append({**meta['tick'],'outcome':op})
                    meta['tick']=None
            else:raise ValueError('Unsupported loop operation')
            self.save(db,sid,mgr.state,meta)
            return self.result(mgr.state,meta,decision=decision if op=='complete' else None)

    @staticmethod
    def matches(meta,req):
        tick=meta.get('tick')
        return bool(tick and tick['id']==req.get('tick_id') and tick['generation']==req.get('generation')
                    and (not tick.get('run_id') or tick['run_id']==req.get('run_id')))

    @staticmethod
    def result(state,meta,**extra):
        return {'loop_state':json.loads(state.to_json()) if state else None,'generation':meta['generation'],'current_tick':meta.get('tick'),'recovery':meta.get('recovery'),**extra}
