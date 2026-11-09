"""Native loop lifecycle, atomic native state + minimal generation/tick metadata.
No model/session worker is loaded by list/recovery. SQLite is the shared writer gate.
"""
from __future__ import annotations
import json,logging,sqlite3,time,uuid
from pathlib import Path
from contextlib import contextmanager
import bridge_recovery as recovery
from bridge_recovery_resources import public as public_recovery

META='webui-loop:'
STARTUP_FAILURE_LIMIT=4
logger=logging.getLogger(__name__)

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

    def save(self,db,sid,state,meta,transition_reason=None):
        previous,_=self.read(db,sid)
        if state and (transition_reason or not previous or (previous.status,previous.paused_reason,previous.last_stop_reason)!=(state.status,state.paused_reason,state.last_stop_reason)):
            reason=transition_reason or (state.paused_reason if state.status=='paused' else state.last_stop_reason)
            if not reason:reason='manual-resume' if previous and previous.status=='paused' and state.status=='active' else 'loop-'+state.status
            meta.setdefault('status_events',[]).append({'id':uuid.uuid4().hex,'timestamp':time.time(),
                'status':state.status,'reason':reason,'ticks_fired':state.ticks_fired,
                'total':state.times or state.max_ticks,'period':state.cadence_label(),'generation':meta['generation']})
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
        if arg.strip().split(None,1)[:1] and arg.strip().split(None,1)[0].lower()=='maintenance-drain':
            return {'session_id':sid,'handled':True,'ok':False,'created':False,'command':'loop','action':'loop','message':'/loop maintenance-drain is obsolete and unsupported'}
        from hermes_cli.loops import dispatch_loop_command
        with self.transaction() as db:
            state,meta=self.read(db,sid);mgr=self.manager(sid,state)
            before=state.to_json() if state else None
            prior_count=state.ticks_fired if state else None
            action=arg.strip().lower()
            if action=='resume' and (recovery.unresolved(meta) or recovery.exhausted(state) or (meta.get('tick') or {}).get('phase')=='unknown'):
                return {'session_id':sid,'handled':True,'ok':False,'message':'/loop: unresolved dispatch/recovery or exhausted count; resume denied',**self.result(state,meta)}
            result=dispatch_loop_command(mgr,arg,route={'platform':'agent_bridge','chat_id':sid,'profile':profile})
            # clear() hides the state but its cleared record must remain in native storage.
            state=mgr.state or (state if arg.lower() in {'stop','clear','cancel'} else None)
            if result['created'] or (state.to_json() if state else None)!=before or action in {'pause','stop','clear','cancel'}:
                running=meta.get('tick')
                if running and running['generation']==meta['generation']:
                    running.setdefault('ticks_fired',prior_count)
                meta['generation']+=1
                meta.pop('startup_failure',None)
                if meta.get('recovery') and recovery.unresolved(meta):
                    meta['recovery'].update(status='held' if action=='pause' else 'superseded',result='new user control invalidated recovery',finished_at=time.time())
            self.save(db,sid,state,meta,transition_reason='loop-created' if result['created'] else None)
            output=result['output']
            running=meta.get('tick')
            if running:output+='\nCurrent wakeup still running; pause/stop only prevent subsequent wakeups. Use the existing abort action to interrupt.'
            if meta.get('recovery'):output+='\nRecovery: '+json.dumps(public_recovery(meta['recovery']),ensure_ascii=False)
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
                    if s and (s.status=='active' or recovery.unresolved(m) or req.get('include_inflight') and m.get('tick') or req.get('include_events') and m.get('status_events')):out.append({'session_id':key[len(META):],'route':s.route,'next_due_at':s.next_due_at,'awaiting_response':s.awaiting_response,'current_tick':m.get('tick'),'generation':m['generation'],'recovery':public_recovery(m.get('recovery')),'status':s.status,'status_events':m.get('status_events',[]) if req.get('include_events') else []})
                return {'loops':out}
        # Native --until may call a judge: evaluate outside the writer transaction,
        # then exact-snapshot CAS below. Controls never wait for the model/judge.
        candidate=None;snapshot=None;decision=None
        if op=='complete':
            with self.transaction() as db:
                state,meta=self.read(db,sid)
                snapshot=(state.to_json() if state else None,json.dumps({k:v for k,v in meta.items() if k!='status_events'},sort_keys=True))
                if not state or (state.route or {}).get('profile')!=req.get('profile') or not self.matches(meta,req):return self.result(state,meta,stale=True)
            if meta['generation']==req['generation']:
                mgr=self.manager(sid,state);decision=mgr.complete_tick(req.get('response',''));candidate=mgr.state
        with self.transaction() as db:
            state,meta=self.read(db,sid);mgr=self.manager(sid,state)
            if op=='event_ack':
                authorized=bool(state and (state.route or {}).get('profile')==req.get('profile'))
                if authorized:
                    meta['status_events']=[e for e in meta.get('status_events',[]) if e['id']!=req.get('event_id')]
                    self.save(db,sid,state,meta)
                return {'acknowledged':authorized}
            if op=='receipt':
                tick=meta.get('tick') if req.get('receipt_op')=='started' else meta.get('claim_fences',{}).get(req.get('tick_id'))
                matched=bool(state and (state.route or {}).get('profile')==req.get('profile')
                    and tick and tick.get('id')==req.get('tick_id') and tick.get('generation')==req.get('generation')
                    and tick.get('profile')==req.get('profile') and tick.get('run_id')==req.get('run_id')
                    and req.get('run_id') and (tick.get('phase')=='started' if req.get('receipt_op')=='started'
                        else req.get('receipt_op')=='complete' and tick.get('outcome')=='complete'))
                return self.result(state,meta,receipt_confirmed=matched)
            if op in {'claim','reconcile_claim','inject','started','complete','unknown','abandon','interrupted','failed'}:
                if not state or (state.route or {}).get('profile')!=req.get('profile'):
                    return self.result(state,meta,stale=True,claimed=False,authorized=False)
            if op=='reconcile_claim':
                return self.reconcile_claim(db,sid,mgr,meta,req)
            if op=='recover':
                recovery.boot(self,mgr,meta,recovery_only=req.get('recovery_only') is True)
            elif op.startswith('recovery_'):
                return recovery.operation(self,db,mgr,meta,req)
            elif op=='claim':
                if (type(req.get('generation')) is not int or req['generation']!=meta['generation']
                    or not isinstance(req.get('tick_id'),str) or not req['tick_id'] or len(req['tick_id'])>128
                    or req['tick_id'] in meta.get('claim_fences',{})):
                    return self.result(state,meta,claimed=False,stale=True)
                if meta.get('tick') or recovery.unresolved(meta) or recovery.exhausted(state) or not mgr.is_due() or goal_blocks_loop_tick(sid):return self.result(state,meta,claimed=False)
                prompt=mgr.fire_tick()
                meta['tick']={'id':req['tick_id'],'generation':meta['generation'],'profile':req['profile'],'phase':'claimed'}
                self.save(db,sid,mgr.state,meta)
                return self.result(mgr.state,meta,claimed=True,prompt=prompt)
            elif op == 'inject':
                authorized = self.matches(meta,req) and meta['tick']['phase']=='claimed' and meta['generation']==req['generation'] and state and state.status=='active'
                if authorized:
                    meta['tick']['phase']='injecting'
                    meta.pop('startup_failure',None)
                    self.save(db,sid,state,meta)
                return self.result(state,meta,authorized=bool(authorized))
            elif op in {'complete','unknown','abandon','started','interrupted','failed'}:
                if not self.matches(meta,req):return self.result(state,meta,stale=True)
                if op=='abandon' and (req.get('not_started') is not True
                                      or meta['tick'].get('phase') not in {'claimed','injecting'}
                                      or meta['tick'].get('run_id')):
                    raise ValueError('abandon requires positive no-dispatch proof and no started run')
                if op=='started':
                    if meta['tick']['phase']!='injecting':return self.result(state,meta,stale=True)
                    meta['tick']['phase']='started';meta['tick']['run_id']=req['run_id']
                else:
                    if op=='complete' and meta['generation']==req['generation'] and snapshot!=(state.to_json() if state else None,json.dumps({k:v for k,v in meta.items() if k!='status_events'},sort_keys=True)):
                        return self.result(state,meta,stale=True)
                    if meta['generation']==req['generation']:
                        if op=='complete':mgr=self.manager(sid,candidate)
                        elif op=='abandon':
                            state.awaiting_response=False;mgr.pause('injection-not-executed: count retained; manual resume required')
                        elif op=='interrupted':mgr.pause('user-interrupted: count retained; manually resume when ready')
                        elif op=='failed':mgr.pause('execution-failed: count retained; inspect command/run error then manually resume')
                        else:mgr.pause('execution-unknown: count retained; inspect result then manually resume')
                    # New explicit controls own the new generation. Preserve an
                    # old uncertain receipt for audit, never mutate the new loop.
                    elif op in {'unknown','interrupted','failed'}:
                        meta.setdefault('retired_ticks',[]).append({**meta['tick'],'outcome':op})
                    self.fence(meta,meta['tick'],req['profile'],op)
                    meta['tick']=None
            else:raise ValueError('Unsupported loop operation')
            self.save(db,sid,mgr.state,meta)
            return self.result(mgr.state,meta,decision=decision if op=='complete' else None)

    @staticmethod
    def fence(meta,tick,profile,outcome):
        meta.setdefault('claim_fences',{})[tick['id']]={**tick,'profile':profile,'outcome':outcome}

    def reconcile_claim(self,db,sid,mgr,meta,req):
        state=mgr.state;tick=meta.get('tick');tid=req.get('tick_id');gen=req.get('generation')
        old_due=state.next_due_at
        if type(gen) is not int or not isinstance(tid,str) or not tid or len(tid)>128:
            return self.result(state,meta,stale=True)
        prior=meta.get('claim_fences',{}).get(tid)
        if prior:
            valid=prior['generation']==gen and prior['profile']==req['profile']
            return self.result(state,meta,stale=not valid,claim_resolution=prior['outcome'] if valid else None)
        if tick and tick['id']==tid:
            if tick['generation']!=gen or tick.get('profile',req['profile'])!=req['profile']:
                return self.result(state,meta,stale=True)
            if tick['phase']!='claimed' or tick.get('run_id'):
                # inject is durable BEFORE dispatch; missing run_id is not no-dispatch proof.
                if tick['phase']!='unknown':
                    tick['uncertain_phase']=tick['phase'];tick['phase']='unknown'
                    tick['recovery_reason']='Dispatch may have occurred; retain count and block wakeups. Verify saved run/session evidence through bound recovery; never replay this tick.'
                    if gen==meta['generation'] and state.status=='active':
                        mgr.pause('dispatch-unknown: count retained; verify saved run evidence through bound recovery; never replay')
                    self.save(db,sid,state,meta)
                return self.result(state,meta,claim_resolution='unknown')
            meta.setdefault('retired_ticks',[]).append({**tick,'outcome':'claim-only-retired'})
            self.fence(meta,tick,req['profile'],'claim-only-retired')
            meta['tick']=None
            if gen==meta['generation']:
                state.awaiting_response=False
                if recovery.exhausted(state):
                    mgr.pause('claim-only-retired: count retained; exhausted limit requires explicit new loop control')
                elif state.status=='active':
                    from hermes_cli.loops import self_paced_floor_seconds
                    state.next_due_at=time.time()+max(1,state.current_delay or state.interval_seconds or self_paced_floor_seconds())
                    self.startup_failure(mgr,meta,'claim-only-retired',retry=False)
            resolution='claim-only-retired'
        elif gen==meta['generation'] and not tick:
            # A delayed pre-commit claim must observe this tombstone under the same writer lock.
            self.fence(meta,{'id':tid,'generation':gen},req['profile'],'not-committed-fenced')
            if state.status=='active':
                self.startup_failure(mgr,meta,'not-committed-fenced',retry=True)
            resolution='not-committed-fenced'
        elif gen<meta['generation']:
            return self.result(state,meta,claim_resolution='generation-fenced')
        else:return self.result(state,meta,stale=True)
        self.save(db,sid,state,meta)
        logger.warning('loop_claim_reconciled %s',json.dumps({'session_id':sid,'profile':req['profile'],
            'generation':gen,'tick_id':tid,'resolution':resolution,'old_due_at':old_due,
            'next_due_at':state.next_due_at,'ticks_fired':state.ticks_fired,'status':state.status,
            'startup_failure':meta.get('startup_failure')}))
        return self.result(state,meta,claim_resolution=resolution)

    @staticmethod
    def startup_failure(mgr,meta,resolution,retry):
        previous=meta.get('startup_failure') or {}
        attempts=previous.get('attempts',0)+1 if previous.get('generation')==meta['generation'] else 1
        failure={'generation':meta['generation'],'attempts':attempts,'resolution':resolution,
                 'failed_at':time.time(),'retry_at':None}
        meta['startup_failure']=failure
        if attempts>=STARTUP_FAILURE_LIMIT:
            mgr.pause('startup-failed: repeated claim failures; inspect transport then explicitly resume')
        elif retry:
            mgr.state.next_due_at=time.time()+2**attempts
            failure['retry_at']=mgr.state.next_due_at

    @staticmethod
    def matches(meta,req):
        tick=meta.get('tick')
        return bool(tick and type(req.get('generation')) is int and tick['id']==req.get('tick_id') and tick['generation']==req.get('generation')
                    and tick.get('profile',req.get('profile'))==req.get('profile')
                    and (not tick.get('run_id') or tick['run_id']==req.get('run_id')))

    @staticmethod
    def result(state,meta,**extra):
        return {'loop_state':json.loads(state.to_json()) if state else None,'generation':meta['generation'],'current_tick':meta.get('tick'),'recovery':public_recovery(meta.get('recovery')),'startup_failure':meta.get('startup_failure'),'status_events':meta.get('status_events',[]),**extra}
