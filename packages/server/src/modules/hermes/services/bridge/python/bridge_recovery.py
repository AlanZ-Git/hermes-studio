"""Recovery policy staged inside LoopStore's existing BEGIN IMMEDIATE transaction.
No second scheduler/database; native state remains the only loop configuration.
"""
import copy,time,uuid

UNRESOLVED={'pending','claimed','injecting','started','held','failed'}

def unresolved(meta):
    r=meta.get('recovery')
    return bool(r and r.get('status') in UNRESOLVED)

def exhausted(state):
    return bool(state and ((state.times and state.ticks_fired>=state.times) or
                           (state.max_ticks and state.ticks_fired>=state.max_ticks)))

def boot(store,mgr,meta):
    state=mgr.state;tick=meta.get('tick');r=meta.get('recovery')
    if r and r['status'] in {'claimed','injecting','started'}:
        if r['status']=='claimed' and r['attempts']<2:
            r.update(status='pending',result='restart before recovery dispatch; one retry permitted')
        else:
            r.update(status='failed',result='recovery interrupted; no automatic replay',finished_at=time.time())
        if state and state.status=='active':mgr.pause('recovery interrupted; inspect saved recovery record')
    if not tick and not (state and state.status=='active' and state.awaiting_response):return
    # Archive before clearing the live association, including original generation,
    # next_due, phase, count and route. The save is atomic with native state.
    saved={'version':1,'id':uuid.uuid4().hex,'generation':meta['generation'],
           'interrupted_tick':copy.deepcopy(tick),'next_due_at':state.next_due_at if state else None,
           'ticks_fired':state.ticks_fired if state else None,'created_at':time.time(),
           'attempts':0,'status':'pending','result':'execution uncertain; verify, never replay'}
    # Successful ordinary controls win over a stale generation's in-flight tick.
    if not state or state.status!='active' or tick and tick['generation']!=meta['generation']:
        saved.update(status='superseded',result='newer user control or terminal state retained')
    elif tick and tick.get('phase')=='claimed':
        mgr.abandon_tick()  # only durable pre-dispatch evidence justifies refund
        saved.update(status='released',result='durable claimed phase proves no dispatch')
    else:
        mgr.pause('recovery-required: execution uncertain; original chat will inspect without replay')
    if r:meta.setdefault('recovery_history',[]).append(r)
    meta['recovery']=saved
    meta['tick']=None

def operation(store,db,mgr,meta,req):
    state=mgr.state;r=meta.get('recovery');op=req['op'];now=time.time()
    valid=(r and r['id']==req.get('recovery_id') and
           meta['generation']==r['generation']==req.get('expected_generation') and
           state and state.status=='paused')
    if not valid:return store.result(state,meta,stale=True,authorized=False)
    if op=='recovery_status':return store.result(state,meta,authorized=True)
    if op=='recovery_commit':
        proposal=r.get('proposal') or {'decision':'hold','evidence':'Recovery completed without a bound proposal; missing decision'}
        return operation(store,db,mgr,meta,{**req,**proposal,'op':'recovery_decide'})
    if op=='recovery_propose':
        if r['status'] not in {'injecting','started'} or now>r['deadline_at']:return store.result(state,meta,stale=True)
        if req.get('decision') not in {'resume','finish','hold'} or not isinstance(req.get('evidence'),str) or not req['evidence'].strip() or len(req['evidence'])>4000:raise ValueError('Bound proposal and evidence required')
        if r.get('proposal'):return store.result(state,meta,stale=True)
        r['proposal']={'decision':req['decision'],'evidence':req['evidence']}
    elif op=='recovery_claim':
        if r['status']!='pending' or r['attempts']>=2:return store.result(state,meta,authorized=False)
        r.update(status='claimed',attempts=r['attempts']+1,claimed_at=now)
    elif op=='recovery_inject':
        if r['status']!='claimed':return store.result(state,meta,authorized=False)
        r.update(status='injecting',started_at=now,deadline_at=now+300)
    elif op=='recovery_started':
        if r['status']!='injecting':return store.result(state,meta,authorized=False)
        r.update(status='started',run_id=req['run_id'])
    elif op=='recovery_decide':
        if r['status'] not in {'injecting','started'} or now>r['deadline_at']:
            return store.result(state,meta,stale=True,authorized=False)
        decision=req.get('decision');evidence=req.get('evidence')
        if decision not in {'resume','finish','hold'} or not isinstance(evidence,str) or not evidence.strip() or len(evidence)>4000:
            raise ValueError('Recovery requires resume/finish/hold and bounded nonempty evidence')
        if decision=='resume' and exhausted(state):
            decision='finish' if state.times and state.ticks_fired>=state.times else 'hold'
            evidence+='; count limit retained, resume denied'
        r.update(status={'resume':'resumed','finish':'finished','hold':'held'}[decision],
                 decision=decision,evidence=evidence,result=evidence,finished_at=now)
        state.awaiting_response=False
        if decision=='resume':
            state.status='active';state.paused_reason=None
            # Do NOT use native resume(), which truncates the delay to five seconds.
            from hermes_cli.loops import self_paced_floor_seconds
            state.next_due_at=now+(state.current_delay or state.interval_seconds or self_paced_floor_seconds())
        elif decision=='finish':
            state.status='done';state.last_stop_reason='recovery: '+evidence
        else:mgr.pause('recovery held: '+evidence)
        # A later interrupted business tick belongs to a new generation, and old
        # recovery receipts can never change this state again.
        if decision!='hold':meta['generation']+=1
    elif op=='recovery_failed':
        if r['status'] not in {'claimed','injecting','started'}:return store.result(state,meta,stale=True)
        # Only the caller's positive local proof of no dispatch permits a retry.
        retry=req.get('not_started') is True and r['status']=='claimed' and r['attempts']<2
        r.update(status='pending' if retry else 'failed',result=str(req.get('reason','recovery failed'))[:4000],finished_at=now)
    else:raise ValueError('Unsupported recovery operation')
    store.save(db,req['session_id'],mgr.state,meta)
    return store.result(mgr.state,meta,authorized=True)
