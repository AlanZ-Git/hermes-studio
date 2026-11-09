"""Persistent recovery resource responsibility in the existing LoopStore metadata."""
from __future__ import annotations

import copy
import time

SETTLED = {'exited_proven', 'not_dispatched_proven'}


def records(meta):
    return [r for r in [meta.get('recovery'), *meta.get('recovery_history', [])] if r]


def public(record):
    result = copy.deepcopy(record)
    if result and result.get('resource'):
        result['resource'].pop('model_run_auth', None)
    return result


def enumerate_store(store):
    try:
        store.path.stat()
    except FileNotFoundError:
        return []
    import sqlite3
    with sqlite3.connect(f'file:{store.path}?mode=ro', uri=True) as db:
        rows = db.execute("SELECT key FROM state_meta WHERE key LIKE 'webui-loop:%'").fetchall()
        result = []
        for (key,) in rows:
            sid = key[len('webui-loop:'):]
            state, meta = store.read(db, sid)
            for r in records(meta):
                resource = r.get('resource')
                if resource and resource.get('outcome') not in SETTLED:
                    result.append({'session_id': sid, 'profile': (state.route or {}).get('profile') if state else None,
                                   'recovery_id': r['id'], 'expected_generation': r['generation'],
                                   'decision_revoked': r['generation'] != meta['generation'] or r.get('status') in {'failed', 'superseded'},
                                   'deadline_at': r.get('deadline_at'), **copy.deepcopy(resource)})
                elif not resource and (r.get('status') in {'injecting', 'started'} or r.get('deadline_at')):
                    result.append({'session_id': sid, 'profile': (state.route or {}).get('profile') if state else None,
                                   'recovery_id': r['id'], 'expected_generation': r['generation'],
                                   'phase': 'legacy_unknown', 'outcome': 'unknown', 'reason': 'legacy_dispatch_unproven'})
        return result


def persist_owner_exit(store, session_id, binding, run_id, observation):
    if observation.get('outcome') != 'exited_proven':
        return False
    with store.transaction() as db:
        state, meta = store.read(db, session_id)
        matches = [r for r in records(meta) if (r.get('resource') or {}).get('model_run_auth') == binding.descriptor()]
        if len(matches) != 1:
            return False
        recovery = matches[0]
        resource = recovery['resource']
        if (resource.get('profile') != binding.profile or resource.get('run_id') not in (None, run_id)
                or resource.get('phase') not in {'dispatch_admitted', 'started'}):
            return False
        if resource.get('outcome') in SETTLED:
            return resource['outcome'] == 'exited_proven'
        resource.update(run_id=run_id, phase='started', outcome='exited_proven',
                        last_observed_at=time.time(), evidence=observation)
        store.save(db, session_id, state, meta)
        return True


def operation(store, db, mgr, meta, req):
    r = next((r for r in records(meta) if r['id'] == req.get('recovery_id')
              and r['generation'] == req.get('expected_generation')), None)
    resource = r.get('resource') if r else None
    if (not resource or resource['reservation_id'] != req.get('reservation_id')
            or resource['profile'] != req.get('profile')):
        return store.result(mgr.state, meta, authorized=False, stale=True)
    op = req['op']
    if op == 'recovery_bind':
        from bridge_model_run_auth import ModelRunAuth
        auth = ModelRunAuth.parse(req.get('recovery_auth'), req['profile'])
        if not auth.run_marker.startswith('recovery_v1_'):
            raise ValueError('Recovery marker required')
        prior = resource.get('model_run_auth')
        if prior and prior != auth.descriptor():
            raise ValueError('Immutable recovery owner changed')
        if resource['phase'] != 'reserved' and not prior:
            return store.result(mgr.state, meta, authorized=False)
        resource.update(model_run_auth=auth.descriptor(), run_marker=auth.run_marker)
    elif op == 'recovery_dispatch':
        if resource['phase'] != 'dispatching' or resource.get('cancel_requested'):
            return store.result(mgr.state, meta, authorized=False)
        resource['phase'] = 'dispatch_admitted'
    elif op == 'recovery_resource_started':
        if not resource.get('model_run_auth') or not isinstance(req.get('run_id'), str) or not req['run_id']:
            return store.result(mgr.state, meta, authorized=False)
        if resource.get('run_id') and resource['run_id'] != req['run_id']:
            raise ValueError('Immutable recovery run changed')
        resource.update(run_id=req['run_id'], phase='started')
    elif op == 'recovery_cancel':
        resource.update(cancel_requested=True, reason=str(req.get('reason', 'Stopping recovery'))[:4000])
        if r.get('status') in {'claimed', 'injecting', 'started'}:
            r.update(status='failed', result='Recovery decision revoked; stopping original execution', finished_at=time.time())
    elif op == 'recovery_resource_observed':
        outcome = req['observation']['outcome']
        if resource.get('outcome') in SETTLED:
            return store.result(mgr.state, meta, authorized=True)
        resource.update(outcome=outcome, last_observed_at=time.time(), evidence=req['observation'])
    else:
        raise ValueError('Unsupported recovery resource operation')
    store.save(db, req['session_id'], mgr.state, meta)
    return store.result(mgr.state, meta, authorized=True)
