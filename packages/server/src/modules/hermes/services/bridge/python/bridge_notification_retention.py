"""Metadata-only retention maintenance on the existing trusted Unix bridge.

No worker discovery/start, profile filesystem traversal, task dispatch or retire
operation. Only an exact previously acknowledged/released obsolete claim may go.
"""
from __future__ import annotations

import os
import json
from pathlib import Path
import time


def process_start(pid):
    # comm may contain spaces and parentheses; field 22 follows the final ')'.
    return Path(f'/proc/{int(pid)}/stat').read_text().rsplit(')', 1)[1].split()[19]


def audit_worker(snapshot, phase):
    import fcntl
    from bridge_runtime import _hermes_home
    directory = _hermes_home() / 'logs' / 'notification-retention'
    directory.mkdir(parents=True, exist_ok=True, mode=0o700)
    path = directory / (snapshot['worker_generation'] + '.jsonl')
    record = {'schema_version': 'notification-retention/v1', 'phase': phase,
              'observed_at': time.time(), 'pid': os.getpid(), 'pid_start': process_start(os.getpid()),
              'snapshot': snapshot}
    fd = os.open(path, os.O_WRONLY | os.O_APPEND | os.O_CREAT | os.O_NOFOLLOW, 0o600)
    with os.fdopen(fd, 'a') as stream:
        fcntl.flock(stream, fcntl.LOCK_EX)
        stream.write(json.dumps(record, sort_keys=True) + '\n')
        stream.flush()
        os.fsync(stream.fileno())
    directory_fd = os.open(directory, os.O_DIRECTORY)
    try:
        os.fsync(directory_fd)
    finally:
        os.close(directory_fd)


def worker_identity(broker, worker):
    from bridge_transport import _socket_identity
    identity = _socket_identity(worker.endpoint)
    if (not worker.running or identity is None or identity != worker._endpoint_identity):
        raise ValueError('worker_identity_unavailable')
    return {'broker_generation': broker._notification_generation,
            'worker_key': worker.key, 'profile': worker.profile, 'pid': worker.pid,
            'pid_start': process_start(worker.pid), 'socket_identity': list(identity)}


def remember(broker, key, profile, session):
    broker._notification_receipts[key] = (profile, session)
    while len(broker._notification_receipts) > 512:
        broker._notification_receipts.pop(next(iter(broker._notification_receipts)))


def _request(worker, request):
    if worker.model_run_auth is not None:
        request = {**request, 'model_run_auth': worker.model_run_auth.descriptor()}
    return worker.request(request, timeout=2, allow_start=False)


def _busy(broker, key):
    return (broker._stop.is_set() or key in broker._draining or broker._inflight.get(key)
            or bool(broker._running_run_profile)
            or key in broker._approval_worker_key.values()
            or key in broker._clarify_worker_key.values()
            or key in broker._compression_worker_key.values())


def handle_retention(broker, request):
    if not broker.endpoint.startswith('ipc://'):
        raise ValueError('local_ipc_only')
    if not broker._notification_maintenance.acquire(blocking=False):
        return {'candidates': [], 'reconciled': False, 'reason': 'maintenance_busy'}
    try:
        if request['action'] == 'notification_retention_reconcile':
            return _reconcile(broker, request.get('expected'))
        candidates = []
        with broker._lock:
            keys = sorted(k for k in broker._workers if k > str(request.get('after') or ''))
        for key in keys[:32]:
            with broker._lock:
                worker = broker._workers.get(key)
                if worker is None or _busy(broker, key):
                    continue
                if time.monotonic() - worker.last_used_at <= broker.IDLE_TIMEOUT_SECONDS:
                    continue
                routes = {k: r for k, r in broker._background_claim_routes.items() if r[3] is worker}
                try:
                    with worker._lock:
                        binding = worker_identity(broker, worker)
                        response = _request(worker, {'action': 'notification_retention_scan', 'claim_keys': sorted(routes)[:64]})
                        if worker_identity(broker, worker) != binding:
                            raise ValueError('worker_changed')
                    for item in response['candidates']:
                        claim_key = (item.get('delegation_id'), item.get('claim_id'))
                        route = routes.get(claim_key)
                        if broker._background_claim_bindings.get(claim_key) != binding:
                            candidates.append({'worker': binding, 'obsolete': False, 'reason': 'worker_changed'})
                            continue
                        if route is None or (route[0], route[2]) != (item.get('profile'), item.get('session_id')):
                            candidates.append({'worker': binding, 'obsolete': False, 'reason': 'route_mismatch'})
                            continue
                        candidates.append({**item, 'worker': binding})
                    if response.get('truncated'):
                        candidates.append({'worker': binding, 'obsolete': False, 'reason': 'claim_limit'})
                except Exception as exc:
                    candidates.append({'worker_key': key, 'obsolete': False,
                                       'reason': 'diagnostic_error', 'error_type': type(exc).__name__})
        return {'schema_version': 'notification-retention/v1', 'candidates': candidates,
                'next': keys[31] if len(keys) > 32 else None}
    finally:
        broker._notification_maintenance.release()


def _reconcile(broker, expected):
    if not isinstance(expected, dict) or not isinstance(expected.get('worker'), dict) or not isinstance(expected.get('snapshot'), dict):
        return {'reconciled': False, 'reason': 'partial_identity'}
    binding, snapshot = expected['worker'], expected['snapshot']
    key = binding.get('worker_key')
    claim_key = (snapshot.get('delegation_id'), snapshot.get('claim_id'))
    with broker._lock:
        worker = broker._workers.get(key)
        route = broker._background_claim_routes.get(claim_key)
        if (worker is None or route is None or route[3] is not worker
                or (route[0], route[2]) != (snapshot.get('profile'), snapshot.get('session_id'))):
            # A successful past repair can be retried, but not rebound after restart.
            prior = broker._notification_reconciled.get(claim_key)
            if prior == expected:
                return {'reconciled': True, 'reason': 'already_reconciled'}
            return {'reconciled': False, 'reason': 'route_changed'}
        if _busy(broker, key) or time.monotonic() - worker.last_used_at <= broker.IDLE_TIMEOUT_SECONDS:
            return {'reconciled': False, 'reason': 'broker_responsibility'}
        try:
            # Freeze admission, object, process and endpoint throughout the worker's
            # locked durable verification + CAS. No route is popped on an error.
            with worker._lock:
                if (worker_identity(broker, worker) != binding
                        or broker._background_claim_bindings.get(claim_key) != binding):
                    return {'reconciled': False, 'reason': 'worker_changed'}
                result = _request(worker, {'action': 'notification_retention_reconcile', 'expected': snapshot})
                if (worker_identity(broker, worker) != binding
                        or broker._background_claim_routes.get(claim_key) is not route):
                    return {'reconciled': False, 'reason': 'stale_snapshot'}
                if result.get('ok') is False or result.get('obsolete') is not True or result.get('snapshot') != snapshot:
                    return {'reconciled': False, 'reason': result.get('reason', 'worker_refused')}
                broker._notification_reconciled[claim_key] = expected
                while len(broker._notification_reconciled) > 512:
                    broker._notification_reconciled.pop(next(iter(broker._notification_reconciled)))
                broker._background_claim_routes.pop(claim_key)
                broker._background_claim_bindings.pop(claim_key, None)
                remember(broker, claim_key, route[0], route[2])
                return {'reconciled': True, 'reason': 'obsolete_delivered'}
        except Exception as exc:
            return {'reconciled': False, 'reason': 'reconcile_error', 'error_type': type(exc).__name__}
