"""Local bridge metadata retention; each validated candidate is rechecked by its owner."""
from __future__ import annotations

from contextlib import contextmanager
import fcntl
import hashlib
import json
import math
import os
from pathlib import Path
import socket
import stat
import struct
import time
import uuid

SCHEMA = 'notification-retention/v2'
PAGE_SCHEMA = 'notification-retention/v1'
MAX_AGE = 8 * 3600
AUTH_FIELDS = ('schema_version', 'observed_at', 'candidates', 'errors', 'partial', 'stop_reason')


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(',', ':')).encode()).hexdigest()


def atomic_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    temporary = path.with_name('.' + uuid.uuid4().hex + '.tmp')
    try:
        with temporary.open('x', encoding='utf-8') as stream:
            os.chmod(temporary, 0o600)
            json.dump(value, stream, sort_keys=True)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        fd = os.open(path.parent, os.O_DIRECTORY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
    finally:
        temporary.unlink(missing_ok=True)


@contextmanager
def locked(path):
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    fd = os.open(path, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
    with os.fdopen(fd, 'r+') as stream:
        fcntl.flock(stream, fcntl.LOCK_EX)
        yield


def rpc(root, request):
    config = Path(root) / 'notification-retention-config.json'
    endpoint = os.environ.get('HERMES_AGENT_BRIDGE_ENDPOINT')
    if config.exists():
        endpoint = json.loads(config.read_text())['endpoint']
    if not endpoint or not endpoint.startswith('ipc://'):
        raise ValueError('local_endpoint_not_configured')
    path = Path(endpoint[6:])
    info = path.lstat()
    if not stat.S_ISSOCK(info.st_mode) or info.st_uid != os.getuid() or info.st_mode & 0o077:
        raise PermissionError('untrusted_local_socket')
    with socket.socket(socket.AF_UNIX) as conn:
        conn.settimeout(75 if request.get('action') == 'notification_retention_scan' else 5)
        conn.connect(str(path))
        _, uid, _ = struct.unpack('3i', conn.getsockopt(socket.SOL_SOCKET, socket.SO_PEERCRED, struct.calcsize('3i')))
        if uid != os.getuid():
            raise PermissionError('peer_uid_mismatch')
        conn.sendall((json.dumps(request) + '\n').encode())
        data = bytearray()
        while b'\n' not in data:
            part = conn.recv(65536)
            if not part or len(data) + len(part) > 8 * 1024 * 1024:
                raise ValueError('invalid_bridge_response')
            data.extend(part)
        response = json.loads(data.split(b'\n', 1)[0])
        if not isinstance(response, dict) or response.get('ok') is False:
            raise ValueError('bridge_refused')
        return response


def _text(value):
    return isinstance(value, str) and bool(value) and '\x00' not in value


def validate_candidate(item):
    if not isinstance(item, dict) or type(item.get('obsolete')) is not bool:
        raise ValueError('invalid_candidate')
    if 'reason' in item and not isinstance(item['reason'], str):
        raise ValueError('invalid_reason')
    if item['obsolete'] is False:
        return
    worker, snapshot = item.get('worker'), item.get('snapshot')
    if not isinstance(worker, dict) or not isinstance(snapshot, dict):
        raise ValueError('partial_identity')
    if any(not _text(worker.get(k)) for k in ('broker_generation', 'worker_key', 'profile', 'pid_start')):
        raise ValueError('partial_worker_identity')
    identity = worker.get('socket_identity')
    if (type(worker.get('pid')) is not int or worker['pid'] <= 0 or not isinstance(identity, list)
            or len(identity) != 2 or any(type(v) is not int or v < 0 for v in identity)):
        raise ValueError('invalid_worker_identity')
    if any(not _text(snapshot.get(k)) for k in ('delegation_id', 'claim_id', 'profile', 'session_id')):
        raise ValueError('partial_snapshot')
    if snapshot['profile'] != worker['profile']:
        raise ValueError('profile_mismatch')
    proof = snapshot.get('identity')
    if not _text(snapshot.get('worker_generation')) or not isinstance(proof, dict):
        raise ValueError('partial_snapshot_proof')
    for key in ('event_sha256', 'result_sha256', 'database'):
        value = proof.get(key)
        if not isinstance(value, str) or len(value) != 64 or any(c not in '0123456789abcdef' for c in value):
            raise ValueError('invalid_snapshot_digest')
    if (proof.get('session_id') != snapshot['session_id'] or not _text(proof.get('origin_session'))
            or not isinstance(proof.get('origin_ui_session_id'), str)):
        raise ValueError('invalid_snapshot_session')


def validate_page(page, after):
    if not isinstance(page, dict) or page.get('schema_version') != PAGE_SCHEMA:
        raise ValueError('diagnostic_unavailable')
    items = page.get('candidates')
    cursor = page.get('next')
    if not isinstance(items, list) or 'next' not in page:
        raise ValueError('invalid_page')
    if cursor is not None and (not _text(cursor) or cursor <= after):
        raise ValueError('invalid_page')
    for item in items:
        validate_candidate(item)
    return items, cursor


def _write(root, phase, data):
    data = {'schema_version': SCHEMA, 'phase': phase, **data}
    atomic_json(root / 'notification-retention' / f'{time.time_ns()}-{uuid.uuid4().hex}-{phase}.json', data)
    return data


def scan(root, *, request=None, now=None):
    root = Path(root)
    now = time.time() if now is None else now
    request = request or (lambda req: rpc(root, req))
    with locked(root / 'notification-retention.lock'):
        items, errors, after = [], [], ''
        stop_reason = 'complete'
        try:
            for _ in range(8):
                page, cursor = validate_page(request({'action': 'notification_retention_scan', 'after': after}), after)
                items.extend(page)
                if cursor is None:
                    break
                after = cursor
            else:
                stop_reason = 'worker_limit'
                errors.append({'reason': stop_reason})
        except Exception as exc:
            stop_reason = 'scan_error'
            errors.append({'reason': stop_reason, 'error_type': type(exc).__name__})
        data = {'schema_version': SCHEMA, 'observed_at': now, 'candidates': items,
                'errors': errors, 'partial': bool(errors), 'stop_reason': stop_reason}
        data['scan_id'] = digest(data)
        receipt = _write(root, 'scan', data)
        atomic_json(root / 'notification-retention-latest.json', receipt)
        return {'scan_id': data['scan_id'], 'candidate_count': len(items), 'error_count': len(errors),
                'partial': data['partial'], 'stop_reason': stop_reason}


def mechanical(root, *, request=None, now=None):
    root = Path(root)
    now = time.time() if now is None else now
    request = request or (lambda req: rpc(root, req))
    with locked(root / 'notification-retention.lock'):
        source = root / 'notification-retention-latest.json'
        if not source.exists():
            _write(root, 'apply', {'observed_at': now, 'reason': 'no_scan', 'results': []})
            return {'status': 'no_scan', 'reconciled': 0}
        results = []
        try:
            saved = json.loads(source.read_text())
            check = {k: saved[k] for k in AUTH_FIELDS}
            if saved.get('schema_version') != SCHEMA or digest(check) != saved.get('scan_id'):
                raise ValueError('scan_identity_changed')
            observed = saved['observed_at']
            if type(observed) not in (int, float) or not math.isfinite(observed) or not 0 <= now - observed <= MAX_AGE:
                raise ValueError('scan_expired')
            if (type(saved['partial']) is not bool or not isinstance(saved['errors'], list)
                    or saved['partial'] != bool(saved['errors']) or saved['stop_reason'] not in {'complete', 'worker_limit', 'scan_error'}
                    or (saved['stop_reason'] == 'complete') == saved['partial'] or not isinstance(saved['candidates'], list)):
                raise ValueError('invalid_scan')
            for candidate in saved['candidates']:
                validate_candidate(candidate)
            for candidate in saved['candidates']:
                if candidate['obsolete'] is False:
                    results.append({'before': candidate, 'after': None, 'reason': candidate.get('reason', 'protected')})
                    continue
                _write(root, 'intent', {'observed_at': now, 'scan_id': saved['scan_id'], 'before': candidate, 'outcome': 'not_confirmed'})
                try:
                    reply = request({'action': 'notification_retention_reconcile', 'expected': candidate})
                    after = {k: reply[k] for k in ('reconciled', 'reason', 'error_type') if k in reply}
                except Exception as exc:
                    after = {'reconciled': False, 'reason': 'rpc_error', 'error_type': type(exc).__name__}
                result = {'before': candidate, 'after': after, 'reason': after.get('reason', 'unknown')}
                _write(root, 'result', {'observed_at': now, 'scan_id': saved['scan_id'], **result})
                results.append(result)
            status = 'partial_checked' if saved['partial'] else 'checked'
        except Exception as exc:
            status = 'failed_closed'
            results.append({'reason': 'invalid_scan', 'error_type': type(exc).__name__})
        _write(root, 'apply', {'observed_at': now, 'results': results, 'status': status})
        return {'status': status, 'reconciled': sum(r['after'].get('reconciled') is True for r in results if r.get('after')),
                'count': len(results)}


def main(argv=None):
    import argparse
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('phase', choices=('scan', 'mechanical'))
    parser.add_argument('--root', type=Path, required=True)
    args = parser.parse_args(argv)
    root = args.root.expanduser().resolve(strict=True)
    if not root.is_dir():
        parser.error('--root must be an existing private output directory')
    result = (scan if args.phase == 'scan' else mechanical)(root)
    print(json.dumps(result, sort_keys=True))
    return 1 if result.get('status') == 'failed_closed' or result.get('partial') else 0


if __name__ == '__main__':
    raise SystemExit(main())
