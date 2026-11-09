"""Broker-owned serial recovery admission and exact, existing-worker-only cleanup."""
from __future__ import annotations

import fcntl
import os
from pathlib import Path
import threading
import time
import uuid

from bridge_loops import LoopStore
from bridge_model_run_auth import ModelRunAuth
from bridge_recovery_resources import enumerate_store, SETTLED


class RecoveryCoordinator:
    def __init__(self, broker):
        self.broker = broker
        self.gate = threading.RLock()
        self.lease = None
        self.closed = False
        self.admission_closed = False

    @property
    def base(self):
        return Path(self.broker.hermes_home or os.environ['HERMES_HOME'])

    def own(self):
        if self.closed:
            raise RuntimeError('Recovery admission closed')
        if self.lease is None:
            fd = os.open(self.base / '.studio-recovery-owner.lock', os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BaseException:
                os.close(fd)
                raise RuntimeError('Recovery already has a broker owner') from None
            self.lease = fd

    def stores(self):
        stores = [('default', LoopStore(self.base))]
        directory = self.base / 'profiles'
        try:
            directory.stat()
        except FileNotFoundError:
            return stores
        if not directory.is_dir() or directory.is_symlink():
            raise RuntimeError('Recovery profile enumeration incomplete')
        for entry in sorted(directory.iterdir()):
            if entry.is_symlink():
                raise RuntimeError('Recovery profile enumeration incomplete')
            if entry.is_dir():
                stores.append((entry.name, LoopStore(entry)))
        return stores

    def all(self):
        self.own()
        result = []
        for profile, store in self.stores():
            for resource in enumerate_store(store):
                if resource['profile'] != profile:
                    raise RuntimeError('Recovery profile binding unreadable')
                result.append(resource)
        return result

    def store(self, profile):
        if profile != 'default' and (Path(profile).name != profile or profile in {'.', '..'}):
            raise ValueError('Invalid recovery profile')
        return LoopStore(self.base if profile == 'default' else self.base / 'profiles' / profile)

    def operate(self, req):
        with self.gate:
            self.own()
            if req['op'] in {'recovery_resource_observed', 'recovery_dispatch'}:
                raise ValueError('Exit evidence belongs to the lifecycle owner')
            if req['op'] == 'recovery_claim':
                if self.admission_closed:
                    return {'authorized': False, 'reason': 'recovery_admission_closed'}
                resources = self.all()
                if len(resources) >= 2:
                    return {'authorized': False, 'reason': 'recovery_capacity_held'}
                req = {**req, 'reservation_id': uuid.uuid4().hex}
            result = self.store(req['profile']).operation(req)
            if req['op'] == 'recovery_claim' and result.get('authorized'):
                result['reservation_id'] = req['reservation_id']
            return result

    def check_dispatch(self, req):
        if req.get('model_run_auth') is None:
            return
        binding = ModelRunAuth.parse(req.get('model_run_auth'), req.get('profile'))
        if not binding.run_marker.startswith('recovery_v1_'):
            return
        with self.gate:
            resources = [r for r in self.all() if r.get('model_run_auth') == binding.descriptor()
                         and r['session_id'] == req.get('session_id')]
            if self.admission_closed or len(resources) != 1 or resources[0]['phase'] != 'dispatching' or resources[0].get('cancel_requested'):
                raise ValueError('Recovery dispatch reservation unavailable')
            resource = resources[0]
            result = self.store(resource['profile']).operation({**resource, 'op': 'recovery_dispatch'})
            if not result.get('authorized'):
                raise ValueError('Recovery dispatch reservation changed')

    def control(self, req, cancel=False):
        with self.gate:
            resources = self.all()
            resource = next((r for r in resources if r.get('reservation_id') == req.get('reservation_id')
                             and r['session_id'] == req.get('session_id') and r['profile'] == req.get('profile')
                             and r['recovery_id'] == req.get('recovery_id')
                             and r['expected_generation'] == req.get('expected_generation')), None)
            if resource is None:
                return {'outcome': 'unknown', 'reason': 'reservation_not_found'}
            auth = resource.get('model_run_auth')
            if req.get('model_run_auth') is not None and req['model_run_auth'] != auth:
                raise ValueError('Recovery cleanup auth mismatch')
            base = {'op': 'recovery_cancel', **{k: resource[k] for k in
                    ('session_id', 'profile', 'recovery_id', 'expected_generation', 'reservation_id')}}
            store = self.store(resource['profile'])
            expired = resource.get('deadline_at') is not None and time.time() >= resource['deadline_at']
            if cancel or expired or resource.get('decision_revoked'):
                store.operation({**base, 'reason': req.get('reason', 'Recovery decision revoked' if resource.get('decision_revoked') else 'Recovery deadline expired')})
                resource['cancel_requested'] = True
            if resource['phase'] in {'reserved', 'dispatching'} and resource.get('cancel_requested'):
                observation = {'outcome': 'not_dispatched_proven', 'reason': 'broker_fenced_before_dispatch_admission'}
            elif not auth:
                observation = {'outcome': 'unknown', 'reason': 'original_owner_binding_unavailable'}
            else:
                binding = ModelRunAuth.parse(auth, resource['profile'])
                key = binding.worker_key()
                with self.broker._lock:
                    worker = self.broker._workers.get(key)
                if worker is None or not worker.running or worker.model_run_auth != binding:
                    observation = {'outcome': 'unknown', 'reason': 'original_worker_unavailable'}
                else:
                    try:
                        with self.broker._admit(binding.profile, key) as existing:
                            if existing is not worker:
                                raise RuntimeError('Original worker changed')
                            forwarded = {'session_id': resource['session_id'], 'model_run_auth': auth,
                                         'expected_run_id': resource.get('run_id')}
                            observation = worker.request({**forwarded, 'action': 'observe_recovery_exit'}, timeout=1, allow_start=False)
                            late_run = observation.get('run_id')
                            if not resource.get('run_id') and late_run:
                                store.operation({**base, 'op': 'recovery_resource_started', 'run_id': late_run})
                                forwarded['expected_run_id'] = late_run
                            if resource.get('cancel_requested') and forwarded.get('expected_run_id'):
                                worker.request({**forwarded, 'action': 'request_boundary_interrupt'}, timeout=1, allow_start=False)
                    except Exception:
                        observation = {'outcome': 'unknown', 'reason': 'original_worker_observation_failed'}
            if observation.get('outcome') not in SETTLED | {'running', 'unknown'}:
                observation = {'outcome': 'unknown', 'reason': 'invalid_exit_observation'}
            store.operation({**base, 'op': 'recovery_resource_observed', 'observation': observation})
            return observation

    def close(self):
        with self.gate:
            self.closed = True
            if self.lease is not None:
                os.close(self.lease)
                self.lease = None
