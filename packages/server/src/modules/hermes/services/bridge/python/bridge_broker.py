from __future__ import annotations

import atexit
import json
import os
import socket
import sys
import threading
import time
from contextlib import contextmanager
from collections import deque
from pathlib import Path
from typing import Any

from bridge_runtime import _install_stop_signal_handlers, _jsonable
from bridge_model_run_auth import ModelRunAuth
from bridge_transport import (
    WorkerProcess,
    MAINTENANCE_ACTIONS,
    _socket_identity,
    _unlink_owned_endpoint,
    _make_listen_socket,
    _read_json_request,
    _worker_endpoint,
    _write_json_response,
)

class BridgeBroker:
    IDLE_TIMEOUT_SECONDS = 30 * 60
    GC_INTERVAL_SECONDS = 60

    def __init__(self, endpoint: str, agent_root: str | None = None, hermes_home: str | None = None) -> None:
        self.endpoint = endpoint
        self.agent_root = agent_root
        self.hermes_home = hermes_home
        self._workers: dict[str, WorkerProcess] = {}
        self._run_profile: dict[str, str] = {}
        self._run_worker_key: dict[str, str] = {}
        self._running_run_profile: dict[str, str] = {}
        self._running_run_worker_key: dict[str, str] = {}
        self._session_profile: dict[str, str] = {}
        self._session_worker_key: dict[str, str] = {}
        self._approval_profile: dict[str, str] = {}
        self._approval_worker_key: dict[str, str] = {}
        self._clarify_profile: dict[str, str] = {}
        self._clarify_worker_key: dict[str, str] = {}
        self._compression_profile: dict[str, str] = {}
        self._compression_worker_key: dict[str, str] = {}
        # Claims are issued by a worker, never by the latest session route.
        self._background_claim_routes: dict[tuple[str, str], tuple[str, str, str, Any]] = {}
        self._lock = threading.RLock()
        self._stop = threading.Event()
        self._last_gc = time.monotonic()
        self._inflight: dict[str, int] = {}
        self._draining: set[str] = set()
        self._gc_thread: threading.Thread | None = None
        self._lifecycle_events: deque[dict[str, Any]] = deque(maxlen=128)

    def _normalize_profile(self, value: Any) -> str:
        profile = str(value or "").strip()
        return profile or "default"

    def _normalize_worker_key(self, profile: str, value: Any = None) -> str:
        worker_key = str(value or "").strip()
        return worker_key or profile

    def _worker_for_profile(self, profile: str, worker_key: str | None = None) -> WorkerProcess:
        profile = self._normalize_profile(profile)
        key = self._normalize_worker_key(profile, worker_key)
        with self._lock:
            if self._stop.is_set():
                raise RuntimeError("broker stopped")
            if key in self._draining:
                raise RuntimeError("worker draining")
            worker = self._workers.get(key)
            if worker is None:
                worker = WorkerProcess(key, profile, _worker_endpoint(key, self.endpoint), self.agent_root, self.hermes_home)
                self._workers[key] = worker
        return worker

    @contextmanager
    def _admit(self, profile: str, key: str, *, create: bool = False):
        with self._lock:
            if self._stop.is_set() or key in self._draining:
                raise RuntimeError("broker stopped or worker draining")
            worker = self._worker_for_profile(profile, key) if create else self._workers.get(key)
            if worker is None:
                raise RuntimeError("worker is not loaded")
            self._inflight[key] = self._inflight.get(key, 0) + 1
        try:
            yield worker
        finally:
            with self._lock:
                self._inflight[key] -= 1
                if not self._inflight[key]:
                    self._inflight.pop(key, None)

    def _route_for_run(self, run_id: str) -> tuple[str, str | None]:
        with self._lock:
            profile = self._run_profile.get(run_id)
            worker_key = self._run_worker_key.get(run_id)
        if not profile:
            raise KeyError(f"unknown run: {run_id}")
        return profile, worker_key

    def _route_for_session(self, session_id: str, fallback_profile: Any = None, worker_key: Any = None) -> tuple[str, str | None]:
        with self._lock:
            profile = self._session_profile.get(session_id)
            stored_worker_key = self._session_worker_key.get(session_id)
        if not profile:
            fallback = self._normalize_profile(fallback_profile)
            if fallback_profile is not None and fallback:
                return fallback, self._normalize_worker_key(fallback, worker_key)
            raise KeyError(f"unknown session: {session_id}")
        return profile, self._normalize_worker_key(profile, worker_key) if worker_key is not None else stored_worker_key

    def _record_response_routes(self, profile: str, worker_key: str, resp: dict[str, Any]) -> None:
        run_id = str(resp.get("run_id") or "")
        session_id = str(resp.get("session_id") or "")
        with self._lock:
            if run_id:
                self._run_profile[run_id] = profile
                self._run_worker_key[run_id] = worker_key
                if resp.get("status") == "running":
                    self._running_run_profile[run_id] = profile
                    self._running_run_worker_key[run_id] = worker_key
                else:
                    self._running_run_profile.pop(run_id, None)
                    self._running_run_worker_key.pop(run_id, None)
            if session_id:
                self._session_profile[session_id] = profile
                self._session_worker_key[session_id] = worker_key
            for event in resp.get("events") or []:
                if not isinstance(event, dict):
                    continue
                approval_id = str(event.get("approval_id") or "")
                if approval_id:
                    self._approval_profile[approval_id] = profile
                    self._approval_worker_key[approval_id] = worker_key
                clarify_id = str(event.get("clarify_id") or "")
                if clarify_id:
                    self._clarify_profile[clarify_id] = profile
                    self._clarify_worker_key[clarify_id] = worker_key
                request_id = str(event.get("request_id") or "")
                if event.get("event") == "bridge.compression.requested" and request_id:
                    self._compression_profile[request_id] = profile
                    self._compression_worker_key[request_id] = worker_key
                if event.get("event") in {"bridge.compression.completed", "bridge.compression.failed"} and request_id:
                    self._compression_profile.pop(request_id, None)
                    self._compression_worker_key.pop(request_id, None)

    def stop(self) -> None:
        self._stop.set()
        with self._lock:
            workers = list(self._workers.values())
            self._workers.clear()
            self._run_profile.clear()
            self._run_worker_key.clear()
            self._running_run_profile.clear()
            self._running_run_worker_key.clear()
            self._session_profile.clear()
            self._session_worker_key.clear()
            self._approval_profile.clear()
            self._approval_worker_key.clear()
            self._clarify_profile.clear()
            self._clarify_worker_key.clear()
            self._compression_profile.clear()
            self._compression_worker_key.clear()
            self._background_claim_routes.clear()
        for worker in workers:
            worker.stop()

    def _forward(self, profile: str, req: dict[str, Any], worker_key: str | None = None, *, record_routes: bool = True) -> dict[str, Any]:
        profile = self._normalize_profile(profile)
        key = self._normalize_worker_key(profile, worker_key)
        create = req.get("action") not in MAINTENANCE_ACTIONS
        # Durable notification discovery has a single unbound profile reader;
        # it must never start a cached model-run generation.
        if req.get("action") == "background_poll" and key == profile:
            create = True
        with self._admit(profile, key, create=create) as worker:
            forwarded = dict(req)
            request_session = str(req.get("session_id") or "")
            if request_session and req.get("action") in {"chat", "context_estimate"}:
                with self._lock:
                    self._session_profile[request_session] = profile
                    self._session_worker_key[request_session] = key
            if key.startswith("model-run-v1-"):
                binding = req.get("model_run_auth")
                if binding is not None:
                    bound = ModelRunAuth.parse(binding, profile)
                    if bound.worker_key() != key:
                        raise ValueError("model_run_auth route mismatch")
                    worker.bind_model_run_auth(bound)
                elif getattr(worker, "model_run_auth", None) is None:
                    raise ValueError("bound worker cannot restart without its original descriptor")
                forwarded["model_run_auth"] = worker.model_run_auth.descriptor()
            forwarded["profile"] = profile
            forwarded.pop("worker_key", None)
            try:
                resp = worker.request(forwarded, self._worker_request_timeout(req), allow_start=create)
                routed_resp = resp
                response_session = str(resp.get("session_id") or "")
                with self._lock:
                    current_key = self._session_worker_key.get(response_session)
                    if current_key and current_key != key:
                        routed_resp = {**resp}
                        routed_resp.pop("session_id", None)
                    if resp.get("type") == "loop":
                        routed_resp = {**resp}
                        routed_resp.pop("session_id", None)
                    if record_routes and not self._stop.is_set() and self._workers.get(key) is worker:
                        self._record_response_routes(profile, key, routed_resp)
                return resp
            except RuntimeError as e:
                # Worker returned ok=false or connection error — return error response
                return {"ok": False, "error": str(e)}

    def _worker_request_timeout(self, req: dict[str, Any]) -> float:
        try:
            timeout = float(req.get("timeout", 0) or 0)
        except (TypeError, ValueError):
            timeout = 0
        if timeout <= 0:
            return WorkerProcess.REQUEST_TIMEOUT_SECONDS
        return max(WorkerProcess.REQUEST_TIMEOUT_SECONDS, timeout + 10)

    def _status_if_loaded(self, req: dict[str, Any]) -> dict[str, Any]:
        session_id = str(req.get("session_id") or "")
        with self._lock:
            profile = self._session_profile.get(session_id)
            worker_key = self._session_worker_key.get(session_id)
            if profile:
                key = self._normalize_worker_key(profile, req.get("worker_key")) if "worker_key" in req else worker_key
            else:
                fallback_profile = req.get("profile")
                if fallback_profile is None:
                    return {"ok": True, "session_id": session_id, "exists": False, "running": False, "loaded": False}
                profile = self._normalize_profile(fallback_profile)
                key = self._normalize_worker_key(profile, req.get("worker_key") if "worker_key" in req else None)
            worker = self._workers.get(key or profile)

        if worker is None or not getattr(worker, "running", False):
            return {"ok": True, "session_id": session_id, "exists": False, "running": False, "loaded": False}

        resp = self._forward(profile, {**req, "action": "status"}, key or profile, record_routes=False)
        resp.setdefault("loaded", resp.get("ok") is not False)
        return resp

    def handle(self, req: dict[str, Any]) -> dict[str, Any]:
        action = str(req.get("action") or "").strip()
        if not action:
            raise ValueError("action is required")

        if action == "ping":
            with self._lock:
                worker_details = {
                    key: {
                        "running": worker.running,
                        "pid": worker.pid,
                        "endpoint": worker.endpoint,
                        "profile": getattr(worker, "profile", key),
                        "last_used_at": worker.last_used_at,
                    }
                    for key, worker in self._workers.items()
                }
                workers = {key: details["running"] for key, details in worker_details.items()}
                sessions_by_profile: dict[str, int] = {}
                for profile in self._session_profile.values():
                    sessions_by_profile[profile] = sessions_by_profile.get(profile, 0) + 1
                running_sessions_by_profile: dict[str, int] = {}
                for profile in self._running_run_profile.values():
                    running_sessions_by_profile[profile] = running_sessions_by_profile.get(profile, 0) + 1
                active_sessions = len(self._session_profile)
                running_sessions = len(self._running_run_profile)
            return {
                "pong": True,
                "time": time.time(),
                "mode": "broker",
                "broker": {
                    "pid": os.getpid(),
                    "endpoint": self.endpoint,
                },
                "workers": workers,
                "worker_details": worker_details,
                "active_sessions": active_sessions,
                "running_sessions": running_sessions,
                "sessions_by_profile": sessions_by_profile,
                "running_sessions_by_profile": running_sessions_by_profile,
            }

        if self._stop.is_set() and action != "shutdown":
            raise RuntimeError("broker stopped")

        if action == "lifecycle_snapshot":
            # Existing local IPC trust, with full worker binding on the hop;
            # no HTTP route, profile enumeration, or credential reads.
            profile = self._normalize_profile(req.get("profile"))
            key = self._normalize_worker_key(profile, req.get("worker_key"))
            binding = None
            if key.startswith("model-run-v1-") and req.get("model_run_auth") is not None:
                binding = ModelRunAuth.parse(req.get("model_run_auth"), profile)
                if binding.worker_key() != key:
                    raise ValueError("model_run_auth route mismatch")
            elif key.startswith("model-run-v1-"):
                # Same local IPC trust boundary as worker_ping: the operator
                # supplies an opaque generation key, never a private descriptor.
                # TCP callers must still supply the original full binding.
                if not self.endpoint.startswith("ipc://"):
                    raise ValueError("complete model_run_auth is required for TCP diagnostics")
            elif key != profile or req.get("model_run_auth") is not None:
                raise ValueError("diagnostic route override forbidden")
            with self._lock:
                worker = self._workers.get(key)
                if worker is not None and binding is None and key.startswith("model-run-v1-"):
                    binding = worker.model_run_auth
                if worker is not None and (worker.profile != profile or worker.model_run_auth != binding):
                    raise ValueError("diagnostic worker binding mismatch")
                metadata = {"worker_key": key, "broker_pid": os.getpid(),
                    "loaded": worker is not None and worker.running,
                    "pid": worker.pid if worker is not None else None,
                    "business_idle_seconds": max(0, time.monotonic() - worker.last_used_at) if worker is not None else None,
                    "broker_counts": {"inflight": self._inflight.get(key, 0),
                        "draining": int(key in self._draining),
                        "notification_claim": sum(route[3] is worker for route in self._background_claim_routes.values()) if worker is not None else 0},
                    "events": [dict(event) for event in self._lifecycle_events if event["worker_key"] == key]}
            if not metadata["loaded"]:
                return metadata
            # Admission pins the existing object while observing; it cannot start
            # one or rewrite session/run routes, even if the worker returns them.
            with self._admit(profile, key) as admitted_worker:
                if admitted_worker is not worker:
                    raise RuntimeError("worker changed during lifecycle snapshot")
                forwarded = {"action": action}
                if binding is not None:
                    forwarded["model_run_auth"] = binding.descriptor()
                response = admitted_worker.request(forwarded, timeout=2, allow_start=False)
            return {**metadata, "retired": response["retired"],
                "reasons": response["reasons"], "counts": response["counts"]}

        if action == "worker_ping":
            profile = self._normalize_profile(req.get("profile"))
            worker_key = self._normalize_worker_key(profile, req.get("worker_key"))
            resp = self._forward(profile, {"action": "ping"}, worker_key)
            resp["worker_profile"] = profile
            resp["worker_key"] = worker_key
            return resp

        if action == "chat":
            profile = self._normalize_profile(req.get("profile"))
            binding = ModelRunAuth.parse(req.get("model_run_auth"), profile)
            key = binding.worker_key()
            if req.get("worker_key") is not None and req["worker_key"] != key:
                raise ValueError("model_run_auth worker_key mismatch")
            return self._forward(profile, req, key)

        if action in {"loop_list", "loop_recover"}:
            from bridge_loops import LoopStore
            base = Path(self.hermes_home or os.environ['HERMES_HOME'])
            loops = []
            for profile in dict.fromkeys(req.get('profiles') or []):
                profile = self._normalize_profile(profile)
                if profile != 'default' and (Path(profile).name != profile or profile in {'.', '..'}):
                    raise ValueError('Invalid loop profile')
                home = base if profile == 'default' else base / 'profiles' / profile
                store = LoopStore(home)
                for row in store.operation({'op':'list','include_inflight':action == 'loop_recover'})['loops']:
                    if row['route'].get('profile') != profile:
                        continue
                    if action == 'loop_recover':
                        store.operation({'op':'recover','session_id':row['session_id']})
                    else:
                        loops.append(row)
            return {'loops':loops}

        if action == 'loop_operation':
            if req.get('model_run_auth') is not None or req.get('worker_key') is not None:
                raise ValueError('loop operation route override forbidden')
            profile = self._normalize_profile(req.get('profile'))
            return self._forward(profile, req, profile)

        if action == "context_estimate":
            profile = self._normalize_profile(req.get("profile"))
            binding = ModelRunAuth.parse(req.get("model_run_auth"), profile)
            key = binding.worker_key()
            if req.get("worker_key") is not None and req["worker_key"] != key:
                raise ValueError("model_run_auth worker_key mismatch")
            return self._forward(profile, req, key)

        if action == "provider_credentials":
            profile = self._normalize_profile(req.get("profile"))
            return self._forward(profile, req, self._normalize_worker_key(profile, req.get("worker_key")))

        if action in {"get_result", "get_output", "result_ack"}:
            profile, worker_key = self._route_for_run(str(req.get("run_id") or ""))
            return self._forward(profile, req, worker_key)

        if action == "status_if_loaded":
            return self._status_if_loaded(req)

        if action == "background_poll":
            requested_routes: dict[str, set[str]] = {}
            for route in req.get("routes") or []:
                if not isinstance(route, dict):
                    continue
                profile = self._normalize_profile(route.get("profile"))
                requested_routes.setdefault(profile, set()).update(
                    str(session_id).strip()
                    for session_id in (route.get("session_ids") or [])
                    if str(session_id).strip()
                )
            for profile in requested_routes:
                self._worker_for_profile(profile)
            with self._lock:
                workers = list(self._workers.values())
            sessions: list[dict[str, Any]] = []
            notifications: list[dict[str, Any]] = []
            pending_count = 0
            for worker in workers:
                profile = getattr(worker, "profile", "default") or "default"
                if not worker.running:
                    if worker.key != profile or profile not in requested_routes:
                        continue
                try:
                    poll_req = {
                        "action": "background_poll",
                        "session_ids": sorted(requested_routes.get(profile, set())),
                    }
                    if getattr(worker, "model_run_auth", None) is not None:
                        poll_req["model_run_auth"] = worker.model_run_auth.descriptor()
                    response = self._forward(profile, poll_req, worker.key)
                    if response.get("ok") is False:
                        continue
                except Exception:
                    continue
                pending_count += int(response.get("pending_count") or 0)
                for session in response.get("sessions") or []:
                    if isinstance(session, dict):
                        sessions.append({**session, "profile": profile, "worker_key": worker.key})
                for notification in response.get("notifications") or []:
                    if isinstance(notification, dict):
                        claim_key = (str(notification.get("delegation_id") or ""), str(notification.get("claim_id") or ""))
                        if not all(claim_key):
                            continue
                        claim_route = (profile, worker.key, str(notification.get("session_id") or ""), worker)
                        with self._lock:
                            previous = self._background_claim_routes.setdefault(claim_key, claim_route)
                            if previous != claim_route:
                                continue
                        notifications.append({**notification, "profile": profile})
            return {
                "broker_id": str(os.getpid()),
                "worker_keys": [worker.key for worker in workers if worker.running],
                "pending_count": pending_count,
                "sessions": sessions,
                "notifications": notifications,
            }

        if action in {"background_notification_complete", "background_notification_release"}:
            claim_key = (str(req.get("delegation_id") or ""), str(req.get("claim_id") or ""))
            with self._lock:
                route = self._background_claim_routes.get(claim_key)
                if route is None:
                    raise KeyError("unknown background notification claim")
                profile, worker_key, session_id, worker = route
                if self._workers.get(worker_key) is not worker or not worker.running:
                    raise ValueError("background notification claim worker has ended")
            if str(req.get("session_id") or "") != session_id or (req.get("profile") is not None and self._normalize_profile(req["profile"]) != profile):
                raise ValueError("background notification claim session/profile mismatch")
            if req.get("worker_key") is not None or req.get("model_run_auth") is not None:
                raise ValueError("background notification claim route override forbidden")
            with self._lock:
                if self._background_claim_routes.get(claim_key) is not route:
                    raise KeyError("background notification claim already consumed")
                if self._workers.get(worker_key) is not worker or not worker.running:
                    raise ValueError("background notification claim worker has ended")
            resp = self._forward(profile, req, worker_key)
            outcome = "completed" if action == "background_notification_complete" else "released"
            if resp.get(outcome) is True:
                with self._lock:
                    if self._background_claim_routes.get(claim_key) is route:
                        self._background_claim_routes.pop(claim_key, None)
            return resp

        if action == "background_events_ack":
            profile = self._normalize_profile(req.get("profile"))
            key = self._normalize_worker_key(profile, req.get("worker_key"))
            with self._lock:
                worker = self._workers.get(key)
                if worker is None or worker.profile != profile:
                    raise KeyError("unknown background events worker")
            return self._forward(profile, req, key)

        if action == "command" and str(req.get("command") or "").strip().lower().split(maxsplit=1)[0:1] in (["/loop"], ["/proactive"]):
            # Control does not execute a model. Keep one short-lived profile
            # writer, independent of an old run's expiring identity/worker.
            # HTTP/socket authorization has already bound owner + session.
            if req.get("model_run_auth") is not None or req.get("worker_key") is not None:
                raise ValueError("loop control route override forbidden")
            profile = self._normalize_profile(req.get("profile"))
            response = self._forward(profile, req, profile)
            # _forward records routes for ordinary responses. A control receipt
            # must not replace the session's active model worker route.
            return response

        if action in {"interrupt", "request_boundary_interrupt", "cancel_boundary_interrupt", "steer", "command", "switch_session_model", "goal_evaluate", "goal_pause", "status", "get_history", "get_session_title", "destroy"}:
            session_id = str(req.get("session_id") or "")
            if req.get("model_run_auth") is not None:
                binding = ModelRunAuth.parse(req["model_run_auth"], self._normalize_profile(req.get("profile")))
                profile, worker_key = binding.profile, binding.worker_key()
                if req.get("worker_key") is not None and req["worker_key"] != worker_key:
                    raise ValueError("model_run_auth worker mismatch")
            else:
                profile, worker_key = self._route_for_session(session_id, req.get("profile"), req.get("worker_key") if "worker_key" in req else None)
            if action == "destroy" and (worker_key or "").startswith("model-run-v1-"):
                binding = ModelRunAuth.parse(req.get("model_run_auth"), profile)
                worker_key = binding.worker_key()
                if req.get("worker_key") is not None and req["worker_key"] != worker_key:
                    raise ValueError("model_run_auth cleanup worker mismatch")
            resp = self._forward(profile, req, worker_key)
            if action == "destroy":
                with self._lock:
                    if self._session_worker_key.get(session_id) == worker_key:
                        self._session_profile.pop(session_id, None)
                        self._session_worker_key.pop(session_id, None)
            return resp

        if action == "approval_respond":
            approval_id = str(req.get("approval_id") or "").strip()
            if not approval_id:
                raise ValueError("approval_id is required")
            with self._lock:
                profile = self._approval_profile.get(approval_id)
                worker_key = self._approval_worker_key.get(approval_id)
            if not profile:
                raise KeyError(f"unknown approval request: {approval_id}")
            session_id = str(req.get("session_id") or "").strip()
            requested_profile = str(req.get("profile") or "").strip()
            if not session_id or not requested_profile or self._normalize_profile(requested_profile) != profile:
                return {"approval_id": approval_id, "resolved": False, "reason": "binding_mismatch", "outcome": "binding_mismatch"}
            return self._forward(profile, req, worker_key)

        if action == "clarify_respond":
            clarify_id = str(req.get("clarify_id") or "").strip()
            if not clarify_id:
                raise ValueError("clarify_id is required")
            with self._lock:
                profile = self._clarify_profile.get(clarify_id)
                worker_key = self._clarify_worker_key.get(clarify_id)
            if not profile:
                raise KeyError(f"unknown clarify request: {clarify_id}")
            return self._forward(profile, req, worker_key)

        if action == "compression_respond":
            request_id = str(req.get("request_id") or "").strip()
            if not request_id:
                raise ValueError("request_id is required")
            with self._lock:
                profile = self._compression_profile.get(request_id)
                worker_key = self._compression_worker_key.get(request_id)
            if not profile:
                raise KeyError(f"unknown compression request: {request_id}")
            return self._forward(profile, req, worker_key)

        if action == "destroy_all":
            with self._lock:
                workers = list(self._workers.values())
                self._workers.clear()
                self._run_profile.clear()
                self._run_worker_key.clear()
                self._running_run_profile.clear()
                self._running_run_worker_key.clear()
                self._session_profile.clear()
                self._session_worker_key.clear()
                self._approval_profile.clear()
                self._approval_worker_key.clear()
                self._clarify_profile.clear()
                self._clarify_worker_key.clear()
                self._compression_profile.clear()
                self._compression_worker_key.clear()
            destroyed = 0
            with self._lock:
                self._background_claim_routes.clear()
            for worker in workers:
                try:
                    if worker.running:
                        resp = worker.request({"action": "destroy_all"})
                        destroyed += int(resp.get("destroyed") or 0)
                except Exception:
                    pass
                finally:
                    worker.stop()
            return {"destroyed": destroyed}

        if action == "destroy_profile":
            profile = self._normalize_profile(req.get("profile"))
            with self._lock:
                workers = [
                    worker
                    for key, worker in list(self._workers.items())
                    if getattr(worker, "profile", key) == profile
                ]
                for worker in workers:
                    self._workers.pop(worker.key, None)
                self._run_profile = {key: value for key, value in self._run_profile.items() if value != profile}
                self._run_worker_key = {key: value for key, value in self._run_worker_key.items() if key in self._run_profile}
                self._running_run_profile = {key: value for key, value in self._running_run_profile.items() if value != profile}
                self._running_run_worker_key = {key: value for key, value in self._running_run_worker_key.items() if key in self._running_run_profile}
                self._session_profile = {key: value for key, value in self._session_profile.items() if value != profile}
                self._session_worker_key = {key: value for key, value in self._session_worker_key.items() if key in self._session_profile}
                self._approval_profile = {key: value for key, value in self._approval_profile.items() if value != profile}
                self._approval_worker_key = {key: value for key, value in self._approval_worker_key.items() if key in self._approval_profile}
                self._clarify_profile = {key: value for key, value in self._clarify_profile.items() if value != profile}
                self._clarify_worker_key = {key: value for key, value in self._clarify_worker_key.items() if key in self._clarify_profile}
                self._compression_profile = {key: value for key, value in self._compression_profile.items() if value != profile}
                self._compression_worker_key = {key: value for key, value in self._compression_worker_key.items() if key in self._compression_profile}
                self._background_claim_routes = {key: route for key, route in self._background_claim_routes.items() if route[0] != profile}

            if not workers:
                return {"profile": profile, "destroyed": 0}

            destroyed = 0
            for worker in workers:
                if not worker.running:
                    worker.stop()
                    continue
                try:
                    resp = worker.request({"action": "destroy_all"})
                    destroyed += int(resp.get("destroyed") or 0)
                except Exception:
                    pass
                finally:
                    worker.stop()
            return {"profile": profile, "destroyed": destroyed}

        if action == "list":
            sessions: list[Any] = []
            with self._lock:
                workers = list(self._workers.items())
            for key, worker in workers:
                if not worker.running:
                    continue
                try:
                    resp = worker.request({"action": "list"})
                    for session in resp.get("sessions") or []:
                        if isinstance(session, dict):
                            session.setdefault("profile", getattr(worker, "profile", key))
                            session.setdefault("worker_key", key)
                        sessions.append(session)
                except Exception:
                    pass
            return {"sessions": sessions}

        if action == "shutdown":
            self.stop()
            return {"status": "shutting_down"}

        # ───── MCP Management ─────
        if action.startswith("mcp_"):
            profile = self._normalize_profile(req.get("profile"))
            return self._forward(profile, req)

        raise ValueError(f"unknown action: {action}")

    def _make_server_socket(self) -> socket.socket:
        if self.endpoint.startswith("ipc://") and sys.platform.startswith("linux"):
            from bridge_broker_socket import make_broker_socket
            return make_broker_socket(self.endpoint)
        return _make_listen_socket(self.endpoint)

    def _read_request(self, conn: socket.socket) -> dict[str, Any]:
        return _read_json_request(conn)

    def _write_response(self, conn: socket.socket, resp: dict[str, Any]) -> None:
        _write_json_response(conn, resp)

    def _handle_connection(self, conn: socket.socket) -> None:
        try:
            try:
                req = self._read_request(conn)
                data = self.handle(req)
                resp = {"ok": True, **_jsonable(data)}
            except Exception as exc:
                resp = {
                    "ok": False,
                    "error": str(exc),
                    "error_type": exc.__class__.__name__,
                }
            self._write_response(conn, resp)
        except Exception as exc:
            print(f"[hermes-bridge-broker] connection error: {exc}", file=sys.stderr, flush=True)
        finally:
            try:
                conn.close()
            except OSError:
                pass

    def _forget_worker(self, key: str, worker: WorkerProcess) -> None:
        # Caller holds registry lock; never erase routes moved to a newer worker.
        if self._workers.get(key) is not worker:
            return
        self._workers.pop(key, None)
        for keys, profiles in (
            (self._run_worker_key, self._run_profile),
            (self._running_run_worker_key, self._running_run_profile),
            (self._session_worker_key, self._session_profile),
            (self._approval_worker_key, self._approval_profile),
            (self._clarify_worker_key, self._clarify_profile),
            (self._compression_worker_key, self._compression_profile),
        ):
            for identity, route in list(keys.items()):
                if route == key:
                    keys.pop(identity, None)
                    profiles.pop(identity, None)
        self._background_claim_routes = {claim: route for claim, route in self._background_claim_routes.items()
                                         if route[3] is not worker}

    def _gc_idle_workers(self) -> None:
        now = time.monotonic()
        with self._lock:
            if self._stop.is_set() or now - self._last_gc < self.GC_INTERVAL_SECONDS:
                return
            self._last_gc = now
            candidates = list(self._workers.items())
        for key, worker in candidates:
            with self._lock:
                if (self._stop.is_set() or self._workers.get(key) is not worker
                    or key in self._draining or self._inflight.get(key)
                    or now - worker.last_used_at <= self.IDLE_TIMEOUT_SECONDS
                    or any(route[3] is worker for route in self._background_claim_routes.values())):
                    continue
                self._draining.add(key)
            retired = False
            try:
                if worker.running:
                    req = {"action": "prepare_retire"}
                    if worker.model_run_auth is not None:
                        req["model_run_auth"] = worker.model_run_auth.descriptor()
                    response = worker.request(req, allow_start=False)
                    retired = response.get("retired") is True
                else:
                    # Death is NOT successful drain. Forget the dead object and
                    # stale routes, never manufacture an empty replacement run.
                    retired = True
                if retired:
                    pid = worker.pid
                    was_running = worker.running
                    worker.stop()
                    with self._lock:
                        if self._workers.get(key) is worker:
                            self._forget_worker(key, worker)
                            self._lifecycle_events.append({"worker_key": key, "pid": pid,
                                "time": time.time(), "business_idle_seconds": max(0, now - worker.last_used_at),
                                "outcome": "retired" if was_running else "dead_forgotten"})
            except Exception as exc:
                print(f"[hermes-bridge-broker] drain refused key={key}: {type(exc).__name__}", file=sys.stderr, flush=True)
            finally:
                with self._lock:
                    self._draining.discard(key)

    def _schedule_gc(self) -> None:
        with self._lock:
            if self._stop.is_set() or time.monotonic() - self._last_gc < self.GC_INTERVAL_SECONDS:
                return
            if self._gc_thread is not None and self._gc_thread.is_alive():
                return
            self._gc_thread = threading.Thread(target=self._gc_idle_workers, daemon=True, name="hermes-bridge-gc")
            self._gc_thread.start()

    def serve_forever(self) -> None:
        server = self._make_server_socket()
        socket_identity = _socket_identity(self.endpoint)
        restore_signals = _install_stop_signal_handlers(self._stop)
        atexit.register(self.stop)
        try:
            server.listen(64)
            server.settimeout(0.2)
            lease = getattr(server, '_broker_lease', None)
            if lease is not None:
                socket_identity = lease.public_identity()
            print(json.dumps({"event": "ready", "endpoint": self.endpoint, "mode": "broker", "socket_identity": socket_identity}), flush=True)

            while not self._stop.is_set():
                self._schedule_gc()
                try:
                    try:
                        conn, _addr = server.accept()
                    except socket.timeout:
                        self._schedule_gc()
                        continue
                    threading.Thread(
                        target=self._handle_connection,
                        args=(conn,),
                        daemon=True,
                        name="hermes-bridge-broker-connection",
                    ).start()
                except KeyboardInterrupt:
                    break
                except Exception as exc:
                    print(f"[hermes-bridge-broker] server loop error: {exc}", file=sys.stderr, flush=True)
        finally:
            restore_signals()
            try:
                atexit.unregister(self.stop)
            except Exception:
                pass
            self.stop()
            leased = getattr(server, '_broker_lease', None) is not None
            server.close()
            if not leased:
                _unlink_owned_endpoint(self.endpoint, socket_identity)
