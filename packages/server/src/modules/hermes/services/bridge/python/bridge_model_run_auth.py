"""Immutable internal execution identity; token validation belongs to the API/MCP.

Descriptors originate at the authenticated server boundary, never in prompts.
This module does not read, renew, sign or replace credentials.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
import hashlib
import json
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class ModelRunAuth:
    version: int
    owner_id: str
    profile: str
    run_marker: str
    token_path: str

    @classmethod
    def parse(cls, value: Any, profile: str | None = None) -> ModelRunAuth:
        if isinstance(value, cls):
            value = asdict(value)
        fields = {"version", "owner_id", "profile", "run_marker", "token_path"}
        if not isinstance(value, dict) or set(value) != fields:
            raise ValueError("complete model_run_auth is required")
        if type(value["version"]) is not int or value["version"] != 1:
            raise ValueError("unsupported model_run_auth version")
        for key in fields - {"version"}:
            item = value[key]
            if not isinstance(item, str) or not item.strip() or item != item.strip() or "\x00" in item:
                raise ValueError("invalid model_run_auth field: " + key)
        if not Path(value["token_path"]).is_absolute():
            raise ValueError("model_run_auth token_path must be absolute")
        if profile is not None and value["profile"] != profile:
            raise ValueError("model_run_auth profile mismatch")
        return cls(**value)

    def descriptor(self) -> dict[str, Any]:
        return asdict(self)

    def serialized(self) -> str:
        return json.dumps(self.descriptor(), sort_keys=True, separators=(",", ":"))

    def worker_key(self) -> str:
        return "model-run-v1-" + hashlib.sha256(self.serialized().encode("utf-8")).hexdigest()
