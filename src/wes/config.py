from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    database_url: str = "postgresql://postgres:postgres@localhost:5432/wes"
    # Soft cap on concurrently running steps per tenant (noisy-neighbour guard).
    tenant_max_running: int = 50
    # Upper bound on children a single foreach step may create.
    max_fan_out: int = 10_000
    lease_seconds: float = 30.0
    poll_interval: float = 0.5
    webhook_url: str | None = None

    @classmethod
    def from_env(cls) -> "Settings":
        env = os.environ
        return cls(
            database_url=env.get("WES_DATABASE_URL", cls.database_url),
            tenant_max_running=int(env.get("WES_TENANT_MAX_RUNNING", cls.tenant_max_running)),
            max_fan_out=int(env.get("WES_MAX_FAN_OUT", cls.max_fan_out)),
            lease_seconds=float(env.get("WES_LEASE_SECONDS", cls.lease_seconds)),
            poll_interval=float(env.get("WES_POLL_INTERVAL", cls.poll_interval)),
            webhook_url=env.get("WES_WEBHOOK_URL") or None,
        )
