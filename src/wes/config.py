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
    # Asset bytes: a local directory, or s3://bucket (with s3_endpoint for MinIO).
    blob_url: str = "file://./blobstore"
    s3_endpoint: str | None = None
    s3_region: str | None = None
    s3_create_bucket: bool = False
    max_asset_bytes: int = 5 * 1024**3
    # Discarded assets are deleted (and their blobs, if unshared) after this long.
    asset_gc_grace_seconds: float = 3600.0
    # If set, an admin API key with this value is created at startup.
    bootstrap_admin_key: str | None = None

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
            blob_url=env.get("WES_BLOB_URL", cls.blob_url),
            s3_endpoint=env.get("WES_S3_ENDPOINT") or None,
            s3_region=env.get("WES_S3_REGION") or None,
            s3_create_bucket=env.get("WES_S3_CREATE_BUCKET", "").lower() in ("1", "true", "yes"),
            max_asset_bytes=int(env.get("WES_MAX_ASSET_BYTES", cls.max_asset_bytes)),
            asset_gc_grace_seconds=float(
                env.get("WES_ASSET_GC_GRACE_SECONDS", cls.asset_gc_grace_seconds)
            ),
            bootstrap_admin_key=env.get("WES_BOOTSTRAP_ADMIN_KEY") or None,
        )

    def catalog(self, pool):
        """The asset catalog these settings describe."""
        from .assets import AssetCatalog
        from .blobstore import make_blobstore

        store = make_blobstore(self.blob_url, s3_endpoint=self.s3_endpoint,
                               s3_region=self.s3_region, s3_create_bucket=self.s3_create_bucket)
        return AssetCatalog(pool, store, max_bytes=self.max_asset_bytes)
