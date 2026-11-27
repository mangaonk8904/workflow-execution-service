"""API keys and the principal behind each request.

Keys are random bearer tokens; only their SHA-256 is stored. Each key carries
a role and, unless it is an admin key, exactly one tenant. The tenant a
request acts on always comes from its key, never from the request body.

Roles
  admin     register workflows; act on any tenant (must name it explicitly)
  operator  start, cancel and retry runs; upload assets; read its tenant
  reviewer  complete human tasks; read its tenant
"""

from __future__ import annotations

import hashlib
import secrets
from dataclasses import dataclass

from psycopg_pool import ConnectionPool

ROLES = ("admin", "operator", "reviewer")


@dataclass(frozen=True)
class Principal:
    principal: str
    role: str
    tenant_id: str | None  # None only for admins

    @property
    def is_admin(self) -> bool:
        return self.role == "admin"

    def can_see(self, tenant_id: str) -> bool:
        return self.is_admin or tenant_id == self.tenant_id


def hash_key(key: str) -> str:
    return hashlib.sha256(key.encode()).hexdigest()


def create_api_key(
    pool: ConnectionPool, *, principal: str, role: str, tenant_id: str | None = None,
    key: str | None = None,
) -> str:
    """Create a key and return it. This is the only time the plaintext is available.

    Passing `key` registers a known value (used to bootstrap the first admin
    from the environment); doing so again is a no-op.
    """
    if role not in ROLES:
        raise ValueError(f"role must be one of {ROLES}")
    if role != "admin" and not tenant_id:
        raise ValueError("operator and reviewer keys need a tenant")
    if role == "admin":
        tenant_id = None
    if key is not None and len(key) < 24:
        raise ValueError("API keys must be at least 24 characters")
    key = key or "wes_" + secrets.token_urlsafe(32)
    with pool.connection() as conn:
        conn.execute(
            "INSERT INTO api_keys (key_sha256, tenant_id, principal, role) VALUES (%s, %s, %s, %s) "
            "ON CONFLICT (key_sha256) DO NOTHING",
            (hash_key(key), tenant_id, principal, role),
        )
    return key


def revoke_api_key(pool: ConnectionPool, key: str) -> bool:
    with pool.connection() as conn:
        cur = conn.execute(
            "UPDATE api_keys SET revoked_at = now() WHERE key_sha256 = %s AND revoked_at IS NULL",
            (hash_key(key),),
        )
        return cur.rowcount == 1


def authenticate(pool: ConnectionPool, key: str) -> Principal | None:
    if not key:
        return None
    with pool.connection() as conn:
        row = conn.execute(
            "SELECT principal, role, tenant_id FROM api_keys "
            "WHERE key_sha256 = %s AND revoked_at IS NULL",
            (hash_key(key),),
        ).fetchone()
    return Principal(row["principal"], row["role"], row["tenant_id"]) if row else None
