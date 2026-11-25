from __future__ import annotations

from importlib import resources

from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool


def make_pool(database_url: str, *, min_size: int = 1, max_size: int = 10) -> ConnectionPool:
    """Connection pool with autocommit on; transitions open explicit transactions."""
    return ConnectionPool(
        database_url,
        min_size=min_size,
        max_size=max_size,
        kwargs={"row_factory": dict_row, "autocommit": True},
        open=True,
    )


def migrate(pool: ConnectionPool) -> list[str]:
    """Apply any unapplied SQL files from wes/migrations, in filename order."""
    applied: list[str] = []
    files = sorted(
        (f for f in resources.files("wes.migrations").iterdir() if f.name.endswith(".sql")),
        key=lambda f: f.name,
    )
    with pool.connection() as conn:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS schema_migrations ("
            "version text PRIMARY KEY, applied_at timestamptz NOT NULL DEFAULT now())"
        )
        for f in files:
            with conn.transaction():
                # Serialise concurrent migrators (api, worker and scheduler may start together).
                conn.execute("SELECT pg_advisory_xact_lock(727274)")
                done = conn.execute(
                    "SELECT 1 FROM schema_migrations WHERE version = %s", (f.name,)
                ).fetchone()
                if done:
                    continue
                conn.execute(f.read_text())
                conn.execute("INSERT INTO schema_migrations (version) VALUES (%s)", (f.name,))
                applied.append(f.name)
    return applied
