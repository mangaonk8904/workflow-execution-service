from __future__ import annotations

import os

import psycopg
import pytest

from wes import AssetCatalog, Engine, HandlerRegistry, LocalBlobStore, Worker, WorkflowSpec
from wes.db import make_pool, migrate

TEST_DB = os.environ.get(
    "WES_TEST_DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/wes_test"
)


@pytest.fixture(scope="session")
def pool():
    try:
        psycopg.connect(TEST_DB, connect_timeout=3).close()
    except psycopg.OperationalError as exc:
        pytest.skip(f"Postgres not reachable at WES_TEST_DATABASE_URL: {exc}")
    pool = make_pool(TEST_DB)
    migrate(pool)
    yield pool
    pool.close()


@pytest.fixture
def engine(pool):
    with pool.connection() as conn:
        conn.execute(
            "TRUNCATE asset_edges, assets, events, step_runs, runs, workflow_definitions, "
            "api_keys CASCADE"
        )
    return Engine(pool)


@pytest.fixture
def store(tmp_path):
    return LocalBlobStore(tmp_path / "blobs")


@pytest.fixture
def catalog(engine, store):
    return AssetCatalog(engine.pool, store)


def make_spec(name: str, steps: list[dict]) -> WorkflowSpec:
    for s in steps:  # tests should not sleep through backoff
        s.setdefault("retry", {"max_attempts": 3, "backoff_seconds": 0})
    return WorkflowSpec.model_validate({"name": name, "steps": steps})


def drain(
    engine: Engine, registry: HandlerRegistry, limit: int = 500,
    catalog: AssetCatalog | None = None,
) -> int:
    """Run tasks until the queue is empty. Returns how many were executed."""
    worker = Worker(engine, registry, catalog=catalog, worker_id="test-worker")
    n = 0
    while n < limit and worker.run_once():
        n += 1
    return n


def statuses(engine: Engine, run_id: str) -> dict[str, str]:
    return {s["step_id"]: s["status"] for s in engine.get_run(run_id)["steps"]}


def outputs(engine: Engine, run_id: str) -> dict[str, object]:
    return {s["step_id"]: s["output"] for s in engine.get_run(run_id)["steps"]}
