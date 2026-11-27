"""The orchestrator: every state transition of a run, each in one Postgres transaction.

Concurrency model
-----------------
* Transitions that change a run's shape (complete, fail, cancel, retry, human
  completion) take a row lock on the run first, so they are serialised per run.
* Claiming a task only locks the step row (FOR UPDATE SKIP LOCKED), so workers
  never queue behind each other or behind the per-run lock. The run lock is
  FOR NO KEY UPDATE so the foreign-key check on a claim's event insert does
  not wait on it either.
* `attempt` is a fencing token. It is bumped on every claim; a result is only
  accepted from the worker that holds the current attempt.
* Events are written in the same transaction as the state change (outbox).
* Assets a step stages are committed in the same transaction that accepts its
  result, and discarded by every path that does not (see wes.assets).
"""

from __future__ import annotations

import random
import uuid
from dataclasses import dataclass
from typing import Any, Callable

from psycopg.types.json import Jsonb
from psycopg_pool import ConnectionPool

from . import assets
from .errors import Conflict, Invalid, NotFound
from .models import StepSpec, WorkflowSpec

DONE = ("succeeded", "skipped")
OPEN = ["pending", "ready", "waiting_human", "fanned_out"]

__all__ = ["Conflict", "Engine", "Invalid", "NotFound", "Task"]


@dataclass
class Task:
    """A claimed unit of work handed to a worker."""

    run_id: str
    step_id: str
    attempt: int
    step_type: str
    tenant_id: str
    run_input: dict[str, Any]
    deps: dict[str, Any]
    params: dict[str, Any]
    item: Any = None
    item_index: int | None = None
    policy_transforms: dict[str, str] | None = None


def _resolve(path: str, outputs: dict[str, Any]) -> Any:
    """'step.key.sub' -> outputs['step']['key']['sub'], or None if anything is missing."""
    head, *rest = path.split(".")
    value = outputs.get(head)
    for key in rest:
        if not isinstance(value, dict):
            return None
        value = value.get(key)
    return value


class Engine:
    def __init__(
        self,
        pool: ConnectionPool,
        *,
        tenant_max_running: int = 50,
        max_fan_out: int = 10_000,
    ) -> None:
        self.pool = pool
        self.tenant_max_running = tenant_max_running
        self.max_fan_out = max_fan_out
        # Definitions are immutable per (name, version), so caching is safe.
        self._spec_cache: dict[tuple[str, int], WorkflowSpec] = {}

    # ------------------------------------------------------------------ definitions

    def register_workflow(self, spec: WorkflowSpec) -> int:
        """Store a definition and return its version.

        Versions are immutable. Re-registering an identical spec returns the
        existing version; any change creates the next version. Runs pin the
        version they started on.
        """
        body = spec.model_dump(mode="json")
        with self.pool.connection() as conn, conn.transaction():
            conn.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", (spec.name,))
            latest = conn.execute(
                "SELECT version, spec FROM workflow_definitions WHERE name = %s "
                "ORDER BY version DESC LIMIT 1",
                (spec.name,),
            ).fetchone()
            if latest and latest["spec"] == body:
                return latest["version"]
            version = (latest["version"] + 1) if latest else 1
            conn.execute(
                "INSERT INTO workflow_definitions (name, version, spec) VALUES (%s, %s, %s)",
                (spec.name, version, Jsonb(body)),
            )
            return version

    def get_workflow(self, name: str, version: int | None = None) -> tuple[int, WorkflowSpec]:
        with self.pool.connection() as conn:
            if version is None:
                row = conn.execute(
                    "SELECT version, spec FROM workflow_definitions WHERE name = %s "
                    "ORDER BY version DESC LIMIT 1",
                    (name,),
                ).fetchone()
            else:
                row = conn.execute(
                    "SELECT version, spec FROM workflow_definitions WHERE name = %s AND version = %s",
                    (name, version),
                ).fetchone()
        if not row:
            raise NotFound(f"workflow '{name}'" + (f" v{version}" if version else "") + " not found")
        spec = WorkflowSpec.model_validate(row["spec"])
        self._spec_cache[(name, row["version"])] = spec
        return row["version"], spec

    def list_workflows(self) -> list[dict[str, Any]]:
        """Latest version of every registered workflow, by name."""
        with self.pool.connection() as conn:
            return conn.execute(
                "SELECT DISTINCT ON (name) name, version, spec->>'description' AS description, "
                "jsonb_array_length(spec->'steps') AS step_count, "
                "count(*) OVER (PARTITION BY name) AS version_count, created_at "
                "FROM workflow_definitions ORDER BY name, version DESC"
            ).fetchall()

    def _spec(self, name: str, version: int) -> WorkflowSpec:
        cached = self._spec_cache.get((name, version))
        return cached if cached else self.get_workflow(name, version)[1]

    # ------------------------------------------------------------------ runs

    def start_run(
        self,
        tenant_id: str,
        workflow: str,
        input: dict[str, Any] | None = None,
        *,
        version: int | None = None,
        idempotency_key: str | None = None,
    ) -> tuple[str, bool]:
        """Create a run and schedule its root steps. Returns (run_id, created).

        With an idempotency key, a repeated call returns the original run and
        created=False instead of starting a second one.
        """
        version, spec = self.get_workflow(workflow, version)
        run_id = uuid.uuid4()
        with self.pool.connection() as conn, conn.transaction():
            # Inputs may name assets ({"$asset": id}); they must be this tenant's.
            error = assets.check_refs(conn, tenant_id, assets.asset_refs(input or {}))
            if error:
                raise Invalid(error.replace("output", "run input", 1))
            run = conn.execute(
                "INSERT INTO runs (id, tenant_id, workflow_name, workflow_version, status, input, "
                "idempotency_key) VALUES (%s, %s, %s, %s, 'running', %s, %s) "
                "ON CONFLICT (tenant_id, idempotency_key) WHERE idempotency_key IS NOT NULL "
                "DO NOTHING RETURNING *",
                (run_id, tenant_id, workflow, version, Jsonb(input or {}), idempotency_key),
            ).fetchone()
            if run is None:
                existing = conn.execute(
                    "SELECT id FROM runs WHERE tenant_id = %s AND idempotency_key = %s",
                    (tenant_id, idempotency_key),
                ).fetchone()
                return str(existing["id"]), False
            with conn.cursor() as cur:
                cur.executemany(
                    "INSERT INTO step_runs (run_id, step_id, tenant_id, step_type, kind, status, "
                    "max_attempts, backoff_seconds, max_backoff_seconds, timeout_seconds, sla_seconds) "
                    "VALUES (%s, %s, %s, %s, %s, 'pending', %s, %s, %s, %s, %s)",
                    [
                        (
                            run_id, s.id, tenant_id, s.handler, s.kind, s.retry.max_attempts,
                            s.retry.backoff_seconds, s.retry.max_backoff_seconds,
                            s.timeout_seconds, s.sla_seconds,
                        )
                        for s in spec.steps
                    ],
                )
            self._emit(conn, run, "run.started", payload={"workflow": workflow, "version": version})
            self._advance(conn, run)
        return str(run_id), True

    def get_run(self, run_id: str) -> dict[str, Any]:
        with self.pool.connection() as conn:
            run = conn.execute("SELECT * FROM runs WHERE id = %s", (run_id,)).fetchone()
            if not run:
                raise NotFound(f"run {run_id} not found")
            steps = conn.execute(
                "SELECT step_id, parent_step_id, item_index, step_type, kind, status, attempt, "
                "max_attempts, available_at, lease_owner, lease_expires_at, sla_due_at, escalated_at, "
                "remaining_children, output, error, handler, started_at, finished_at "
                "FROM step_runs WHERE run_id = %s",
                (run_id,),
            ).fetchall()
        # Present steps in definition order, children under their parent.
        order = {
            s.id: i
            for i, s in enumerate(self._spec(run["workflow_name"], run["workflow_version"]).steps)
        }
        steps.sort(
            key=lambda s: (
                order.get(s["parent_step_id"] or s["step_id"], len(order)),
                -1 if s["item_index"] is None else s["item_index"],
            )
        )
        run["steps"] = steps
        return run

    def list_runs(
        self,
        tenant_id: str | None = None,
        status: str | None = None,
        workflow: str | None = None,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        """Most recent runs first, with top-level step progress for each."""
        with self.pool.connection() as conn:
            return conn.execute(
                "SELECT r.id, r.tenant_id, r.workflow_name, r.workflow_version, r.status, r.error, "
                "r.created_at, r.updated_at, r.finished_at, "
                "p.steps_done, p.steps_total, p.waiting_human "
                "FROM runs r CROSS JOIN LATERAL ("
                "  SELECT count(*) FILTER (WHERE s.status IN ('succeeded', 'skipped')) AS steps_done, "
                "         count(*) AS steps_total, "
                "         coalesce(bool_or(s.status = 'waiting_human'), false) AS waiting_human "
                "  FROM step_runs s WHERE s.run_id = r.id AND s.parent_step_id IS NULL"
                ") p "
                "WHERE (%(tenant)s::text IS NULL OR r.tenant_id = %(tenant)s) "
                "AND (%(status)s::text IS NULL OR r.status = %(status)s) "
                "AND (%(workflow)s::text IS NULL OR r.workflow_name = %(workflow)s) "
                "ORDER BY r.created_at DESC LIMIT %(limit)s",
                {"tenant": tenant_id, "status": status, "workflow": workflow, "limit": limit},
            ).fetchall()

    def list_events(self, run_id: str, after_id: int = 0, limit: int = 500) -> list[dict[str, Any]]:
        with self.pool.connection() as conn:
            return conn.execute(
                "SELECT id, run_id, step_id, type, payload, created_at FROM events "
                "WHERE run_id = %s AND id > %s ORDER BY id LIMIT %s",
                (run_id, after_id, limit),
            ).fetchall()

    def cancel_run(self, run_id: str) -> bool:
        """Cancel a running run. Returns False if it had already finished."""
        with self.pool.connection() as conn, conn.transaction():
            run = self._lock_run(conn, run_id)
            if run["status"] != "running":
                return False
            conn.execute(
                "UPDATE runs SET status = 'cancelled', finished_at = now(), updated_at = now() "
                "WHERE id = %s",
                (run_id,),
            )
            self._cancel_open_steps(conn, run_id)
            self._emit(conn, run, "run.cancelled")
            return True

    def retry_run(self, run_id: str) -> None:
        """Resume a failed run from the failed steps. Work that succeeded is kept."""
        with self.pool.connection() as conn, conn.transaction():
            run = self._lock_run(conn, run_id)
            if run["status"] != "failed":
                raise Conflict(f"run is {run['status']}; only failed runs can be retried")
            # Fan-out children: redo only the ones that did not succeed.
            conn.execute(
                "UPDATE step_runs SET status = 'ready', attempt = 0, error = NULL, "
                "available_at = now(), lease_owner = NULL, lease_expires_at = NULL, "
                "deadline_at = NULL, finished_at = NULL, updated_at = now() "
                "WHERE run_id = %s AND parent_step_id IS NOT NULL AND status IN ('failed', 'cancelled')",
                (run_id,),
            )
            parents = conn.execute(
                "SELECT s.step_id, "
                "  (SELECT count(*) FROM step_runs c WHERE c.run_id = s.run_id "
                "     AND c.parent_step_id = s.step_id) AS children, "
                "  (SELECT count(*) FROM step_runs c WHERE c.run_id = s.run_id "
                "     AND c.parent_step_id = s.step_id AND c.status <> 'succeeded') AS unfinished "
                "FROM step_runs s WHERE s.run_id = %s AND s.parent_step_id IS NULL "
                "AND s.status IN ('failed', 'cancelled')",
                (run_id,),
            ).fetchall()
            for p in parents:
                if p["children"] and p["unfinished"]:
                    conn.execute(
                        "UPDATE step_runs SET status = 'fanned_out', remaining_children = %s, "
                        "error = NULL, finished_at = NULL, updated_at = now() "
                        "WHERE run_id = %s AND step_id = %s",
                        (p["unfinished"], run_id, p["step_id"]),
                    )
                elif p["children"]:
                    self._join_children(conn, run, p["step_id"])
                else:
                    conn.execute(
                        "UPDATE step_runs SET status = 'pending', attempt = 0, error = NULL, "
                        "output = NULL, available_at = now(), lease_owner = NULL, "
                        "lease_expires_at = NULL, deadline_at = NULL, sla_due_at = NULL, "
                        "escalated_at = NULL, finished_at = NULL, updated_at = now() "
                        "WHERE run_id = %s AND step_id = %s",
                        (run_id, p["step_id"]),
                    )
            conn.execute(
                "UPDATE runs SET status = 'running', error = NULL, finished_at = NULL, "
                "updated_at = now() WHERE id = %s",
                (run_id,),
            )
            run["status"] = "running"
            self._emit(conn, run, "run.retried")
            self._advance(conn, run)

    # ------------------------------------------------------------------ worker protocol

    def claim(
        self, worker_id: str, step_types: list[str], lease_seconds: float = 30.0
    ) -> Task | None:
        """Lease one ready task of the given types, or return None.

        Fairness: tenants with fewer running steps go first, and a tenant at
        its concurrency cap is skipped. The cap is soft: two workers claiming at
        the same instant can overshoot it by one.
        """
        if not step_types:
            return None
        with self.pool.connection() as conn, conn.transaction():
            picked = conn.execute(
                """
                WITH busy AS (
                    SELECT tenant_id, count(*) AS n FROM step_runs
                    WHERE status = 'running' GROUP BY tenant_id
                )
                SELECT s.run_id, s.step_id
                FROM step_runs s
                LEFT JOIN busy b ON b.tenant_id = s.tenant_id
                WHERE s.status = 'ready'
                  AND s.available_at <= clock_timestamp()
                  AND s.step_type = ANY(%s)
                  AND coalesce(b.n, 0) < %s
                ORDER BY coalesce(b.n, 0), s.available_at
                LIMIT 1
                FOR UPDATE OF s SKIP LOCKED
                """,
                (step_types, self.tenant_max_running),
            ).fetchone()
            if not picked:
                return None
            step = conn.execute(
                """
                UPDATE step_runs SET
                    status = 'running',
                    attempt = attempt + 1,
                    lease_owner = %s,
                    lease_expires_at = clock_timestamp() + make_interval(secs => %s),
                    deadline_at = CASE WHEN timeout_seconds IS NULL THEN NULL
                        ELSE clock_timestamp() + make_interval(secs => timeout_seconds) END,
                    started_at = clock_timestamp(),
                    updated_at = now()
                WHERE run_id = %s AND step_id = %s
                RETURNING *
                """,
                (worker_id, lease_seconds, picked["run_id"], picked["step_id"]),
            ).fetchone()
            run = conn.execute("SELECT * FROM runs WHERE id = %s", (step["run_id"],)).fetchone()
            spec = self._spec(run["workflow_name"], run["workflow_version"])
            step_spec = spec.step(step["parent_step_id"] or step["step_id"])
            deps = {
                r["step_id"]: r["output"]
                for r in conn.execute(
                    "SELECT step_id, output FROM step_runs WHERE run_id = %s AND step_id = ANY(%s)",
                    (step["run_id"], step_spec.depends_on),
                ).fetchall()
            }
            self._emit(
                conn, run, "step.started", step["step_id"],
                {"attempt": step["attempt"], "worker": worker_id},
            )
        return Task(
            run_id=str(step["run_id"]),
            step_id=step["step_id"],
            attempt=step["attempt"],
            step_type=step["step_type"],
            tenant_id=step["tenant_id"],
            run_input=run["input"],
            deps=deps,
            params=step_spec.params,
            item=step["item"],
            item_index=step["item_index"],
            policy_transforms=step_spec.policy_transforms,
        )

    def heartbeat(
        self, run_id: str, step_id: str, attempt: int, worker_id: str, lease_seconds: float = 30.0
    ) -> bool:
        """Extend the lease. False means the lease was lost and the work should stop."""
        with self.pool.connection() as conn:
            cur = conn.execute(
                "UPDATE step_runs SET lease_expires_at = clock_timestamp() + make_interval(secs => %s) "
                "WHERE run_id = %s AND step_id = %s AND status = 'running' "
                "AND attempt = %s AND lease_owner = %s",
                (lease_seconds, run_id, step_id, attempt, worker_id),
            )
            return cur.rowcount == 1

    def complete_step(
        self, run_id: str, step_id: str, attempt: int, worker_id: str,
        output: dict[str, Any] | None = None, *, handler: dict[str, Any] | None = None,
    ) -> bool:
        """Record a result. Returns False if it was stale and has been ignored.

        The attempt's staged assets are committed here, atomically with the
        result. If the output references assets it may not, or an asset fails
        the step's `requires_policy` gate, the step fails permanently instead.
        `handler` ({"name", "version", "models"}) is recorded as provenance.
        """
        with self.pool.connection() as conn, conn.transaction():
            run, step = self._lock_attempt(conn, run_id, step_id, attempt, worker_id)
            if step is None:
                return False
            step_spec = self._spec(run["workflow_name"], run["workflow_version"]).step(
                step["parent_step_id"] or step["step_id"]
            )
            committed, error = assets.commit_attempt(
                conn, run, step, output, step_spec.requires_policy
            )
            if error:
                self._fail(conn, run, step, error, retryable=False)
                return True
            if handler is not None:
                conn.execute(
                    "UPDATE step_runs SET handler = %s WHERE run_id = %s AND step_id = %s",
                    (Jsonb(handler), run_id, step_id),
                )
            for a in committed:
                self._emit(conn, run, "asset.committed", step_id, a)
            extra = {"handler": handler} if handler else {}
            if committed:
                extra["assets"] = [a["id"] for a in committed]
            self._succeed(conn, run, step, output, extra=extra)
            return True

    def fail_step(
        self, run_id: str, step_id: str, attempt: int, worker_id: str, error: str,
        *, retryable: bool = True,
    ) -> bool:
        """Record a failure: retry with backoff, or fail the run when attempts are exhausted."""
        with self.pool.connection() as conn, conn.transaction():
            run, step = self._lock_attempt(conn, run_id, step_id, attempt, worker_id)
            if step is None:
                return False
            self._fail(conn, run, step, error, retryable)
            return True

    # ------------------------------------------------------------------ human steps

    def list_human_tasks(self, tenant_id: str | None = None, limit: int = 100) -> list[dict[str, Any]]:
        with self.pool.connection() as conn:
            return conn.execute(
                "SELECT s.run_id, s.step_id, s.tenant_id, s.step_type, s.sla_due_at, s.escalated_at, "
                "s.started_at AS waiting_since, r.workflow_name "
                "FROM step_runs s JOIN runs r ON r.id = s.run_id "
                "WHERE s.status = 'waiting_human' AND (%s::text IS NULL OR s.tenant_id = %s) "
                "ORDER BY s.sla_due_at NULLS LAST, s.started_at LIMIT %s",
                (tenant_id, tenant_id, limit),
            ).fetchall()

    def complete_human_step(
        self, run_id: str, step_id: str, output: dict[str, Any] | None = None,
        *, actor: str | None = None,
    ) -> None:
        with self.pool.connection() as conn, conn.transaction():
            run = self._lock_run(conn, run_id)
            step = conn.execute(
                "SELECT * FROM step_runs WHERE run_id = %s AND step_id = %s FOR UPDATE",
                (run_id, step_id),
            ).fetchone()
            if not step:
                raise NotFound(f"step '{step_id}' not found in run {run_id}")
            if run["status"] != "running" or step["status"] != "waiting_human":
                raise Conflict(
                    f"step is {step['status']} and run is {run['status']}; nothing to complete"
                )
            error = assets.check_refs(conn, run["tenant_id"], assets.asset_refs(output))
            if error:
                raise Invalid(error)
            self._succeed(conn, run, step, output, extra={"actor": actor})

    # ------------------------------------------------------------------ background duties

    def reap(self, limit: int = 100) -> int:
        """Fail attempts whose lease expired or that ran past their timeout.

        This is what recovers from a worker that died mid-step. The failure goes
        through the normal retry policy.
        """
        with self.pool.connection() as conn:
            candidates = conn.execute(
                "SELECT run_id, step_id, attempt FROM step_runs WHERE status = 'running' "
                "AND (lease_expires_at < clock_timestamp() OR deadline_at < clock_timestamp()) "
                "LIMIT %s",
                (limit,),
            ).fetchall()
        reaped = 0
        for c in candidates:
            with self.pool.connection() as conn, conn.transaction():
                run = self._lock_run(conn, c["run_id"])
                step = conn.execute(
                    "SELECT *, (deadline_at < clock_timestamp()) AS timed_out, "
                    "(lease_expires_at < clock_timestamp()) AS lease_expired "
                    "FROM step_runs WHERE run_id = %s AND step_id = %s FOR UPDATE",
                    (c["run_id"], c["step_id"]),
                ).fetchone()
                if (
                    step["status"] != "running"
                    or step["attempt"] != c["attempt"]
                    or not (step["timed_out"] or step["lease_expired"])
                ):
                    continue  # finished or re-heartbeated since we looked
                if run["status"] != "running":
                    self._mark_cancelled(conn, step)
                else:
                    reason = "timeout exceeded" if step["timed_out"] else "lease expired (worker lost)"
                    self._fail(conn, run, step, reason, retryable=True)
                reaped += 1
        return reaped

    def escalate_overdue_human_tasks(self) -> int:
        """Emit step.sla_breached once for each human task past its SLA."""
        with self.pool.connection() as conn, conn.transaction():
            rows = conn.execute(
                "UPDATE step_runs SET escalated_at = now(), updated_at = now() "
                "WHERE status = 'waiting_human' AND sla_due_at < clock_timestamp() "
                "AND escalated_at IS NULL RETURNING run_id, step_id, tenant_id, sla_due_at"
            ).fetchall()
            for r in rows:
                conn.execute(
                    "INSERT INTO events (run_id, step_id, tenant_id, type, payload) "
                    "VALUES (%s, %s, %s, 'step.sla_breached', %s)",
                    (r["run_id"], r["step_id"], r["tenant_id"],
                     Jsonb({"sla_due_at": r["sla_due_at"].isoformat()})),
                )
            return len(rows)

    def relay_outbox(self, sink: Callable[[dict[str, Any]], None], batch: int = 100) -> int:
        """Publish unpublished events to `sink`, oldest first, then mark them published.

        Delivery is at-least-once: if the sink raises, the batch rolls back and
        is retried. Run a single relay (or partition by run) to keep order.
        """
        with self.pool.connection() as conn, conn.transaction():
            events = conn.execute(
                "SELECT id, run_id, step_id, tenant_id, type, payload, created_at FROM events "
                "WHERE published_at IS NULL ORDER BY id LIMIT %s FOR UPDATE SKIP LOCKED",
                (batch,),
            ).fetchall()
            for e in events:
                sink(e)
            if events:
                conn.execute(
                    "UPDATE events SET published_at = now() WHERE id = ANY(%s)",
                    ([e["id"] for e in events],),
                )
            return len(events)

    # ------------------------------------------------------------------ internals

    def _lock_run(self, conn, run_id) -> dict[str, Any]:
        # NO KEY UPDATE, not UPDATE: inserts into step_runs/events take a KEY SHARE
        # lock on the run row for their foreign key. A plain FOR UPDATE would make
        # claim() wait on this lock while holding a step row, which can deadlock
        # with a completion for the same run.
        run = conn.execute(
            "SELECT * FROM runs WHERE id = %s FOR NO KEY UPDATE", (run_id,)
        ).fetchone()
        if not run:
            raise NotFound(f"run {run_id} not found")
        return run

    def _lock_attempt(self, conn, run_id, step_id, attempt, worker_id):
        """Lock run then step, and check the caller still owns this attempt."""
        run = self._lock_run(conn, run_id)
        step = conn.execute(
            "SELECT * FROM step_runs WHERE run_id = %s AND step_id = %s FOR UPDATE",
            (run_id, step_id),
        ).fetchone()
        if (
            not step
            or step["status"] != "running"
            or step["attempt"] != attempt
            or step["lease_owner"] != worker_id
        ):
            return run, None  # stale: a newer attempt owns the step, or it was reaped
        if run["status"] != "running":
            self._mark_cancelled(conn, step)  # run was cancelled or failed underneath us
            return run, None
        return run, step

    def _emit(self, conn, run, type_: str, step_id: str | None = None, payload: dict | None = None):
        conn.execute(
            "INSERT INTO events (run_id, step_id, tenant_id, type, payload) VALUES (%s, %s, %s, %s, %s)",
            (run["id"], step_id, run["tenant_id"], type_, Jsonb(payload or {})),
        )

    def _mark_cancelled(self, conn, step) -> None:
        assets.discard_attempt(conn, step["run_id"], step["step_id"], step["attempt"])
        conn.execute(
            "UPDATE step_runs SET status = 'cancelled', lease_owner = NULL, lease_expires_at = NULL, "
            "deadline_at = NULL, finished_at = now(), updated_at = now() "
            "WHERE run_id = %s AND step_id = %s",
            (step["run_id"], step["step_id"]),
        )

    def _cancel_open_steps(self, conn, run_id) -> None:
        # Steps that are mid-flight stay 'running'; their result is rejected
        # and they are marked cancelled when the worker reports or the lease expires.
        conn.execute(
            "UPDATE step_runs SET status = 'cancelled', finished_at = now(), updated_at = now() "
            "WHERE run_id = %s AND status = ANY(%s)",
            (run_id, OPEN),
        )

    def _succeed(self, conn, run, step, output, extra: dict | None = None) -> None:
        conn.execute(
            "UPDATE step_runs SET status = 'succeeded', output = %s, error = NULL, "
            "lease_owner = NULL, lease_expires_at = NULL, deadline_at = NULL, "
            "finished_at = now(), updated_at = now() WHERE run_id = %s AND step_id = %s",
            (Jsonb(output), step["run_id"], step["step_id"]),
        )
        self._emit(
            conn, run, "step.succeeded", step["step_id"],
            {"attempt": step["attempt"], **(extra or {})},
        )
        if step["parent_step_id"]:
            remaining = conn.execute(
                "UPDATE step_runs SET remaining_children = remaining_children - 1, updated_at = now() "
                "WHERE run_id = %s AND step_id = %s RETURNING remaining_children",
                (step["run_id"], step["parent_step_id"]),
            ).fetchone()["remaining_children"]
            if remaining > 0:
                return
            self._join_children(conn, run, step["parent_step_id"])
        self._advance(conn, run)

    def _join_children(self, conn, run, parent_step_id: str) -> None:
        """All children done: the parent succeeds with their outputs in item order."""
        items = [
            r["output"]
            for r in conn.execute(
                "SELECT output FROM step_runs WHERE run_id = %s AND parent_step_id = %s "
                "ORDER BY item_index",
                (run["id"], parent_step_id),
            ).fetchall()
        ]
        conn.execute(
            "UPDATE step_runs SET status = 'succeeded', output = %s, remaining_children = 0, "
            "error = NULL, finished_at = now(), updated_at = now() WHERE run_id = %s AND step_id = %s",
            (Jsonb({"items": items}), run["id"], parent_step_id),
        )
        self._emit(conn, run, "step.succeeded", parent_step_id, {"children": len(items)})

    def _fail(self, conn, run, step, error: str, retryable: bool) -> None:
        assets.discard_attempt(conn, step["run_id"], step["step_id"], step["attempt"])
        if retryable and step["attempt"] < step["max_attempts"]:
            # Exponential backoff with jitter so retries from one outage don't land together.
            delay = min(
                step["backoff_seconds"] * (2 ** (step["attempt"] - 1)), step["max_backoff_seconds"]
            ) * random.uniform(0.5, 1.0)
            conn.execute(
                "UPDATE step_runs SET status = 'ready', error = %s, lease_owner = NULL, "
                "lease_expires_at = NULL, deadline_at = NULL, "
                "available_at = clock_timestamp() + make_interval(secs => %s), updated_at = now() "
                "WHERE run_id = %s AND step_id = %s",
                (error, delay, step["run_id"], step["step_id"]),
            )
            self._emit(
                conn, run, "step.retry_scheduled", step["step_id"],
                {"attempt": step["attempt"], "error": error, "delay_seconds": round(delay, 3)},
            )
            return
        conn.execute(
            "UPDATE step_runs SET status = 'failed', error = %s, lease_owner = NULL, "
            "lease_expires_at = NULL, deadline_at = NULL, finished_at = now(), updated_at = now() "
            "WHERE run_id = %s AND step_id = %s",
            (error, step["run_id"], step["step_id"]),
        )
        self._emit(
            conn, run, "step.failed", step["step_id"], {"attempt": step["attempt"], "error": error}
        )
        if step["parent_step_id"]:
            conn.execute(
                "UPDATE step_runs SET status = 'failed', error = %s, finished_at = now(), "
                "updated_at = now() WHERE run_id = %s AND step_id = %s",
                (f"child {step['step_id']} failed: {error}", step["run_id"], step["parent_step_id"]),
            )
        self._fail_run(conn, run, f"step '{step['step_id']}' failed: {error}")

    def _fail_run(self, conn, run, error: str) -> None:
        conn.execute(
            "UPDATE runs SET status = 'failed', error = %s, finished_at = now(), updated_at = now() "
            "WHERE id = %s",
            (error, run["id"]),
        )
        run["status"] = "failed"
        self._cancel_open_steps(conn, run["id"])
        self._emit(conn, run, "run.failed", payload={"error": error})

    def _advance(self, conn, run) -> None:
        """Move every step whose dependencies are satisfied out of 'pending'.

        Loops because some transitions finish instantly (a skipped step, an
        empty fan-out) and may unblock further steps in the same transaction.
        """
        spec = self._spec(run["workflow_name"], run["workflow_version"])
        while True:
            rows = {
                r["step_id"]: r
                for r in conn.execute(
                    "SELECT step_id, status, output FROM step_runs "
                    "WHERE run_id = %s AND parent_step_id IS NULL",
                    (run["id"],),
                ).fetchall()
            }
            outputs = {sid: r["output"] for sid, r in rows.items()}
            instant = False
            for step in spec.steps:
                if rows[step.id]["status"] != "pending":
                    continue
                if any(rows[d]["status"] not in DONE for d in step.depends_on):
                    continue
                if step.when and not _resolve(step.when, outputs):
                    conn.execute(
                        "UPDATE step_runs SET status = 'skipped', finished_at = now(), "
                        "updated_at = now() WHERE run_id = %s AND step_id = %s",
                        (run["id"], step.id),
                    )
                    self._emit(conn, run, "step.skipped", step.id, {"when": step.when})
                    instant = True
                elif step.foreach:
                    outcome = self._fan_out(conn, run, step, _resolve(step.foreach, outputs))
                    if outcome == "failed":
                        return
                    instant = instant or outcome == "empty"
                elif step.kind == "human":
                    conn.execute(
                        "UPDATE step_runs SET status = 'waiting_human', started_at = now(), "
                        "sla_due_at = CASE WHEN sla_seconds IS NULL THEN NULL "
                        "  ELSE clock_timestamp() + make_interval(secs => sla_seconds) END, "
                        "updated_at = now() WHERE run_id = %s AND step_id = %s",
                        (run["id"], step.id),
                    )
                    self._emit(conn, run, "step.waiting_human", step.id)
                else:
                    conn.execute(
                        "UPDATE step_runs SET status = 'ready', available_at = now(), "
                        "updated_at = now() WHERE run_id = %s AND step_id = %s",
                        (run["id"], step.id),
                    )
                    self._emit(conn, run, "step.ready", step.id)
            if not instant:
                break
        if all(r["status"] in DONE for r in rows.values()):
            conn.execute(
                "UPDATE runs SET status = 'succeeded', finished_at = now(), updated_at = now() "
                "WHERE id = %s",
                (run["id"],),
            )
            run["status"] = "succeeded"
            self._emit(conn, run, "run.succeeded")

    def _fan_out(self, conn, run, step: StepSpec, items: Any) -> str:
        """Expand a foreach step into one child per item. Returns expanded | empty | failed."""
        problem = None
        if not isinstance(items, list):
            problem = f"foreach '{step.foreach}' did not resolve to a list"
        elif len(items) > self.max_fan_out:
            problem = f"foreach produced {len(items)} items; limit is {self.max_fan_out}"
        if problem:
            conn.execute(
                "UPDATE step_runs SET status = 'failed', error = %s, finished_at = now(), "
                "updated_at = now() WHERE run_id = %s AND step_id = %s",
                (problem, run["id"], step.id),
            )
            self._emit(conn, run, "step.failed", step.id, {"error": problem})
            self._fail_run(conn, run, f"step '{step.id}' failed: {problem}")
            return "failed"
        if not items:
            conn.execute(
                "UPDATE step_runs SET status = 'succeeded', output = %s, remaining_children = 0, "
                "finished_at = now(), updated_at = now() WHERE run_id = %s AND step_id = %s",
                (Jsonb({"items": []}), run["id"], step.id),
            )
            self._emit(conn, run, "step.succeeded", step.id, {"children": 0})
            return "empty"
        with conn.cursor() as cur:
            cur.executemany(
                "INSERT INTO step_runs (run_id, step_id, tenant_id, parent_step_id, item_index, item, "
                "step_type, kind, status, max_attempts, backoff_seconds, max_backoff_seconds, "
                "timeout_seconds) VALUES (%s, %s, %s, %s, %s, %s, %s, 'task', 'ready', %s, %s, %s, %s)",
                [
                    (
                        run["id"], f"{step.id}#{i}", run["tenant_id"], step.id, i, Jsonb(item),
                        step.handler, step.retry.max_attempts, step.retry.backoff_seconds,
                        step.retry.max_backoff_seconds, step.timeout_seconds,
                    )
                    for i, item in enumerate(items)
                ],
            )
        conn.execute(
            "UPDATE step_runs SET status = 'fanned_out', remaining_children = %s, started_at = now(), "
            "updated_at = now() WHERE run_id = %s AND step_id = %s",
            (len(items), run["id"], step.id),
        )
        self._emit(conn, run, "step.fanned_out", step.id, {"children": len(items)})
        return "expanded"
