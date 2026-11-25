from __future__ import annotations

import logging
import os
import socket
import threading
import time
import traceback
import uuid

import psycopg

from .engine import Engine, Task
from .handlers import Context, HandlerRegistry, PermanentError

log = logging.getLogger("wes.worker")


class Worker:
    """Pulls tasks for the handler types it knows, runs them, reports the result.

    While a handler runs, a heartbeat thread keeps the lease alive. If the
    worker process dies, the heartbeat stops, the lease expires and the
    scheduler's reaper hands the step to another worker.
    """

    def __init__(
        self,
        engine: Engine,
        registry: HandlerRegistry,
        *,
        worker_id: str | None = None,
        lease_seconds: float = 30.0,
        poll_interval: float = 0.5,
    ) -> None:
        self.engine = engine
        self.registry = registry
        self.worker_id = worker_id or f"{socket.gethostname()}-{os.getpid()}-{uuid.uuid4().hex[:6]}"
        self.lease_seconds = lease_seconds
        self.poll_interval = poll_interval

    def run_once(self) -> bool:
        """Claim and execute at most one task. Returns False when the queue was empty."""
        task = self.engine.claim(self.worker_id, self.registry.types(), self.lease_seconds)
        if task is None:
            return False
        self._execute(task)
        return True

    def run_forever(self, stop: threading.Event | None = None) -> None:
        stop = stop or threading.Event()
        log.info("worker %s serving %s", self.worker_id, self.registry.types())
        while not stop.is_set():
            try:
                if not self.run_once():
                    stop.wait(self.poll_interval)
            except Exception:  # keep the loop alive across transient DB errors
                log.exception("worker loop error")
                stop.wait(self.poll_interval)

    def _execute(self, task: Task) -> None:
        ctx = Context(
            run_id=task.run_id, step_id=task.step_id, attempt=task.attempt,
            tenant_id=task.tenant_id, run_input=task.run_input, deps=task.deps,
            params=task.params, item=task.item, item_index=task.item_index,
        )
        done = threading.Event()
        beat = threading.Thread(target=self._heartbeat, args=(task, ctx, done), daemon=True)
        beat.start()
        ident = (task.run_id, task.step_id, task.attempt, self.worker_id)
        try:
            try:
                output = self.registry.get(task.step_type)(ctx)
            except PermanentError as exc:
                log.warning("%s/%s failed permanently: %s", task.run_id, task.step_id, exc)
                accepted = self._report(self.engine.fail_step, *ident, str(exc), retryable=False)
            except Exception as exc:
                detail = f"{type(exc).__name__}: {exc}"
                log.warning("%s/%s attempt %d failed: %s\n%s", task.run_id, task.step_id,
                            task.attempt, detail, traceback.format_exc(limit=3))
                accepted = self._report(self.engine.fail_step, *ident, detail, retryable=True)
            else:
                accepted = self._report(self.engine.complete_step, *ident, output)
        finally:
            done.set()
            beat.join(timeout=1)
        if not accepted:
            log.warning("%s/%s attempt %d: result ignored (lease lost or run no longer running)",
                        task.run_id, task.step_id, task.attempt)

    @staticmethod
    def _report(fn, *args, **kwargs) -> bool:
        """Report a result, retrying transient database errors.

        Reporting is safe to repeat: the engine only accepts a result for the
        current attempt once. If every try fails, the lease expires and the
        step is retried, which is the at-least-once fallback.
        """
        for delay in (0.1, 0.5, 2.0):
            try:
                return fn(*args, **kwargs)
            except psycopg.OperationalError:
                log.warning("transient database error while reporting; retrying", exc_info=True)
                time.sleep(delay)
        return fn(*args, **kwargs)

    def _heartbeat(self, task: Task, ctx: Context, done: threading.Event) -> None:
        while not done.wait(self.lease_seconds / 3):
            try:
                alive = self.engine.heartbeat(
                    task.run_id, task.step_id, task.attempt, self.worker_id, self.lease_seconds
                )
            except Exception:
                log.exception("heartbeat error")
                continue
            if not alive:
                ctx.lost_lease.set()  # cooperative cancellation signal for long handlers
                return
