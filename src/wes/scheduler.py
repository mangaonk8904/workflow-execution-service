from __future__ import annotations

import json
import logging
import threading
import urllib.request
from typing import Any, Callable

from .engine import Engine

log = logging.getLogger("wes.scheduler")


def stdout_sink(event: dict[str, Any]) -> None:
    print(json.dumps(event, default=str), flush=True)


def webhook_sink(url: str) -> Callable[[dict[str, Any]], None]:
    def sink(event: dict[str, Any]) -> None:
        req = urllib.request.Request(
            url, data=json.dumps(event, default=str).encode(),
            headers={"Content-Type": "application/json", "X-WES-Event-Id": str(event["id"])},
        )
        urllib.request.urlopen(req, timeout=10).close()  # raises on failure -> batch retried
    return sink


def run_scheduler(
    engine: Engine,
    sink: Callable[[dict[str, Any]], None] = stdout_sink,
    *,
    interval: float = 1.0,
    stop: threading.Event | None = None,
) -> None:
    """Background duties: recover lost leases, escalate overdue human tasks, relay the outbox.

    Safe to run more than one copy; each duty tolerates concurrent runners.
    """
    stop = stop or threading.Event()
    log.info("scheduler started")
    while not stop.is_set():
        try:
            reaped = engine.reap()
            escalated = engine.escalate_overdue_human_tasks()
            relayed = engine.relay_outbox(sink)
            if reaped or escalated:
                log.info("reaped=%d escalated=%d relayed=%d", reaped, escalated, relayed)
        except Exception:
            log.exception("scheduler tick failed")
        stop.wait(interval)
