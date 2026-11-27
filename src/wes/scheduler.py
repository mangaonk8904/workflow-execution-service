from __future__ import annotations

import json
import logging
import threading
import time
import urllib.request
from typing import Any, Callable

from .assets import AssetCatalog
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
    catalog: AssetCatalog | None = None,
    gc_grace_seconds: float = 3600.0,
    gc_interval: float = 60.0,
) -> None:
    """Background duties: recover lost leases, escalate overdue human tasks, relay
    the outbox and, with a catalog, delete discarded assets and their unshared blobs.

    Safe to run more than one copy; each duty tolerates concurrent runners.
    """
    stop = stop or threading.Event()
    log.info("scheduler started")
    next_gc = time.monotonic()
    while not stop.is_set():
        try:
            reaped = engine.reap()
            escalated = engine.escalate_overdue_human_tasks()
            relayed = engine.relay_outbox(sink)
            collected = 0
            if catalog is not None and time.monotonic() >= next_gc:
                collected = catalog.collect_garbage(gc_grace_seconds)
                next_gc = time.monotonic() + gc_interval
            if reaped or escalated or collected:
                log.info("reaped=%d escalated=%d relayed=%d assets_collected=%d",
                         reaped, escalated, relayed, collected)
        except Exception:
            log.exception("scheduler tick failed")
        stop.wait(interval)
