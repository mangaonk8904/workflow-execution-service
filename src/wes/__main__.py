from __future__ import annotations

import argparse
import logging
import signal
import sys
import threading

from .config import Settings
from .db import make_pool, migrate
from .engine import Engine
from .handlers import load_registry
from .scheduler import run_scheduler, stdout_sink, webhook_sink
from .worker import Worker


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="wes", description="Workflow Execution Service")
    sub = parser.add_subparsers(dest="cmd", required=True)
    sub.add_parser("migrate", help="apply database migrations")
    api = sub.add_parser("api", help="run the HTTP API")
    api.add_argument("--host", default="0.0.0.0")
    api.add_argument("--port", type=int, default=8000)
    worker = sub.add_parser("worker", help="run a worker")
    worker.add_argument("--handlers", required=True,
                        help="module exposing `registry`, e.g. examples.interview_pipeline.handlers")
    worker.add_argument("--concurrency", type=int, default=4)
    sub.add_parser("scheduler", help="run the reaper, SLA escalation and outbox relay")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    settings = Settings.from_env()

    if args.cmd == "api":
        import uvicorn

        uvicorn.run("wes.api:app", host=args.host, port=args.port)
        return 0

    pool = make_pool(settings.database_url)
    applied = migrate(pool)
    if args.cmd == "migrate":
        print("applied:", ", ".join(applied) if applied else "nothing to do")
        return 0

    engine = Engine(pool, tenant_max_running=settings.tenant_max_running,
                    max_fan_out=settings.max_fan_out)
    stop = threading.Event()
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: stop.set())

    if args.cmd == "worker":
        registry = load_registry(args.handlers)
        threads = [
            threading.Thread(
                target=Worker(engine, registry, lease_seconds=settings.lease_seconds,
                              poll_interval=settings.poll_interval).run_forever,
                args=(stop,), daemon=True,
            )
            for _ in range(args.concurrency)
        ]
        for t in threads:
            t.start()
        stop.wait()
    elif args.cmd == "scheduler":
        sink = webhook_sink(settings.webhook_url) if settings.webhook_url else stdout_sink
        run_scheduler(engine, sink, stop=stop)
    pool.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
