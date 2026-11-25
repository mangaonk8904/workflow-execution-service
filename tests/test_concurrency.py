from __future__ import annotations

import threading
import time
from collections import Counter

from wes import HandlerRegistry, Worker

from .conftest import make_spec


def test_many_workers_never_run_a_step_twice(engine):
    """8 competing workers, 30 runs with a 5-way fan-out each: 210 tasks, each executed once."""
    executions: Counter[tuple[str, str]] = Counter()
    lock = threading.Lock()
    reg = HandlerRegistry()

    def record(ctx):
        with lock:
            executions[(ctx.run_id, ctx.step_id)] += 1

    @reg.register("split")
    def split(ctx):
        record(ctx)
        return {"parts": list(range(5))}

    @reg.register("work")
    def work(ctx):
        record(ctx)
        return {"v": ctx.item}

    @reg.register("join")
    def join(ctx):
        record(ctx)
        return {"sum": sum(i["v"] for i in ctx.deps["work"]["items"])}

    spec = make_spec("stress", [
        {"id": "split"},
        {"id": "work", "depends_on": ["split"], "foreach": "split.parts"},
        {"id": "join", "depends_on": ["work"]},
    ])
    engine.register_workflow(spec)
    run_ids = [engine.start_run(f"tenant-{i % 3}", "stress")[0] for i in range(30)]

    stop = threading.Event()
    threads = [
        threading.Thread(target=Worker(engine, reg, poll_interval=0.01).run_forever, args=(stop,))
        for _ in range(8)
    ]
    for t in threads:
        t.start()
    deadline = time.time() + 60
    try:
        while time.time() < deadline:
            if all(engine.get_run(r)["status"] != "running" for r in run_ids):
                break
            time.sleep(0.05)
    finally:
        stop.set()
        for t in threads:
            t.join()

    runs = [engine.get_run(r) for r in run_ids]
    assert [r["status"] for r in runs] == ["succeeded"] * 30
    assert len(executions) == 30 * 7 and set(executions.values()) == {1}
    for r in runs:
        assert next(s for s in r["steps"] if s["step_id"] == "join")["output"] == {"sum": 10}
