"""End-to-end demo: python -m examples.interview_pipeline.demo

Needs only Postgres (WES_DATABASE_URL). Runs two workers and the scheduler in
threads, plays the expert reviewer when the human step appears, then prints
the run and the delivered manifest.
"""

from __future__ import annotations

import json
import sys
import threading
import time
from pathlib import Path

from wes import Engine, Worker, WorkflowSpec
from wes.config import Settings
from wes.db import make_pool, migrate
from wes.scheduler import run_scheduler

from .handlers import registry

HERE = Path(__file__).parent


def main() -> int:
    settings = Settings.from_env()
    pool = make_pool(settings.database_url)
    migrate(pool)
    engine = Engine(pool)

    spec = WorkflowSpec.model_validate_json((HERE / "workflow.json").read_text())
    version = engine.register_workflow(spec)
    run_input = json.loads((HERE / "sample_input.json").read_text())
    run_id, _ = engine.start_run("lab-acme", spec.name, run_input)
    print(f"registered {spec.name} v{version}; started run {run_id}\n")

    stop = threading.Event()
    threads = [
        threading.Thread(target=Worker(engine, registry, poll_interval=0.05).run_forever,
                         args=(stop,), daemon=True)
        for _ in range(2)
    ]
    published: list[dict] = []
    threads.append(threading.Thread(
        target=run_scheduler, args=(engine, published.append),
        kwargs={"interval": 0.2, "stop": stop}, daemon=True))
    for t in threads:
        t.start()

    deadline = time.time() + 30
    run = engine.get_run(run_id)
    while run["status"] == "running" and time.time() < deadline:
        for task in engine.list_human_tasks("lab-acme"):
            if str(task["run_id"]) == run_id:
                q = next(s for s in run["steps"] if s["step_id"] == "quality")["output"]
                print(f"human review requested: {q['reasons']} "
                      f"(segments {q['low_confidence_segments']}); accepting as reviewer\n")
                engine.complete_human_step(
                    run_id, task["step_id"],
                    {"verdict": "accept", "notes": "Checked segment audio; transcript is accurate."},
                    actor="reviewer@lab-ops",
                )
        time.sleep(0.1)
        run = engine.get_run(run_id)
    time.sleep(0.5)  # let the relay flush the last events
    stop.set()

    print(f"{'step':<16}{'status':<12}{'attempt':<8}")
    for s in run["steps"]:
        indent = "  " if s["parent_step_id"] else ""
        print(f"{indent + s['step_id']:<16}{s['status']:<12}{s['attempt']:<8}")
    print(f"\nrun status: {run['status']}")
    if run["status"] != "succeeded":
        print("error:", run["error"])
        return 1
    package = next(s for s in run["steps"] if s["step_id"] == "package")["output"]
    manifest = json.loads(Path(package["manifest_uri"]).read_text())
    print(f"manifest: {package['manifest_uri']} ({package['records']} records)")
    print(f"events recorded: {len(engine.list_events(run_id))}, relayed by outbox: {len(published)}")
    print("\nfirst record:\n" + json.dumps(manifest["records"][0], indent=2))
    pool.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
