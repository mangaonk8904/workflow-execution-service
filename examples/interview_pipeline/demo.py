"""End-to-end demo: python -m examples.interview_pipeline.demo

Needs Postgres (WES_DATABASE_URL); asset bytes go to WES_BLOB_URL (default
./blobstore). Uploads a stand-in recording with the expert's consent, runs two
workers and the scheduler in threads, plays the expert reviewer when the human
step appears, then prints the run, a delivered record and its provenance.
"""

from __future__ import annotations

import json
import sys
import threading
import time
from pathlib import Path

from wes import Engine, Worker, WorkflowSpec, ref
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
    catalog = settings.catalog(pool)

    source = catalog.upload(
        "lab-acme", b"\x00\x00\x00\x18ftypmp42 stand-in for a recorded interview",
        kind="video", media_type="video/mp4", name="interview.mp4",
        metadata={"duration_seconds": 175.0},
        policy={"consent": ["internal_processing", "share_with_customers"], "pii": "present",
                "retention_days": 365},
        created_by="demo",
    )
    print(f"uploaded source recording as asset {source['id']} (sha256 {source['sha256'][:12]}...)")

    spec = WorkflowSpec.model_validate_json((HERE / "workflow.json").read_text())
    version = engine.register_workflow(spec)
    run_input = json.loads((HERE / "sample_input.json").read_text())
    run_input["source"] = ref(source)
    run_id, _ = engine.start_run("lab-acme", spec.name, run_input)
    print(f"registered {spec.name} v{version}; started run {run_id}\n")

    stop = threading.Event()
    threads = [
        threading.Thread(target=Worker(engine, registry, catalog=catalog,
                                       poll_interval=0.05).run_forever,
                         args=(stop,), daemon=True)
        for _ in range(2)
    ]
    published: list[dict] = []
    threads.append(threading.Thread(
        target=run_scheduler, args=(engine, published.append),
        kwargs={"interval": 0.2, "stop": stop, "catalog": catalog}, daemon=True))
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
    manifest_id = package["manifest"]["$asset"]
    meta = catalog.get(manifest_id)
    with catalog.open(manifest_id) as f:
        manifest = json.loads(f.read())
    print(f"manifest: asset {manifest_id} ({package['records']} records), policy {meta['policy']}")
    print(f"assets committed: {len(catalog.run_assets(run_id))}; events recorded: "
          f"{len(engine.list_events(run_id))}, relayed by outbox: {len(published)}")
    first = manifest["records"][0]
    print("\nfirst record:\n" + json.dumps(first["content"], indent=2))
    prov = first["provenance"]
    print("\nits provenance, newest first:")
    for node in prov["chain"]:
        print(f"  {'  ' * node['depth']}{node['name']} ({node['kind']})")
    for span in prov["media_spans"]:
        print(f"  spoken at {span['start']}s-{span['end']}s of the recording's audio track")
    print(f"  built by: {', '.join(prov['handlers'])}")
    pool.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
