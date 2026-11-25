from __future__ import annotations

import json
from pathlib import Path

from examples.interview_pipeline.handlers import registry
from wes import WorkflowSpec

from .conftest import drain, outputs, statuses

EXAMPLE = Path(__file__).parent.parent / "examples" / "interview_pipeline"


def load(engine, monkeypatch, tmp_path, mutate=None):
    monkeypatch.setenv("WES_DELIVERY_DIR", str(tmp_path))
    spec = WorkflowSpec.model_validate_json((EXAMPLE / "workflow.json").read_text())
    # No random sampling in tests: review must be triggered by the data alone.
    spec.step("quality").params["review_sample_rate"] = 0.0
    engine.register_workflow(spec)
    run_input = json.loads((EXAMPLE / "sample_input.json").read_text())
    if mutate:
        mutate(run_input)
    return engine.start_run("lab-acme", spec.name, run_input)[0]


def test_low_confidence_interview_goes_to_review_then_ships(engine, monkeypatch, tmp_path):
    run_id = load(engine, monkeypatch, tmp_path)
    drain(engine, registry)
    assert statuses(engine, run_id)["expert_review"] == "waiting_human"
    quality = outputs(engine, run_id)["quality"]
    assert quality["reasons"] == ["low_asr_confidence"] and quality["low_confidence_segments"] == [2]

    engine.complete_human_step(run_id, "expert_review", {"verdict": "accept"}, actor="reviewer")
    drain(engine, registry)
    assert engine.get_run(run_id)["status"] == "succeeded"

    manifest = json.loads(Path(outputs(engine, run_id)["package"]["manifest_uri"]).read_text())
    assert len(manifest["records"]) == 3
    assert manifest["review"]["verdict"] == "accept"
    assert manifest["expert_ref"].startswith("exp_") and "4821" not in manifest["expert_ref"]
    # Every record cites the segment and timestamps it was extracted from.
    assert [r["evidence"]["segment_index"] for r in manifest["records"]] == [0, 1, 2]
    assert "spark" in manifest["records"][0]["skills"]
    # PII never reaches the delivered data.
    delivered = json.dumps(manifest)
    for secret in ("Jordan Rivera", "jordan@example.com", "512-555-0142"):
        assert secret not in delivered
    assert outputs(engine, run_id)["redact"]["redactions"] == 3


def test_clean_interview_skips_review(engine, monkeypatch, tmp_path):
    def all_confident(run_input):
        for t in run_input["turns"]:
            t["asr_confidence"] = 0.99

    run_id = load(engine, monkeypatch, tmp_path, all_confident)
    drain(engine, registry)
    assert statuses(engine, run_id)["expert_review"] == "skipped"
    assert engine.get_run(run_id)["status"] == "succeeded"
    manifest = json.loads(Path(outputs(engine, run_id)["package"]["manifest_uri"]).read_text())
    assert manifest["review"] == {"verdict": "auto_accepted"}


def test_missing_consent_stops_the_run_before_any_processing(engine, monkeypatch, tmp_path):
    run_id = load(engine, monkeypatch, tmp_path, lambda i: i["consent"].update(share_with_customers=False))
    assert drain(engine, registry) == 1
    run = engine.get_run(run_id)
    assert run["status"] == "failed" and "consent" in run["error"]
    assert not list(tmp_path.rglob("*.json"))


def test_reviewer_rejection_blocks_delivery(engine, monkeypatch, tmp_path):
    run_id = load(engine, monkeypatch, tmp_path)
    drain(engine, registry)
    engine.complete_human_step(run_id, "expert_review", {"verdict": "reject", "notes": "wrong speaker"})
    drain(engine, registry)
    run = engine.get_run(run_id)
    assert run["status"] == "failed" and "wrong speaker" in run["error"]
    assert not list(tmp_path.rglob("*.json"))
