from __future__ import annotations

import json
from pathlib import Path

import pytest

from examples.interview_pipeline.handlers import registry
from wes import WorkflowSpec

from .conftest import drain, outputs, statuses

EXAMPLE = Path(__file__).parent.parent / "examples" / "interview_pipeline"
CONSENTED = {"consent": ["internal_processing", "share_with_customers"], "pii": "present"}


def upload_source(catalog, policy=CONSENTED, tenant="lab-acme"):
    return catalog.upload(
        tenant, b"\x00\x00\x00\x18ftypmp42 stub interview recording", kind="video",
        media_type="video/mp4", name="interview.mp4", policy=policy,
        metadata={"duration_seconds": 175.0},
    )


def load(engine, catalog, *, policy=CONSENTED, mutate_spec=None, mutate_input=None):
    spec = WorkflowSpec.model_validate_json((EXAMPLE / "workflow.json").read_text())
    # No random sampling in tests: review must be triggered by the data alone.
    spec.step("quality").params["review_sample_rate"] = 0.0
    if mutate_spec:
        mutate_spec(spec)
    engine.register_workflow(spec)
    run_input = json.loads((EXAMPLE / "sample_input.json").read_text())
    run_input["source"] = {"$asset": str(upload_source(catalog, policy)["id"])}
    if mutate_input:
        mutate_input(run_input)
    return engine.start_run("lab-acme", spec.name, run_input)[0]


def manifest_of(engine, catalog, run_id):
    asset_id = outputs(engine, run_id)["package"]["manifest"]["$asset"]
    with catalog.open(asset_id) as f:
        return catalog.get(asset_id), json.loads(f.read())


def test_low_confidence_interview_goes_to_review_then_ships(engine, catalog):
    run_id = load(engine, catalog)
    drain(engine, registry, catalog=catalog)
    assert statuses(engine, run_id)["expert_review"] == "waiting_human"
    quality = outputs(engine, run_id)["quality"]
    assert quality["reasons"] == ["low_asr_confidence"] and quality["low_confidence_segments"] == [2]
    assert catalog.get(quality["review_proxy"]["$asset"])["metadata"]["height"] == 480

    engine.complete_human_step(run_id, "expert_review", {"verdict": "accept"}, actor="reviewer")
    drain(engine, registry, catalog=catalog)
    assert engine.get_run(run_id)["status"] == "succeeded"

    meta, manifest = manifest_of(engine, catalog, run_id)
    assert meta["policy"]["pii"] == "redacted"
    assert "share_with_customers" in meta["policy"]["consent"]
    assert len(manifest["records"]) == 3
    assert manifest["review"]["verdict"] == "accept"
    assert manifest["expert_ref"].startswith("exp_") and "4821" not in manifest["expert_ref"]
    first = manifest["records"][0]
    assert [r["content"]["evidence"]["segment_index"] for r in manifest["records"]] == [0, 1, 2]
    assert "spark" in first["content"]["skills"]
    # PII never reaches the delivered data.
    delivered = json.dumps(manifest)
    for secret in ("Jordan Rivera", "jordan@example.com", "512-555-0142"):
        assert secret not in delivered
    assert outputs(engine, run_id)["redact"]["redactions"] == 3


def test_every_record_traces_back_to_the_recording_and_its_time_range(engine, catalog):
    run_id = load(engine, catalog)
    drain(engine, registry, catalog=catalog)
    engine.complete_human_step(run_id, "expert_review", {"verdict": "accept"})
    drain(engine, registry, catalog=catalog)
    source = engine.get_run(run_id)["input"]["source"]["$asset"]
    _, manifest = manifest_of(engine, catalog, run_id)

    expected_spans = [(4.0, 58.5), (61.0, 119.0), (121.5, 170.0)]
    for record, (start, end) in zip(manifest["records"], expected_spans):
        prov = record["provenance"]
        assert [s["asset"] for s in prov["sources"]] == [source]
        assert [(s["start"], s["end"]) for s in prov["media_spans"]] == [(start, end)]
        names = [n["name"] for n in prov["chain"]]
        assert names[0] == "record" and names[-1] == "interview.mp4"
        assert {"transcript", "audio", "mezzanine"} <= set(names)
        assert "transcribe@2.0.0" in prov["handlers"] and "redact@2.0.0" in prov["handlers"]

    # Fine-grained: each claim points at the exact span of redacted transcript it came from.
    record_id = manifest["records"][0]["asset"]
    graph = catalog.lineage(record_id, direction="up", depth=1)
    claim_edges = [e for e in graph["edges"] if (e["child_selector"] or {}).get("pointer") == "/claims/0"]
    assert len(claim_edges) == 1
    sel = claim_edges[0]["parent_selector"]
    with catalog.open(str(claim_edges[0]["parent_id"])) as f:
        transcript = f.read().decode()
    assert transcript[sel["start"]:sel["end"]] == manifest["records"][0]["content"]["claims"][0]

    # And downwards: everything built from the recording, for impact analysis.
    down = catalog.lineage(source, direction="down")
    kinds = {n["name"] for n in down["nodes"]}
    assert {"mezzanine", "audio", "review_proxy", "transcript", "record", "manifest"} <= kinds


def test_handler_builds_are_recorded_on_steps_and_edges(engine, catalog):
    run_id = load(engine, catalog, mutate_input=lambda i: [t.update(asr_confidence=0.99) for t in i["turns"]])
    drain(engine, registry, catalog=catalog)
    steps = {s["step_id"]: s for s in engine.get_run(run_id)["steps"]}
    assert steps["transcribe#0"]["handler"] == {
        "name": "transcribe", "version": "2.0.0", "models": {"asr": "stub-asr-1"},
    }
    committed = [e for e in engine.list_events(run_id) if e["type"] == "asset.committed"]
    # 3 normalize outputs, 3 transcripts, 3 redactions, 3 records, 1 manifest
    assert len(committed) == 13


def test_clean_interview_skips_review(engine, catalog):
    run_id = load(engine, catalog, mutate_input=lambda i: [t.update(asr_confidence=0.99) for t in i["turns"]])
    drain(engine, registry, catalog=catalog)
    assert statuses(engine, run_id)["expert_review"] == "skipped"
    assert engine.get_run(run_id)["status"] == "succeeded"
    _, manifest = manifest_of(engine, catalog, run_id)
    assert manifest["review"] == {"verdict": "auto_accepted"}


def test_missing_consent_stops_the_run_before_any_processing(engine, catalog):
    run_id = load(engine, catalog, policy={"consent": ["internal_processing"], "pii": "present"})
    assert drain(engine, registry, catalog=catalog) == 1
    run = engine.get_run(run_id)
    assert run["status"] == "failed" and "consent" in run["error"]
    assert catalog.run_assets(run_id) == []


def test_policy_gate_blocks_delivery_even_if_handlers_skip_the_check(engine, catalog):
    """Consent travels with the data: drop the early check and the gate still holds."""
    def no_early_check(spec):
        spec.step("probe").params["required_consent"] = []

    run_id = load(engine, catalog, policy={"consent": ["internal_processing"], "pii": "present"},
                  mutate_spec=no_early_check,
                  mutate_input=lambda i: [t.update(asr_confidence=0.99) for t in i["turns"]])
    drain(engine, registry, catalog=catalog)
    run = engine.get_run(run_id)
    assert run["status"] == "failed"
    assert "policy gate" in run["error"] and "share_with_customers" in run["error"]
    assert not [a for a in catalog.run_assets(run_id) if a["name"] == "manifest"]


def test_redaction_needs_the_spec_to_grant_it(engine, catalog):
    def no_grant(spec):
        spec.step("redact").policy_transforms = {}

    run_id = load(engine, catalog, mutate_spec=no_grant)
    drain(engine, registry, catalog=catalog)
    run = engine.get_run(run_id)
    assert run["status"] == "failed" and "may not lower pii" in run["error"]


def test_tampered_source_bytes_are_refused(engine, catalog, store):
    run_id = load(engine, catalog)
    source = catalog.get(engine.get_run(run_id)["input"]["source"]["$asset"])
    path = Path(store.uri_for("lab-acme", source["sha256"])[len("file://"):])
    path.write_bytes(b"something else")
    drain(engine, registry, catalog=catalog)
    run = engine.get_run(run_id)
    assert run["status"] == "failed" and "do not match" in run["error"]


def test_reviewer_rejection_blocks_delivery(engine, catalog):
    run_id = load(engine, catalog)
    drain(engine, registry, catalog=catalog)
    engine.complete_human_step(run_id, "expert_review", {"verdict": "reject", "notes": "wrong speaker"})
    drain(engine, registry, catalog=catalog)
    run = engine.get_run(run_id)
    assert run["status"] == "failed" and "wrong speaker" in run["error"]
    assert not [a for a in catalog.run_assets(run_id) if a["name"] == "manifest"]


def test_run_input_cannot_name_another_tenants_asset(engine, catalog):
    other = upload_source(catalog, tenant="someone-else")
    spec = WorkflowSpec.model_validate_json((EXAMPLE / "workflow.json").read_text())
    engine.register_workflow(spec)
    from wes import Invalid
    with pytest.raises(Invalid, match="not visible"):
        engine.start_run("lab-acme", spec.name, {"source": {"$asset": str(other["id"])}})
