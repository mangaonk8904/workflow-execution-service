from __future__ import annotations

import json

import pytest
from fastapi.testclient import TestClient

from wes import HandlerRegistry, ref
from wes.api import create_app
from wes.auth import create_api_key, revoke_api_key
from wes.config import Settings

from .conftest import drain

SPEC = {
    "name": "api-flow",
    "steps": [
        {"id": "a"},
        {"id": "approve", "kind": "human", "depends_on": ["a"]},
        {"id": "b", "depends_on": ["approve"]},
    ],
}


@pytest.fixture
def client(engine, catalog):
    settings = Settings(max_asset_bytes=1024)
    with TestClient(create_app(engine, catalog, settings=settings)) as c:
        yield c


@pytest.fixture
def keys(engine):
    def auth(**kw):
        return {"Authorization": f"Bearer {create_api_key(engine.pool, **kw)}"}
    return {
        "admin": auth(principal="root", role="admin"),
        "op1": auth(principal="ops@t1", role="operator", tenant_id="t1"),
        "rev1": auth(principal="sam@t1", role="reviewer", tenant_id="t1"),
        "op2": auth(principal="ops@t2", role="operator", tenant_id="t2"),
    }


def test_full_flow_over_http(client, engine, keys):
    reg = HandlerRegistry()
    reg.register("a")(lambda ctx: {"x": 1})
    reg.register("b")(lambda ctx: {"approved_by": ctx.deps["approve"]["by"]})

    assert client.get("/healthz").json() == {"status": "ok"}
    r = client.post("/workflows", json=SPEC, headers=keys["admin"])
    assert r.json() == {"name": "api-flow", "version": 1}

    body = {"workflow": "api-flow", "input": {"k": "v"}}
    r = client.post("/runs", json=body, headers={**keys["op1"], "Idempotency-Key": "abc"})
    assert r.status_code == 201 and r.json()["tenant_id"] == "t1"
    run_id = r.json()["id"]
    replay = client.post("/runs", json=body, headers={**keys["op1"], "Idempotency-Key": "abc"})
    assert replay.status_code == 200 and replay.json()["id"] == run_id

    drain(engine, reg)
    tasks = client.get("/human-tasks", headers=keys["rev1"]).json()
    assert [t["step_id"] for t in tasks] == ["approve"]
    r = client.post(f"/runs/{run_id}/steps/approve/complete",
                    json={"output": {"by": "sam"}, "actor": "someone-else"}, headers=keys["rev1"])
    assert r.status_code == 200
    again = client.post(f"/runs/{run_id}/steps/approve/complete", json={}, headers=keys["rev1"])
    assert again.status_code == 409

    drain(engine, reg)
    run = client.get(f"/runs/{run_id}", headers=keys["op1"]).json()
    assert run["status"] == "succeeded"
    assert [s["step_id"] for s in run["steps"]] == ["a", "approve", "b"]
    assert run["steps"][-1]["output"] == {"approved_by": "sam"}
    events = client.get(f"/runs/{run_id}/events", headers=keys["op1"]).json()
    assert events[0]["type"] == "run.started" and events[-1]["type"] == "run.succeeded"
    # The actor is the key holder, not what the request body claimed.
    done = next(e for e in events if e["type"] == "step.succeeded" and e["step_id"] == "approve")
    assert done["payload"]["actor"] == "sam@t1"


def test_requests_need_a_valid_key(client, engine, keys):
    assert client.get("/workflows/api-flow").status_code == 401
    assert client.get("/workflows/x", headers={"Authorization": "Bearer nope"}).status_code == 401
    key = create_api_key(engine.pool, principal="temp", role="operator", tenant_id="t1")
    revoke_api_key(engine.pool, key)
    assert client.get("/workflows/x", headers={"Authorization": f"Bearer {key}"}).status_code == 401


def test_roles_limit_what_each_key_can_do(client, keys):
    assert client.post("/workflows", json=SPEC, headers=keys["op1"]).status_code == 403
    client.post("/workflows", json=SPEC, headers=keys["admin"])
    assert client.post("/runs", json={"workflow": "api-flow"}, headers=keys["rev1"]).status_code == 403
    run_id = client.post("/runs", json={"workflow": "api-flow"}, headers=keys["op1"]).json()["id"]
    r = client.post(f"/runs/{run_id}/steps/approve/complete", json={}, headers=keys["op1"])
    assert r.status_code == 403  # operators run things; reviewers sign them off
    assert client.post(f"/runs/{run_id}/cancel", headers=keys["rev1"]).status_code == 403


def test_tenants_cannot_see_or_act_on_each_other(client, engine, keys):
    client.post("/workflows", json=SPEC, headers=keys["admin"])
    run_id = client.post("/runs", json={"workflow": "api-flow"}, headers=keys["op1"]).json()["id"]
    reg = HandlerRegistry()
    reg.register("a")(lambda ctx: {})
    drain(engine, reg)

    for path in (f"/runs/{run_id}", f"/runs/{run_id}/events", f"/runs/{run_id}/assets"):
        assert client.get(path, headers=keys["op2"]).status_code == 404
    assert client.post(f"/runs/{run_id}/cancel", headers=keys["op2"]).status_code == 404
    assert client.get("/human-tasks", headers=keys["op2"]).json() == []
    assert client.get("/human-tasks", params={"tenant_id": "t1"}, headers=keys["op2"]).status_code == 403
    r = client.post("/runs", json={"workflow": "api-flow", "tenant_id": "t1"}, headers=keys["op2"])
    assert r.status_code == 403
    # Admins act on any tenant but must say which.
    assert client.post("/runs", json={"workflow": "api-flow"}, headers=keys["admin"]).status_code == 422
    r = client.post("/runs", json={"workflow": "api-flow", "tenant_id": "t2"}, headers=keys["admin"])
    assert r.status_code == 201 and r.json()["tenant_id"] == "t2"
    assert len(client.get("/human-tasks", headers=keys["admin"]).json()) == 1


def test_assets_over_http(client, engine, catalog, keys):
    policy = {"consent": ["share"], "pii": "present"}
    r = client.post(
        "/assets", params={"kind": "audio", "name": "call.flac"}, content=b"fLaC audio",
        headers={**keys["op1"], "Content-Type": "audio/flac",
                 "X-Asset-Policy": json.dumps(policy),
                 "X-Asset-Metadata": json.dumps({"duration_seconds": 30})},
    )
    assert r.status_code == 201, r.text
    src = r.json()
    assert src["tenant_id"] == "t1" and src["created_by"] == "ops@t1" and src["policy"]["consent"] == ["share"]
    assert "uri" not in src  # storage layout is not part of the API

    content = client.get(f"/assets/{src['id']}/content", headers=keys["rev1"])
    assert content.content == b"fLaC audio" and content.headers["etag"] == f'"{src["sha256"]}"'
    assert content.headers["content-type"] == "audio/flac"

    reg = HandlerRegistry()

    @reg.register("transcribe")
    def transcribe(ctx):
        t = ctx.assets.put_text("hello", name="transcript",
                                derived_from=[(ctx.run_input["audio"], "transcript_of",
                                               {"type": "time", "start": 1, "end": 2})])
        return {"t": ref(t)}

    client.post("/workflows", json={"name": "t", "steps": [{"id": "transcribe"}]}, headers=keys["admin"])
    run = client.post("/runs", json={"workflow": "t", "input": {"audio": {"$asset": src["id"]}}},
                      headers=keys["op1"]).json()
    drain(engine, reg, catalog=catalog)
    [made] = client.get(f"/runs/{run['id']}/assets", headers=keys["op1"]).json()
    lineage = client.get(f"/assets/{made['id']}/lineage", headers=keys["op1"]).json()
    assert [n["name"] for n in lineage["nodes"]] == ["transcript", "call.flac"]
    assert lineage["edges"][0]["parent_selector"] == {"type": "time", "start": 1.0, "end": 2.0}
    down = client.get(f"/assets/{src['id']}/lineage", params={"direction": "down"}, headers=keys["op1"])
    assert {n["name"] for n in down.json()["nodes"]} == {"call.flac", "transcript"}

    # Isolation, validation, limits.
    for path in (f"/assets/{src['id']}", f"/assets/{src['id']}/content", f"/assets/{src['id']}/lineage"):
        assert client.get(path, headers=keys["op2"]).status_code == 404
    r = client.post("/runs", json={"workflow": "t", "input": {"audio": {"$asset": src["id"]}}},
                    headers=keys["op2"])
    assert r.status_code == 422  # cannot start a run on another tenant's asset
    bad = {**keys["op1"], "Content-Type": "audio/flac", "X-Asset-Policy": "{not json"}
    assert client.post("/assets", params={"kind": "audio", "name": "x"}, content=b"1", headers=bad).status_code == 422
    r = client.post("/assets", params={"kind": "audio", "name": "x"}, content=b"1",
                    headers={**keys["op1"], "Content-Type": "audio/flac",
                             "X-Asset-Policy": json.dumps({"pii": "maybe"})})
    assert r.status_code == 422
    r = client.post("/assets", params={"kind": "audio", "name": "big"}, content=b"x" * 2048,
                    headers={**keys["op1"], "Content-Type": "audio/flac"})
    assert r.status_code == 413
    assert client.post("/assets", params={"kind": "audio", "name": "x"}, content=b"1",
                       headers=keys["rev1"]).status_code == 403
    assert client.get(f"/assets/{src['id']}/lineage", params={"direction": "sideways"},
                      headers=keys["op1"]).status_code == 422


def test_list_endpoints_and_ui(client, keys):
    admin, op1, op2 = keys["admin"], keys["op1"], keys["op2"]
    assert client.get("/workflows", headers=op1).json() == []
    assert client.get("/runs", headers=op1).json() == []
    client.post("/workflows", json=SPEC, headers=admin)
    client.post("/workflows", json={**SPEC, "description": "v2"}, headers=admin)
    [wf] = client.get("/workflows", headers=op1).json()
    assert (wf["name"], wf["version"], wf["version_count"], wf["step_count"]) == ("api-flow", 2, 2, 3)
    assert wf["description"] == "v2"

    r1 = client.post("/runs", json={"workflow": "api-flow"}, headers=op1).json()["id"]
    r2 = client.post("/runs", json={"workflow": "api-flow"}, headers=op2).json()["id"]
    client.post(f"/runs/{r2}/cancel", headers=op2)
    assert [r["id"] for r in client.get("/runs", headers=admin).json()] == [r2, r1]  # newest first
    assert [r["id"] for r in client.get("/runs", params={"tenant_id": "t1"}, headers=admin).json()] == [r1]
    assert [r["id"] for r in client.get("/runs", params={"status": "cancelled"}, headers=admin).json()] == [r2]
    assert client.get("/runs", params={"workflow": "other"}, headers=admin).json() == []
    assert client.get("/runs", params={"status": "bogus"}, headers=admin).status_code == 422
    [run] = client.get("/runs", headers=op1).json()
    assert (run["id"], run["steps_done"], run["steps_total"], run["waiting_human"]) == (r1, 0, 3, False)
    # A tenant key only ever lists its own runs.
    assert [r["id"] for r in client.get("/runs", headers=op2).json()] == [r2]
    assert client.get("/runs", params={"tenant_id": "t1"}, headers=op2).status_code == 403
    assert client.get("/runs").status_code == 401 and client.get("/workflows").status_code == 401

    assert client.get("/me", headers=op1).json() == {"principal": "ops@t1", "role": "operator", "tenant_id": "t1"}
    page = client.get("/")  # the page itself is public; its API calls are not
    assert page.status_code == 200 and "text/html" in page.headers["content-type"]


def test_errors_map_to_http_status_codes(client, keys):
    admin, op = keys["admin"], keys["op1"]
    cyclic = {"name": "bad", "steps": [{"id": "a", "depends_on": ["b"]}, {"id": "b", "depends_on": ["a"]}]}
    assert client.post("/workflows", json=cyclic, headers=admin).status_code == 422
    assert client.post("/runs", json={"workflow": "missing"}, headers=op).status_code == 404
    assert client.get("/runs/not-a-uuid", headers=op).status_code == 404
    assert client.get("/runs/00000000-0000-0000-0000-000000000000", headers=op).status_code == 404
    assert client.get("/assets/not-a-uuid", headers=op).status_code == 404
    client.post("/workflows", json=SPEC, headers=admin)
    run_id = client.post("/runs", json={"workflow": "api-flow"}, headers=op).json()["id"]
    assert client.post(f"/runs/{run_id}/retry", headers=op).status_code == 409  # not failed
    assert client.post(f"/runs/{run_id}/cancel", headers=op).json() == {"cancelled": True}
    assert client.get(f"/runs/{run_id}", headers=op).json()["status"] == "cancelled"
