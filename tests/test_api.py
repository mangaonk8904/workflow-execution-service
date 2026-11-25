from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from wes import HandlerRegistry
from wes.api import create_app

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
def client(engine):
    with TestClient(create_app(engine)) as c:
        yield c


def test_full_flow_over_http(client, engine):
    reg = HandlerRegistry()
    reg.register("a")(lambda ctx: {"x": 1})
    reg.register("b")(lambda ctx: {"approved_by": ctx.deps["approve"]["by"]})

    assert client.get("/healthz").json() == {"status": "ok"}
    assert client.post("/workflows", json=SPEC).json() == {"name": "api-flow", "version": 1}

    body = {"tenant_id": "t1", "workflow": "api-flow", "input": {"k": "v"}}
    r = client.post("/runs", json=body, headers={"Idempotency-Key": "abc"})
    assert r.status_code == 201
    run_id = r.json()["id"]
    replay = client.post("/runs", json=body, headers={"Idempotency-Key": "abc"})
    assert replay.status_code == 200 and replay.json()["id"] == run_id

    drain(engine, reg)
    tasks = client.get("/human-tasks", params={"tenant_id": "t1"}).json()
    assert [t["step_id"] for t in tasks] == ["approve"]
    r = client.post(f"/runs/{run_id}/steps/approve/complete",
                    json={"output": {"by": "sam"}, "actor": "sam"})
    assert r.status_code == 200
    assert client.post(f"/runs/{run_id}/steps/approve/complete", json={}).status_code == 409

    drain(engine, reg)
    run = client.get(f"/runs/{run_id}").json()
    assert run["status"] == "succeeded"
    assert [s["step_id"] for s in run["steps"]] == ["a", "approve", "b"]
    assert run["steps"][-1]["output"] == {"approved_by": "sam"}
    events = client.get(f"/runs/{run_id}/events").json()
    assert events[0]["type"] == "run.started" and events[-1]["type"] == "run.succeeded"


def test_errors_map_to_http_status_codes(client):
    cyclic = {"name": "bad", "steps": [{"id": "a", "depends_on": ["b"]}, {"id": "b", "depends_on": ["a"]}]}
    assert client.post("/workflows", json=cyclic).status_code == 422
    assert client.post("/runs", json={"tenant_id": "t", "workflow": "missing"}).status_code == 404
    assert client.get("/runs/not-a-uuid").status_code == 404
    assert client.get("/runs/00000000-0000-0000-0000-000000000000").status_code == 404
    client.post("/workflows", json=SPEC)
    run_id = client.post("/runs", json={"tenant_id": "t", "workflow": "api-flow"}).json()["id"]
    assert client.post(f"/runs/{run_id}/retry").status_code == 409  # not failed
    assert client.post(f"/runs/{run_id}/cancel").json() == {"cancelled": True}
    assert client.get(f"/runs/{run_id}").json()["status"] == "cancelled"
