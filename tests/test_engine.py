from __future__ import annotations

import time

import pytest

from wes import Conflict, HandlerRegistry, PermanentError

from .conftest import drain, make_spec, outputs, statuses


def start(engine, spec, input=None, tenant="t1", **kw):
    engine.register_workflow(spec)
    return engine.start_run(tenant, spec.name, input or {}, **kw)[0]


# ---------------------------------------------------------------- happy paths


def test_linear_run_passes_outputs_downstream(engine):
    reg = HandlerRegistry()

    @reg.register("a")
    def a(ctx):
        return {"n": ctx.run_input["n"] + 1}

    @reg.register("b")
    def b(ctx):
        return {"n": ctx.deps["a"]["n"] * 10}

    run_id = start(engine, make_spec("linear", [{"id": "a"}, {"id": "b", "depends_on": ["a"]}]), {"n": 1})
    assert statuses(engine, run_id) == {"a": "ready", "b": "pending"}
    assert drain(engine, reg) == 2
    run = engine.get_run(run_id)
    assert run["status"] == "succeeded"
    assert outputs(engine, run_id)["b"] == {"n": 20}


def test_diamond_join_waits_for_both_branches(engine):
    reg = HandlerRegistry()
    for name in "abcd":
        reg.register(name)(lambda ctx: {"deps": sorted(ctx.deps)})
    spec = make_spec("diamond", [
        {"id": "a"},
        {"id": "b", "depends_on": ["a"]},
        {"id": "c", "depends_on": ["a"]},
        {"id": "d", "depends_on": ["b", "c"]},
    ])
    run_id = start(engine, spec)
    worker_types = reg.types()
    t = engine.claim("w", worker_types)
    engine.complete_step(t.run_id, t.step_id, t.attempt, "w", {})
    assert statuses(engine, run_id) == {"a": "succeeded", "b": "ready", "c": "ready", "d": "pending"}
    t = engine.claim("w", worker_types)
    engine.complete_step(t.run_id, t.step_id, t.attempt, "w", {})
    assert statuses(engine, run_id)["d"] == "pending"  # one branch is not enough
    drain(engine, reg)
    assert engine.get_run(run_id)["status"] == "succeeded"
    assert outputs(engine, run_id)["d"] == {"deps": ["b", "c"]}


def test_start_run_is_idempotent_per_tenant(engine):
    spec = make_spec("idem", [{"id": "a"}])
    engine.register_workflow(spec)
    first, created = engine.start_run("t1", "idem", idempotency_key="k1")
    again, created_again = engine.start_run("t1", "idem", idempotency_key="k1")
    other_tenant, created_other = engine.start_run("t2", "idem", idempotency_key="k1")
    assert created and not created_again and created_other
    assert first == again != other_tenant


def test_definitions_are_versioned_and_runs_pin_their_version(engine):
    v1 = engine.register_workflow(make_spec("wf", [{"id": "a"}]))
    assert engine.register_workflow(make_spec("wf", [{"id": "a"}])) == v1  # identical -> same version
    run_id, _ = engine.start_run("t1", "wf")
    v2 = engine.register_workflow(make_spec("wf", [{"id": "a"}, {"id": "b", "depends_on": ["a"]}]))
    assert (v1, v2) == (1, 2)
    assert set(statuses(engine, run_id)) == {"a"}  # in-flight run keeps the old shape
    assert set(statuses(engine, engine.start_run("t1", "wf")[0])) == {"a", "b"}


# ---------------------------------------------------------------- failure handling


def test_transient_failure_is_retried_then_succeeds(engine):
    reg = HandlerRegistry()
    calls = []

    @reg.register("flaky")
    def flaky(ctx):
        calls.append(ctx.attempt)
        if ctx.attempt < 3:
            raise RuntimeError("upstream 503")
        return {"ok": True}

    run_id = start(engine, make_spec("retry", [{"id": "flaky"}]))
    drain(engine, reg)
    assert calls == [1, 2, 3]
    assert engine.get_run(run_id)["status"] == "succeeded"
    types = [e["type"] for e in engine.list_events(run_id)]
    assert types.count("step.retry_scheduled") == 2


def test_backoff_delays_the_next_attempt(engine):
    reg = HandlerRegistry()

    @reg.register("a")
    def a(ctx):
        raise RuntimeError("boom")

    spec = make_spec("backoff", [{"id": "a", "retry": {"max_attempts": 3, "backoff_seconds": 30}}])
    run_id = start(engine, spec)
    assert drain(engine, reg) == 1  # second attempt is not claimable yet
    assert statuses(engine, run_id) == {"a": "ready"}


def test_exhausted_retries_fail_the_run_and_cancel_the_rest(engine):
    reg = HandlerRegistry()

    @reg.register("a")
    def a(ctx):
        raise RuntimeError("always")

    reg.register("b")(lambda ctx: {})
    spec = make_spec("fail", [
        {"id": "a", "retry": {"max_attempts": 2, "backoff_seconds": 0}},
        {"id": "b", "depends_on": ["a"]},
    ])
    run_id = start(engine, spec)
    assert drain(engine, reg) == 2
    run = engine.get_run(run_id)
    assert run["status"] == "failed" and "always" in run["error"]
    assert statuses(engine, run_id) == {"a": "failed", "b": "cancelled"}


def test_permanent_error_skips_retries(engine):
    reg = HandlerRegistry()

    @reg.register("a")
    def a(ctx):
        raise PermanentError("bad input")

    run_id = start(engine, make_spec("perm", [{"id": "a"}]))
    assert drain(engine, reg) == 1
    assert engine.get_run(run_id)["status"] == "failed"


def test_dead_worker_is_recovered_and_its_late_result_is_fenced_off(engine):
    spec = make_spec("lease", [{"id": "a"}])
    run_id = start(engine, spec)
    dead = engine.claim("worker-dead", ["a"], lease_seconds=0.05)
    assert dead.attempt == 1
    assert engine.claim("worker-2", ["a"]) is None  # leased, nobody else can take it
    time.sleep(0.1)
    assert engine.reap() == 1
    alive = engine.claim("worker-2", ["a"])
    assert alive.attempt == 2
    # The original worker wakes up and reports: rejected, it no longer owns the step.
    assert engine.complete_step(run_id, "a", dead.attempt, "worker-dead", {"from": "dead"}) is False
    assert engine.heartbeat(run_id, "a", dead.attempt, "worker-dead") is False
    assert engine.complete_step(run_id, "a", alive.attempt, "worker-2", {"from": "alive"}) is True
    assert outputs(engine, run_id)["a"] == {"from": "alive"}


def test_heartbeat_keeps_a_slow_step_alive(engine):
    run_id = start(engine, make_spec("hb", [{"id": "a"}]))
    t = engine.claim("w", ["a"], lease_seconds=0.2)
    time.sleep(0.12)
    assert engine.heartbeat(run_id, "a", t.attempt, "w", lease_seconds=0.2)
    time.sleep(0.12)  # past the original lease, inside the extended one
    assert engine.reap() == 0
    assert engine.complete_step(run_id, "a", t.attempt, "w", {})


def test_step_timeout_fails_the_attempt(engine):
    spec = make_spec("timeout", [
        {"id": "a", "timeout_seconds": 1, "retry": {"max_attempts": 1, "backoff_seconds": 0}}
    ])
    run_id = start(engine, spec)
    engine.claim("w", ["a"], lease_seconds=60)
    time.sleep(1.1)
    assert engine.reap() == 1
    run = engine.get_run(run_id)
    assert run["status"] == "failed" and "timeout" in run["error"]


def test_retry_run_resumes_from_the_failed_step(engine):
    reg = HandlerRegistry()
    executed = []
    healthy = {"ok": False}

    @reg.register("a")
    def a(ctx):
        executed.append("a")
        return {}

    @reg.register("b")
    def b(ctx):
        executed.append("b")
        if not healthy["ok"]:
            raise PermanentError("dependency down")
        return {}

    reg.register("c")(lambda ctx: executed.append("c") or {})
    spec = make_spec("resume", [
        {"id": "a"}, {"id": "b", "depends_on": ["a"]}, {"id": "c", "depends_on": ["b"]},
    ])
    run_id = start(engine, spec)
    drain(engine, reg)
    assert engine.get_run(run_id)["status"] == "failed"
    healthy["ok"] = True
    engine.retry_run(run_id)
    drain(engine, reg)
    assert engine.get_run(run_id)["status"] == "succeeded"
    assert executed == ["a", "b", "b", "c"]  # 'a' was not re-run
    with pytest.raises(Conflict):
        engine.retry_run(run_id)


def test_cancel_stops_new_work_and_rejects_in_flight_results(engine):
    spec = make_spec("cancel", [{"id": "a"}, {"id": "b", "depends_on": ["a"]}])
    run_id = start(engine, spec)
    t = engine.claim("w", ["a", "b"])
    assert engine.cancel_run(run_id) is True
    assert engine.cancel_run(run_id) is False  # already cancelled
    assert engine.complete_step(run_id, "a", t.attempt, "w", {}) is False
    assert statuses(engine, run_id) == {"a": "cancelled", "b": "cancelled"}
    assert engine.claim("w", ["a", "b"]) is None


# ---------------------------------------------------------------- fan-out, conditions, humans


def test_foreach_fans_out_and_joins_in_item_order(engine):
    reg = HandlerRegistry()
    reg.register("split")(lambda ctx: {"parts": [3, 1, 2]})
    reg.register("square")(lambda ctx: {"v": ctx.item ** 2, "i": ctx.item_index})
    reg.register("total")(lambda ctx: {"sum": sum(x["v"] for x in ctx.deps["square"]["items"])})
    spec = make_spec("map", [
        {"id": "split"},
        {"id": "square", "depends_on": ["split"], "foreach": "split.parts"},
        {"id": "total", "depends_on": ["square"]},
    ])
    run_id = start(engine, spec)
    assert drain(engine, reg) == 5
    out = outputs(engine, run_id)
    assert out["square"] == {"items": [{"v": 9, "i": 0}, {"v": 1, "i": 1}, {"v": 4, "i": 2}]}
    assert out["total"] == {"sum": 14}
    assert engine.get_run(run_id)["status"] == "succeeded"


def test_empty_foreach_completes_immediately(engine):
    reg = HandlerRegistry()
    reg.register("split")(lambda ctx: {"parts": []})
    reg.register("work")(lambda ctx: {})
    reg.register("after")(lambda ctx: {"n": len(ctx.deps["work"]["items"])})
    spec = make_spec("empty", [
        {"id": "split"},
        {"id": "work", "depends_on": ["split"], "foreach": "split.parts"},
        {"id": "after", "depends_on": ["work"]},
    ])
    run_id = start(engine, spec)
    drain(engine, reg)
    assert outputs(engine, run_id)["after"] == {"n": 0}


def test_foreach_over_a_non_list_fails_the_run(engine):
    reg = HandlerRegistry()
    reg.register("split")(lambda ctx: {"parts": "oops"})
    spec = make_spec("badmap", [
        {"id": "split"}, {"id": "work", "depends_on": ["split"], "foreach": "split.parts"},
    ])
    run_id = start(engine, spec)
    drain(engine, reg)
    run = engine.get_run(run_id)
    assert run["status"] == "failed" and "did not resolve to a list" in run["error"]


def test_failed_child_fails_the_run_and_retry_redoes_only_that_child(engine):
    reg = HandlerRegistry()
    seen = []
    poison = {"on": True}
    reg.register("split")(lambda ctx: {"parts": ["x", "y", "z"]})

    @reg.register("work")
    def work(ctx):
        seen.append(ctx.item)
        if ctx.item == "y" and poison["on"]:
            raise PermanentError("poison item")
        return {"item": ctx.item}

    spec = make_spec("childfail", [
        {"id": "split"}, {"id": "work", "depends_on": ["split"], "foreach": "split.parts"},
    ])
    run_id = start(engine, spec)
    drain(engine, reg)
    assert engine.get_run(run_id)["status"] == "failed"
    assert statuses(engine, run_id)["work"] == "failed"
    done_before = [s for s, st in statuses(engine, run_id).items() if st == "succeeded" and "#" in s]
    poison["on"] = False
    seen.clear()
    engine.retry_run(run_id)
    drain(engine, reg)
    assert engine.get_run(run_id)["status"] == "succeeded"
    redone = {f"work#{'xyz'.index(i)}" for i in seen}
    assert redone.isdisjoint(done_before) and "y" in seen
    assert [i["item"] for i in outputs(engine, run_id)["work"]["items"]] == ["x", "y", "z"]


def test_when_false_skips_the_step_and_dependents_still_run(engine):
    reg = HandlerRegistry()
    reg.register("check")(lambda ctx: {"needs_review": False})
    reg.register("publish")(lambda ctx: {"review": ctx.deps["review"]})
    spec = make_spec("cond", [
        {"id": "check"},
        {"id": "review", "kind": "human", "depends_on": ["check"], "when": "check.needs_review"},
        {"id": "publish", "depends_on": ["review"]},
    ])
    run_id = start(engine, spec)
    drain(engine, reg)
    assert statuses(engine, run_id) == {"check": "succeeded", "review": "skipped", "publish": "succeeded"}
    assert outputs(engine, run_id)["publish"] == {"review": None}


def test_human_step_parks_the_run_until_completed(engine):
    reg = HandlerRegistry()
    reg.register("check")(lambda ctx: {"needs_review": True})
    reg.register("publish")(lambda ctx: {"verdict": ctx.deps["review"]["verdict"]})
    spec = make_spec("human", [
        {"id": "check"},
        {"id": "review", "kind": "human", "depends_on": ["check"], "when": "check.needs_review"},
        {"id": "publish", "depends_on": ["review"]},
    ])
    run_id = start(engine, spec)
    drain(engine, reg)
    assert statuses(engine, run_id)["review"] == "waiting_human"
    assert engine.claim("w", reg.types() + ["review"]) is None  # workers never get human steps
    tasks = engine.list_human_tasks("t1")
    assert [(str(t["run_id"]), t["step_id"]) for t in tasks] == [(run_id, "review")]
    assert engine.list_human_tasks("someone-else") == []
    engine.complete_human_step(run_id, "review", {"verdict": "accept"}, actor="pat")
    with pytest.raises(Conflict):
        engine.complete_human_step(run_id, "review", {"verdict": "reject"})
    drain(engine, reg)
    assert outputs(engine, run_id)["publish"] == {"verdict": "accept"}
    assert engine.get_run(run_id)["status"] == "succeeded"


def test_overdue_human_task_is_escalated_once(engine):
    spec = make_spec("sla", [{"id": "review", "kind": "human", "sla_seconds": 1}])
    run_id = start(engine, spec)
    assert engine.escalate_overdue_human_tasks() == 0
    time.sleep(1.1)
    assert engine.escalate_overdue_human_tasks() == 1
    assert engine.escalate_overdue_human_tasks() == 0
    assert "step.sla_breached" in [e["type"] for e in engine.list_events(run_id)]
    assert statuses(engine, run_id)["review"] == "waiting_human"  # still waiting, just flagged


# ---------------------------------------------------------------- fairness and outbox


def test_tenant_concurrency_cap_and_fair_ordering(pool, engine):
    from wes import Engine

    capped = Engine(pool, tenant_max_running=2)
    spec = make_spec("fair", [{"id": "work"}])
    capped.register_workflow(spec)
    for _ in range(5):
        capped.start_run("noisy", "fair")
    capped.start_run("quiet", "fair")  # arrives last
    first = [capped.claim("w", ["work"]).tenant_id for _ in range(2)]
    assert first[0] == "noisy"
    assert "quiet" in first or capped.claim("w", ["work"]).tenant_id == "quiet"
    claimed = []
    while (t := capped.claim("w", ["work"])) is not None:
        claimed.append(t.tenant_id)
    with pool.connection() as conn:
        running = conn.execute(
            "SELECT tenant_id, count(*) AS n FROM step_runs WHERE status = 'running' GROUP BY 1"
        ).fetchall()
    by_tenant = {r["tenant_id"]: r["n"] for r in running}
    assert by_tenant == {"noisy": 2, "quiet": 1}  # noisy tenant held at its cap


def test_outbox_relays_in_order_exactly_once_when_sink_succeeds(engine):
    reg = HandlerRegistry()
    reg.register("a")(lambda ctx: {})
    run_id = start(engine, make_spec("outbox", [{"id": "a"}]))
    drain(engine, reg)
    seen = []
    assert engine.relay_outbox(seen.append) == len(seen) > 0
    assert [e["type"] for e in seen] == [
        "run.started", "step.ready", "step.started", "step.succeeded", "run.succeeded"
    ]
    assert [e["id"] for e in seen] == sorted(e["id"] for e in seen)
    assert engine.relay_outbox(seen.append) == 0


def test_outbox_redelivers_when_the_sink_fails(engine):
    start(engine, make_spec("outbox2", [{"id": "a"}]))

    def broken(_):
        raise ConnectionError("kafka down")

    with pytest.raises(ConnectionError):
        engine.relay_outbox(broken)
    seen = []
    assert engine.relay_outbox(seen.append) == 2  # nothing was lost
