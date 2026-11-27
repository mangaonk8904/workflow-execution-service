from __future__ import annotations

import time

import pytest
from pydantic import ValidationError

from wes import (
    AssetClient, AssetError, HandlerRegistry, Invalid, LeaseLost, NotFound, Policy, ref,
)
from wes.models import PointerSelector, TimeSelector

from .conftest import drain, make_spec, outputs, statuses

VIDEO = {"duration_seconds": 60.0, "width": 1920, "height": 1080}


def upload(catalog, data=b"video bytes", *, tenant="t1", kind="video", policy=None, metadata=None,
           name="src.mp4", media_type="video/mp4"):
    return catalog.upload(tenant, data, kind=kind, media_type=media_type, name=name,
                          policy=policy or {"consent": ["a", "b"], "pii": "present"},
                          metadata=VIDEO if metadata is None else metadata)


def start(engine, steps, input=None, tenant="t1", name="assets"):
    spec = make_spec(name, steps)
    engine.register_workflow(spec)
    return engine.start_run(tenant, spec.name, input or {})[0]


def client_for(catalog, task, worker_id, transforms=None):
    return AssetClient(catalog, tenant_id=task.tenant_id, run_id=task.run_id, step_id=task.step_id,
                       attempt=task.attempt, worker_id=worker_id, handler={"name": task.step_type},
                       params=task.params, policy_transforms=transforms or {})


# ---------------------------------------------------------------- commit protocol


def test_assets_are_staged_then_committed_with_their_provenance(engine, catalog):
    src = upload(catalog)
    reg = HandlerRegistry()

    @reg.register("thumb", version="1.2.0", models={"renderer": "r-7"})
    def thumb(ctx):
        a = ctx.assets.put(b"jpeg", name="thumbnail", kind="image", media_type="image/jpeg",
                           derived_from=[(ctx.run_input["src"], "rendition_of",
                                          {"type": "region", "x": 0, "y": 0, "w": 640, "h": 360, "t": 12.5})],
                           metadata={"width": 640, "height": 360})
        # Staged assets are invisible outside the attempt.
        with pytest.raises(NotFound):
            ctx.assets.catalog.get(str(a["id"]))
        return {"thumbnail": ref(a)}

    run_id = start(engine, [{"id": "thumb", "params": {"size": 640}}], {"src": ref(src)})
    drain(engine, reg, catalog=catalog)
    assert engine.get_run(run_id)["status"] == "succeeded"
    asset = catalog.get(outputs(engine, run_id)["thumb"]["thumbnail"]["$asset"])
    assert asset["status"] == "committed" and asset["kind"] == "image"
    assert asset["policy"] == {"consent": ["a", "b"], "pii": "present", "retention_days": None,
                               "restrictions": []}
    edge = catalog.lineage(str(asset["id"]))["edges"][0]
    assert edge["relation"] == "rendition_of" and edge["parent_selector"]["t"] == 12.5
    act = edge["activity"]
    assert act["handler"] == {"name": "thumb", "version": "1.2.0", "models": {"renderer": "r-7"}}
    assert act["run_id"] == run_id and act["attempt"] == 1 and len(act["params_sha256"]) == 64


def test_a_failed_attempt_leaves_nothing_behind_and_the_retry_commits_once(engine, catalog):
    src = upload(catalog)
    reg = HandlerRegistry()
    calls = []

    @reg.register("work")
    def work(ctx):
        calls.append(ctx.attempt)
        a = ctx.assets.put(b"out", name="out", kind="text", media_type="text/plain",
                           derived_from=[ctx.run_input["src"]])
        if ctx.attempt == 1:
            raise RuntimeError("crashed after writing")
        return {"out": ref(a)}

    run_id = start(engine, [{"id": "work"}], {"src": ref(src)})
    drain(engine, reg, catalog=catalog)
    assert calls == [1, 2] and engine.get_run(run_id)["status"] == "succeeded"
    with engine.pool.connection() as conn:
        rows = conn.execute("SELECT attempt, status FROM assets WHERE run_id = %s ORDER BY attempt",
                            (run_id,)).fetchall()
    assert [(r["attempt"], r["status"]) for r in rows] == [(1, "discarded"), (2, "committed")]
    # Same bytes, one object in the store.
    assert len([p for p in catalog.store.root.rglob("*") if p.is_file()]) == 2  # src + out


def test_a_worker_that_lost_its_lease_cannot_write_or_commit(engine, catalog):
    src = upload(catalog)
    run_id = start(engine, [{"id": "a"}], {"src": ref(src)})
    dead = engine.claim("worker-dead", ["a"], lease_seconds=0.05)
    zombie = client_for(catalog, dead, "worker-dead")
    early = zombie.put(b"early", name="out", kind="text", media_type="text/plain",
                       derived_from=[src])
    time.sleep(0.1)
    assert engine.reap() == 1
    alive = engine.claim("worker-2", ["a"])

    with pytest.raises(LeaseLost):
        zombie.put(b"late", name="out", kind="text", media_type="text/plain", derived_from=[src])
    assert engine.complete_step(run_id, "a", dead.attempt, "worker-dead", {"x": ref(early)}) is False
    assert engine.complete_step(run_id, "a", alive.attempt, "worker-2", {}) is True
    assert catalog.get(str(early["id"]), committed_only=False)["status"] == "discarded"
    assert catalog.run_assets(run_id) == []


def test_reputting_a_slot_in_one_attempt_replaces_it(engine, catalog):
    src = upload(catalog)
    reg = HandlerRegistry()

    @reg.register("a")
    def a(ctx):
        ctx.assets.put(b"draft", name="out", kind="text", media_type="text/plain", derived_from=[src])
        final = ctx.assets.put(b"final", name="out", kind="text", media_type="text/plain",
                               derived_from=[src])
        return {"out": ref(final)}

    run_id = start(engine, [{"id": "a"}])
    drain(engine, reg, catalog=catalog)
    [only] = catalog.run_assets(run_id)
    with catalog.open(str(only["id"])) as f:
        assert f.read() == b"final"


def test_outputs_may_only_reference_visible_assets(engine, catalog):
    mine, theirs = upload(catalog), upload(catalog, tenant="t2")
    reg = HandlerRegistry()
    reg.register("ok")(lambda ctx: {"passthrough": ref(mine)})
    reg.register("leak")(lambda ctx: {"other": ref(theirs)})
    reg.register("bogus")(lambda ctx: {"x": {"$asset": "not-a-uuid"}})
    ok = start(engine, [{"id": "ok"}], name="ok")
    leak = start(engine, [{"id": "leak"}], name="leak")
    bogus = start(engine, [{"id": "bogus"}], name="bogus")
    drain(engine, reg, catalog=catalog)
    assert engine.get_run(ok)["status"] == "succeeded"
    assert "not visible" in engine.get_run(leak)["error"]
    assert "malformed" in engine.get_run(bogus)["error"]
    assert engine.get_run(leak)["steps"][0]["attempt"] == 1  # permanent: no retries


def test_human_output_references_are_checked(engine, catalog):
    theirs = upload(catalog, tenant="t2")
    run_id = start(engine, [{"id": "review", "kind": "human"}])
    with pytest.raises(Invalid):
        engine.complete_human_step(run_id, "review", {"evidence": ref(theirs)})
    assert statuses(engine, run_id) == {"review": "waiting_human"}


# ---------------------------------------------------------------- provenance rules


def _try_put(engine, catalog, put_kwargs, transforms=None, steps=None):
    src = upload(catalog)
    reg = HandlerRegistry()
    errors = []

    @reg.register("a")
    def a(ctx):
        kwargs = put_kwargs(src) if callable(put_kwargs) else put_kwargs
        try:
            return {"out": ref(ctx.assets.put(b"x", **kwargs))}
        except AssetError as exc:
            errors.append(str(exc))
            raise

    start(engine, steps or [{"id": "a", "policy_transforms": transforms or {}}])
    drain(engine, reg, catalog=catalog)
    return errors


def test_outputs_need_a_parent_that_establishes_provenance(engine, catalog):
    base = {"name": "o", "kind": "text", "media_type": "text/plain"}
    assert "at least one parent" in _try_put(engine, catalog, {**base, "derived_from": []})[0]
    errs = _try_put(engine, catalog, lambda s: {**base, "derived_from": [(s, "references")]})
    assert "at least one parent" in errs[0]


def test_selectors_must_fit_the_asset(engine, catalog):
    base = {"name": "o", "kind": "text", "media_type": "text/plain"}
    errs = _try_put(engine, catalog, lambda s: {
        **base, "derived_from": [(s, "transcript_of", {"type": "page", "page": 1})]})
    assert "page selector cannot point into a video" in errs[0]
    errs = _try_put(engine, catalog, lambda s: {
        **base, "derived_from": [(s, "transcript_of", {"type": "time", "start": 50, "end": 61})]})
    assert "60.0s long" in errs[0]
    errs = _try_put(engine, catalog, lambda s: {
        **base, "derived_from": [{"parent": str(s["id"]), "child_selector": {"type": "time", "start": 0, "end": 1}}]})
    assert "cannot point into a text asset" in errs[0]
    with pytest.raises(ValidationError):
        TimeSelector(start=5, end=5)
    with pytest.raises(ValidationError):
        PointerSelector(pointer="no-leading-slash")


def test_policy_is_inherited_strictly_from_all_parents(engine, catalog):
    a = upload(catalog, b"a", policy={"consent": ["x", "y"], "pii": "none", "retention_days": 90,
                                      "restrictions": ["no_training"]})
    b = upload(catalog, b"b", policy={"consent": ["y", "z"], "pii": "unknown", "retention_days": 30})
    c = upload(catalog, b"c", policy={"consent": [], "pii": "present"})
    reg = HandlerRegistry()

    @reg.register("merge")
    def merge(ctx):
        out = ctx.assets.put(b"m", name="m", kind="video", media_type="video/mp4",
                             derived_from=[a, b, (c, "references")], metadata={})
        return {"m": ref(out)}

    run_id = start(engine, [{"id": "merge"}])
    drain(engine, reg, catalog=catalog)
    merged = catalog.get(outputs(engine, run_id)["merge"]["m"]["$asset"])
    assert merged["policy"] == {"consent": ["y"], "pii": "unknown", "retention_days": 30,
                                "restrictions": ["no_training"]}


def test_steps_can_tighten_but_only_loosen_what_their_spec_grants(engine, catalog):
    base = {"name": "o", "kind": "text", "media_type": "text/plain"}
    put = lambda pol: (lambda s: {**base, "derived_from": [s], "policy": pol})  # noqa: E731
    assert "cannot widen consent" in _try_put(engine, catalog, put({"consent": ["a", "c"]}))[0]
    assert "may not lower pii" in _try_put(engine, catalog, put({"pii": "redacted"}))[0]
    assert _try_put(engine, catalog, put({"pii": "redacted"}), transforms={"pii": "redacted"}) == []
    assert _try_put(engine, catalog, put({"consent": ["a"], "retention_days": 7,
                                          "restrictions": ["embargo"]})) == []
    with pytest.raises(ValidationError, match="never be loosened"):
        make_spec("bad", [{"id": "a", "policy_transforms": {"consent": "everyone"}}])


def test_policy_gate_applies_to_referenced_upstream_assets_too(engine, catalog):
    src = upload(catalog, policy={"consent": ["internal"], "pii": "present"})
    reg = HandlerRegistry()
    reg.register("deliver")(lambda ctx: {"file": ctx.run_input["src"]})
    run_id = start(engine, [{"id": "deliver", "requires_policy": {"consent": ["share"]}}],
                   {"src": ref(src)})
    drain(engine, reg, catalog=catalog)
    run = engine.get_run(run_id)
    assert run["status"] == "failed" and "missing consent ['share']" in run["error"]


def test_policy_model_rules():
    p = Policy(consent=["b", "a", "a"], pii="redacted")
    assert p.consent == ["a", "b"]
    assert Policy.inherit([p, Policy(consent=["a"], pii="none")]).model_dump() == {
        "consent": ["a"], "pii": "redacted", "retention_days": None, "restrictions": []}
    with pytest.raises(ValueError, match="extend retention"):
        Policy(retention_days=10).apply({"retention_days": None}, {})
    with pytest.raises(ValueError, match="drop restrictions"):
        Policy(restrictions=["x"]).apply({"restrictions": []}, {})
    _, changes = Policy(pii="present").apply({"pii": "none"}, {"pii": "none"})
    assert changes == [{"field": "pii", "from": "present", "to": "none"}]


# ---------------------------------------------------------------- lineage and housekeeping


def test_lineage_walks_both_ways_and_hides_uncommitted_work(engine, catalog):
    src = upload(catalog)
    reg = HandlerRegistry()

    @reg.register("chain")
    def chain(ctx):
        a = ctx.assets.put(b"1", name="one", kind="audio", media_type="audio/flac",
                           derived_from=[src], metadata={"duration_seconds": 60})
        b = ctx.assets.put(b"2", name="two", kind="text", media_type="text/plain",
                           derived_from=[(a, "transcript_of")])
        return {"b": ref(b)}

    run_id = start(engine, [{"id": "chain"}])
    drain(engine, reg, catalog=catalog)
    leaf = outputs(engine, run_id)["chain"]["b"]["$asset"]
    up = catalog.lineage(leaf, direction="up")
    assert [(n["name"], n["depth"]) for n in up["nodes"]] == [("two", 0), ("one", 1), ("src.mp4", 2)]
    assert len(catalog.lineage(leaf, direction="up", depth=1)["nodes"]) == 2
    down = catalog.lineage(str(src["id"]), direction="down")
    assert {n["name"] for n in down["nodes"]} == {"src.mp4", "one", "two"}

    # A second run's failed attempt stages a child of src; it must not show up.
    reg2 = HandlerRegistry()

    @reg2.register("fails")
    def fails(ctx):
        ctx.assets.put(b"z", name="ghost", kind="text", media_type="text/plain", derived_from=[src])
        raise RuntimeError("boom")

    start(engine, [{"id": "fails", "retry": {"max_attempts": 1}}], name="fails")
    drain(engine, reg2, catalog=catalog)
    assert "ghost" not in {n["name"] for n in catalog.lineage(str(src["id"]), direction="down")["nodes"]}
    with pytest.raises(NotFound):
        catalog.lineage(leaf, "other-tenant")


def test_garbage_collection_keeps_blobs_still_in_use(engine, catalog):
    src = upload(catalog)
    reg = HandlerRegistry()

    @reg.register("a")
    def a(ctx):
        shared = ctx.assets.put(b"same bytes", name="shared", kind="text", media_type="text/plain",
                                derived_from=[src])
        ctx.assets.put(b"only here", name="lonely", kind="text", media_type="text/plain",
                       derived_from=[src])
        if ctx.attempt == 1:
            raise RuntimeError("retry me")
        ctx.assets.put(b"x", name="lonely", kind="text", media_type="text/plain", derived_from=[src])
        return {"s": ref(shared)}

    run_id = start(engine, [{"id": "a"}])
    drain(engine, reg, catalog=catalog)
    store = catalog.store
    lonely_uri = store.uri_for("t1", __import__("hashlib").sha256(b"only here").hexdigest())
    shared_uri = store.uri_for("t1", __import__("hashlib").sha256(b"same bytes").hexdigest())
    assert store.exists(lonely_uri) and store.exists(shared_uri)
    assert catalog.collect_garbage(grace_seconds=3600) == 0  # too recent
    assert catalog.collect_garbage(grace_seconds=0) == 3     # attempt 1's three rows
    assert not store.exists(lonely_uri)  # nothing else uses it
    assert store.exists(shared_uri)      # attempt 2 committed the same bytes
    assert [a["name"] for a in catalog.run_assets(run_id)] == ["lonely", "shared"]


def test_upload_validation(catalog):
    with pytest.raises(Invalid, match="unknown asset kind"):
        catalog.upload("t1", b"x", kind="hologram", media_type="x/y", name="n")
    with pytest.raises(Invalid, match="MIME"):
        catalog.upload("t1", b"x", kind="text", media_type="text", name="n")
    with pytest.raises(Invalid, match="policy"):
        catalog.upload("t1", b"x", kind="text", media_type="text/plain", name="n",
                       policy={"pii": "sort of"})
    with pytest.raises(ValueError, match="not safe"):
        catalog.upload("../etc", b"x", kind="text", media_type="text/plain", name="n")
    small = type(catalog)(catalog.pool, catalog.store, max_bytes=3)
    with pytest.raises(Invalid, match="exceeds"):
        small.upload("t1", b"four", kind="text", media_type="text/plain", name="n")
    a = catalog.upload("t1", b"hello", kind="text", media_type="text/plain", name="n")
    assert a["policy"]["pii"] == "unknown" and a["size_bytes"] == 5 and catalog.verify(str(a["id"]))
    with pytest.raises(NotFound):
        catalog.get(str(a["id"]), "t2")
