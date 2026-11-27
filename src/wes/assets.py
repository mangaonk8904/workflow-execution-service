"""The asset catalog: every piece of media or data, its provenance, and its policy.

Write protocol (why a retried or fenced-out step can never leak an asset)
------------------------------------------------------------------------
1. A handler calls ``ctx.assets.put(...)``. The bytes are hashed into a local
   temp file.
2. One transaction checks that the caller still holds the step's current
   attempt, validates parents, selectors and policy, and inserts the asset as
   ``staged`` along with its lineage edges.
3. The bytes are uploaded to the content-addressed blob store.
4. When the engine accepts the step's result, ``commit_attempt`` promotes that
   attempt's staged assets to ``committed`` in the same transaction as the
   state change. Any other outcome (failure, retry, lease lost, cancel) marks
   them ``discarded``. Nothing outside the attempt ever sees a staged asset.

Blob garbage collection removes a discarded asset's object only when no live
asset in the tenant has the same hash. Steps 2 and the collector serialise on
an advisory lock per (tenant, sha256), and uploads happen after step 2
commits, so the collector can never delete an object a new asset needs.
"""

from __future__ import annotations

import hashlib
import json
import uuid
from typing import Any, BinaryIO, Iterable

from psycopg.types.json import Jsonb
from psycopg_pool import ConnectionPool
from pydantic import ValidationError

from .blobstore import BlobStore, BlobTooLarge, Spooled, spool
from .errors import Invalid, NotFound
from .handlers import PermanentError
from .models import (
    NON_INHERITING,
    Derivation,
    Policy,
    PolicyRequirement,
    check_selector,
)

KINDS = ("image", "video", "audio", "document", "text", "structured", "bundle")
REF_KEY = "$asset"
PUBLIC_COLUMNS = (
    "id, tenant_id, kind, media_type, name, sha256, size_bytes, metadata, policy, status, "
    "origin, run_id, step_id, attempt, created_by, created_at, committed_at"
)


class AssetError(PermanentError):
    """A handler used the asset API wrongly. Retrying would not help."""


class LeaseLost(Exception):
    """The attempt writing this asset is no longer current; stop working."""


def ref(asset: dict[str, Any] | str) -> dict[str, str]:
    """`{"$asset": id}`: how step outputs point at assets."""
    return {REF_KEY: str(asset["id"] if isinstance(asset, dict) else asset)}


def asset_refs(value: Any) -> set[str]:
    """Every asset id referenced anywhere inside a JSON value."""
    found: set[str] = set()
    stack = [value]
    while stack:
        v = stack.pop()
        if isinstance(v, dict):
            if set(v) == {REF_KEY}:
                found.add(str(v[REF_KEY]))
            else:
                stack.extend(v.values())
        elif isinstance(v, list):
            stack.extend(v)
    return found


def _uuid_or_none(value: str) -> str | None:
    try:
        return str(uuid.UUID(str(value)))
    except ValueError:
        return None


def _lock_content(conn, tenant_id: str, sha256: str) -> None:
    conn.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", (f"blob:{tenant_id}:{sha256}",))


def _check_kind(kind: str, media_type: str, name: str) -> None:
    if kind not in KINDS:
        raise Invalid(f"unknown asset kind {kind!r}; expected one of {list(KINDS)}")
    if not media_type or "/" not in media_type or len(media_type) > 255:
        raise Invalid(f"media_type {media_type!r} is not a MIME type")
    if not name or len(name) > 255:
        raise Invalid("asset name must be 1-255 characters")


# ---------------------------------------------------------------- engine hooks
# Called by the engine inside its own transaction, after it has locked the run
# and confirmed the attempt is current.


def commit_attempt(
    conn, run: dict, step: dict, output: Any, requirement: PolicyRequirement | None
) -> tuple[list[dict], str | None]:
    """Promote the attempt's staged assets. Returns (committed, error).

    On error nothing is promoted and the caller fails the step permanently:
    the output references an asset it may not, or an asset breaks the step's
    policy gate.
    """
    staged = conn.execute(
        "SELECT id, kind, name, sha256, policy FROM assets WHERE run_id = %s AND step_id = %s "
        "AND attempt = %s AND worker_id = %s AND status = 'staged'",
        (step["run_id"], step["step_id"], step["attempt"], step["lease_owner"]),
    ).fetchall()
    staged_ids = {str(a["id"]) for a in staged}
    referenced = asset_refs(output)
    error = _check_refs(conn, run["tenant_id"], referenced - staged_ids)
    if error:
        return [], error
    if requirement:
        others = conn.execute(
            "SELECT id, name, policy FROM assets WHERE id = ANY(%s)",
            (list(referenced - staged_ids),),
        ).fetchall() if referenced - staged_ids else []
        for a in [*staged, *others]:
            problems = requirement.violations(Policy.model_validate(a["policy"]))
            if problems:
                return [], f"asset {a['id']} ({a['name']}) fails the step's policy gate: " + "; ".join(problems)
    if staged:
        conn.execute(
            "UPDATE assets SET status = 'committed', committed_at = now() WHERE id = ANY(%s)",
            ([a["id"] for a in staged],),
        )
    # Whatever earlier attempts of this step left staged can never be committed now.
    conn.execute(
        "UPDATE assets SET status = 'discarded' WHERE run_id = %s AND step_id = %s "
        "AND status = 'staged' AND NOT (id = ANY(%s))",
        (step["run_id"], step["step_id"], [a["id"] for a in staged]),
    )
    return [
        {"id": str(a["id"]), "name": a["name"], "kind": a["kind"], "sha256": a["sha256"]}
        for a in staged
    ], None


def discard_attempt(conn, run_id, step_id: str, attempt: int | None = None) -> int:
    """Mark an attempt's (or every attempt's) staged assets discarded."""
    if attempt is None:
        cur = conn.execute(
            "UPDATE assets SET status = 'discarded' WHERE run_id = %s AND step_id = %s "
            "AND status = 'staged'",
            (run_id, step_id),
        )
    else:
        cur = conn.execute(
            "UPDATE assets SET status = 'discarded' WHERE run_id = %s AND step_id = %s "
            "AND attempt = %s AND status = 'staged'",
            (run_id, step_id, attempt),
        )
    return cur.rowcount


def check_refs(conn, tenant_id: str, refs: Iterable[str]) -> str | None:
    """Every id must be a committed asset of this tenant. Returns an error or None."""
    return _check_refs(conn, tenant_id, set(refs))


def _check_refs(conn, tenant_id: str, refs: set[str]) -> str | None:
    if not refs:
        return None
    ids = {r: _uuid_or_none(r) for r in refs}
    bad = sorted(r for r, u in ids.items() if u is None)
    if bad:
        return f"output references malformed asset ids: {bad}"
    found = {
        str(r["id"])
        for r in conn.execute(
            "SELECT id FROM assets WHERE id = ANY(%s) AND tenant_id = %s AND status = 'committed'",
            (list(ids.values()), tenant_id),
        ).fetchall()
    }
    missing = sorted(set(ids.values()) - found)
    if missing:
        return f"output references assets that do not exist or are not visible: {missing}"
    return None


# ---------------------------------------------------------------- catalog


class AssetCatalog:
    def __init__(self, pool: ConnectionPool, store: BlobStore, *, max_bytes: int | None = None):
        self.pool = pool
        self.store = store
        self.max_bytes = max_bytes

    # -------------------------------------------------------------- writes

    def upload(
        self,
        tenant_id: str,
        data: bytes | BinaryIO | str,
        *,
        kind: str,
        media_type: str,
        name: str,
        metadata: dict[str, Any] | None = None,
        policy: dict[str, Any] | Policy | None = None,
        created_by: str | None = None,
    ) -> dict[str, Any]:
        """Bring outside content into the catalog. It is committed immediately."""
        _check_kind(kind, media_type, name)
        try:
            pol = policy if isinstance(policy, Policy) else Policy.model_validate(policy or {})
        except ValidationError as exc:
            raise Invalid(f"invalid policy: {exc.errors()[0]['msg']}") from None
        spooled = self._spool(data)
        try:
            asset_id = uuid.uuid4()
            uri = self.store.uri_for(tenant_id, spooled.sha256)
            with self.pool.connection() as conn, conn.transaction():
                _lock_content(conn, tenant_id, spooled.sha256)
                conn.execute(
                    "INSERT INTO assets (id, tenant_id, kind, media_type, name, sha256, size_bytes, "
                    "uri, metadata, policy, status, origin, created_by, committed_at) VALUES "
                    "(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'committed', 'upload', %s, now())",
                    (asset_id, tenant_id, kind, media_type, name, spooled.sha256, spooled.size,
                     uri, Jsonb(metadata or {}), Jsonb(pol.model_dump()), created_by),
                )
            try:
                self.store.upload(spooled, tenant_id)
            except Exception:
                with self.pool.connection() as conn:
                    conn.execute("UPDATE assets SET status = 'discarded' WHERE id = %s", (asset_id,))
                raise
        finally:
            spooled.cleanup()
        return self.get(str(asset_id), tenant_id)

    def stage(
        self,
        *,
        tenant_id: str,
        run_id: str,
        step_id: str,
        attempt: int,
        worker_id: str,
        data: bytes | BinaryIO | str,
        name: str,
        kind: str,
        media_type: str,
        derived_from: list[Derivation],
        metadata: dict[str, Any] | None = None,
        policy: dict[str, Any] | None = None,
        policy_transforms: dict[str, str] | None = None,
        activity: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Write a step's output asset as staged. See the module docstring."""
        try:
            _check_kind(kind, media_type, name)
        except Invalid as exc:
            raise AssetError(str(exc)) from None
        if not any(d.relation not in NON_INHERITING for d in derived_from):
            raise AssetError(
                f"asset '{name}' needs at least one parent it is derived from "
                "(a 'references' edge alone does not establish provenance)"
            )
        metadata = metadata or {}
        for d in derived_from:
            if d.child_selector is not None:
                try:
                    check_selector(d.child_selector, kind, metadata)
                except ValueError as exc:
                    raise AssetError(f"child selector on '{name}': {exc}") from None
        spooled = self._spool(data)
        asset_id = uuid.uuid4()
        try:
            with self.pool.connection() as conn, conn.transaction():
                current = conn.execute(
                    "SELECT status, attempt, lease_owner FROM step_runs "
                    "WHERE run_id = %s AND step_id = %s",
                    (run_id, step_id),
                ).fetchone()
                if (
                    not current or current["status"] != "running"
                    or current["attempt"] != attempt or current["lease_owner"] != worker_id
                ):
                    raise LeaseLost(f"{run_id}/{step_id} attempt {attempt} is no longer current")
                parents = self._parents(
                    conn, tenant_id, derived_from, (run_id, step_id, attempt, worker_id)
                )
                inheriting = [
                    Policy.model_validate(parents[d.parent]["policy"])
                    for d in derived_from if d.relation not in NON_INHERITING
                ]
                try:
                    final, changes = Policy.inherit(inheriting).apply(policy, policy_transforms or {})
                except ValueError as exc:
                    raise AssetError(f"policy for '{name}': {exc}") from None
                edge_activity = {**(activity or {}), "policy_changes": changes} if changes else (activity or {})
                _lock_content(conn, tenant_id, spooled.sha256)
                # Re-putting a slot within the same attempt replaces the earlier version.
                conn.execute(
                    "UPDATE assets SET status = 'discarded' WHERE run_id = %s AND step_id = %s "
                    "AND attempt = %s AND name = %s AND status = 'staged'",
                    (run_id, step_id, attempt, name),
                )
                conn.execute(
                    "INSERT INTO assets (id, tenant_id, kind, media_type, name, sha256, size_bytes, "
                    "uri, metadata, policy, status, origin, run_id, step_id, attempt, worker_id) VALUES "
                    "(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'staged', 'step', %s, %s, %s, %s)",
                    (asset_id, tenant_id, kind, media_type, name, spooled.sha256, spooled.size,
                     self.store.uri_for(tenant_id, spooled.sha256), Jsonb(metadata),
                     Jsonb(final.model_dump()), run_id, step_id, attempt, worker_id),
                )
                with conn.cursor() as cur:
                    cur.executemany(
                        "INSERT INTO asset_edges (tenant_id, child_id, parent_id, relation, "
                        "child_selector, parent_selector, activity) VALUES (%s, %s, %s, %s, %s, %s, %s)",
                        [
                            (tenant_id, asset_id, d.parent, d.relation,
                             Jsonb(d.child_selector.model_dump()) if d.child_selector else None,
                             Jsonb(d.parent_selector.model_dump()) if d.parent_selector else None,
                             Jsonb(edge_activity))
                            for d in derived_from
                        ],
                    )
            try:
                self.store.upload(spooled, tenant_id)
            except Exception:
                with self.pool.connection() as conn:  # never let a row point at a missing object
                    conn.execute("UPDATE assets SET status = 'discarded' WHERE id = %s", (asset_id,))
                raise
        finally:
            spooled.cleanup()
        with self.pool.connection() as conn:
            return conn.execute(
                f"SELECT {PUBLIC_COLUMNS} FROM assets WHERE id = %s", (asset_id,)
            ).fetchone()

    def _spool(self, data) -> Spooled:
        try:
            return spool(data, self.max_bytes)
        except BlobTooLarge as exc:
            raise Invalid(str(exc)) from None

    def _parents(
        self, conn, tenant_id: str, derived_from: list[Derivation], attempt_key: tuple
    ) -> dict[str, dict]:
        """Load and check parents: committed assets of this tenant, or assets this
        same attempt staged earlier (they commit or are discarded together)."""
        wanted = {}
        for d in derived_from:
            pid = _uuid_or_none(d.parent)
            if pid is None:
                raise AssetError(f"malformed parent asset id {d.parent!r}")
            d.parent = pid
            wanted[pid] = d
        rows = {
            str(r["id"]): r
            for r in conn.execute(
                "SELECT id, tenant_id, kind, status, policy, metadata, run_id, step_id, attempt, "
                "worker_id FROM assets WHERE id = ANY(%s)",
                (list(wanted),),
            ).fetchall()
        }
        for pid in wanted:
            row = rows.get(pid)
            same_attempt = (
                row is not None and row["status"] == "staged"
                and (str(row["run_id"]), row["step_id"], row["attempt"], row["worker_id"])
                == (str(attempt_key[0]), *attempt_key[1:])
            )
            if not row or row["tenant_id"] != tenant_id or not (
                row["status"] == "committed" or same_attempt
            ):
                raise AssetError(f"parent asset {pid} does not exist or is not visible")
        for d in derived_from:
            if d.parent_selector is not None:
                parent = rows[d.parent]
                try:
                    check_selector(d.parent_selector, parent["kind"], parent["metadata"])
                except ValueError as exc:
                    raise AssetError(f"selector on parent {d.parent}: {exc}") from None
        return rows

    # -------------------------------------------------------------- reads

    def get(self, asset_id: str, tenant_id: str | None = None, *, committed_only: bool = True) -> dict:
        """Asset metadata. With tenant_id, another tenant's asset is reported as not found."""
        aid = _uuid_or_none(asset_id)
        row = None
        if aid:
            with self.pool.connection() as conn:
                row = conn.execute(
                    f"SELECT {PUBLIC_COLUMNS} FROM assets WHERE id = %s", (aid,)
                ).fetchone()
        if (
            not row
            or (tenant_id is not None and row["tenant_id"] != tenant_id)
            or (committed_only and row["status"] != "committed")
        ):
            raise NotFound(f"asset {asset_id} not found")
        return row

    def open(self, asset_id: str, tenant_id: str | None = None) -> BinaryIO:
        with self.pool.connection() as conn:
            row = conn.execute(
                "SELECT uri, tenant_id, status FROM assets WHERE id = %s",
                (_uuid_or_none(asset_id),),
            ).fetchone()
        if not row or row["status"] != "committed" or (tenant_id and row["tenant_id"] != tenant_id):
            raise NotFound(f"asset {asset_id} not found")
        return self.store.open(row["uri"])

    def verify(self, asset_id: str, tenant_id: str | None = None) -> bool:
        """Re-hash the stored bytes and compare with the catalog."""
        expected = self.get(asset_id, tenant_id)["sha256"]
        digest = hashlib.sha256()
        with self.open(asset_id, tenant_id) as f:
            for chunk in iter(lambda: f.read(1024 * 1024), b""):
                digest.update(chunk)
        return digest.hexdigest() == expected

    def run_assets(self, run_id: str) -> list[dict]:
        with self.pool.connection() as conn:
            return conn.execute(
                f"SELECT {PUBLIC_COLUMNS} FROM assets WHERE run_id = %s AND status = 'committed' "
                "ORDER BY step_id, name",
                (run_id,),
            ).fetchall()

    def lineage(
        self, asset_id: str, tenant_id: str | None = None, *, direction: str = "up", depth: int = 25
    ) -> dict[str, Any]:
        """The provenance graph around an asset.

        up: what it was made from, back to the uploads. down: everything made
        from it (impact analysis: which outputs contain this source?).
        Only committed assets appear.
        """
        if direction not in ("up", "down"):
            raise Invalid("direction must be 'up' or 'down'")
        depth = max(1, min(depth, 100))
        root = self.get(asset_id, tenant_id)
        near, far = ("child_id", "parent_id") if direction == "up" else ("parent_id", "child_id")
        with self.pool.connection() as conn:
            reached = conn.execute(
                f"""
                WITH RECURSIVE walk(id, depth) AS (
                    SELECT %s::uuid, 0
                    UNION
                    SELECT e.{far}, w.depth + 1
                    FROM asset_edges e
                    JOIN walk w ON e.{near} = w.id
                    JOIN assets a ON a.id = e.{far} AND a.status = 'committed'
                    WHERE w.depth < %s
                )
                SELECT id, min(depth) AS depth FROM walk GROUP BY id
                """,
                (root["id"], depth),
            ).fetchall()
            depths = {r["id"]: r["depth"] for r in reached}
            nodes = conn.execute(
                f"SELECT {PUBLIC_COLUMNS} FROM assets WHERE id = ANY(%s)", (list(depths),)
            ).fetchall()
            edges = conn.execute(
                "SELECT parent_id, child_id, relation, parent_selector, child_selector, activity "
                "FROM asset_edges WHERE child_id = ANY(%s) AND parent_id = ANY(%s) ORDER BY id",
                (list(depths), list(depths)),
            ).fetchall()
        for n in nodes:
            n["depth"] = depths[n["id"]]
        nodes.sort(key=lambda n: (n["depth"], str(n["created_at"])))
        return {"root": str(root["id"]), "direction": direction, "nodes": nodes, "edges": edges}

    # -------------------------------------------------------------- housekeeping

    def collect_garbage(self, grace_seconds: float = 3600, limit: int = 500) -> int:
        """Delete discarded assets older than the grace period, and their blobs if unshared."""
        with self.pool.connection() as conn:
            rows = conn.execute(
                "SELECT id, tenant_id, sha256, uri FROM assets WHERE status = 'discarded' "
                "AND created_at < now() - make_interval(secs => %s) LIMIT %s",
                (grace_seconds, limit),
            ).fetchall()
        groups: dict[tuple[str, str, str], list] = {}
        for r in rows:
            groups.setdefault((r["tenant_id"], r["sha256"], r["uri"]), []).append(r["id"])
        removed = 0
        for (tenant_id, sha, uri), ids in groups.items():
            with self.pool.connection() as conn, conn.transaction():
                _lock_content(conn, tenant_id, sha)
                live = conn.execute(
                    "SELECT 1 FROM assets WHERE tenant_id = %s AND sha256 = %s "
                    "AND status <> 'discarded' LIMIT 1",
                    (tenant_id, sha),
                ).fetchone()
                if not live:
                    self.store.delete(uri)
                removed += conn.execute(
                    "DELETE FROM assets WHERE id = ANY(%s) AND status = 'discarded'", (ids,)
                ).rowcount
        return removed


# ---------------------------------------------------------------- handler API


def _derivation(d: Any) -> Derivation:
    """Accept a Derivation, a dict, an asset id, or (parent, relation[, parent_selector])."""
    try:
        if isinstance(d, Derivation):
            return d
        if isinstance(d, dict) and REF_KEY in d and len(d) == 1:
            return Derivation(parent=d[REF_KEY])
        if isinstance(d, dict) and "id" in d and "parent" not in d:
            return Derivation(parent=str(d["id"]))
        if isinstance(d, dict):
            return Derivation.model_validate(d)
        if isinstance(d, str):
            return Derivation(parent=d)
        if isinstance(d, (tuple, list)):
            parent, relation, *rest = d
            parent = parent.get(REF_KEY) or parent.get("id") if isinstance(parent, dict) else parent
            return Derivation(
                parent=str(parent), relation=relation, parent_selector=rest[0] if rest else None
            )
    except (ValidationError, ValueError) as exc:
        raise AssetError(f"invalid derivation {d!r}: {exc}") from None
    raise AssetError(f"invalid derivation {d!r}")


class AssetClient:
    """`ctx.assets`: read inputs and write outputs for one attempt of one step."""

    def __init__(self, catalog: AssetCatalog, *, tenant_id: str, run_id: str, step_id: str,
                 attempt: int, worker_id: str, handler: dict[str, Any],
                 params: dict[str, Any], policy_transforms: dict[str, str]) -> None:
        self.catalog = catalog
        self.tenant_id = tenant_id
        self._where = {"run_id": run_id, "step_id": step_id, "attempt": attempt, "worker_id": worker_id}
        self._transforms = policy_transforms
        self._activity = {
            **self._where,
            "handler": handler,
            "params_sha256": hashlib.sha256(
                json.dumps(params, sort_keys=True, default=str).encode()
            ).hexdigest(),
        }

    @staticmethod
    def _id(asset: Any) -> str:
        if isinstance(asset, dict):
            return str(asset.get(REF_KEY) or asset["id"])
        return str(asset)

    def get(self, asset: Any) -> dict[str, Any]:
        try:
            return self.catalog.get(self._id(asset), self.tenant_id)
        except NotFound as exc:
            raise AssetError(str(exc)) from None

    def open(self, asset: Any) -> BinaryIO:
        try:
            return self.catalog.open(self._id(asset), self.tenant_id)
        except NotFound as exc:
            raise AssetError(str(exc)) from None

    def read_bytes(self, asset: Any) -> bytes:
        with self.open(asset) as f:
            return f.read()

    def read_text(self, asset: Any) -> str:
        return self.read_bytes(asset).decode("utf-8")

    def read_json(self, asset: Any) -> Any:
        return json.loads(self.read_bytes(asset))

    def verify(self, asset: Any) -> bool:
        return self.catalog.verify(self._id(asset), self.tenant_id)

    def lineage(self, asset: Any, direction: str = "up", depth: int = 25) -> dict[str, Any]:
        return self.catalog.lineage(self._id(asset), self.tenant_id, direction=direction, depth=depth)

    def put(
        self,
        data: bytes | BinaryIO | str,
        *,
        name: str,
        kind: str,
        media_type: str,
        derived_from: list[Any],
        metadata: dict[str, Any] | None = None,
        policy: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Stage an output asset. It becomes visible only if this attempt succeeds.

        derived_from: at least one parent, as an asset row/ref/id, a Derivation,
        or a (parent, relation[, parent_selector]) tuple.
        policy: changes to the inherited policy. Tightening is always allowed;
        loosening only as the step's spec grants in policy_transforms.
        """
        return self.catalog.stage(
            tenant_id=self.tenant_id, **self._where, data=data, name=name, kind=kind,
            media_type=media_type, derived_from=[_derivation(d) for d in derived_from],
            metadata=metadata, policy=policy, policy_transforms=self._transforms,
            activity=self._activity,
        )

    def put_text(self, text: str, *, name: str, derived_from: list[Any], kind: str = "text",
                 media_type: str = "text/plain; charset=utf-8", metadata: dict | None = None,
                 policy: dict | None = None) -> dict[str, Any]:
        meta = {"char_count": len(text), **(metadata or {})}
        return self.put(text.encode("utf-8"), name=name, kind=kind, media_type=media_type,
                        derived_from=derived_from, metadata=meta, policy=policy)

    def put_json(self, value: Any, *, name: str, derived_from: list[Any], kind: str = "structured",
                 metadata: dict | None = None, policy: dict | None = None) -> dict[str, Any]:
        body = json.dumps(value, sort_keys=True, ensure_ascii=False, default=str).encode("utf-8")
        return self.put(body, name=name, kind=kind, media_type="application/json",
                        derived_from=derived_from, metadata=metadata, policy=policy)
