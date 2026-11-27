from __future__ import annotations

import json
import tempfile
import uuid
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, Callable, Literal

from fastapi import Depends, FastAPI, Header, HTTPException, Query, Request, Response
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from pydantic import BaseModel, Field

from .assets import KINDS, AssetCatalog
from .auth import Principal, authenticate, create_api_key
from .config import Settings
from .db import make_pool, migrate
from .engine import Engine
from .errors import Conflict, Invalid, NotFound
from .models import WorkflowSpec

UI_INDEX = Path(__file__).parent / "ui" / "index.html"
RunStatus = Literal["running", "succeeded", "failed", "cancelled"]


class StartRun(BaseModel):
    # Taken from the API key. Admin keys must name the tenant; others may
    # omit it or repeat their own.
    tenant_id: str | None = Field(None, min_length=1, max_length=128, pattern=r"^[A-Za-z0-9._-]+$")
    workflow: str
    version: int | None = None
    input: dict[str, Any] = Field(default_factory=dict)
    idempotency_key: str | None = Field(None, max_length=256)


class HumanResult(BaseModel):
    output: dict[str, Any] = Field(default_factory=dict)


def create_app(
    engine: Engine | None = None, catalog: AssetCatalog | None = None,
    settings: Settings | None = None,
) -> FastAPI:
    """Build the HTTP API. Pass an engine (and catalog) in tests; otherwise both
    are made from the environment."""
    state: dict[str, Any] = {"engine": engine, "catalog": catalog}

    @asynccontextmanager
    async def lifespan(_: FastAPI):
        pool = None
        cfg = settings or Settings.from_env()
        state["max_bytes"] = cfg.max_asset_bytes
        if state["engine"] is None:
            pool = make_pool(cfg.database_url)
            migrate(pool)
            state["engine"] = Engine(
                pool, tenant_max_running=cfg.tenant_max_running, max_fan_out=cfg.max_fan_out,
            )
        if state["catalog"] is None:
            state["catalog"] = cfg.catalog(state["engine"].pool)
        if cfg.bootstrap_admin_key:
            create_api_key(state["engine"].pool, principal="bootstrap-admin", role="admin",
                           key=cfg.bootstrap_admin_key)
        yield
        if pool is not None:
            pool.close()

    app = FastAPI(title="Workflow Execution Service", version="0.2.0", lifespan=lifespan)
    bearer = HTTPBearer(auto_error=False)

    def eng() -> Engine:
        return state["engine"]

    def cat() -> AssetCatalog:
        return state["catalog"]

    def current(creds: HTTPAuthorizationCredentials | None = Depends(bearer)) -> Principal:
        principal = authenticate(eng().pool, creds.credentials) if creds else None
        if principal is None:
            raise HTTPException(401, "missing or invalid API key",
                                headers={"WWW-Authenticate": "Bearer"})
        return principal

    def role(*allowed: str) -> Callable[..., Principal]:
        def dep(p: Principal = Depends(current)) -> Principal:
            if p.role not in allowed:
                raise HTTPException(403, f"role '{p.role}' may not do this")
            return p
        return dep

    anyone = Depends(current)

    def acting_tenant(p: Principal, requested: str | None) -> str:
        if p.is_admin:
            if not requested:
                raise HTTPException(422, "admin keys must name tenant_id")
            return requested
        if requested and requested != p.tenant_id:
            raise HTTPException(403, "this key belongs to a different tenant")
        return p.tenant_id

    def visible_run(p: Principal, run_id: str) -> dict:
        run = eng().get_run(_uuid(run_id))
        if not p.can_see(run["tenant_id"]):
            raise NotFound(f"run {run_id} not found")  # do not reveal other tenants' runs
        return run

    def tenant_scope(p: Principal) -> str | None:
        return None if p.is_admin else p.tenant_id

    @app.exception_handler(NotFound)
    async def _not_found(_, exc: NotFound):
        return JSONResponse(status_code=404, content={"detail": str(exc)})

    @app.exception_handler(Conflict)
    async def _conflict(_, exc: Conflict):
        return JSONResponse(status_code=409, content={"detail": str(exc)})

    @app.exception_handler(Invalid)
    async def _invalid(_, exc: Invalid):
        return JSONResponse(status_code=422, content={"detail": str(exc)})

    @app.get("/", include_in_schema=False)
    def ui():
        # A static page; every call it makes to the API carries the user's key.
        return FileResponse(UI_INDEX)

    @app.get("/me")
    def me(p: Principal = anyone):
        return {"principal": p.principal, "role": p.role, "tenant_id": p.tenant_id}

    @app.get("/healthz")
    def healthz():
        with eng().pool.connection() as conn:
            conn.execute("SELECT 1")
        return {"status": "ok"}

    # ------------------------------------------------------------ workflows

    @app.post("/workflows", status_code=201)
    def register_workflow(spec: WorkflowSpec, _: Principal = Depends(role("admin"))):
        return {"name": spec.name, "version": eng().register_workflow(spec)}

    @app.get("/workflows")
    def list_workflows(_: Principal = anyone):
        return eng().list_workflows()

    @app.get("/workflows/{name}")
    def get_workflow(name: str, version: int | None = None, _: Principal = anyone):
        v, spec = eng().get_workflow(name, version)
        return {"name": name, "version": v, "spec": spec.model_dump(mode="json")}

    # ------------------------------------------------------------ runs

    @app.post("/runs", status_code=201)
    def start_run(
        body: StartRun,
        response: Response,
        idempotency_key: str | None = Header(None, alias="Idempotency-Key"),
        p: Principal = Depends(role("admin", "operator")),
    ):
        run_id, created = eng().start_run(
            acting_tenant(p, body.tenant_id), body.workflow, body.input, version=body.version,
            idempotency_key=body.idempotency_key or idempotency_key,
        )
        if not created:
            response.status_code = 200  # replay of an earlier request
        return eng().get_run(run_id)

    @app.get("/runs")
    def list_runs(
        tenant_id: str | None = None,
        status: RunStatus | None = None,
        workflow: str | None = None,
        limit: int = 50,
        p: Principal = anyone,
    ):
        scope = tenant_id if p.is_admin else acting_tenant(p, tenant_id)
        return eng().list_runs(scope, status, workflow, max(1, min(limit, 500)))

    @app.get("/runs/{run_id}")
    def get_run(run_id: str, p: Principal = anyone):
        return visible_run(p, run_id)

    @app.get("/runs/{run_id}/events")
    def get_events(run_id: str, after_id: int = 0, limit: int = 500, p: Principal = anyone):
        visible_run(p, run_id)
        return eng().list_events(_uuid(run_id), after_id, min(limit, 1000))

    @app.get("/runs/{run_id}/assets")
    def run_assets(run_id: str, p: Principal = anyone):
        visible_run(p, run_id)
        return cat().run_assets(_uuid(run_id))

    @app.post("/runs/{run_id}/cancel")
    def cancel_run(run_id: str, p: Principal = Depends(role("admin", "operator"))):
        visible_run(p, run_id)
        return {"cancelled": eng().cancel_run(_uuid(run_id))}

    @app.post("/runs/{run_id}/retry")
    def retry_run(run_id: str, p: Principal = Depends(role("admin", "operator"))):
        visible_run(p, run_id)
        eng().retry_run(_uuid(run_id))
        return eng().get_run(_uuid(run_id))

    # ------------------------------------------------------------ human tasks

    @app.get("/human-tasks")
    def human_tasks(tenant_id: str | None = None, limit: int = 100, p: Principal = anyone):
        scope = tenant_id if p.is_admin else acting_tenant(p, tenant_id)
        return eng().list_human_tasks(scope, min(limit, 500))

    @app.post("/runs/{run_id}/steps/{step_id}/complete")
    def complete_human(run_id: str, step_id: str, body: HumanResult,
                       p: Principal = Depends(role("admin", "reviewer"))):
        visible_run(p, run_id)
        # The actor is whoever holds the key, not what the request claims.
        eng().complete_human_step(_uuid(run_id), step_id, body.output, actor=p.principal)
        return eng().get_run(_uuid(run_id))

    # ------------------------------------------------------------ assets

    @app.post("/assets", status_code=201)
    async def upload_asset(
        request: Request,
        kind: str = Query(..., description=f"one of {list(KINDS)}"),
        name: str = Query(..., min_length=1, max_length=255),
        tenant_id: str | None = None,
        content_type: str = Header("application/octet-stream", alias="Content-Type"),
        policy: str | None = Header(None, alias="X-Asset-Policy"),
        metadata: str | None = Header(None, alias="X-Asset-Metadata"),
        p: Principal = Depends(role("admin", "operator")),
    ):
        """Upload raw bytes as the request body. Policy and technical metadata are
        JSON in the X-Asset-Policy and X-Asset-Metadata headers."""
        tenant = acting_tenant(p, tenant_id)
        pol, meta = _json_header("X-Asset-Policy", policy), _json_header("X-Asset-Metadata", metadata)
        limit, size = state.get("max_bytes"), 0
        body = tempfile.SpooledTemporaryFile(max_size=8 * 1024 * 1024)
        try:
            async for chunk in request.stream():
                size += len(chunk)
                if limit is not None and size > limit:
                    raise HTTPException(413, f"upload exceeds {limit} bytes")
                body.write(chunk)
            body.seek(0)
            return await run_in_threadpool(
                cat().upload, tenant, body, kind=kind, media_type=content_type, name=name,
                metadata=meta, policy=pol, created_by=p.principal,
            )
        finally:
            body.close()

    @app.get("/assets/{asset_id}")
    def get_asset(asset_id: str, p: Principal = anyone):
        return cat().get(asset_id, tenant_scope(p))

    @app.get("/assets/{asset_id}/content")
    def get_asset_content(asset_id: str, p: Principal = anyone):
        asset = cat().get(asset_id, tenant_scope(p))
        stream = cat().open(asset_id, tenant_scope(p))

        def chunks():
            try:
                yield from iter(lambda: stream.read(1024 * 1024), b"")
            finally:
                stream.close()

        return StreamingResponse(
            chunks(), media_type=asset["media_type"],
            headers={"ETag": f'"{asset["sha256"]}"', "Content-Length": str(asset["size_bytes"]),
                     "X-Content-SHA256": asset["sha256"]},
        )

    @app.get("/assets/{asset_id}/lineage")
    def get_lineage(asset_id: str, direction: str = "up", depth: int = 25, p: Principal = anyone):
        return cat().lineage(asset_id, tenant_scope(p), direction=direction, depth=depth)

    return app


def _json_header(name: str, value: str | None) -> dict | None:
    if not value:
        return None
    try:
        parsed = json.loads(value)
    except ValueError:
        raise HTTPException(422, f"{name} must be a JSON object") from None
    if not isinstance(parsed, dict):
        raise HTTPException(422, f"{name} must be a JSON object")
    return parsed


def _uuid(value: str) -> str:
    try:
        return str(uuid.UUID(value))
    except ValueError:
        raise HTTPException(status_code=404, detail="run not found") from None


app = create_app()
