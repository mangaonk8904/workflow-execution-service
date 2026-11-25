from __future__ import annotations

from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, Header, HTTPException, Response
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from .config import Settings
from .db import make_pool, migrate
from .engine import Conflict, Engine, NotFound
from .models import WorkflowSpec


class StartRun(BaseModel):
    tenant_id: str = Field(min_length=1, max_length=128)
    workflow: str
    version: int | None = None
    input: dict[str, Any] = Field(default_factory=dict)
    idempotency_key: str | None = Field(None, max_length=256)


class HumanResult(BaseModel):
    output: dict[str, Any] = Field(default_factory=dict)
    actor: str | None = None


def create_app(engine: Engine | None = None) -> FastAPI:
    """Build the HTTP API. Pass an engine in tests; otherwise one is made from the environment."""
    state: dict[str, Any] = {"engine": engine}

    @asynccontextmanager
    async def lifespan(_: FastAPI):
        pool = None
        if state["engine"] is None:
            settings = Settings.from_env()
            pool = make_pool(settings.database_url)
            migrate(pool)
            state["engine"] = Engine(
                pool, tenant_max_running=settings.tenant_max_running,
                max_fan_out=settings.max_fan_out,
            )
        yield
        if pool is not None:
            pool.close()

    app = FastAPI(title="Workflow Execution Service", version="0.1.0", lifespan=lifespan)

    def eng() -> Engine:
        return state["engine"]

    @app.exception_handler(NotFound)
    async def _not_found(_, exc: NotFound):
        return JSONResponse(status_code=404, content={"detail": str(exc)})

    @app.exception_handler(Conflict)
    async def _conflict(_, exc: Conflict):
        return JSONResponse(status_code=409, content={"detail": str(exc)})

    @app.get("/healthz")
    def healthz():
        with eng().pool.connection() as conn:
            conn.execute("SELECT 1")
        return {"status": "ok"}

    @app.post("/workflows", status_code=201)
    def register_workflow(spec: WorkflowSpec):
        return {"name": spec.name, "version": eng().register_workflow(spec)}

    @app.get("/workflows/{name}")
    def get_workflow(name: str, version: int | None = None):
        v, spec = eng().get_workflow(name, version)
        return {"name": name, "version": v, "spec": spec.model_dump(mode="json")}

    @app.post("/runs", status_code=201)
    def start_run(
        body: StartRun,
        response: Response,
        idempotency_key: str | None = Header(None, alias="Idempotency-Key"),
    ):
        run_id, created = eng().start_run(
            body.tenant_id, body.workflow, body.input, version=body.version,
            idempotency_key=body.idempotency_key or idempotency_key,
        )
        if not created:
            response.status_code = 200  # replay of an earlier request
        return eng().get_run(run_id)

    @app.get("/runs/{run_id}")
    def get_run(run_id: str):
        return eng().get_run(_uuid(run_id))

    @app.get("/runs/{run_id}/events")
    def get_events(run_id: str, after_id: int = 0, limit: int = 500):
        return eng().list_events(_uuid(run_id), after_id, min(limit, 1000))

    @app.post("/runs/{run_id}/cancel")
    def cancel_run(run_id: str):
        return {"cancelled": eng().cancel_run(_uuid(run_id))}

    @app.post("/runs/{run_id}/retry")
    def retry_run(run_id: str):
        eng().retry_run(_uuid(run_id))
        return eng().get_run(run_id)

    @app.get("/human-tasks")
    def human_tasks(tenant_id: str | None = None, limit: int = 100):
        return eng().list_human_tasks(tenant_id, min(limit, 500))

    @app.post("/runs/{run_id}/steps/{step_id}/complete")
    def complete_human(run_id: str, step_id: str, body: HumanResult):
        eng().complete_human_step(_uuid(run_id), step_id, body.output, actor=body.actor)
        return eng().get_run(run_id)

    return app


def _uuid(value: str) -> str:
    import uuid

    try:
        return str(uuid.UUID(value))
    except ValueError:
        raise HTTPException(status_code=404, detail="run not found") from None


app = create_app()
