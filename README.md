# Workflow Execution Service

A small, durable workflow engine on Postgres. You define a workflow as a DAG of
steps, start runs, and the service executes each step in dependency order,
tracks state, retries failures, recovers from dead workers, parks on human
review, and records every transition.

It ships with a worked example: turning a recorded **expert interview** into
structured, reviewed, traceable records for delivery to a customer.

Python 3.11+ · FastAPI · Postgres 14+ · no other infrastructure.

## Architecture

```mermaid
flowchart LR
    C[Clients] -->|REST| API[API<br/>definitions, runs, human tasks]
    API --> E[Engine<br/>per-run state machine]
    E -->|one transaction:<br/>state + events| PG[(Postgres<br/>definitions, runs,<br/>step_runs, events)]
    W[Workers<br/>handlers by step type] -->|claim: SKIP LOCKED<br/>lease + heartbeat| PG
    W -->|complete / fail<br/>fenced by attempt| E
    S[Scheduler] -->|reap expired leases<br/>escalate SLAs| E
    S -->|relay outbox| OUT[stdout / webhook]
    H[Human reviewers] -->|complete task| API
```

| Process | Command | Role |
|---|---|---|
| API | `wes api` | Register workflows, start/cancel/retry runs, read status and events, complete human tasks |
| Worker | `wes worker --handlers <module>` | Claims ready steps for the handler types it has, runs them, reports results |
| Scheduler | `wes scheduler` | Recovers steps from dead workers, enforces timeouts, escalates overdue human tasks, relays the event outbox |

All three are stateless; state lives in Postgres. Run as many copies as you need.

## Quickstart

With Docker (brings up Postgres, the API, a worker with the interview handlers, and the scheduler):

```bash
docker compose up --build
```

Or locally against your own Postgres:

```bash
pip install -e ".[dev]"
export WES_DATABASE_URL=postgresql://postgres:postgres@localhost:5432/wes
python -m examples.interview_pipeline.demo      # self-contained end-to-end run
```

Open http://localhost:8000 for the web UI: browse and filter runs, watch a run's
graph and event log update live, complete human steps, and register or start
workflows. It is one static file, [src/wes/ui/index.html](src/wes/ui/index.html), served
by the API, so there is no separate frontend build. Interactive API docs are at `/docs`.

Driving it over HTTP:

```bash
# 1. register the workflow (returns its immutable version)
curl -X POST localhost:8000/workflows -H 'content-type: application/json' \
     -d @examples/interview_pipeline/workflow.json

# 2. start a run (Idempotency-Key makes the call safe to retry)
jq '{tenant_id: "lab-acme", workflow: "expert-interview", input: .}' \
     examples/interview_pipeline/sample_input.json \
  | curl -X POST localhost:8000/runs -H 'content-type: application/json' \
         -H 'Idempotency-Key: interview-4821' -d @-

# 3. watch it
curl localhost:8000/runs/<run_id>
curl localhost:8000/runs/<run_id>/events

# 4. the sample has one low-confidence segment, so it stops for review
curl 'localhost:8000/human-tasks?tenant_id=lab-acme'
curl -X POST localhost:8000/runs/<run_id>/steps/expert_review/complete \
     -H 'content-type: application/json' \
     -d '{"output": {"verdict": "accept"}, "actor": "you@example.com"}'
```

Interactive API docs are at `localhost:8000/docs`.

## Defining a workflow

```json
{
  "name": "expert-interview",
  "steps": [
    {"id": "segment"},
    {"id": "transcribe", "depends_on": ["segment"], "foreach": "segment.segments",
     "timeout_seconds": 600, "retry": {"max_attempts": 4, "backoff_seconds": 1}},
    {"id": "quality", "depends_on": ["transcribe"]},
    {"id": "expert_review", "kind": "human", "depends_on": ["quality"],
     "when": "quality.needs_review", "sla_seconds": 86400},
    {"id": "package", "depends_on": ["transcribe", "expert_review"]}
  ]
}
```

| Field | Meaning |
|---|---|
| `depends_on` | Steps that must finish first. Their outputs arrive in `ctx.deps`. |
| `type` | Handler name (defaults to `id`). Workers subscribe by type. |
| `foreach` | `"<dep>.<key>"` naming a list: one child task per item, joined in item order as `{"items": [...]}`. |
| `when` | `"<dep>.<key>"`: the step is skipped when the value is falsy. Dependents still run and see `None`. |
| `kind: human` | Not given to workers. The run parks until `POST .../complete`. `sla_seconds` triggers a one-time `step.sla_breached` event. |
| `retry` | `max_attempts`, `backoff_seconds`, `max_backoff_seconds`: exponential backoff with jitter. |
| `timeout_seconds` | Hard limit per attempt, enforced by the scheduler. |

Handlers are plain functions:

```python
from wes import Context, HandlerRegistry, PermanentError

registry = HandlerRegistry()

@registry.register("transcribe")
def transcribe(ctx: Context) -> dict:
    segment = ctx.item                      # this child's item
    if not segment: raise PermanentError("empty segment")   # no retry
    return {"text": run_asr(segment), "key": ctx.idempotency_key}
```

## Guarantees and how they are met

| Guarantee | Mechanism |
|---|---|
| A run is never lost or half-updated | Each transition is one Postgres transaction covering state and its events |
| Starting a run twice creates one run | Unique `(tenant_id, idempotency_key)` |
| Steps run **at least once** | Leases: a claimed step that stops heartbeating is reclaimed by the reaper |
| A slow or dead worker cannot corrupt state | `attempt` is a fencing token; results from a superseded attempt are rejected |
| A step never runs on two workers at once | `SELECT ... FOR UPDATE SKIP LOCKED` on claim |
| State changes and events never diverge | Transactional outbox: events are rows written with the state change, relayed afterwards (at-least-once) |
| In-flight runs are not broken by a new definition | Definitions are immutable per version; a run pins its version |
| One tenant cannot starve the others | Claims prefer tenants with fewer running steps and skip tenants at their cap (soft limit) |
| Failed runs can be resumed | `POST /runs/{id}/retry` re-queues only failed steps and failed fan-out children |

Because delivery is at-least-once, **handlers must be idempotent**. `ctx.idempotency_key`
is stable across retries of a step; the example's handlers write to paths derived
from content hashes and run ids so a retry overwrites instead of duplicating.

## Design decisions

- **Postgres as the queue.** `SKIP LOCKED` is simple, transactional with the
  state, and comfortably handles hundreds of claims per second. The seam to
  swap in SQS or Kafka is `Engine.claim`; you would then need the outbox to
  feed the queue, which is why the outbox already exists.
- **Per-run row lock for transitions.** Completions for the same run are
  serialised, which makes join logic trivially correct. Claims do not take it,
  so workers never wait on each other. A run with a very wide fan-out
  serialises its completions; shard the join counter if that becomes the limit.
- **The run lock is `FOR NO KEY UPDATE`.** A plain `FOR UPDATE` deadlocked
  under load: a claim holding a step row waited on the run row for its event's
  foreign-key check, while a completion holding the run row waited on that step
  row. The stress test caught it; the weaker lock mode removes the wait.
- **Fan-out join by counter.** A parent keeps `remaining_children`; the last
  child to finish triggers the join. No scan of siblings on each completion.
- **Outputs in JSONB.** Fine for metadata and references. Large payloads
  (media, transcripts at scale) belong in object storage with the URI in the
  output, as the example's `normalize` step does.
- **Human steps are first-class.** They are a status, not a worker that
  blocks, so a review that takes two days costs nothing while it waits.

## The interview pipeline

`examples/interview_pipeline` models the flow for a recorded expert interview:

```
probe -> normalize -> segment -> transcribe (per segment) -> redact
      -> extract (per turn) -> quality -> expert_review (only if needed) -> package
```

- `probe` refuses to start without the expert's consent to share.
- `redact` removes emails, phone numbers and the expert's name before extraction.
- `extract` produces one record per question, each citing the segment and timestamps it came from.
- `quality` sends the run to a human when transcription confidence is low, an extraction is empty, or the run falls in a deterministic random sample.
- `package` writes a manifest with a pseudonymous expert reference, the quality report, the review verdict and model lineage. A reviewer's `reject` blocks delivery.

**The media and model work is stubbed.** `turns` in the run input stands in for
diarisation and speech-recognition output, extraction is rule-based, and the
"object store" is a local `deliveries/` folder. Each handler's docstring says
what the production version would call. The orchestration around them is real.

## Tests

```bash
export WES_TEST_DATABASE_URL=postgresql://postgres:postgres@localhost:5432/wes_test
pytest
```

36 tests against a real Postgres: DAG validation, retries and backoff, lease
expiry and fencing, timeouts, cancel and resume, fan-out and join, conditional
and human steps, tenant fairness, the outbox, the HTTP API, the interview
pipeline end to end, and a stress test with 8 competing workers.

## Not built yet

- Authentication and tenant isolation on the API (tenant id is trusted input).
- A separate queue or log (Kafka/SQS) and CDC-based outbox relay.
- Cron and delayed starts, sub-workflows, loops, compensation steps.
- Asset catalog and lineage store for multimodal data; the manifest carries lineage inline for now.
- Metrics and tracing. The `events` table is the audit trail.
