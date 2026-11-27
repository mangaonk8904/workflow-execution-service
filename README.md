# Workflow Execution Service

A small, durable workflow engine on Postgres. You define a workflow as a DAG of
steps, start runs, and the service executes each step in dependency order,
tracks state, retries failures, recovers from dead workers, parks on human
review, and records every transition.

Data that flows through runs (video, audio, images, documents, text and
structured records) lives in an **asset catalog**: content-addressed bytes
in a blob store, with lineage edges that say which part of which parent every
asset came from, and a policy (consent, PII state, retention, restrictions)
that derived assets inherit from their parents.

It ships with a worked example: turning a recorded **expert interview** into
structured, reviewed records for delivery to a customer. Every record can be
traced to the second of the recording it came from.

Python 3.11+ · FastAPI · Postgres 14+ · a local directory or any S3-compatible store.

## Architecture

```mermaid
flowchart LR
    C[Clients] -->|REST + API key| API[API<br/>definitions, runs, human tasks, assets]
    API --> E[Engine<br/>per-run state machine]
    E -->|one transaction:<br/>state + events + asset commit| PG[(Postgres<br/>definitions, runs, step_runs,<br/>events, assets, asset_edges)]
    W[Workers<br/>handlers by step type] -->|claim: SKIP LOCKED<br/>lease + heartbeat| PG
    W -->|stage assets + lineage| PG
    W -->|read / write bytes| BS[(Blob store<br/>local dir or S3/MinIO<br/>content-addressed)]
    API -->|uploads, downloads| BS
    W -->|complete / fail<br/>fenced by attempt| E
    S[Scheduler] -->|reap expired leases<br/>escalate SLAs<br/>collect discarded blobs| E
    S -->|relay outbox| OUT[stdout / webhook]
    H[Human reviewers] -->|complete task| API
```

| Process | Command | Role |
|---|---|---|
| API | `wes api` | Register workflows, start/cancel/retry runs, read status and events, complete human tasks, upload and read assets and their lineage |
| Worker | `wes worker --handlers <module>` | Claims ready steps for the handler types it has, runs them, reports results |
| Scheduler | `wes scheduler` | Recovers steps from dead workers, enforces timeouts, escalates overdue human tasks, relays the event outbox, deletes discarded assets |

All three are stateless; state lives in Postgres and bytes in the blob store. Run as many copies as you need.

## Quickstart

With Docker (brings up Postgres, MinIO for asset bytes, the API, a worker with the
interview handlers, and the scheduler):

```bash
docker compose up --build
```

Or locally against your own Postgres (asset bytes go to `./blobstore`):

```bash
pip install -e ".[dev]"
export WES_DATABASE_URL=postgresql://postgres:postgres@localhost:5432/wes
python -m examples.interview_pipeline.demo      # self-contained end-to-end run
```

Open http://localhost:8000 for the web UI: browse and filter runs, watch a run's
graph and event log update live, complete human steps, and register or start
workflows. Paste an API key into the key field in the header (it is kept in the
browser's local storage); the UI shows what that key's role and tenant allow.
It is one static file, [src/wes/ui/index.html](src/wes/ui/index.html), served
by the API, so there is no separate frontend build. Interactive API docs are at `/docs`.

Driving it over HTTP. Every request except `/healthz` and the UI page needs an
API key. The compose file creates a development admin key; mint tenant keys
with the CLI:

```bash
ADMIN='Authorization: Bearer wes_dev_admin_key_change_me_0000'
OP="Authorization: Bearer $(docker compose exec -T api python -m wes create-key \
      --principal ops@lab-acme --role operator --tenant lab-acme)"
REV="Authorization: Bearer $(docker compose exec -T api python -m wes create-key \
      --principal reviewer@lab-acme --role reviewer --tenant lab-acme)"

# 1. register the workflow (admin; returns its immutable version)
curl -X POST localhost:8000/workflows -H "$ADMIN" -H 'content-type: application/json' \
     -d @examples/interview_pipeline/workflow.json

# 2. upload the recording; consent and PII state travel with it from here on
SRC=$(curl -s -X POST 'localhost:8000/assets?kind=video&name=interview.mp4' -H "$OP" \
        -H 'Content-Type: video/mp4' \
        -H 'X-Asset-Policy: {"consent": ["share_with_customers"], "pii": "present"}' \
        -H 'X-Asset-Metadata: {"duration_seconds": 175}' \
        --data-binary @path/to/interview.mp4 | jq -r .id)

# 3. start a run on it (Idempotency-Key makes the call safe to retry)
jq --arg id "$SRC" '{workflow: "expert-interview", input: (. + {source: {"$asset": $id}})}' \
     examples/interview_pipeline/sample_input.json \
  | curl -X POST localhost:8000/runs -H "$OP" -H 'content-type: application/json' \
         -H 'Idempotency-Key: interview-4821' -d @-

# 4. watch it
curl localhost:8000/runs/<run_id> -H "$OP"
curl localhost:8000/runs/<run_id>/events -H "$OP"
curl localhost:8000/runs/<run_id>/assets -H "$OP"

# 5. the sample has one low-confidence segment, so it stops for review
curl localhost:8000/human-tasks -H "$REV"
curl -X POST localhost:8000/runs/<run_id>/steps/expert_review/complete -H "$REV" \
     -H 'content-type: application/json' -d '{"output": {"verdict": "accept"}}'

# 6. fetch the manifest and trace any asset back to its sources
curl localhost:8000/assets/<manifest_id>/content -H "$OP"
curl 'localhost:8000/assets/<asset_id>/lineage?direction=up' -H "$OP"
curl "localhost:8000/assets/$SRC/lineage?direction=down" -H "$OP"   # everything made from it
```

Interactive API docs are at `localhost:8000/docs`.

### API keys and tenants

| Role | Can |
|---|---|
| `admin` | Register workflows; act on any tenant, naming it with `tenant_id` |
| `operator` | Upload assets; start, cancel and retry runs; read its tenant |
| `reviewer` | Complete human tasks; read its tenant |

The tenant comes from the key, never from the request body. Another tenant's
runs and assets answer 404. The actor recorded on a human task is the key's
principal. Only a SHA-256 of each key is stored; `wes create-key` prints the
key once. `WES_BOOTSTRAP_ADMIN_KEY` creates an admin key at startup (for
development).

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
| `policy_transforms` | Policy loosenings this step's assets may declare. Only `{"pii": "redacted"}` or `{"pii": "none"}`; consent, retention and restrictions can never be loosened by a step. |
| `requires_policy` | A gate checked when the step completes, e.g. `{"consent": ["share_with_customers"], "pii": ["none", "redacted"]}`. Every asset the step commits or references in its output must satisfy it, or the step fails. |

Handlers are plain functions. `version` and `models` are recorded on every
result and lineage edge the handler produces:

```python
from wes import Context, HandlerRegistry, PermanentError, ref

registry = HandlerRegistry()

@registry.register("transcribe", version="2.1.0", models={"asr": "whisper-large-v3"})
def transcribe(ctx: Context) -> dict:
    seg = ctx.item                                          # this child's item
    if not seg: raise PermanentError("empty segment")       # no retry
    audio = ctx.assets.read_bytes(seg["audio"])             # an input asset
    text = ctx.assets.put_text(
        run_asr(audio, seg["start"], seg["end"]), name="transcript",
        # made from this time range of the audio
        derived_from=[(seg["audio"], "transcript_of",
                       {"type": "time", "start": seg["start"], "end": seg["end"]})],
    )
    return {"transcript": ref(text)}                        # {"$asset": "<id>"}
```

## Assets, lineage and policy

An **asset** is an immutable record of one piece of content: its `kind`
(`image`, `video`, `audio`, `document`, `text`, `structured`, `bundle`), media
type, SHA-256, size, technical metadata (duration, dimensions, page count) and
policy. Its bytes live in the blob store at `tenants/<tenant>/sha256/<hash>`, so
identical bytes are stored once while each asset keeps its own provenance.
Step outputs and run inputs refer to assets as `{"$asset": "<id>"}`. The engine
checks that every such reference is a committed asset of the run's tenant.

**Lineage.** `ctx.assets.put(..., derived_from=[...])` records one edge per
parent: a relation (`transcode_of`, `transcript_of`, `redaction_of`,
`extracted_from`, `rendition_of`, `part_of`, `annotates`, `references`, ...),
the activity (run, step, attempt, handler name/version/models, params hash),
and optional **selectors** on either end:

| Selector | Points into | Example |
|---|---|---|
| `time` | audio, video | `{"type": "time", "start": 61.0, "end": 119.0}` |
| `region` | image, video frame (`t`), document | `{"type": "region", "x": 10, "y": 20, "w": 300, "h": 80, "t": 12.5}` |
| `page` | document | `{"type": "page", "page": 3, "end_page": 4}` |
| `text` | text, document | `{"type": "text", "start": 120, "end": 188}` |
| `json_pointer` | structured, bundle | `{"type": "json_pointer", "pointer": "/claims/0"}` |
| `composite` | any of the above together | a page and a region on it |

Selectors are checked against the asset's kind and, when known, its bounds.
Every step output needs at least one parent, so provenance always reaches an
upload. `GET /assets/{id}/lineage?direction=up` walks back to the sources;
`direction=down` lists everything built from an asset (impact analysis).

**Policy.** Uploads declare consent, PII state (`none` < `redacted` <
`unknown` < `present`), retention and restrictions. A derived asset gets the
strictest combination of its parents: consent intersected, worst PII state,
shortest retention, all restrictions. `references` edges do not pass policy
on. Handlers may tighten a policy freely but can loosen it only as the step's
`policy_transforms` allows. A step's `requires_policy` gate then refuses to
commit anything that may not go where the step sends it. So consent is
enforced at delivery from the data itself, even if handler code forgets to check.

**Write protocol.** `put` hashes the bytes, then in one transaction checks that
the caller still holds the step's current attempt, validates parents,
selectors and policy, and inserts the asset as `staged` with its edges. Only
then does it upload. The engine commits the attempt's staged assets in the
same transaction that accepts its result. Every other outcome (failure, retry,
lease lost, cancel) marks them `discarded`, and they are never visible. The
scheduler deletes discarded assets after a grace period, and their blobs only
if no live asset shares the hash. An advisory lock per hash orders that
against new writes.

Blob store: `WES_BLOB_URL=file:///path` (default `./blobstore`) or
`s3://bucket` with `WES_S3_ENDPOINT`, `WES_S3_REGION` and the usual AWS
credentials (`pip install '.[s3]'`). `WES_S3_CREATE_BUCKET=true` creates the
bucket on startup.

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
| A retried or superseded attempt leaves no assets behind | Assets are staged per attempt, committed with the result, discarded otherwise |
| Every derived asset traces back to its sources | At least one parent edge is required; edges carry selectors and the producing handler build |
| Consent and PII state follow the data | Derived assets inherit the strictest parent policy; only spec-granted loosenings; `requires_policy` gates |
| Stored bytes are what was cataloged | Content addressing by SHA-256; `ctx.assets.verify` re-hashes |
| Tenants cannot read or act on each other's data | Tenant taken from the API key; runs, assets and blob keys are scoped by tenant |

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
- **Outputs in JSONB, content in assets.** Outputs carry small values and
  `{"$asset": id}` references. Bytes go to the blob store, and their
  provenance and policy go in the catalog.
- **Lineage in Postgres, not a graph database.** Edges are a table and walks
  are recursive CTEs. Assets are only added, never edited, and every parent
  exists before its child, so the graph can't contain cycles.
- **Policy is computed, not copied.** A derived asset's policy is a function
  of its parents and the step's declared grants, worked out when the asset is
  written. It can't drift from its sources.
- **Human steps are first-class.** They are a status, not a worker that
  blocks, so a review that takes two days costs nothing while it waits.

## The interview pipeline

`examples/interview_pipeline` models the flow for a recorded expert interview:

```
upload -> probe -> normalize -> segment -> transcribe (per segment) -> redact
       -> extract (per turn) -> quality -> expert_review (only if needed) -> package
```

- The recording is uploaded with the expert's consent and `pii: present`.
- `probe` re-hashes the stored bytes against the catalog and refuses to start
  without consent to share.
- `normalize` writes a mezzanine, an audio track and a 480p review proxy, each
  an asset derived from the one before.
- `segment` cuts the audio into question turns as time selectors, not copies.
- `transcribe` writes one transcript per segment, linked to its time range of
  the audio.
- `redact` removes emails, phone numbers and the expert's name. Its outputs
  declare `pii: redacted`, which the spec grants.
- `extract` writes one record per question. Its summary and each claim map to
  the exact character span of the transcript they came from.
- `quality` sends the run to a human when transcription confidence is low, an
  extraction is empty, or the run falls in a deterministic random sample.
- `package` writes the manifest as an asset. Each record carries its
  provenance: source asset and hash, media time span, chain of intermediates,
  handler builds. A reviewer's `reject` blocks delivery. The step's
  `requires_policy` gate blocks delivery without consent or with unredacted PII.

**The media and model work is stubbed.** `turns` in the run input stands in
for diarisation and speech-recognition output, derived media files are
placeholder bytes, and extraction is rule-based. Each handler's docstring says
what the production version would call. The orchestration, catalog, lineage
and policy around them are real.

## Tests

```bash
export WES_TEST_DATABASE_URL=postgresql://postgres:postgres@localhost:5432/wes_test
pytest
```

66 tests against a real Postgres. They cover DAG validation, retries and
backoff, lease expiry and fencing, timeouts, cancel and resume, fan-out and
join, conditional and human steps, tenant fairness, the outbox, and a stress
test with 8 competing workers. They also cover the asset write protocol under
retries and lost leases, policy inheritance and gates, selectors, lineage
walks, garbage collection, API keys, roles and tenant isolation, and the
interview pipeline end to end, including provenance down to the second.
Set `WES_TEST_S3_ENDPOINT` (e.g. a local MinIO) to also test the S3 store.

## Not built yet

- Validation: declarative output contracts per step, stored quality-check
  results, gates that route to review.
- Review: structured verdicts, annotations anchored to selectors, reviewer
  assignment, a revise loop that produces new asset versions.
- Versioning and reprocessing: `supersedes` chains, step memoisation, rerun
  from a step with a new model.
- Delivery: versioned bundles, signed manifests, delivery records, revocation
  that follows lineage downwards.
- Mixed media: sub-workflows and routing by asset kind, more example pipelines
  (image, document, audio, video plus slides).
- A separate queue or log (Kafka/SQS) and CDC-based outbox relay.
- Cron and delayed starts, loops, compensation steps.
- Metrics and tracing. The `events` table is the audit trail.
