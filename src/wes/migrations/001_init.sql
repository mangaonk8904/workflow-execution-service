-- Workflow Execution Service: core schema.
-- Postgres is the single source of truth: definitions, run state, the task
-- queue (step_runs in status 'ready') and the outbox (events) all live here so
-- every state transition is one transaction.

CREATE TABLE workflow_definitions (
    name        text        NOT NULL,
    version     int         NOT NULL,
    spec        jsonb       NOT NULL,
    created_at  timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (name, version)
);

CREATE TABLE runs (
    id               uuid        PRIMARY KEY,
    tenant_id        text        NOT NULL,
    workflow_name    text        NOT NULL,
    workflow_version int         NOT NULL,
    status           text        NOT NULL
        CHECK (status IN ('running', 'succeeded', 'failed', 'cancelled')),
    input            jsonb       NOT NULL DEFAULT '{}'::jsonb,
    idempotency_key  text,
    error            text,
    created_at       timestamptz NOT NULL DEFAULT now(),
    updated_at       timestamptz NOT NULL DEFAULT now(),
    finished_at      timestamptz,
    FOREIGN KEY (workflow_name, workflow_version)
        REFERENCES workflow_definitions (name, version)
);

-- Start-run dedupe: the same key from the same tenant returns the same run.
CREATE UNIQUE INDEX runs_idempotency
    ON runs (tenant_id, idempotency_key) WHERE idempotency_key IS NOT NULL;
CREATE INDEX runs_tenant_created ON runs (tenant_id, created_at DESC);

CREATE TABLE step_runs (
    run_id              uuid        NOT NULL REFERENCES runs (id) ON DELETE CASCADE,
    step_id             text        NOT NULL,   -- 'transcribe' or 'transcribe#3' for a fan-out child
    tenant_id           text        NOT NULL,   -- denormalised so claiming never joins runs
    parent_step_id      text,                   -- set on fan-out children
    item_index          int,
    item                jsonb,
    step_type           text        NOT NULL,   -- handler name; workers subscribe by type
    kind                text        NOT NULL CHECK (kind IN ('task', 'human')),
    status              text        NOT NULL CHECK (status IN (
        'pending', 'ready', 'running', 'waiting_human', 'fanned_out',
        'succeeded', 'failed', 'skipped', 'cancelled')),
    attempt             int         NOT NULL DEFAULT 0,   -- fencing token, bumped on every claim
    max_attempts        int         NOT NULL DEFAULT 3,
    backoff_seconds     double precision NOT NULL DEFAULT 2,
    max_backoff_seconds double precision NOT NULL DEFAULT 60,
    timeout_seconds     int,
    sla_seconds         int,
    available_at        timestamptz NOT NULL DEFAULT now(),  -- retry backoff: not claimable before this
    lease_owner         text,
    lease_expires_at    timestamptz,
    deadline_at         timestamptz,            -- hard timeout for the current attempt
    sla_due_at          timestamptz,            -- human steps
    escalated_at        timestamptz,
    remaining_children  int,                    -- fan-out join counter
    output              jsonb,
    error               text,
    started_at          timestamptz,
    finished_at         timestamptz,
    updated_at          timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (run_id, step_id)
);

CREATE INDEX step_runs_claimable ON step_runs (step_type, available_at) WHERE status = 'ready';
CREATE INDEX step_runs_running   ON step_runs (lease_expires_at)        WHERE status = 'running';
CREATE INDEX step_runs_human     ON step_runs (tenant_id, sla_due_at)   WHERE status = 'waiting_human';
CREATE INDEX step_runs_children  ON step_runs (run_id, parent_step_id)  WHERE parent_step_id IS NOT NULL;

-- Append-only history of every state change. Doubles as the transactional
-- outbox: rows with published_at IS NULL have not been relayed yet.
CREATE TABLE events (
    id           bigserial   PRIMARY KEY,
    run_id       uuid        NOT NULL REFERENCES runs (id) ON DELETE CASCADE,
    step_id      text,
    tenant_id    text        NOT NULL,
    type         text        NOT NULL,
    payload      jsonb       NOT NULL DEFAULT '{}'::jsonb,
    created_at   timestamptz NOT NULL DEFAULT clock_timestamp(),
    published_at timestamptz
);

CREATE INDEX events_unpublished ON events (id) WHERE published_at IS NULL;
CREATE INDEX events_run ON events (run_id, id);
