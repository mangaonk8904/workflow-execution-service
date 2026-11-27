-- Phase 0-2: API keys, handler metadata, the asset catalog and its lineage graph.

-- API keys. Only a SHA-256 of the key is stored. tenant_id NULL means an
-- admin key that may act on any tenant.
CREATE TABLE api_keys (
    key_sha256  text        PRIMARY KEY,
    tenant_id   text,
    principal   text        NOT NULL,
    role        text        NOT NULL CHECK (role IN ('admin', 'operator', 'reviewer')),
    created_at  timestamptz NOT NULL DEFAULT now(),
    revoked_at  timestamptz,
    CHECK (role = 'admin' OR tenant_id IS NOT NULL)
);

-- Which handler build produced a step's result: {"name", "version", "models"}.
ALTER TABLE step_runs ADD COLUMN handler jsonb;

-- Every piece of media or data the system holds. Rows are immutable once
-- committed; content lives in the blob store at `uri`, addressed by sha256.
-- Identical bytes produced twice are two assets (two provenances) sharing one blob.
CREATE TABLE assets (
    id            uuid        PRIMARY KEY,
    tenant_id     text        NOT NULL,
    kind          text        NOT NULL CHECK (kind IN (
        'image', 'video', 'audio', 'document', 'text', 'structured', 'bundle')),
    media_type    text        NOT NULL,
    name          text        NOT NULL,     -- output slot for step assets, file name for uploads
    sha256        text        NOT NULL CHECK (sha256 ~ '^[0-9a-f]{64}$'),
    size_bytes    bigint      NOT NULL CHECK (size_bytes >= 0),
    uri           text        NOT NULL,
    metadata      jsonb       NOT NULL DEFAULT '{}'::jsonb,  -- technical: duration, dimensions, pages, ...
    policy        jsonb       NOT NULL,                      -- consent, pii, retention, restrictions
    -- staged: written by a running attempt, invisible to everyone else.
    -- committed: the attempt's result was accepted. discarded: it was not.
    status        text        NOT NULL CHECK (status IN ('staged', 'committed', 'discarded')),
    origin        text        NOT NULL CHECK (origin IN ('upload', 'step')),
    run_id        uuid        REFERENCES runs (id) ON DELETE CASCADE,
    step_id       text,
    attempt       int,
    worker_id     text,
    created_by    text,                      -- principal, for uploads
    created_at    timestamptz NOT NULL DEFAULT now(),
    committed_at  timestamptz,
    CHECK (origin = 'upload' OR (run_id IS NOT NULL AND step_id IS NOT NULL AND attempt IS NOT NULL))
);

-- One live asset per output slot per attempt; re-putting the same slot replaces it.
CREATE UNIQUE INDEX assets_step_slot ON assets (run_id, step_id, attempt, name)
    WHERE origin = 'step' AND status <> 'discarded';
CREATE INDEX assets_tenant_created ON assets (tenant_id, created_at DESC);
CREATE INDEX assets_run ON assets (run_id, step_id) WHERE run_id IS NOT NULL;
CREATE INDEX assets_sha ON assets (tenant_id, sha256);
CREATE INDEX assets_discarded ON assets (created_at) WHERE status = 'discarded';

-- Lineage: child was made from parent. Selectors say which part of each
-- (a time range of a video, a box on an image, a page of a PDF, a character
-- span of a transcript, a JSON pointer into a record). Several edges between
-- the same pair are allowed, one per mapped region.
CREATE TABLE asset_edges (
    id               bigserial   PRIMARY KEY,
    tenant_id        text        NOT NULL,
    child_id         uuid        NOT NULL REFERENCES assets (id) ON DELETE CASCADE,
    parent_id        uuid        NOT NULL REFERENCES assets (id) ON DELETE CASCADE,
    relation         text        NOT NULL,
    child_selector   jsonb,
    parent_selector  jsonb,
    -- run, step, attempt, handler name/version/models, params hash, policy changes
    activity         jsonb       NOT NULL DEFAULT '{}'::jsonb,
    created_at       timestamptz NOT NULL DEFAULT now(),
    CHECK (child_id <> parent_id)
);

CREATE INDEX asset_edges_child  ON asset_edges (child_id);
CREATE INDEX asset_edges_parent ON asset_edges (parent_id);
