//! Schema migrations. Step `i` takes a database from version `i` to `i + 1`
//! (the first step also runs on an empty database). Append a step; never edit
//! a released one.

use super::policy::CompatTable;
use super::DbKind;

/// Tables every database shares: the append-only event journal and the
/// references GC works on.
const COMMON: &str = "
CREATE TABLE event (
    seq          INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id       TEXT,
    attempt_id   TEXT,
    -- 1, 2, 3, ... per attempt with no holes (NULL for an event of no
    -- attempt): the sequence a watcher of one attempt follows. Numbered by
    -- the INSERT itself, in the writer's transaction.
    attempt_seq  INTEGER CHECK ((attempt_id IS NULL) = (attempt_seq IS NULL)),
    kind         TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    at_ms        INTEGER NOT NULL
);
CREATE INDEX event_attempt ON event(attempt_id, seq);
CREATE UNIQUE INDEX event_attempt_seq ON event(attempt_id, attempt_seq)
    WHERE attempt_id IS NOT NULL;
CREATE TRIGGER event_no_update BEFORE UPDATE ON event
BEGIN SELECT RAISE(ABORT, 'event journal is append-only'); END;
CREATE TRIGGER event_no_delete BEFORE DELETE ON event
BEGIN SELECT RAISE(ABORT, 'event journal is append-only'); END;
-- A file whose last reference GC dropped. The row is written in the same
-- transaction that drops the reference, and removed only after the file is
-- gone; add_blob_ref refuses a locator listed here, so a new reference can
-- never point at a file that is about to be deleted.
CREATE TABLE pending_delete (
    locator      TEXT PRIMARY KEY,
    queued_at_ms INTEGER NOT NULL
);
";

const COORDINATOR_V1: &str = "
CREATE TABLE cluster (
    cluster_id     TEXT PRIMARY KEY,
    coordinator_id TEXT NOT NULL,
    created_at_ms  INTEGER NOT NULL
);
CREATE TABLE coordinator_state (
    singleton  INTEGER PRIMARY KEY CHECK (singleton = 1),
    cluster_id TEXT NOT NULL REFERENCES cluster(cluster_id),
    epoch      INTEGER NOT NULL
);
CREATE TABLE source_ref (
    source_id       TEXT PRIMARY KEY,
    manifest_digest TEXT NOT NULL,
    created_at_ms   INTEGER NOT NULL
);
CREATE TABLE secret_bundle_ref (
    bundle_id     TEXT PRIMARY KEY,
    digest        TEXT NOT NULL,
    created_at_ms INTEGER NOT NULL
);
CREATE TABLE run (
    run_id           TEXT PRIMARY KEY,
    source_id        TEXT REFERENCES source_ref(source_id),
    secret_bundle_id TEXT REFERENCES secret_bundle_ref(bundle_id),
    spec_json        TEXT NOT NULL,
    created_at_ms    INTEGER NOT NULL,
    closed_at_ms     INTEGER
);
CREATE TABLE task (
    task_id   TEXT PRIMARY KEY,
    run_id        TEXT NOT NULL REFERENCES run(run_id),
    spec_json     TEXT NOT NULL,
    created_at_ms INTEGER NOT NULL
);
CREATE TABLE attempt (
    attempt_id             TEXT PRIMARY KEY,
    task_id            TEXT NOT NULL REFERENCES task(task_id),
    worker_id              TEXT NOT NULL,
    generation             INTEGER NOT NULL,
    payload_hash           TEXT NOT NULL,
    coordinator_epoch      INTEGER NOT NULL,
    permit_id              TEXT,
    state                  TEXT NOT NULL CHECK (state IN
        ('accepted','preparing','ready','starting','active','cancelling','finalizing','terminal')),
    termination_json       TEXT,
    receipts_json          TEXT NOT NULL,
    observation            TEXT NOT NULL DEFAULT 'known'
        CHECK (observation IN ('known','reconciling','unknown')),
    cancel_requested_at_ms INTEGER,
    retired                INTEGER NOT NULL DEFAULT 0 CHECK (retired IN (0, 1)),
    -- When the retire was confirmed: where GC measures retention from for an
    -- attempt that never became terminal.
    retired_at_ms          INTEGER,
    -- The coordinator gave up collecting the outputs: GC no longer waits.
    collection_failed_at_ms INTEGER,
    created_at_ms          INTEGER NOT NULL,
    terminal_at_ms         INTEGER,
    CHECK ((state = 'terminal') = (termination_json IS NOT NULL)),
    CHECK ((state = 'terminal') = (terminal_at_ms IS NOT NULL)),
    CHECK ((retired = 1) = (retired_at_ms IS NOT NULL)),
    UNIQUE (task_id, generation)
);
-- At most one attempt per task that is neither terminal nor confirmed retired.
CREATE UNIQUE INDEX attempt_one_live ON attempt(task_id)
    WHERE state <> 'terminal' AND retired = 0;
CREATE INDEX attempt_worker ON attempt(worker_id);
CREATE TABLE placement (
    placement_id  INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id   TEXT NOT NULL REFERENCES task(task_id),
    worker_id     TEXT NOT NULL,
    attempt_id    TEXT REFERENCES attempt(attempt_id),
    demand_json   TEXT NOT NULL,
    created_at_ms INTEGER NOT NULL,
    acked_at_ms   INTEGER
);
-- Kept for the life of the cluster: it is what stops an old submit from running.
CREATE TABLE retire_tombstone (
    attempt_id       TEXT PRIMARY KEY REFERENCES attempt(attempt_id),
    worker_id        TEXT NOT NULL,
    reason           TEXT NOT NULL,
    requested_at_ms  INTEGER NOT NULL,
    confirmed_at_ms  INTEGER
);
CREATE TRIGGER retire_tombstone_no_delete BEFORE DELETE ON retire_tombstone
BEGIN SELECT RAISE(ABORT, 'retire tombstones are kept for the life of the cluster'); END;
-- Only the confirmation may be added; what was asked for never changes.
CREATE TRIGGER retire_tombstone_fixed BEFORE UPDATE ON retire_tombstone
WHEN NEW.attempt_id <> OLD.attempt_id OR NEW.worker_id <> OLD.worker_id
    OR NEW.reason <> OLD.reason OR NEW.requested_at_ms <> OLD.requested_at_ms
    OR (OLD.confirmed_at_ms IS NOT NULL AND NEW.confirmed_at_ms IS NOT OLD.confirmed_at_ms)
BEGIN SELECT RAISE(ABORT, 'a retire tombstone only gains its confirmation'); END;
CREATE TABLE matrix_group (
    group_id      TEXT PRIMARY KEY,
    run_id        TEXT NOT NULL REFERENCES run(run_id),
    max_parallel  INTEGER NOT NULL CHECK (max_parallel > 0),
    created_at_ms INTEGER NOT NULL
);
CREATE TABLE matrix_permit (
    permit_id      TEXT PRIMARY KEY,
    group_id       TEXT NOT NULL REFERENCES matrix_group(group_id),
    attempt_id     TEXT UNIQUE REFERENCES attempt(attempt_id),
    held           INTEGER NOT NULL DEFAULT 1 CHECK (held IN (0, 1)),
    created_at_ms  INTEGER NOT NULL,
    released_at_ms INTEGER
);
CREATE TABLE artifact_receipt (
    attempt_id          TEXT NOT NULL REFERENCES attempt(attempt_id),
    name                TEXT NOT NULL,
    required            INTEGER NOT NULL CHECK (required IN (0, 1)),
    rel_path            TEXT,
    size                INTEGER,
    digest              TEXT,
    source_id           TEXT,
    plan_digest         TEXT,
    profile_fingerprint TEXT,
    persisted_at_ms     INTEGER,
    PRIMARY KEY (attempt_id, name)
);
CREATE TABLE blob_ref (
    ref_id        TEXT PRIMARY KEY,
    attempt_id    TEXT NOT NULL REFERENCES attempt(attempt_id),
    kind          TEXT NOT NULL CHECK (kind IN ('input','log','result')),
    locator       TEXT NOT NULL,
    pins          INTEGER NOT NULL DEFAULT 0 CHECK (pins >= 0),
    created_at_ms INTEGER NOT NULL
);
CREATE INDEX blob_ref_attempt ON blob_ref(attempt_id);
CREATE INDEX blob_ref_locator ON blob_ref(locator);
";

/// The queue: a cancel of a task is durable before any attempt of it exists,
/// and a cancelled task is never placed.
const COORDINATOR_V2: &str = "
ALTER TABLE task ADD COLUMN cancel_requested_at_ms INTEGER;
CREATE INDEX placement_attempt ON placement(attempt_id);
";

/// Result collection: what was read from the worker, by digest, and how the
/// collection of an ended attempt was closed.
const COORDINATOR_V3: &str = "
CREATE TABLE collection (
    attempt_id    TEXT PRIMARY KEY REFERENCES attempt(attempt_id),
    result_digest TEXT,
    result_json   TEXT,
    logs_json     TEXT,
    verdict       TEXT CHECK (verdict IN ('complete','required_missing','gave_up')),
    acked_at_ms   INTEGER,
    closed_at_ms  INTEGER,
    strikes       INTEGER NOT NULL DEFAULT 0 CHECK (strikes >= 0),
    updated_at_ms INTEGER NOT NULL,
    CHECK ((result_digest IS NULL) = (result_json IS NULL)),
    CHECK (acked_at_ms IS NULL OR closed_at_ms IS NOT NULL),
    CHECK ((closed_at_ms IS NULL) = (verdict IS NULL))
);
";

const WORKER_V1: &str = "
CREATE TABLE worker_state (
    singleton             INTEGER PRIMARY KEY CHECK (singleton = 1),
    worker_id             TEXT NOT NULL,
    agent_epoch           INTEGER NOT NULL,
    max_coordinator_epoch INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE worker_attempt (
    attempt_id             TEXT PRIMARY KEY,
    run_id                 TEXT NOT NULL,
    task_id            TEXT NOT NULL,
    generation             INTEGER NOT NULL,
    payload_hash           TEXT NOT NULL,
    coordinator_epoch      INTEGER NOT NULL,
    spec_json              TEXT NOT NULL,
    state                  TEXT NOT NULL CHECK (state IN
        ('accepted','preparing','ready','starting','active','cancelling','finalizing','terminal')),
    termination_json       TEXT,
    receipts_json          TEXT NOT NULL,
    cancel_requested_at_ms INTEGER,
    retired                INTEGER NOT NULL DEFAULT 0 CHECK (retired IN (0, 1)),
    accepted_at_ms         INTEGER NOT NULL,
    terminal_at_ms         INTEGER,
    result_json            TEXT,
    acked_at_ms            INTEGER,
    CHECK ((state = 'terminal') = (termination_json IS NOT NULL)),
    CHECK ((state = 'terminal') = (terminal_at_ms IS NOT NULL))
);
-- A retired attempt still counts until it is terminal (confirmed stopped).
CREATE UNIQUE INDEX worker_attempt_one_live ON worker_attempt(task_id)
    WHERE state <> 'terminal';
-- A cancel or retire that arrived before (or without) the submit. Never deleted.
CREATE TABLE tombstone (
    attempt_id        TEXT PRIMARY KEY,
    kind              TEXT NOT NULL CHECK (kind IN ('cancel','retire')),
    coordinator_epoch INTEGER NOT NULL,
    recorded_at_ms    INTEGER NOT NULL
);
CREATE TRIGGER tombstone_no_delete BEFORE DELETE ON tombstone
BEGIN SELECT RAISE(ABORT, 'tombstones are kept for the life of the cluster'); END;
-- A cancel may become a retire; the attempt and the epoch it was recorded
-- under never change, and a retire never goes back to a cancel.
CREATE TRIGGER tombstone_fixed BEFORE UPDATE ON tombstone
WHEN NEW.attempt_id <> OLD.attempt_id OR NEW.coordinator_epoch <> OLD.coordinator_epoch
    OR NEW.recorded_at_ms <> OLD.recorded_at_ms
    OR (OLD.kind = 'retire' AND NEW.kind <> 'retire')
BEGIN SELECT RAISE(ABORT, 'a tombstone keeps its attempt, epoch and time; a retire stays a retire'); END;
CREATE TABLE command_launch (
    attempt_id       TEXT NOT NULL REFERENCES worker_attempt(attempt_id),
    node_id          TEXT NOT NULL,
    intent_at_ms     INTEGER NOT NULL,
    boot_id          TEXT,
    pid              INTEGER,
    start_identity   TEXT,
    identity_at_ms   INTEGER,
    PRIMARY KEY (attempt_id, node_id)
);
CREATE TABLE lease (
    attempt_id        TEXT PRIMARY KEY REFERENCES worker_attempt(attempt_id),
    coordinator_epoch INTEGER NOT NULL,
    lease_seq         INTEGER NOT NULL,
    renewed_at_ms     INTEGER NOT NULL,
    expired_at_ms     INTEGER
);
CREATE TABLE log_spool (
    attempt_id    TEXT NOT NULL REFERENCES worker_attempt(attempt_id),
    stream        TEXT NOT NULL,
    written_bytes INTEGER NOT NULL DEFAULT 0,
    dropped_bytes INTEGER NOT NULL DEFAULT 0,
    last_seq      INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (attempt_id, stream)
);
CREATE TABLE blob_ref (
    ref_id        TEXT PRIMARY KEY,
    attempt_id    TEXT NOT NULL REFERENCES worker_attempt(attempt_id),
    kind          TEXT NOT NULL CHECK (kind IN ('input','log','result')),
    locator       TEXT NOT NULL,
    pins          INTEGER NOT NULL DEFAULT 0 CHECK (pins >= 0),
    created_at_ms INTEGER NOT NULL
);
CREATE INDEX blob_ref_attempt ON blob_ref(attempt_id);
CREATE INDEX blob_ref_locator ON blob_ref(locator);
";

/// The migration list of a database kind.
pub(crate) fn steps(kind: DbKind) -> Vec<String> {
    match kind {
        DbKind::Coordinator => vec![
            format!("{COMMON}{COORDINATOR_V1}"),
            COORDINATOR_V2.to_string(),
            COORDINATOR_V3.to_string(),
            "CREATE INDEX task_run_key ON task(run_id, task_id);".to_string(),
        ],
        DbKind::Worker => vec![format!("{COMMON}{WORKER_V1}")],
    }
}

/// The compatibility table for a list of migration steps.
pub(crate) fn compat(step_count: usize) -> CompatTable {
    CompatTable {
        current: step_count as u32,
        min_readable: 1,
        min_migratable: 1,
    }
}
