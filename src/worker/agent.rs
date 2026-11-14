//! The worker agent: every worker-side operation of spec chapter 11 over the
//! worker database, and the life of an accepted attempt up to its supervisor.
//!
//! The agent never runs a user command. It persists the acceptance, prepares
//! the workspace, writes the supervisor's spec and starts `supervise` as a
//! detached process; from then on the supervisor owns the attempt and the
//! agent only watches. One thread per attempt (`drive`) does this, and the
//! same loop is what a restarted agent runs for the attempts it finds: a
//! living supervisor is left alone (adopted), a dead one is replaced by a
//! `supervise` that recovers and never spawns the command again.
//!
//! Everything here blocks (SQLite, files, child processes): [`super::serve`]
//! calls it through `spawn_blocking`.

use super::blob_policy::BlobLimits;
use super::blobs;
use super::host::Host;
use super::policy::{self, AttemptFacts, Recovery, Settle, Step, SupervisorExit};
use crate::error::{ApiError, ConflictReason, ErrorCode};
use crate::ids::*;
use crate::platform;
use crate::setup::builder::SetupPoll;
use crate::state::{
    AttemptState, Outcome, Receipt, Termination, TimeoutInfo, TimeoutKind, TimeoutPhase,
};
use crate::storage::{
    AcceptedAttempt, CancelOutcome, GcReport, LeaseOutcome, NewAcceptance, ProcessLock,
    StorageError, SubmitOutcome, WorkerStore,
};
use crate::supervisor::lease::LeaseSeed;
use crate::supervisor::lock::SupervisorLock;
use crate::supervisor::outcome::drain_pending_deletes;
use crate::supervisor::process::spawn_detached;
use crate::supervisor::spec::{
    ArtifactFile, ArtifactsSpec, MaskValues, NodeSpec, SuperviseSpec, DEFAULT_ARTIFACT_BYTES,
    DEFAULT_LOG_LIMIT_BYTES,
};
use crate::supervisor::spool::{SpoolError, SpoolReader};
use crate::supervisor::spool_plan::result_not_stored;
use crate::supervisor::termination::{termination, StopReason};
use crate::wire::*;
use crate::workspace::plan::check_cwd_relative;
use crate::workspace::Workspace;
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io::Write;
use std::os::unix::fs::{DirBuilderExt, PermissionsExt};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::path::{Path, PathBuf};
use std::process::Child;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::thread::JoinHandle;
use std::time::Duration;

/// What an agent is started with. Nothing is read from the process
/// environment: the boot ID and the supervisor binary are inputs.
#[derive(Debug, Clone)]
pub struct AgentConfig {
    /// `worker.db`, the attempts and the locks live here (mode 0700).
    pub state_dir: PathBuf,
    pub worker_id: WorkerId,
    /// This boot of the machine (`platform::boot_id()` in production).
    pub boot_id: WorkerBootId,
    /// The binary whose `supervise --spec <file>` runs an attempt.
    pub supervisor_exe: PathBuf,
    pub traffic_state_dir: PathBuf,
    pub traffic_config: Option<PathBuf>,
    pub profile: String,
    pub profile_fingerprint: Digest,
    pub max_active_task: u32,
    pub prefetch_per_worker: u32,
    /// How long a forecast answer stays usable (`scheduler.forecast_ttl_ms`,
    /// spec default 2000). It is the `ttl_ms` the answer carries.
    pub forecast_ttl_ms: u64,
    pub term_grace_ms: u64,
    /// Maximum bytes retained per supervisor log spool.
    pub log_limit_bytes: u64,
    pub kill_wait_ms: u64,
    /// How often the supervisors and the agent's own loops look again.
    pub poll_ms: u64,
    pub inhibit_sleep: bool,
    /// How long `retire_attempt` waits for the stop before it answers
    /// `unconfirmed` (the coordinator asks again).
    pub retire_wait_ms: u64,
    /// How long a prepared attempt waits for its secret bundle.
    pub secret_wait_ms: u64,
    /// Pause before a supervisor that could not settle is started again.
    pub retry_ms: u64,
    /// How long a start waits for the agent lock. A process that was forked
    /// by the previous agent holds the lock's descriptor until its `exec`,
    /// so the lock of an agent that just went away can look held for a
    /// moment; a living agent holds it for good and the start is refused.
    pub lock_wait_ms: u64,
    /// Files of ended attempts are kept this long (spec 13: seven days).
    pub retention_ms: u64,
    /// Without `ack_collected`, the protection ends this long after the end.
    pub ack_wait_ms: u64,
    /// Attempts that are not terminal, at most. A new attempt beyond that is
    /// refused with `OVERLOADED`; nothing is stored for it.
    pub max_live_attempts: u32,
    /// Workspaces prepared at once. The other attempts wait their turn.
    pub max_concurrent_prepares: u32,
    /// How long the preparation of one workspace may take before the attempt
    /// fails (the default of the workspace tool: 15 minutes).
    pub prepare_timeout_ms: u64,
    /// Supervisor starts that may fail in a row before the agent gives up.
    pub max_spawn_failures: u32,
    /// Attempts in one `list_attempts` answer; more is an error.
    pub max_list_attempts: u32,
    /// Journal entries of one attempt a `watch_events` reads; more is an
    /// error.
    pub max_event_scan: u32,
    /// What a coordinator may send with `blob_put`.
    pub blobs: BlobLimits,
}

impl AgentConfig {
    pub fn new(
        state_dir: PathBuf,
        worker_id: WorkerId,
        boot_id: WorkerBootId,
        supervisor_exe: PathBuf,
    ) -> Self {
        AgentConfig {
            traffic_state_dir: state_dir.join("traffic"),
            state_dir,
            worker_id,
            boot_id,
            supervisor_exe,
            traffic_config: None,
            profile: "default".into(),
            profile_fingerprint: Digest::of_bytes(b""),
            max_active_task: 1,
            prefetch_per_worker: 1,
            forecast_ttl_ms: 2000,
            log_limit_bytes: DEFAULT_LOG_LIMIT_BYTES,
            term_grace_ms: crate::supervisor::spec::DEFAULT_TERM_GRACE_MS,
            kill_wait_ms: 5_000,
            poll_ms: 200,
            inhibit_sleep: true,
            retire_wait_ms: 2_000,
            secret_wait_ms: 60_000,
            retry_ms: 5_000,
            lock_wait_ms: 2_000,
            retention_ms: crate::storage::DEFAULT_ACK_WAIT_MS,
            ack_wait_ms: crate::storage::DEFAULT_ACK_WAIT_MS,
            max_live_attempts: policy::DEFAULT_MAX_LIVE_ATTEMPTS,
            max_concurrent_prepares: policy::DEFAULT_MAX_PREPARES,
            prepare_timeout_ms: crate::workspace::DEFAULT_PREPARE_TIMEOUT.as_millis() as u64,
            max_spawn_failures: policy::DEFAULT_MAX_SPAWN_FAILURES,
            max_list_attempts: policy::MAX_LIST_ATTEMPTS,
            max_event_scan: policy::MAX_EVENT_SCAN,
            blobs: BlobLimits::default(),
        }
    }

    /// Where each attempt keeps its files: `<root>/<attempt>/` holds the
    /// supervisor spec, the workspace record, `logs/` and `artifacts/`.
    pub fn attempts_root(&self) -> PathBuf {
        self.state_dir.join("attempts")
    }

    /// The agent's Unix socket.
    pub fn socket_path(&self) -> PathBuf {
        self.state_dir.join("agent.sock")
    }
}

/// What the connection's `hello` established.
#[derive(Debug, Clone, Copy)]
pub struct Session {
    pub coordinator_epoch: CoordinatorEpoch,
}

/// What a startup found and did, for the log and the tests.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StartupReport {
    pub steps: Vec<Step>,
    pub boot_changed: bool,
    /// Attempts fixed as interrupted or cancelled without a supervisor.
    pub settled: Vec<AttemptId>,
    /// Attempts handed to a watcher thread (adopt, recover or resume).
    pub watched: Vec<AttemptId>,
    /// Attempts that may have run and cannot be recovered.
    pub unresolved: Vec<AttemptId>,
}

pub struct Agent {
    cfg: AgentConfig,
    store: WorkerStore,
    host: Arc<dyn Host>,
    epoch: AgentEpoch,
    lock: Mutex<Option<ProcessLock>>,
    driving: Mutex<BTreeSet<String>>,
    threads: Mutex<Vec<JoinHandle<()>>>,
    stopping: AtomicBool,
    report: StartupReport,
    /// Workspaces being prepared.
    prepares: Arc<Permits>,
    /// One lock per attempt (by hash): the cleanup of an attempt and the
    /// placement of its secrets never run at once, from whichever thread.
    shards: Arc<Vec<Mutex<()>>>,
    /// When this process accepted an attempt and last extended its lease.
    clocks: Mutex<BTreeMap<String, Clock>>,
    /// A renewal (its commit and the new grant time) and the measuring of a
    /// lease before a start (the applied key, the grant time, the expiry it
    /// may write) never interleave: whoever measures sees a key together
    /// with the time of that key's grant, and no renewal lands between the
    /// finding that a lease ran out and the record of it. Not the attempt's
    /// shard: that one is held across a cleanup, which may take long.
    lease_step: Mutex<()>,
    /// See [`Agent::set_probe`].
    probe: Mutex<Option<Probe>>,
    /// Supervisors this agent started that had not exited when their watcher
    /// let go of them: waited for later, so none stays a zombie.
    children: Mutex<Vec<Child>>,
    /// Attempts the agent stopped trying to start or recover.
    gave_up: Mutex<BTreeSet<String>>,
    /// The last diagnostic written per subject, so a repeating failure is
    /// one event.
    noted: Mutex<BTreeMap<String, String>>,
    /// One new acceptance at a time: the live cap and the insert are one step.
    submitting: Mutex<()>,
    ticking: AtomicBool,
    /// Directory enumeration continues across bounded terminal cleanup passes.
    cleanup_scan: Mutex<Option<fs::ReadDir>>,
}

/// A point in the agent's work a test may stop at. See [`Agent::set_probe`].
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProbePoint {
    /// A start is about to measure the lease of the attempt.
    LeaseMeasure,
    /// A renewal is committed; its grant time is not recorded yet.
    LeaseRenewed,
}

#[doc(hidden)]
pub type Probe = Arc<dyn Fn(ProbePoint, &AttemptId) + Send + Sync>;

/// Monotonic times (suspend included) of this process.
#[derive(Debug, Clone, Copy)]
struct Clock {
    accepted_ms: u64,
    granted_ms: u64,
}

struct Permits {
    used: Mutex<u32>,
    max: u32,
}

struct Permit(Arc<Permits>);

impl Permits {
    fn try_acquire(p: &Arc<Permits>) -> Option<Permit> {
        let mut used = lock(&p.used);
        if *used >= p.max {
            return None;
        }
        *used += 1;
        Some(Permit(p.clone()))
    }
}

impl Drop for Permit {
    fn drop(&mut self) {
        let mut used = lock(&self.0.used);
        *used = used.saturating_sub(1);
    }
}

/// What a preparation running on its own thread left for its waiter.
enum Slot {
    Waiting,
    Done(Result<Workspace, ApiError>),
    /// The waiter gave up: a workspace that still appears is removed.
    Abandoned,
}

/// Takes the attempt out of `driving` when its watcher ends, however it ends.
struct Driving {
    agent: Arc<Agent>,
    attempt: AttemptId,
}

impl Drop for Driving {
    fn drop(&mut self) {
        lock(&self.agent.driving).remove(self.attempt.as_str());
    }
}

struct Ticking<'a>(&'a AtomicBool);

impl Drop for Ticking<'_> {
    fn drop(&mut self) {
        self.0.store(false, Ordering::SeqCst);
    }
}

const SHARDS: usize = 64;

/// A panic elsewhere must not take the agent's bookkeeping with it: the data
/// behind these locks stays consistent under every partial update.
fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

fn shard<'a>(shards: &'a [Mutex<()>], attempt: &AttemptId) -> &'a Mutex<()> {
    use std::hash::{Hash, Hasher};
    let mut h = std::collections::hash_map::DefaultHasher::new();
    attempt.as_str().hash(&mut h);
    &shards[(h.finish() % shards.len() as u64) as usize]
}

fn now() -> u64 {
    platform::wall_ms()
}

fn failed() -> Termination {
    Termination::new(Outcome::Failed, None, None).expect("failed without an exit is valid")
}

fn api(e: StorageError) -> ApiError {
    // The one conflict a sender must not answer by sending again.
    if matches!(e, StorageError::PayloadConflict(_)) {
        return ApiError::conflict(ConflictReason::PayloadMismatch, e.to_string());
    }
    // A busy database committed nothing: the same request may be sent again.
    let code = if e.is_retryable() {
        ErrorCode::Overloaded
    } else {
        e.code()
    };
    ApiError::new(code, e.to_string())
}

fn internal(what: &str, e: impl std::fmt::Display) -> ApiError {
    ApiError::new(ErrorCode::Internal, format!("{what}: {e}"))
}

fn invalid(m: impl Into<String>) -> ApiError {
    ApiError::new(ErrorCode::InvalidInput, m)
}

/// Refuse a payload that declares one of the attempt's secret files (the
/// declared ones and what `.worktreeinclude` matched) as an artifact.
fn check_artifacts_not_secret(
    payload: &AttemptPayload,
    secrets: &[String],
) -> Result<(), ApiError> {
    if policy::artifact_is_secret(&payload.artifacts, secrets) {
        return Err(invalid(
            "a declared artifact is one of the attempt's secret files",
        ));
    }
    Ok(())
}

/// The file is this user's alone (0600): the supervisor refuses a spec that
/// anybody else could read or that somebody else wrote.
fn write_atomic(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    let dir = path.parent().expect("a file has a parent");
    let mut tmp = tempfile::NamedTempFile::new_in(dir)?;
    tmp.as_file()
        .set_permissions(fs::Permissions::from_mode(0o600))?;
    tmp.write_all(bytes)?;
    tmp.as_file().sync_all()?;
    tmp.persist(path).map_err(|e| e.error)?;
    Ok(())
}

/// `<dir>` exists, is a directory of this user's, and nobody else can enter.
fn private_dir(dir: &Path) -> std::io::Result<()> {
    fs::DirBuilder::new()
        .recursive(true)
        .mode(0o700)
        .create(dir)?;
    fs::set_permissions(dir, fs::Permissions::from_mode(0o700))
}

/// What an ended attempt answers with: its termination, the stored and the
/// unstored artifacts, the digest of the result, and whether a result is
/// stored (otherwise the digest is of the termination alone).
type Ended = (
    Termination,
    Vec<ArtifactRecord>,
    Vec<ArtifactNotStored>,
    Digest,
    bool,
);

impl Agent {
    /// Run the startup steps in [`policy::STARTUP_ORDER`] and return the
    /// running agent. Any failing step refuses the start.
    pub fn start(cfg: AgentConfig, host: Arc<dyn Host>) -> Result<Arc<Agent>, ApiError> {
        let mut lock = None;
        let mut opened: Option<(WorkerStore, AgentEpoch)> = None;
        let mut report = StartupReport::default();
        let mut todo: Vec<AttemptId> = Vec::new();
        let boot_file = cfg.state_dir.join("boot_id");
        for step in policy::STARTUP_ORDER {
            let store = opened.as_ref().map(|o| &o.0);
            match step {
                Step::StateDir => {
                    private_dir(&cfg.state_dir).map_err(|e| internal("state directory", e))?;
                    private_dir(&cfg.attempts_root())
                        .map_err(|e| internal("attempts directory", e))?;
                }
                Step::CoreDumps => host.disable_core_dumps()?,
                Step::Lock => lock = Some(acquire_lock(&cfg)?),
                Step::Store => {
                    let s = WorkerStore::open(&cfg.state_dir, &cfg.worker_id).map_err(api)?;
                    let e = s.start_agent(now()).map_err(api)?;
                    let recorded = match fs::read_to_string(&boot_file) {
                        Ok(t) => Some(t),
                        Err(e) if e.kind() == std::io::ErrorKind::NotFound => None,
                        Err(e) => return Err(internal("boot id record", e)),
                    };
                    let has_history = s.count_nonterminal().map_err(api)? > 0
                        || s.count_launches().map_err(api)? > 0;
                    report.boot_changed = policy::boot_changed(
                        recorded.as_deref(),
                        cfg.boot_id.as_str(),
                        has_history,
                    );
                    opened = Some((s, e));
                }
                Step::StoreSecretCopies => host.sweep_store_copies()?,
                Step::Vault => host.sweep_vault()?,
                Step::PlacedSecrets => {
                    let s = store.expect("the store is open");
                    // Unknown or unreadable counts as ended: fail closed.
                    host.reconcile_secrets(
                        &|a| !matches!(s.get(a), Ok(Some(r)) if r.state != AttemptState::Terminal),
                    )?
                }
                Step::Workspaces => {
                    let s = store.expect("the store is open");
                    // An unreadable record keeps the workspace.
                    host.sweep_workspaces(&|a| {
                        !matches!(s.get(a), Ok(None))
                            && !matches!(s.get(a), Ok(Some(r)) if r.state == AttemptState::Terminal)
                    })?
                }
                Step::Attempts => {
                    let s = store.expect("the store is open");
                    for rec in s.list_nonterminal().map_err(api)? {
                        let a = rec.attempt_id.clone();
                        match recovery_of(&cfg, s, &rec, report.boot_changed)? {
                            Recovery::Settle(how) => {
                                if settle(&cfg, s, &a, how_termination(how), how, false)? {
                                    report.settled.push(a.clone());
                                }
                                // Settled or held by a living supervisor:
                                // the watcher cleans up or adopts.
                                todo.push(a);
                            }
                            Recovery::Unresolved => {
                                note(s, &rec, "agent_unresolved", json!({"why": "no spec"}));
                                if report.boot_changed {
                                    // Kept per attempt: the next start no
                                    // longer sees the change.
                                    note(s, &rec, "agent_boot_changed", json!({}));
                                }
                                report.unresolved.push(a);
                            }
                            _ => todo.push(a),
                        }
                    }
                }
                Step::BootRecord => write_atomic(&boot_file, cfg.boot_id.as_str().as_bytes())
                    .map_err(|e| internal("boot id record", e))?,
            }
            report.steps.push(step);
        }
        let (store, epoch) = opened.expect("the store step ran");
        report.watched = todo.clone();
        let max_prepares = cfg.max_concurrent_prepares.max(1);
        let agent = Arc::new(Agent {
            cfg,
            store,
            host,
            epoch,
            lock: Mutex::new(lock),
            driving: Mutex::default(),
            threads: Mutex::default(),
            stopping: AtomicBool::new(false),
            report,
            prepares: Arc::new(Permits {
                used: Mutex::new(0),
                max: max_prepares,
            }),
            shards: Arc::new((0..SHARDS).map(|_| Mutex::new(())).collect()),
            clocks: Mutex::default(),
            lease_step: Mutex::new(()),
            probe: Mutex::new(None),
            children: Mutex::default(),
            gave_up: Mutex::default(),
            noted: Mutex::default(),
            submitting: Mutex::default(),
            ticking: AtomicBool::new(false),
            cleanup_scan: Mutex::new(None),
        });
        for a in todo {
            agent.kick(&a);
        }
        // Attempts that ended while no agent ran still hold a workspace.
        agent.cleanup_terminal(32);
        Ok(agent)
    }

    pub fn config(&self) -> &AgentConfig {
        &self.cfg
    }

    pub fn store(&self) -> &WorkerStore {
        &self.store
    }

    pub fn agent_epoch(&self) -> AgentEpoch {
        self.epoch
    }

    pub fn startup_report(&self) -> &StartupReport {
        &self.report
    }

    /// Stop watching and release the agent lock. Supervisors keep running:
    /// they do not depend on the agent (spec chapter 12).
    pub fn shutdown(&self) {
        self.stopping.store(true, Ordering::SeqCst);
        let threads = std::mem::take(&mut *lock(&self.threads));
        for t in threads {
            let _ = t.join();
        }
        lock(&self.lock).take();
    }

    /// For the tests: `probe` is called at each [`ProbePoint`] the agent
    /// passes, on the thread that passes it, and that thread goes on when it
    /// returns. This is how a test puts a renewal and a start in an order.
    #[doc(hidden)]
    pub fn set_probe(&self, probe: Probe) {
        *lock(&self.probe) = Some(probe);
    }

    fn at(&self, point: ProbePoint, attempt: &AttemptId) {
        let probe = lock(&self.probe).clone();
        if let Some(probe) = probe {
            probe(point, attempt);
        }
    }

    fn stopping(&self) -> bool {
        self.stopping.load(Ordering::SeqCst)
    }

    fn pause(&self, ms: u64) {
        let step = self.cfg.poll_ms.clamp(1, 100);
        let mut left = ms;
        while left > 0 && !self.stopping() {
            let d = left.min(step);
            std::thread::sleep(Duration::from_millis(d));
            left -= d;
        }
    }

    /// [`Self::pause`], ended early when the attempt is cancelled or retired
    /// meanwhile (or is gone): that is acted on at the next look, not after
    /// the wait.
    fn pause_unless_news(&self, rec: &AcceptedAttempt, ms: u64) {
        let step = self.cfg.poll_ms.max(1);
        let mut left = ms;
        while left > 0 && !self.stopping() {
            let d = left.min(step);
            self.pause(d);
            left -= d;
            let news = match self.store.get(&rec.attempt_id) {
                Ok(Some(now)) => policy::wakes(
                    rec.cancel_requested_at_ms.is_some(),
                    now.cancel_requested_at_ms.is_some(),
                    rec.retired,
                    now.retired,
                ),
                Ok(None) => true,
                Err(_) => false,
            };
            if news {
                break;
            }
        }
    }

    // ------------------------------------------------------------ attempts

    /// Make sure a thread watches `attempt`. Idempotent.
    pub fn kick(self: &Arc<Self>, attempt: &AttemptId) {
        self.reap();
        if self.stopping() || lock(&self.gave_up).contains(attempt.as_str()) {
            return;
        }
        if !lock(&self.driving).insert(attempt.to_string()) {
            return;
        }
        // Made before the thread exists: a thread that cannot be started
        // drops the guard with its closure, a panicking one while unwinding.
        let guard = Driving {
            agent: self.clone(),
            attempt: attempt.clone(),
        };
        let spawned = std::thread::Builder::new()
            .name("attempt".into())
            .spawn(move || {
                let (me, a) = (&guard.agent, &guard.attempt);
                if catch_unwind(AssertUnwindSafe(|| me.drive(a))).is_err() {
                    me.drive_panicked(a);
                }
            });
        match spawned {
            Ok(t) => {
                let mut threads = lock(&self.threads);
                threads.retain(|t| !t.is_finished());
                threads.push(t);
            }
            // The next tick tries again.
            Err(e) => self.note_once(
                attempt,
                "agent_thread_failed",
                json!({ "error": e.to_string() }),
            ),
        }
    }

    /// The watcher of `attempt` panicked. Nothing may be left half-watched
    /// for ever: the attempt is failed when no command was started, and
    /// reported as unknown otherwise.
    fn drive_panicked(&self, attempt: &AttemptId) {
        let Ok(Some(rec)) = self.store.get(attempt) else {
            return;
        };
        note(&self.store, &rec, "agent_drive_panicked", json!({}));
        if rec.state != AttemptState::Terminal {
            self.give_up(&rec, "the watcher panicked", failed());
        }
    }

    /// Stop trying to start or recover `rec`. With no launch intent the
    /// result is fixed here as `t`; an attempt whose command may have run is
    /// left as it is, no longer watched, and reported as unknown (a retire
    /// settles it once the machine has rebooted).
    fn give_up(&self, rec: &AcceptedAttempt, why: &str, t: Termination) {
        let a = &rec.attempt_id;
        let settled = settle(&self.cfg, &self.store, a, t, Settle::Interrupted, false);
        let settled = matches!(settled, Ok(true));
        note(
            &self.store,
            rec,
            "agent_gave_up",
            json!({ "why": why, "settled": settled }),
        );
        if settled {
            self.clean(a);
        } else {
            lock(&self.gave_up).insert(a.to_string());
        }
    }

    fn drive(&self, attempt: &AttemptId) {
        let mut child: Option<Child> = None;
        let mut noted_exit = false;
        // Supervisor starts and supervisors that failed, in a row.
        let mut failures = 0u32;
        // Supervisors, in a row, that could not tell whose a process is.
        let mut unknowns = 0u32;
        let mut settle_errors = 0u32;
        while !self.stopping() {
            let rec = match self.store.get(attempt) {
                Ok(Some(r)) => r,
                Ok(None) => break,
                Err(_) => {
                    self.pause(self.cfg.poll_ms);
                    continue;
                }
            };
            if rec.state == AttemptState::Terminal {
                // The supervisor is about to exit; it is waited for later.
                self.park(child.take());
                self.clean(attempt);
                return;
            }
            // A supervisor we started and that still runs owns the attempt.
            if let Some(c) = child.as_mut() {
                match c.try_wait() {
                    Ok(None) => {
                        self.pause(self.cfg.poll_ms);
                        continue;
                    }
                    Ok(Some(status)) => {
                        let code = status.code();
                        child = None;
                        let exit = policy::supervisor_exit(code);
                        if exit == SupervisorExit::IdentityUnknown {
                            // Nothing was signalled and nothing is known. Not
                            // a failure: the attempt is never given up for
                            // it. It is looked at again, later each time.
                            let wait = policy::backoff_ms(self.cfg.retry_ms, unknowns);
                            if unknowns == 0 || policy::backoff_capped(unknowns) {
                                note(
                                    &self.store,
                                    &rec,
                                    "agent_identity_unknown",
                                    json!({
                                        "code": code,
                                        "retry_ms": wait,
                                        "longest": policy::backoff_capped(unknowns),
                                    }),
                                );
                            }
                            unknowns = unknowns.saturating_add(1);
                            self.pause_unless_news(&rec, wait);
                        } else if exit == SupervisorExit::Failed {
                            if !noted_exit {
                                noted_exit = true;
                                note(
                                    &self.store,
                                    &rec,
                                    "agent_supervisor_exit",
                                    json!({ "code": code }),
                                );
                            }
                            failures += 1;
                            if policy::gives_up(failures, self.cfg.max_spawn_failures) {
                                self.give_up(
                                    &rec,
                                    "the supervisor kept failing",
                                    termination(None, None),
                                );
                                return;
                            }
                            self.pause(self.cfg.retry_ms);
                        }
                        continue;
                    }
                    Err(_) => {
                        child = None;
                    }
                }
            }
            // The lock tells whether any supervisor (ours or an adopted one)
            // lives. Only this thread starts supervisors for the attempt, so
            // taking it for a moment cannot make a new one give up. (The
            // acknowledgement of an attempt that ended without a result
            // takes it too, for an attempt that is terminal already.)
            match SupervisorLock::try_acquire(&self.cfg.state_dir, attempt) {
                Ok(Some(lock)) => drop(lock),
                Ok(None) | Err(_) => {
                    self.pause(self.cfg.poll_ms);
                    continue;
                }
            }
            let started = match recovery_of(&self.cfg, &self.store, &rec, false) {
                Ok(Recovery::Nothing) | Err(_) => {
                    self.pause(self.cfg.poll_ms);
                    continue;
                }
                Ok(Recovery::Unresolved) => break,
                Ok(Recovery::Settle(how)) => {
                    let done = settle(
                        &self.cfg,
                        &self.store,
                        attempt,
                        how_termination(how),
                        how,
                        false,
                    );
                    self.after_settle(attempt, done, &mut settle_errors);
                    continue;
                }
                Ok(Recovery::Supervise) => self.spawn(&rec).map(Some),
                Ok(Recovery::Resume) => {
                    if rec.state == AttemptState::Ready && self.spec_path(attempt).is_file() {
                        self.launch(&rec)
                    } else {
                        if let Err(e) = self.prepare(&rec) {
                            note(
                                &self.store,
                                &rec,
                                "agent_prepare_failed",
                                json!({ "code": e.code, "message": e.message }),
                            );
                            let done = settle(
                                &self.cfg,
                                &self.store,
                                attempt,
                                failed(),
                                Settle::Interrupted,
                                false,
                            );
                            self.after_settle(attempt, done, &mut settle_errors);
                        }
                        continue;
                    }
                }
            };
            match started {
                Ok(Some(c)) => child = Some(c),
                // Settled instead of started: the next look sees the end.
                Ok(None) => {}
                Err(()) => {
                    failures += 1;
                    if policy::gives_up(failures, self.cfg.max_spawn_failures) {
                        self.give_up(
                            &rec,
                            "the supervisor could not be started",
                            termination(None, None),
                        );
                        return;
                    }
                    self.pause(self.cfg.retry_ms);
                }
            }
        }
        self.park(child);
    }

    /// A settle that failed is tried again, later each time; one that found
    /// the attempt owned by somebody else waits for the next look.
    fn after_settle(&self, attempt: &AttemptId, done: Result<bool, ApiError>, errors: &mut u32) {
        match done {
            Ok(true) => *errors = 0,
            Ok(false) => self.pause(self.cfg.poll_ms),
            Err(e) => {
                self.note_once(
                    attempt,
                    "agent_settle_failed",
                    json!({ "code": e.code, "message": e.message }),
                );
                self.pause(policy::backoff_ms(self.cfg.retry_ms, *errors));
                *errors = errors.saturating_add(1);
            }
        }
    }

    fn spawn(&self, rec: &AcceptedAttempt) -> Result<Child, ()> {
        spawn_detached(&self.cfg.supervisor_exe, &self.spec_path(&rec.attempt_id)).map_err(|e| {
            note(
                &self.store,
                rec,
                "agent_spawn_failed",
                json!({ "error": e.to_string() }),
            );
        })
    }

    /// Start the supervisor that will start the commands of a prepared
    /// attempt. The lease and the deadline have been running since the
    /// acceptance: with the lease run out nothing is started (spec chapter
    /// 12), and the supervisor is given what is left of both: the deadline,
    /// and the lease as a seed for its own lease clock, so that it stops the
    /// commands when the grant made before its start runs out.
    /// `Ok(None)`: the attempt was settled instead (or will be, next look).
    fn launch(&self, rec: &AcceptedAttempt) -> Result<Option<Child>, ()> {
        let a = &rec.attempt_id;
        let payload: AttemptPayload = serde_json::from_value(rec.spec.clone()).map_err(|e| {
            self.note_once(
                a,
                "agent_spawn_failed",
                json!({ "error": format!("stored payload: {e}") }),
            );
        })?;
        self.at(ProbePoint::LeaseMeasure, a);
        // The key and the time of its grant, with no renewal in between
        // (`lease_step`): a key read with the time of an earlier grant would
        // end a lease that was just renewed, here or in the supervisor. A
        // renewal that arrives after this makes the seed void (the supervisor
        // then counts that grant in full), never too short.
        let measuring = lock(&self.lease_step);
        let measured_at = platform::monotonic_ms();
        let measured = self
            .store
            .lease(a)
            .and_then(|lease| Ok((lease.map(|(k, _)| k), self.elapsed(rec, measured_at)?)));
        let (key, (since_accept, since_grant)) = match measured {
            Ok(m) => m,
            Err(e) => {
                // Without the key the supervisor would take the seed for
                // void and count a full period from its start; without the
                // grant time the lease cannot be measured at all. Nothing is
                // started and nothing is given up: the next look reads again.
                drop(measuring);
                self.note_once(
                    a,
                    "agent_lease_unreadable",
                    json!({ "error": e.to_string() }),
                );
                self.pause(self.cfg.retry_ms);
                return Ok(None);
            }
        };
        let Some(lease_left) = policy::remaining_ms(payload.lease_ms, since_grant) else {
            // The journal gets `lease_expired` with it. Still under the
            // lock: a renewal either came before the measuring or is refused.
            let _ = self.store.expire_lease(a, now());
            drop(measuring);
            let t = termination(Some(StopReason::LeaseExpired), None);
            self.settle_unstarted(a, t);
            return Ok(None);
        };
        drop(measuring);
        let mut deadline_left = None;
        if let Some(total) = payload.deadline_ms {
            let Some(left) = policy::remaining_ms(total, since_accept) else {
                self.note_once(a, "agent_deadline_before_start", json!({}));
                let info = TimeoutInfo {
                    kind: TimeoutKind::Deadline,
                    phase: TimeoutPhase::Preparing,
                };
                let t = termination(Some(StopReason::Timeout(info)), None);
                self.settle_unstarted(a, t);
                return Ok(None);
            };
            deadline_left = Some(left);
        }
        let seed = LeaseSeed {
            ends_mono_ms: measured_at.saturating_add(lease_left),
            key,
        };
        // Written at every start: an earlier start's values are stale.
        self.rewrite_start(a, deadline_left, seed).map_err(|e| {
            self.note_once(
                a,
                "agent_spawn_failed",
                json!({ "error": format!("supervisor spec: {e}") }),
            );
        })?;
        self.spawn(rec).map(Some)
    }

    fn settle_unstarted(&self, attempt: &AttemptId, t: Termination) {
        if settle(
            &self.cfg,
            &self.store,
            attempt,
            t,
            Settle::Interrupted,
            false,
        )
        .is_err()
        {
            self.pause(self.cfg.retry_ms);
        }
    }

    /// The supervisor counts its deadline from its own start, and its lease
    /// from the seed.
    fn rewrite_start(
        &self,
        attempt: &AttemptId,
        deadline_left_ms: Option<u64>,
        seed: LeaseSeed,
    ) -> Result<(), String> {
        let path = self.spec_path(attempt);
        let text = fs::read_to_string(&path).map_err(|e| e.to_string())?;
        let mut spec: SuperviseSpec = serde_json::from_str(&text).map_err(|e| e.to_string())?;
        spec.deadline_ms = deadline_left_ms;
        spec.lease_seed = Some(seed);
        let bytes = serde_json::to_vec(&spec).map_err(|e| e.to_string())?;
        write_atomic(&path, &bytes).map_err(|e| e.to_string())
    }

    /// How long ago the attempt was accepted and its lease last granted, in
    /// milliseconds: on this process's monotonic clock when it saw both, else
    /// (the agent restarted since) from the wall-clock times this worker
    /// recorded. No other host's time is involved. A renewal time that
    /// cannot be read is an error, not "never renewed": that would end a
    /// renewed lease early.
    fn elapsed(&self, rec: &AcceptedAttempt, mono: u64) -> Result<(u64, u64), StorageError> {
        if let Some(c) = lock(&self.clocks).get(rec.attempt_id.as_str()) {
            return Ok((
                mono.saturating_sub(c.accepted_ms),
                mono.saturating_sub(c.granted_ms),
            ));
        }
        let wall = now();
        let granted = self
            .store
            .lease_renewed_at(&rec.attempt_id)?
            .unwrap_or(rec.accepted_at_ms)
            .max(rec.accepted_at_ms);
        Ok((
            wall.saturating_sub(rec.accepted_at_ms),
            wall.saturating_sub(granted),
        ))
    }

    /// Wait (without blocking) for the supervisors that have exited.
    fn reap(&self) {
        lock(&self.children).retain_mut(|c| matches!(c.try_wait(), Ok(None)));
    }

    fn park(&self, child: Option<Child>) {
        if let Some(mut c) = child {
            if matches!(c.try_wait(), Ok(None)) {
                lock(&self.children).push(c);
            }
        }
    }

    fn attempt_dir(&self, attempt: &AttemptId) -> PathBuf {
        self.cfg.attempts_root().join(attempt.as_str())
    }

    fn spec_path(&self, attempt: &AttemptId) -> PathBuf {
        spec_path(&self.cfg, attempt)
    }

    fn workspace_path(&self, attempt: &AttemptId) -> PathBuf {
        self.attempt_dir(attempt).join("workspace.json")
    }

    fn workspace_of(&self, attempt: &AttemptId) -> Option<Workspace> {
        let text = fs::read_to_string(self.workspace_path(attempt)).ok()?;
        serde_json::from_str(&text).ok()
    }

    /// `host.prepare` under the limits: at most `max_concurrent_prepares` at
    /// once, at most `prepare_timeout_ms` long. It runs on its own thread,
    /// which keeps its permit until the host returns, however late; a
    /// workspace nobody waits for any more is removed. `Ok(None)`: the agent
    /// is stopping or the attempt is being stopped.
    fn host_prepare(
        &self,
        rec: &AcceptedAttempt,
        payload: &AttemptPayload,
    ) -> Result<Option<Workspace>, ApiError> {
        let a = &rec.attempt_id;
        let permit = loop {
            if self.stopping() || self.stop_requested(a) {
                return Ok(None);
            }
            if let Some(p) = Permits::try_acquire(&self.prepares) {
                break p;
            }
            self.pause(self.cfg.poll_ms);
        };
        let slot = Arc::new(Mutex::new(Slot::Waiting));
        let (host, shards, theirs) = (self.host.clone(), self.shards.clone(), slot.clone());
        let (rec2, payload2) = (rec.clone(), payload.clone());
        std::thread::Builder::new()
            .name("prepare".into())
            .spawn(move || {
                let _permit = permit;
                let made = catch_unwind(AssertUnwindSafe(|| host.prepare(&rec2, &payload2)))
                    .unwrap_or_else(|_| Err(internal("workspace", "the preparation panicked")));
                let mut slot = lock(&theirs);
                if matches!(*slot, Slot::Abandoned) {
                    drop(slot);
                    if let Ok(ws) = made {
                        let _one = lock(shard(&shards, &rec2.attempt_id));
                        let _ = host.cleanup(&rec2.attempt_id, Some(&ws));
                    }
                } else {
                    *slot = Slot::Done(made);
                }
            })
            .map_err(|e| internal("prepare thread", e))?;
        let end = platform::monotonic_ms().saturating_add(self.cfg.prepare_timeout_ms);
        loop {
            {
                let mut slot = lock(&slot);
                match std::mem::replace(&mut *slot, Slot::Abandoned) {
                    Slot::Done(made) => return made.map(Some),
                    _ if platform::monotonic_ms() >= end => {
                        return Err(internal(
                            "workspace",
                            format!(
                                "the preparation took longer than {} ms",
                                self.cfg.prepare_timeout_ms
                            ),
                        ))
                    }
                    _ if self.stopping() => return Ok(None),
                    waiting => *slot = waiting,
                }
            }
            std::thread::sleep(Duration::from_millis(self.cfg.poll_ms.clamp(1, 100)));
        }
    }

    /// `Accepted | Preparing -> Ready`: the workspace, the secret files, the
    /// supervisor's spec. No command is started here.
    fn prepare(&self, rec: &AcceptedAttempt) -> Result<(), ApiError> {
        let attempt = &rec.attempt_id;
        let payload: AttemptPayload =
            serde_json::from_value(rec.spec.clone()).map_err(|e| internal("stored payload", e))?;
        if rec.state == AttemptState::Accepted {
            self.store
                .transition(attempt, AttemptState::Preparing, now())
                .map_err(api)?;
        }
        let dir = self.attempt_dir(attempt);
        private_dir(&dir).map_err(|e| internal("attempt directory", e))?;
        // An agent that died after making the workspace left its record.
        let ws = match self.workspace_of(attempt) {
            Some(ws) if ws.root.is_dir() => ws,
            _ => match self.host_prepare(rec, &payload)? {
                Some(ws) => ws,
                None => return Ok(()),
            },
        };
        let ws_json = serde_json::to_vec(&ws).map_err(|e| internal("workspace record", e))?;
        write_atomic(&self.workspace_path(attempt), &ws_json)
            .map_err(|e| internal("workspace record", e))?;

        // The prepared setup steps are copied in (or waited for); the
        // commands that are such a step are left out.
        let Some(skip) = self.setup_wait(rec, &payload, &ws)? else {
            return Ok(());
        };
        let commands = crate::setup::policy::prune_commands(&payload.commands, &skip);
        // Every command was a prepared setup step (an install-only task with
        // its template there): the supervisor settles it as succeeded
        // without a command. The journal says which steps were not run.
        let prepared_only = commands.is_empty();
        if prepared_only {
            self.note_once(
                attempt,
                "agent_setup_satisfied",
                json!({ "steps": skip.iter().collect::<Vec<_>>() }),
            );
        }

        // The secret bundle arrives by its own request once the workspace is
        // there; the commands do not start before the set is complete.
        //
        // From the look that finds the set complete until the attempt is
        // `Ready`, the attempt's shard is held, the lock a
        // `secret_bundle_put` places its files under: a put is either done
        // before that look, and the mask below is made of all it placed, or
        // it finds the attempt `Ready` and places nothing. No file is read
        // for the mask while it is being written, and none arrives after.
        let mut waited = 0;
        let _placed = loop {
            let one = lock(shard(&self.shards, attempt));
            if self.host.secrets_ready(rec)? {
                break one;
            }
            drop(one);
            if self.stopping() || self.stop_requested(attempt) {
                return Ok(());
            }
            if waited >= self.cfg.secret_wait_ms {
                return Err(ApiError::blocked(
                    crate::error::BlockedReason::SecretUnavailable,
                    "the secret bundle did not arrive",
                ));
            }
            self.pause(self.cfg.poll_ms);
            waited += self.cfg.poll_ms;
        };

        // The values of the secret files in place, read now that the set is
        // complete: everything the commands print goes through a masker made
        // of them before it is stored. They are read from the files, so an
        // agent that restarted since the bundle arrived masks the same.
        let placed = self.host.placed_secrets(rec)?;
        let (secret, secret_files) = (placed.values, placed.files);
        if !secret.skipped_keys.is_empty() {
            // Key names are not secret, but the count says enough.
            self.note_once(
                attempt,
                "agent_mask_skipped",
                json!({ "too_short_to_mask": secret.skipped_keys.len() }),
            );
        }
        let mask = policy::mask_strings(secret.values).map_err(|e| {
            ApiError::blocked(
                crate::error::BlockedReason::SecretUnavailable,
                format!("{e}: its output could not be masked, nothing is started"),
            )
        })?;

        // The declared artifacts, checked again against the secret files of
        // the source (which is here now: the workspace was made from it). An
        // acceptance that came before the source could not look.
        let artifacts = if payload.artifacts.is_empty() {
            None
        } else {
            let secrets = self.host.secret_paths(&payload)?.ok_or_else(|| {
                ApiError::new(
                    ErrorCode::NotFound,
                    format!(
                        "source {} is not in this worker's store: its secret files cannot be told from the declared artifacts",
                        payload.source_id
                    ),
                )
            })?;
            check_artifacts_not_secret(&payload, &secrets)?;
            Some(ArtifactsSpec {
                // Declared paths are relative to the workspace root, like
                // the paths of the secret files they are compared with.
                workspace: ws.root.clone(),
                source_id: payload.source_id.clone(),
                plan_digest: payload.plan_digest.clone(),
                profile_fingerprint: self.cfg.profile_fingerprint.clone(),
                max_bytes: DEFAULT_ARTIFACT_BYTES,
                files: payload
                    .artifacts
                    .iter()
                    .map(|d| ArtifactFile {
                        name: d.name.clone(),
                        source: PathBuf::from(&d.path),
                        if_missing: d.if_missing,
                    })
                    .collect(),
                // What the names above may not lead to, by any spelling or
                // link: the files the mask was read from.
                secret_files,
            })
        };

        // In the order they run: after what they depend on.
        let order = policy::execution_order(&commands).map_err(invalid)?;
        let mut nodes = Vec::with_capacity(commands.len());
        for c in order.into_iter().map(|i| &commands[i]) {
            let rel = Path::new(&c.cwd);
            check_cwd_relative(rel)?;
            nodes.push(NodeSpec {
                node_id: c.node_id.clone(),
                argv: c.argv.clone(),
                cwd: ws.cwd.join(rel),
                env: c.env.clone(),
                weight: None,
                timeout_ms: c.timeout_ms,
            });
        }
        let spec = SuperviseSpec {
            state_dir: self.cfg.state_dir.clone(),
            worker_id: self.cfg.worker_id.clone(),
            attempt_id: attempt.clone(),
            traffic_state_dir: self.cfg.traffic_state_dir.clone(),
            traffic_config: self.cfg.traffic_config.clone(),
            nodes,
            lease_ms: payload.lease_ms,
            term_grace_ms: self.cfg.term_grace_ms,
            kill_wait_ms: self.cfg.kill_wait_ms,
            poll_ms: self.cfg.poll_ms,
            command_timeout_ms: payload.command_timeout_ms,
            resource_wait_timeout_ms: payload.resource_wait_timeout_ms,
            // What is left of both is written when the supervisor is started.
            deadline_ms: payload.deadline_ms,
            lease_seed: None,
            inhibit_sleep: self.cfg.inhibit_sleep,
            // The spool and this file share `<state_dir>/attempts/<attempt>`.
            spool_root: Some(self.cfg.attempts_root()),
            log_limit_bytes: self.cfg.log_limit_bytes,
            workspace_warnings: ws.warnings.clone(),
            // This file therefore holds secrets: it is written 0600 and
            // removed with the workspace when the attempt has ended.
            mask: MaskValues(mask),
            artifacts,
            prepared_only,
        };
        spec.validate().map_err(|e| invalid(e.to_string()))?;
        let bytes = serde_json::to_vec(&spec).map_err(|e| internal("supervisor spec", e))?;
        write_atomic(&self.spec_path(attempt), &bytes)
            .map_err(|e| internal("supervisor spec", e))?;
        if self.stop_requested(attempt) {
            // The loop settles it as cancelled; nothing is started.
            return Ok(());
        }
        self.store
            .transition(attempt, AttemptState::Ready, now())
            .map_err(api)?;
        Ok(())
    }

    /// Wait for the setup of the attempt (`Host::setup_poll`), as long as
    /// the preparation may take. `None`: the agent is stopping or the
    /// attempt is being stopped. An attempt that waited too long, or whose
    /// template could not be had, runs every step itself.
    fn setup_wait(
        &self,
        rec: &AcceptedAttempt,
        payload: &AttemptPayload,
        ws: &Workspace,
    ) -> Result<Option<std::collections::BTreeSet<String>>, ApiError> {
        let attempt = &rec.attempt_id;
        let begun = platform::monotonic_ms();
        loop {
            match self.host.setup_poll(rec, payload, ws)? {
                SetupPoll::Done { skip, note } => {
                    if let Some(why) = note {
                        self.note_once(
                            attempt,
                            "agent_setup_unavailable",
                            json!({ "reason": why }),
                        );
                    }
                    return Ok(Some(skip));
                }
                SetupPoll::Pending => {
                    if self.stopping() || self.stop_requested(attempt) {
                        self.host.setup_release(attempt);
                        return Ok(None);
                    }
                    if platform::monotonic_ms().saturating_sub(begun) >= self.cfg.prepare_timeout_ms
                    {
                        self.host.setup_release(attempt);
                        let why = format!(
                            "the setup was not ready within {} ms",
                            self.cfg.prepare_timeout_ms
                        );
                        self.note_once(
                            attempt,
                            "agent_setup_unavailable",
                            json!({ "reason": why }),
                        );
                        return Ok(Some(Default::default()));
                    }
                    self.pause(self.cfg.poll_ms);
                }
            }
        }
    }

    fn stop_requested(&self, attempt: &AttemptId) -> bool {
        match self.store.get(attempt) {
            Ok(Some(r)) => r.cancel_requested_at_ms.is_some() || r.retired,
            // Unknown is not a licence to start.
            _ => true,
        }
    }

    /// After the end: secret files and the workspace go at once (spec 22.3
    /// does not wait for the logs to be collected); logs, artifacts and the
    /// result stay until GC. One cleanup of an attempt at a time: the tick
    /// and the attempt's watcher both come here.
    fn clean(&self, attempt: &AttemptId) {
        let _one = lock(shard(&self.shards, attempt));
        let pending = self.attempt_dir(attempt).join("cleanup-pending");
        // Persist retry evidence before removing the last secret-bearing spec,
        // including when there is no workspace record. A restart sees it too.
        if self.attempt_dir(attempt).exists() && !pending.exists() {
            if let Err(e) = write_atomic(&pending, b"") {
                self.note_once(
                    attempt,
                    "agent_cleanup_failed",
                    json!({ "message": e.to_string() }),
                );
                return;
            }
        }
        // First, and whatever becomes of the rest: the supervisor's spec
        // carries the secret values to mask. The attempt is terminal, so no
        // supervisor is started from it again (one that still runs has read
        // it already).
        let _ = fs::remove_file(self.spec_path(attempt));
        // And what a write of it that was cut short left next to it.
        if let Ok(rd) = fs::read_dir(self.attempt_dir(attempt)) {
            for ent in rd.flatten() {
                let is_file = ent.file_type().is_ok_and(|t| t.is_file());
                if is_file && ent.file_name().to_string_lossy().starts_with(".tmp") {
                    let _ = fs::remove_file(ent.path());
                }
            }
        }
        let ws = self.workspace_of(attempt);
        let marker = self.workspace_path(attempt);
        if ws.is_none() && marker.exists() {
            // An unreadable record: keep it, a person has to look.
            return;
        }
        match self.host.cleanup(attempt, ws.as_ref()) {
            Ok(()) => {
                let _ = fs::remove_file(pending);
                let _ = fs::remove_file(marker);
                let _ = fs::remove_dir(self.attempt_dir(attempt));
                lock(&self.clocks).remove(attempt.as_str());
                let mine = format!("{attempt}/");
                lock(&self.noted).retain(|k, _| !k.starts_with(&mine));
            }
            // Tried again at every tick; said once per distinct failure.
            Err(e) => {
                self.note_once(
                    attempt,
                    "agent_cleanup_failed",
                    json!({ "code": e.code, "message": e.message }),
                );
            }
        }
    }

    /// Examine at most `limit` directory entries for terminal cleanup, without
    /// deleting retained results, logs or artifacts. Enumeration resumes on the
    /// next call; after EOF a later call starts a new scan, retrying failures.
    /// Every entry counts, including invalid names and nonterminal attempts.
    pub fn cleanup_terminal(&self, limit: usize) -> usize {
        if limit == 0 {
            return 0;
        }
        let mut scan = lock(&self.cleanup_scan);
        if scan.is_none() {
            *scan = fs::read_dir(self.cfg.attempts_root()).ok();
        }
        let mut examined = 0;
        while examined < limit {
            let Some(entry) = scan.as_mut().and_then(Iterator::next) else {
                *scan = None;
                break;
            };
            examined += 1;
            let Ok(entry) = entry else { continue };
            let Ok(a) = AttemptId::parse(&entry.file_name().to_string_lossy()) else {
                continue;
            };
            if !self.workspace_path(&a).exists()
                && !self.spec_path(&a).exists()
                && !self.attempt_dir(&a).join("cleanup-pending").exists()
            {
                continue;
            }
            if matches!(self.store.get(&a), Ok(Some(r)) if r.state == AttemptState::Terminal) {
                self.clean(&a);
            }
        }
        examined
    }

    /// Clean every ended attempt that still has cleanup files.
    pub fn clean_ended(&self) {
        let Ok(rd) = fs::read_dir(self.cfg.attempts_root()) else {
            return;
        };
        for ent in rd.flatten() {
            let Ok(a) = AttemptId::parse(&ent.file_name().to_string_lossy()) else {
                continue;
            };
            if !self.workspace_path(&a).exists()
                && !self.spec_path(&a).exists()
                && !self.attempt_dir(&a).join("cleanup-pending").exists()
            {
                continue;
            }
            if matches!(self.store.get(&a), Ok(Some(r)) if r.state == AttemptState::Terminal) {
                self.clean(&a);
            }
        }
    }

    /// One GC pass: drop what the store says may go (never an attempt that
    /// runs or waits for `ack_collected`), then delete the files. What could
    /// not be deleted, and queued names that are not this store's, are
    /// written to the journal (once each while they stay the same).
    pub fn gc(&self) -> Result<GcReport, ApiError> {
        let report = self
            .store
            .gc(now(), self.cfg.retention_ms, self.cfg.ack_wait_ms)
            .map_err(api)?;
        let drained = drain_pending_deletes(&self.store, &self.cfg.attempts_root())
            .map_err(|e| internal("gc", e))?;
        for (locator, error) in &drained.failed {
            self.note_global(
                "agent_gc_delete_failed",
                locator,
                json!({ "locator": locator, "error": error }),
            );
        }
        for locator in &drained.foreign {
            self.note_global(
                "agent_gc_foreign_locator",
                locator,
                json!({ "locator": locator }),
            );
        }
        Ok(report)
    }

    /// Supervise durable attempts and retry bounded terminal cleanup.
    /// Retention GC is deliberately separate.
    pub fn supervise(self: &Arc<Self>) {
        if self.ticking.swap(true, Ordering::SeqCst) {
            return;
        }
        let _pass = Ticking(&self.ticking);
        self.supervise_attempts();
        self.cleanup_terminal(32);
    }

    fn supervise_attempts(self: &Arc<Self>) {
        self.reap();
        if let Ok(list) = self.store.list_nonterminal() {
            for rec in list {
                self.kick(&rec.attempt_id);
            }
        }
    }

    /// Periodic work: watch attempts nobody watches, clean, collect garbage.
    /// One pass at a time; a call during a pass returns at once.
    pub fn tick(self: &Arc<Self>) {
        if self.ticking.swap(true, Ordering::SeqCst) {
            return;
        }
        let _pass = Ticking(&self.ticking);
        self.supervise_attempts();
        self.clean_ended();
        if let Err(e) = self.gc() {
            self.note_global(
                "agent_gc_failed",
                "",
                json!({ "code": e.code, "message": e.message }),
            );
        }
    }

    /// Whether a diagnostic about `subject` is new (not the one written last).
    fn is_news(&self, subject: String, payload: &Value) -> bool {
        let text = payload.to_string();
        let mut noted = lock(&self.noted);
        if noted.get(&subject) == Some(&text) {
            return false;
        }
        if noted.len() >= 4096 {
            noted.clear();
        }
        noted.insert(subject, text);
        true
    }

    /// A diagnostic event of an attempt, written once while it repeats.
    fn note_once(&self, attempt: &AttemptId, kind: &str, payload: Value) {
        if !self.is_news(format!("{attempt}/{kind}"), &payload) {
            return;
        }
        if let Ok(Some(rec)) = self.store.get(attempt) {
            note(&self.store, &rec, kind, payload);
        }
    }

    /// A diagnostic event of no attempt, written once while it repeats.
    fn note_global(&self, kind: &str, subject: &str, payload: Value) {
        if self.is_news(format!("/{kind}/{subject}"), &payload) {
            let _ = self.store.append_event(None, None, kind, &payload, now());
        }
    }

    /// A retire of an attempt that may have run and that nobody can watch or
    /// recover: once every launch of it is known to be from another boot of
    /// the machine, nothing of it runs any more and it is fixed as
    /// interrupted. On the same boot it stays unknown.
    fn settle_lost(&self, attempt: &AttemptId) -> Result<(), ApiError> {
        let Some(rec) = self.store.get(attempt).map_err(api)? else {
            return Ok(());
        };
        if rec.state == AttemptState::Terminal {
            return Ok(());
        }
        let lost = lock(&self.gave_up).contains(attempt.as_str())
            || recovery_of(&self.cfg, &self.store, &rec, false)? == Recovery::Unresolved;
        if !lost {
            return Ok(());
        }
        let launches = self.store.launches(attempt).map_err(api)?;
        let boots: Vec<Option<&str>> = launches
            .iter()
            .map(|(_, _, ident)| ident.as_ref().map(|i| i.boot_id.as_str()))
            .collect();
        let rebooted = policy::launched_on_another_boot(&boots, self.cfg.boot_id.as_str())
            || !self
                .store
                .events_of(attempt, "agent_boot_changed")
                .map_err(api)?
                .is_empty();
        if !rebooted {
            return Ok(());
        }
        // A look by a watcher holds the supervisor lock for an instant; a
        // living supervisor holds it for good.
        for _ in 0..3 {
            let t = termination(None, None);
            if settle(
                &self.cfg,
                &self.store,
                attempt,
                t,
                Settle::Interrupted,
                true,
            )? {
                note(&self.store, &rec, "agent_settled_lost", json!({}));
                lock(&self.gave_up).remove(attempt.as_str());
                self.clean(attempt);
                break;
            }
            self.pause(self.cfg.poll_ms);
        }
        Ok(())
    }

    // ---------------------------------------------------------- operations

    /// Answer one request. Mutations are committed before this returns.
    pub fn handle(
        self: &Arc<Self>,
        session: Session,
        request: Request,
    ) -> Result<Response, ApiError> {
        request
            .ensure_current_schema()
            .map_err(|e| ApiError::new(ErrorCode::ProtocolIncompatible, e.to_string()))?;
        match request {
            Request::Hello(_) => Err(invalid("hello is the first request of a connection")),
            Request::Inspect(_) => self.inspect().map(Response::Inspect),
            Request::SubmitAttempt(r) => self.submit(r).map(Response::SubmitAttempt),
            Request::GetAttempt(r) => {
                let rec = self.known(&r.attempt_id)?;
                self.status(&rec).map(Response::GetAttempt)
            }
            Request::ListAttempts(r) => self.list(r).map(Response::ListAttempts),
            Request::RenewLease(r) => self.renew(r).map(Response::RenewLease),
            Request::CancelAttempt(r) => self.cancel(r).map(Response::CancelAttempt),
            Request::RetireAttempt(r) => self.retire(r).map(Response::RetireAttempt),
            Request::WatchEvents(r) => self.watch(r).map(Response::WatchEvents),
            Request::ReadLog(r) => self.read_log(r).map(Response::ReadLog),
            Request::GetResult(r) => self.result(r).map(Response::GetResult),
            Request::AckCollected(r) => self.ack(session, r).map(Response::AckCollected),
            Request::SecretBundlePut(r) => self.secrets(r).map(Response::SecretBundlePut),
            Request::BlobPut(r) => blobs::put(&self.store, self.host.as_ref(), &self.cfg.blobs, &r)
                .map(Response::BlobPut),
            Request::BlobVerify(r) => {
                blobs::verify(self.host.as_ref(), &self.cfg.blobs, &r).map(Response::BlobVerify)
            }
            Request::BlobGet(r) => {
                self.known(&r.attempt_id)?;
                blobs::get(&self.store, &self.cfg.attempts_root(), &r).map(Response::BlobGet)
            }
            Request::Forecast(r) => self.forecast(r).map(Response::Forecast),
            Request::EnvReport(_) | Request::EnvSync(_) => Err(ApiError::new(
                ErrorCode::NotImplemented,
                "this operation is not implemented by the worker agent yet",
            )),
        }
    }

    /// The record, or why there is none: `RETIRED` for a tombstone without an
    /// attempt, else `NOT_FOUND`.
    fn known(&self, attempt: &AttemptId) -> Result<AcceptedAttempt, ApiError> {
        if let Some(rec) = self.store.get(attempt).map_err(api)? {
            return Ok(rec);
        }
        if self.store.tombstone(attempt).map_err(api)?.is_some() {
            return Err(ApiError::new(
                ErrorCode::Retired,
                format!("attempt {attempt} has a tombstone and was never accepted"),
            ));
        }
        Err(ApiError::new(
            ErrorCode::NotFound,
            format!("attempt {attempt} is unknown"),
        ))
    }

    fn generation(rec: &AcceptedAttempt, got: Generation) -> Result<(), ApiError> {
        if rec.generation == got {
            Ok(())
        } else {
            Err(ApiError::new(
                ErrorCode::Conflict,
                format!(
                    "attempt {} is generation {}, not {}",
                    rec.attempt_id, rec.generation.0, got.0
                ),
            ))
        }
    }

    fn submit(
        self: &Arc<Self>,
        r: SubmitAttemptRequest,
    ) -> Result<SubmitAttemptResponse, ApiError> {
        // The claimed hash is the sender's word: recompute it.
        if !r.hash_matches().map_err(|e| invalid(e.to_string()))? {
            return Err(invalid("payload_hash does not match the payload"));
        }
        // One acceptance at a time, so the cap and the insert are one step.
        let one = lock(&self.submitting);
        // A resend of an accepted attempt is answered with what is there
        // (spec 11, idempotency): its payload was checked when it was
        // accepted, and it adds no load. The store still compares the hash.
        let fresh = self.store.get(&r.attempt_id).map_err(api)?.is_none()
            && self.store.tombstone(&r.attempt_id).map_err(api)?.is_none();
        if fresh {
            policy::check_payload(&r.payload, self.cfg.poll_ms).map_err(invalid)?;
            for c in &r.payload.commands {
                check_cwd_relative(Path::new(&c.cwd))?;
            }
            // A setup the builder would refuse is refused here, as bad
            // input, not later as a preparation that failed.
            if let Some(setup) = &r.payload.setup {
                crate::setup::policy::validate(setup).map_err(invalid)?;
            }
            // Against the secret files of its source, when that source is
            // already here. When it is not, the same check runs before the
            // supervisor's spec is written ([`Self::prepare`]): a
            // declaration that names a secret file never starts a command.
            if !r.payload.artifacts.is_empty() {
                // Only a source that is not here yet (`None`) waits for the
                // later check. One that is here with another manifest than
                // the payload names, or a store that cannot be read, is the
                // answer: nothing is accepted.
                if let Some(secrets) = self.host.secret_paths(&r.payload)? {
                    check_artifacts_not_secret(&r.payload, &secrets)?;
                }
            }
            let live = self.store.count_nonterminal().map_err(api)?;
            if policy::at_live_cap(live, self.cfg.max_live_attempts) {
                return Err(ApiError::new(
                    ErrorCode::Overloaded,
                    format!("{live} attempts are not terminal on this worker; ask again later"),
                ));
            }
        }
        let spec = serde_json::to_value(&r.payload).map_err(|e| internal("payload", e))?;
        let outcome = self
            .store
            .submit(
                NewAcceptance {
                    attempt_id: r.attempt_id.clone(),
                    run_id: r.payload.run_id.clone(),
                    task_id: r.payload.task_id.clone(),
                    generation: r.payload.generation,
                    payload_hash: r.payload_hash,
                    coordinator_epoch: r.coordinator_epoch,
                    spec,
                },
                now(),
            )
            .map_err(api)?;
        // The acceptance is committed; only now may anything start.
        drop(one);
        let (rec, duplicate) = match outcome {
            SubmitOutcome::Accepted(rec) => {
                let at = platform::monotonic_ms();
                lock(&self.clocks).insert(
                    rec.attempt_id.to_string(),
                    Clock {
                        accepted_ms: at,
                        granted_ms: at,
                    },
                );
                (rec, false)
            }
            SubmitOutcome::Existing(rec) => (rec, true),
        };
        if rec.state != AttemptState::Terminal {
            self.kick(&rec.attempt_id);
        }
        Ok(SubmitAttemptResponse {
            schema_version: SchemaVersion::CURRENT,
            attempt_id: rec.attempt_id,
            generation: rec.generation,
            state: rec.state,
            receipts: rec.receipts,
            duplicate,
        })
    }

    fn status(&self, rec: &AcceptedAttempt) -> Result<AttemptStatus, ApiError> {
        let a = &rec.attempt_id;
        let out = self.store.log_position(a, "stdout").map_err(api)?;
        let err = self.store.log_position(a, "stderr").map_err(api)?;
        let dropped = out.dropped_bytes.saturating_add(err.dropped_bytes);
        let gave_up = lock(&self.gave_up).contains(a.as_str());
        let recovery = recovery_of(&self.cfg, &self.store, rec, false)?;
        Ok(AttemptStatus {
            schema_version: SchemaVersion::CURRENT,
            attempt_id: a.clone(),
            run_id: rec.run_id.clone(),
            task_id: rec.task_id.clone(),
            generation: rec.generation,
            worker_boot_id: self.cfg.boot_id.clone(),
            agent_epoch: self.epoch,
            state: rec.state,
            receipts: rec.receipts,
            spawned_at_ms: rec.receipts.at_ms(Receipt::Running),
            observation: policy::observation(rec.state, gave_up, recovery),
            termination: rec.termination,
            nodes: Vec::new(),
            steps: Vec::new(),
            retired: rec.retired,
            log_truncated: dropped > 0,
            log_dropped_bytes: dropped,
        })
    }

    fn list(&self, r: ListAttemptsRequest) -> Result<ListAttemptsResponse, ApiError> {
        // One more than fits: an answer that would be cut is an error, the
        // caller narrows `states` instead of trusting a part of the list.
        let cap = self.cfg.max_list_attempts;
        let recs = self
            .store
            .list_attempts(
                r.states.as_deref(),
                r.include_terminal,
                cap.saturating_add(1),
            )
            .map_err(api)?;
        if !policy::fits(recs.len(), cap) {
            return Err(invalid(format!(
                "more than {cap} attempts match; narrow `states`"
            )));
        }
        let mut attempts = Vec::with_capacity(recs.len());
        for rec in recs {
            attempts.push(self.status(&rec)?);
        }
        Ok(ListAttemptsResponse {
            schema_version: SchemaVersion::CURRENT,
            attempts,
        })
    }

    fn renew(&self, r: RenewLeaseRequest) -> Result<RenewLeaseResponse, ApiError> {
        let rec = self.known(&r.attempt_id)?;
        Self::generation(&rec, r.generation)?;
        // The new key and the time of its grant change as one step for a
        // start that measures the lease (`lease_step`).
        let renewing = lock(&self.lease_step);
        let outcome = self
            .store
            .renew_lease(&r.attempt_id, r.key(), now())
            .map_err(api)?;
        let extended = outcome == LeaseOutcome::Extended;
        if extended {
            self.at(ProbePoint::LeaseRenewed, &r.attempt_id);
            if let Some(c) = lock(&self.clocks).get_mut(r.attempt_id.as_str()) {
                c.granted_ms = platform::monotonic_ms();
            }
        }
        drop(renewing);
        let lease_ms = serde_json::from_value::<AttemptPayload>(rec.spec)
            .map(|p| p.lease_ms)
            .unwrap_or(0);
        Ok(RenewLeaseResponse {
            schema_version: SchemaVersion::CURRENT,
            attempt_id: r.attempt_id,
            lease_seq: r.lease_seq,
            extended,
            // The supervisor counts the lease on its own monotonic clock; a
            // renewal that extended starts a full period there.
            lease_remaining_ms: if extended { lease_ms } else { 0 },
            state: rec.state,
        })
    }

    fn cancel(
        self: &Arc<Self>,
        r: CancelAttemptRequest,
    ) -> Result<CancelAttemptResponse, ApiError> {
        if let Some(rec) = self.store.get(&r.attempt_id).map_err(api)? {
            Self::generation(&rec, r.generation)?;
        }
        // Persisted first; the supervisor (or the watcher, before a
        // supervisor exists) acts on it.
        let outcome = self
            .store
            .cancel(&r.attempt_id, r.coordinator_epoch, now())
            .map_err(api)?;
        let (state, tombstone) = match outcome {
            CancelOutcome::Requested(rec) | CancelOutcome::AlreadyRequested(rec) => {
                if rec.state != AttemptState::Terminal {
                    self.kick(&rec.attempt_id);
                }
                (Some(rec.state), false)
            }
            CancelOutcome::Tombstoned => (None, true),
        };
        Ok(CancelAttemptResponse {
            schema_version: SchemaVersion::CURRENT,
            attempt_id: r.attempt_id,
            phase: policy::cancel_phase(state),
            state,
            tombstone,
        })
    }

    fn retire(
        self: &Arc<Self>,
        r: RetireAttemptRequest,
    ) -> Result<RetireAttemptResponse, ApiError> {
        let a = &r.attempt_id;
        let before = self.store.get(a).map_err(api)?;
        if let Some(rec) = &before {
            Self::generation(rec, r.generation)?;
        }
        let was_terminal = before
            .as_ref()
            .is_some_and(|b| b.state == AttemptState::Terminal);
        // The tombstone is durable before anything else: a submit that
        // arrives late is refused from here on.
        let known = self
            .store
            .retire(a, r.coordinator_epoch, now())
            .map_err(api)?
            .is_some();
        let mut now_terminal = was_terminal;
        if known && !was_terminal {
            // Before the watcher is woken: it takes the supervisor lock for
            // a moment at each look.
            self.settle_lost(a)?;
            self.kick(a);
            let mut waited = 0;
            loop {
                now_terminal = matches!(
                    self.store.get(a).map_err(api)?,
                    Some(rec) if rec.state == AttemptState::Terminal
                );
                if now_terminal || waited >= self.cfg.retire_wait_ms || self.stopping() {
                    break;
                }
                let step = self.cfg.poll_ms.clamp(1, 50);
                std::thread::sleep(Duration::from_millis(step));
                waited += step;
            }
        }
        let had_launches = known && !self.store.launches(a).map_err(api)?.is_empty();
        Ok(RetireAttemptResponse {
            schema_version: SchemaVersion::CURRENT,
            attempt_id: r.attempt_id,
            tombstone: true,
            status: policy::retire_status(known, was_terminal, now_terminal, had_launches),
        })
    }

    fn watch(&self, r: WatchEventsRequest) -> Result<WatchEventsResponse, ApiError> {
        // The wire `seq` is per attempt, so one cursor follows one attempt.
        let Some(a) = r.attempt_id else {
            return Err(invalid("watch_events needs an attempt_id"));
        };
        self.known(&a)?;
        let scan = self.cfg.max_event_scan;
        let journal = self
            .store
            .events_of_attempt(&a, scan.saturating_add(1))
            .map_err(api)?;
        if !policy::fits(journal.len(), scan) {
            // Numbering what was read would renumber the events behind it.
            return Err(internal(
                "watch_events",
                format!("the journal of attempt {a} has more than {scan} entries"),
            ));
        }
        let wire: Vec<_> = journal
            .iter()
            .map(|e| policy::wire_event(&e.kind, &e.payload))
            .collect();
        let is_wire: Vec<bool> = wire.iter().map(Option::is_some).collect();
        let mut next_seq = r.after_seq;
        let mut events = Vec::new();
        for (seq, i) in policy::select_events(&is_wire, r.after_seq, r.limit) {
            let w = wire[i].expect("selected entries are wire events");
            next_seq = seq;
            events.push(AttemptEvent {
                seq,
                attempt_id: a.clone(),
                kind: w.kind,
                state: w.state,
                receipt: w.receipt,
                node_id: None,
                step: None,
            });
        }
        Ok(WatchEventsResponse {
            schema_version: SchemaVersion::CURRENT,
            events,
            next_seq,
        })
    }

    fn read_log(&self, r: ReadLogRequest) -> Result<ReadLogResponse, ApiError> {
        let rec = self.known(&r.attempt_id)?;
        let reader = match SpoolReader::open(&self.cfg.attempts_root(), &r.attempt_id) {
            Ok(reader) => reader,
            // Nothing was written yet. Only an ended attempt has no more to
            // come.
            Err(SpoolError::NotFound(_)) => {
                return Ok(ReadLogResponse {
                    schema_version: SchemaVersion::CURRENT,
                    offset: r.offset,
                    data: Bytes(Vec::new()),
                    next_offset: r.offset,
                    eof: rec.state == AttemptState::Terminal,
                    log_truncated: false,
                    dropped_bytes: 0,
                })
            }
            Err(e) => return Err(internal("log spool", e)),
        };
        let chunk = reader
            .read(r.stream, r.offset, r.max_bytes)
            .map_err(|e| internal("log spool", e))?;
        Ok(ReadLogResponse {
            schema_version: SchemaVersion::CURRENT,
            offset: chunk.offset,
            data: Bytes(chunk.data),
            next_offset: chunk.next_offset,
            eof: chunk.eof,
            log_truncated: chunk.log_truncated,
            dropped_bytes: chunk.dropped_bytes,
        })
    }

    /// The ended attempt's result, its digest, and whether a result is stored.
    /// The stored result (written
    /// by the supervisor) is the truth when there is one; an attempt that
    /// ended without one (settled by the agent or by recovery) has its
    /// termination and nothing else.
    fn ended(&self, attempt: &AttemptId, generation: Generation) -> Result<Ended, ApiError> {
        let rec = self.known(attempt)?;
        Self::generation(&rec, generation)?;
        let Some(t) = rec.termination else {
            return Err(ApiError::new(
                ErrorCode::Conflict,
                format!("attempt {attempt} has not ended"),
            ));
        };
        let stored = self.store.get_result(attempt).map_err(api)?;
        let artifacts: Vec<ArtifactRecord> = match &stored {
            Some(v) => serde_json::from_value(v["artifacts"].clone())
                .map_err(|e| internal("stored artifacts", e))?,
            None => Vec::new(),
        };
        // Unreadable is never "all were stored".
        let not_stored = match &stored {
            Some(v) => result_not_stored(v).ok_or_else(|| {
                internal("stored result", "its unstored artifacts cannot be read")
            })?,
            None => Vec::new(),
        };
        let has_result = stored.is_some();
        let value = stored.unwrap_or_else(|| json!({ "termination": t, "artifacts": [] }));
        let digest = payload_hash(&value).map_err(|e| internal("result digest", e))?;
        Ok((t, artifacts, not_stored, digest, has_result))
    }

    fn result(&self, r: GetResultRequest) -> Result<GetResultResponse, ApiError> {
        let (termination, artifacts, artifacts_not_stored, result_digest, _) =
            self.ended(&r.attempt_id, r.generation)?;
        Ok(GetResultResponse {
            schema_version: SchemaVersion::CURRENT,
            attempt_id: r.attempt_id,
            generation: r.generation,
            termination,
            nodes: Vec::new(),
            artifacts,
            artifacts_not_stored,
            result_digest,
        })
    }

    fn ack(
        &self,
        session: Session,
        r: AckCollectedRequest,
    ) -> Result<AckCollectedResponse, ApiError> {
        let (_, _, _, digest, has_result) = self.ended(&r.attempt_id, r.generation)?;
        if digest != r.result_digest {
            // The parent stored something else than what is here: the
            // protection stays.
            return Err(ApiError::new(
                ErrorCode::Conflict,
                "result_digest is not the digest of this attempt's result",
            ));
        }
        // An attempt without a result can still get one while a supervisor
        // of it lives, and a result that arrives after the acknowledgement
        // is not protected. The supervisor holds its lock for as long as it
        // may store one: the acknowledgement is recorded under that lock,
        // or not now.
        let _no_supervisor = if has_result {
            None
        } else {
            match SupervisorLock::try_acquire(&self.cfg.state_dir, &r.attempt_id) {
                Ok(Some(lock)) => Some(lock),
                Ok(None) => {
                    return Err(ApiError::new(
                        ErrorCode::Overloaded,
                        "a supervisor of the attempt may still store its result; acknowledge again later",
                    ))
                }
                Err(e) => return Err(internal("supervisor lock", e)),
            }
        };
        // A result that is stored between that read and this write is
        // another result than the acknowledged one: refused like a digest
        // that does not match.
        self.store
            .ack_collected_as_read(&r.attempt_id, session.coordinator_epoch, now(), has_result)
            .map_err(api)?;
        Ok(AckCollectedResponse {
            schema_version: SchemaVersion::CURRENT,
            attempt_id: r.attempt_id,
            released: true,
        })
    }

    fn secrets(&self, r: SecretBundlePutRequest) -> Result<SecretBundlePutResponse, ApiError> {
        // The cleanup of an ended attempt takes the same lock and runs only
        // after the end is in the database: read under the lock, the state
        // either still allows the placement (the cleanup then erases what is
        // placed) or it shows the end and nothing is placed.
        let _one = lock(shard(&self.shards, &r.attempt_id));
        let rec = self.known(&r.attempt_id)?;
        Self::generation(&rec, r.generation)?;
        let ws = self.workspace_of(&r.attempt_id);
        let stop = rec.retired || rec.cancel_requested_at_ms.is_some();
        let ws = match (policy::secret_put(rec.state, stop, ws.is_some()), ws) {
            (policy::SecretPut::Place, Some(ws)) => ws,
            (policy::SecretPut::Stopping, _) => {
                return Err(ApiError::new(
                    ErrorCode::Retired,
                    "the attempt is being stopped",
                ))
            }
            (policy::SecretPut::TooLate, _) => {
                return Err(ApiError::new(
                    ErrorCode::Conflict,
                    format!(
                        "attempt {} is {:?}: secrets go in before the start",
                        rec.attempt_id, rec.state
                    ),
                ))
            }
            // Not `OVERLOADED` (the worker is not refusing load) and not
            // `CONFLICT` (that one is final): what the bundle goes into does
            // not exist yet. The sender sends it again.
            (policy::SecretPut::NotReady, _) | (policy::SecretPut::Place, None) => {
                return Err(ApiError::new(
                    ErrorCode::NotFound,
                    "the workspace is not prepared yet; send the bundle again",
                ))
            }
        };
        let placed = self.host.place_secrets(&rec, &ws, &r.files)?;
        Ok(SecretBundlePutResponse {
            schema_version: SchemaVersion::CURRENT,
            attempt_id: r.attempt_id,
            verified: true,
            placed,
        })
    }

    /// Read-only: nothing is written, whatever the request.
    fn forecast(&self, r: ForecastRequest) -> Result<ForecastResponse, ApiError> {
        let attempts = self.store.list_nonterminal().map_err(api)?;
        let max_epoch = self.store.max_coordinator_epoch().map_err(api)?;
        let ident = crate::forecast::plan::Ident {
            worker_id: self.cfg.worker_id.clone(),
            boot_id: self.cfg.boot_id.clone(),
            agent_epoch: self.epoch,
            forecast_ttl_ms: self.cfg.forecast_ttl_ms,
        };
        crate::forecast::answer(
            &self.cfg.traffic_state_dir,
            self.cfg.traffic_config.as_deref(),
            &ident,
            &attempts,
            max_epoch,
            &r,
        )
    }

    fn inspect(&self) -> Result<InspectResponse, ApiError> {
        let live = self.store.list_nonterminal().map_err(api)?;
        let (active, pending) = policy::load_counts(live.iter().map(|r| r.state));
        let policy_bytes = self
            .cfg
            .traffic_config
            .as_ref()
            .and_then(|p| fs::read(p).ok())
            .unwrap_or_default();
        let disk = rustix::fs::statvfs(&self.cfg.state_dir)
            .map(|s| s.f_bavail.saturating_mul(s.f_frsize))
            .unwrap_or(0);
        Ok(InspectResponse {
            schema_version: SchemaVersion::CURRENT,
            worker_id: self.cfg.worker_id.clone(),
            worker_boot_id: self.cfg.boot_id.clone(),
            agent_epoch: self.epoch,
            snapshot_digest: Digest::of_bytes(
                format!("{}/{active}/{pending}", self.epoch.0).as_bytes(),
            ),
            policy_digest: Digest::of_bytes(&policy_bytes),
            active_task: active,
            prefetch_pending: pending,
            max_active_task: self.cfg.max_active_task,
            prefetch_per_worker: self.cfg.prefetch_per_worker,
            disk_free_bytes: disk,
            draining: false,
        })
    }
}

fn acquire_lock(cfg: &AgentConfig) -> Result<ProcessLock, ApiError> {
    let mut waited = 0;
    loop {
        match ProcessLock::acquire(&cfg.state_dir, "agent.lock") {
            Err(StorageError::Locked(_)) if waited < cfg.lock_wait_ms => {
                std::thread::sleep(Duration::from_millis(50));
                waited += 50;
            }
            other => return other.map_err(api),
        }
    }
}

fn spec_path(cfg: &AgentConfig, attempt: &AttemptId) -> PathBuf {
    cfg.attempts_root()
        .join(attempt.as_str())
        .join("supervise.json")
}

fn recovery_of(
    cfg: &AgentConfig,
    store: &WorkerStore,
    rec: &AcceptedAttempt,
    boot_changed: bool,
) -> Result<Recovery, ApiError> {
    let facts = AttemptFacts {
        state: rec.state,
        has_launches: !store.launches(&rec.attempt_id).map_err(api)?.is_empty(),
        stop_requested: rec.cancel_requested_at_ms.is_some() || rec.retired,
        has_spec: spec_path(cfg, &rec.attempt_id).is_file(),
    };
    Ok(policy::decide_recovery(boot_changed, facts))
}

fn how_termination(how: Settle) -> Termination {
    match how {
        Settle::Cancelled => termination(Some(StopReason::Cancel), None),
        Settle::Interrupted => termination(None, None),
    }
}

/// Fix the result of an attempt whose command was never started, holding the
/// supervisor lock so no supervisor can be starting it meanwhile. `false`
/// when a supervisor lives (it owns the attempt) or a launch appeared.
/// `lost` skips the look at the launches: the caller has shown that whatever
/// was launched cannot run any more (another boot).
fn settle(
    cfg: &AgentConfig,
    store: &WorkerStore,
    attempt: &AttemptId,
    t: Termination,
    how: Settle,
    lost: bool,
) -> Result<bool, ApiError> {
    let lock = SupervisorLock::try_acquire(&cfg.state_dir, attempt)
        .map_err(|e| internal("supervisor lock", e))?;
    let Some(_lock) = lock else {
        return Ok(false);
    };
    if !lost && !store.launches(attempt).map_err(api)?.is_empty() {
        return Ok(false);
    }
    if how == Settle::Cancelled {
        // Already there, or past it: both are fine.
        let _ = store.transition(attempt, AttemptState::Cancelling, now());
    }
    crate::supervisor::run::finalize(store, attempt, t)
        .map_err(|e| internal("settling the attempt", e))?;
    Ok(true)
}

fn note(store: &WorkerStore, rec: &AcceptedAttempt, kind: &str, payload: Value) {
    let _ = store.append_event(
        Some(&rec.run_id),
        Some(&rec.attempt_id),
        kind,
        &payload,
        now(),
    );
}
