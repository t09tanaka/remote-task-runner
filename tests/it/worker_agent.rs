//! The worker agent against real supervisors (separate processes) and real
//! connections: A18 (a resent attempt starts once), A19 (lost ACK, retire
//! tombstone), A23 (an agent restart adopts the supervisor), A24 (a changed
//! boot ID, simulated), GC protection until `ack_collected`, and the order of
//! the startup steps.

use crate::common::{alive, read_pid};
use crate::proto_common;

use proto_common::client_cfg;
use remote_task_runner::error::{ApiError, ConflictReason, ErrorCode};
use remote_task_runner::ids::*;
use remote_task_runner::protocol::transport::duplex_pair;
use remote_task_runner::protocol::{connect, CallError, Client};
use remote_task_runner::secrets::envfile::secret_values;
use remote_task_runner::secrets::place::{FileIdentity, Placed};
use remote_task_runner::state::{AttemptState, Outcome, Receipt};
use remote_task_runner::storage::{AcceptedAttempt, BlobKind, NewBlobRef, WorkerStore};
use remote_task_runner::wire::*;
use remote_task_runner::worker::policy::{Step, STARTUP_ORDER};
use remote_task_runner::worker::serve::serve_connection;
use remote_task_runner::worker::{Agent, AgentConfig, Host, ProbePoint, Session};
use remote_task_runner::workspace::{Workspace, WorkspaceKind};
use std::collections::BTreeMap;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{mpsc, Arc, Mutex};
use std::time::{Duration, Instant};
use tempfile::TempDir;
use tokio::sync::Semaphore;

const T: Duration = Duration::from_secs(20);

/// The machine, reduced to plain directories, with a record of every call.
struct DirHost {
    base: PathBuf,
    calls: Mutex<Vec<String>>,
    secrets_ready: AtomicBool,
    fail_core_dumps: AtomicBool,
    /// `prepare` does not return while this is set.
    block_prepare: AtomicBool,
    preparing_now: AtomicUsize,
    preparing_max: AtomicUsize,
    /// `secrets_ready` panics (it runs on the attempt's watcher thread).
    panic_secrets: AtomicBool,
    /// A secret value that is not text is among the ones to mask.
    binary_secret: AtomicBool,
    fail_cleanup: AtomicBool,
    slow_cleanup_ms: AtomicU64,
    /// The attempts being cleaned right now, and whether one ever was twice.
    cleaning: Mutex<Vec<String>>,
    overlap: AtomicBool,
    /// Called by `placed_secrets` once it has read the values.
    on_mask: Mutex<Option<MaskHook>>,
    /// The secret files of every source; `None`: the source is not here.
    secret_paths: Mutex<Option<Vec<String>>>,
    /// The source is here and its secret files cannot be told.
    secret_paths_error: Mutex<Option<ApiError>>,
    /// `setup_poll` answers `Pending` while this is set; then it is done,
    /// with the setup steps of `setup_skip` in the workspace.
    setup_pending: AtomicBool,
    setup_skip: Mutex<std::collections::BTreeSet<String>>,
}

type MaskHook = Box<dyn Fn(&AttemptId) + Send + Sync>;

impl DirHost {
    fn log(&self, what: impl Into<String>) {
        self.calls.lock().unwrap().push(what.into());
    }
    fn calls(&self) -> Vec<String> {
        self.calls.lock().unwrap().clone()
    }
    fn count(&self, what: &str) -> usize {
        self.calls().iter().filter(|c| c.as_str() == what).count()
    }
    fn ws_dir(&self, a: &AttemptId) -> PathBuf {
        self.base.join("ws").join(a.as_str())
    }
}

impl Host for DirHost {
    fn disable_core_dumps(&self) -> Result<(), ApiError> {
        self.log("core_dumps");
        if self.fail_core_dumps.load(Ordering::SeqCst) {
            return Err(ApiError::new(ErrorCode::Internal, "no rlimit"));
        }
        Ok(())
    }
    fn sweep_store_copies(&self) -> Result<(), ApiError> {
        self.log("store_copies");
        Ok(())
    }
    fn sweep_vault(&self) -> Result<(), ApiError> {
        self.log("vault");
        Ok(())
    }
    fn reconcile_secrets(&self, erase: &dyn Fn(&AttemptId) -> bool) -> Result<(), ApiError> {
        // An attempt nobody knows is erased.
        assert!(erase(&AttemptId::generate()));
        self.log("secrets");
        Ok(())
    }
    fn sweep_workspaces(&self, has_record: &dyn Fn(&AttemptId) -> bool) -> Result<(), ApiError> {
        assert!(!has_record(&AttemptId::generate()));
        self.log("workspaces");
        Ok(())
    }
    fn prepare(&self, a: &AcceptedAttempt, _: &AttemptPayload) -> Result<Workspace, ApiError> {
        self.log(format!("prepare {}", a.attempt_id));
        let now = self.preparing_now.fetch_add(1, Ordering::SeqCst) + 1;
        self.preparing_max.fetch_max(now, Ordering::SeqCst);
        while self.block_prepare.load(Ordering::SeqCst) {
            std::thread::sleep(Duration::from_millis(5));
        }
        self.preparing_now.fetch_sub(1, Ordering::SeqCst);
        let dir = self.ws_dir(&a.attempt_id);
        std::fs::create_dir_all(&dir).unwrap();
        Ok(Workspace {
            root: dir.clone(),
            cwd: dir,
            kind: WorkspaceKind::Directory,
            index_reproduced: true,
            warnings: Vec::new(),
        })
    }
    fn setup_poll(
        &self,
        a: &AcceptedAttempt,
        _: &AttemptPayload,
        _: &Workspace,
    ) -> Result<remote_task_runner::setup::builder::SetupPoll, ApiError> {
        use remote_task_runner::setup::builder::SetupPoll;
        self.log(format!("setup_poll {}", a.attempt_id));
        if self.setup_pending.load(Ordering::SeqCst) {
            return Ok(SetupPoll::Pending);
        }
        Ok(SetupPoll::Done {
            skip: self.setup_skip.lock().unwrap().clone(),
            note: Some("a note".into()),
        })
    }
    fn setup_release(&self, a: &AttemptId) {
        self.log(format!("setup_release {a}"));
    }
    fn secret_paths(&self, _: &AttemptPayload) -> Result<Option<Vec<String>>, ApiError> {
        if let Some(e) = self.secret_paths_error.lock().unwrap().clone() {
            return Err(e);
        }
        Ok(self.secret_paths.lock().unwrap().clone())
    }
    fn place_secrets(
        &self,
        a: &AcceptedAttempt,
        ws: &Workspace,
        files: &[SecretFile],
    ) -> Result<u32, ApiError> {
        self.log(format!("place {}", a.attempt_id));
        for f in files {
            std::fs::write(ws.root.join(&f.path), &f.content.0).unwrap();
        }
        self.secrets_ready.store(true, Ordering::SeqCst);
        Ok(files.len() as u32)
    }
    fn secrets_ready(&self, _: &AcceptedAttempt) -> Result<bool, ApiError> {
        if self.panic_secrets.load(Ordering::SeqCst) {
            panic!("the host broke (a test of the watcher's panic handling)");
        }
        Ok(self.secrets_ready.load(Ordering::SeqCst))
    }
    /// From the file in the workspace, like the real host: nothing is kept
    /// in memory between the bundle's arrival and the start.
    fn placed_secrets(&self, a: &AcceptedAttempt) -> Result<Placed, ApiError> {
        let env = self.ws_dir(&a.attempt_id).join(".env");
        let files = std::fs::symlink_metadata(&env)
            .iter()
            .map(FileIdentity::of)
            .collect();
        let content = std::fs::read(&env).unwrap_or_default();
        let mut values = secret_values(&content);
        if self.binary_secret.load(Ordering::SeqCst) {
            values
                .values
                .push(vec![0xff, 0xfe, 0xfd, 0xfc, 0xfb, 0xfa, 0xf9, 0xf8]);
        }
        if let Some(hook) = self.on_mask.lock().unwrap().as_ref() {
            hook(&a.attempt_id);
        }
        Ok(Placed { values, files })
    }
    fn cleanup(&self, a: &AttemptId, ws: Option<&Workspace>) -> Result<(), ApiError> {
        self.log(format!("cleanup {a}"));
        {
            let mut cleaning = self.cleaning.lock().unwrap();
            if cleaning.iter().any(|c| c == a.as_str()) {
                self.overlap.store(true, Ordering::SeqCst);
            }
            cleaning.push(a.to_string());
        }
        std::thread::sleep(Duration::from_millis(
            self.slow_cleanup_ms.load(Ordering::SeqCst),
        ));
        self.cleaning.lock().unwrap().retain(|c| c != a.as_str());
        if self.fail_cleanup.load(Ordering::SeqCst) {
            return Err(ApiError::new(ErrorCode::Internal, "the workspace is busy"));
        }
        if let Some(ws) = ws {
            let _ = std::fs::remove_dir_all(&ws.root);
        }
        Ok(())
    }
}

pub(crate) struct Fx {
    dir: TempDir,
    host: Arc<DirHost>,
    pub(crate) cluster: ClusterId,
    agents: Mutex<Vec<Arc<Agent>>>,
}

pub(crate) fn fx() -> Fx {
    // Not under the system temp directory: a supervisor a failed test leaves
    // behind must not write into a directory a test runner clears.
    let dir = tempfile::Builder::new()
        .prefix("rtr-agent-")
        .tempdir_in(env!("CARGO_TARGET_TMPDIR"))
        .unwrap();
    let host = Arc::new(DirHost {
        base: dir.path().to_path_buf(),
        calls: Mutex::default(),
        secrets_ready: AtomicBool::new(true),
        fail_core_dumps: AtomicBool::new(false),
        block_prepare: AtomicBool::new(false),
        preparing_now: AtomicUsize::new(0),
        preparing_max: AtomicUsize::new(0),
        panic_secrets: AtomicBool::new(false),
        binary_secret: AtomicBool::new(false),
        fail_cleanup: AtomicBool::new(false),
        slow_cleanup_ms: AtomicU64::new(0),
        cleaning: Mutex::default(),
        overlap: AtomicBool::new(false),
        on_mask: Mutex::default(),
        secret_paths: Mutex::new(Some(Vec::new())),
        secret_paths_error: Mutex::new(None),
        setup_pending: AtomicBool::new(false),
        setup_skip: Mutex::default(),
    });
    Fx {
        dir,
        host,
        cluster: ClusterId::generate(),
        agents: Mutex::default(),
    }
}

impl Drop for Fx {
    fn drop(&mut self) {
        for a in self.agents.lock().unwrap().iter() {
            a.shutdown();
        }
        // Whatever still runs for this fixture: supervisors (their spec path
        // is under the directory) and the commands they started.
        let state = self.state();
        if let Ok(store) = WorkerStore::open_read_only(&state) {
            for rec in store.list_attempts(None, true, 500).unwrap_or_default() {
                for (_, _, ident) in store.launches(&rec.attempt_id).unwrap_or_default() {
                    // By the recorded identity, never by the number alone.
                    if let Some(i) = ident {
                        crate::common::kill_recorded_group(i.pid, &i.start_identity);
                    }
                }
            }
        }
        // What a search by the fixture's directory finds is killed only
        // while it still is the process with that directory in its command
        // line: a number alone is not signalled.
        let pattern = state.to_string_lossy().into_owned();
        for pid in pgrep(&pattern) {
            crate::common::kill_if_command(pid, &pattern);
        }
    }
}

/// What only the command line of the attempt's supervisor contains.
fn supervisor_pattern(a: &AttemptId) -> String {
    format!("{}/supervise.json", a.as_str())
}

fn pgrep(pattern: &str) -> Vec<i32> {
    let out = std::process::Command::new("pgrep")
        .args(["-f", pattern])
        .output()
        .unwrap();
    String::from_utf8_lossy(&out.stdout)
        .lines()
        .filter_map(|l| l.trim().parse().ok())
        .collect()
}

pub(crate) fn wait_for(what: &str, f: impl Fn() -> bool) {
    let end = Instant::now() + T;
    while !f() {
        assert!(Instant::now() < end, "timed out waiting for {what}");
        std::thread::sleep(Duration::from_millis(20));
    }
}

impl Fx {
    fn state(&self) -> PathBuf {
        self.dir.path().join("state")
    }
    fn path(&self, name: &str) -> PathBuf {
        self.dir.path().join(name)
    }
    fn cfg(&self, boot: &str) -> AgentConfig {
        let mut cfg = AgentConfig::new(
            self.state(),
            WorkerId::parse("w-test").unwrap(),
            WorkerBootId::parse(boot).unwrap(),
            PathBuf::from(env!("CARGO_BIN_EXE_remote-task-runner")),
        );
        cfg.poll_ms = 25;
        cfg.inhibit_sleep = false;
        cfg.retry_ms = 200;
        cfg.retire_wait_ms = 15_000;
        cfg.secret_wait_ms = 15_000;
        cfg.term_grace_ms = 2_000;
        cfg.kill_wait_ms = 2_000;
        cfg
    }
    fn start_with(&self, cfg: AgentConfig) -> Arc<Agent> {
        let agent = Agent::start(cfg, self.host.clone()).unwrap();
        self.agents.lock().unwrap().push(agent.clone());
        agent
    }
    pub(crate) fn start(&self, boot: &str) -> Arc<Agent> {
        self.start_with(self.cfg(boot))
    }
    /// A new connection to the agent: its own stream and its own `hello`.
    async fn connect(&self, agent: &Arc<Agent>) -> Client {
        let (a, b) = duplex_pair(64 * 1024);
        let permits = Arc::new(Semaphore::new(64));
        tokio::spawn(serve_connection(
            agent.clone(),
            b,
            self.cluster.clone(),
            permits,
        ));
        connect(a, client_cfg(&self.cluster)).await.unwrap().client
    }
    fn store(&self) -> WorkerStore {
        WorkerStore::open_read_only(&self.state()).unwrap()
    }
    fn rec(&self, a: &AttemptId) -> AcceptedAttempt {
        self.store().get(a).unwrap().unwrap()
    }
    fn wait_terminal(&self, a: &AttemptId) -> Outcome {
        let store = self.store();
        let end = Instant::now() + T;
        while store.get(a).unwrap().unwrap().state != AttemptState::Terminal {
            // Say where it stopped, not only that it did: the journal of the
            // attempt is what tells a stuck recovery from a slow one.
            assert!(
                Instant::now() < end,
                "timed out waiting for the attempt to end: {:?}; events: {:?}",
                store.get(a).unwrap().unwrap().state,
                store
                    .events_of_attempt(a, 200)
                    .unwrap()
                    .iter()
                    .map(|e| format!("{}@{} {}", e.kind, e.at_ms, e.payload))
                    .collect::<Vec<_>>()
            );
            std::thread::sleep(Duration::from_millis(20));
        }
        self.rec(a).termination.unwrap().outcome()
    }
    fn supervisors(&self, a: &AttemptId) -> Vec<i32> {
        pgrep(&supervisor_pattern(a))
    }
    /// The supervisors whose spec is under `dir`.
    fn supervisors_under(&self, dir: &Path) -> Vec<i32> {
        pgrep(&dir.to_string_lossy())
    }
    /// SIGKILL the attempt's supervisors: each only while it still is the
    /// process the search found (its identity and command line are read
    /// again right before the signal).
    fn kill_supervisors(&self, a: &AttemptId) {
        let pattern = supervisor_pattern(a);
        for pid in pgrep(&pattern) {
            crate::common::kill_if_command(pid, &pattern);
        }
    }
}

fn payload(script: &str) -> AttemptPayload {
    let d = Digest::of_bytes(b"x");
    let mut env = BTreeMap::new();
    env.insert("PATH".to_string(), "/usr/bin:/bin".to_string());
    AttemptPayload {
        setup: None,
        run_id: RunId::generate(),
        task_id: TaskId::generate(),
        generation: Generation(1),
        source_id: SourceId::generate(),
        plan_digest: d.clone(),
        manifest_digest: d,
        permit_id: None,
        commands: vec![CommandSpec {
            setup_step: None,
            node_id: NodeId::parse("main").unwrap(),
            argv: vec!["/bin/sh".into(), "-c".into(), script.into()],
            cwd: String::new(),
            env,
            depends_on: vec![],
            timeout_ms: None,
        }],
        lease_ms: 60_000,
        command_timeout_ms: None,
        deadline_ms: None,
        resource_wait_timeout_ms: None,
        has_secret_bundle: false,
        artifacts: Vec::new(),
    }
}

fn decl(name: &str, path: &str, if_missing: MissingPolicy) -> ArtifactDecl {
    ArtifactDecl {
        name: name.into(),
        path: path.into(),
        if_missing,
    }
}

fn submit_req(a: &AttemptId, p: &AttemptPayload) -> Request {
    Request::SubmitAttempt(
        SubmitAttemptRequest::new(
            RequestId::generate(),
            CoordinatorEpoch(1),
            a.clone(),
            p.clone(),
        )
        .unwrap(),
    )
}

async fn call(c: &Client, r: Request) -> Result<Response, ApiError> {
    match c.call(r, T).await {
        Ok(r) => Ok(r),
        Err(CallError::Remote(e)) => Err(e),
        Err(e) => panic!("{e:?}"),
    }
}

async fn submit(c: &Client, a: &AttemptId, p: &AttemptPayload) -> SubmitAttemptResponse {
    match call(c, submit_req(a, p)).await.unwrap() {
        Response::SubmitAttempt(r) => r,
        other => panic!("{other:?}"),
    }
}

fn cancel_req(a: &AttemptId) -> Request {
    Request::CancelAttempt(CancelAttemptRequest {
        schema_version: SchemaVersion::CURRENT,
        request_id: RequestId::generate(),
        coordinator_epoch: CoordinatorEpoch(1),
        attempt_id: a.clone(),
        generation: Generation(1),
        reason: None,
    })
}

fn retire_req(a: &AttemptId) -> Request {
    Request::RetireAttempt(RetireAttemptRequest {
        schema_version: SchemaVersion::CURRENT,
        request_id: RequestId::generate(),
        coordinator_epoch: CoordinatorEpoch(1),
        attempt_id: a.clone(),
        generation: Generation(1),
    })
}

fn lines(p: &Path) -> usize {
    std::fs::read_to_string(p)
        .map(|t| t.lines().count())
        .unwrap_or(0)
}

/// A command that records each start and its PID, then stays.
pub(crate) fn stay(f: &Fx) -> (String, PathBuf, PathBuf) {
    let (marker, pidfile) = (f.path("starts"), f.path("pid"));
    let script = format!(
        "echo start >> {}; echo $$ > {}; exec sleep 300",
        marker.display(),
        pidfile.display()
    );
    (script, marker, pidfile)
}

// ---------------------------------------------------------------- A18

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a18_an_attempt_resent_100_times_starts_its_command_once() {
    let f = fx();
    let agent = f.start("boot-a");
    let marker = f.path("starts");
    let p = payload(&format!("echo start >> {}; sleep 0.3", marker.display()));
    let a = AttemptId::generate();
    let mut tasks = Vec::new();
    // Four connections, 25 submits each, all at once.
    for _ in 0..4 {
        let c = f.connect(&agent).await;
        let (a, p) = (a.clone(), p.clone());
        tasks.push(tokio::spawn(async move {
            let mut fresh = 0;
            for _ in 0..25 {
                let r = submit(&c, &a, &p).await;
                assert_eq!(r.attempt_id, a);
                fresh += usize::from(!r.duplicate);
            }
            fresh
        }));
    }
    let mut fresh = 0;
    for t in tasks {
        fresh += t.await.unwrap();
    }
    assert_eq!(fresh, 1, "exactly one submit is the acceptance");
    assert_eq!(f.wait_terminal(&a), Outcome::Succeeded);
    // Resends after the end change nothing either.
    let c = f.connect(&agent).await;
    let r = submit(&c, &a, &p).await;
    assert!(r.duplicate);
    assert_eq!(r.state, AttemptState::Terminal);
    std::thread::sleep(Duration::from_millis(300));
    assert_eq!(lines(&marker), 1, "the command started once");
    assert_eq!(f.store().launches(&a).unwrap().len(), 1);
    assert_eq!(f.host.count(&format!("prepare {a}")), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_same_attempt_with_another_payload_is_a_conflict() {
    let f = fx();
    let agent = f.start("boot-a");
    let c = f.connect(&agent).await;
    f.host.secrets_ready.store(false, Ordering::SeqCst);
    let marker = f.path("starts");
    let p = payload(&format!("echo start >> {}", marker.display()));
    let a = AttemptId::generate();
    assert!(!submit(&c, &a, &p).await.duplicate);
    assert!(submit(&c, &a, &p).await.duplicate);

    // Another payload under the same attempt id, with its own correct hash.
    let mut other = p.clone();
    other.commands[0].argv[2] = "echo other".into();
    let e = call(&c, submit_req(&a, &other)).await.unwrap_err();
    assert_eq!(e.code, ErrorCode::Conflict);
    assert_eq!(e.conflict_reason, Some(ConflictReason::PayloadMismatch));

    // A hash that is not the payload's is refused before anything is stored.
    let fresh = AttemptId::generate();
    let Request::SubmitAttempt(mut bad) = submit_req(&fresh, &p) else {
        unreachable!()
    };
    bad.payload.lease_ms += 1;
    let e = call(&c, Request::SubmitAttempt(bad)).await.unwrap_err();
    assert_eq!(e.code, ErrorCode::InvalidInput);
    assert!(f.store().get(&fresh).unwrap().is_none());

    // A working directory that leaves the workspace is refused too.
    let mut escape = p.clone();
    escape.commands[0].cwd = "../x".into();
    let e = call(&c, submit_req(&AttemptId::generate(), &escape))
        .await
        .unwrap_err();
    assert_eq!(e.code, ErrorCode::InvalidInput);
    // So is a per-command timeout that is none.
    let mut timed = p.clone();
    timed.commands[0].timeout_ms = Some(0);
    let e = call(&c, submit_req(&AttemptId::generate(), &timed))
        .await
        .unwrap_err();
    assert_eq!(e.code, ErrorCode::InvalidInput);

    // The stored attempt is still the first payload.
    let stored: AttemptPayload = serde_json::from_value(f.rec(&a).spec).unwrap();
    assert_eq!(stored, p);
    assert_eq!(lines(&marker), 0);
}

// ---------------------------------------------------------------- A19

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a19_a_submit_that_arrives_after_the_retire_is_refused() {
    let f = fx();
    let agent = f.start("boot-a");
    let c = f.connect(&agent).await;
    let marker = f.path("starts");
    let p = payload(&format!("echo start >> {}", marker.display()));
    let a = AttemptId::generate();

    // The coordinator gave up on a submit it never saw answered and retires
    // the attempt before placing the task elsewhere.
    let Response::RetireAttempt(r) = call(&c, retire_req(&a)).await.unwrap() else {
        panic!()
    };
    assert!(r.tombstone);
    assert_eq!(r.status, RetireStatus::NotStarted);

    // The delayed submit arrives, on another connection, again and again.
    let late = f.connect(&agent).await;
    for _ in 0..3 {
        let e = call(&late, submit_req(&a, &p)).await.unwrap_err();
        assert_eq!(e.code, ErrorCode::Retired);
    }
    // The tombstone survives an agent restart.
    agent.shutdown();
    let agent = f.start("boot-a");
    let c = f.connect(&agent).await;
    let e = call(&c, submit_req(&a, &p)).await.unwrap_err();
    assert_eq!(e.code, ErrorCode::Retired);
    let e = call(
        &c,
        Request::GetAttempt(GetAttemptRequest {
            schema_version: SchemaVersion::CURRENT,
            request_id: RequestId::generate(),
            attempt_id: a.clone(),
        }),
    )
    .await
    .unwrap_err();
    assert_eq!(e.code, ErrorCode::Retired, "a tombstone is not NOT_FOUND");
    std::thread::sleep(Duration::from_millis(200));
    assert_eq!(lines(&marker), 0);
    assert_eq!(f.host.count(&format!("prepare {a}")), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a19_a_lost_ack_is_answered_by_the_resend_and_retire_stops_the_run() {
    let f = fx();
    let agent = f.start("boot-a");
    let (script, marker, pidfile) = stay(&f);
    let p = payload(&script);
    let a = AttemptId::generate();

    // The first submit is accepted, but its connection dies before the answer
    // is read: the coordinator does not know.
    {
        let c = f.connect(&agent).await;
        let _ = tokio::time::timeout(Duration::from_millis(1), c.call(submit_req(&a, &p), T)).await;
    }
    wait_for("the acceptance", || f.store().get(&a).unwrap().is_some());
    // It asks the same worker again and gets the existing attempt.
    let c = f.connect(&agent).await;
    let r = submit(&c, &a, &p).await;
    assert!(r.duplicate);
    wait_for("the command", || read_pid(&pidfile).is_some());
    let pid = read_pid(&pidfile).unwrap();

    // Retire: tombstone first, then the stop is confirmed.
    let Response::RetireAttempt(r) = call(&c, retire_req(&a)).await.unwrap() else {
        panic!()
    };
    assert!(r.tombstone);
    assert_eq!(r.status, RetireStatus::Stopped);
    assert!(!alive(pid), "the command is gone when retire says stopped");
    assert_eq!(f.rec(&a).termination.unwrap().outcome(), Outcome::Cancelled);
    assert!(f.rec(&a).retired);

    // Old submits and a second retire.
    let e = call(&c, submit_req(&a, &p)).await.unwrap_err();
    assert_eq!(e.code, ErrorCode::Retired);
    let Response::RetireAttempt(r) = call(&c, retire_req(&a)).await.unwrap() else {
        panic!()
    };
    assert_eq!(r.status, RetireStatus::AlreadyTerminal);
    assert_eq!(lines(&marker), 1);
}

// ---------------------------------------------------------------- A23

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a23_a_restarted_agent_adopts_the_living_supervisor() {
    let f = fx();
    let agent = f.start("boot-a");
    let c = f.connect(&agent).await;
    let (script, marker, pidfile) = stay(&f);
    let a = AttemptId::generate();
    submit(&c, &a, &payload(&script)).await;
    wait_for("the command", || read_pid(&pidfile).is_some());
    wait_for("the running state", || {
        f.rec(&a).state == AttemptState::Active
    });
    let pid = read_pid(&pidfile).unwrap();
    let sup = f.supervisors(&a);
    assert_eq!(sup.len(), 1);
    let epoch = agent.agent_epoch();

    // The agent goes away; the supervisor and the command do not.
    agent.shutdown();
    drop(c);
    assert!(alive(pid));

    let agent = f.start("boot-a");
    assert!(agent.agent_epoch() > epoch);
    let report = agent.startup_report();
    assert!(!report.boot_changed);
    assert_eq!(report.watched, vec![a.clone()]);
    assert!(report.settled.is_empty());
    // Give the watcher time to do something wrong.
    std::thread::sleep(Duration::from_millis(500));
    assert!(alive(pid), "the same command still runs");
    assert_eq!(f.supervisors(&a), sup, "no second supervisor");
    assert_eq!(lines(&marker), 1, "the command was not started again");
    assert_eq!(f.store().launches(&a).unwrap().len(), 1);
    assert_eq!(f.host.count(&format!("prepare {a}")), 1);
    assert_eq!(f.rec(&a).state, AttemptState::Active);

    // The adopted attempt obeys the new agent.
    let c = f.connect(&agent).await;
    let Response::CancelAttempt(r) = call(&c, cancel_req(&a)).await.unwrap() else {
        panic!()
    };
    assert_eq!(r.phase, CancelPhase::Accepted);
    assert_eq!(f.wait_terminal(&a), Outcome::Cancelled);
    assert!(!alive(pid));
    wait_for("the cleanup", || !f.host.ws_dir(&a).exists());
    let Response::CancelAttempt(r) = call(&c, cancel_req(&a)).await.unwrap() else {
        panic!()
    };
    assert_eq!(r.phase, CancelPhase::Completed);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a23_a_dead_supervisor_is_recovered_and_the_command_is_not_started_again() {
    let f = fx();
    let agent = f.start("boot-a");
    let c = f.connect(&agent).await;
    let (script, marker, pidfile) = stay(&f);
    let a = AttemptId::generate();
    submit(&c, &a, &payload(&script)).await;
    wait_for("the command", || read_pid(&pidfile).is_some());
    wait_for("the running state", || {
        f.rec(&a).state == AttemptState::Active
    });
    let pid = read_pid(&pidfile).unwrap();
    agent.shutdown();
    f.kill_supervisors(&a);
    wait_for("the supervisor to die", || f.supervisors(&a).is_empty());
    assert!(alive(pid), "the command outlives its supervisor");

    // The database still says active. The agent must not run it again: the
    // new supervisor stops what is left and fixes the result.
    assert_eq!(f.rec(&a).state, AttemptState::Active);
    let _agent = f.start("boot-a");
    assert_eq!(f.wait_terminal(&a), Outcome::Interrupted);
    assert!(!alive(pid));
    assert_eq!(lines(&marker), 1);
    assert_eq!(f.store().launches(&a).unwrap().len(), 1);
}

// ---------------------------------------------------------------- A24

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a24_a_changed_boot_id_interrupts_everything_and_a_retry_runs() {
    let f = fx();
    let agent = f.start("boot-a");
    let c = f.connect(&agent).await;
    let (script, marker, pidfile) = stay(&f);
    let running = AttemptId::generate();
    let p_running = payload(&script);
    submit(&c, &running, &p_running).await;
    wait_for("the command", || read_pid(&pidfile).is_some());
    let pid = read_pid(&pidfile).unwrap();
    // Read while the command runs: what the simulated restart kills is this
    // process and its group, never whatever has the number later.
    let pid_start = crate::common::identity_of(pid as u32).expect("the running command");

    // A second attempt is accepted but still waits for its secret bundle.
    f.host.secrets_ready.store(false, Ordering::SeqCst);
    let waiting = AttemptId::generate();
    let waiting_marker = f.path("waiting-starts");
    let p_waiting = payload(&format!("echo start >> {}", waiting_marker.display()));
    submit(&c, &waiting, &p_waiting).await;
    wait_for("the preparation", || {
        f.rec(&waiting).state == AttemptState::Preparing
            && f.host.count(&format!("prepare {waiting}")) == 1
    });

    // The machine restarts: nothing survives. (Simulated: the processes are
    // killed and the next agent is told another boot ID.)
    agent.shutdown();
    f.kill_supervisors(&running);
    crate::common::kill_recorded_group(pid as u32, &pid_start);
    wait_for("the processes to die", || {
        f.supervisors(&running).is_empty() && !alive(pid)
    });
    f.host.secrets_ready.store(true, Ordering::SeqCst);

    let agent = f.start("boot-b");
    let report = agent.startup_report().clone();
    assert!(report.boot_changed);
    // The one that never started is fixed before the agent serves anything.
    assert_eq!(report.settled, vec![waiting.clone()]);
    assert_eq!(
        f.rec(&waiting).termination.unwrap().outcome(),
        Outcome::Interrupted
    );
    assert_eq!(f.wait_terminal(&running), Outcome::Interrupted);
    wait_for("the cleanup", || {
        !f.host.ws_dir(&running).exists() && !f.host.ws_dir(&waiting).exists()
    });
    assert_eq!(lines(&marker), 1);
    assert_eq!(
        lines(&waiting_marker),
        0,
        "it is not started after a reboot"
    );
    assert_eq!(f.host.count(&format!("prepare {waiting}")), 1);

    // After `interrupted` the same task can be tried again as a new attempt.
    let c = f.connect(&agent).await;
    let mut again = p_waiting.clone();
    again.generation = Generation(2);
    let retry = AttemptId::generate();
    assert!(!submit(&c, &retry, &again).await.duplicate);
    assert_eq!(f.wait_terminal(&retry), Outcome::Succeeded);
    assert_eq!(lines(&waiting_marker), 1);

    // The next start under the same boot ID is an ordinary restart.
    agent.shutdown();
    let agent = f.start("boot-b");
    assert!(!agent.startup_report().boot_changed);
}

// ------------------------------------------------------- cancel, secrets

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_cancel_before_the_start_means_the_command_never_starts() {
    let f = fx();
    let agent = f.start("boot-a");
    let c = f.connect(&agent).await;
    f.host.secrets_ready.store(false, Ordering::SeqCst);
    let marker = f.path("starts");
    let p = payload(&format!("echo start >> {}", marker.display()));
    let a = AttemptId::generate();
    submit(&c, &a, &p).await;
    wait_for("the workspace", || f.host.ws_dir(&a).exists());
    let Response::CancelAttempt(r) = call(&c, cancel_req(&a)).await.unwrap() else {
        panic!()
    };
    assert_eq!((r.phase, r.tombstone), (CancelPhase::Accepted, false));
    assert_eq!(f.wait_terminal(&a), Outcome::Cancelled);
    wait_for("the cleanup", || !f.host.ws_dir(&a).exists());
    assert!(f.supervisors(&a).is_empty());
    assert_eq!(lines(&marker), 0);
    assert!(f.store().launches(&a).unwrap().is_empty());

    // A cancel for an attempt that has not arrived keeps a tombstone.
    let unknown = AttemptId::generate();
    let Response::CancelAttempt(r) = call(&c, cancel_req(&unknown)).await.unwrap() else {
        panic!()
    };
    assert!(r.tombstone);
    assert_eq!(r.state, None);
    let e = call(&c, submit_req(&unknown, &p)).await.unwrap_err();
    assert_eq!(e.code, ErrorCode::Retired);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_command_waits_for_its_secret_bundle() {
    let f = fx();
    let agent = f.start("boot-a");
    let c = f.connect(&agent).await;
    f.host.secrets_ready.store(false, Ordering::SeqCst);
    let mut p = payload("cat .env > out; test \"$(cat out)\" = token");
    p.has_secret_bundle = true;
    let a = AttemptId::generate();
    submit(&c, &a, &p).await;
    wait_for("the workspace record", || {
        f.state()
            .join("attempts")
            .join(a.as_str())
            .join("workspace.json")
            .exists()
    });
    std::thread::sleep(Duration::from_millis(200));
    assert_eq!(f.rec(&a).state, AttemptState::Preparing);
    let put = Request::SecretBundlePut(SecretBundlePutRequest {
        schema_version: SchemaVersion::CURRENT,
        request_id: RequestId::generate(),
        coordinator_epoch: CoordinatorEpoch(1),
        attempt_id: a.clone(),
        generation: Generation(1),
        files: vec![SecretFile {
            path: ".env".into(),
            hmac_digest: HmacDigest::parse(&format!("hmac-sha256:{}", "a".repeat(64))).unwrap(),
            content: Bytes(b"token".to_vec()),
        }],
    });
    let Response::SecretBundlePut(r) = call(&c, put.clone()).await.unwrap() else {
        panic!()
    };
    assert_eq!(r.placed, 1);
    assert_eq!(f.wait_terminal(&a), Outcome::Succeeded);
    // Once it started, nothing more is placed.
    let e = call(&c, put).await.unwrap_err();
    assert_eq!(e.code, ErrorCode::Conflict);
}

// ------------------------------------- receipts, masking, order, timeouts

async fn watched(c: &Client, a: &AttemptId) -> Vec<AttemptEvent> {
    let Response::WatchEvents(w) = call(
        c,
        Request::WatchEvents(WatchEventsRequest {
            schema_version: SchemaVersion::CURRENT,
            request_id: RequestId::generate(),
            attempt_id: Some(a.clone()),
            after_seq: 0,
            limit: 500,
        }),
    )
    .await
    .unwrap() else {
        panic!()
    };
    w.events
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn watch_events_shows_the_receipts_of_the_command_in_the_order_they_were_reached() {
    let f = fx();
    let agent = f.start("boot-a");
    let c = f.connect(&agent).await;
    let a = AttemptId::generate();
    submit(&c, &a, &payload("sleep 0.5")).await;
    assert_eq!(f.wait_terminal(&a), Outcome::Succeeded);
    let ev = watched(&c, &a).await;
    let receipts: Vec<Receipt> = ev.iter().filter_map(|e| e.receipt).collect();
    assert_eq!(
        receipts,
        [
            Receipt::WorkerAccepted,
            Receipt::CommandRegistered,
            Receipt::Running,
            Receipt::Terminal
        ],
        "{ev:?}"
    );
    for r in [Receipt::CommandRegistered, Receipt::Running] {
        let e = ev.iter().find(|e| e.receipt == Some(r)).unwrap();
        assert_eq!((e.kind, e.state), (EventKind::ReceiptReached, None));
    }
    assert!(ev.iter().all(|e| e.kind != EventKind::LeaseExpired));
    // The same receipts, with the same times, as the status shows.
    let rec = f.rec(&a);
    assert!(rec.receipts.reached(Receipt::CommandRegistered));
    assert!(rec.receipts.reached(Receipt::Running));
}

/// Every regular file below `dir` that holds `needle`.
fn files_holding(dir: &Path, needle: &[u8]) -> Vec<PathBuf> {
    let mut found = Vec::new();
    let mut stack = vec![dir.to_path_buf()];
    while let Some(d) = stack.pop() {
        // The directory is alive: one that went meanwhile holds nothing.
        let Ok(entries) = std::fs::read_dir(&d) else {
            continue;
        };
        for e in entries.flatten() {
            let p = e.path();
            let Ok(meta) = std::fs::symlink_metadata(&p) else {
                continue;
            };
            if meta.is_dir() {
                stack.push(p);
            } else if meta.is_file() {
                let name = e.file_name();
                let bytes = if matches!(
                    name.to_str(),
                    Some("worker.db" | "worker.db-wal" | "worker.db-shm")
                ) {
                    // A raw close in this process would release SQLite's POSIX
                    // locks. A separate process preserves those locks and scans
                    // all physical bytes, including WAL and deleted-row pages.
                    let output = std::process::Command::new("/bin/cat")
                        .arg(&p)
                        .output()
                        .unwrap();
                    if output.status.success() {
                        output.stdout
                    } else if !p.exists() {
                        Vec::new() // SQLite can retire a WAL/SHM between entries.
                    } else {
                        panic!("could not inspect SQLite file {}", p.display());
                    }
                } else {
                    std::fs::read(&p).unwrap_or_default()
                };
                if bytes.windows(needle.len()).any(|w| w == needle) {
                    found.push(p);
                }
            }
        }
    }
    found
}

async fn read_all(c: &Client, a: &AttemptId, stream: LogStream) -> Vec<u8> {
    let Response::ReadLog(log) = call(
        c,
        Request::ReadLog(ReadLogRequest {
            schema_version: SchemaVersion::CURRENT,
            request_id: RequestId::generate(),
            attempt_id: a.clone(),
            node_id: NodeId::parse("main").unwrap(),
            stream,
            offset: 0,
            max_bytes: 1 << 20,
        }),
    )
    .await
    .unwrap() else {
        panic!()
    };
    log.data.0
}

#[test]
fn secret_scan_uses_an_isolated_reader_including_live_wal_events() {
    let dir = tempfile::tempdir().unwrap();
    let worker = WorkerId::parse("snapshot-worker").unwrap();
    let store = WorkerStore::open(dir.path(), &worker).unwrap();
    // Put the schema in the main DB first. The following single event commit
    // then stays below SQLite's automatic WAL checkpoint threshold.
    let checkpoint = rusqlite::Connection::open(dir.path().join("worker.db")).unwrap();
    checkpoint
        .execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")
        .unwrap();
    drop(checkpoint);
    let attempt = AttemptId::generate();
    let marker = "synthetic-snapshot-marker-123456789";
    store
        .append_event(
            None,
            Some(&attempt),
            "snapshot-check",
            &serde_json::json!({"marker": marker}),
            1,
        )
        .unwrap();
    assert!(
        std::fs::metadata(dir.path().join("worker.db-wal"))
            .unwrap()
            .len()
            > 32
    );
    assert!(
        files_holding(dir.path(), marker.as_bytes()).contains(&dir.path().join("worker.db-wal"))
    );
    let readonly = WorkerStore::open_read_only(dir.path()).unwrap();
    assert_eq!(
        readonly
            .events_of(&attempt, "snapshot-check")
            .unwrap()
            .len(),
        1
    );
    drop(readonly);
    let connection = rusqlite::Connection::open(dir.path().join("worker.db")).unwrap();
    let deleted_marker = "synthetic-deleted-scan-marker-987654321";
    connection
        .execute_batch("CREATE TABLE physical_scan_probe(value TEXT)")
        .unwrap();
    connection
        .execute(
            "INSERT INTO physical_scan_probe VALUES (?1)",
            [deleted_marker],
        )
        .unwrap();
    connection
        .execute("DELETE FROM physical_scan_probe", [])
        .unwrap();
    assert_eq!(
        connection
            .query_row("SELECT count(*) FROM physical_scan_probe", [], |r| r
                .get::<_, i64>(0))
            .unwrap(),
        0
    );
    drop(connection);
    // Logical deletion must not hide bytes still present in physical WAL pages.
    assert!(files_holding(dir.path(), deleted_marker.as_bytes())
        .contains(&dir.path().join("worker.db-wal")));
    assert_eq!(
        WorkerStore::open_read_only(dir.path())
            .unwrap()
            .events_of(&attempt, "snapshot-check")
            .unwrap()
            .len(),
        1
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_secret_of_the_bundle_never_reaches_the_spool_the_journal_or_a_file_left_behind() {
    const SECRET: &str = "s3cr3t-Value-0123456789";
    let f = fx();
    let agent = f.start("boot-a");
    let c = f.connect(&agent).await;
    f.host.secrets_ready.store(false, Ordering::SeqCst);
    let go = f.path("go");
    // Printed whole, on both streams, and split over two writes.
    let script = format!(
        "cat .env; cat .env >&2; printf 'half=%s' s3cr3t-Value-; sleep 0.2; echo 0123456789; \
         while [ ! -e {} ]; do sleep 0.05; done; echo end-of-output",
        go.display()
    );
    let mut p = payload(&script);
    p.has_secret_bundle = true;
    let a = AttemptId::generate();
    submit(&c, &a, &p).await;
    wait_for("the workspace record", || workspace_record(&f, &a).exists());
    let put = Request::SecretBundlePut(SecretBundlePutRequest {
        schema_version: SchemaVersion::CURRENT,
        request_id: RequestId::generate(),
        coordinator_epoch: CoordinatorEpoch(1),
        attempt_id: a.clone(),
        generation: Generation(1),
        files: vec![SecretFile {
            path: ".env".into(),
            hmac_digest: HmacDigest::parse(&format!("hmac-sha256:{}", "a".repeat(64))).unwrap(),
            content: Bytes(format!("API_TOKEN={SECRET}\nPORT=80\n").into_bytes()),
        }],
    });
    call(&c, put).await.unwrap();
    wait_for("the running state", || {
        f.rec(&a).state == AttemptState::Active
    });
    // While it runs, the one file of the state directory that holds the
    // value is the supervisor's spec, readable by this user alone.
    let spec = f
        .state()
        .join("attempts")
        .join(a.as_str())
        .join("supervise.json");
    let mode = std::fs::metadata(&spec).unwrap().permissions().mode();
    assert_eq!(mode & 0o777, 0o600);
    wait_for("the output", || {
        let log = f.state().join("attempts").join(a.as_str()).join("logs");
        !files_holding(&log, b"half=").is_empty()
    });
    assert_eq!(
        files_holding(&f.state(), SECRET.as_bytes()).as_slice(),
        std::slice::from_ref(&spec)
    );
    f.host.fail_cleanup.store(true, Ordering::SeqCst);
    std::fs::write(&go, "").unwrap();
    assert_eq!(f.wait_terminal(&a), Outcome::Succeeded);

    wait_for("the failed terminal cleanup", || {
        events(&f, &a, "agent_cleanup_failed") == 1
    });
    assert!(f.host.ws_dir(&a).join(".env").exists());
    assert!(
        !spec.exists(),
        "the masking spec goes even if host cleanup fails"
    );
    f.host.fail_cleanup.store(false, Ordering::SeqCst);
    wait_for("supervise-only terminal cleanup", || {
        agent.supervise();
        !f.host.ws_dir(&a).exists() && !workspace_record(&f, &a).exists()
    });
    assert!(f.rec(&a).acked_at_ms.is_none());
    assert!(f
        .state()
        .join("attempts")
        .join(a.as_str())
        .join("logs")
        .exists());
    let _retained_result = result_of(&c, &a).await;

    let out = String::from_utf8(read_all(&c, &a, LogStream::Stdout).await).unwrap();
    assert!(out.contains("API_TOKEN=***"), "{out}");
    assert!(out.contains("half=***"), "{out}");
    assert!(out.contains("end-of-output"), "{out}");
    // A value too short to mask stays, and is counted once in the journal.
    assert!(out.contains("PORT=80"), "{out}");
    assert_eq!(events(&f, &a, "agent_mask_skipped"), 1);
    assert!(
        !out.contains(SECRET) && !out.contains("0123456789"),
        "{out}"
    );
    // The spec that carried the values goes with the workspace, and then
    // nothing the worker keeps (spool, database, its journal) holds it.
    wait_for("the spec to go", || !spec.exists());
    assert_eq!(
        files_holding(&f.state(), SECRET.as_bytes()),
        Vec::<PathBuf>::new()
    );
    let journal = f.store().events_of_attempt(&a, 1000).unwrap();
    assert!(journal
        .iter()
        .all(|e| !e.payload.to_string().contains(SECRET)));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_agent_restarted_after_the_bundle_arrived_still_masks_it() {
    const SECRET: &str = "another-S3cret-value";
    let f = fx();
    let agent = f.start("boot-a");
    let c = f.connect(&agent).await;
    f.host.secrets_ready.store(false, Ordering::SeqCst);
    let mut p = payload("cat .env");
    p.has_secret_bundle = true;
    let a = AttemptId::generate();
    submit(&c, &a, &p).await;
    wait_for("the workspace record", || workspace_record(&f, &a).exists());
    // The file is in place, but the agent goes before it says "ready".
    std::fs::write(f.host.ws_dir(&a).join(".env"), format!("KEY={SECRET}\n")).unwrap();
    agent.shutdown();
    drop(c);
    assert_eq!(f.rec(&a).state, AttemptState::Preparing);
    f.host.secrets_ready.store(true, Ordering::SeqCst);
    let agent = f.start("boot-a");
    let c = f.connect(&agent).await;
    assert_eq!(f.wait_terminal(&a), Outcome::Succeeded);
    let out = String::from_utf8(read_all(&c, &a, LogStream::Stdout).await).unwrap();
    assert_eq!(out.trim(), "KEY=***");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_secret_file_sent_while_the_mask_is_made_is_refused_and_never_placed() {
    let f = fx();
    let agent = f.start("boot-a");
    let c = f.connect(&agent).await;
    f.host.secrets_ready.store(false, Ordering::SeqCst);
    let mut p = payload("cat .env");
    p.has_secret_bundle = true;
    let a = AttemptId::generate();
    let put = |content: &str| {
        let Request::SecretBundlePut(mut r) = put_req(&a) else {
            panic!()
        };
        r.files[0].content = Bytes(content.as_bytes().to_vec());
        Request::SecretBundlePut(r)
    };
    // The values are read for the mask; before the attempt is `Ready`,
    // another file for the same path arrives (this host would overwrite).
    let late = put("KEY=a-later-S3cret-value\n");
    type Answer = Result<Response, ApiError>;
    let (seen_tx, seen_rx) = mpsc::channel::<(Option<Answer>, mpsc::Receiver<Answer>)>();
    {
        let agent = agent.clone();
        *f.host.on_mask.lock().unwrap() = Some(Box::new(move |_| {
            let (tx, rx) = mpsc::channel();
            let (agent, late) = (agent.clone(), late.clone());
            std::thread::spawn(move || {
                let session = Session {
                    coordinator_epoch: CoordinatorEpoch(1),
                };
                let _ = tx.send(agent.handle(session, late));
            });
            // Long enough for a put that is not kept out to be answered; a
            // put that is kept out is never answered in here.
            let early = rx.recv_timeout(Duration::from_millis(500)).ok();
            let _ = seen_tx.send((early, rx));
        }));
    }
    submit(&c, &a, &p).await;
    wait_for("the workspace record", || workspace_record(&f, &a).exists());
    call(&c, put("KEY=the-first-S3cret-value\n")).await.unwrap();
    let (early, later) = seen_rx.recv_timeout(T).unwrap();
    assert!(
        early.is_none(),
        "answered while the mask was made: {early:?}"
    );
    // Answered once the attempt is `Ready`: too late, for good.
    let e = later.recv_timeout(T).unwrap().unwrap_err();
    assert_eq!(e.code, ErrorCode::Conflict, "{e:?}");
    *f.host.on_mask.lock().unwrap() = None;
    assert_eq!(f.wait_terminal(&a), Outcome::Succeeded);
    assert_eq!(f.host.count(&format!("place {a}")), 1);
    // What the command read is what the mask was made of.
    let out = String::from_utf8(read_all(&c, &a, LogStream::Stdout).await).unwrap();
    assert_eq!(out.trim(), "KEY=***");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_secret_that_cannot_be_masked_keeps_the_command_from_starting() {
    let f = fx();
    let agent = f.start("boot-a");
    let c = f.connect(&agent).await;
    f.host.binary_secret.store(true, Ordering::SeqCst);
    let marker = f.path("ran");
    let a = AttemptId::generate();
    submit(&c, &a, &payload(&format!("touch {}", marker.display()))).await;
    assert_eq!(f.wait_terminal(&a), Outcome::Failed);
    assert!(!marker.exists());
    assert!(f.store().launches(&a).unwrap().is_empty());
    assert_eq!(events(&f, &a, "agent_prepare_failed"), 1);
}

fn command(name: &str, script: &str, depends_on: &[&str]) -> CommandSpec {
    let mut c = payload(script).commands.remove(0);
    c.node_id = NodeId::parse(name).unwrap();
    c.depends_on = depends_on
        .iter()
        .map(|d| NodeId::parse(d).unwrap())
        .collect();
    c
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_command_runs_after_the_ones_it_depends_on_also_when_they_are_given_later() {
    let f = fx();
    let agent = f.start("boot-a");
    let c = f.connect(&agent).await;
    let order = f.path("order");
    let say = |name: &str| format!("echo {name} >> {}", order.display());
    let mut p = payload("true");
    p.commands = vec![
        command("test", &say("test"), &["build", "fetch"]),
        command("build", &say("build"), &["fetch"]),
        command("lint", &say("lint"), &[]),
        command("fetch", &say("fetch"), &[]),
    ];
    let a = AttemptId::generate();
    submit(&c, &a, &p).await;
    assert_eq!(f.wait_terminal(&a), Outcome::Succeeded);
    assert_eq!(
        std::fs::read_to_string(&order).unwrap(),
        "lint\nfetch\nbuild\ntest\n"
    );
    let launched: Vec<String> = f
        .store()
        .launches(&a)
        .unwrap()
        .into_iter()
        .map(|l| l.0.to_string())
        .collect();
    assert_eq!(launched, ["lint", "fetch", "build", "test"]);

    // A dependency that fails: what waits for it never starts.
    let mut p = payload("true");
    p.commands = vec![
        command("test", &say("never"), &["build"]),
        command("build", "exit 7", &[]),
    ];
    let b = AttemptId::generate();
    submit(&c, &b, &p).await;
    assert_eq!(f.wait_terminal(&b), Outcome::Failed);
    assert_eq!(f.store().launches(&b).unwrap().len(), 1);
    assert!(!std::fs::read_to_string(&order).unwrap().contains("never"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_command_s_own_timeout_overrides_the_default_of_the_attempt() {
    let f = fx();
    let agent = f.start("boot-a");
    let c = f.connect(&agent).await;
    // Longer than the default: the command may take its time.
    let mut p = payload("sleep 1.2");
    p.command_timeout_ms = Some(400);
    p.commands[0].timeout_ms = Some(30_000);
    let a = AttemptId::generate();
    submit(&c, &a, &p).await;
    assert_eq!(f.wait_terminal(&a), Outcome::Succeeded);
    // Shorter than the default, and with no default at all.
    for default in [Some(60_000), None] {
        let mut p = payload("sleep 30");
        p.command_timeout_ms = default;
        p.commands[0].timeout_ms = Some(400);
        let b = AttemptId::generate();
        let started = Instant::now();
        submit(&c, &b, &p).await;
        assert_eq!(f.wait_terminal(&b), Outcome::TimedOut);
        assert!(started.elapsed() < Duration::from_secs(15));
        let t = f.rec(&b).termination.unwrap();
        assert_eq!(
            t.timeout().unwrap().kind,
            remote_task_runner::state::TimeoutKind::Command
        );
    }
}

// ------------------------------------------------- results, events, GC

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn nothing_is_collected_as_garbage_before_ack_collected() {
    let f = fx();
    let mut cfg = f.cfg("boot-a");
    // Keep nothing longer than necessary: only the missing ACK protects.
    cfg.retention_ms = 0;
    let agent = f.start_with(cfg);
    let c = f.connect(&agent).await;
    let a = AttemptId::generate();
    submit(&c, &a, &payload("echo output")).await;
    assert_eq!(f.wait_terminal(&a), Outcome::Succeeded);

    // The supervisor stored the command's output and references it.
    let logs = f.state().join("attempts").join(a.as_str()).join("logs");
    let stored = || std::fs::read_dir(&logs).is_ok_and(|mut d| d.next().is_some());
    assert!(stored(), "the output of the command is in the spool");

    std::thread::sleep(Duration::from_millis(5));
    for _ in 0..3 {
        let report = agent.gc().unwrap();
        assert!(report.removed.is_empty(), "{report:?}");
        agent.tick();
        assert!(stored());
    }

    let get = |generation| {
        Request::GetResult(GetResultRequest {
            schema_version: SchemaVersion::CURRENT,
            request_id: RequestId::generate(),
            attempt_id: a.clone(),
            generation: Generation(generation),
        })
    };
    let e = call(&c, get(2)).await.unwrap_err();
    assert_eq!(e.code, ErrorCode::Conflict);
    let Response::GetResult(result) = call(&c, get(1)).await.unwrap() else {
        panic!()
    };
    assert_eq!(result.termination.outcome(), Outcome::Succeeded);
    let ack = |digest: Digest| {
        Request::AckCollected(AckCollectedRequest {
            schema_version: SchemaVersion::CURRENT,
            request_id: RequestId::generate(),
            attempt_id: a.clone(),
            generation: Generation(1),
            result_digest: digest,
        })
    };
    // An ACK for something else than this result releases nothing.
    let e = call(&c, ack(Digest::of_bytes(b"other"))).await.unwrap_err();
    assert_eq!(e.code, ErrorCode::Conflict);
    assert!(agent.gc().unwrap().removed.is_empty());
    assert!(stored());

    for _ in 0..2 {
        let Response::AckCollected(r) = call(&c, ack(result.result_digest.clone())).await.unwrap()
        else {
            panic!()
        };
        assert!(r.released);
    }
    assert!(f.rec(&a).acked_at_ms.is_some());
    let report = agent.gc().unwrap();
    assert_eq!(report.removed.len(), 1);
    assert!(!logs.exists());
    // The attempt's record and tombstone-level facts stay: a resend is still
    // the same attempt, never a new run.
    assert!(
        submit(&c, &a, &serde_json::from_value(f.rec(&a).spec).unwrap())
            .await
            .duplicate
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn status_events_logs_lease_and_inspect_answer_from_the_store() {
    let f = fx();
    let agent = f.start("boot-a");
    let c = f.connect(&agent).await;
    let (script, _marker, pidfile) = stay(&f);
    let a = AttemptId::generate();
    submit(&c, &a, &payload(&script)).await;
    wait_for("the command", || read_pid(&pidfile).is_some());
    wait_for("the running state", || {
        f.rec(&a).state == AttemptState::Active
    });

    let renew = |seq| {
        Request::RenewLease(RenewLeaseRequest {
            schema_version: SchemaVersion::CURRENT,
            request_id: RequestId::generate(),
            coordinator_epoch: CoordinatorEpoch(1),
            attempt_id: a.clone(),
            generation: Generation(1),
            lease_seq: seq,
            lease_ms: 60_000,
        })
    };
    let Response::RenewLease(r) = call(&c, renew(1)).await.unwrap() else {
        panic!()
    };
    assert!(r.extended);
    assert_eq!(r.lease_remaining_ms, 60_000);
    let Response::RenewLease(r) = call(&c, renew(1)).await.unwrap() else {
        panic!()
    };
    assert!(!r.extended, "a replay does not extend");

    let Response::Inspect(i) = call(
        &c,
        Request::Inspect(InspectRequest {
            schema_version: SchemaVersion::CURRENT,
            request_id: RequestId::generate(),
        }),
    )
    .await
    .unwrap() else {
        panic!()
    };
    assert_eq!((i.active_task, i.prefetch_pending), (1, 0));
    assert_eq!(i.agent_epoch, agent.agent_epoch());
    assert!(i.disk_free_bytes > 0);

    let list = |include_terminal, states| {
        Request::ListAttempts(ListAttemptsRequest {
            schema_version: SchemaVersion::CURRENT,
            request_id: RequestId::generate(),
            states,
            include_terminal,
        })
    };
    let Response::ListAttempts(l) = call(&c, list(false, None)).await.unwrap() else {
        panic!()
    };
    assert_eq!(l.attempts.len(), 1);
    assert_eq!(l.attempts[0].state, AttemptState::Active);
    assert_eq!(l.attempts[0].worker_boot_id.as_str(), "boot-a");
    let Response::ListAttempts(l) = call(&c, list(false, Some(vec![AttemptState::Ready])))
        .await
        .unwrap()
    else {
        panic!()
    };
    assert!(l.attempts.is_empty());

    call(&c, cancel_req(&a)).await.unwrap();
    assert_eq!(f.wait_terminal(&a), Outcome::Cancelled);
    let e = call(&c, renew(2)).await.unwrap_err();
    assert_eq!(e.code, ErrorCode::LeaseNotRenewable);
    let Response::ListAttempts(l) = call(&c, list(false, None)).await.unwrap() else {
        panic!()
    };
    assert!(l.attempts.is_empty());
    let Response::ListAttempts(l) = call(&c, list(true, None)).await.unwrap() else {
        panic!()
    };
    assert_eq!(l.attempts.len(), 1);
    assert!(l.attempts[0].termination.is_some());

    // Events: a per-attempt sequence without holes, resumable.
    let watch = |attempt: Option<AttemptId>, after_seq, limit| {
        Request::WatchEvents(WatchEventsRequest {
            schema_version: SchemaVersion::CURRENT,
            request_id: RequestId::generate(),
            attempt_id: attempt,
            after_seq,
            limit,
        })
    };
    let Response::WatchEvents(w) = call(&c, watch(Some(a.clone()), 0, 500)).await.unwrap() else {
        panic!()
    };
    let seqs: Vec<u64> = w.events.iter().map(|e| e.seq).collect();
    assert_eq!(seqs, (1..=seqs.len() as u64).collect::<Vec<_>>());
    assert_eq!(w.next_seq, seqs.len() as u64);
    assert_eq!(w.events[0].kind, EventKind::ReceiptReached);
    assert_eq!(w.events.last().unwrap().state, Some(AttemptState::Terminal));
    let mut cursor = remote_task_runner::protocol::chunk::EventCursor::new(a.clone(), 0);
    assert_eq!(
        cursor.fresh(&w.events, w.next_seq).unwrap().len(),
        seqs.len()
    );
    let Response::WatchEvents(page) = call(&c, watch(Some(a.clone()), 2, 2)).await.unwrap() else {
        panic!()
    };
    assert_eq!(page.events, w.events[2..4].to_vec());
    assert_eq!(page.next_seq, 4);
    let Response::WatchEvents(end) = call(&c, watch(Some(a.clone()), w.next_seq, 10))
        .await
        .unwrap()
    else {
        panic!()
    };
    assert!(end.events.is_empty());
    assert_eq!(end.next_seq, w.next_seq);
    let e = call(&c, watch(None, 0, 10)).await.unwrap_err();
    assert_eq!(e.code, ErrorCode::InvalidInput);
    let e = call(&c, watch(Some(AttemptId::generate()), 0, 10))
        .await
        .unwrap_err();
    assert_eq!(e.code, ErrorCode::NotFound);

    // No spool was written: an ended attempt has an empty, complete log.
    let Response::ReadLog(log) = call(
        &c,
        Request::ReadLog(ReadLogRequest {
            schema_version: SchemaVersion::CURRENT,
            request_id: RequestId::generate(),
            attempt_id: a.clone(),
            node_id: NodeId::parse("main").unwrap(),
            stream: LogStream::Stdout,
            offset: 0,
            max_bytes: 1024,
        }),
    )
    .await
    .unwrap() else {
        panic!()
    };
    assert!(log.eof && log.data.0.is_empty());

    // Operations of other rows say so instead of pretending.
    let e = call(
        &c,
        Request::BlobVerify(BlobVerifyRequest {
            schema_version: SchemaVersion::CURRENT,
            request_id: RequestId::generate(),
            source_id: SourceId::generate(),
            blobs: vec![],
        }),
    )
    .await
    .unwrap_err();
    assert_eq!(e.code, ErrorCode::NotImplemented);
}

// ------------------------------------------------- limits and failures

fn list_req(states: Option<Vec<AttemptState>>, include_terminal: bool) -> Request {
    Request::ListAttempts(ListAttemptsRequest {
        schema_version: SchemaVersion::CURRENT,
        request_id: RequestId::generate(),
        states,
        include_terminal,
    })
}

async fn listed(c: &Client, states: Option<Vec<AttemptState>>, terminal: bool) -> Vec<AttemptId> {
    match call(c, list_req(states, terminal)).await.unwrap() {
        Response::ListAttempts(l) => l.attempts.into_iter().map(|s| s.attempt_id).collect(),
        other => panic!("{other:?}"),
    }
}

fn put_req(a: &AttemptId) -> Request {
    Request::SecretBundlePut(SecretBundlePutRequest {
        schema_version: SchemaVersion::CURRENT,
        request_id: RequestId::generate(),
        coordinator_epoch: CoordinatorEpoch(1),
        attempt_id: a.clone(),
        generation: Generation(1),
        files: vec![SecretFile {
            path: ".env".into(),
            hmac_digest: HmacDigest::parse(&format!("hmac-sha256:{}", "a".repeat(64))).unwrap(),
            content: Bytes(b"token".to_vec()),
        }],
    })
}

fn events(f: &Fx, a: &AttemptId, kind: &str) -> usize {
    f.store().events_of(a, kind).unwrap().len()
}

fn workspace_record(f: &Fx, a: &AttemptId) -> PathBuf {
    f.state()
        .join("attempts")
        .join(a.as_str())
        .join("workspace.json")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn list_attempts_filters_in_the_database_and_never_answers_with_a_part() {
    let f = fx();
    let mut cfg = f.cfg("boot-a");
    cfg.max_list_attempts = 3;
    cfg.secret_wait_ms = 120_000;
    let agent = f.start_with(cfg);
    let c = f.connect(&agent).await;
    // The two oldest attempts are still being prepared (they wait for their
    // secret bundle); four newer ones have ended. No command runs, so the
    // test does not depend on how many commands the machine admits at once.
    f.host.secrets_ready.store(false, Ordering::SeqCst);
    let mut running = Vec::new();
    for _ in 0..2 {
        let a = AttemptId::generate();
        submit(&c, &a, &payload("true")).await;
        running.push(a);
    }
    wait_for("both to wait for their secrets", || {
        running
            .iter()
            .all(|a| f.rec(a).state == AttemptState::Preparing && workspace_record(&f, a).exists())
    });
    for _ in 0..4 {
        let a = AttemptId::generate();
        submit(&c, &a, &payload("true")).await;
        call(&c, cancel_req(&a)).await.unwrap();
        f.wait_terminal(&a);
    }

    // Six attempts, three fit. The filter runs before the limit: the two
    // live ones are found behind four newer ones.
    let mut got = listed(&c, Some(vec![AttemptState::Preparing]), true).await;
    got.sort();
    running.sort();
    assert_eq!(got, running);
    assert_eq!(listed(&c, None, false).await.len(), 2);
    assert!(listed(&c, Some(vec![]), true).await.is_empty());
    assert!(listed(&c, Some(vec![AttemptState::Terminal]), false)
        .await
        .is_empty());
    // More than fit is an error, not the newest three.
    for req in [
        list_req(None, true),
        list_req(Some(vec![AttemptState::Terminal]), true),
    ] {
        let e = call(&c, req).await.unwrap_err();
        assert_eq!(e.code, ErrorCode::InvalidInput);
        assert!(e.message.contains("more than 3"), "{}", e.message);
    }
    for a in &running {
        call(&c, retire_req(a)).await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn watch_events_refuses_a_journal_longer_than_it_reads() {
    let f = fx();
    let mut cfg = f.cfg("boot-a");
    cfg.max_event_scan = 2;
    let agent = f.start_with(cfg);
    let c = f.connect(&agent).await;
    let a = AttemptId::generate();
    submit(&c, &a, &payload("true")).await;
    f.wait_terminal(&a);
    let e = call(
        &c,
        Request::WatchEvents(WatchEventsRequest {
            schema_version: SchemaVersion::CURRENT,
            request_id: RequestId::generate(),
            attempt_id: Some(a.clone()),
            after_seq: 0,
            limit: 10,
        }),
    )
    .await
    .unwrap_err();
    assert_eq!(e.code, ErrorCode::Internal);
    assert!(e.message.contains("more than 2"), "{}", e.message);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_new_attempt_beyond_the_live_cap_is_refused_and_a_resend_is_not() {
    let f = fx();
    let mut cfg = f.cfg("boot-a");
    cfg.max_live_attempts = 2;
    let agent = f.start_with(cfg);
    let c = f.connect(&agent).await;
    // They wait for a secret bundle that does not come.
    f.host.secrets_ready.store(false, Ordering::SeqCst);
    let (a1, a2, a3) = (
        AttemptId::generate(),
        AttemptId::generate(),
        AttemptId::generate(),
    );
    let (p1, p3) = (payload("true"), payload("true"));
    submit(&c, &a1, &p1).await;
    submit(&c, &a2, &payload("true")).await;
    let e = call(&c, submit_req(&a3, &p3)).await.unwrap_err();
    assert_eq!(e.code, ErrorCode::Overloaded);
    assert!(f.store().get(&a3).unwrap().is_none(), "nothing was stored");
    // The cap is about new load: an accepted attempt is answered as before.
    assert!(submit(&c, &a1, &p1).await.duplicate);
    // And another payload under an accepted id is still a conflict.
    let e = call(&c, submit_req(&a1, &p3)).await.unwrap_err();
    assert_eq!(e.code, ErrorCode::Conflict);

    call(&c, cancel_req(&a1)).await.unwrap();
    assert_eq!(f.wait_terminal(&a1), Outcome::Cancelled);
    assert!(!submit(&c, &a3, &p3).await.duplicate);
    for a in [&a2, &a3] {
        call(&c, cancel_req(a)).await.unwrap();
        f.wait_terminal(a);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_submit_that_no_supervisor_could_honour_is_refused_before_it_is_stored() {
    let f = fx();
    let agent = f.start("boot-a");
    let c = f.connect(&agent).await;
    let a = AttemptId::generate();
    let mut p = payload("true");
    // The fixture's supervisors look every 25 ms.
    p.lease_ms = 25;
    let e = call(&c, submit_req(&a, &p)).await.unwrap_err();
    assert_eq!(e.code, ErrorCode::InvalidInput);
    p.lease_ms = 25 * 60 * 60 * 1000;
    let e = call(&c, submit_req(&a, &p)).await.unwrap_err();
    assert_eq!(e.code, ErrorCode::InvalidInput);
    p.lease_ms = 60_000;
    p.commands[0].env.insert("A=B".into(), "x".into());
    let e = call(&c, submit_req(&a, &p)).await.unwrap_err();
    assert_eq!(e.code, ErrorCode::InvalidInput);
    // A command that waits for itself, or for one that does not exist.
    let mut p = payload("true");
    p.commands[0].depends_on = vec![NodeId::parse("main").unwrap()];
    let e = call(&c, submit_req(&a, &p)).await.unwrap_err();
    assert_eq!(e.code, ErrorCode::InvalidInput);
    p.commands[0].depends_on = vec![NodeId::parse("nowhere").unwrap()];
    let e = call(&c, submit_req(&a, &p)).await.unwrap_err();
    assert_eq!(e.code, ErrorCode::InvalidInput);
    let mut p = payload("true");
    p.commands[0].timeout_ms = Some(0);
    let e = call(&c, submit_req(&a, &p)).await.unwrap_err();
    assert_eq!(e.code, ErrorCode::InvalidInput);
    assert!(f.store().get(&a).unwrap().is_none());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_submit_with_a_refused_artifact_declaration_is_refused_before_it_is_stored() {
    use MissingPolicy::{Optional, Required};
    let f = fx();
    let agent = f.start("boot-a");
    let c = f.connect(&agent).await;
    *f.host.secret_paths.lock().unwrap() =
        Some(vec![".env".to_string(), "config/local.json".to_string()]);
    let a = AttemptId::generate();
    let refused = |artifacts: Vec<ArtifactDecl>| {
        let mut p = payload("true");
        p.artifacts = artifacts;
        submit_req(&a, &p)
    };
    let cases: Vec<(&str, Vec<ArtifactDecl>)> = vec![
        (
            "too many",
            (0..1025)
                .map(|i| decl(&format!("n{i}"), &format!("p{i}"), Optional))
                .collect(),
        ),
        ("no name", vec![decl("", "a", Required)]),
        ("long name", vec![decl(&"n".repeat(256), "a", Required)]),
        ("long path", vec![decl("a", &"p".repeat(1025), Required)]),
        ("absolute", vec![decl("a", "/etc/hosts", Required)]),
        ("dot dot", vec![decl("a", "out/../../a", Required)]),
        ("name leaves the store", vec![decl("../a", "a", Required)]),
        (
            "same name",
            vec![decl("a", "x", Required), decl("a", "y", Optional)],
        ),
        (
            "same path",
            vec![decl("a", "x", Required), decl("b", "x", Optional)],
        ),
        (
            "a secret file",
            vec![decl("a", "out/a", Required), decl("env", ".env", Optional)],
        ),
        (
            "a secret file spelled in another case",
            vec![decl("a", "Config/Local.json", Required)],
        ),
    ];
    for (what, artifacts) in cases {
        let e = call(&c, refused(artifacts)).await.expect_err(what);
        assert_eq!(e.code, ErrorCode::InvalidInput, "{what}: {e:?}");
    }
    assert!(f.store().get(&a).unwrap().is_none());
    assert_eq!(f.host.count(&format!("prepare {a}")), 0);

    // The same declarations without the fault are accepted, and the stored
    // payload holds them.
    let mut p = payload("true");
    p.artifacts = vec![decl("a", "out/a", Required), decl("b", "out/b", Optional)];
    assert!(!submit(&c, &a, &p).await.duplicate);
    let stored: AttemptPayload = serde_json::from_value(f.rec(&a).spec).unwrap();
    assert_eq!(stored.artifacts, p.artifacts);
    // A resend is the same attempt; the same attempt declaring otherwise is
    // another payload (the declarations are part of the hash).
    assert!(submit(&c, &a, &p).await.duplicate);
    p.artifacts[1].if_missing = Required;
    let e = call(&c, submit_req(&a, &p)).await.unwrap_err();
    assert_eq!(e.code, ErrorCode::Conflict);
    f.wait_terminal(&a);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_secret_file_declared_as_an_artifact_starts_no_command_when_the_source_came_late() {
    let f = fx();
    let agent = f.start("boot-a");
    let c = f.connect(&agent).await;
    // The source is not on the worker at the acceptance, so nothing can be
    // compared yet; it is there when the workspace has been made.
    *f.host.secret_paths.lock().unwrap() = None;
    f.host.block_prepare.store(true, Ordering::SeqCst);
    let marker = f.path("ran");
    let a = AttemptId::generate();
    let mut p = payload(&format!("touch {}", marker.display()));
    p.artifacts = vec![decl("env", ".env", MissingPolicy::Optional)];
    submit(&c, &a, &p).await;
    *f.host.secret_paths.lock().unwrap() = Some(vec![".env".to_string()]);
    f.host.block_prepare.store(false, Ordering::SeqCst);
    assert_eq!(f.wait_terminal(&a), Outcome::Failed);
    assert!(!marker.exists());
    assert!(f.store().launches(&a).unwrap().is_empty());
    let failed = f.store().events_of(&a, "agent_prepare_failed").unwrap();
    assert_eq!(failed.len(), 1);
    assert_eq!(failed[0].payload["code"], "INVALID_INPUT");

    // A source that never came: the declarations cannot be told from its
    // secret files, and nothing is started either.
    *f.host.secret_paths.lock().unwrap() = None;
    let b = AttemptId::generate();
    let mut p = payload(&format!("touch {}", marker.display()));
    p.artifacts = vec![decl("out", "out.txt", MissingPolicy::Optional)];
    submit(&c, &b, &p).await;
    assert_eq!(f.wait_terminal(&b), Outcome::Failed);
    assert!(!marker.exists());
    assert!(f.store().launches(&b).unwrap().is_empty());
    assert_eq!(events(&f, &b, "agent_prepare_failed"), 1);

    // An attempt that declares nothing does not need the list.
    let d = AttemptId::generate();
    submit(&c, &d, &payload("true")).await;
    assert_eq!(f.wait_terminal(&d), Outcome::Succeeded);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_source_whose_secret_files_cannot_be_told_refuses_the_submit_that_declares_artifacts() {
    let f = fx();
    let agent = f.start("boot-a");
    let c = f.connect(&agent).await;
    // The source is here, and it is not the one the payload names; or the
    // store cannot be read. Neither is "not here yet": the answer is the
    // error, and nothing is accepted.
    for code in [ErrorCode::Conflict, ErrorCode::Internal] {
        *f.host.secret_paths_error.lock().unwrap() = Some(ApiError::new(code, "the manifest"));
        let a = AttemptId::generate();
        let mut p = payload("true");
        p.artifacts = vec![decl("out", "out.txt", MissingPolicy::Optional)];
        let e = call(&c, submit_req(&a, &p)).await.unwrap_err();
        assert_eq!(e.code, code, "{e:?}");
        assert!(f.store().get(&a).unwrap().is_none());
        assert_eq!(f.host.count(&format!("prepare {a}")), 0);
    }
    // An attempt that declares nothing does not ask.
    let b = AttemptId::generate();
    submit(&c, &b, &payload("true")).await;
    assert_eq!(f.wait_terminal(&b), Outcome::Succeeded);
}

async fn result_of(c: &Client, a: &AttemptId) -> GetResultResponse {
    match call(
        c,
        Request::GetResult(GetResultRequest {
            schema_version: SchemaVersion::CURRENT,
            request_id: RequestId::generate(),
            attempt_id: a.clone(),
            generation: Generation(1),
        }),
    )
    .await
    .unwrap()
    {
        Response::GetResult(r) => r,
        other => panic!("{other:?}"),
    }
}

/// Why the journal says the artifact `name` of `a` was not stored.
fn why_not_stored(f: &Fx, a: &AttemptId, name: &str) -> String {
    let said = f.store().events_of(a, "artifact_not_stored").unwrap();
    let of_name: Vec<_> = said.iter().filter(|e| e.payload["name"] == name).collect();
    assert_eq!(of_name.len(), 1, "{name}");
    of_name[0].payload["why"].as_str().unwrap().to_string()
}

/// The link is refused for the file it is and the copy for what it holds.
fn refused_as_link_and_copy(f: &Fx, a: &AttemptId) {
    let (leak, copy) = (why_not_stored(f, a, "leak"), why_not_stored(f, a, "copy"));
    assert!(
        leak.contains("is one of the attempt's secret files"),
        "{leak}"
    );
    assert!(copy.contains("contains a secret value"), "{copy}");
    assert!(!format!("{leak}{copy}").contains("s3cr3t"));
}

/// `(name, reason)` of what the result says was not stored, by name.
fn not_stored(r: &GetResultResponse) -> Vec<(String, NotStoredReason)> {
    let mut v: Vec<_> = r
        .artifacts_not_stored
        .iter()
        .map(|n| (n.name.clone(), n.reason))
        .collect();
    v.sort_by(|a, b| a.0.cmp(&b.0));
    v
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_link_to_a_placed_secret_file_and_a_copy_of_its_value_are_not_kept_as_artifacts() {
    const SECRET: &str = "s3cr3t-Value-0123456789";
    let f = fx();
    let agent = f.start("boot-a");
    let c = f.connect(&agent).await;
    f.host.secrets_ready.store(false, Ordering::SeqCst);
    // Neither declared path is a secret file's path: the acceptance has
    // nothing to refuse. What the names lead to is known only at the end.
    let mut p = payload("ln .env leak.txt && cp .env copy.txt && printf fine > fine.txt");
    p.has_secret_bundle = true;
    p.artifacts = vec![
        decl("leak", "leak.txt", MissingPolicy::Required),
        decl("copy", "copy.txt", MissingPolicy::Required),
        decl("fine", "fine.txt", MissingPolicy::Required),
    ];
    let a = AttemptId::generate();
    submit(&c, &a, &p).await;
    wait_for("the workspace record", || workspace_record(&f, &a).exists());
    let put = Request::SecretBundlePut(SecretBundlePutRequest {
        schema_version: SchemaVersion::CURRENT,
        request_id: RequestId::generate(),
        coordinator_epoch: CoordinatorEpoch(1),
        attempt_id: a.clone(),
        generation: Generation(1),
        files: vec![SecretFile {
            path: ".env".into(),
            hmac_digest: HmacDigest::parse(&format!("hmac-sha256:{}", "a".repeat(64))).unwrap(),
            content: Bytes(format!("API_TOKEN={SECRET}\n").into_bytes()),
        }],
    });
    call(&c, put).await.unwrap();
    assert_eq!(f.wait_terminal(&a), Outcome::Succeeded);
    let result = result_of(&c, &a).await;
    let names: Vec<&str> = result.artifacts.iter().map(|r| r.name.as_str()).collect();
    assert_eq!(names, ["fine"]);
    assert_eq!(
        not_stored(&result),
        [
            ("copy".to_string(), NotStoredReason::Refused),
            ("leak".to_string(), NotStoredReason::Refused),
        ]
    );
    refused_as_link_and_copy(&f, &a);
    let kept = f
        .state()
        .join("attempts")
        .join(a.as_str())
        .join("artifacts");
    let files: Vec<_> = std::fs::read_dir(&kept)
        .unwrap()
        .map(|e| e.unwrap().file_name())
        .collect();
    assert_eq!(files, ["fine"]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn declared_artifacts_reach_the_supervisor_and_the_result_tells_required_from_optional() {
    use MissingPolicy::{Optional, Required};
    let f = fx();
    let mut cfg = f.cfg("boot-a");
    cfg.profile_fingerprint = Digest::of_bytes(b"the profile of this worker");
    let agent = f.start_with(cfg.clone());
    let c = f.connect(&agent).await;
    let a = AttemptId::generate();
    let mut p = payload("mkdir out && printf abc > out/report.txt && printf x > note.txt");
    p.source_id = SourceId::generate();
    p.plan_digest = Digest::of_bytes(b"the plan");
    p.artifacts = vec![
        decl("reports/unit.txt", "out/report.txt", Required),
        decl("note", "note.txt", Optional),
        decl("needed", "out/never.txt", Required),
        decl("nice", "nowhere/nice.txt", Optional),
        decl("dir", "out", Required),
    ];
    submit(&c, &a, &p).await;
    assert_eq!(f.wait_terminal(&a), Outcome::Succeeded);

    let Response::GetResult(result) = call(
        &c,
        Request::GetResult(GetResultRequest {
            schema_version: SchemaVersion::CURRENT,
            request_id: RequestId::generate(),
            attempt_id: a.clone(),
            generation: Generation(1),
        }),
    )
    .await
    .unwrap() else {
        panic!()
    };
    // Everything chapter 13 lists, per artifact.
    let record = |name: &str, content: &[u8]| ArtifactRecord {
        name: name.to_string(),
        path: name.to_string(),
        size: content.len() as u64,
        digest: Digest::of_bytes(content),
        source_id: p.source_id.clone(),
        plan_digest: p.plan_digest.clone(),
        attempt_id: a.clone(),
        profile_fingerprint: cfg.profile_fingerprint.clone(),
    };
    assert_eq!(
        result.artifacts,
        vec![record("note", b"x"), record("reports/unit.txt", b"abc")]
    );
    let unstored = |name: &str, path: &str, if_missing, reason| ArtifactNotStored {
        name: name.to_string(),
        path: path.to_string(),
        if_missing,
        reason,
    };
    assert_eq!(
        result.artifacts_not_stored,
        vec![
            unstored("dir", "out", Required, NotStoredReason::Refused),
            unstored(
                "needed",
                "out/never.txt",
                Required,
                NotStoredReason::Missing
            ),
            unstored(
                "nice",
                "nowhere/nice.txt",
                Optional,
                NotStoredReason::Missing
            ),
        ]
    );
    // The files are in the attempt's store, under their names, and the
    // stored result (what the digest is over) holds both lists.
    let kept = f
        .state()
        .join("attempts")
        .join(a.as_str())
        .join("artifacts");
    assert_eq!(
        std::fs::read(kept.join("reports/unit.txt")).unwrap(),
        b"abc"
    );
    assert_eq!(std::fs::read(kept.join("note")).unwrap(), b"x");
    assert!(!kept.join("needed").exists() && !kept.join("dir").exists());
    let stored = f.store().get_result(&a).unwrap().unwrap();
    assert_eq!(stored["artifacts"].as_array().unwrap().len(), 2);
    assert_eq!(stored["artifacts_not_stored"].as_array().unwrap().len(), 3);
    assert_eq!(
        result.result_digest,
        remote_task_runner::wire::payload_hash(&stored).unwrap()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn workspaces_are_prepared_no_more_than_the_limit_at_once() {
    let f = fx();
    let mut cfg = f.cfg("boot-a");
    cfg.max_concurrent_prepares = 1;
    let agent = f.start_with(cfg);
    let c = f.connect(&agent).await;
    f.host.block_prepare.store(true, Ordering::SeqCst);
    let (a1, a2) = (AttemptId::generate(), AttemptId::generate());
    submit(&c, &a1, &payload("true")).await;
    submit(&c, &a2, &payload("true")).await;
    wait_for("both to be in preparation", || {
        [&a1, &a2]
            .iter()
            .all(|a| f.rec(a).state == AttemptState::Preparing)
    });
    std::thread::sleep(Duration::from_millis(200));
    assert_eq!(f.host.preparing_max.load(Ordering::SeqCst), 1);
    f.host.block_prepare.store(false, Ordering::SeqCst);
    assert_eq!(f.wait_terminal(&a1), Outcome::Succeeded);
    assert_eq!(f.wait_terminal(&a2), Outcome::Succeeded);
    assert_eq!(f.host.preparing_max.load(Ordering::SeqCst), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_watcher_that_panics_fails_its_attempt_and_frees_it() {
    let f = fx();
    let agent = f.start("boot-a");
    let c = f.connect(&agent).await;
    f.host.panic_secrets.store(true, Ordering::SeqCst);
    let a = AttemptId::generate();
    submit(&c, &a, &payload("true")).await;
    assert_eq!(f.wait_terminal(&a), Outcome::Failed);
    assert_eq!(events(&f, &a, "agent_drive_panicked"), 1);
    // `give_up` settles the attempt first and notes the event after: the
    // event is waited for, not read the moment the attempt is terminal.
    wait_for("the agent_gave_up event", || {
        events(&f, &a, "agent_gave_up") >= 1
    });
    assert_eq!(events(&f, &a, "agent_gave_up"), 1);
    wait_for("the cleanup", || f.host.count(&format!("cleanup {a}")) > 0);
    // The agent goes on: the next attempt runs.
    f.host.panic_secrets.store(false, Ordering::SeqCst);
    let b = AttemptId::generate();
    submit(&c, &b, &payload("true")).await;
    assert_eq!(f.wait_terminal(&b), Outcome::Succeeded);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_preparation_that_takes_too_long_fails_the_attempt_and_its_late_workspace_goes() {
    let f = fx();
    let mut cfg = f.cfg("boot-a");
    cfg.prepare_timeout_ms = 150;
    let agent = f.start_with(cfg);
    let c = f.connect(&agent).await;
    f.host.block_prepare.store(true, Ordering::SeqCst);
    let a = AttemptId::generate();
    submit(&c, &a, &payload("true")).await;
    assert_eq!(f.wait_terminal(&a), Outcome::Failed);
    assert_eq!(events(&f, &a, "agent_prepare_failed"), 1);
    // The host is still at it and holds its permit. When it returns, the
    // workspace nobody waits for is removed.
    assert_eq!(f.host.preparing_now.load(Ordering::SeqCst), 1);
    f.host.block_prepare.store(false, Ordering::SeqCst);
    wait_for("the late workspace to go", || {
        f.host.preparing_now.load(Ordering::SeqCst) == 0
            && f.host.count(&format!("cleanup {a}")) >= 2
            && !f.host.ws_dir(&a).exists()
    });
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_lease_that_ran_out_during_the_preparation_starts_no_command() {
    let f = fx();
    let agent = f.start("boot-a");
    let c = f.connect(&agent).await;
    f.host.block_prepare.store(true, Ordering::SeqCst);
    let (script, marker, _) = stay(&f);
    let mut p = payload(&script);
    p.lease_ms = 300;
    let a = AttemptId::generate();
    submit(&c, &a, &p).await;
    std::thread::sleep(Duration::from_millis(500));
    f.host.block_prepare.store(false, Ordering::SeqCst);
    assert_eq!(f.wait_terminal(&a), Outcome::Interrupted);
    assert_eq!(lines(&marker), 0);
    assert!(f.store().launches(&a).unwrap().is_empty());
    assert!(matches!(f.store().lease(&a).unwrap(), Some((_, true))));
    let Response::WatchEvents(w) = call(
        &c,
        Request::WatchEvents(WatchEventsRequest {
            schema_version: SchemaVersion::CURRENT,
            request_id: RequestId::generate(),
            attempt_id: Some(a.clone()),
            after_seq: 0,
            limit: 100,
        }),
    )
    .await
    .unwrap() else {
        panic!()
    };
    let expired = |e: &&AttemptEvent| e.kind == EventKind::LeaseExpired;
    assert_eq!(w.events.iter().filter(expired).count(), 1, "{:?}", w.events);
    // From the journal entry the database wrote with the expiry itself.
    assert_eq!(events(&f, &a, "lease_expired"), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_supervisor_gets_what_is_left_of_the_deadline_and_none_left_starts_nothing() {
    let f = fx();
    let agent = f.start("boot-a");
    let c = f.connect(&agent).await;
    f.host.block_prepare.store(true, Ordering::SeqCst);
    let (script, marker, pidfile) = stay(&f);
    let mut p = payload(&script);
    p.deadline_ms = Some(60_000);
    let a = AttemptId::generate();
    submit(&c, &a, &p).await;
    std::thread::sleep(Duration::from_millis(300));
    f.host.block_prepare.store(false, Ordering::SeqCst);
    wait_for("the command", || read_pid(&pidfile).is_some());
    let spec: serde_json::Value = serde_json::from_slice(
        &std::fs::read(
            f.state()
                .join("attempts")
                .join(a.as_str())
                .join("supervise.json"),
        )
        .unwrap(),
    )
    .unwrap();
    let left = spec["deadline_ms"].as_u64().unwrap();
    assert!(left > 0 && left <= 60_000 - 300, "{left}");
    call(&c, retire_req(&a)).await.unwrap();
    f.wait_terminal(&a);
    assert_eq!(lines(&marker), 1);

    // The whole deadline passes while the workspace is made.
    f.host.block_prepare.store(true, Ordering::SeqCst);
    let mut p = payload(&script);
    p.deadline_ms = Some(200);
    let b = AttemptId::generate();
    submit(&c, &b, &p).await;
    std::thread::sleep(Duration::from_millis(400));
    f.host.block_prepare.store(false, Ordering::SeqCst);
    assert_eq!(f.wait_terminal(&b), Outcome::TimedOut);
    assert_eq!(lines(&marker), 1, "the second command never started");
    assert!(f.store().launches(&b).unwrap().is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_lease_counts_from_its_grant_not_from_the_supervisors_start() {
    let f = fx();
    let agent = f.start("boot-a");
    let c = f.connect(&agent).await;
    // Never renewed: the supervisor starts 2 s into a 4 s lease and would
    // count 4 s of its own (ending 6 s after the acceptance). It is given
    // what is left and ends the lease when the grant ends.
    f.host.block_prepare.store(true, Ordering::SeqCst);
    let (script, marker, _) = stay(&f);
    let mut p = payload(&script);
    p.lease_ms = 4_000;
    let a = AttemptId::generate();
    submit(&c, &a, &p).await;
    std::thread::sleep(Duration::from_millis(2_000));
    f.host.block_prepare.store(false, Ordering::SeqCst);
    assert_eq!(f.wait_terminal(&a), Outcome::Interrupted);
    assert_eq!(lines(&marker), 1, "the command did start");
    // The supervisor saw it on its own clock, when the grant ended: the
    // time of the expiry itself, however long the stop took after it.
    let expired = f.store().events_of(&a, "lease_expired").unwrap();
    assert_eq!(expired.len(), 1);
    let after = expired[0].at_ms - f.rec(&a).accepted_at_ms;
    assert!((3_500..5_500).contains(&after), "{after} ms");
    assert_eq!(events(&f, &a, "agent_lease_expired"), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_renewal_before_the_start_seeds_the_supervisor_with_what_is_left_of_it() {
    let f = fx();
    let agent = f.start("boot-a");
    let c = f.connect(&agent).await;
    f.host.block_prepare.store(true, Ordering::SeqCst);
    let (script, marker, _) = stay(&f);
    let p = payload(&script);
    let a = AttemptId::generate();
    submit(&c, &a, &p).await;
    let accepted = remote_task_runner::platform::monotonic_ms();
    wait_for("preparation to begin", || {
        f.host.count(&format!("prepare {a}")) == 1
    });
    // Distinct clock ticks make the wrong acceptance/start origin fail
    // without requiring any operation to finish within a small budget.
    wait_for("a tick after acceptance", || {
        remote_task_runner::platform::monotonic_ms() > accepted
    });
    let before = remote_task_runner::platform::monotonic_ms();
    let Response::RenewLease(r) = call(&c, renew_req(&a, 1, p.lease_ms)).await.unwrap() else {
        panic!()
    };
    let after = remote_task_runner::platform::monotonic_ms();
    assert!(r.extended);
    assert_eq!(f.store().lease(&a).unwrap().unwrap().0.lease_seq, 1);
    assert!(f.store().lease_renewed_at(&a).unwrap().is_some());
    assert_eq!(lines(&marker), 0, "preparation still owns the start gate");
    wait_for("a tick after renewal", || {
        remote_task_runner::platform::monotonic_ms() > after
    });
    f.host.block_prepare.store(false, Ordering::SeqCst);
    wait_for("the command to start", || lines(&marker) == 1);
    let seed = seed_of(&f, &a);
    assert_eq!(seed["key"]["lease_seq"], 1);
    // The seed ends at the renewal's grant, regardless of how long the
    // real supervisor needed to start. It is neither the acceptance's
    // deadline nor a full lease counted from the supervisor's start.
    let end = seed["ends_mono_ms"].as_u64().unwrap();
    assert!(
        (before + p.lease_ms..=after + p.lease_ms).contains(&end),
        "seed {end}, renewal bracket {before}..={after}"
    );
    assert_eq!(events(&f, &a, "lease_expired"), 0);
    call(&c, cancel_req(&a)).await.unwrap();
    assert_eq!(f.wait_terminal(&a), Outcome::Cancelled);
}

fn renew_req(a: &AttemptId, seq: u64, lease_ms: u64) -> Request {
    Request::RenewLease(RenewLeaseRequest {
        schema_version: SchemaVersion::CURRENT,
        request_id: RequestId::generate(),
        coordinator_epoch: CoordinatorEpoch(1),
        attempt_id: a.clone(),
        generation: Generation(1),
        lease_seq: seq,
        lease_ms,
    })
}

fn seed_of(f: &Fx, a: &AttemptId) -> serde_json::Value {
    let spec = f
        .state()
        .join("attempts")
        .join(a.as_str())
        .join("supervise.json");
    let spec: serde_json::Value = serde_json::from_slice(&std::fs::read(spec).unwrap()).unwrap();
    spec["lease_seed"].clone()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_start_that_meets_a_renewal_half_done_waits_for_all_of_it() {
    let f = fx();
    let agent = f.start("boot-a");
    let c = f.connect(&agent).await;
    f.host.block_prepare.store(true, Ordering::SeqCst);
    let (script, marker, _) = stay(&f);
    let mut p = payload(&script);
    // As long as its neighbours': the supervisor of a busy machine needs
    // more than a moment to start the command within the renewed lease.
    p.lease_ms = 4_000;
    let a = AttemptId::generate();
    // The order, made by the probe and not by luck: the renewal is
    // committed (the new key is in the database), then the start comes to
    // measure the lease, and only then is the renewal's grant time recorded.
    let (measure_tx, measure_rx) = mpsc::channel::<()>();
    let measure_rx = Mutex::new(measure_rx);
    let host = f.host.clone();
    agent.set_probe(Arc::new(move |point, _| match point {
        ProbePoint::LeaseMeasure => {
            let _ = measure_tx.send(());
        }
        ProbePoint::LeaseRenewed => {
            host.block_prepare.store(false, Ordering::SeqCst);
            measure_rx
                .lock()
                .unwrap()
                .recv_timeout(T)
                .expect("the start came to measure the lease");
            // A start that is not kept out reads the new key with the first
            // grant's time now. Time for it to do that; a start that is
            // kept out does nothing however long this is.
            std::thread::sleep(Duration::from_millis(300));
        }
    }));
    submit(&c, &a, &p).await;
    // The first grant runs out during the preparation.
    std::thread::sleep(Duration::from_millis(4_300));
    let Response::RenewLease(r) = call(&c, renew_req(&a, 1, 4_000)).await.unwrap() else {
        panic!()
    };
    assert!(r.extended);
    // The renewed lease is neither ended by the agent nor cut short by the
    // seed: the command starts, and the seed is the renewal's.
    wait_for("the command to start", || lines(&marker) == 1);
    assert_eq!(events(&f, &a, "lease_expired"), 0);
    assert_eq!(seed_of(&f, &a)["key"]["lease_seq"], 1);
    let renewed_at = f.store().lease_renewed_at(&a).unwrap().unwrap();
    assert_eq!(f.wait_terminal(&a), Outcome::Interrupted);
    let expired = f.store().events_of(&a, "lease_expired").unwrap();
    assert_eq!(expired.len(), 1);
    let after = expired[0].at_ms - renewed_at;
    assert!((3_500..6_500).contains(&after), "{after} ms");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_lease_that_cannot_be_read_starts_nothing_until_it_can_be() {
    let f = fx();
    let agent = f.start("boot-a");
    let c = f.connect(&agent).await;
    f.host.block_prepare.store(true, Ordering::SeqCst);
    let (script, marker, _) = stay(&f);
    let a = AttemptId::generate();
    submit(&c, &a, &payload(&script)).await;
    let Response::RenewLease(r) = call(&c, renew_req(&a, 1, 60_000)).await.unwrap() else {
        panic!()
    };
    assert!(r.extended);
    // Only the lease cannot be read: the table is away, the rest of the
    // database answers.
    let db = rusqlite::Connection::open(f.state().join("worker.db")).unwrap();
    db.busy_timeout(T).unwrap();
    db.execute_batch("ALTER TABLE lease RENAME TO lease_away")
        .unwrap();
    assert!(f.store().lease(&a).is_err());
    f.host.block_prepare.store(false, Ordering::SeqCst);
    wait_for("the unreadable lease to be noted", || {
        events(&f, &a, "agent_lease_unreadable") == 1
    });
    // Several looks later (one every 200 ms): prepared, nothing started,
    // nothing given up, one entry in the journal.
    std::thread::sleep(Duration::from_millis(1_000));
    assert_eq!(f.rec(&a).state, AttemptState::Ready);
    assert_eq!(lines(&marker), 0);
    assert!(f.store().launches(&a).unwrap().is_empty());
    assert!(f.supervisors(&a).is_empty());
    assert_eq!(seed_of(&f, &a), serde_json::Value::Null);
    assert_eq!(events(&f, &a, "agent_lease_unreadable"), 1);
    assert_eq!(events(&f, &a, "agent_gave_up"), 0);
    db.execute_batch("ALTER TABLE lease_away RENAME TO lease")
        .unwrap();
    // The next look reads it: the start is seeded with the applied key.
    wait_for("the command to start", || lines(&marker) == 1);
    assert_eq!(seed_of(&f, &a)["key"]["lease_seq"], 1);
    call(&c, cancel_req(&a)).await.unwrap();
    assert_eq!(f.wait_terminal(&a), Outcome::Cancelled);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_renewal_after_the_start_is_observed_by_the_running_supervisor() {
    let f = fx();
    let agent = f.start("boot-a");
    let c = f.connect(&agent).await;
    let (script, marker, _) = stay(&f);
    let p = payload(&script);
    let a = AttemptId::generate();
    submit(&c, &a, &p).await;
    // The command can create its marker before the supervisor commits Running.
    // Wait for both observations before testing a renewal after that receipt.
    wait_for("the command and its running receipt", || {
        lines(&marker) == 1 && f.rec(&a).receipts.at_ms(Receipt::Running).is_some()
    });
    assert_eq!(seed_of(&f, &a)["key"], serde_json::Value::Null);
    let Response::RenewLease(r) = call(&c, renew_req(&a, 1, p.lease_ms)).await.unwrap() else {
        panic!()
    };
    assert!(r.extended);
    assert_eq!(f.store().lease(&a).unwrap().unwrap().0.lease_seq, 1);
    assert!(f.store().lease_renewed_at(&a).unwrap().is_some());
    // A durable ACK alone does not prove that the separate supervisor
    // process has read the new key and moved its clock. Its diagnostic does.
    wait_for("the supervisor to observe the renewal", || {
        events(&f, &a, "lease_renewal_observed") == 1
    });
    let observed = f.store().events_of(&a, "lease_renewal_observed").unwrap();
    assert_eq!(observed[0].payload["key"]["lease_seq"], 1);
    assert_eq!(f.rec(&a).state, AttemptState::Active);
    assert_eq!(events(&f, &a, "lease_expired"), 0);
    call(&c, cancel_req(&a)).await.unwrap();
    assert_eq!(f.wait_terminal(&a), Outcome::Cancelled);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_supervisor_that_cannot_be_started_is_tried_five_times_then_the_attempt_ends() {
    let f = fx();
    let mut cfg = f.cfg("boot-a");
    cfg.supervisor_exe = f.path("no-such-binary");
    cfg.retry_ms = 10;
    let agent = f.start_with(cfg);
    let c = f.connect(&agent).await;
    let a = AttemptId::generate();
    submit(&c, &a, &payload("true")).await;
    assert_eq!(f.wait_terminal(&a), Outcome::Interrupted);
    assert_eq!(events(&f, &a, "agent_spawn_failed"), 5);
    // `give_up` settles the attempt first and notes the event after: the
    // event is waited for, not read the moment the attempt is terminal.
    wait_for("the agent_gave_up event", || {
        events(&f, &a, "agent_gave_up") >= 1
    });
    assert_eq!(events(&f, &a, "agent_gave_up"), 1);
    wait_for("the cleanup", || f.host.count(&format!("cleanup {a}")) > 0);

    // The attempt ended without a result. While a supervisor of it may
    // live (its lock is held), one could still be stored: the
    // acknowledgement waits, and nothing is released.
    let Response::GetResult(result) = call(
        &c,
        Request::GetResult(GetResultRequest {
            schema_version: SchemaVersion::CURRENT,
            request_id: RequestId::generate(),
            attempt_id: a.clone(),
            generation: Generation(1),
        }),
    )
    .await
    .unwrap() else {
        panic!()
    };
    let ack = || {
        Request::AckCollected(AckCollectedRequest {
            schema_version: SchemaVersion::CURRENT,
            request_id: RequestId::generate(),
            attempt_id: a.clone(),
            generation: Generation(1),
            result_digest: result.result_digest.clone(),
        })
    };
    let held = remote_task_runner::supervisor::lock::SupervisorLock::try_acquire(&f.state(), &a)
        .unwrap()
        .expect("no supervisor lives");
    let e = call(&c, ack()).await.unwrap_err();
    assert_eq!(e.code, ErrorCode::Overloaded);
    assert!(f.rec(&a).acked_at_ms.is_none());
    drop(held);
    let Response::AckCollected(r) = call(&c, ack()).await.unwrap() else {
        panic!()
    };
    assert!(r.released);
    assert!(f.rec(&a).acked_at_ms.is_some());
    // And the lock is free again for whoever comes next.
    assert!(
        remote_task_runner::supervisor::lock::SupervisorLock::try_acquire(&f.state(), &a)
            .unwrap()
            .is_some()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_supervisor_that_cannot_tell_an_identity_is_started_again_and_never_given_up() {
    let f = fx();
    let mut cfg = f.cfg("boot-a");
    // A supervisor that only ever says "an identity could not be told".
    let stub = f.path("identity-unknown-supervisor");
    crate::common::write_executable(&stub, "#!/bin/sh\nexit 5\n");
    cfg.supervisor_exe = stub;
    cfg.retry_ms = 10;
    let agent = f.start_with(cfg);
    let c = f.connect(&agent).await;
    let a = AttemptId::generate();
    submit(&c, &a, &payload("true")).await;
    // Said at the first one and when the pause stops growing (the sixth):
    // well past the five failures after which a failing supervisor is
    // given up.
    wait_for("the longest pause", || {
        events(&f, &a, "agent_identity_unknown") == 2
    });
    assert_eq!(events(&f, &a, "agent_gave_up"), 0);
    assert_eq!(events(&f, &a, "agent_supervisor_exit"), 0);
    assert_ne!(f.rec(&a).state, AttemptState::Terminal);
    assert_eq!(f.rec(&a).termination, None);
    // It goes on, without another word in the log.
    std::thread::sleep(Duration::from_millis(700));
    assert_eq!(events(&f, &a, "agent_identity_unknown"), 2);
    assert_eq!(events(&f, &a, "agent_gave_up"), 0);
    assert_ne!(f.rec(&a).state, AttemptState::Terminal);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_lost_boot_record_next_to_live_attempts_counts_as_a_reboot() {
    let f = fx();
    let agent = f.start("boot-a");
    assert!(!agent.startup_report().boot_changed, "a first start");
    let c = f.connect(&agent).await;
    f.host.secrets_ready.store(false, Ordering::SeqCst);
    let a = AttemptId::generate();
    submit(&c, &a, &payload("true")).await;
    wait_for("the workspace record", || workspace_record(&f, &a).exists());
    agent.shutdown();
    std::fs::remove_file(f.state().join("boot_id")).unwrap();

    let agent = f.start("boot-a");
    assert!(agent.startup_report().boot_changed);
    assert_eq!(agent.startup_report().settled, vec![a.clone()]);
    assert_eq!(f.wait_terminal(&a), Outcome::Interrupted);
    agent.shutdown();
    // With nothing live and nothing launched, a missing record is a first
    // start again... but a launch intent anywhere keeps it a reboot.
    std::fs::remove_file(f.state().join("boot_id")).unwrap();
    assert!(!f.start("boot-a").startup_report().boot_changed);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn one_cleanup_of_an_attempt_at_a_time_and_a_failing_one_is_said_once() {
    let f = fx();
    let agent = f.start("boot-a");
    let c = f.connect(&agent).await;
    f.host.fail_cleanup.store(true, Ordering::SeqCst);
    let a = AttemptId::generate();
    submit(&c, &a, &payload("true")).await;
    assert_eq!(f.wait_terminal(&a), Outcome::Succeeded);
    wait_for("the first cleanup", || {
        events(&f, &a, "agent_cleanup_failed") == 1
    });
    for _ in 0..5 {
        agent.tick();
    }
    assert!(f.host.count(&format!("cleanup {a}")) >= 6);
    assert_eq!(events(&f, &a, "agent_cleanup_failed"), 1);

    // Now it works, slowly, and everybody comes at once.
    f.host.fail_cleanup.store(false, Ordering::SeqCst);
    f.host.slow_cleanup_ms.store(100, Ordering::SeqCst);
    let racers: Vec<_> = (0..6)
        .map(|i| {
            let agent = agent.clone();
            std::thread::spawn(move || {
                if i % 2 == 0 {
                    agent.clean_ended()
                } else {
                    agent.tick()
                }
            })
        })
        .collect();
    for r in racers {
        r.join().unwrap();
    }
    assert!(!f.host.overlap.load(Ordering::SeqCst));
    assert!(!f.host.ws_dir(&a).exists());
    assert!(!workspace_record(&f, &a).exists());

    // A tick during a tick returns at once instead of queueing behind it.
    f.host.fail_cleanup.store(true, Ordering::SeqCst);
    f.host.slow_cleanup_ms.store(600, Ordering::SeqCst);
    let b = AttemptId::generate();
    submit(&c, &b, &payload("true")).await;
    assert_eq!(f.wait_terminal(&b), Outcome::Succeeded);
    wait_for("the watcher's cleanup to end", || {
        events(&f, &b, "agent_cleanup_failed") == 1
    });
    let slow = {
        let agent = agent.clone();
        std::thread::spawn(move || agent.tick())
    };
    wait_for("the slow pass", || {
        !f.host.cleaning.lock().unwrap().is_empty()
    });
    let started = Instant::now();
    agent.tick();
    assert!(started.elapsed() < Duration::from_millis(300));
    slow.join().unwrap();
    f.host.fail_cleanup.store(false, Ordering::SeqCst);
    f.host.slow_cleanup_ms.store(0, Ordering::SeqCst);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_exited_supervisor_does_not_stay_a_zombie() {
    let f = fx();
    let agent = f.start("boot-a");
    let c = f.connect(&agent).await;
    let (script, _, pidfile) = stay(&f);
    let a = AttemptId::generate();
    submit(&c, &a, &payload(&script)).await;
    wait_for("the command", || read_pid(&pidfile).is_some());
    let supervisors = f.supervisors(&a);
    assert_eq!(supervisors.len(), 1);
    call(&c, retire_req(&a)).await.unwrap();
    f.wait_terminal(&a);
    // A zombie still answers a signal 0; a reaped process does not.
    wait_for("the supervisor to be reaped", || {
        agent.tick();
        !alive(supervisors[0])
    });
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_secret_bundle_for_a_workspace_that_is_not_there_yet_is_sent_again() {
    let f = fx();
    let agent = f.start("boot-a");
    let c = f.connect(&agent).await;
    f.host.block_prepare.store(true, Ordering::SeqCst);
    f.host.secrets_ready.store(false, Ordering::SeqCst);
    let a = AttemptId::generate();
    submit(&c, &a, &payload("true")).await;
    // Not `OVERLOADED`, and not the final `CONFLICT`.
    let e = call(&c, put_req(&a)).await.unwrap_err();
    assert_eq!(e.code, ErrorCode::NotFound);
    f.host.block_prepare.store(false, Ordering::SeqCst);
    wait_for("the workspace record", || workspace_record(&f, &a).exists());
    call(&c, cancel_req(&a)).await.unwrap();
    let e = call(&c, put_req(&a)).await.unwrap_err();
    assert_eq!(e.code, ErrorCode::Retired);
    assert_eq!(f.wait_terminal(&a), Outcome::Cancelled);
    assert_eq!(f.host.count(&format!("place {a}")), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_attempt_nobody_can_recover_is_unknown_until_a_retire_after_a_reboot() {
    let real = remote_task_runner::platform::boot_id().expect("this machine has a boot id");
    let f = fx();
    let mut cfg = f.cfg(&real);
    cfg.retire_wait_ms = 300;
    let agent = f.start_with(cfg.clone());
    let c = f.connect(&agent).await;
    let (script, marker, pidfile) = stay(&f);
    let a = AttemptId::generate();
    submit(&c, &a, &payload(&script)).await;
    wait_for("the command", || read_pid(&pidfile).is_some());
    wait_for("the running state", || {
        f.rec(&a).state == AttemptState::Active
    });
    agent.shutdown();
    f.kill_supervisors(&a);
    wait_for("the supervisor to die", || f.supervisors(&a).is_empty());
    std::fs::remove_file(
        f.state()
            .join("attempts")
            .join(a.as_str())
            .join("supervise.json"),
    )
    .unwrap();

    // Same boot: the command may still run, and nothing can recover it.
    let agent = f.start_with(cfg.clone());
    assert_eq!(agent.startup_report().unresolved, vec![a.clone()]);
    let c = f.connect(&agent).await;
    let get = || {
        Request::GetAttempt(GetAttemptRequest {
            schema_version: SchemaVersion::CURRENT,
            request_id: RequestId::generate(),
            attempt_id: a.clone(),
        })
    };
    let Response::GetAttempt(status) = call(&c, get()).await.unwrap() else {
        panic!()
    };
    assert_eq!(
        status.observation,
        remote_task_runner::state::Observation::Unknown
    );
    let Response::RetireAttempt(r) = call(&c, retire_req(&a)).await.unwrap() else {
        panic!()
    };
    assert_eq!(r.status, RetireStatus::Unconfirmed);
    assert_eq!(f.rec(&a).state, AttemptState::Active);
    agent.shutdown();

    // Another boot: whatever was launched is gone with the old boot.
    cfg.boot_id = WorkerBootId::parse("boot-next").unwrap();
    let agent = f.start_with(cfg);
    assert!(agent.startup_report().boot_changed);
    assert_eq!(agent.startup_report().unresolved, vec![a.clone()]);
    let c = f.connect(&agent).await;
    let Response::RetireAttempt(r) = call(&c, retire_req(&a)).await.unwrap() else {
        panic!()
    };
    assert_eq!(r.status, RetireStatus::Stopped);
    assert_eq!(f.wait_terminal(&a), Outcome::Interrupted);
    assert_eq!(events(&f, &a, "agent_settled_lost"), 1);
    let Response::GetAttempt(status) = call(&c, get()).await.unwrap() else {
        panic!()
    };
    assert_eq!(
        status.observation,
        remote_task_runner::state::Observation::Known
    );
    assert_eq!(lines(&marker), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn gc_writes_down_what_it_cannot_delete() {
    let f = fx();
    let mut cfg = f.cfg("boot-a");
    cfg.retention_ms = 0;
    let agent = f.start_with(cfg);
    let c = f.connect(&agent).await;
    let a = AttemptId::generate();
    submit(&c, &a, &payload("true")).await;
    f.wait_terminal(&a);
    agent
        .store()
        .add_blob_ref(
            &NewBlobRef {
                ref_id: format!("{a}/elsewhere"),
                attempt_id: a.to_string(),
                kind: BlobKind::Log,
                locator: "../elsewhere".into(),
            },
            1,
        )
        .unwrap();
    let Response::GetResult(result) = call(
        &c,
        Request::GetResult(GetResultRequest {
            schema_version: SchemaVersion::CURRENT,
            request_id: RequestId::generate(),
            attempt_id: a.clone(),
            generation: Generation(1),
        }),
    )
    .await
    .unwrap() else {
        panic!()
    };
    call(
        &c,
        Request::AckCollected(AckCollectedRequest {
            schema_version: SchemaVersion::CURRENT,
            request_id: RequestId::generate(),
            attempt_id: a.clone(),
            generation: Generation(1),
            result_digest: result.result_digest,
        }),
    )
    .await
    .unwrap();
    std::thread::sleep(Duration::from_millis(5));
    for _ in 0..3 {
        agent.gc().unwrap();
    }
    let said = f
        .store()
        .events_after(0, 10_000)
        .unwrap()
        .into_iter()
        .filter(|e| e.kind == "agent_gc_foreign_locator")
        .count();
    assert_eq!(said, 1, "still queued, said once");
    assert_eq!(f.store().pending_deletes().unwrap().len(), 1);
}

/// A stop and a secret bundle race the submit, each on its own connection.
/// Whatever the order: the attempt ends (or never exists), its command starts
/// at most once, and no workspace or secret file is left.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancel_and_retire_race_the_submit_and_the_preparation() {
    let f = fx();
    let agent = f.start("boot-a");
    let (c1, c2, c3) = (
        f.connect(&agent).await,
        f.connect(&agent).await,
        f.connect(&agent).await,
    );
    for i in 0..16 {
        let marker = f.path(&format!("race-{i}"));
        let a = AttemptId::generate();
        let mut p = payload(&format!("echo start >> {}; sleep 0.05", marker.display()));
        p.has_secret_bundle = i % 4 < 2;
        let stop = if i % 2 == 0 {
            cancel_req(&a)
        } else {
            retire_req(&a)
        };
        let (submitted, stopped, put) = tokio::join!(
            call(&c1, submit_req(&a, &p)),
            call(&c2, stop),
            call(&c3, put_req(&a)),
        );
        stopped.unwrap();
        match submitted {
            Ok(_) => {
                f.wait_terminal(&a);
            }
            // The tombstone was first.
            Err(e) => {
                assert_eq!(e.code, ErrorCode::Retired, "{e:?}");
                assert!(f.store().get(&a).unwrap().is_none());
            }
        }
        if let Err(e) = put {
            assert!(
                matches!(
                    e.code,
                    ErrorCode::NotFound | ErrorCode::Retired | ErrorCode::Conflict
                ),
                "{e:?}"
            );
        }
        wait_for("the workspace to go", || {
            !f.host.ws_dir(&a).exists() && !workspace_record(&f, &a).exists()
        });
        assert!(lines(&marker) <= 1);
        // A late resend never brings it back.
        match call(&c1, submit_req(&a, &p)).await {
            Ok(Response::SubmitAttempt(r)) => assert!(r.duplicate),
            Ok(other) => panic!("{other:?}"),
            Err(e) => assert_eq!(e.code, ErrorCode::Retired),
        }
        assert_eq!(f.store().count_nonterminal().unwrap(), 0);
    }
}

// ---------------------------------------------------------------- startup

#[test]
fn startup_runs_every_sweep_in_order_before_any_attempt_is_prepared() {
    let f = fx();
    // An attempt accepted by an agent that died before preparing it.
    let state = f.state();
    std::fs::create_dir_all(&state).unwrap();
    std::fs::set_permissions(&state, std::fs::Permissions::from_mode(0o755)).unwrap();
    let a = AttemptId::generate();
    {
        let store = WorkerStore::open(&state, &WorkerId::parse("w-test").unwrap()).unwrap();
        let p = payload("true");
        let req = SubmitAttemptRequest::new(
            RequestId::generate(),
            CoordinatorEpoch(1),
            a.clone(),
            p.clone(),
        )
        .unwrap();
        store
            .submit(
                remote_task_runner::storage::NewAcceptance {
                    attempt_id: a.clone(),
                    run_id: p.run_id.clone(),
                    task_id: p.task_id.clone(),
                    generation: p.generation,
                    payload_hash: req.payload_hash,
                    coordinator_epoch: CoordinatorEpoch(1),
                    spec: serde_json::to_value(&p).unwrap(),
                },
                remote_task_runner::platform::wall_ms(),
            )
            .unwrap();
    }
    // That agent had recorded the boot (without the record, an attempt in
    // the database means the record was lost: a reboot).
    std::fs::write(state.join("boot_id"), "boot-a").unwrap();
    let agent = f.start("boot-a");
    assert_eq!(agent.startup_report().steps, STARTUP_ORDER.to_vec());
    assert_eq!(agent.startup_report().watched, vec![a.clone()]);
    assert_eq!(f.wait_terminal(&a), Outcome::Succeeded);
    let calls = f.host.calls();
    assert_eq!(
        calls[..6],
        [
            "core_dumps".to_string(),
            "store_copies".into(),
            "vault".into(),
            "secrets".into(),
            "workspaces".into(),
            format!("prepare {a}"),
        ]
    );
    // The state directory is private whatever it was before.
    let mode = std::fs::metadata(&state).unwrap().permissions().mode() & 0o777;
    assert_eq!(mode, 0o700);
    assert_eq!(
        std::fs::read_to_string(state.join("boot_id")).unwrap(),
        "boot-a"
    );

    // One agent per state directory.
    let mut cfg = f.cfg("boot-a");
    cfg.lock_wait_ms = 300;
    let second = Agent::start(cfg, f.host.clone());
    assert_eq!(second.err().unwrap().code, ErrorCode::Conflict);
    // The refused start changed nothing: the sweeps did not run again.
    assert_eq!(f.host.count("store_copies"), 1);
}

#[test]
fn a_failing_startup_step_refuses_the_start_and_runs_nothing_after_it() {
    let f = fx();
    f.host.fail_core_dumps.store(true, Ordering::SeqCst);
    let e = Agent::start(f.cfg("boot-a"), f.host.clone()).err().unwrap();
    assert_eq!(e.code, ErrorCode::Internal);
    assert_eq!(f.host.calls(), vec!["core_dumps".to_string()]);
    assert!(!f.state().join("worker.db").exists());
    assert!(!f.state().join("boot_id").exists());
    assert_eq!(STARTUP_ORDER[1], Step::CoreDumps);
}

// ------------------------------------------------------------ real host

/// The real host, except for the process-wide core dump limit (this test
/// shares its process with the others).
struct RealButCore(remote_task_runner::worker::machine::Machine);

impl Host for RealButCore {
    fn disable_core_dumps(&self) -> Result<(), ApiError> {
        Ok(())
    }
    fn sweep_store_copies(&self) -> Result<(), ApiError> {
        self.0.sweep_store_copies()
    }
    fn sweep_vault(&self) -> Result<(), ApiError> {
        self.0.sweep_vault()
    }
    fn reconcile_secrets(&self, e: &dyn Fn(&AttemptId) -> bool) -> Result<(), ApiError> {
        self.0.reconcile_secrets(e)
    }
    fn sweep_workspaces(&self, h: &dyn Fn(&AttemptId) -> bool) -> Result<(), ApiError> {
        self.0.sweep_workspaces(h)
    }
    fn prepare(&self, a: &AcceptedAttempt, p: &AttemptPayload) -> Result<Workspace, ApiError> {
        self.0.prepare(a, p)
    }
    fn setup_poll(
        &self,
        a: &AcceptedAttempt,
        p: &AttemptPayload,
        w: &Workspace,
    ) -> Result<remote_task_runner::setup::builder::SetupPoll, ApiError> {
        self.0.setup_poll(a, p, w)
    }
    fn setup_release(&self, a: &AttemptId) {
        self.0.setup_release(a)
    }
    fn secret_paths(&self, p: &AttemptPayload) -> Result<Option<Vec<String>>, ApiError> {
        self.0.secret_paths(p)
    }
    fn place_secrets(
        &self,
        a: &AcceptedAttempt,
        ws: &Workspace,
        files: &[SecretFile],
    ) -> Result<u32, ApiError> {
        self.0.place_secrets(a, ws, files)
    }
    fn secrets_ready(&self, a: &AcceptedAttempt) -> Result<bool, ApiError> {
        self.0.secrets_ready(a)
    }
    fn placed_secrets(&self, a: &AcceptedAttempt) -> Result<Placed, ApiError> {
        self.0.placed_secrets(a)
    }
    fn cleanup(&self, a: &AttemptId, ws: Option<&Workspace>) -> Result<(), ApiError> {
        self.0.cleanup(a, ws)
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_real_host_runs_the_command_in_the_snapshot_of_the_run_and_removes_it() {
    use remote_task_runner::source::{capture_for_run, CaptureParams, SourceRequest, SourceStore};
    let f = fx();
    let state = f.state();
    let env: BTreeMap<String, String> = [
        ("PATH", "/usr/bin:/bin:/usr/local/bin:/opt/homebrew/bin"),
        ("HOME", f.path("home").to_str().unwrap()),
        ("GIT_CONFIG_GLOBAL", "/dev/null"),
        ("GIT_CONFIG_NOSYSTEM", "1"),
    ]
    .into_iter()
    .map(|(k, v)| (k.to_string(), v.to_string()))
    .collect();
    std::fs::create_dir_all(f.path("home")).unwrap();
    let plain = f.path("plain");
    std::fs::create_dir_all(plain.join("sub")).unwrap();
    std::fs::write(plain.join("sub/x.txt"), "from the snapshot\n").unwrap();

    // The source of the run is in the worker's store.
    let run = RunId::generate();
    let store = SourceStore::open(&state).unwrap();
    let captured = capture_for_run(
        &env,
        &store,
        &run,
        &SourceRequest::Directory { dir: plain.clone() },
        &CaptureParams::default(),
    )
    .unwrap();
    // An orphan of an attempt nobody knows: the startup sweep takes it.
    let orphan = state
        .join("workspaces")
        .join(AttemptId::generate().as_str());
    std::fs::create_dir_all(&orphan).unwrap();

    let machine =
        remote_task_runner::worker::machine::Machine::open(&state, env, state.join("cluster.key"))
            .unwrap();
    let agent = Agent::start(f.cfg("boot-a"), Arc::new(RealButCore(machine))).unwrap();
    f.agents.lock().unwrap().push(agent.clone());
    assert!(!orphan.exists());

    let c = f.connect(&agent).await;
    let out = f.path("out");
    let mut p = payload(&format!("pwd > {0}; cat x.txt >> {0}", out.display()));
    p.run_id = run;
    p.source_id = captured.manifest.source_id.clone();
    let digest = &captured.manifest.manifest_digest;
    p.manifest_digest = Digest::parse(digest)
        .or_else(|_| Digest::parse(&format!("sha256:{digest}")))
        .unwrap();
    p.commands[0].cwd = "sub".into();
    let a = AttemptId::generate();
    submit(&c, &a, &p).await;
    assert_eq!(f.wait_terminal(&a), Outcome::Succeeded);
    let text = std::fs::read_to_string(&out).unwrap();
    let mut it = text.lines();
    let cwd = PathBuf::from(it.next().unwrap());
    assert!(cwd.ends_with(format!("workspaces/{a}/sub")), "{cwd:?}");
    assert_eq!(it.next(), Some("from the snapshot"));
    // The original is untouched and the workspace goes at the end.
    wait_for("the workspace to go", || !cwd.exists());
    assert!(plain.join("sub/x.txt").exists());

    // A source that never arrived, and a source whose manifest is not the
    // one the attempt names, fail as such; nothing is started.
    let absent = payload("true");
    let mut other_manifest = p.clone();
    other_manifest.task_id = TaskId::generate();
    other_manifest.manifest_digest = Digest::of_bytes(b"another manifest");
    for q in [absent, other_manifest] {
        let b = AttemptId::generate();
        submit(&c, &b, &q).await;
        assert_eq!(f.wait_terminal(&b), Outcome::Failed);
        assert!(f.store().launches(&b).unwrap().is_empty());
        assert!(!state.join("workspaces").join(b.as_str()).exists());
    }

    // A run whose source has a secret file: the worker knows its path from
    // the manifest in its store, and refuses to keep it as an artifact.
    use local_ci::source::SecretSpec;
    use remote_task_runner::secrets::capture::{capture_run, SecretInputs};
    std::fs::write(plain.join("sub/.env"), "TOKEN=0123456789abcdef\n").unwrap();
    let run = RunId::generate();
    let key = remote_task_runner::secrets::key::ClusterKey::from_bytes(vec![5; 32]).unwrap();
    let with_secret = capture_run(
        &BTreeMap::new(),
        &store,
        &remote_task_runner::secrets::vault::Vault::open(&state).unwrap(),
        &SecretInputs {
            key: &key,
            declared: &[SecretSpec {
                glob: "sub/.env".into(),
                required: true,
            }],
        },
        &run,
        &SourceRequest::Directory { dir: plain.clone() },
        &CaptureParams::default(),
    )
    .unwrap();
    assert_eq!(with_secret.manifest.secret_files.len(), 1);
    let mut q = payload("true");
    q.run_id = run;
    q.source_id = with_secret.manifest.source_id.clone();
    let digest = &with_secret.manifest.manifest_digest;
    q.manifest_digest = Digest::parse(digest)
        .or_else(|_| Digest::parse(&format!("sha256:{digest}")))
        .unwrap();
    q.artifacts = vec![decl("env", "Sub/.env", MissingPolicy::Optional)];
    let b = AttemptId::generate();
    let e = call(&c, submit_req(&b, &q)).await.unwrap_err();
    assert_eq!(e.code, ErrorCode::InvalidInput, "{e:?}");
    assert!(f.store().get(&b).unwrap().is_none());
    // Another file of the same source is no secret: the declaration passes.
    q.artifacts = vec![decl("x", "sub/x.txt", MissingPolicy::Optional)];
    assert!(call(&c, submit_req(&b, &q)).await.is_ok());
    call(&c, cancel_req(&b)).await.unwrap();
    f.wait_terminal(&b);
}

/// The `forecast` operation is answered by the agent, with its own epoch and
/// boot id, and it writes nothing to the worker's store.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_agent_answers_a_forecast_and_writes_nothing() {
    let f = fx();
    let agent = f.start("boot-f");
    let c = f.connect(&agent).await;
    let a = AttemptId::generate();
    let n = NodeId::parse("main").unwrap();
    let before = (
        f.store().list_attempts(None, true, 100).unwrap().len(),
        f.store().events_after(0, 1000).unwrap().len(),
    );
    let req = |epoch: u64| {
        Request::Forecast(ForecastRequest {
            schema_version: SchemaVersion::CURRENT,
            request_id: RequestId::generate(),
            coordinator_epoch: CoordinatorEpoch(epoch),
            workload: WorkloadPlan {
                attempt_id: a.clone(),
                plan_digest: Digest::of_bytes(b"plan"),
                expansion_seq: 0,
                nodes: vec![WorkloadNode {
                    node_id: n.clone(),
                    depends_on: vec![],
                    priority: 0,
                    weight: Some(1),
                    predicted_rss_bytes: Some(1),
                    predicted_runtime_ms: Some(5_000),
                    external_delay_ms: None,
                    ready_in_ms: Some(0),
                    concurrency: ConcurrencyLimits::default(),
                    correlation: Correlation {
                        attempt_id: a.clone(),
                        node_id: n.clone(),
                        ledger_entry: None,
                    },
                    shared_builder: None,
                }],
            },
            pending: vec![],
        })
    };
    let Response::Forecast(r) = call(&c, req(0)).await.unwrap() else {
        panic!("a forecast answer")
    };
    assert_eq!(r.agent_epoch, agent.agent_epoch());
    assert_eq!(r.worker_boot_id.as_str(), "boot-f");
    assert_eq!(r.ttl_ms, 2000);
    assert_eq!(r.target.runtime_source, RuntimeSource::Measured);
    assert!(r.coverage.iter().any(|c| c.node_ids.contains(&n)));
    let after = (
        f.store().list_attempts(None, true, 100).unwrap().len(),
        f.store().events_after(0, 1000).unwrap().len(),
    );
    assert_eq!(before, after);
}

fn real_env(f: &Fx) -> BTreeMap<String, String> {
    std::fs::create_dir_all(f.path("home")).unwrap();
    [
        ("PATH", "/usr/bin:/bin:/usr/local/bin:/opt/homebrew/bin"),
        ("HOME", f.path("home").to_str().unwrap()),
        ("GIT_CONFIG_GLOBAL", "/dev/null"),
        ("GIT_CONFIG_NOSYSTEM", "1"),
    ]
    .into_iter()
    .map(|(k, v)| (k.to_string(), v.to_string()))
    .collect()
}

fn digest_of(manifest_digest: &str) -> Digest {
    Digest::parse(manifest_digest)
        .or_else(|_| Digest::parse(&format!("sha256:{manifest_digest}")))
        .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_real_host_knows_a_worktreeinclude_file_and_a_placed_secret_under_any_name() {
    use remote_task_runner::secrets::capture::{capture_run, SecretInputs};
    use remote_task_runner::secrets::send::batch_requests;
    use remote_task_runner::secrets::vault::Vault;
    use remote_task_runner::source::{CaptureParams, SourceRequest, SourceStore};
    const SECRET: &str = "s3cr3t-Value-0123456789";
    let f = fx();
    let state = f.state();
    let env = real_env(&f);
    let key = remote_task_runner::secrets::key::init(&state.join("cluster.key")).unwrap();
    let store = SourceStore::open(&state).unwrap();
    let vault = Vault::open(&state).unwrap();

    // 1. A repository whose `.worktreeinclude` names an ignored file. Nobody
    // declared it: it is a secret file of the manifest because the pattern
    // matched.
    let repo = f.path("repo");
    std::fs::create_dir_all(&repo).unwrap();
    let git = |args: &[&str]| {
        let out = std::process::Command::new("git")
            .args(["-c", "user.email=t@example.com", "-c", "user.name=t"])
            .args(["-c", "commit.gpgsign=false"])
            .args(args)
            .current_dir(&repo)
            .env_clear()
            .envs(&env)
            .output()
            .unwrap();
        assert!(out.status.success(), "git {args:?}: {out:?}");
    };
    git(&["init", "-q", "-b", "main"]);
    std::fs::write(repo.join(".gitignore"), "local.env\n").unwrap();
    std::fs::write(repo.join(".worktreeinclude"), "local.env\n").unwrap();
    std::fs::write(repo.join("tracked.txt"), "t\n").unwrap();
    git(&["add", "."]);
    git(&["commit", "-qm", "base"]);
    std::fs::write(repo.join("local.env"), format!("TOKEN={SECRET}\n")).unwrap();
    let run = RunId::generate();
    let included = capture_run(
        &env,
        &store,
        &vault,
        &SecretInputs {
            key: &key,
            declared: &[],
        },
        &run,
        &SourceRequest::Worktree { dir: repo.clone() },
        &CaptureParams::default(),
    )
    .unwrap();
    let secret_files: Vec<String> = included
        .manifest
        .secret_files
        .iter()
        .map(|s| s.path.display())
        .collect();
    assert_eq!(secret_files, ["local.env"]);

    let machine = remote_task_runner::worker::machine::Machine::open(
        &state,
        env.clone(),
        state.join("cluster.key"),
    )
    .unwrap();
    let agent = Agent::start(f.cfg("boot-a"), Arc::new(RealButCore(machine))).unwrap();
    f.agents.lock().unwrap().push(agent.clone());
    let c = f.connect(&agent).await;

    // Declaring it is refused at the acceptance, as what it is.
    let mut q = payload("true");
    q.run_id = run;
    q.source_id = included.manifest.source_id.clone();
    q.manifest_digest = digest_of(&included.manifest.manifest_digest);
    q.artifacts = vec![decl("env", "local.env", MissingPolicy::Optional)];
    let a = AttemptId::generate();
    let e = call(&c, submit_req(&a, &q)).await.unwrap_err();
    assert_eq!(e.code, ErrorCode::InvalidInput, "{e:?}");
    assert!(e.message.contains("secret files"), "{e:?}");
    assert!(f.store().get(&a).unwrap().is_none());
    // The same source with another manifest than the worker holds: the
    // declarations cannot be compared, and that is the answer.
    q.manifest_digest = Digest::of_bytes(b"another manifest");
    let e = call(&c, submit_req(&a, &q)).await.unwrap_err();
    assert_eq!(e.code, ErrorCode::Conflict, "{e:?}");
    assert!(f.store().get(&a).unwrap().is_none());

    // 2. A secret file placed by the real host: a hard link to it and a copy
    // of it, under names no declaration check knows, are not kept.
    let plain = f.path("plain");
    std::fs::create_dir_all(&plain).unwrap();
    std::fs::write(plain.join("x.txt"), "x\n").unwrap();
    std::fs::write(plain.join(".env"), format!("TOKEN={SECRET}\n")).unwrap();
    let run = RunId::generate();
    let declared = [local_ci::source::SecretSpec {
        glob: ".env".into(),
        required: true,
    }];
    let captured = capture_run(
        &BTreeMap::new(),
        &store,
        &vault,
        &SecretInputs {
            key: &key,
            declared: &declared,
        },
        &run,
        &SourceRequest::Directory { dir: plain.clone() },
        &CaptureParams::default(),
    )
    .unwrap();
    let bundle = vault
        .load(&run, &captured.manifest.secret_files, &key)
        .unwrap();
    let mut p = payload("ln .env leak.txt && cp .env copy.txt && cp x.txt fine.txt");
    p.run_id = run;
    p.source_id = captured.manifest.source_id.clone();
    p.manifest_digest = digest_of(&captured.manifest.manifest_digest);
    p.has_secret_bundle = true;
    p.artifacts = vec![
        decl("leak", "leak.txt", MissingPolicy::Required),
        decl("copy", "copy.txt", MissingPolicy::Required),
        decl("fine", "fine.txt", MissingPolicy::Required),
    ];
    let b = AttemptId::generate();
    submit(&c, &b, &p).await;
    wait_for("the workspace record", || workspace_record(&f, &b).exists());
    for r in batch_requests(&bundle, CoordinatorEpoch(1), &b, Generation(1)) {
        call(&c, r).await.unwrap();
    }
    assert_eq!(f.wait_terminal(&b), Outcome::Succeeded);
    let result = result_of(&c, &b).await;
    let names: Vec<&str> = result.artifacts.iter().map(|r| r.name.as_str()).collect();
    assert_eq!(names, ["fine"]);
    assert_eq!(
        not_stored(&result),
        [
            ("copy".to_string(), NotStoredReason::Refused),
            ("leak".to_string(), NotStoredReason::Refused),
        ]
    );
    refused_as_link_and_copy(&f, &b);
}

// ---------------------------------------------------------------- setup

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_commands_of_a_prepared_setup_are_left_out_and_the_others_run() {
    let f = fx();
    let agent = f.start("boot-a");
    let c = f.connect(&agent).await;
    f.host.setup_pending.store(true, Ordering::SeqCst);
    *f.host.setup_skip.lock().unwrap() = ["s1".to_string()].into();
    let order = f.path("order");
    let say = |name: &str| format!("echo {name} >> {}", order.display());
    let step = |c: CommandSpec, id: &str| CommandSpec {
        setup_step: Some(id.into()),
        ..c
    };
    let mut p = payload("true");
    p.commands = vec![
        step(command("install", &say("install"), &[]), "s1"),
        // a step the template does not hold stays
        step(command("tools", &say("tools"), &["install"]), "s2"),
        command("test", &say("test"), &["install", "tools"]),
    ];
    let a = AttemptId::generate();
    submit(&c, &a, &p).await;
    // while the builder works the attempt waits and nothing starts
    wait_for("the setup to be asked about", || {
        f.host.count(&format!("setup_poll {a}")) >= 3
    });
    assert_eq!(f.rec(&a).state, AttemptState::Preparing);
    assert!(!order.exists());
    f.host.setup_pending.store(false, Ordering::SeqCst);
    assert_eq!(f.wait_terminal(&a), Outcome::Succeeded);
    // `install` is in the workspace already: it does not run, and nothing
    // waits for it
    assert_eq!(std::fs::read_to_string(&order).unwrap(), "tools\ntest\n");
    let launched: Vec<String> = f
        .store()
        .launches(&a)
        .unwrap()
        .into_iter()
        .map(|l| l.0.to_string())
        .collect();
    assert_eq!(launched, ["tools", "test"]);
    // what the host said about a template it could not have is in the journal
    let events = f.store().events_of(&a, "agent_setup_unavailable").unwrap();
    assert_eq!(events.len(), 1);
    assert!(events[0].payload.to_string().contains("a note"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_submit_with_a_setup_that_is_not_valid_is_refused_before_it_is_stored() {
    use remote_task_runner::wire::{ReuseScope, SetupSpec, SetupStep};
    let f = fx();
    let agent = f.start("boot-a");
    let c = f.connect(&agent).await;
    let step = |id: &str, dir: &str| SetupStep {
        id: id.into(),
        dir: dir.into(),
        run: "true".into(),
        shell: None,
        env: BTreeMap::new(),
        unresolved_env: vec![],
    };
    let setup = |steps: Vec<SetupStep>| SetupSpec {
        steps,
        toolchain: vec![],
        scope: ReuseScope::Persistent,
        freshness_secs: None,
        secrets: vec![],
        no_cache: false,
        step_timeout_ms: None,
    };
    let a = AttemptId::generate();
    for (what, steps) in [
        ("the same id twice", vec![step("s", ""), step("s", "")]),
        ("a dir out of the workspace", vec![step("s", "../x")]),
    ] {
        let mut p = payload("true");
        p.setup = Some(setup(steps));
        let e = call(&c, submit_req(&a, &p)).await.expect_err(what);
        assert_eq!(e.code, ErrorCode::InvalidInput, "{what}: {e:?}");
    }
    assert!(f.store().get(&a).unwrap().is_none());
    assert_eq!(f.host.count(&format!("prepare {a}")), 0);
    // a valid one is accepted
    let mut p = payload("true");
    p.setup = Some(setup(vec![step("s", "web")]));
    assert!(!submit(&c, &a, &p).await.duplicate);
    f.wait_terminal(&a);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_attempt_whose_commands_are_all_prepared_succeeds_without_starting_one() {
    // An install-only task whose template is there: every command is left
    // out. It is a success with no exit status and no log, its declared
    // artifact is taken from the workspace, and the journal says why
    // nothing ran.
    let f = fx();
    let agent = f.start("boot-a");
    let c = f.connect(&agent).await;
    f.host.setup_pending.store(true, Ordering::SeqCst);
    *f.host.setup_skip.lock().unwrap() = ["s1".to_string(), "s2".to_string()].into();
    *f.host.secret_paths.lock().unwrap() = Some(Vec::new());
    let ran = f.path("ran");
    let say = format!("echo x >> {}", ran.display());
    let step = |c: CommandSpec, id: &str| CommandSpec {
        setup_step: Some(id.into()),
        ..c
    };
    let mut p = payload("true");
    p.commands = vec![
        step(command("install", &say, &[]), "s1"),
        step(command("tools", &say, &["install"]), "s2"),
    ];
    p.artifacts = vec![decl("lock", "made/lock.txt", MissingPolicy::Required)];
    let a = AttemptId::generate();
    submit(&c, &a, &p).await;
    wait_for("the setup to be asked about", || {
        f.host.count(&format!("setup_poll {a}")) >= 1
    });
    // what the template puts into the workspace
    let made = f.host.ws_dir(&a).join("made");
    std::fs::create_dir_all(&made).unwrap();
    std::fs::write(made.join("lock.txt"), "locked\n").unwrap();
    f.host.setup_pending.store(false, Ordering::SeqCst);
    assert_eq!(f.wait_terminal(&a), Outcome::Succeeded);
    assert!(!ran.exists(), "a prepared step was run");
    assert!(f.store().launches(&a).unwrap().is_empty());
    let result = result_of(&c, &a).await;
    assert_eq!(result.termination.outcome(), Outcome::Succeeded);
    assert_eq!(result.termination.exit(), None);
    assert!(result.nodes.is_empty());
    let names: Vec<&str> = result.artifacts.iter().map(|r| r.name.as_str()).collect();
    assert_eq!(names, ["lock"]);
    assert!(result.artifacts_not_stored.is_empty());
    let said = f.store().events_of(&a, "agent_setup_satisfied").unwrap();
    assert_eq!(said.len(), 1);
    assert_eq!(said[0].payload["steps"], serde_json::json!(["s1", "s2"]));
    assert_eq!(f.store().events_of(&a, "prepared_only").unwrap().len(), 1);
    // The parent collects it like any result: it reads the log of the first
    // declared command to its end, and that log is empty and complete.
    for stream in [LogStream::Stdout, LogStream::Stderr] {
        let Response::ReadLog(log) = call(
            &c,
            Request::ReadLog(ReadLogRequest {
                schema_version: SchemaVersion::CURRENT,
                request_id: RequestId::generate(),
                attempt_id: a.clone(),
                node_id: NodeId::parse("install").unwrap(),
                stream,
                offset: 0,
                max_bytes: 1024,
            }),
        )
        .await
        .unwrap() else {
            panic!()
        };
        assert!(log.eof && log.data.0.is_empty());
    }
    assert!(result.result_digest.to_string().starts_with("sha256:"));
    assert!(f
        .store()
        .events_of(&a, "agent_prepare_failed")
        .unwrap()
        .is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_attempt_cancelled_while_it_waits_for_its_setup_lets_go_of_the_builder() {
    let f = fx();
    let agent = f.start("boot-a");
    let c = f.connect(&agent).await;
    f.host.setup_pending.store(true, Ordering::SeqCst);
    let marker = f.path("starts");
    let p = payload(&format!("echo start >> {}", marker.display()));
    let a = AttemptId::generate();
    submit(&c, &a, &p).await;
    wait_for("the setup to be asked about", || {
        f.host.count(&format!("setup_poll {a}")) >= 2
    });
    let Response::CancelAttempt(_) = call(&c, cancel_req(&a)).await.unwrap() else {
        panic!()
    };
    assert_eq!(f.wait_terminal(&a), Outcome::Cancelled);
    assert!(f.host.count(&format!("setup_release {a}")) >= 1);
    assert_eq!(lines(&marker), 0);
    assert!(f.store().launches(&a).unwrap().is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_attempt_that_waited_too_long_for_its_setup_runs_every_command_itself() {
    let f = fx();
    let mut cfg = f.cfg("boot-a");
    cfg.prepare_timeout_ms = 300;
    let agent = f.start_with(cfg);
    let c = f.connect(&agent).await;
    f.host.setup_pending.store(true, Ordering::SeqCst);
    *f.host.setup_skip.lock().unwrap() = ["s1".to_string()].into();
    let marker = f.path("starts");
    let mut p = payload("true");
    let mut only = command(
        "install",
        &format!("echo start >> {}", marker.display()),
        &[],
    );
    only.setup_step = Some("s1".into());
    p.commands = vec![only];
    let a = AttemptId::generate();
    submit(&c, &a, &p).await;
    assert_eq!(f.wait_terminal(&a), Outcome::Succeeded);
    assert_eq!(lines(&marker), 1);
    assert!(f.host.count(&format!("setup_release {a}")) >= 1);
    let events = f.store().events_of(&a, "agent_setup_unavailable").unwrap();
    assert!(events[0]
        .payload
        .to_string()
        .contains("not ready within 300"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_real_host_prepares_a_setup_once_and_two_attempts_use_it() {
    use remote_task_runner::setup::builder::SetupConfig;
    use remote_task_runner::source::{capture_for_run, CaptureParams, SourceRequest, SourceStore};
    let f = fx();
    let state = f.state();
    let env: BTreeMap<String, String> = [
        ("PATH", "/usr/bin:/bin:/usr/local/bin:/opt/homebrew/bin"),
        ("HOME", f.path("home").to_str().unwrap()),
        ("GIT_CONFIG_GLOBAL", "/dev/null"),
        ("GIT_CONFIG_NOSYSTEM", "1"),
    ]
    .into_iter()
    .map(|(k, v)| (k.to_string(), v.to_string()))
    .collect();
    std::fs::create_dir_all(f.path("home")).unwrap();
    let plain = f.path("plain");
    std::fs::create_dir_all(&plain).unwrap();
    std::fs::write(plain.join("package.json"), "{}\n").unwrap();
    let run = RunId::generate();
    let store = SourceStore::open(&state).unwrap();
    let captured = capture_for_run(
        &env,
        &store,
        &run,
        &SourceRequest::Directory { dir: plain.clone() },
        &CaptureParams::default(),
    )
    .unwrap();
    let machine = remote_task_runner::worker::machine::Machine::open(
        &state,
        env.clone(),
        state.join("cluster.key"),
    )
    .unwrap();
    let mut setup = SetupConfig::new(
        state.clone(),
        WorkerId::parse("w-test").unwrap(),
        PathBuf::from(env!("CARGO_BIN_EXE_remote-task-runner")),
    );
    setup.poll_ms = 25;
    setup.lease_ms = 3_000;
    setup.term_grace_ms = 1_000;
    setup.kill_wait_ms = 2_000;
    setup.env = env;
    let machine = machine.with_setup(setup).unwrap();
    let setups = machine.setups().unwrap().clone();
    let agent = Agent::start(f.cfg("boot-a"), Arc::new(RealButCore(machine))).unwrap();
    f.agents.lock().unwrap().push(agent.clone());
    let c = f.connect(&agent).await;

    let (count, installed) = (f.path("count"), f.path("installed"));
    let setup_spec = SetupSpec {
        steps: vec![SetupStep {
            id: "i1".into(),
            dir: String::new(),
            run: format!(
                "echo x >> {}; mkdir -p node_modules; echo dep > node_modules/f; sleep 1",
                count.display()
            ),
            shell: None,
            env: BTreeMap::new(),
            unresolved_env: vec![],
        }],
        toolchain: vec![],
        scope: ReuseScope::Persistent,
        freshness_secs: None,
        secrets: vec![],
        no_cache: false,
        step_timeout_ms: None,
    };
    let make = |out: &Path| {
        let mut p = payload("true");
        p.run_id = run.clone();
        p.source_id = captured.manifest.source_id.clone();
        let digest = &captured.manifest.manifest_digest;
        p.manifest_digest = Digest::parse(digest)
            .or_else(|_| Digest::parse(&format!("sha256:{digest}")))
            .unwrap();
        let mut install = command(
            "install",
            &format!("echo ran >> {}", installed.display()),
            &[],
        );
        install.setup_step = Some("i1".into());
        let test = command(
            "test",
            &format!("cat node_modules/f > {}", out.display()),
            &["install"],
        );
        p.commands = vec![install, test];
        p.setup = Some(setup_spec.clone());
        p
    };
    let (out_a, out_b) = (f.path("out_a"), f.path("out_b"));
    let (a, b) = (AttemptId::generate(), AttemptId::generate());
    submit(&c, &a, &make(&out_a)).await;
    let mut pb = make(&out_b);
    pb.task_id = TaskId::generate();
    submit(&c, &b, &pb).await;
    assert_eq!(f.wait_terminal(&a), Outcome::Succeeded);
    assert_eq!(f.wait_terminal(&b), Outcome::Succeeded);
    // built once, copied into both workspaces; the install command did not run
    assert_eq!(setups.builds_started(), 1);
    assert_eq!(lines(&count), 1);
    assert_eq!(lines(&installed), 0);
    assert_eq!(std::fs::read_to_string(&out_a).unwrap(), "dep\n");
    assert_eq!(std::fs::read_to_string(&out_b).unwrap(), "dep\n");
    for x in [&a, &b] {
        let launched: Vec<String> = f
            .store()
            .launches(x)
            .unwrap()
            .into_iter()
            .map(|l| l.0.to_string())
            .collect();
        assert_eq!(launched, ["test"]);
    }
    // nothing of the builder is left: its workspace and directory are gone
    wait_for("the builder to be removed", || {
        std::fs::read_dir(state.join("setup/builders")).map_or(true, |rd| rd.count() == 0)
    });
    wait_for("the workspaces to go", || {
        std::fs::read_dir(state.join("workspaces")).map_or(true, |rd| rd.count() == 0)
    });
    assert!(f.supervisors_under(&state.join("setup")).is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bounded_terminal_cleanup_progresses_and_retries_without_touching_running_work() {
    let f = fx();
    let agent = f.start("boot-a");
    let c = f.connect(&agent).await;
    f.host.fail_cleanup.store(true, Ordering::SeqCst);
    let ended = AttemptId::generate();
    submit(&c, &ended, &payload("true")).await;
    assert_eq!(f.wait_terminal(&ended), Outcome::Succeeded);
    wait_for("failed cleanup", || {
        events(&f, &ended, "agent_cleanup_failed") == 1
    });
    f.host.secrets_ready.store(false, Ordering::SeqCst);
    let running = AttemptId::generate();
    submit(&c, &running, &payload("true")).await;
    wait_for("running workspace", || {
        workspace_record(&f, &running).exists()
    });
    let root = f.state().join("attempts");
    for i in 0..40 {
        std::fs::create_dir(root.join(format!("invalid-{i}"))).unwrap();
    }
    let before = f.host.count(&format!("cleanup {ended}"));
    assert_eq!(agent.cleanup_terminal(0), 0);
    assert_eq!(f.host.count(&format!("cleanup {ended}")), before);
    let mut retried = false;
    for _ in 0..100 {
        assert!(agent.cleanup_terminal(1) <= 1);
        retried |= f.host.count(&format!("cleanup {ended}")) > before;
    }
    assert!(
        retried,
        "a failed entry is visited again after the scan wraps"
    );
    f.host.fail_cleanup.store(false, Ordering::SeqCst);
    for _ in 0..100 {
        assert!(agent.cleanup_terminal(1) <= 1);
        if !workspace_record(&f, &ended).exists() {
            break;
        }
    }
    assert!(!workspace_record(&f, &ended).exists());
    assert!(!f.host.ws_dir(&ended).exists());
    assert!(workspace_record(&f, &running).exists());
    assert!(f.host.ws_dir(&running).exists());
    assert_eq!(f.host.count(&format!("cleanup {running}")), 0);
    // A cut-short spec write can remain without a workspace record. Removing
    // the spec must not lose a failed host cleanup from subsequent passes.
    let spec = root.join(ended.as_str()).join("supervise.json");
    std::fs::write(&spec, b"leftover masking values").unwrap();
    f.host.fail_cleanup.store(true, Ordering::SeqCst);
    let before = f.host.count(&format!("cleanup {ended}"));
    for _ in 0..100 {
        assert!(agent.cleanup_terminal(1) <= 1);
    }
    assert!(!spec.exists());
    assert!(f.host.count(&format!("cleanup {ended}")) > before + 1);
    let pending = root.join(ended.as_str()).join("cleanup-pending");
    assert!(pending.exists());
    agent.shutdown();
    let agent = f.start("boot-a");
    let before_restart_retry = f.host.count(&format!("cleanup {ended}"));
    for _ in 0..100 {
        agent.cleanup_terminal(1);
    }
    assert!(f.host.count(&format!("cleanup {ended}")) > before_restart_retry);
    f.host.fail_cleanup.store(false, Ordering::SeqCst);
    for _ in 0..100 {
        agent.cleanup_terminal(1);
    }
    assert!(!pending.exists());
    let cleaned = f.host.count(&format!("cleanup {ended}"));
    for _ in 0..100 {
        agent.cleanup_terminal(1);
    }
    assert_eq!(f.host.count(&format!("cleanup {ended}")), cleaned);
    assert!(f.rec(&ended).acked_at_ms.is_none());
}
