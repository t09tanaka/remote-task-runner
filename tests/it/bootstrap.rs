//! Real owner processes, all under private temporary state and environment.
use remote_task_runner::ids::{ClusterId, WorkerId};
use remote_task_runner::service::bootstrap::{ensure, Spec};
use remote_task_runner::service::resident::{Client, Operation, Reply};
use remote_task_runner::storage::CoordinatorStore;
use std::collections::BTreeMap;
use std::io::{Read, Write};
use std::os::unix::fs::PermissionsExt;
use std::process::{Command, Stdio};
use std::time::Duration;

fn fixture() -> (tempfile::TempDir, Spec) {
    let root = tempfile::Builder::new()
        .prefix("rtr-boot-")
        .tempdir_in(std::fs::canonicalize("/tmp").unwrap())
        .unwrap();
    std::fs::set_permissions(root.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    let state = root.path().join("parent");
    let worker = root.path().join("worker");
    for dir in [&state, &worker] {
        std::fs::create_dir(dir).unwrap();
        std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o700)).unwrap();
    }
    let key = root.path().join("key");
    remote_task_runner::secrets::key::init(&key).unwrap();
    let mut spec = Spec {
        state_dir: state,
        worker_state_dir: worker,
        worker_id: WorkerId::parse("persistent-local").unwrap(),
        cluster_key_path: key,
        executable: env!("CARGO_BIN_EXE_remote-task-runner").into(),
        env: BTreeMap::from([
            ("HOME".into(), root.path().display().to_string()),
            ("PATH".into(), "/usr/bin:/bin".into()),
            ("TOKIO_WORKER_THREADS".into(), "2".into()),
            ("GIT_CONFIG_GLOBAL".into(), "/dev/null".into()),
            ("GIT_CONFIG_NOSYSTEM".into(), "1".into()),
        ]),
        repo_root: None,
        expected_cluster: None,
    };
    // Instrumented CLI/daemon/supervisor descendants keep the harness's private
    // profile destination (including %p/%m), outside captured inputs and HOME.
    if let Some(profile) = std::env::var_os("LLVM_PROFILE_FILE") {
        spec.env.insert(
            "LLVM_PROFILE_FILE".into(),
            profile
                .into_string()
                .expect("coverage profile path is UTF-8"),
        );
    }
    (root, spec)
}
// Private fixture metadata only: never dump argv, env, paths, journal payloads,
// or arbitrary parse errors. Cap reads even when a child leaves malformed files.
fn diagnose_attempt(spec: &Spec, attempt: &remote_task_runner::ids::AttemptId) {
    use remote_task_runner::supervisor::spec::{command_id, SuperviseSpec};
    use task_traffic_controller::managed::Sidecar;
    fn read_json<T: serde::de::DeserializeOwned>(path: &std::path::Path) -> Option<T> {
        let file = std::fs::File::open(path).ok()?;
        if file.metadata().ok()?.len() > 256 * 1024 {
            return None;
        }
        serde_json::from_reader(file.take(256 * 1024)).ok()
    }
    let dir = spec
        .worker_state_dir
        .join("attempts")
        .join(attempt.as_str());
    let input = dir.join("supervise.json");
    let Some(supervise) = read_json::<SuperviseSpec>(&input) else {
        eprintln!("supervisor spec exists={} readable=false", input.exists());
        return;
    };
    let traffic = &supervise.traffic_state_dir;
    let ledger_path = traffic.join("ledger.json");
    let ledger = read_json::<serde_json::Value>(&ledger_path);
    eprintln!(
        "private TTC ledger exists={} readable={} entries={:?}",
        ledger_path.exists(),
        ledger.is_some(),
        ledger
            .as_ref()
            .and_then(|l| l.get("entries_v2").or_else(|| l.get("entries")))
            .and_then(|e| e.as_array())
            .map(Vec::len)
    );
    for index in 0..supervise.nodes.len().min(8) {
        let id = command_id(attempt, index);
        let sidecar_path = traffic.join("managed").join(format!("{id}.json"));
        let sidecar = read_json::<Sidecar>(&sidecar_path);
        eprintln!(
            "private TTC node={index} pipe_exists={} lease_exists={} sidecar_exists={} readable={}",
            remote_task_runner::supervisor::spool::logs_dir(&supervise.spool_dir(), attempt)
                .join(format!("node-{index}.pipe"))
                .exists(),
            traffic.join("leases").join(format!("{id}.lock")).exists(),
            sidecar_path.exists(),
            sidecar.is_some()
        );
        if let Some(sc) = sidecar {
            eprintln!(
                "private TTC phase={:?} owner_pid={} child_pid={:?} terminal={} descendants={}",
                sc.phase,
                sc.owner_pid,
                sc.child.map(|c| c.pid),
                sc.terminal.is_some(),
                sc.descendants.len()
            );
        }
    }
}
async fn shutdown(client: &Client) {
    assert!(matches!(
        client.request(Operation::Shutdown).await.unwrap(),
        Reply::Stopping
    ));
    tokio::time::timeout(Duration::from_secs(5), async {
        while client.socket_path().exists() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
}
fn load(spec: &Spec) -> Client {
    let store = CoordinatorStore::open_read_only(&spec.state_dir).unwrap();
    Client::load(&spec.state_dir, store.cluster_id().clone()).unwrap()
}

#[test]
#[ignore = "invoked as a dedicated launcher subprocess by the process regression"]
fn launcher_process() {
    let mut bytes = Vec::new();
    std::io::stdin().read_to_end(&mut bytes).unwrap();
    let spec: Spec = serde_json::from_slice(&bytes).unwrap();
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(ensure(spec)).unwrap();
    // Runtime teardown and launcher exit must leave the owner running.
}

#[tokio::test]
async fn owner_survives_launcher_exit_and_rotation_reconnects() {
    let (_root, spec) = fixture();
    let mut launcher = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "bootstrap::launcher_process",
            "--ignored",
            "--nocapture",
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    launcher
        .stdin
        .take()
        .unwrap()
        .write_all(&serde_json::to_vec(&spec).unwrap())
        .unwrap();
    let output = tokio::task::spawn_blocking(move || launcher.wait_with_output().unwrap())
        .await
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let client = load(&spec);
    let epoch = match client.request(Operation::Ping).await.unwrap() {
        Reply::Ready { epoch } => epoch,
        other => panic!("{other:?}"),
    };
    shutdown(&client).await;
    let mut again = spec.clone();
    again.expected_cluster = Some(client.cluster_id().clone());
    let fresh = ensure(again).await.unwrap();
    assert!(
        matches!(fresh.request(Operation::Ping).await.unwrap(), Reply::Ready { epoch: e } if e != epoch)
    );
    assert!(client.request(Operation::Ping).await.is_err());
    assert_eq!(fresh.cluster_id(), client.cluster_id());
    shutdown(&fresh).await;
}

#[tokio::test]
async fn concurrent_startup_has_one_owner_and_foreign_cluster_is_refused() {
    let (_root, spec) = fixture();
    let mut joins = tokio::task::JoinSet::new();
    for _ in 0..8 {
        let copy = spec.clone();
        joins.spawn(ensure(copy));
    }
    let mut clients = Vec::new();
    while let Some(result) = joins.join_next().await {
        clients.push(result.unwrap().unwrap());
    }
    let first = clients.first().unwrap();
    let before = first.request(Operation::Ping).await.unwrap();
    for client in &clients {
        assert_eq!(client.cluster_id(), first.cluster_id());
        assert_eq!(
            serde_json::to_value(client.request(Operation::Ping).await.unwrap()).unwrap(),
            serde_json::to_value(&before).unwrap()
        );
    }
    let mut foreign = spec.clone();
    foreign.expected_cluster = Some(ClusterId::generate());
    assert!(ensure(foreign)
        .await
        .err()
        .unwrap()
        .to_string()
        .contains("foreign"));
    shutdown(first).await;
}

#[tokio::test]
async fn unsafe_keys_and_tokens_are_rejected_without_changing_files() {
    let (_root, spec) = fixture();
    std::fs::set_permissions(
        &spec.cluster_key_path,
        std::fs::Permissions::from_mode(0o644),
    )
    .unwrap();
    assert!(ensure(spec.clone()).await.is_err());
    assert!(!spec.state_dir.join("coordinator.db").exists());
    std::fs::set_permissions(
        &spec.cluster_key_path,
        std::fs::Permissions::from_mode(0o600),
    )
    .unwrap();
    let key = std::fs::read(&spec.cluster_key_path).unwrap();
    std::fs::OpenOptions::new()
        .write(true)
        .open(&spec.cluster_key_path)
        .unwrap()
        .set_len(1 << 34)
        .unwrap();
    assert!(ensure(spec.clone())
        .await
        .err()
        .unwrap()
        .to_string()
        .contains("exceeds"));
    assert!(!spec.state_dir.join("coordinator.db").exists());
    std::fs::write(&spec.cluster_key_path, key).unwrap();
    let client = ensure(spec.clone()).await.unwrap();
    let token = client.socket_path().parent().unwrap().join("owner.token");
    let original = std::fs::read(&token).unwrap();
    std::fs::set_permissions(&token, std::fs::Permissions::from_mode(0o644)).unwrap();
    assert!(Client::load(&spec.state_dir, client.cluster_id().clone()).is_err());
    std::fs::set_permissions(&token, std::fs::Permissions::from_mode(0o600)).unwrap();
    let alias = token.with_extension("alias");
    std::fs::hard_link(&token, &alias).unwrap();
    assert!(Client::load(&spec.state_dir, client.cluster_id().clone()).is_err());
    std::fs::remove_file(alias).unwrap();
    let saved = token.with_extension("saved");
    std::fs::rename(&token, &saved).unwrap();
    std::os::unix::fs::symlink(&saved, &token).unwrap();
    assert!(Client::load(&spec.state_dir, client.cluster_id().clone()).is_err());
    std::fs::remove_file(&token).unwrap();
    std::fs::rename(saved, &token).unwrap();
    std::fs::write(&token, "bad-token").unwrap();
    assert!(Client::load(&spec.state_dir, client.cluster_id().clone()).is_err());
    std::fs::write(&token, original).unwrap();
    shutdown(&client).await;
}

#[tokio::test]
async fn retained_input_is_private_idempotent_and_restarts_through_shared_argv() {
    use remote_task_runner::service::bootstrap::{daemon_argv, write_input};
    let (root, spec) = fixture();
    let input = root.path().join("resident.json");
    let managed = write_input(&input, &spec).unwrap();
    let same = write_input(&input, &spec).unwrap();
    assert_eq!(
        (managed.device, managed.inode, &managed.sha256),
        (same.device, same.inode, &same.sha256)
    );
    assert_eq!(
        std::fs::metadata(&input).unwrap().permissions().mode() & 0o777,
        0o600
    );
    let mut changed = spec.clone();
    changed.worker_id = WorkerId::parse("different").unwrap();
    assert!(write_input(&input, &changed).is_err());
    let mut previous = None;
    for _ in 0..2 {
        let mut child = tokio::process::Command::new(&spec.executable)
            .args(daemon_argv(&input).unwrap())
            .kill_on_drop(true)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        let client = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                assert!(child.try_wait().unwrap().is_none(), "daemon exited early");
                if let Ok(store) = CoordinatorStore::open_read_only(&spec.state_dir) {
                    if let Ok(client) = Client::load(&spec.state_dir, store.cluster_id().clone()) {
                        if matches!(
                            client.request(Operation::Ping).await,
                            Ok(Reply::Ready { .. })
                        ) {
                            break client;
                        }
                    }
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .unwrap();
        assert_eq!(
            std::fs::read_to_string(spec.worker_state_dir.join("boot_id")).unwrap(),
            remote_task_runner::platform::boot_id().unwrap()
        );
        let worker =
            remote_task_runner::storage::WorkerStore::open_read_only(&spec.worker_state_dir)
                .unwrap();
        assert_eq!(worker.worker_id(), &spec.worker_id);
        let store = CoordinatorStore::open_read_only(&spec.state_dir).unwrap();
        if let Some(epoch) = previous {
            assert_ne!(store.epoch(), epoch);
        }
        previous = Some(store.epoch());
        shutdown(&client).await;
        assert!(child.wait().await.unwrap().success());
        assert!(input.exists());
    }
    std::fs::set_permissions(&input, std::fs::Permissions::from_mode(0o644)).unwrap();
    let failed = tokio::process::Command::new(&spec.executable)
        .args(daemon_argv(&input).unwrap())
        .stdin(Stdio::null())
        .output()
        .await
        .unwrap();
    assert!(!failed.status.success());
    assert!(write_input(&input, &spec).is_err());
}

#[tokio::test]
async fn failed_child_and_interrupted_startup_do_not_keep_startup_lock() {
    let (root, spec) = fixture();
    // System utilities may be symlinks, rejected before startup by the private
    // executable checks. Exercise an actual failed child on every platform.
    let executable = root.path().join("failed-child");
    let started = root.path().join("failed-child-started");
    std::fs::write(
        &executable,
        "#!/bin/sh\n: > \"$BOOTSTRAP_FAILED_MARKER\"\nexit 1\n",
    )
    .unwrap();
    std::fs::set_permissions(&executable, std::fs::Permissions::from_mode(0o700)).unwrap();
    let mut failed = spec.clone();
    failed.executable = executable;
    failed.env.insert(
        "BOOTSTRAP_FAILED_MARKER".into(),
        started.to_str().unwrap().into(),
    );
    let error = ensure(failed).await.err().expect("child must fail startup");
    assert!(started.exists(), "failed child did not start: {error:#}");
    let lock = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(spec.state_dir.join("startup.lock"))
        .unwrap();
    lock.lock().unwrap();
    let waiting = tokio::spawn(ensure(spec.clone()));
    tokio::time::sleep(Duration::from_millis(80)).await;
    waiting.abort();
    assert!(waiting.await.err().unwrap().is_cancelled());
    drop(lock);
    let client = ensure(spec).await.unwrap();
    shutdown(&client).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bounded_queries_cli_configured_fixture_does_not_advance_epoch() {
    use remote_task_runner::ids::{RunId, TaskId};
    use serde_json::json;
    let (root, spec) = fixture();
    let run = RunId::generate();
    let task = TaskId::generate();
    {
        let owner = CoordinatorStore::open(&spec.state_dir, 1).unwrap();
        owner
            .create_run(&run, None, None, &json!({"env":"PRIVATE_PAYLOAD"}), 1)
            .unwrap();
        owner
            .create_task(&task, &run, &json!({"argv":["PRIVATE_PAYLOAD"]}), 1)
            .unwrap();
        owner.close_run(&run, 2).unwrap();
    }
    let dir = root.path().join(".config/remote-task-runner");
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o700)).unwrap();
    remote_task_runner::service::bootstrap::write_input(&dir.join("coordinator.json"), &spec)
        .unwrap();
    // Simulate the pre-index schema with durable rows and no running owner.
    let db = rusqlite::Connection::open(spec.state_dir.join("coordinator.db")).unwrap();
    db.execute_batch(
        "DROP INDEX task_run_key; UPDATE schema_meta SET value='3' WHERE key='schema_version';",
    )
    .unwrap();
    let old_epoch: i64 = db
        .query_row("SELECT epoch FROM coordinator_state", [], |r| r.get(0))
        .unwrap();
    drop(db);
    for args in [
        vec!["--json", "ls"],
        vec!["--json", "show", run.as_str()],
        vec!["--json", "show", task.as_str()],
    ] {
        let output = Command::new(&spec.executable)
            .args(args)
            .current_dir(root.path())
            .env_clear()
            .envs(
                std::env::var_os("LLVM_PROFILE_FILE").map(|profile| ("LLVM_PROFILE_FILE", profile)),
            )
            .envs(&spec.env)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stdout)
        );
        assert!(output.stderr.is_empty());
    }
    assert!(
        !spec.state_dir.join("parent").exists(),
        "read-only CLI started owner IPC"
    );
    assert!(
        !spec.state_dir.join("startup.lock").exists(),
        "read-only CLI entered bootstrap"
    );
    let db = rusqlite::Connection::open_with_flags(
        spec.state_dir.join("coordinator.db"),
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
    )
    .unwrap();
    assert_eq!(
        db.query_row("SELECT epoch FROM coordinator_state", [], |r| r
            .get::<_, i64>(0))
            .unwrap(),
        old_epoch
    );
    assert_eq!(
        db.query_row(
            "SELECT value FROM schema_meta WHERE key='schema_version'",
            [],
            |r| r.get::<_, String>(0)
        )
        .unwrap(),
        "3"
    );
    assert_eq!(
        db.query_row(
            "SELECT count(*) FROM sqlite_master WHERE name='task_run_key'",
            [],
            |r| r.get::<_, i64>(0)
        )
        .unwrap(),
        0
    );
    drop(db);
    let client = ensure(spec.clone()).await.unwrap();
    let epoch = CoordinatorStore::open_read_only(&spec.state_dir)
        .unwrap()
        .epoch();
    for args in [
        vec!["--json", "ls", "--limit", "1"],
        vec!["--json", "show", run.as_str()],
        vec!["--json", "show", task.as_str()],
    ] {
        let output = Command::new(&spec.executable)
            .args(args)
            .current_dir(root.path())
            .env_clear()
            .envs(
                std::env::var_os("LLVM_PROFILE_FILE").map(|profile| ("LLVM_PROFILE_FILE", profile)),
            )
            .envs(&spec.env)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stdout)
        );
        assert!(output.stderr.is_empty());
        let text = String::from_utf8(output.stdout).unwrap();
        assert!(!text.contains("PRIVATE_PAYLOAD"));
        let value: serde_json::Value = serde_json::from_str(&text).unwrap();
        assert_eq!(value["ok"], true);
    }
    for (id, code) in [
        ("invalid".to_string(), 2),
        (RunId::generate().to_string(), 73),
    ] {
        let output = Command::new(&spec.executable)
            .args(["--json", "show", &id])
            .current_dir(root.path())
            .env_clear()
            .envs(
                std::env::var_os("LLVM_PROFILE_FILE").map(|profile| ("LLVM_PROFILE_FILE", profile)),
            )
            .envs(&spec.env)
            .output()
            .unwrap();
        assert_eq!(output.status.code(), Some(code));
        assert!(output.stderr.is_empty());
    }
    assert_eq!(
        CoordinatorStore::open_read_only(&spec.state_dir)
            .unwrap()
            .epoch(),
        epoch
    );
    shutdown(&client).await;
}

#[tokio::test]
async fn readonly_queries_refuse_foreign_cluster_and_unsafe_key_without_owner() {
    let (_root, mut spec) = fixture();
    let epoch = {
        let owner = CoordinatorStore::open(&spec.state_dir, 1).unwrap();
        owner.epoch()
    };
    spec.expected_cluster = Some(ClusterId::generate());
    assert!(
        remote_task_runner::service::query::Reader::open(spec.clone())
            .await
            .is_err()
    );
    spec.expected_cluster = None;
    std::fs::set_permissions(
        &spec.cluster_key_path,
        std::fs::Permissions::from_mode(0o644),
    )
    .unwrap();
    assert!(
        remote_task_runner::service::query::Reader::open(spec.clone())
            .await
            .is_err()
    );
    assert_eq!(
        CoordinatorStore::open_read_only(&spec.state_dir)
            .unwrap()
            .epoch(),
        epoch
    );
    assert!(!spec.state_dir.join("startup.lock").exists());
    assert!(!spec.state_dir.join("parent").exists());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn configured_cli_submit_captures_worktree_and_secrets_then_outlives_caller() {
    use remote_task_runner::ids::TaskId;
    use remote_task_runner::service::bootstrap::write_input;
    let (root, mut spec) = fixture();
    let repo = root.path().join("repo");
    std::fs::create_dir(&repo).unwrap();
    let git = |args: &[&str]| {
        let output = Command::new("git")
            .args(args)
            .current_dir(&repo)
            .env_clear()
            .envs(
                std::env::var_os("LLVM_PROFILE_FILE").map(|profile| ("LLVM_PROFILE_FILE", profile)),
            )
            .envs(&spec.env)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
    };
    git(&["init", "-q"]);
    git(&["config", "user.email", "fixture@example.invalid"]);
    git(&["config", "user.name", "Fixture"]);
    std::fs::write(repo.join("input.txt"), "committed").unwrap();
    std::fs::write(repo.join(".gitignore"), "private.env\n").unwrap();
    std::fs::write(
        repo.join(".remote-task-runner.toml"),
        "schema_version = 1\n[source]\nsecret_files = [{ glob = 'private.env', required = true }]\n",
    )
    .unwrap();
    git(&["add", "."]);
    git(&["commit", "-qm", "fixture"]);
    std::fs::write(repo.join("input.txt"), "dirty-snapshot").unwrap();
    let secret = "fixture-secret-value-abcdefghijkl";
    std::fs::write(repo.join("private.env"), format!("TOKEN={secret}\n")).unwrap();
    spec.env
        .insert("PATH".into(), std::env::var("PATH").unwrap());
    let configdir = root.path().join(".config/remote-task-runner");
    std::fs::create_dir_all(&configdir).unwrap();
    std::fs::set_permissions(&configdir, std::fs::Permissions::from_mode(0o700)).unwrap();
    write_input(&configdir.join("coordinator.json"), &spec).unwrap();
    let output=tokio::process::Command::new(&spec.executable).args(["--json","submit","--","/bin/sh","-c","set -e; sleep 1; test \"$(cat input.txt)\" = dirty-snapshot; test \"$(wc -c <private.env)\" -eq 40; cat private.env"])
        .current_dir(&repo).env_clear().envs(std::env::var_os("LLVM_PROFILE_FILE").map(|profile|("LLVM_PROFILE_FILE",profile))).envs(&spec.env).output().await.unwrap();
    assert!(
        output.status.success(),
        "{} {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let value: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(value["result"]["state"], "coordinator_accepted");
    assert!(!String::from_utf8_lossy(&output.stdout).contains(secret));
    let task = TaskId::parse(value["result"]["task_id"].as_str().unwrap()).unwrap();
    let second = tokio::process::Command::new(&spec.executable)
        .args(["--json", "submit", "--", "/usr/bin/true"])
        .current_dir(&repo)
        .env_clear()
        .envs(std::env::var_os("LLVM_PROFILE_FILE").map(|profile| ("LLVM_PROFILE_FILE", profile)))
        .envs(&spec.env)
        .output()
        .await
        .unwrap();
    assert!(
        second.status.success(),
        "{}",
        String::from_utf8_lossy(&second.stdout)
    );
    let second: serde_json::Value = serde_json::from_slice(&second.stdout).unwrap();
    assert_eq!(
        second["result"]["source_id"], value["result"]["source_id"],
        "same immutable input must be reusable"
    );
    let second_task = TaskId::parse(second["result"]["task_id"].as_str().unwrap()).unwrap();
    std::fs::write(repo.join("input.txt"), "changed-after-caller-exit").unwrap();
    std::fs::write(repo.join("private.env"), "changed-after-capture").unwrap();
    let client = load(&spec);
    let attempts = tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            if let Reply::Task { attempts } = client
                .request(Operation::Task {
                    task_id: task.clone(),
                })
                .await
                .unwrap()
            {
                if attempts
                    .iter()
                    .any(|a| a.state == remote_task_runner::state::AttemptState::Terminal)
                {
                    break attempts;
                }
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await;
    let attempts = match attempts {
        Ok(attempts) => attempts,
        Err(error) => {
            let snapshot = client
                .request(Operation::Task {
                    task_id: task.clone(),
                })
                .await;
            eprintln!("timed out task snapshot: {snapshot:?}");
            if let Ok(Reply::Task { attempts }) = &snapshot {
                let worker = remote_task_runner::storage::WorkerStore::open_read_only(
                    &spec.worker_state_dir,
                )
                .unwrap();
                for a in attempts.iter().take(8) {
                    diagnose_attempt(&spec, &a.attempt_id);
                    eprintln!(
                        "worker launch intents={:?}",
                        worker.launches(&a.attempt_id).map(|l| l.len())
                    );
                    if let Some(rec) = worker.get(&a.attempt_id).unwrap() {
                        eprintln!("worker state: {:?}", rec.state);
                    }
                    for event in worker.events_of_attempt(&a.attempt_id, 30).unwrap() {
                        eprintln!("worker event: {}", event.kind);
                    }
                }
            }
            shutdown(&client).await;
            panic!("terminal timeout: {error}");
        }
    };
    assert_eq!(attempts.len(), 1);
    assert_eq!(
        attempts[0].termination.as_ref().unwrap().outcome(),
        remote_task_runner::state::Outcome::Succeeded,
        "{attempts:?}"
    );
    let attempt_dir = spec
        .worker_state_dir
        .join("attempts")
        .join(attempts[0].attempt_id.as_str());
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if !attempt_dir.join("workspace.json").exists()
                && !attempt_dir.join("supervise.json").exists()
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .unwrap();
    let results =
        remote_task_runner::results::ResultStore::open(&spec.state_dir.join("results")).unwrap();
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if results
                .read_result(&attempts[0].attempt_id)
                .unwrap()
                .is_some()
                && results
                    .log_len(
                        &attempts[0].attempt_id,
                        remote_task_runner::wire::LogStream::Stdout,
                    )
                    .unwrap()
                    > 0
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .unwrap();
    let log = results
        .read_log(
            &attempts[0].attempt_id,
            remote_task_runner::wire::LogStream::Stdout,
        )
        .unwrap();
    assert!(!String::from_utf8_lossy(&log).contains(secret));
    assert!(
        !log.is_empty(),
        "collected masked stdout must survive workspace cleanup"
    );
    let sources = remote_task_runner::source::SourceStore::open(&spec.state_dir).unwrap();
    let id =
        remote_task_runner::ids::RunId::parse(value["result"]["run_id"].as_str().unwrap()).unwrap();
    let ci = sources.source_of_run(&id).unwrap().unwrap();
    let manifest = sources.manifest(&ci).unwrap().unwrap();
    assert!(!manifest
        .object_manifest
        .entries
        .iter()
        .any(|e| e.path.0 == b"private.env"));
    assert_eq!(manifest.secret_files.len(), 1);
    let vault = remote_task_runner::secrets::vault::Vault::open(&spec.state_dir).unwrap();
    let key = remote_task_runner::secrets::key::load(&spec.cluster_key_path).unwrap();
    assert!(vault.has(&id));
    assert_eq!(
        vault.load(&id, &manifest.secret_files, &key).unwrap().files[0]
            .content
            .0,
        format!("TOKEN={secret}\n").as_bytes()
    );
    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            if let Reply::Task { attempts } = client
                .request(Operation::Task {
                    task_id: second_task.clone(),
                })
                .await
                .unwrap()
            {
                if let Some(a) = attempts
                    .iter()
                    .find(|a| a.state == remote_task_runner::state::AttemptState::Terminal)
                {
                    if a.termination.as_ref().unwrap().outcome()
                        != remote_task_runner::state::Outcome::Succeeded
                    {
                        eprintln!("second attempt: {a:?}");
                        let worker = remote_task_runner::storage::WorkerStore::open_read_only(
                            &spec.worker_state_dir,
                        )
                        .unwrap();
                        for event in worker.events_of_attempt(&a.attempt_id, 40).unwrap() {
                            eprintln!(
                                "second event {}: code={:?} message={:?}",
                                event.kind,
                                event.payload.get("code"),
                                event.payload.get("message")
                            );
                        }
                        eprintln!(
                            "second result: {:?}",
                            worker.get_result(&a.attempt_id).unwrap()
                        );
                        shutdown(&client).await;
                        panic!("same-source second attempt failed");
                    }
                    break;
                }
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .unwrap();
    let count_runs = || {
        let db = rusqlite::Connection::open_with_flags(
            spec.state_dir.join("coordinator.db"),
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
        )
        .unwrap();
        db.query_row("SELECT COUNT(*) FROM run", [], |row| row.get::<_, i64>(0))
            .unwrap()
    };
    let before = count_runs();
    std::fs::remove_file(repo.join("private.env")).unwrap();
    let failed = tokio::process::Command::new(&spec.executable)
        .args(["--json", "submit", "--", "/usr/bin/true"])
        .current_dir(&repo)
        .env_clear()
        .envs(std::env::var_os("LLVM_PROFILE_FILE").map(|profile| ("LLVM_PROFILE_FILE", profile)))
        .envs(&spec.env)
        .output()
        .await
        .unwrap();
    assert!(!failed.status.success());
    let error: serde_json::Value = serde_json::from_slice(&failed.stdout).unwrap();
    assert_eq!(error["ok"], false);
    assert_eq!(count_runs(), before, "failed capture dispatched a run");
    shutdown(&client).await;
}

#[test]
fn command_capture_directory_is_bound_once_and_source_view_reuses_it() {
    use remote_task_runner::service::{context::Context, input};
    let (root, spec) = fixture();
    let dir = root.path().join("directory");
    std::fs::create_dir(&dir).unwrap();
    std::fs::write(dir.join("input"), "first immutable input").unwrap();
    let mut context = Context::load(spec.env.clone(), dir.clone(), spec).unwrap();
    let run = remote_task_runner::ids::RunId::generate();
    let first = input::capture(&context, &run, None).unwrap();
    std::fs::write(dir.join("input"), "later content").unwrap();
    let second = input::capture(&context, &run, None).unwrap();
    assert_eq!(first.manifest.source_id, second.manifest.source_id);
    let view = root.path().join("view");
    input::source_view(&context, &run, &view).unwrap();
    assert_eq!(
        std::fs::read_to_string(view.join("input")).unwrap(),
        "first immutable input"
    );
    assert_eq!(
        std::fs::read_to_string(dir.join("input")).unwrap(),
        "later content"
    );
    assert!(
        !context.state_dir().join("coordinator.db").exists(),
        "capture took writable owner state"
    );
    assert!(input::capture(
        &context,
        &remote_task_runner::ids::RunId::generate(),
        Some("HEAD")
    )
    .is_err());
    context.config.source_mode.value = remote_task_runner::config::model::SourceMode::Commit;
    assert!(input::capture(&context, &remote_task_runner::ids::RunId::generate(), None).is_err());
    let mut invalid_env = context.env.clone();
    invalid_env.insert(
        "GIT_DIR".into(),
        root.path().join("absent-git-dir").display().to_string(),
    );
    assert!(
        Context::load(invalid_env, dir, context.bootstrap.clone()).is_err(),
        "invalid Git context must not become a directory snapshot"
    );
}

#[test]
fn uninitialized_submit_is_typed_without_creating_config_or_state() {
    let root = tempfile::tempdir().unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_remote-task-runner"))
        .args(["--json", "submit", "--", "true"])
        .env_clear()
        .envs(std::env::var_os("LLVM_PROFILE_FILE").map(|profile| ("LLVM_PROFILE_FILE", profile)))
        .env("HOME", root.path())
        .env("PATH", "/usr/bin:/bin")
        .current_dir(root.path())
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(output.stderr.is_empty());
    let value: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(value["error"]["code"], "NOT_FOUND");
    assert!(value["error"]["message"].as_str().unwrap().contains("init"));
    assert_eq!(std::fs::read_dir(root.path()).unwrap().count(), 0);
}

#[test]
fn command_commit_capture_requires_and_preserves_explicit_revision() {
    use remote_task_runner::service::{context::Context, input};
    let (root, spec) = fixture();
    let repo = root.path().join("commit-repo");
    std::fs::create_dir(&repo).unwrap();
    let git = |args: &[&str]| {
        let output = Command::new("git")
            .args(args)
            .current_dir(&repo)
            .env_clear()
            .envs(
                std::env::var_os("LLVM_PROFILE_FILE").map(|profile| ("LLVM_PROFILE_FILE", profile)),
            )
            .envs(&spec.env)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8(output.stdout).unwrap()
    };
    git(&["init", "-q"]);
    git(&["config", "user.name", "Fixture"]);
    git(&["config", "user.email", "fixture@example.invalid"]);
    std::fs::write(repo.join("input"), "explicit old revision").unwrap();
    git(&["add", "."]);
    git(&["commit", "-qm", "first"]);
    let old = git(&["rev-parse", "HEAD"]).trim().to_string();
    std::fs::write(repo.join("input"), "new HEAD").unwrap();
    git(&["commit", "-qam", "second"]);
    std::fs::write(repo.join("input"), "dirty worktree").unwrap();
    let mut context = Context::load(spec.env.clone(), repo.clone(), spec).unwrap();
    context.config.source_mode.value = remote_task_runner::config::model::SourceMode::Commit;
    assert!(input::capture(&context, &remote_task_runner::ids::RunId::generate(), None).is_err());
    let run = remote_task_runner::ids::RunId::generate();
    let captured = input::capture(&context, &run, Some(&old)).unwrap();
    assert_eq!(
        captured.manifest.original_head.as_deref(),
        Some(old.as_str())
    );
    let view = root.path().join("commit-view");
    input::source_view(&context, &run, &view).unwrap();
    assert_eq!(
        std::fs::read_to_string(view.join("input")).unwrap(),
        "explicit old revision"
    );
    assert_eq!(
        std::fs::read_to_string(repo.join("input")).unwrap(),
        "dirty worktree"
    );
}
