//! Private IPC and real local-worker composition; no process bootstrap claim.
use remote_task_runner::config::runtime::WorkerInputs;
use remote_task_runner::config::{resolve, Inputs as ConfigInputs, Overlay, ResolvedConfig};
use remote_task_runner::coordinator::TaskSpec;
use remote_task_runner::ids::*;
use remote_task_runner::platform::wall_ms;
use remote_task_runner::results::CollectConfig;
use remote_task_runner::service::resident::{
    Inputs, Operation, Reply, Request, Resident, MAX_REQUEST_BYTES,
};
use remote_task_runner::source::{capture_for_run, CaptureParams, SourceRequest, SourceStore};
use remote_task_runner::state::Outcome;
use remote_task_runner::storage::CoordinatorStore;
use remote_task_runner::wire::{CommandSpec, LogStream};
use std::collections::BTreeMap;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixStream;

fn root() -> tempfile::TempDir {
    tempfile::Builder::new()
        .prefix("rtr-res-")
        .tempdir_in(std::fs::canonicalize("/tmp").unwrap())
        .unwrap()
}
fn config() -> ResolvedConfig {
    let mut c = resolve(&ConfigInputs {
        overlay: &Overlay::default(),
        project: None,
        legacy: None,
        user: None,
    })
    .unwrap();
    c.heartbeat_seconds.value = 1;
    c.stale_seconds.value = 3;
    c
}
fn inputs(root: &Path) -> Inputs {
    remote_task_runner::secrets::key::init(&root.join("key")).unwrap();
    Inputs {
        state_dir: root.join("parent-state"),
        worker: WorkerInputs {
            state_dir: root.join("worker"),
            worker_id: WorkerId::parse("local").unwrap(),
            boot_id: WorkerBootId::parse("boot-test").unwrap(),
            supervisor_exe: env!("CARGO_BIN_EXE_remote-task-runner").into(),
            cluster_key_path: root.join("key"),
            env: BTreeMap::from([
                ("PATH".into(), "/usr/bin:/bin".into()),
                ("HOME".into(), root.display().to_string()),
                ("GIT_CONFIG_GLOBAL".into(), "/dev/null".into()),
                ("GIT_CONFIG_NOSYSTEM".into(), "1".into()),
            ]),
        },
        collection_interval: Duration::from_millis(30),
        collection: CollectConfig::default(),
    }
}
async fn stop(
    client: &remote_task_runner::service::resident::Client,
    task: tokio::task::JoinHandle<anyhow::Result<()>>,
) {
    assert!(matches!(
        client.request(Operation::Shutdown).await.unwrap(),
        Reply::Stopping
    ));
    tokio::time::timeout(Duration::from_secs(15), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
}
async fn raw(path: &Path, request: &Request) -> UnixStream {
    let mut stream = UnixStream::connect(path).await.unwrap();
    let bytes = serde_json::to_vec(request).unwrap();
    stream.write_u32(bytes.len() as u32).await.unwrap();
    stream.write_all(&bytes).await.unwrap();
    stream
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn resident_socket_keeps_accepted_work_running_after_submitter_disconnects() {
    let root = root();
    let input = inputs(root.path());
    std::fs::create_dir(&input.worker.state_dir).unwrap();
    std::fs::set_permissions(
        &input.worker.state_dir,
        std::fs::Permissions::from_mode(0o700),
    )
    .unwrap();
    let source = root.path().join("source");
    std::fs::create_dir(&source).unwrap();
    std::fs::write(source.join("input.txt"), "snapshot\n").unwrap();
    let run = RunId::generate();
    // Source forwarding is outside this layer: seed the immutable worker store.
    let store = SourceStore::open(&input.worker.state_dir).unwrap();
    let captured = capture_for_run(
        &input.worker.env,
        &store,
        &run,
        &SourceRequest::Directory { dir: source },
        &CaptureParams::default(),
    )
    .unwrap();
    let resident = Resident::start(&config(), input).await.unwrap();
    let client = resident.client();
    let runner = tokio::spawn(resident.run());
    let task = TaskId::generate();
    let manifest = &captured.manifest.manifest_digest;
    let spec = TaskSpec {
        setup: None,
        plan_digest: Digest::of_bytes(b"plan"),
        manifest_digest: Digest::parse(manifest)
            .or_else(|_| Digest::parse(&format!("sha256:{manifest}")))
            .unwrap(),
        commands: vec![CommandSpec {
            setup_step: None,
            node_id: NodeId::parse("main").unwrap(),
            argv: vec![
                "/bin/sh".into(),
                "-c".into(),
                "sleep 1; cat input.txt".into(),
            ],
            cwd: String::new(),
            env: BTreeMap::from([("PATH".into(), "/usr/bin:/bin".into())]),
            depends_on: vec![],
            timeout_ms: None,
        }],
        command_timeout_ms: None,
        deadline_ms: None,
        resource_wait_timeout_ms: None,
        has_secret_bundle: false,
        artifacts: vec![],
    };
    let submitter = client.clone();
    let reply = submitter
        .request(Operation::Submit {
            run_id: run.clone(),
            source_id: captured.manifest.source_id,
            tasks: vec![(task.clone(), spec)],
        })
        .await
        .unwrap();
    assert!(
        matches!(&reply, Reply::Accepted { run_id } if run_id == &run),
        "{reply:?}"
    );
    drop(submitter); // The submitting connection has already closed.
    let attempt = tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            if let Reply::Task { attempts } = client
                .request(Operation::Task {
                    task_id: task.clone(),
                })
                .await
                .unwrap()
            {
                if let Some(a) = attempts.into_iter().next() {
                    if a.receipts.collected_at_ms.is_some() {
                        break a;
                    }
                    if a.termination
                        .is_some_and(|t| t.outcome() != Outcome::Succeeded)
                    {
                        panic!("unexpected termination: {:?}", a.termination);
                    }
                }
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(attempt.termination.unwrap().outcome(), Outcome::Succeeded);
    stop(&client, runner).await;
    let files =
        remote_task_runner::results::ResultStore::open(&root.path().join("parent-state/results"))
            .unwrap();
    assert_eq!(
        files
            .read_log(&attempt.attempt_id, LogStream::Stdout)
            .unwrap(),
        b"snapshot\n"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn resident_has_one_writable_owner_and_client_queries_never_advance_epoch() {
    let root = root();
    let resident = Resident::start(&config(), inputs(root.path()))
        .await
        .unwrap();
    let client = resident.client();
    let state = root.path().join("parent-state");
    let epoch = CoordinatorStore::open_read_only(&state).unwrap().epoch();
    assert!(Resident::start(&config(), inputs(root.path()))
        .await
        .is_err());
    assert!(CoordinatorStore::open(&state, wall_ms()).is_err());
    let runner = tokio::spawn(resident.run());
    for _ in 0..8 {
        assert!(
            matches!(client.request(Operation::Task { task_id: TaskId::generate() }).await.unwrap(), Reply::Task { attempts } if attempts.is_empty())
        );
    }
    assert_eq!(
        CoordinatorStore::open_read_only(&state).unwrap().epoch(),
        epoch
    );
    stop(&client, runner).await;
    let restarted = Resident::start(&config(), inputs(root.path()))
        .await
        .unwrap();
    assert_ne!(
        CoordinatorStore::open_read_only(&state).unwrap().epoch(),
        epoch
    );
    drop(restarted);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn resident_rejects_foreign_credentials_oversized_frames_and_schema() {
    let root = root();
    let resident = Resident::start(&config(), inputs(root.path()))
        .await
        .unwrap();
    let client = resident.client();
    assert_eq!(
        std::fs::metadata(client.socket_path())
            .unwrap()
            .permissions()
            .mode()
            & 0o777,
        0o600
    );
    assert_eq!(
        std::fs::metadata(client.socket_path().parent().unwrap())
            .unwrap()
            .permissions()
            .mode()
            & 0o777,
        0o700
    );
    let token = std::fs::read_to_string(client.socket_path().parent().unwrap().join("owner.token"))
        .unwrap();
    let runner = tokio::spawn(resident.run());
    for (version, cluster, owner) in [
        (1, ClusterId::generate(), token.clone()),
        (1, client.cluster_id().clone(), "wrong".into()),
        (2, client.cluster_id().clone(), token),
    ] {
        let mut stream = raw(
            client.socket_path(),
            &Request {
                schema_version: version,
                cluster_id: cluster,
                owner_token: owner,
                operation: Operation::Shutdown,
            },
        )
        .await;
        let mut byte = [0];
        assert_eq!(stream.read(&mut byte).await.unwrap(), 0);
    }
    let mut stream = UnixStream::connect(client.socket_path()).await.unwrap();
    stream
        .write_u32(MAX_REQUEST_BYTES as u32 + 1)
        .await
        .unwrap();
    let mut byte = [0];
    assert_eq!(stream.read(&mut byte).await.unwrap(), 0);
    assert!(matches!(
        client
            .request(Operation::Task {
                task_id: TaskId::generate()
            })
            .await
            .unwrap(),
        Reply::Task { .. }
    ));
    stop(&client, runner).await;
}

#[tokio::test]
async fn resident_rejects_symlinked_or_shared_state_before_opening_database() {
    let root = root();
    let target = root.path().join("target");
    std::fs::create_dir(&target).unwrap();
    let mut input = inputs(root.path());
    std::os::unix::fs::symlink(&target, &input.state_dir).unwrap();
    assert!(Resident::start(&config(), input).await.is_err());
    assert!(!target.join("coordinator.db").exists());
    input = inputs(root.path());
    input.state_dir = target;
    assert!(Resident::start(&config(), input).await.is_err());
}

#[tokio::test(flavor = "current_thread")]
async fn resident_startup_does_not_block_the_executor_during_git_preflight() {
    use std::io::{Read, Write};
    let root = root();
    let bin = root.path().join("bin");
    std::fs::create_dir(&bin).unwrap();
    let ready = bin.join("ready");
    let release = bin.join("release");
    assert!(std::process::Command::new("mkfifo")
        .arg(&ready)
        .arg(&release)
        .status()
        .unwrap()
        .success());
    let git = bin.join("git");
    std::fs::write(&git, "#!/bin/sh\ndir=${0%/*}\nprintf 'ready\\n' > \"$dir/ready\"\nread -r released < \"$dir/release\"\nprintf 'git version 2.50.0\\n'\n").unwrap();
    std::fs::set_permissions(&git, std::fs::Permissions::from_mode(0o700)).unwrap();
    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    // The watchdog releases a regressed blocking startup so this test fails
    // with evidence instead of hanging. Success requires the async task to
    // release a preflight that is known to be blocked in the FIFO read.
    let observer = std::thread::spawn(move || {
        let mut message = String::new();
        std::fs::File::open(ready)
            .unwrap()
            .read_to_string(&mut message)
            .unwrap();
        assert_eq!(message, "ready\n");
        ready_tx.send(()).unwrap();
        let progressed = release_rx.recv_timeout(Duration::from_secs(5)).is_ok();
        std::fs::OpenOptions::new()
            .write(true)
            .open(release)
            .unwrap()
            .write_all(b"release\n")
            .unwrap();
        progressed
    });
    let releaser = tokio::spawn(async move {
        ready_rx.await.unwrap();
        let _ = release_tx.send(());
    });
    let mut input = inputs(root.path());
    input
        .worker
        .env
        .insert("PATH".into(), format!("{}:/usr/bin:/bin", bin.display()));
    let resident = Resident::start(&config(), input).await.unwrap();
    releaser.await.unwrap();
    assert!(
        observer.join().unwrap(),
        "startup blocked the sole executor thread until the watchdog released Git"
    );
    let client = resident.client();
    stop(&client, tokio::spawn(resident.run())).await;
}

#[tokio::test(flavor = "current_thread")]
async fn failed_listener_startup_releases_both_owner_and_agent_locks() {
    let root = root();
    let input = inputs(root.path());
    let ipc = input.state_dir.join("parent");
    std::fs::create_dir_all(&ipc).unwrap();
    for directory in [&input.state_dir, &ipc] {
        std::fs::set_permissions(directory, std::fs::Permissions::from_mode(0o700)).unwrap();
    }
    let occupied = ipc.join("request.sock");
    std::fs::write(&occupied, b"preserve this file").unwrap();
    assert!(Resident::start(&config(), input).await.is_err());
    assert_eq!(std::fs::read(&occupied).unwrap(), b"preserve this file");
    // No leftover owner or Agent lock may prevent a clean subsequent startup.
    std::fs::remove_file(occupied).unwrap();
    let resident = Resident::start(&config(), inputs(root.path()))
        .await
        .unwrap();
    let client = resident.client();
    stop(&client, tokio::spawn(resident.run())).await;
}
