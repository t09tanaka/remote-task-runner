//! Single-owner local runtime with durable captured input acceptance and local delivery.
//! Remote connection/reconnection ownership remains a separate layer.
use crate::config::runtime::{WorkerInputs, WorkerTarget};
use crate::config::ResolvedConfig;
use crate::coordinator::{Coordinator, TaskSpec};
use crate::ids::*;
use crate::platform::wall_ms;
use crate::protocol::{connect, ClientConfig, Io};
use crate::results::{CollectConfig, ResultStore};
use crate::scheduler::UnknownCosts;
use crate::state::{AttemptState, Observation, Receipts, Termination};
use crate::storage::CoordinatorStore;
use crate::worker::{agent::Agent, serve::serve_connection};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Write};
use std::os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt, PermissionsExt};
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::{watch, Mutex, Semaphore};
use tokio::task::JoinSet;

pub const MAX_REQUEST_BYTES: usize = 262_144;
const IO_TIMEOUT: Duration = Duration::from_secs(5);

/// All paths and worker identity/environment come from the bootstrap caller.
pub struct Inputs {
    pub state_dir: PathBuf,
    pub worker: WorkerInputs,
    pub collection_interval: Duration,
    pub collection: CollectConfig,
}

/// Private, versioned IPC. Clients capture into the configured private parent stores.
/// SubmitCaptured forwards that immutable input; legacy Submit accepts prepositioned
/// local sources only and explicitly refuses secret-bearing tasks.
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Request {
    pub schema_version: u32,
    pub cluster_id: ClusterId,
    pub owner_token: String,
    pub operation: Operation,
}
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
pub enum Operation {
    Ping,
    Submit {
        run_id: RunId,
        source_id: SourceId,
        tasks: Vec<(TaskId, TaskSpec)>,
    },
    SubmitCaptured {
        run_id: RunId,
        source_id: SourceId,
        tasks: Vec<(TaskId, TaskSpec)>,
    },
    Task {
        task_id: TaskId,
    },
    Query {
        query: super::query::Query,
    },
    Shutdown,
}
#[derive(Debug, Serialize, Deserialize)]
pub struct Attempt {
    pub attempt_id: AttemptId,
    pub state: AttemptState,
    pub observation: Observation,
    pub termination: Option<Termination>,
    pub receipts: Receipts,
}
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum Reply {
    Ready {
        epoch: CoordinatorEpoch,
    },
    Accepted {
        run_id: RunId,
    },
    Task {
        attempts: Vec<Attempt>,
    },
    Query {
        response: super::query::Response,
    },
    QueryError {
        code: crate::error::ErrorCode,
        message: String,
    },
    Stopping,
    Error {
        message: String,
    },
}

/// No store is exposed: callers only receive socket credentials.
#[derive(Clone)]
pub struct Client {
    socket: PathBuf,
    cluster_id: ClusterId,
    owner_token: String,
}
impl Client {
    /// Load credentials without taking writable coordinator ownership.
    /// The expected cluster is supplied by the caller, never inferred from a token.
    pub fn load(state: &Path, cluster_id: ClusterId) -> anyhow::Result<Self> {
        check_private_dir(state)?;
        let ipc = state.join("parent");
        check_private_dir(&ipc)?;
        let mut file = secure_file(&ipc.join("owner.token"), false)?;
        let mut token = String::new();
        Read::by_ref(&mut file)
            .take(65)
            .read_to_string(&mut token)?;
        anyhow::ensure!(
            token.len() == 64
                && token
                    .bytes()
                    .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase()),
            "invalid owner token"
        );
        let socket = ipc.join("request.sock");
        let meta = std::fs::symlink_metadata(&socket)?;
        use std::os::unix::fs::FileTypeExt;
        anyhow::ensure!(
            meta.file_type().is_socket() && meta.uid() == uid() && meta.mode() & 0o077 == 0,
            "unsafe owner socket"
        );
        Ok(Self {
            socket,
            cluster_id,
            owner_token: token,
        })
    }

    pub async fn query(
        &self,
        query: super::query::Query,
    ) -> Result<super::query::Response, super::query::Error> {
        use super::query::Error;
        use crate::error::ErrorCode;
        match self
            .request(Operation::Query { query })
            .await
            .map_err(|e| Error::new(ErrorCode::CommunicationUnknown, e))?
        {
            Reply::Query { response } => Ok(response),
            Reply::QueryError { code, message } => Err(Error::new(code, message)),
            Reply::Error { message } => Err(Error::new(ErrorCode::CommunicationUnknown, message)),
            _ => Err(Error::new(ErrorCode::Internal, "unexpected query response")),
        }
    }
    pub fn socket_path(&self) -> &Path {
        &self.socket
    }
    pub fn cluster_id(&self) -> &ClusterId {
        &self.cluster_id
    }
    pub async fn request(&self, operation: Operation) -> anyhow::Result<Reply> {
        let mut stream = tokio::time::timeout(IO_TIMEOUT, UnixStream::connect(&self.socket))
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "parent connect timeout"))??;
        let response_timeout = if matches!(&operation, Operation::SubmitCaptured { .. }) {
            Duration::from_secs(300)
        } else {
            IO_TIMEOUT
        };
        let request = Request {
            schema_version: 1,
            cluster_id: self.cluster_id.clone(),
            owner_token: self.owner_token.clone(),
            operation,
        };
        write_message(&mut stream, &request).await?;
        Ok(read_message_with_timeout(&mut stream, response_timeout).await?)
    }
}

struct CapturedInputs {
    transfer: Arc<Mutex<()>>,
    sources: crate::source::SourceStore,
    vault: crate::secrets::vault::Vault,
    key: crate::secrets::key::ClusterKey,
    env: std::collections::BTreeMap<String, String>,
}

pub struct Resident {
    coordinator: Arc<Coordinator>,
    files: Arc<ResultStore>,
    agent: Arc<Agent>,
    sources: crate::source::SourceStore,
    captured: Arc<CapturedInputs>,
    worker_client: crate::protocol::Client,
    worker_id: WorkerId,
    listener: UnixListener,
    worker_connection: tokio::task::JoinHandle<()>,
    client: Client,
    heartbeat: Duration,
    collection_interval: Duration,
    collection: CollectConfig,
    _token_file: File,
}

struct StartupParts {
    coordinator: Arc<Coordinator>,
    files: Arc<ResultStore>,
    sources: crate::source::SourceStore,
    captured: Arc<CapturedInputs>,
    agent: Arc<Agent>,
    token_file: File,
    socket: PathBuf,
    token: String,
    worker_id: WorkerId,
    cluster: ClusterId,
    heartbeat: Duration,
    collection_interval: Duration,
    collection: CollectConfig,
}
impl StartupParts {
    fn prepare(config: ResolvedConfig, inputs: Inputs) -> anyhow::Result<Self> {
        private_dir(&inputs.state_dir)?;
        // Refuse symlinked existing state entries before storage follows them.
        reject_symlinks(&inputs.state_dir)?;
        let settings = config.runtime_settings()?;
        let store = CoordinatorStore::open(&inputs.state_dir, wall_ms())?;
        let coordinator = Arc::new(settings.start_coordinator(store, Arc::new(UnknownCosts))?);
        let files = Arc::new(ResultStore::open(&inputs.state_dir.join("results"))?);
        let ipc = inputs.state_dir.join("parent");
        private_dir(&ipc)?;
        let token_path = ipc.join("owner.token");
        let mut token_file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(false)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW)
            .open(token_path)?;
        anyhow::ensure!(
            token_file.metadata()?.is_file()
                && token_file.metadata()?.nlink() == 1
                && token_file.metadata()?.uid() == uid()
                && token_file.metadata()?.permissions().mode() & 0o077 == 0,
            "unsafe owner token"
        );
        let mut bytes = [0; 32];
        getrandom::fill(&mut bytes).map_err(|e| anyhow::anyhow!("owner token: {e}"))?;
        let token: String = bytes.iter().map(|b| format!("{b:02x}")).collect();
        token_file.set_len(0)?;
        token_file.write_all(token.as_bytes())?;
        token_file.sync_all()?;
        let socket = ipc.join("request.sock");
        let worker_id = inputs.worker.worker_id.clone();
        private_dir(&inputs.worker.state_dir)?;
        reject_symlinks(&inputs.worker.state_dir)?;
        let sources = crate::source::SourceStore::open(&inputs.worker.state_dir)?;
        let captured = Arc::new(CapturedInputs {
            transfer: Arc::new(Mutex::new(())),
            sources: crate::source::SourceStore::open(&inputs.state_dir)?,
            vault: crate::secrets::vault::Vault::open(&inputs.state_dir)?,
            key: crate::secrets::key::load(&inputs.worker.cluster_key_path)?,
            env: inputs.worker.env.clone(),
        });
        let runtime = config.worker_runtime(inputs.worker)?;
        let agent = runtime.start().map_err(|e| anyhow::anyhow!(e.message))?;
        let cluster = coordinator.store().cluster_id().clone();
        Ok(StartupParts {
            cluster,
            coordinator,
            files,
            sources,
            captured,
            agent,
            token_file,
            socket,
            token,
            worker_id,
            heartbeat: Duration::from_millis(settings.heartbeat_ms),
            collection_interval: inputs.collection_interval,
            collection: inputs.collection,
        })
    }
}
struct Startup {
    parts: Option<StartupParts>,
}
impl Startup {
    fn cleanup(mut self) {
        if let Some(parts) = self.parts.take() {
            parts.agent.shutdown();
        }
    }
}
impl Drop for Startup {
    fn drop(&mut self) {
        if let Some(parts) = self.parts.take() {
            // Cancellation must also retain ownership until agent watchers stop.
            if let Ok(runtime) = tokio::runtime::Handle::try_current() {
                runtime.spawn_blocking(move || {
                    parts.agent.shutdown();
                    drop(parts);
                });
            } else {
                parts.agent.shutdown();
            }
        }
    }
}
async fn connect_local(
    startup: &Startup,
    config: &ResolvedConfig,
) -> anyhow::Result<(
    UnixListener,
    tokio::task::JoinHandle<()>,
    crate::protocol::Client,
)> {
    let parts = startup.parts.as_ref().expect("prepared startup");
    let listener = crate::protocol::transport::listen_unix(&parts.socket)?;
    let (parent, worker) = UnixStream::pair()?;
    let worker_agent = parts.agent.clone();
    let cluster = parts.cluster.clone();
    let server_cluster = cluster.clone();
    let worker_connection = tokio::spawn(async move {
        let _ = serve_connection(
            worker_agent,
            Io::from_stream(worker),
            server_cluster,
            Arc::new(Semaphore::new(64)),
        )
        .await;
    });
    let connected = match connect(
        Io::from_stream(parent),
        ClientConfig::new(
            cluster.clone(),
            parts.coordinator.store().coordinator_id().clone(),
            parts.coordinator.epoch(),
        ),
    )
    .await
    {
        Ok(c) => c,
        Err(e) => {
            worker_connection.abort();
            return Err(e.into());
        }
    };
    let worker_client = connected.client.clone();
    if let Err(e) = config.attach_worker(
        &parts.coordinator,
        WorkerTarget::Local(&parts.worker_id),
        connected,
    ) {
        worker_connection.abort();
        anyhow::bail!(e.message);
    }
    Ok((listener, worker_connection, worker_client))
}

impl Resident {
    /// Take the process lock before starting a new epoch, then construct exactly
    /// one coordinator and one ResultStore. A second owner fails before mutation.
    pub async fn start(config: &ResolvedConfig, inputs: Inputs) -> anyhow::Result<Self> {
        anyhow::ensure!(
            !inputs.collection_interval.is_zero(),
            "zero collection interval"
        );
        anyhow::ensure!(
            config.local_worker.value,
            "local resident requires local worker"
        );
        let config_owned = config.clone();
        let mut startup = tokio::task::spawn_blocking(move || {
            StartupParts::prepare(config_owned, inputs).map(|parts| Startup { parts: Some(parts) })
        })
        .await??;
        let connected = connect_local(&startup, config).await;
        let (listener, worker_connection, worker_client) = match connected {
            Ok(value) => value,
            Err(e) => {
                tokio::task::spawn_blocking(move || startup.cleanup()).await?;
                return Err(e);
            }
        };
        let parts = startup.parts.take().expect("prepared startup");
        Ok(Self {
            coordinator: parts.coordinator,
            files: parts.files,
            agent: parts.agent,
            sources: parts.sources,
            captured: parts.captured,
            worker_client,
            worker_id: parts.worker_id,
            listener,
            worker_connection,
            client: Client {
                socket: parts.socket,
                cluster_id: parts.cluster,
                owner_token: parts.token,
            },
            heartbeat: parts.heartbeat,
            collection_interval: parts.collection_interval,
            collection: parts.collection,
            _token_file: parts.token_file,
        })
    }
    pub fn client(&self) -> Client {
        self.client.clone()
    }

    /// Run until owner-authenticated shutdown or fencing. Disconnecting a
    /// request never cancels accepted work. All work loops have separate clocks.
    pub async fn run(mut self) -> anyhow::Result<()> {
        let (stop, rx) = watch::channel(false);
        let mut loops = JoinSet::new();
        let coordinator = self.coordinator.clone();
        let stop_tick = stop.clone();
        let mut tick_stop = rx.clone();
        let heartbeat = self.heartbeat;
        loops.spawn(async move {
            let mut timer = tokio::time::interval(heartbeat);
            timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tokio::select! { _ = tick_stop.changed() => return Ok::<(), anyhow::Error>(()), _ = timer.tick() => {} }
                if let Err(e) = coordinator.tick().await { let _ = stop_tick.send(true); return Err(e.into()); }
            }
        });
        let coordinator = self.coordinator.clone();
        let files = self.files.clone();
        let collection = self.collection;
        let cadence = self.collection_interval;
        let mut collect_stop = rx.clone();
        let stop_collect = stop.clone();
        loops.spawn(async move {
            let mut timer = tokio::time::interval(cadence);
            timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tokio::select! { _ = collect_stop.changed() => return Ok(()), _ = timer.tick() => {} }
                if let Err(e) = coordinator.collect(&files, collection).await { let _ = stop_collect.send(true); return Err(e.into()); }
            }
        });
        let agent = self.agent.clone();
        let mut maintain_stop = rx.clone();
        loops.spawn(async move {
            let mut timer = tokio::time::interval(heartbeat);
            timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tokio::select! { _ = maintain_stop.changed() => return Ok(()), _ = timer.tick() => {} }
                let agent = agent.clone();
                tokio::task::spawn_blocking(move || agent.supervise()).await?;
            }
        });
        let coordinator = self.coordinator.clone();
        let captured = self.captured.clone();
        let worker_client = self.worker_client.clone();
        let worker_id = self.worker_id.clone();
        let mut secret_stop = rx.clone();
        loops.spawn(async move {
            let mut timer = tokio::time::interval(Duration::from_millis(100));
            timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            let mut sent = std::collections::BTreeSet::new();
            let mut next = 0usize;
            loop {
                tokio::select! { _ = secret_stop.changed() => return Ok(()), _ = timer.tick() => {} }
                let owner = coordinator.clone(); let worker = worker_id.clone();
                let attempts = tokio::task::spawn_blocking(move || owner.store().live_attempts(Some(&worker))).await??;
                let active: std::collections::BTreeSet<_> = attempts.iter().map(|a| a.record.attempt_id.clone()).collect();
                sent.retain(|id| active.contains(id));
                let start = next.min(attempts.len());
                next = if start.saturating_add(16) >= attempts.len() { 0 } else { start + 16 };
                for attempt in attempts.into_iter().skip(start).take(16) {
                    let Some(placement) = attempt.placement else { continue; };
                    let payload: crate::wire::AttemptPayload = serde_json::from_value(placement.demand["payload"].clone())?;
                    if !payload.has_secret_bundle || sent.contains(&attempt.record.attempt_id) { continue; }
                    let capture = captured.clone(); let run = payload.run_id.clone(); let source = payload.source_id.clone();
                    let bundle = tokio::task::spawn_blocking(move || -> anyhow::Result<_> {
                        let id = crate::source::classify::ci_id_of(&source).ok_or_else(|| anyhow::anyhow!("invalid source"))?;
                        let manifest = capture.sources.manifest(&id)?.ok_or_else(|| anyhow::anyhow!("input source unavailable"))?;
                        Ok(capture.vault.load(&run, &manifest.secret_files, &capture.key)?)
                    }).await?;
                    // Missing/tampered bundles never become an empty bundle. The worker's
                    // secret gate eventually reports SECRET_UNAVAILABLE without running.
                    let Ok(bundle) = bundle else { continue; };
                    let delivered = tokio::select! {
                        _ = secret_stop.changed() => return Ok(()),
                        result = crate::secrets::send::send_bundle(&worker_client, &bundle,
                            coordinator.epoch(), &attempt.record.attempt_id, attempt.record.generation, IO_TIMEOUT) => result.is_ok(),
                    };
                    if delivered { sent.insert(attempt.record.attempt_id); }
                }
            }
        });
        let permits = Arc::new(Semaphore::new(32));
        // Serializes acceptance with shutdown, not with placement/collection.
        let admission = Arc::new(Mutex::new(false));
        let mut requests = JoinSet::new();
        let mut stopped = rx;
        let mut failure = None;
        loop {
            tokio::select! {
                _ = stopped.changed() => break,
                result = &mut self.worker_connection => {
                    failure = Some(anyhow::anyhow!("local worker connection ended: {result:?}"));
                    break;
                },
                result = loops.join_next(), if !loops.is_empty() => {
                    match result { Some(Ok(Err(e))) => failure = Some(e), Some(Err(e)) => failure = Some(e.into()), _ => {} }
                    break;
                },
                ended = requests.join_next(), if !requests.is_empty() => { if let Some(Err(e)) = ended { failure = Some(anyhow::Error::from(e)); break; } },
                conn = self.listener.accept() => {
                    let (stream, _) = match conn { Ok(c) => c, Err(e) => { failure = Some(e.into()); break; } };
                    if stream.peer_cred().map(|peer| peer.uid()).ok() != Some(uid()) { continue; }
                    let Ok(permit) = permits.clone().try_acquire_owned() else { continue; };
                    let coordinator = self.coordinator.clone();
                    let sources = self.sources.clone();
                    let captured = self.captured.clone();
                    let worker_client = self.worker_client.clone();
                    let client = self.client.clone();
                    let stop = stop.clone();
                    let admission = admission.clone();
                    requests.spawn(async move {
                        let _permit = permit;
                        let _ = handle(stream, coordinator, sources, captured, worker_client, client, admission, stop).await;
                    });
                }
            }
        }
        *admission.lock().await = true;
        let _ = stop.send(true);
        while let Some(result) = requests.join_next().await {
            if let Err(e) = result {
                failure.get_or_insert(e.into());
            }
        }
        while let Some(result) = loops.join_next().await {
            match result {
                Ok(Ok(())) => {}
                Ok(Err(e)) => {
                    failure.get_or_insert(e);
                }
                Err(e) => {
                    failure.get_or_insert(e.into());
                }
            }
        }
        // No cancellation or terminal write: supervisors keep running, and a
        // restarted owner adopts the same durable attempts.
        let agent = self.agent.clone();
        tokio::task::spawn_blocking(move || agent.shutdown()).await?;
        self.worker_connection.abort();
        match failure {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }
}
impl Drop for Resident {
    fn drop(&mut self) {
        self.worker_connection.abort();
        self.agent.shutdown();
        // Only this lock-holding owner's private socket is removed.
        let _ = std::fs::remove_file(&self.client.socket);
    }
}

#[allow(clippy::too_many_arguments)]
async fn handle(
    mut stream: UnixStream,
    coordinator: Arc<Coordinator>,
    sources: crate::source::SourceStore,
    captured: Arc<CapturedInputs>,
    worker_client: crate::protocol::Client,
    client: Client,
    admission: Arc<Mutex<bool>>,
    stop: watch::Sender<bool>,
) -> anyhow::Result<()> {
    let request: Request = read_message(&mut stream).await?;
    anyhow::ensure!(
        request.schema_version == 1
            && request.cluster_id == client.cluster_id
            && request.owner_token == client.owner_token,
        "unauthorized parent request"
    );
    // Metadata reads use their own read-only SQLite thread, never the owner's
    // admission lock or update connection. Dropping the response cannot write state.
    if let Operation::Query { query } = request.operation {
        let state = coordinator
            .store()
            .db
            .path()
            .parent()
            .expect("database parent")
            .to_owned();
        let reply = tokio::task::spawn_blocking(move || {
            match crate::storage::CoordinatorStore::open_read_only(&state)
                .and_then(|s| s.query(query))
            {
                Ok(response) => Reply::Query { response },
                Err(e) => Reply::QueryError {
                    code: e.code(),
                    message: e.to_string(),
                },
            }
        })
        .await?;
        return write_message(&mut stream, &reply).await.map_err(Into::into);
    }
    let mut gate = admission.lock().await;
    let reply = if *gate || coordinator.is_fenced() {
        Reply::Error {
            message: "resident is stopping".into(),
        }
    } else {
        match request.operation {
            Operation::Ping => Reply::Ready {
                epoch: coordinator.store().epoch(),
            },
            Operation::Submit {
                run_id,
                source_id,
                tasks,
            } => {
                if tasks.is_empty() || tasks.len() > 64 {
                    Reply::Error {
                        message: "submit requires 1..64 tasks".into(),
                    }
                } else {
                    let source = source_id.clone();
                    let expected = tasks[0].1.manifest_digest.clone();
                    if tasks
                        .iter()
                        .any(|(_, task)| task.manifest_digest != expected || task.has_secret_bundle)
                    {
                        return write_message(
                            &mut stream,
                            &Reply::Error {
                                message:
                                    "source digests must match; secret forwarding is unavailable"
                                        .into(),
                            },
                        )
                        .await
                        .map_err(Into::into);
                    }
                    let owner = coordinator.clone();
                    let checked = tokio::task::spawn_blocking(move || -> anyhow::Result<()> {
                        let ci = crate::source::classify::ci_id_of(&source)
                            .ok_or_else(|| anyhow::anyhow!("source is not a captured source"))?;
                        let manifest = sources.manifest(&ci)?.ok_or_else(|| {
                            anyhow::anyhow!("source has not reached the local worker")
                        })?;
                        crate::source::store::verify_manifest(&manifest)?;
                        let digest = Digest::parse(&manifest.manifest_digest).or_else(|_| {
                            Digest::parse(&format!("sha256:{}", manifest.manifest_digest))
                        })?;
                        anyhow::ensure!(digest == expected, "source manifest digest mismatch");
                        owner
                            .store()
                            .put_source_ref(&source, &expected, wall_ms())?;
                        Ok(())
                    })
                    .await?;
                    if let Err(e) = checked {
                        return write_message(
                            &mut stream,
                            &Reply::Error {
                                message: e.to_string(),
                            },
                        )
                        .await
                        .map_err(Into::into);
                    }
                    match coordinator.enqueue(run_id.clone(), source_id, tasks).await {
                        Ok(()) => Reply::Accepted { run_id },
                        Err(e) => Reply::Error {
                            message: e.to_string(),
                        },
                    }
                }
            }
            Operation::SubmitCaptured {
                run_id,
                source_id,
                tasks,
            } => {
                // Transfer can be slow; admission/shutdown and other RPCs stay responsive.
                drop(gate);
                let source = source_id.clone();
                let run = run_id.clone();
                let capture = captured.clone();
                let mut stopping = stop.subscribe();
                let checked =
                    tokio::task::spawn_blocking(move || -> anyhow::Result<(Digest, bool)> {
                        let id = crate::source::classify::ci_id_of(&source)
                            .ok_or_else(|| anyhow::anyhow!("invalid source"))?;
                        anyhow::ensure!(
                            capture.sources.source_of_run(&run)?.as_deref() == Some(&id),
                            "run source binding mismatch"
                        );
                        let manifest = capture
                            .sources
                            .manifest(&id)?
                            .ok_or_else(|| anyhow::anyhow!("source unavailable"))?;
                        crate::source::store::verify_manifest(&manifest)?;
                        capture
                            .vault
                            .load(&run, &manifest.secret_files, &capture.key)?;
                        Ok((
                            Digest::parse(&manifest.manifest_digest).or_else(|_| {
                                Digest::parse(&format!("sha256:{}", manifest.manifest_digest))
                            })?,
                            !manifest.secret_files.is_empty(),
                        ))
                    });
                let result = async {
                    let (digest, has_secrets) = tokio::select! {
                        biased;
                        _=stopping.wait_for(|stopped| *stopped)=>anyhow::bail!("resident is stopping"),
                        checked=checked=>checked??,
                    };
                    anyhow::ensure!(
                        !tasks.is_empty() && tasks.len() <= 64,
                        "submit requires 1..64 tasks"
                    );
                    anyhow::ensure!(
                        tasks.iter().all(|(_, task)| task.manifest_digest == digest
                            && task.has_secret_bundle == has_secrets),
                        "source digest mismatch"
                    );
                    let transfer=tokio::select! {
                        biased;
                        _=stopping.wait_for(|stopped| *stopped)=>anyhow::bail!("resident is stopping"),
                        transfer=captured.transfer.clone().lock_owned()=>transfer,
                    };
                    let source=source_id.clone(); let store=captured.sources.clone();
                    let env=captured.env.clone(); let worker=worker_client.clone();
                    let epoch=coordinator.epoch(); let transfer_stop=stopping.clone();
                    // A detached transfer owns only source IO and its locks. It drains any
                    // current blocking operation before cleanup, never coordinator ownership.
                    let forwarding=tokio::spawn(async move {
                        super::forward::source(&worker,epoch,store,&source,env,transfer_stop,transfer).await
                    });
                    tokio::select! {
                        biased;
                        _=stopping.wait_for(|stopped| *stopped)=>anyhow::bail!("resident is stopping"),
                        forwarded=forwarding=>forwarded??,
                    }
                    let guard = admission.lock().await;
                    anyhow::ensure!(!*guard && !coordinator.is_fenced(), "resident is stopping");
                    let owner = coordinator.clone();
                    let source = source_id.clone();
                    tokio::task::spawn_blocking(move || {
                        owner.store().put_source_ref(&source, &digest, wall_ms())
                    })
                    .await??;
                    coordinator
                        .enqueue(run_id.clone(), source_id, tasks)
                        .await?;
                    Ok::<_, anyhow::Error>(())
                }
                .await;
                let answer = match result {
                    Ok(()) => Reply::Accepted { run_id },
                    Err(e) => Reply::Error {
                        message: e.to_string(),
                    },
                };
                return write_message(&mut stream, &answer)
                    .await
                    .map_err(Into::into);
            }
            Operation::Task { task_id } => {
                let coordinator = coordinator.clone();
                match tokio::task::spawn_blocking(move || {
                    coordinator.store().latest_attempt_of_task(&task_id)
                })
                .await?
                {
                    Ok(records) => Reply::Task {
                        attempts: records
                            .into_iter()
                            .map(|a| Attempt {
                                attempt_id: a.attempt_id,
                                state: a.state,
                                observation: a.observation,
                                termination: a.termination,
                                receipts: a.receipts,
                            })
                            .collect(),
                    },
                    Err(e) => Reply::Error {
                        message: e.to_string(),
                    },
                }
            }
            Operation::Query { .. } => unreachable!("query handled before admission"),
            Operation::Shutdown => {
                *gate = true;
                let _ = stop.send(true);
                Reply::Stopping
            }
        }
    };
    drop(gate);
    write_message(&mut stream, &reply).await?;
    Ok(())
}
async fn read_message<T: DeserializeOwned>(stream: &mut UnixStream) -> io::Result<T> {
    read_message_with_timeout(stream, IO_TIMEOUT).await
}
async fn read_message_with_timeout<T: DeserializeOwned>(
    stream: &mut UnixStream,
    timeout: Duration,
) -> io::Result<T> {
    tokio::time::timeout(timeout, async {
        let length = stream.read_u32().await? as usize;
        if length == 0 || length > MAX_REQUEST_BYTES {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "parent frame size",
            ));
        }
        let mut bytes = vec![0; length];
        stream.read_exact(&mut bytes).await?;
        serde_json::from_slice(&bytes).map_err(io::Error::other)
    })
    .await
    .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "parent request timeout"))?
}
async fn write_message<T: Serialize>(stream: &mut UnixStream, message: &T) -> io::Result<()> {
    let bytes = serde_json::to_vec(message).map_err(io::Error::other)?;
    if bytes.len() > MAX_REQUEST_BYTES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "parent frame size",
        ));
    }
    tokio::time::timeout(IO_TIMEOUT, async {
        stream.write_u32(bytes.len() as u32).await?;
        stream.write_all(&bytes).await
    })
    .await
    .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "parent response timeout"))?
}
fn uid() -> u32 {
    unsafe { libc::geteuid() }
}
pub(super) fn private_dir(path: &Path) -> io::Result<()> {
    validate_components(path)?;
    match std::fs::symlink_metadata(path) {
        Ok(m) if m.is_dir() && m.uid() == uid() && m.permissions().mode() & 0o077 == 0 => Ok(()),
        Ok(_) => Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "unsafe resident directory",
        )),
        Err(e) if e.kind() == io::ErrorKind::NotFound => {
            std::fs::DirBuilder::new().mode(0o700).create(path)
        }
        Err(e) => Err(e),
    }
}
pub(super) fn validate_components(path: &Path) -> io::Result<()> {
    if !path.is_absolute() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "resident path must be absolute",
        ));
    }
    let mut current = PathBuf::new();
    for c in path.components() {
        match c {
            Component::RootDir | Component::Normal(_) => current.push(c),
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "resident path component",
                ))
            }
        }
        match std::fs::symlink_metadata(&current) {
            Ok(m) if m.file_type().is_symlink() => {
                return Err(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "symlinked resident path",
                ))
            }
            Ok(_) => {}
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => return Err(e),
        }
    }
    Ok(())
}
pub(super) fn reject_symlinks(path: &Path) -> io::Result<()> {
    for e in std::fs::read_dir(path)? {
        let e = e?;
        if e.file_type()?.is_symlink() {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "symlinked resident state",
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests;

/// Open private regular files without following a link or blocking on a FIFO.
pub(super) fn secure_file(path: &Path, writable: bool) -> io::Result<File> {
    validate_components(path)?;
    let file = OpenOptions::new()
        .read(true)
        .write(writable)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
        .open(path)?;
    let meta = file.metadata()?;
    if !meta.is_file() || meta.nlink() != 1 || meta.uid() != uid() || meta.mode() & 0o077 != 0 {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "unsafe resident file",
        ));
    }
    Ok(file)
}
pub(super) fn check_private_dir(path: &Path) -> io::Result<()> {
    validate_components(path)?;
    let meta = std::fs::symlink_metadata(path)?;
    if !meta.is_dir() || meta.uid() != uid() || meta.mode() & 0o077 != 0 {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "unsafe resident directory",
        ));
    }
    Ok(())
}
