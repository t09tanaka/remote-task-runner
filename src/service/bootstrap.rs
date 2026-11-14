//! Detached process bootstrap. Service registration and public init are separate.
use super::resident::{self, Client, Operation, Reply, Resident};
use crate::config::{self, runtime::WorkerInputs, Overlay};
use crate::ids::{ClusterId, WorkerBootId, WorkerId};
use crate::storage::CoordinatorStore;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fs::{File, OpenOptions, TryLockError};
use std::io::{Read, Write};
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;
use tokio::io::AsyncWriteExt;

const LIMIT: u64 = 262_144;
const STARTUP: Duration = Duration::from_secs(20);

/// Paths, persistent worker identity, and environment are chosen by the caller.
/// No config/key/identity is implicitly created, and no global environment changes.
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Spec {
    pub state_dir: PathBuf,
    pub worker_state_dir: PathBuf,
    pub worker_id: WorkerId,
    pub cluster_key_path: PathBuf,
    pub executable: PathBuf,
    pub env: BTreeMap<String, String>,
    pub repo_root: Option<PathBuf>,
    pub expected_cluster: Option<ClusterId>,
}

/// A retained service input. The creator owns its lifecycle; retain across service
/// restarts. A future uninstaller must verify this digest and file identity before
/// removing it; this module never deletes/replaces an existing input.
#[derive(Debug)]
pub struct ManagedInput {
    pub path: PathBuf,
    pub sha256: String,
    pub device: u64,
    pub inode: u64,
}

/// Publish a bounded private launch input. An identical existing file is accepted;
/// any other content or unsafe metadata is refused without modification.
pub fn write_input(path: &Path, spec: &Spec) -> anyhow::Result<ManagedInput> {
    use sha2::{Digest, Sha256};
    validate(spec)?;
    resident::validate_components(path)?;
    resident::check_private_dir(
        path.parent()
            .ok_or_else(|| anyhow::anyhow!("input parent missing"))?,
    )?;
    let bytes = serde_json::to_vec(spec)?;
    anyhow::ensure!(bytes.len() as u64 <= LIMIT, "bootstrap input too large");
    let mut temporary = tempfile::NamedTempFile::new_in(path.parent().expect("checked parent"))?;
    temporary.write_all(&bytes)?;
    temporary.as_file().sync_all()?;
    match temporary.persist_noclobber(path) {
        Ok(_) => {}
        Err(e) if e.error.kind() == std::io::ErrorKind::AlreadyExists => {
            let mut existing = Vec::new();
            resident::secure_file(path, false)?
                .take(LIMIT + 1)
                .read_to_end(&mut existing)?;
            anyhow::ensure!(existing == bytes, "existing bootstrap input differs");
        }
        Err(e) => return Err(e.error.into()),
    }
    use std::os::unix::fs::MetadataExt;
    let meta = resident::secure_file(path, false)?.metadata()?;
    File::open(path.parent().expect("checked parent"))?.sync_all()?;
    Ok(ManagedInput {
        path: path.to_owned(),
        sha256: format!("{:x}", Sha256::digest(&bytes)),
        device: meta.dev(),
        inode: meta.ino(),
    })
}

/// Shared service argv: private values remain in the retained input, never argv.
pub fn daemon_argv(input: &Path) -> anyhow::Result<Vec<String>> {
    resident::validate_components(input)?;
    Ok(vec![
        "daemon".into(),
        "--input".into(),
        input
            .to_str()
            .ok_or_else(|| anyhow::anyhow!("input path must be UTF-8"))?
            .into(),
    ])
}

fn validate(spec: &Spec) -> anyhow::Result<()> {
    resident::check_private_dir(&spec.state_dir)?;
    resident::check_private_dir(&spec.worker_state_dir)?;
    resident::validate_components(&spec.executable)?;
    anyhow::ensure!(
        spec.repo_root.as_ref().is_none_or(|p| p.is_absolute()),
        "project root must be absolute"
    );
    let mut key_file = resident::secure_file(&spec.cluster_key_path, false)?;
    crate::secrets::key::load_opened(&spec.cluster_key_path, &mut key_file)?;
    if spec.expected_cluster.is_some() {
        let _ = read_cluster(spec)?;
    }
    Ok(())
}

fn startup_lock(state: &Path) -> anyhow::Result<File> {
    let path = state.join("startup.lock");
    resident::validate_components(&path)?;
    // The private state directory prevents an untrusted user from substituting it.
    let created = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW)
        .open(&path);
    match created {
        Ok(file) => drop(file),
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {}
        Err(e) => return Err(e.into()),
    }
    Ok(resident::secure_file(&path, true)?)
}

fn read_store(spec: &Spec) -> anyhow::Result<CoordinatorStore> {
    // Read-only inspection never takes ownership, creates state, or increments epoch.
    resident::reject_symlinks(&spec.state_dir)?;
    let db = spec.state_dir.join("coordinator.db");
    resident::validate_components(&db)?;
    use std::os::unix::fs::MetadataExt;
    let meta = std::fs::symlink_metadata(&db)?;
    anyhow::ensure!(
        meta.is_file() && meta.nlink() == 1 && meta.uid() == rustix::process::geteuid().as_raw(),
        "unsafe coordinator database"
    );
    let store = CoordinatorStore::open_read_only(&spec.state_dir)?;
    let cluster = store.cluster_id().clone();
    if let Some(expected) = &spec.expected_cluster {
        anyhow::ensure!(expected == &cluster, "foreign coordinator cluster");
    }
    Ok(store)
}
fn read_cluster(spec: &Spec) -> anyhow::Result<ClusterId> {
    Ok(read_store(spec)?.cluster_id().clone())
}
/// Validate credentials, identities, private paths and schema without writes.
pub(crate) fn read_only_store(spec: &Spec) -> anyhow::Result<CoordinatorStore> {
    validate(spec)?;
    read_store(spec)
}
fn load_client(spec: &Spec) -> anyhow::Result<Client> {
    Client::load(&spec.state_dir, read_cluster(spec)?)
}

/// Connect to the owner or start one while holding a separate startup lock.
/// The child owns the writable store; caller exit does not stop the child.
pub async fn ensure(spec: Spec) -> anyhow::Result<Client> {
    let copy = spec.clone();
    let lock = tokio::task::spawn_blocking(move || -> anyhow::Result<File> {
        validate(&copy)?;
        startup_lock(&copy.state_dir)
    })
    .await??;
    let deadline = tokio::time::Instant::now() + STARTUP;
    loop {
        match lock.try_lock() {
            Ok(()) => break,
            Err(TryLockError::WouldBlock) if tokio::time::Instant::now() < deadline => {
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
            Err(e) => return Err(anyhow::anyhow!("resident startup lock: {e}")),
        }
    }
    let copy = spec.clone();
    let existing = tokio::task::spawn_blocking(move || load_client(&copy)).await?;
    if let Ok(client) = existing {
        if matches!(
            client.request(Operation::Ping).await,
            Ok(Reply::Ready { .. })
        ) {
            return Ok(client);
        }
    }
    let bytes = serde_json::to_vec(&spec)?;
    anyhow::ensure!(bytes.len() as u64 <= LIMIT, "bootstrap input too large");
    let mut command = tokio::process::Command::new(&spec.executable);
    command
        .arg("daemon")
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    command.env_clear().envs(&spec.env);
    // A distinct process group prevents caller-terminal Ctrl-C from reaching owner.
    command.process_group(0);
    let mut child = command.spawn()?;
    let startup = async {
        let mut input = child.stdin.take().expect("piped stdin");
        input.write_all(&bytes).await?;
        input.shutdown().await?;
        drop(input);
        loop {
            if let Some(status) = child.try_wait()? {
                anyhow::bail!("resident exited during startup: {status}");
            }
            let copy = spec.clone();
            if let Ok(client) = tokio::task::spawn_blocking(move || load_client(&copy)).await? {
                if matches!(
                    client.request(Operation::Ping).await,
                    Ok(Reply::Ready { .. })
                ) {
                    return Ok(client);
                }
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    };
    match tokio::time::timeout_at(deadline, startup).await {
        Ok(Ok(client)) => {
            // Reap this child when the caller stays alive; dropping the runtime detaches it.
            tokio::spawn(async move {
                let _ = child.wait().await;
            });
            Ok(client)
        }
        result => {
            // Only our own Child handle is signalled; never a PID from a state file.
            let _ = child.kill().await;
            let _ = child.wait().await;
            match result {
                Ok(Err(e)) => Err(e),
                Err(_) => anyhow::bail!("resident startup timed out"),
                Ok(Ok(_)) => unreachable!(),
            }
        }
    }
}

/// Read the retained private input without creating any runtime state.
pub fn read_input(path: &Path) -> anyhow::Result<Spec> {
    resident::check_private_dir(
        path.parent()
            .ok_or_else(|| anyhow::anyhow!("input parent missing"))?,
    )?;
    let mut bytes = Vec::new();
    resident::secure_file(path, false)?
        .take(LIMIT + 1)
        .read_to_end(&mut bytes)?;
    anyhow::ensure!(bytes.len() as u64 <= LIMIT, "bootstrap input too large");
    let spec: Spec = serde_json::from_slice(&bytes)?;
    validate(&spec)?;
    Ok(spec)
}

/// Hidden process entry. Stdin is one bounded, private bootstrap specification.
pub fn daemon_main(input: Option<&Path>) -> i32 {
    let result = (|| -> anyhow::Result<()> {
        let mut bytes = Vec::new();
        match input {
            Some(path) => {
                resident::check_private_dir(
                    path.parent()
                        .ok_or_else(|| anyhow::anyhow!("input parent missing"))?,
                )?;
                resident::secure_file(path, false)?
                    .take(LIMIT + 1)
                    .read_to_end(&mut bytes)?;
            }
            None => {
                std::io::stdin().take(LIMIT + 1).read_to_end(&mut bytes)?;
            }
        }
        anyhow::ensure!(bytes.len() as u64 <= LIMIT, "bootstrap input too large");
        let spec: Spec = serde_json::from_slice(&bytes)?;
        validate(&spec)?;
        let config = config::load::load(&spec.env, spec.repo_root.as_deref(), &Overlay::default())?;
        let boot = crate::platform::boot_id()
            .ok_or_else(|| anyhow::anyhow!("OS boot identity unavailable"))?;
        let inputs = resident::Inputs {
            state_dir: spec.state_dir,
            worker: WorkerInputs {
                state_dir: spec.worker_state_dir,
                worker_id: spec.worker_id,
                boot_id: WorkerBootId::parse(&boot)?,
                supervisor_exe: spec.executable,
                cluster_key_path: spec.cluster_key_path,
                env: spec.env,
            },
            collection_interval: Duration::from_millis(100),
            collection: Default::default(),
        };
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()?
            .block_on(async { Resident::start(&config, inputs).await?.run().await })
    })();
    match result {
        Ok(()) => 0,
        Err(e) => {
            let _ = writeln!(std::io::stderr(), "error: {e}");
            1
        }
    }
}
