//! Chunked immutable source delivery on an independent multiplexed worker link.
use crate::ids::*;
use crate::protocol::{
    transfer::{put_blob_as, put_blob_reader_as},
    Client,
};
use crate::source::{store::UploadFile, SourceStore};
use crate::wire::*;
use std::collections::BTreeMap;
use std::fs::{File, OpenOptions, TryLockError};
use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{watch, OwnedMutexGuard};

// This lease outlives an interrupted request and every blocking operation it started.
// The file lock also excludes a restarted owner's exporter/cleanup while old IO ends.
struct Lease {
    _local: OwnedMutexGuard<()>,
    _file: File,
}
// Retain the existing transfer lease even if cancellation drops the upload
// future while one bounded blocking read is still finishing.
struct OwnedUpload {
    file: File,
    _lease: Arc<Lease>,
}
impl std::io::Read for OwnedUpload {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        std::io::Read::read(&mut self.file, buf)
    }
}
impl std::io::Seek for OwnedUpload {
    fn seek(&mut self, from: std::io::SeekFrom) -> std::io::Result<u64> {
        std::io::Seek::seek(&mut self.file, from)
    }
}
async fn stopped(stop: &mut watch::Receiver<bool>) {
    let _ = stop.wait_for(|value| *value).await;
}
fn check_stop(stop: &watch::Receiver<bool>) -> anyhow::Result<()> {
    anyhow::ensure!(!*stop.borrow(), "resident is stopping");
    Ok(())
}

const TIMEOUT: Duration = Duration::from_secs(5);

async fn put(
    client: &Client,
    epoch: CoordinatorEpoch,
    source: &SourceId,
    kind: SourceBlobKind,
    bytes: &[u8],
    stop: &mut watch::Receiver<bool>,
) -> anyhow::Result<()> {
    check_stop(stop)?;
    let answer = tokio::select! {
        biased;
        _=stopped(stop)=>anyhow::bail!("resident is stopping"),
        answer=put_blob_as(client, epoch, source, kind, bytes, 0, 64 * 1024, TIMEOUT)=>answer?,
    };
    anyhow::ensure!(answer.complete, "source transfer incomplete");
    Ok(())
}
async fn put_file(
    client: &Client,
    epoch: CoordinatorEpoch,
    source: &SourceId,
    kind: SourceBlobKind,
    upload: UploadFile,
    stop: &mut watch::Receiver<bool>,
    lease: Arc<Lease>,
) -> anyhow::Result<()> {
    check_stop(stop)?;
    let reader = OwnedUpload {
        file: upload.file,
        _lease: lease,
    };
    let answer = tokio::select! {
        biased;
        _=stopped(stop)=>anyhow::bail!("resident is stopping"),
        answer=put_blob_reader_as(client,epoch,source,kind,reader,upload.digest,upload.size,0,64*1024,TIMEOUT)=>answer?,
    };
    anyhow::ensure!(answer.complete, "source transfer incomplete");
    Ok(())
}
async fn verify(
    client: &Client,
    source: &SourceId,
    stop: &mut watch::Receiver<bool>,
) -> anyhow::Result<BlobVerifyResponse> {
    check_stop(stop)?;
    let call = client.call(
        Request::BlobVerify(BlobVerifyRequest {
            schema_version: SchemaVersion::CURRENT,
            request_id: RequestId::generate(),
            source_id: source.clone(),
            blobs: vec![],
        }),
        TIMEOUT,
    );
    let answer = tokio::select! {
        biased;
        _=stopped(stop)=>anyhow::bail!("resident is stopping"),
        answer=call=>answer?,
    };
    match answer {
        Response::BlobVerify(answer) => Ok(answer),
        _ => anyhow::bail!("unexpected source verification answer"),
    }
}
/// Retrying uses only the run-bound store and never captures the original again.
pub async fn source(
    client: &Client,
    epoch: CoordinatorEpoch,
    store: SourceStore,
    source: &SourceId,
    env: BTreeMap<String, String>,
    mut stop: watch::Receiver<bool>,
    ownership: OwnedMutexGuard<()>,
) -> anyhow::Result<()> {
    check_stop(&stop)?;
    let path = store.root().join("transfer.lock");
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
        .open(path)?;
    let meta = file.metadata()?;
    anyhow::ensure!(
        meta.is_file()
            && meta.nlink() == 1
            && meta.uid() == rustix::process::geteuid().as_raw()
            && meta.mode() & 0o077 == 0,
        "unsafe source transfer lock"
    );
    loop {
        check_stop(&stop)?;
        match file.try_lock() {
            Ok(()) => break,
            Err(TryLockError::WouldBlock) => {
                tokio::select! {biased; _=stopped(&mut stop)=>anyhow::bail!("resident is stopping"), _=tokio::time::sleep(Duration::from_millis(25))=>{}}
            }
            Err(error) => return Err(error.into()),
        }
    }
    let lease = Arc::new(Lease {
        _local: ownership,
        _file: file,
    });
    let outcome = source_inner(client, epoch, &store, source, env, &mut stop, &lease).await;
    // Do not abort the worker on request cancellation: an exporter already on a
    // blocking thread must finish before cleanup. Neither job owns CoordinatorStore.
    let clean = store.clone();
    let lease = lease.clone();
    if let Some(id) = crate::source::classify::ci_id_of(source) {
        tokio::task::spawn_blocking(move || {
            let _lease = lease;
            clean.remove_bundles(&id)
        })
        .await??;
    }
    outcome
}
async fn source_inner(
    client: &Client,
    epoch: CoordinatorEpoch,
    store: &SourceStore,
    source: &SourceId,
    env: BTreeMap<String, String>,
    stop: &mut watch::Receiver<bool>,
    lease: &Arc<Lease>,
) -> anyhow::Result<()> {
    let id = crate::source::classify::ci_id_of(source)
        .ok_or_else(|| anyhow::anyhow!("invalid captured source"))?;
    let read = store.clone();
    let ownership = lease.clone();
    let (manifest, closure) = tokio::task::spawn_blocking(move || -> anyhow::Result<_> {
        let _ownership = ownership;
        let manifest = read
            .manifest(&id)?
            .ok_or_else(|| anyhow::anyhow!("input source unavailable"))?;
        let closure = read.closure(&manifest)?;
        Ok((manifest, closure))
    })
    .await??;
    check_stop(stop)?;
    if verify(client, source, stop).await?.ready {
        return Ok(());
    }
    // The wire requires the root manifest first; remaining manifests are its closure.
    // Secrets are metadata only.
    for item in closure.iter().rev() {
        let bytes = serde_json::to_vec(item)?;
        put(
            client,
            epoch,
            source,
            SourceBlobKind::Manifest,
            &bytes,
            stop,
        )
        .await?;
    }
    let mut hashes = BTreeMap::new();
    for item in &closure {
        for entry in &item.object_manifest.entries {
            if let Some(size) = hashes.insert(entry.hash.clone(), entry.size) {
                anyhow::ensure!(size == entry.size, "source blob has conflicting sizes");
            }
        }
    }
    for (hash, size) in hashes {
        let read = store.clone();
        let ownership = lease.clone();
        let upload = tokio::task::spawn_blocking(move || {
            let _ownership = ownership;
            let upload = read.open_blob_verified(&hash)?;
            anyhow::ensure!(upload.size == size, "source blob size mismatch");
            Ok::<_, anyhow::Error>(upload)
        })
        .await??;
        check_stop(stop)?;
        put_file(
            client,
            epoch,
            source,
            SourceBlobKind::Content,
            upload,
            stop,
            lease.clone(),
        )
        .await?;
    }
    // At most the original bundle and two bounded base rebuilds.
    let outcome = async {
        for _ in 0..3 {
            let response = verify(client, source, stop).await?;
            if response.ready { return Ok(()); }
            anyhow::ensure!(response.bundle_needed, "source verification did not request a bundle");
            let have = response.have.into_iter().flat_map(|h| h.commits).collect::<Vec<_>>();
            let read = store.clone(); let manifest = manifest.clone(); let env = env.clone();
            let ownership=lease.clone(); let cancellation=stop.clone();
            let upload = tokio::task::spawn_blocking(move || -> anyhow::Result<_> {
                let _ownership=ownership;
                let path = read.export_bundle(&local_ci::ExecContext::new(env), &manifest, &have)?;
                let upload=read.open_bundle_verified(&path);
                // Runtime teardown may drop the async job while this export is
                // still running. Finish cleanup under the retained file lease.
                if *cancellation.borrow() {
                    read.remove_bundles(&manifest.source_id)?;
                    anyhow::bail!("resident is stopping");
                }
                upload
            }).await??;
            match put_file(client, epoch, source, SourceBlobKind::Bundle, upload, stop, lease.clone()).await {
                Ok(()) => {},
                Err(error) if error.downcast_ref::<crate::protocol::transfer::TransferError>().is_some_and(|e| matches!(e,
                    crate::protocol::transfer::TransferError::Call(crate::protocol::CallError::Remote(api)) if matches!(api.conflict_reason, Some(crate::error::ConflictReason::BundleBaseMissing { .. })))) => continue,
                Err(error) => return Err(error),
            }
        }
        anyhow::ensure!(verify(client, source, stop).await?.ready, "source bundle rebuild limit reached");
        Ok(())
    }.await;
    outcome
}
