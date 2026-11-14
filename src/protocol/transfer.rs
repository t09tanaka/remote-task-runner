//! Resumable blob and log transfer over a [`Client`].
//!
//! The offset and digest rules are in [`super::chunk`]; these helpers only turn
//! them into requests. A transfer cut by a disconnect is resumed by calling the
//! helper again on a new connection with what is already held.

use super::chunk::{
    chunk_at, clamp_chunk, ChunkError, ChunkReceiver, EventCursor, EventError, LogCursor, LogError,
    MAX_CHUNK_BYTES,
};
use super::mux::{CallError, Client};
use crate::ids::{AttemptId, CoordinatorEpoch, Digest, NodeId, RequestId, SourceId};
use crate::wire::{
    AttemptEvent, BlobGetRequest, BlobPutRequest, BlobPutResponse, Bytes, LogStream,
    ReadLogRequest, Request, Response, SchemaVersion, SourceBlobKind, WatchEventsRequest,
};
use std::time::Duration;
use thiserror::Error;

/// Answers that do not advance the transfer before it is given up.
const MAX_STALLS: u32 = 3;
/// Times the worker may report a `received` below what it already reported over
/// one upload before the transfer is given up. An honest worker loses progress
/// at most when it restarts and drops a partial blob (once, maybe twice);
/// a worker that keeps going back and forth would otherwise keep the upload
/// running forever, each forward step resetting the stall counter.
const MAX_REGRESSIONS: u32 = 2;

#[derive(Debug, Error)]
pub enum TransferError {
    #[error(transparent)]
    Call(#[from] CallError),
    #[error(transparent)]
    Chunk(#[from] ChunkError),
    #[error(transparent)]
    Log(#[from] LogError),
    #[error(transparent)]
    Event(#[from] EventError),
    /// The peer keeps answering without making progress.
    #[error("transfer is not advancing")]
    Stalled,
    #[error("unexpected answer: {0}")]
    Unexpected(&'static str),
    /// The peer announced a blob larger than the caller allows.
    #[error("blob of {total} bytes is larger than the allowed {max}")]
    TooLarge { total: u64, max: u64 },
    /// The peer announced a size other than the one the caller knows.
    #[error("blob size is {actual}, expected {expected}")]
    SizeMismatch { expected: u64, actual: u64 },
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    ReaderTask(#[from] tokio::task::JoinError),
}

/// Send `data` as blob `source_id`, starting at offset `start` (what a previous
/// attempt already delivered; 0 for a fresh one). The worker's `received` steers
/// the next chunk, so a worker that already holds more, or less, is followed.
/// Returns once the worker reports the blob complete and its digest verified.
pub async fn put_blob(
    client: &Client,
    coordinator_epoch: CoordinatorEpoch,
    source_id: &SourceId,
    data: &[u8],
    start: u64,
    chunk_bytes: usize,
    timeout: Duration,
) -> Result<BlobPutResponse, TransferError> {
    let kind = SourceBlobKind::Content;
    put_blob_as(
        client,
        coordinator_epoch,
        source_id,
        kind,
        data,
        start,
        chunk_bytes,
        timeout,
    )
    .await
}

/// [`put_blob`] for any kind of blob of a source (its manifests and its
/// bundle go the same way as its files).
#[allow(clippy::too_many_arguments)]
pub async fn put_blob_as(
    client: &Client,
    coordinator_epoch: CoordinatorEpoch,
    source_id: &SourceId,
    kind: SourceBlobKind,
    data: &[u8],
    start: u64,
    chunk_bytes: usize,
    timeout: Duration,
) -> Result<BlobPutResponse, TransferError> {
    let digest = Digest::of_bytes(data);
    let total_size = data.len() as u64;
    let chunk_bytes = chunk_bytes.clamp(1, MAX_CHUNK_BYTES);
    let mut offset = start.min(total_size);
    let mut stalls = 0;
    let mut regressions = 0;
    let mut highest = offset;
    loop {
        let chunk = chunk_at(data, offset, chunk_bytes);
        let request = Request::BlobPut(BlobPutRequest {
            schema_version: SchemaVersion::CURRENT,
            request_id: RequestId::generate(),
            coordinator_epoch,
            source_id: source_id.clone(),
            kind,
            digest: digest.clone(),
            total_size,
            offset,
            data: Bytes(chunk.to_vec()),
        });
        let Response::BlobPut(resp) = client.call(request, timeout).await? else {
            return Err(TransferError::Unexpected(
                "blob_put answered with another operation",
            ));
        };
        if resp.complete {
            return Ok(resp);
        }
        if resp.received < highest {
            regressions += 1;
            if regressions > MAX_REGRESSIONS {
                return Err(TransferError::Stalled);
            }
        }
        highest = highest.max(resp.received.min(total_size));
        if resp.received > offset {
            stalls = 0;
        } else {
            stalls += 1;
            if stalls > MAX_STALLS {
                return Err(TransferError::Stalled);
            }
        }
        offset = resp.received.min(total_size);
    }
}

/// Maximum allocation/read request of a file-backed upload, independent of file size.
pub const UPLOAD_READ_BYTES: usize = 64 * 1024;

/// Send a seekable reader with the already verified digest and exact size.
/// Disk seek/read happens on blocking threads. The receiver's offset may move
/// backwards or ahead, exactly as with `put_blob_as`; each read is bounded.
/// The receiver verifies the original digest, so changed bytes cannot publish.
#[allow(clippy::too_many_arguments)]
pub async fn put_blob_reader_as<R: std::io::Read + std::io::Seek + Send + 'static>(
    client: &Client,
    coordinator_epoch: CoordinatorEpoch,
    source_id: &SourceId,
    kind: SourceBlobKind,
    mut reader: R,
    digest: Digest,
    total_size: u64,
    start: u64,
    chunk_bytes: usize,
    timeout: Duration,
) -> Result<BlobPutResponse, TransferError> {
    let chunk_bytes = chunk_bytes.clamp(1, UPLOAD_READ_BYTES);
    let mut offset = start.min(total_size);
    let mut stalls = 0;
    let mut regressions = 0;
    loop {
        let (returned, data) = tokio::task::spawn_blocking(move || -> Result<_, TransferError> {
            use std::io::SeekFrom;
            let size = reader.seek(SeekFrom::End(0))?;
            if size != total_size {
                return Err(TransferError::SizeMismatch {
                    expected: total_size,
                    actual: size,
                });
            }
            reader.seek(SeekFrom::Start(offset))?;
            let count = (total_size - offset).min(chunk_bytes as u64) as usize;
            let mut data = vec![0; count];
            reader.read_exact(&mut data)?;
            let size = reader.seek(SeekFrom::End(0))?;
            if size != total_size {
                return Err(TransferError::SizeMismatch {
                    expected: total_size,
                    actual: size,
                });
            }
            Ok((reader, data))
        })
        .await??;
        reader = returned;
        let request = Request::BlobPut(BlobPutRequest {
            schema_version: SchemaVersion::CURRENT,
            request_id: RequestId::generate(),
            coordinator_epoch,
            source_id: source_id.clone(),
            kind,
            digest: digest.clone(),
            total_size,
            offset,
            data: Bytes(data),
        });
        let Response::BlobPut(resp) = client.call(request, timeout).await? else {
            return Err(TransferError::Unexpected(
                "blob_put answered with another operation",
            ));
        };
        if resp.digest != digest {
            return Err(TransferError::Unexpected(
                "blob_put answered for another digest",
            ));
        }
        if resp.complete {
            if resp.received != total_size {
                return Err(TransferError::Unexpected(
                    "blob_put completed at another size",
                ));
            }
            return Ok(resp);
        }
        if resp.received > total_size {
            return Err(TransferError::Unexpected(
                "blob_put offset exceeds total size",
            ));
        }
        if resp.received < offset {
            regressions += 1;
            if regressions > MAX_REGRESSIONS {
                return Err(TransferError::Stalled);
            }
        }
        if resp.received > offset {
            stalls = 0;
        } else {
            stalls += 1;
            if stalls > MAX_STALLS {
                return Err(TransferError::Stalled);
            }
        }
        offset = resp.received.min(total_size);
    }
}

/// Fetch the artifact `digest` of `attempt_id`. `prefix` is what an earlier, interrupted fetch already
/// verified into place (empty for a fresh one). The result is checked against
/// the digest before it is returned.
///
/// The announced `total_size` is the peer's word, and the result is held in
/// memory: a size above `max_total` is refused before any byte is kept, and so
/// is one that differs from `expected_size` when the caller knows it (the
/// manifest says how big the blob is). The size must not change mid-transfer.
#[allow(clippy::too_many_arguments)]
pub async fn get_blob(
    client: &Client,
    attempt_id: &AttemptId,
    digest: &Digest,
    prefix: Vec<u8>,
    max_bytes: u32,
    max_total: u64,
    expected_size: Option<u64>,
    timeout: Duration,
) -> Result<Vec<u8>, TransferError> {
    let mut buf = prefix;
    let mut receiver: Option<ChunkReceiver> = None;
    let mut stalls = 0;
    loop {
        let offset = receiver.as_ref().map_or(buf.len() as u64, |r| r.received());
        let request = Request::BlobGet(BlobGetRequest {
            schema_version: SchemaVersion::CURRENT,
            request_id: RequestId::generate(),
            attempt_id: attempt_id.clone(),
            digest: digest.clone(),
            offset,
            max_bytes: clamp_chunk(max_bytes) as u32,
        });
        let Response::BlobGet(resp) = client.call(request, timeout).await? else {
            return Err(TransferError::Unexpected(
                "blob_get answered with another operation",
            ));
        };
        if resp.digest != *digest {
            return Err(TransferError::Unexpected(
                "blob_get answered for another digest",
            ));
        }
        check_total(resp.total_size, max_total, expected_size)?;
        let rcv = match receiver.as_mut() {
            Some(r) => {
                if r.total_size() != resp.total_size {
                    return Err(TransferError::Unexpected("blob size changed mid-transfer"));
                }
                r
            }
            None => {
                let mut r = ChunkReceiver::new(digest.clone(), resp.total_size);
                if !buf.is_empty() {
                    r.accept(0, &buf)?;
                }
                receiver.insert(r)
            }
        };
        let accepted = rcv.accept(resp.offset, &resp.data.0)?;
        buf.extend_from_slice(accepted.fresh);
        if accepted.complete {
            return Ok(buf);
        }
        if accepted.fresh.is_empty() {
            stalls += 1;
            if stalls > MAX_STALLS || resp.eof {
                return Err(TransferError::Stalled);
            }
        } else {
            stalls = 0;
        }
    }
}

fn check_total(total: u64, max: u64, expected: Option<u64>) -> Result<(), TransferError> {
    if total > max {
        return Err(TransferError::TooLarge { total, max });
    }
    match expected {
        Some(expected) if expected != total => Err(TransferError::SizeMismatch {
            expected,
            actual: total,
        }),
        _ => Ok(()),
    }
}

/// One `read_log` round: the new bytes (never a byte twice), and the cursor
/// moved past them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogRead {
    pub fresh: Vec<u8>,
    pub eof: bool,
    pub log_truncated: bool,
    pub dropped_bytes: u64,
}

pub async fn read_log(
    client: &Client,
    attempt_id: &AttemptId,
    node_id: &NodeId,
    stream: LogStream,
    cursor: &mut LogCursor,
    max_bytes: u32,
    timeout: Duration,
) -> Result<LogRead, TransferError> {
    let request = Request::ReadLog(ReadLogRequest {
        schema_version: SchemaVersion::CURRENT,
        request_id: RequestId::generate(),
        attempt_id: attempt_id.clone(),
        node_id: node_id.clone(),
        stream,
        offset: cursor.next_offset(),
        max_bytes: clamp_chunk(max_bytes) as u32,
    });
    let Response::ReadLog(resp) = client.call(request, timeout).await? else {
        return Err(TransferError::Unexpected(
            "read_log answered with another operation",
        ));
    };
    let fresh = cursor
        .apply(
            resp.offset,
            &resp.data.0,
            resp.next_offset,
            resp.log_truncated,
        )?
        .to_vec();
    Ok(LogRead {
        fresh,
        eof: resp.eof,
        log_truncated: resp.log_truncated,
        dropped_bytes: resp.dropped_bytes,
    })
}

/// One `watch_events` round for the cursor's attempt: the events not seen
/// before. `seq` is per attempt, so the watch is always scoped to one.
pub async fn watch_events(
    client: &Client,
    cursor: &mut EventCursor,
    limit: u32,
    timeout: Duration,
) -> Result<Vec<AttemptEvent>, TransferError> {
    let request = Request::WatchEvents(WatchEventsRequest {
        schema_version: SchemaVersion::CURRENT,
        request_id: RequestId::generate(),
        attempt_id: Some(cursor.attempt_id().clone()),
        after_seq: cursor.after_seq(),
        limit,
    });
    let Response::WatchEvents(resp) = client.call(request, timeout).await? else {
        return Err(TransferError::Unexpected(
            "watch_events answered with another operation",
        ));
    };
    Ok(cursor
        .fresh(&resp.events, resp.next_seq)?
        .into_iter()
        .cloned()
        .collect())
}
