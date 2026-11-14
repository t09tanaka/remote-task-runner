use crate::proto_common as common;

use common::*;
use remote_task_runner::error::ErrorCode;
use remote_task_runner::ids::*;
use remote_task_runner::protocol::chunk::{ChunkReceiver, LogCursor};
use remote_task_runner::protocol::frame::{encode, Body, Frame, MAX_FRAME_LEN};
use remote_task_runner::protocol::io::{read_frame, write_frame};
use remote_task_runner::protocol::mux::{run_writer, Outbox, Shared};
use remote_task_runner::protocol::mux::{
    INBOUND_BULK_MAX_REQUESTS, INBOUND_CONTROL_MAX_REQUESTS, OUTBOX_CONTROL_MAX_FRAMES,
};
use remote_task_runner::protocol::sched::Outgoing;
use remote_task_runner::protocol::transfer::{get_blob, put_blob, read_log, TransferError};
use remote_task_runner::protocol::transport::duplex_pair;
use remote_task_runner::protocol::{accept, connect, CallError, ConnectError, Unconfirmed};
use remote_task_runner::wire::*;
use std::sync::{Arc, Mutex};
use std::time::Duration;

// --- hello ----------------------------------------------------------------

#[tokio::test]
async fn a_matching_peer_connects_and_requests_round_trip() {
    let cluster = ClusterId::generate();
    let (conn, srv) = pair(&cluster).await;
    assert_eq!(conn.negotiated.version, 1);
    assert_eq!(conn.worker.worker_id.as_str(), "w1");
    assert_eq!(conn.worker.cluster_id, cluster);
    assert_eq!(srv.negotiated, conn.negotiated);
    serve(srv.incoming, Arc::new(|r| Some(Ok(ack_response(&r)))));
    let req = ack_request();
    let resp = conn.client.call(req.clone(), T).await.unwrap();
    assert_eq!(resp, ack_response(&req));
    assert!(conn.client.ping(T).await.is_ok());
}

#[tokio::test]
async fn incompatible_versions_are_refused_on_both_sides() {
    let cluster = ClusterId::generate();
    let (a, b) = duplex_pair(4096);
    let mut cfg = client_cfg(&cluster);
    cfg.protocol_versions = vec![9];
    let (c, s) = tokio::join!(connect(a, cfg.clone()), accept(b, server_cfg(&cluster)));
    match c {
        Err(ConnectError::Refused(e)) => assert_eq!(e.code, ErrorCode::ProtocolIncompatible),
        _ => panic!("coordinator should be refused"),
    }
    match s {
        Err(ConnectError::Incompatible(e)) => assert_eq!(e.code, ErrorCode::ProtocolIncompatible),
        _ => panic!("worker should refuse"),
    }
}

#[tokio::test]
async fn a_different_cluster_is_refused() {
    let (a, b) = duplex_pair(4096);
    let (c, s) = tokio::join!(
        connect(a, client_cfg(&ClusterId::generate())),
        accept(b, server_cfg(&ClusterId::generate()))
    );
    match c {
        Err(ConnectError::Refused(e)) => {
            assert_eq!(e.code, ErrorCode::ProtocolIncompatible);
            assert!(e.message.contains("cluster_id"));
        }
        _ => panic!("expected refusal"),
    }
    assert!(s.is_err());
}

#[tokio::test]
async fn a_required_feature_the_worker_lacks_is_refused() {
    let cluster = ClusterId::generate();
    let (a, b) = duplex_pair(4096);
    let mut cfg = client_cfg(&cluster);
    cfg.required_features = vec!["telepathy".into()];
    cfg.features.push("telepathy".into());
    let (c, _s) = tokio::join!(connect(a, cfg.clone()), accept(b, server_cfg(&cluster)));
    // The worker does not know the feature, so it is not negotiated; the
    // coordinator refuses the answer.
    match c {
        Err(ConnectError::Incompatible(e)) => assert!(e.message.contains("telepathy")),
        _ => panic!("expected incompatible"),
    }
}

#[tokio::test]
async fn another_schema_version_in_hello_is_refused_with_protocol_incompatible() {
    let cluster = ClusterId::generate();
    let (mut raw, b) = duplex_pair(4096);
    let server = tokio::spawn(async move { accept(b, server_cfg(&cluster.clone())).await });
    let cluster2 = ClusterId::generate();
    let hello = HelloRequest {
        schema_version: SchemaVersion(99),
        request_id: RequestId::generate(),
        cluster_id: cluster2,
        coordinator_id: CoordinatorId::generate(),
        coordinator_epoch: CoordinatorEpoch(1),
        protocol_versions: vec![1],
        features: vec![],
    };
    let f = Frame::message(0, Message::Request(Request::Hello(hello)));
    write_frame(&mut raw.writer, &f, MAX_FRAME_LEN)
        .await
        .unwrap();
    let reply = read_frame(&mut raw.reader, MAX_FRAME_LEN)
        .await
        .unwrap()
        .unwrap();
    match reply.body {
        Body::Message(Message::Error(e)) => {
            assert_eq!(e.error.code, ErrorCode::ProtocolIncompatible)
        }
        other => panic!("{other:?}"),
    }
    assert!(server.await.unwrap().is_err());
    // The worker closed the connection.
    assert!(read_frame(&mut raw.reader, MAX_FRAME_LEN)
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn a_worker_must_get_hello_first() {
    let cluster = ClusterId::generate();
    let (mut raw, b) = duplex_pair(4096);
    let server = tokio::spawn(async move { accept(b, server_cfg(&cluster)).await });
    write_frame(&mut raw.writer, &Frame::ping(1), MAX_FRAME_LEN)
        .await
        .unwrap();
    let reply = read_frame(&mut raw.reader, MAX_FRAME_LEN)
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(reply.body, Body::Message(Message::Error(_))));
    assert!(matches!(
        server.await.unwrap(),
        Err(ConnectError::Unexpected(_))
    ));
}

#[tokio::test]
async fn a_lying_worker_answer_is_refused_by_the_coordinator() {
    let cluster = ClusterId::generate();
    let (a, mut raw) = duplex_pair(4096);
    let c2 = cluster.clone();
    let coordinator = tokio::spawn(async move { connect(a, client_cfg(&c2)).await });
    let req = read_frame(&mut raw.reader, MAX_FRAME_LEN)
        .await
        .unwrap()
        .unwrap();
    let Body::Message(Message::Request(Request::Hello(h))) = req.body else {
        panic!("expected hello")
    };
    let mut resp = identity("w1").response(
        &cluster,
        &remote_task_runner::protocol::hello::Negotiated {
            version: 77,
            features: vec![],
        },
    );
    resp.protocol_version = 77;
    let reply = Frame::message(
        req.stream,
        Message::Response {
            request_id: h.request_id,
            response: Response::Hello(resp),
        },
    );
    write_frame(&mut raw.writer, &reply, MAX_FRAME_LEN)
        .await
        .unwrap();
    assert!(matches!(
        coordinator.await.unwrap(),
        Err(ConnectError::Incompatible(_))
    ));
}

#[tokio::test]
async fn hello_times_out_and_a_silent_close_is_reported() {
    let cluster = ClusterId::generate();
    let (a, _keep) = duplex_pair(4096);
    let mut cfg = client_cfg(&cluster);
    cfg.hello_timeout = Duration::from_millis(50);
    match connect(a, cfg.clone()).await {
        Err(ConnectError::TimedOut) => {}
        _ => panic!("expected timeout"),
    }
    let (a, b) = duplex_pair(4096);
    drop(b);
    assert!(matches!(
        connect(a, client_cfg(&cluster)).await,
        Err(ConnectError::Closed) | Err(ConnectError::Frame(_))
    ));
    let e = ConnectError::TimedOut.api_error();
    assert_eq!(e.code, ErrorCode::CommunicationUnknown);
}

// --- unconfirmed communication --------------------------------------------

#[tokio::test]
async fn a_disconnect_leaves_outstanding_requests_unconfirmed() {
    let cluster = ClusterId::generate();
    let (conn, srv) = pair(&cluster).await;
    let mut incoming = srv.incoming;
    let caller = {
        let client = conn.client.clone();
        tokio::spawn(async move { client.call(ack_request(), T).await })
    };
    // The worker received the request and then died without answering.
    let inbound = incoming.next().await.unwrap();
    // Dying means no answer of any kind (a dropped responder would answer
    // INTERNAL), so the responder is leaked rather than dropped.
    std::mem::forget(inbound);
    drop(incoming);
    let err = caller.await.unwrap().unwrap_err();
    assert!(matches!(
        err,
        CallError::Unconfirmed(Unconfirmed::Disconnected)
    ));
    assert!(err.is_unconfirmed());
    assert_eq!(err.to_api_error().code, ErrorCode::CommunicationUnknown);
    // The next call on the dead connection was certainly not sent.
    let err = conn.client.call(ack_request(), T).await.unwrap_err();
    assert!(matches!(err, CallError::NotSent), "{err:?}");
    assert!(!err.is_unconfirmed());
    assert!(conn.client.is_closed());
}

#[tokio::test]
async fn a_timeout_is_unconfirmed_and_a_late_answer_is_ignored() {
    let cluster = ClusterId::generate();
    let (conn, srv) = pair(&cluster).await;
    let mut incoming = srv.incoming;
    let err = {
        let call = conn.client.call(ack_request(), Duration::from_millis(50));
        let (r, _) = tokio::join!(call, async {
            // Hold the request past the caller's timeout.
            tokio::time::sleep(Duration::from_millis(200)).await;
        });
        r.unwrap_err()
    };
    assert!(matches!(err, CallError::Unconfirmed(Unconfirmed::TimedOut)));
    let late = incoming.next().await.unwrap();
    let resp = ack_response(&late.request);
    late.responder.respond(resp).await.unwrap();
    // The connection is unharmed: the late reply is dropped, new calls work.
    serve(incoming, Arc::new(|r| Some(Ok(ack_response(&r)))));
    assert!(conn.client.call(ack_request(), T).await.is_ok());
    assert!(conn.client.ping(T).await.is_ok());
}

#[tokio::test]
async fn a_worker_error_comes_back_as_a_remote_error_not_unconfirmed() {
    let cluster = ClusterId::generate();
    let (conn, srv) = pair(&cluster).await;
    serve(
        srv.incoming,
        Arc::new(|_| {
            Some(Err(remote_task_runner::error::ApiError::new(
                ErrorCode::StaleEpoch,
                "old",
            )))
        }),
    );
    match conn.client.call(ack_request(), T).await.unwrap_err() {
        CallError::Remote(e) => assert_eq!(e.code, ErrorCode::StaleEpoch),
        other => panic!("{other:?}"),
    }
}

#[tokio::test]
async fn a_request_over_the_frame_limit_is_refused_before_sending() {
    let cluster = ClusterId::generate();
    let (conn, srv) = pair(&cluster).await;
    serve(srv.incoming, Arc::new(|r| Some(Ok(ack_response(&r)))));
    let big = Request::BlobPut(BlobPutRequest {
        schema_version: SchemaVersion::CURRENT,
        request_id: RequestId::generate(),
        coordinator_epoch: CoordinatorEpoch(1),
        source_id: SourceId::generate(),
        kind: SourceBlobKind::Content,
        digest: Digest::of_bytes(b""),
        total_size: 0,
        offset: 0,
        data: Bytes(vec![0u8; MAX_FRAME_LEN]),
    });
    assert!(matches!(
        conn.client.call(big, T).await.unwrap_err(),
        CallError::Frame(_)
    ));
    // The connection is still fine.
    assert!(conn.client.call(ack_request(), T).await.is_ok());
}

// --- control before bulk ---------------------------------------------------

fn blob_item(i: u32) -> Outgoing {
    let req = Request::BlobPut(BlobPutRequest {
        schema_version: SchemaVersion::CURRENT,
        request_id: RequestId::generate(),
        coordinator_epoch: CoordinatorEpoch(1),
        source_id: SourceId::generate(),
        kind: SourceBlobKind::Content,
        digest: Digest::of_bytes(b""),
        total_size: 0,
        offset: 0,
        data: Bytes(vec![i as u8; 100_000]),
    });
    let frame = Frame::message(i + 1, Message::Request(req));
    Outgoing {
        kind: frame.kind,
        bytes: encode(&frame, MAX_FRAME_LEN).unwrap(),
    }
}

#[tokio::test]
async fn a_heartbeat_overtakes_a_queue_of_large_blob_frames() {
    // A tiny link makes the writer block inside the first blob frame; thirty
    // more are queued behind it. The heartbeat must come out right after the
    // frame in flight, not after the queue.
    let (a, mut b) = duplex_pair(2048);
    let outbox = Outbox::new();
    tokio::spawn(run_writer(
        a.writer,
        Arc::clone(&outbox),
        Shared::new(MAX_FRAME_LEN),
    ));
    let producer = {
        let outbox = Arc::clone(&outbox);
        tokio::spawn(async move {
            for i in 0..30 {
                outbox.send(blob_item(i)).await.unwrap();
            }
        })
    };
    tokio::time::sleep(Duration::from_millis(100)).await;
    outbox
        .send_control(&Frame::ping(500), MAX_FRAME_LEN)
        .unwrap();
    let mut ping_at = None;
    for n in 0..40 {
        let f = read_frame(&mut b.reader, MAX_FRAME_LEN)
            .await
            .unwrap()
            .unwrap();
        if matches!(f.body, Body::Ping) {
            ping_at = Some(n);
            break;
        }
    }
    let ping_at = ping_at.expect("the ping was written");
    assert!(ping_at <= 1, "ping came after {ping_at} blob frames");
    producer.abort();
}

#[tokio::test]
async fn pings_stay_fast_while_a_blob_transfers_over_a_real_connection() {
    let cluster = ClusterId::generate();
    let (a, b) = duplex_pair(16 * 1024);
    let (c, s) = tokio::join!(
        connect(a, client_cfg(&cluster)),
        accept(b, server_cfg(&cluster))
    );
    let (conn, srv) = (c.unwrap(), s.unwrap());
    let state: Arc<Mutex<Option<ChunkReceiver>>> = Arc::new(Mutex::new(None));
    serve(srv.incoming, blob_worker(Arc::clone(&state)));
    let data = pseudo_random(3 * 1024 * 1024);
    let client = conn.client.clone();
    let source = SourceId::generate();
    let put = {
        let data = data.clone();
        tokio::spawn(async move {
            put_blob(
                &client,
                CoordinatorEpoch(1),
                &source,
                &data,
                0,
                64 * 1024,
                T,
            )
            .await
        })
    };
    let mut worst = Duration::ZERO;
    while !put.is_finished() {
        worst = worst.max(conn.client.ping(T).await.unwrap());
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert!(put.await.unwrap().unwrap().complete);
    // Generous: one 64 KiB frame over an in-memory link, not the 3 MiB blob.
    assert!(worst < Duration::from_secs(2), "worst ping {worst:?}");
}

// --- blob transfer ---------------------------------------------------------

/// A worker that stores one blob, keeping its state across connections.
fn blob_worker(state: Arc<Mutex<Option<ChunkReceiver>>>) -> Handler {
    Arc::new(move |req| match req {
        Request::BlobPut(r) => {
            let mut guard = state.lock().unwrap();
            let rcv =
                guard.get_or_insert_with(|| ChunkReceiver::new(r.digest.clone(), r.total_size));
            Some(match rcv.accept(r.offset, &r.data.0) {
                Ok(a) => Ok(Response::BlobPut(BlobPutResponse {
                    schema_version: SchemaVersion::CURRENT,
                    digest: r.digest,
                    received: a.received,
                    complete: a.complete,
                })),
                Err(remote_task_runner::protocol::chunk::ChunkError::Gap { received }) => {
                    Ok(Response::BlobPut(BlobPutResponse {
                        schema_version: SchemaVersion::CURRENT,
                        digest: r.digest,
                        received,
                        complete: false,
                    }))
                }
                Err(e) => Err(remote_task_runner::error::ApiError::new(
                    ErrorCode::Conflict,
                    e.to_string(),
                )),
            })
        }
        _ => None,
    })
}

#[tokio::test]
async fn an_interrupted_blob_upload_resumes_from_the_workers_offset() {
    let cluster = ClusterId::generate();
    let data = pseudo_random(1000);
    let source = SourceId::generate();
    let state: Arc<Mutex<Option<ChunkReceiver>>> = Arc::new(Mutex::new(None));

    // First connection: the worker takes three chunks and then dies.
    let (conn, srv) = pair(&cluster).await;
    let seen = Arc::new(Mutex::new(0u32));
    let handler: Handler = {
        let inner = blob_worker(Arc::clone(&state));
        let seen = Arc::clone(&seen);
        Arc::new(move |req| {
            let mut n = seen.lock().unwrap();
            *n += 1;
            if *n > 3 {
                None
            } else {
                inner(req)
            }
        })
    };
    let worker = serve(srv.incoming, handler);
    let err = put_blob(
        &conn.client,
        CoordinatorEpoch(1),
        &source,
        &data,
        0,
        100,
        Duration::from_millis(100),
    )
    .await
    .unwrap_err();
    assert!(
        matches!(err, TransferError::Call(CallError::Unconfirmed(_))),
        "{err:?}"
    );
    worker.abort();
    assert_eq!(state.lock().unwrap().as_ref().unwrap().received(), 300);

    // Second connection: the sender starts from 0 again; the worker's offset
    // steers it, and only the missing 700 bytes count as new.
    let (conn, srv) = pair(&cluster).await;
    serve(srv.incoming, blob_worker(Arc::clone(&state)));
    let done = put_blob(&conn.client, CoordinatorEpoch(1), &source, &data, 0, 100, T)
        .await
        .unwrap();
    assert!(done.complete);
    assert_eq!(done.received, 1000);
}

#[tokio::test]
async fn a_blob_with_the_wrong_digest_is_refused_and_not_completed() {
    let cluster = ClusterId::generate();
    let (conn, srv) = pair(&cluster).await;
    let state = Arc::new(Mutex::new(None));
    serve(srv.incoming, blob_worker(state));
    let data = pseudo_random(64);
    let lie = Request::BlobPut(BlobPutRequest {
        schema_version: SchemaVersion::CURRENT,
        request_id: RequestId::generate(),
        coordinator_epoch: CoordinatorEpoch(1),
        source_id: SourceId::generate(),
        kind: SourceBlobKind::Content,
        digest: Digest::of_bytes(b"not these bytes"),
        total_size: 64,
        offset: 0,
        data: Bytes(data),
    });
    match conn.client.call(lie, T).await.unwrap_err() {
        CallError::Remote(e) => {
            assert_eq!(e.code, ErrorCode::Conflict);
            assert!(e.message.contains("digest mismatch"));
        }
        other => panic!("{other:?}"),
    }
}

#[tokio::test]
async fn a_blob_download_resumes_from_a_verified_prefix_and_checks_the_digest() {
    let cluster = ClusterId::generate();
    let (conn, srv) = pair(&cluster).await;
    let data = Arc::new(pseudo_random(5000));
    let digest = Digest::of_bytes(&data);
    let store = Arc::clone(&data);
    serve(
        srv.incoming,
        Arc::new(move |req| match req {
            Request::BlobGet(r) => {
                let start = (r.offset as usize).min(store.len());
                let end = (start + r.max_bytes as usize).min(store.len());
                Some(Ok(Response::BlobGet(BlobGetResponse {
                    schema_version: SchemaVersion::CURRENT,
                    digest: r.digest,
                    offset: start as u64,
                    total_size: store.len() as u64,
                    data: Bytes(store[start..end].to_vec()),
                    eof: end == store.len(),
                })))
            }
            _ => None,
        }),
    );
    let whole = get_blob(
        &conn.client,
        &AttemptId::generate(),
        &digest,
        vec![],
        700,
        1 << 20,
        Some(5000),
        T,
    )
    .await
    .unwrap();
    assert_eq!(whole, *data);
    let resumed = get_blob(
        &conn.client,
        &AttemptId::generate(),
        &digest,
        data[..1234].to_vec(),
        700,
        1 << 20,
        None,
        T,
    )
    .await
    .unwrap();
    assert_eq!(resumed, *data);
    // A prefix that is not the start of this blob fails the digest check.
    let mut bad = data[..1234].to_vec();
    bad[0] ^= 1;
    let err = get_blob(
        &conn.client,
        &AttemptId::generate(),
        &digest,
        bad,
        700,
        1 << 20,
        None,
        T,
    )
    .await
    .unwrap_err();
    assert!(matches!(err, TransferError::Chunk(_)), "{err:?}");
}

// --- logs ------------------------------------------------------------------

#[tokio::test]
async fn non_utf8_log_bytes_round_trip_and_a_reread_is_harmless() {
    let cluster = ClusterId::generate();
    let (conn, srv) = pair(&cluster).await;
    let mut spool = vec![0xff, 0xfe, 0x00, 0x80, b'\n', 0xc3, 0x28, 0xf0, 0x9f, 0x92];
    spool.extend(pseudo_random(2000));
    let store = Arc::new(spool.clone());
    serve(
        srv.incoming,
        Arc::new(move |req| match req {
            Request::ReadLog(r) => {
                let start = (r.offset as usize).min(store.len());
                let end = (start + r.max_bytes as usize).min(store.len());
                Some(Ok(Response::ReadLog(ReadLogResponse {
                    schema_version: SchemaVersion::CURRENT,
                    offset: start as u64,
                    data: Bytes(store[start..end].to_vec()),
                    next_offset: end as u64,
                    eof: end == store.len(),
                    log_truncated: false,
                    dropped_bytes: 0,
                })))
            }
            _ => None,
        }),
    );
    let attempt = AttemptId::generate();
    let node = NodeId::parse("build").unwrap();
    let mut cursor = LogCursor::new(0);
    let mut got = Vec::new();
    // Bounded: a cursor that makes no progress must fail this test, not loop.
    let rounds = spool.len() / 300 + 2;
    let mut finished = false;
    for _ in 0..rounds {
        let before = cursor.next_offset();
        let r = read_log(
            &conn.client,
            &attempt,
            &node,
            LogStream::Stdout,
            &mut cursor,
            300,
            T,
        )
        .await
        .unwrap();
        got.extend_from_slice(&r.fresh);
        if r.eof {
            finished = true;
            break;
        }
        assert!(cursor.next_offset() > before, "the cursor made no progress");
    }
    assert!(finished, "the log did not reach eof in {rounds} rounds");
    assert_eq!(got, spool);
    assert!(std::str::from_utf8(&got).is_err());
    // After a reconnect the reader may ask again from an older offset; the
    // cursor drops what it already has.
    let mut again = LogCursor::new(0);
    let r = read_log(
        &conn.client,
        &attempt,
        &node,
        LogStream::Stdout,
        &mut again,
        300,
        T,
    )
    .await
    .unwrap();
    assert_eq!(r.fresh, spool[..300]);
    let r2 = read_log(
        &conn.client,
        &attempt,
        &node,
        LogStream::Stdout,
        &mut again,
        300,
        T,
    )
    .await
    .unwrap();
    assert_eq!(r2.fresh, spool[300..600]);
}

// --- overload ---------------------------------------------------------------

fn blob_get_request() -> Request {
    Request::BlobGet(BlobGetRequest {
        schema_version: SchemaVersion::CURRENT,
        request_id: RequestId::generate(),
        attempt_id: AttemptId::generate(),
        digest: Digest::of_bytes(b"x"),
        offset: 0,
        max_bytes: 1,
    })
}

fn blob_put_request(bytes: usize) -> Request {
    Request::BlobPut(BlobPutRequest {
        schema_version: SchemaVersion::CURRENT,
        request_id: RequestId::generate(),
        coordinator_epoch: CoordinatorEpoch(1),
        source_id: SourceId::generate(),
        kind: SourceBlobKind::Content,
        digest: Digest::of_bytes(b"x"),
        total_size: bytes as u64,
        offset: 0,
        data: Bytes(pseudo_random(bytes)),
    })
}

type Calls = Vec<tokio::task::JoinHandle<Result<Response, CallError>>>;

// Arrival and draining each get a bounded budget. Calls must outlive both
// phases so a loaded machine cannot turn an overload result into a timeout.
const FLOOD_PHASE_TIMEOUT: Duration = Duration::from_secs(30);
const FLOOD_CALL_TIMEOUT: Duration = Duration::from_secs(90);

/// Call `requests` at once on a worker that is not taking anything off its queues.
fn start_flood(client: &remote_task_runner::protocol::Client, requests: Vec<Request>) -> Calls {
    requests
        .into_iter()
        .map(|r| {
            let c = client.clone();
            tokio::spawn(async move { c.call(r, FLOOD_CALL_TIMEOUT).await })
        })
        .collect()
}

/// Wait until `n` of the calls have ended. Before anything is taken off the
/// queue only refused calls can end, so this waits for `n` refusals.
async fn wait_for_refusals(calls: &Calls, n: usize) {
    tokio::time::timeout(FLOOD_PHASE_TIMEOUT, async {
        while calls.iter().filter(|c| c.is_finished()).count() < n {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("the refusals did not arrive");
}

/// Drain until every caller has a result, rather than treating a quiet interval
/// as proof that all requests arrived. Dropping each accepted request answers
/// it INTERNAL; refused requests answer OVERLOADED.
async fn finish_flood(
    incoming: &mut remote_task_runner::protocol::Incoming,
    calls: Calls,
) -> (usize, usize) {
    let total = calls.len();
    let mut pending = tokio::task::JoinSet::new();
    for call in calls {
        pending.spawn(call);
    }
    tokio::time::timeout(FLOOD_PHASE_TIMEOUT, async {
        let (mut busy, mut internal, mut accepted) = (0, 0, 0);
        let mut incoming_open = true;
        while !pending.is_empty() {
            tokio::select! {
                inbound = incoming.next(), if incoming_open => {
                    if let Some(inbound) = inbound {
                        accepted += 1;
                        drop(inbound);
                    } else {
                        incoming_open = false;
                    }
                }
                result = pending.join_next() => {
                    match result.unwrap().unwrap().unwrap() {
                        Err(CallError::Remote(e)) if e.code == ErrorCode::Overloaded => busy += 1,
                        Err(CallError::Remote(e)) if e.code == ErrorCode::Internal => internal += 1,
                        other => panic!("{other:?}"),
                    }
                }
            }
        }
        assert_eq!(
            internal, accepted,
            "every accepted request was answered once"
        );
        assert_eq!(busy + accepted, total, "every caller was accounted for");
        (busy, accepted)
    })
    .await
    .expect("the flood did not finish")
}

#[tokio::test]
async fn finishing_a_flood_waits_for_every_call_to_arrive() {
    let cluster = ClusterId::generate();
    let (conn, srv) = pair(&cluster).await;
    let mut incoming = srv.incoming;
    let (answered, wait_for_answer) = tokio::sync::oneshot::channel();
    let first = {
        let client = conn.client.clone();
        tokio::spawn(async move {
            let result = client.call(blob_get_request(), FLOOD_CALL_TIMEOUT).await;
            answered.send(()).unwrap();
            result
        })
    };
    let second = tokio::spawn(async move {
        // The second request cannot arrive until draining answers the first.
        // A helper that stops at a quiet interval leaves this call stranded.
        wait_for_answer.await.unwrap();
        conn.client
            .call(blob_get_request(), FLOOD_CALL_TIMEOUT)
            .await
    });
    assert_eq!(
        finish_flood(&mut incoming, vec![first, second]).await,
        (0, 2)
    );
}

#[tokio::test]
async fn a_bulk_flood_is_refused_beyond_the_limit_while_heartbeats_and_the_queue_keep_working() {
    let cluster = ClusterId::generate();
    let (conn, srv) = pair(&cluster).await;
    let mut incoming = srv.incoming;
    let extra = 20;
    let requests = (0..INBOUND_BULK_MAX_REQUESTS + extra)
        .map(|_| blob_get_request())
        .collect();
    let calls = start_flood(&conn.client, requests);
    // The reader is never stopped by the flood: pings still come back.
    conn.client.ping(T).await.unwrap();
    // Nothing is taken off the queue until every surplus request was refused.
    wait_for_refusals(&calls, extra).await;
    let (busy, accepted) = finish_flood(&mut incoming, calls).await;
    assert_eq!(busy, extra);
    assert_eq!(accepted, INBOUND_BULK_MAX_REQUESTS);
    // Serving frees room: the next request is accepted and answered.
    let again = {
        let c = conn.client.clone();
        tokio::spawn(async move { c.call(blob_get_request(), T).await })
    };
    let inbound = incoming.next().await.unwrap();
    let Request::BlobGet(g) = &inbound.request else {
        panic!("not a blob_get")
    };
    let answer = Response::BlobGet(BlobGetResponse {
        schema_version: SchemaVersion::CURRENT,
        digest: g.digest.clone(),
        offset: 0,
        total_size: 0,
        data: Bytes(vec![]),
        eof: true,
    });
    inbound.responder.respond(answer).await.unwrap();
    assert!(again.await.unwrap().is_ok());
}

#[tokio::test]
async fn bulk_bytes_are_bounded_too() {
    let cluster = ClusterId::generate();
    let (conn, srv) = pair(&cluster).await;
    let mut incoming = srv.incoming;
    // Few requests, each big: the byte budget, not the count, is what bites.
    let chunk = 700_000;
    let count = 14;
    assert!(count < INBOUND_BULK_MAX_REQUESTS);
    let requests: Vec<_> = (0..count).map(|_| blob_put_request(chunk)).collect();
    let calls = start_flood(&conn.client, requests);
    // Fewer requests than the count limit: a refusal can only be the bytes.
    wait_for_refusals(&calls, 1).await;
    let (busy, accepted) = finish_flood(&mut incoming, calls).await;
    assert!(busy >= 1, "no request was refused");
    assert!(accepted >= 1, "everything was refused");
    assert_eq!(busy + accepted, count);
}

#[tokio::test]
async fn a_control_backlog_beyond_the_limit_closes_the_connection() {
    let cluster = ClusterId::generate();
    let (conn, srv) = pair(&cluster).await;
    let _incoming = srv.incoming;
    let calls: Vec<_> = (0..INBOUND_CONTROL_MAX_REQUESTS + 50)
        .map(|_| {
            let c = conn.client.clone();
            tokio::spawn(async move { c.call(ack_request(), T).await })
        })
        .collect();
    for call in calls {
        let err = call.await.unwrap().unwrap_err();
        assert!(
            matches!(err, CallError::Unconfirmed(Unconfirmed::Disconnected)),
            "{err:?}"
        );
    }
    assert!(conn.client.is_closed());
}

#[tokio::test]
async fn a_peer_that_never_reads_cannot_grow_the_control_queue_without_bound() {
    // A writer whose peer never reads: it blocks on the first frame, so
    // everything after piles up in the control queue.
    let (writer_end, _peer) = tokio::io::duplex(16);
    let shared = Shared::new(MAX_FRAME_LEN);
    let outbox = Outbox::new();
    tokio::spawn(run_writer(
        writer_end,
        Arc::clone(&outbox),
        Arc::clone(&shared),
    ));
    let mut refused = None;
    for i in 0..OUTBOX_CONTROL_MAX_FRAMES + 10 {
        if outbox
            .send_control(&Frame::ping(i as u32), MAX_FRAME_LEN)
            .is_err()
        {
            refused = Some(i);
            break;
        }
    }
    let at = refused.expect("the queue accepted frames without limit");
    assert!(at <= OUTBOX_CONTROL_MAX_FRAMES + 1, "{at}");
    // The connection is cut even though the writer is stuck mid-write.
    tokio::time::timeout(T, async {
        while !shared.is_closed() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the connection was not closed");
    // And nothing more is accepted.
    assert!(outbox.send_control(&Frame::ping(0), MAX_FRAME_LEN).is_err());
}

#[tokio::test]
async fn a_request_dropped_without_an_answer_is_answered_internal() {
    let cluster = ClusterId::generate();
    let (conn, srv) = pair(&cluster).await;
    let mut incoming = srv.incoming;
    let caller = {
        let c = conn.client.clone();
        tokio::spawn(async move { c.call(ack_request(), T).await })
    };
    let inbound = incoming.next().await.unwrap();
    drop(inbound);
    match caller.await.unwrap() {
        Err(CallError::Remote(e)) => assert_eq!(e.code, ErrorCode::Internal),
        other => panic!("{other:?}"),
    }
    // The connection is fine.
    assert!(conn.client.ping(T).await.is_ok());
}

#[tokio::test]
async fn an_answer_that_cannot_be_encoded_becomes_an_error_answer() {
    let cluster = ClusterId::generate();
    let (conn, srv) = pair(&cluster).await;
    let mut incoming = srv.incoming;
    let caller = {
        let c = conn.client.clone();
        tokio::spawn(async move { c.call(blob_get_request(), T).await })
    };
    let inbound = incoming.next().await.unwrap();
    let huge = Response::BlobGet(BlobGetResponse {
        schema_version: SchemaVersion::CURRENT,
        digest: Digest::of_bytes(b"x"),
        offset: 0,
        total_size: 4 << 20,
        data: Bytes(vec![0; 3 << 20]),
        eof: true,
    });
    assert!(inbound.responder.respond(huge).await.is_err());
    match caller.await.unwrap() {
        Err(CallError::Remote(e)) => {
            assert_eq!(e.code, ErrorCode::Internal);
            assert!(e.message.contains("encoded"), "{e:?}");
        }
        other => panic!("{other:?}"),
    }
}

// --- transfer limits --------------------------------------------------------

/// A worker that answers every `blob_get` with a made-up total size.
fn lying_size_worker(totals: Vec<u64>) -> Handler {
    let calls = Arc::new(Mutex::new(0usize));
    Arc::new(move |req| match req {
        Request::BlobGet(r) => {
            let mut n = calls.lock().unwrap();
            let total = totals[(*n).min(totals.len() - 1)];
            *n += 1;
            Some(Ok(Response::BlobGet(BlobGetResponse {
                schema_version: SchemaVersion::CURRENT,
                digest: r.digest,
                offset: r.offset,
                total_size: total,
                data: Bytes(vec![0; 10]),
                eof: false,
            })))
        }
        _ => None,
    })
}

#[tokio::test]
async fn a_blob_larger_than_allowed_is_refused_before_anything_is_kept() {
    let cluster = ClusterId::generate();
    let digest = Digest::of_bytes(b"whatever");
    for announced in [(1u64 << 20) + 1, u64::MAX] {
        let (conn, srv) = pair(&cluster).await;
        serve(srv.incoming, lying_size_worker(vec![announced]));
        let err = get_blob(
            &conn.client,
            &AttemptId::generate(),
            &digest,
            vec![],
            100,
            1 << 20,
            None,
            T,
        )
        .await
        .unwrap_err();
        assert!(
            matches!(err, TransferError::TooLarge { total, max } if total == announced && max == 1 << 20),
            "{err:?}"
        );
    }
}

#[tokio::test]
async fn a_blob_whose_size_differs_from_the_expected_one_or_changes_is_refused() {
    let cluster = ClusterId::generate();
    let digest = Digest::of_bytes(b"whatever");
    let (conn, srv) = pair(&cluster).await;
    serve(srv.incoming, lying_size_worker(vec![5000]));
    let err = get_blob(
        &conn.client,
        &AttemptId::generate(),
        &digest,
        vec![],
        100,
        1 << 20,
        Some(4000),
        T,
    )
    .await
    .unwrap_err();
    assert!(
        matches!(
            err,
            TransferError::SizeMismatch {
                expected: 4000,
                actual: 5000
            }
        ),
        "{err:?}"
    );
    // A size that moves between rounds cannot be trusted either.
    let (conn, srv) = pair(&cluster).await;
    serve(srv.incoming, lying_size_worker(vec![5000, 6000]));
    let err = get_blob(
        &conn.client,
        &AttemptId::generate(),
        &digest,
        vec![],
        100,
        1 << 20,
        None,
        T,
    )
    .await
    .unwrap_err();
    assert!(matches!(err, TransferError::Unexpected(_)), "{err:?}");
}

/// A worker whose `received` follows a script, then the honest receiver.
fn scripted_put_worker(script: Vec<u64>, state: Arc<Mutex<Option<ChunkReceiver>>>) -> Handler {
    let honest = blob_worker(state);
    let step = Arc::new(Mutex::new(0usize));
    Arc::new(move |req| {
        let Request::BlobPut(r) = &req else {
            return None;
        };
        let mut n = step.lock().unwrap();
        let i = *n;
        *n += 1;
        match script.get(i) {
            Some(&received) => Some(Ok(Response::BlobPut(BlobPutResponse {
                schema_version: SchemaVersion::CURRENT,
                digest: r.digest.clone(),
                received,
                complete: false,
            }))),
            None => honest(req),
        }
    })
}

#[tokio::test]
async fn an_upload_that_keeps_going_backwards_stalls_but_one_restart_is_tolerated() {
    let cluster = ClusterId::generate();
    let data = pseudo_random(1000);
    let source = SourceId::generate();
    // Forward, back, forward, back, ...: every forward step resets the plain
    // stall counter, so only counting the regressions ends this.
    let script: Vec<u64> = (1..50).flat_map(|i| [i * 10, 0]).collect();
    let (conn, srv) = pair(&cluster).await;
    serve(
        srv.incoming,
        scripted_put_worker(script, Arc::new(Mutex::new(None))),
    );
    let err = put_blob(&conn.client, CoordinatorEpoch(1), &source, &data, 0, 10, T)
        .await
        .unwrap_err();
    assert!(matches!(err, TransferError::Stalled), "{err:?}");

    // A single loss of progress (the worker restarted) is followed.
    let (conn, srv) = pair(&cluster).await;
    serve(
        srv.incoming,
        scripted_put_worker(vec![100, 0], Arc::new(Mutex::new(None))),
    );
    let done = put_blob(&conn.client, CoordinatorEpoch(1), &source, &data, 0, 100, T)
        .await
        .unwrap();
    assert!(done.complete);
}

#[tokio::test]
async fn an_answer_that_cannot_be_read_is_unconfirmed_and_not_the_peers_refusal() {
    // The worker may have done what was asked: what this side makes of an
    // answer of another schema version, or to another operation, is not an
    // error the worker gave.
    let another_schema: Handler = Arc::new(|req| {
        let Response::AckCollected(mut r) = ack_response(&req) else {
            unreachable!()
        };
        r.schema_version = SchemaVersion(99);
        Some(Ok(Response::AckCollected(r)))
    });
    let another_operation: Handler = Arc::new(|_| {
        Some(Ok(Response::BlobPut(BlobPutResponse {
            schema_version: SchemaVersion::CURRENT,
            digest: Digest::of_bytes(b"x"),
            received: 0,
            complete: false,
        })))
    });
    for handler in [another_schema, another_operation] {
        let cluster = ClusterId::generate();
        let (conn, srv) = pair(&cluster).await;
        serve(srv.incoming, handler);
        let err = conn.client.call(ack_request(), T).await.unwrap_err();
        assert!(
            matches!(err, CallError::Unconfirmed(Unconfirmed::Unreadable)),
            "{err:?}"
        );
    }
}

struct BoundedUploadReader {
    inner: std::io::Cursor<Vec<u8>>,
    executor: std::thread::ThreadId,
    reads: Arc<Mutex<Vec<usize>>>,
}
impl std::io::Read for BoundedUploadReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        assert_ne!(
            std::thread::current().id(),
            self.executor,
            "upload blocked executor"
        );
        assert!(buf.len() <= 64 * 1024, "unbounded upload read");
        self.reads.lock().unwrap().push(buf.len());
        std::io::Read::read(&mut self.inner, buf)
    }
}
impl std::io::Seek for BoundedUploadReader {
    fn seek(&mut self, from: std::io::SeekFrom) -> std::io::Result<u64> {
        assert_ne!(
            std::thread::current().id(),
            self.executor,
            "upload seek blocked executor"
        );
        std::io::Seek::seek(&mut self.inner, from)
    }
}
#[tokio::test]
async fn streamed_upload_is_bounded_off_executor_and_follows_receiver_offsets() {
    use remote_task_runner::protocol::transfer::put_blob_reader_as;
    let data = pseudo_random(3 * 64 * 1024 + 17);
    let digest = Digest::of_bytes(&data);
    let source = SourceId::generate();
    let cluster = ClusterId::generate();
    for script in [vec![], vec![100, 0]] {
        let reads = Arc::new(Mutex::new(Vec::new()));
        let reader = BoundedUploadReader {
            inner: std::io::Cursor::new(data.clone()),
            executor: std::thread::current().id(),
            reads: reads.clone(),
        };
        let (conn, srv) = pair(&cluster).await;
        serve(
            srv.incoming,
            scripted_put_worker(script, Arc::new(Mutex::new(None))),
        );
        let done = put_blob_reader_as(
            &conn.client,
            CoordinatorEpoch(1),
            &source,
            SourceBlobKind::Content,
            reader,
            digest.clone(),
            data.len() as u64,
            0,
            usize::MAX,
            T,
        )
        .await
        .unwrap();
        assert!(done.complete);
        assert_eq!(done.received, data.len() as u64);
        assert!(reads.lock().unwrap().len() >= 4);
    }
    // A fresh connection may start ahead of the receiver, which requests offset zero.
    let (conn, srv) = pair(&cluster).await;
    serve(srv.incoming, blob_worker(Arc::new(Mutex::new(None))));
    assert!(
        put_blob_reader_as(
            &conn.client,
            CoordinatorEpoch(1),
            &source,
            SourceBlobKind::Bundle,
            std::io::Cursor::new(data.clone()),
            digest,
            data.len() as u64,
            99999,
            4096,
            T
        )
        .await
        .unwrap()
        .complete
    );
}
#[tokio::test]
async fn streamed_upload_empty_stall_regression_and_changed_bytes_are_checked() {
    use remote_task_runner::protocol::transfer::put_blob_reader_as;
    let cluster = ClusterId::generate();
    let source = SourceId::generate();
    let (conn, srv) = pair(&cluster).await;
    serve(srv.incoming, blob_worker(Arc::new(Mutex::new(None))));
    assert!(
        put_blob_reader_as(
            &conn.client,
            CoordinatorEpoch(1),
            &source,
            SourceBlobKind::Content,
            std::io::Cursor::new(Vec::new()),
            Digest::of_bytes(b""),
            0,
            u64::MAX,
            0,
            T
        )
        .await
        .unwrap()
        .complete
    );
    for script in [vec![0; 10], (1..20).flat_map(|i| [i * 10, 0]).collect()] {
        let (conn, srv) = pair(&cluster).await;
        serve(
            srv.incoming,
            scripted_put_worker(script, Arc::new(Mutex::new(None))),
        );
        let err = put_blob_reader_as(
            &conn.client,
            CoordinatorEpoch(1),
            &source,
            SourceBlobKind::Content,
            std::io::Cursor::new(vec![7; 1000]),
            Digest::of_bytes(&vec![7; 1000]),
            1000,
            0,
            100,
            T,
        )
        .await
        .unwrap_err();
        assert!(matches!(err, TransferError::Stalled), "{err:?}");
    }
    let (conn, srv) = pair(&cluster).await;
    serve(srv.incoming, blob_worker(Arc::new(Mutex::new(None))));
    let err = put_blob_reader_as(
        &conn.client,
        CoordinatorEpoch(1),
        &source,
        SourceBlobKind::Content,
        std::io::Cursor::new(b"changed!".to_vec()),
        Digest::of_bytes(b"original"),
        8,
        0,
        4,
        T,
    )
    .await
    .unwrap_err();
    assert!(
        matches!(err, TransferError::Call(CallError::Remote(_))),
        "{err:?}"
    );
    let (conn, _srv) = pair(&cluster).await;
    let err = put_blob_reader_as(
        &conn.client,
        CoordinatorEpoch(1),
        &source,
        SourceBlobKind::Content,
        std::io::Cursor::new(b"grown".to_vec()),
        Digest::of_bytes(b"orig"),
        4,
        0,
        4,
        T,
    )
    .await
    .unwrap_err();
    assert!(matches!(
        err,
        TransferError::SizeMismatch {
            expected: 4,
            actual: 5
        }
    ));
}

#[tokio::test]
async fn streamed_upload_detects_file_mutation_between_chunks() {
    use remote_task_runner::protocol::transfer::put_blob_reader_as;
    use std::io::{Seek, SeekFrom, Write};
    use std::os::unix::fs::FileExt;
    for grows in [false, true] {
        let mut file = tempfile::tempfile().unwrap();
        file.write_all(b"abcdefgh").unwrap();
        file.seek(SeekFrom::Start(0)).unwrap();
        let change = file.try_clone().unwrap();
        let honest = blob_worker(Arc::new(Mutex::new(None)));
        let handler: Handler = Arc::new(move |request| {
            let first = matches!(&request,Request::BlobPut(r) if r.offset==0);
            let response = honest(request);
            if first {
                if grows {
                    change.set_len(9).unwrap();
                } else {
                    assert_eq!(change.write_at(b"WXYZ", 4).unwrap(), 4);
                }
            }
            response
        });
        let (conn, srv) = pair(&ClusterId::generate()).await;
        serve(srv.incoming, handler);
        let err = put_blob_reader_as(
            &conn.client,
            CoordinatorEpoch(1),
            &SourceId::generate(),
            SourceBlobKind::Content,
            file,
            Digest::of_bytes(b"abcdefgh"),
            8,
            0,
            4,
            T,
        )
        .await
        .unwrap_err();
        if grows {
            assert!(
                matches!(
                    err,
                    TransferError::SizeMismatch {
                        expected: 8,
                        actual: 9
                    }
                ),
                "{err:?}"
            );
        } else {
            assert!(
                matches!(err, TransferError::Call(CallError::Remote(_))),
                "changed bytes were accepted: {err:?}"
            );
        }
    }
}
