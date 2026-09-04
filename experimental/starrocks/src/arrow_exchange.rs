//! Arrow-over-brpc exchange transport: the `SIRIUS_CN_EXCHANGE_TRANSPORT=arrow` alternative to the
//! nixl tier for a sender whose output goes to a REMOTE receiver.
//!
//! Wire shape, per parked output stream: the sender pops each parked batch as a host Arrow
//! `RecordBatch` (`export_arrow_next`), slices it into chunks of at most [`MAX_CHUNK_BYTES`],
//! serializes every chunk as one Arrow IPC stream into the brpc attachment of the existing
//! `transmit_packed` RPC (`arrow_ipc = true`, `offset == length == 0`, no staging lease, no nixl
//! WRITE), then sends the `eos` frame and drops the parked output. The receiver
//! (`compute_node_service.rs`, `handle_transmit_packed`) decodes each attachment back into
//! `RecordBatch`es and stages them lease-free; the engine feeds them through `push_arrow`.
//!
//! Same-CN exchanges never come here: they stay native relays (or fusions) on the GPU.
//!
//! THREADING: a drain is [`prepare`]d on the sender's RPC thread (the peer is dialed there, so a
//! destination nothing listens on still fails the sender's `exec_plan_fragment`) and then run by
//! [`send_fragment`] on a drain thread the service spawns, after the RPC has replied. A drain at
//! SF1000 moves tens of GB and took 59 s inside the RPC in the first end-to-end run, past the
//! FE's ~60 s per-RPC deadline: the FE cancelled the query mid-drain and retried it, and the
//! retry's re-planned scan is what ran the receiving CN out of GPU memory. A drain that fails
//! after the RPC replied fails the sender's query on this CN ([`ServiceCore::fail_fragment`]) and
//! cancels the receiver at the peer ([`cancel_peer_receiver`]), so the FE's `fetch_data` there
//! reports the cause instead of waiting for a frame that never comes.
//!
//! ORDERING: the receiver fails a query on a `seq` gap per (exchange key, sender ordinal). Every
//! frame of one destination — the counter and the eos — is issued by the one call of
//! [`send_fragment`] that drains that destination, so the invariant holds without a dedicated
//! transport thread; distinct destinations are independent counters.

use std::io::Cursor;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{Receiver, sync_channel};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use arrow_array::RecordBatch;
use arrow_ipc::reader::StreamReader;
use arrow_ipc::writer::StreamWriter;
use arrow_schema::{Field, Schema};
use prost::Message;
use starrocks_thrift::status_code::TStatusCode;
use tracing::{info, warn};

use crate::fragment_executor::{FragmentExecutor, FragmentLabel};
use crate::nixl_transport::RemoteSendSpec;
use crate::proto::starrocks::p_internal_service_brpc::methods;
use crate::proto::starrocks::{
    PCancelPlanFragmentRequest, PCancelPlanFragmentResult, PPlanFragmentCancelReason,
    PTransmitPackedParams, PTransmitPackedResult, PUniqueId, StatusPb,
};
use crate::prpc_client::PrpcClient;

/// Upper bound on the Arrow buffer bytes one `transmit_packed` attachment carries. The PRPC
/// decoder refuses a whole message above 256 MiB (`prpc.rs`, `MAX_PRPC_MESSAGE_SIZE`), so a
/// chunk must sit well under it with room for IPC framing and the variable-width slack of an
/// estimate by rows.
pub(crate) const MAX_CHUNK_BYTES: usize = 64 << 20;

/// One remote Arrow drain, dialed and ready to run: the parked output stream `spec.slot` names,
/// shipped to `spec.host:spec.brpc_port`. Built by [`prepare`] on the sender's RPC thread and
/// consumed by [`send_fragment`] on a drain thread.
#[derive(Debug)]
pub(crate) struct ArrowDrain {
    /// Where the output goes and which parked stream it is.
    pub(crate) spec: RemoteSendSpec,
    /// The sender fragment this drain belongs to, for failure attribution and the peer cancel.
    pub(crate) label: FragmentLabel,
    /// One connection to the peer per encode-and-send worker, dialed by [`prepare`].
    clients: Vec<PrpcClient>,
}

/// Dials `workers` connections to the destination of `spec` (one per encode-and-send worker,
/// at least one) so a peer nothing listens on fails here, on the caller's thread, exactly as it
/// did when the whole drain ran inside the RPC. Nothing is exported yet.
pub(crate) fn prepare(
    spec: RemoteSendSpec,
    label: FragmentLabel,
    workers: usize,
) -> Result<ArrowDrain, String> {
    let clients = (0..workers.max(1))
        .map(|_| {
            let mut client = PrpcClient::new(&spec.host, spec.brpc_port);
            client.connect()?;
            Ok(client)
        })
        .collect::<Result<Vec<_>, String>>()?;
    Ok(ArrowDrain {
        spec,
        label,
        clients,
    })
}

/// Slices `batch` into row ranges whose estimated buffer bytes stay at or under `max_bytes`,
/// preserving row order; one chunk when it already fits. The estimate is
/// `RecordBatch::get_array_memory_size` spread evenly over the rows, so a batch of very uneven
/// variable-width rows can still overshoot on one chunk — the bound is a sizing rule for the
/// PRPC cap, not a wire guarantee. Every chunk shares the input's buffers (no copy).
pub(crate) fn chunk_by_rows(batch: &RecordBatch, max_bytes: usize) -> Vec<RecordBatch> {
    let rows = batch.num_rows();
    let bytes = batch.get_array_memory_size();
    if rows <= 1 || bytes <= max_bytes {
        return vec![batch.clone()];
    }
    let chunks = bytes.div_ceil(max_bytes.max(1)).min(rows);
    let rows_per_chunk = rows.div_ceil(chunks);
    (0..rows)
        .step_by(rows_per_chunk)
        .map(|start| batch.slice(start, rows_per_chunk.min(rows - start)))
        .collect()
}

/// Serializes one batch as an Arrow IPC stream (schema message, one record batch, end marker).
pub(crate) fn encode_ipc(batch: &RecordBatch) -> Result<Vec<u8>, String> {
    let mut out = Vec::with_capacity(batch.get_array_memory_size() + 4096);
    let mut writer = StreamWriter::try_new(&mut out, batch.schema_ref().as_ref())
        .map_err(|err| format!("failed to start an Arrow IPC stream: {err}"))?;
    writer
        .write(batch)
        .map_err(|err| format!("failed to serialize a record batch as Arrow IPC: {err}"))?;
    writer
        .finish()
        .map_err(|err| format!("failed to finish an Arrow IPC stream: {err}"))?;
    drop(writer);
    Ok(out)
}

/// Decodes one Arrow IPC stream into its record batches (one or more).
pub(crate) fn decode_ipc(bytes: &[u8]) -> Result<Vec<RecordBatch>, String> {
    let reader = StreamReader::try_new(Cursor::new(bytes), None)
        .map_err(|err| format!("attachment is not an Arrow IPC stream: {err}"))?;
    reader
        .collect::<Result<Vec<_>, _>>()
        .map_err(|err| format!("Arrow IPC stream did not decode: {err}"))
}

/// The batch with its columns renamed positionally to `names` (the engine exports types only;
/// the sender's plan knows the names). Shares the column buffers.
pub(crate) fn with_names(batch: &RecordBatch, names: &[String]) -> Result<RecordBatch, String> {
    if names.len() != batch.num_columns() {
        return Err(format!(
            "exported Arrow batch carries {} columns but the sender plan names {}",
            batch.num_columns(),
            names.len()
        ));
    }
    let fields: Vec<Field> = batch
        .schema_ref()
        .fields()
        .iter()
        .zip(names)
        .map(|(field, name)| field.as_ref().clone().with_name(name))
        .collect();
    RecordBatch::try_new(Arc::new(Schema::new(fields)), batch.columns().to_vec())
        .map_err(|err| format!("failed to rename the exported Arrow batch: {err}"))
}

/// Turns a method-level StarRocks status into `Err` naming the method.
pub(crate) fn check_status(what: &str, status: &StatusPb) -> Result<(), String> {
    if status.status_code == TStatusCode::OK.0 {
        return Ok(());
    }
    Err(format!(
        "{what} failed with status {}: {}",
        status.status_code,
        status.error_msgs.join("; ")
    ))
}

/// `transmit_packed` over brpc with an Arrow IPC stream (or nothing, for eos) in the attachment.
fn rpc_transmit(
    client: &mut PrpcClient,
    params: PTransmitPackedParams,
    attachment: Vec<u8>,
) -> Result<(), String> {
    let response = client.call(methods::TRANSMIT_PACKED, params.encode_to_vec(), attachment)?;
    let result = PTransmitPackedResult::decode(response.body.as_slice())
        .map_err(|err| format!("undecodable transmit_packed reply: {err}"))?;
    check_status("transmit_packed", &result.status)
}

/// Cancels the receiver `spec.slot` feeds at the peer, with `error` as the cause, so the FE's
/// `fetch_data` on that CN reports the sender's failure instead of polling until its query
/// timeout. Best-effort: the peer may already be gone, and the sender's own query-level failure
/// (`fail_fragment`) is recorded regardless.
pub(crate) fn cancel_peer_receiver(spec: &RemoteSendSpec, label: &FragmentLabel, error: &str) {
    let (hi, lo) = spec.slot.fragment_instance_id.as_halves();
    let request = PCancelPlanFragmentRequest {
        finst_id: PUniqueId { hi, lo },
        cancel_reason: Some(PPlanFragmentCancelReason::InternalError as i32),
        is_pipeline: None,
        query_id: label.query_id.map(|query_id| {
            let (hi, lo) = query_id.as_halves();
            PUniqueId { hi, lo }
        }),
        error_message: Some(format!(
            "remote Arrow sender {} failed while transmitting into exchange {}: {error}",
            label.log_ids().1,
            spec.slot.node_id
        )),
    };
    let mut client = PrpcClient::new(&spec.host, spec.brpc_port);
    let outcome = client
        .call(
            methods::CANCEL_PLAN_FRAGMENT,
            request.encode_to_vec(),
            Vec::new(),
        )
        .and_then(|response| {
            PCancelPlanFragmentResult::decode(response.body.as_slice())
                .map_err(|err| format!("undecodable cancel_plan_fragment reply: {err}"))
        })
        .and_then(|result| check_status("cancel_plan_fragment", &result.status));
    match outcome {
        Ok(()) => info!(
            receiver_fragment_instance_id = %spec.slot.fragment_instance_id,
            stream_id = spec.slot.node_id,
            dest = %client.peer(),
            "cancelled the receiver of a failed Arrow drain at the peer"
        ),
        Err(err) => warn!(
            receiver_fragment_instance_id = %spec.slot.fragment_instance_id,
            stream_id = spec.slot.node_id,
            dest = %client.peer(),
            error = %err,
            "could not cancel the receiver of a failed Arrow drain at the peer"
        ),
    }
}

/// What one encode-and-send worker moved, summed into the `transmitted batches via arrow` line.
#[derive(Debug, Default)]
struct WorkerStats {
    /// Frames sent (chunks of parked batches).
    batches: u64,
    /// IPC bytes sent, the total the receiver's `received remote batches via arrow` counts.
    bytes: u64,
    /// Time encoding chunks as Arrow IPC.
    encode: Duration,
    /// Time inside `transmit_packed` round trips (write, the peer's decode, the reply).
    send: Duration,
}

impl WorkerStats {
    fn add(&mut self, other: &WorkerStats) {
        self.batches += other.batches;
        self.bytes += other.bytes;
        self.encode += other.encode;
        self.send += other.send;
    }
}

/// One `transmit_packed` frame of `spec`'s stream: a data frame carrying `rows`, or the eos.
fn frame(spec: &RemoteSendSpec, eos: bool, seq: i64, rows: Option<u64>) -> PTransmitPackedParams {
    let (hi, lo) = spec.slot.fragment_instance_id.as_halves();
    PTransmitPackedParams {
        finst_id: Some(PUniqueId { hi, lo }),
        node_id: Some(spec.slot.node_id),
        sender_id: Some(spec.slot.sender_id),
        eos: Some(eos),
        seq: Some(seq),
        // No staging lease exists for an Arrow frame; the receiver reads the attachment.
        offset: Some(0),
        length: Some(0),
        column_names: spec.names.clone(),
        canary: None,
        rows,
        arrow_ipc: Some(true),
    }
}

/// One encode-and-send worker: takes `(seq, chunk)`s off the shared queue until the exporter
/// closes it, IPC-encodes each and ships it over this worker's own connection. Stops at the
/// first failure and raises `failed`, so the exporter stops feeding the queue; the connection
/// comes back either way (one of them sends the eos).
fn send_worker(
    mut client: PrpcClient,
    spec: &RemoteSendSpec,
    chunks: &Mutex<Receiver<(i64, RecordBatch)>>,
    failed: &AtomicBool,
) -> (PrpcClient, Result<WorkerStats, String>) {
    let mut stats = WorkerStats::default();
    loop {
        // Hold the queue's lock only while waiting for the next chunk: the workers then take
        // turns at the head of the queue and encode/send concurrently.
        let next = chunks.lock().unwrap_or_else(PoisonError::into_inner).recv();
        let Ok((seq, chunk)) = next else {
            return (client, Ok(stats));
        };
        let outcome = (|| -> Result<(), String> {
            let encoding = Instant::now();
            let payload = encode_ipc(&chunk)?;
            stats.encode += encoding.elapsed();
            stats.bytes += payload.len() as u64;
            let sending = Instant::now();
            rpc_transmit(
                &mut client,
                frame(spec, false, seq, Some(chunk.num_rows() as u64)),
                payload,
            )?;
            stats.send += sending.elapsed();
            stats.batches += 1;
            Ok(())
        })();
        if let Err(err) = outcome {
            failed.store(true, Ordering::SeqCst);
            return (client, Err(err));
        }
    }
}

/// Sender flow: drain one parked output to a remote receiver as Arrow IPC frames and drop the
/// parked output. Blocks until every chunk and the eos frame have been acknowledged; on a failed
/// send the parked output is still dropped (best-effort), so a dead query does not pin it, and
/// the receiver is cancelled at the peer.
///
/// Pipelined: this thread exports parked batches (`export_arrow_next`, one engine round trip
/// and one D2H copy each) and queues their chunks with their `seq`; the drain's workers (one
/// per connection [`prepare`] dialed) encode and send them concurrently, so N frames are in
/// flight per destination and the export overlaps the wire. The queue is bounded to the worker
/// count, so host memory per drain stays within about three chunks per worker. Frames may reach
/// the receiver out of `seq` order; it holds the early ones (`local_exchange.rs`,
/// `push_remote_frame`). The eos goes last, once every worker has returned with its data frames
/// acknowledged, so an eos never overtakes a data frame.
pub(crate) fn send_fragment(
    executor: &dyn FragmentExecutor,
    drain: ArrowDrain,
) -> Result<(), String> {
    let ArrowDrain {
        spec,
        label,
        clients,
    } = drain;
    let started = Instant::now();
    let workers = clients.len();
    let spec = Arc::new(spec);
    let failed = Arc::new(AtomicBool::new(false));
    let (chunk_tx, chunk_rx) = sync_channel::<(i64, RecordBatch)>(workers.max(1));
    let chunk_rx = Arc::new(Mutex::new(chunk_rx));
    let mut handles = Vec::with_capacity(workers);
    let mut spawn_error = None;
    for client in clients {
        let spec = Arc::clone(&spec);
        let chunk_rx = Arc::clone(&chunk_rx);
        let failed = Arc::clone(&failed);
        match std::thread::Builder::new()
            .name("arrow-send".to_string())
            .spawn(move || send_worker(client, &spec, &chunk_rx, &failed))
        {
            Ok(handle) => handles.push(handle),
            Err(err) => {
                spawn_error = Some(format!("failed to spawn an Arrow send worker: {err}"));
                break;
            }
        }
    }

    let mut seq: i64 = 0;
    let mut export = Duration::ZERO;
    let exported = match spawn_error {
        Some(err) => Err(err),
        None => (|| -> Result<(), String> {
            loop {
                if failed.load(Ordering::SeqCst) {
                    // The worker's own error is reported below; stop exporting into a queue
                    // nothing will drain.
                    return Err("an Arrow send worker failed".to_string());
                }
                let exporting = Instant::now();
                let Some(batch) = executor.export_arrow_next(spec.slot)? else {
                    return Ok(());
                };
                export += exporting.elapsed();
                let named = with_names(&batch, &spec.names)?;
                for chunk in chunk_by_rows(&named, MAX_CHUNK_BYTES) {
                    if chunk_tx.send((seq, chunk)).is_err() {
                        return Err(
                            "every Arrow send worker exited before the drain finished".to_string()
                        );
                    }
                    seq += 1;
                }
            }
        })(),
    };
    // Closing the queue ends the workers once they have sent what was queued.
    drop(chunk_tx);
    let mut stats = WorkerStats::default();
    let mut worker_error = None;
    let mut clients = Vec::with_capacity(workers);
    for handle in handles {
        match handle.join() {
            Ok((client, Ok(worker_stats))) => {
                stats.add(&worker_stats);
                clients.push(client);
            }
            Ok((client, Err(err))) => {
                worker_error.get_or_insert(err);
                clients.push(client);
            }
            Err(_) => {
                worker_error.get_or_insert("an Arrow send worker panicked".to_string());
            }
        }
    }
    // A worker's failure is the root cause when the exporter also stopped because of it.
    let sent = match (worker_error, exported) {
        (Some(err), _) => Err(err),
        (None, Err(err)) => Err(err),
        (None, Ok(())) => {
            // Every data frame is acknowledged: the eos may go.
            let sending = Instant::now();
            let outcome = match clients.first_mut() {
                Some(client) => rpc_transmit(client, frame(&spec, true, seq, None), Vec::new()),
                None => Err("no connection left to send the Arrow eos frame".to_string()),
            };
            stats.send += sending.elapsed();
            outcome
        }
    };
    if let Err(err) = &sent {
        // Best-effort GPU cleanup, as the nixl tier does: without it a failed transmit pins the
        // parked output for the process lifetime. A slot already retired with its query is Ok.
        if let Err(drop_err) = executor.drop_parked(spec.slot) {
            warn!(
                slot = ?spec.slot,
                error = %drop_err,
                "failed to drop the parked output of a failed remote Arrow transmit"
            );
        }
        cancel_peer_receiver(&spec, &label, err);
    }
    sent?;
    executor.drop_parked(spec.slot)?;
    let (query_id, fragment_instance_id) = label.log_ids();
    let dest = format!("{}:{}", spec.host, spec.brpc_port);
    // `encode_ms` and `send_ms` are summed over the workers, so they exceed `elapsed_ms` when
    // the workers overlap; `export_ms` is this thread's time inside the engine.
    info!(
        %query_id,
        %fragment_instance_id,
        stream_id = spec.slot.node_id,
        sender_id = spec.slot.sender_id,
        %dest,
        batches = stats.batches,
        bytes = stats.bytes,
        elapsed_ms = started.elapsed().as_millis() as u64,
        export_ms = export.as_millis() as u64,
        encode_ms = stats.encode.as_millis() as u64,
        send_ms = stats.send.as_millis() as u64,
        workers,
        "transmitted batches via arrow"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use arrow_array::{
        Array, ArrayRef, BooleanArray, Date32Array, Decimal64Array, Decimal128Array, Float64Array,
        Int64Array, StringArray,
    };
    use arrow_schema::DataType;

    use super::*;

    fn fixed_width_batch(rows: usize) -> RecordBatch {
        let ids = Int64Array::from_iter_values(0..rows as i64);
        let xs = Float64Array::from_iter_values((0..rows).map(|i| i as f64 * 0.5));
        RecordBatch::try_from_iter([
            ("id", Arc::new(ids) as ArrayRef),
            ("x", Arc::new(xs) as ArrayRef),
        ])
        .unwrap()
    }

    /// The bytes bound: every chunk of a 16 MiB batch serializes under a 1 MiB budget plus
    /// framing; row conservation: the chunks tile the input in order with nothing lost or
    /// duplicated.
    #[test]
    fn chunks_stay_under_the_byte_bound_and_conserve_rows() {
        let rows = 1 << 20; // 16 MiB of fixed-width buffers (plus array bookkeeping)
        let batch = fixed_width_batch(rows);
        let max_bytes = 1 << 20;
        let chunks = chunk_by_rows(&batch, max_bytes);
        assert_eq!(
            chunks.len(),
            batch.get_array_memory_size().div_ceil(max_bytes),
            "one chunk per max_bytes of the estimate"
        );
        assert!(
            chunks.len() >= 16,
            "16 MiB of buffers need at least 16 chunks"
        );

        let mut next_id = 0i64;
        for chunk in &chunks {
            let encoded = encode_ipc(chunk).unwrap();
            assert!(
                encoded.len() <= max_bytes + 4096,
                "a chunk serialized to {} bytes, over the {max_bytes} bound",
                encoded.len()
            );
            let ids = chunk
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            for id in ids.values() {
                assert_eq!(*id, next_id, "rows must tile the input in order");
                next_id += 1;
            }
        }
        assert_eq!(next_id, rows as i64, "every row lands in exactly one chunk");
        assert_eq!(
            chunks.iter().map(RecordBatch::num_rows).sum::<usize>(),
            rows
        );
    }

    /// A batch that already fits, and an empty one, are passed through as one chunk.
    #[test]
    fn a_fitting_or_empty_batch_is_one_chunk() {
        let small = fixed_width_batch(10);
        let chunks = chunk_by_rows(&small, MAX_CHUNK_BYTES);
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0], small);

        let empty = fixed_width_batch(0);
        let chunks = chunk_by_rows(&empty, 1);
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].num_rows(), 0);
        // And an empty batch still round-trips through IPC (a zero-row parked batch is legal).
        assert_eq!(
            decode_ipc(&encode_ipc(&empty).unwrap()).unwrap(),
            vec![empty]
        );
    }

    /// The IPC hop preserves values, nulls and schema for the types the engine exports: the
    /// TPC-H column set (int64, double, utf8, date32, decimal64 as cudf spells DECIMAL(15,2))
    /// plus bool and decimal128.
    #[test]
    fn ipc_round_trip_preserves_values_nulls_and_schema() {
        let batch = RecordBatch::try_from_iter_with_nullable([
            (
                "id",
                Arc::new(Int64Array::from(vec![Some(1), None, Some(3)])) as ArrayRef,
                true,
            ),
            (
                "x",
                Arc::new(Float64Array::from(vec![Some(0.5), Some(-1.0), None])) as ArrayRef,
                true,
            ),
            (
                "flag",
                Arc::new(BooleanArray::from(vec![Some(true), None, Some(false)])) as ArrayRef,
                true,
            ),
            (
                "name",
                Arc::new(StringArray::from(vec![Some("a"), Some(""), None])) as ArrayRef,
                true,
            ),
            (
                "price",
                Arc::new(
                    Decimal64Array::from(vec![Some(1234), None, Some(-5)])
                        .with_precision_and_scale(18, 2)
                        .unwrap(),
                ) as ArrayRef,
                true,
            ),
            (
                "wide",
                Arc::new(
                    Decimal128Array::from(vec![Some(1), Some(2), None])
                        .with_precision_and_scale(38, 4)
                        .unwrap(),
                ) as ArrayRef,
                true,
            ),
            (
                "day",
                Arc::new(Date32Array::from(vec![Some(19000), None, Some(0)])) as ArrayRef,
                true,
            ),
        ])
        .unwrap();

        let decoded = decode_ipc(&encode_ipc(&batch).unwrap()).unwrap();
        assert_eq!(decoded, vec![batch.clone()]);
        assert_eq!(
            decoded[0].schema_ref().field(4).data_type(),
            &DataType::Decimal64(18, 2)
        );

        // A sliced chunk (non-zero offsets on every child) round-trips as the slice alone.
        let slice = batch.slice(1, 2);
        let decoded = decode_ipc(&encode_ipc(&slice).unwrap()).unwrap();
        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0].num_rows(), 2);
        assert_eq!(decoded[0], slice);
    }

    /// Garbage is refused, never decoded into rows.
    #[test]
    fn a_non_ipc_attachment_is_an_error() {
        let err = decode_ipc(&[0xAB; 16]).unwrap_err();
        assert!(err.contains("Arrow IPC"), "{err}");
    }

    /// The engine exports types only; the sender's plan names ride along positionally.
    #[test]
    fn with_names_renames_positionally_and_refuses_a_count_mismatch() {
        let batch = fixed_width_batch(3);
        let named = with_names(&batch, &["a".to_string(), "b".to_string()]).unwrap();
        assert_eq!(named.schema_ref().field(0).name(), "a");
        assert_eq!(named.schema_ref().field(1).name(), "b");
        assert_eq!(named.column(0), batch.column(0));

        let err = with_names(&batch, &["only".to_string()]).unwrap_err();
        assert!(
            err.contains("2 columns") && err.contains("names 1"),
            "{err}"
        );
    }
}
