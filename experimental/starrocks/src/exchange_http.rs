//! HTTP control plane for the NIXL packed hop between CN processes.
//!
//! Three routes share `http_port`:
//!
//! - `POST /nixl-md` — body is the caller's agent metadata; reply body is this CN's cached
//!   local metadata. The reply is served from a cached blob; only loading the caller's
//!   metadata needs the agent thread (wired in a later commit).
//! - `POST /staging-lease` — `X-Length` bytes of the receiver arena; reply headers
//!   `X-Remote-Addr` and `X-Offset`.
//! - `POST /exchange` — packed-hop frame: identity headers plus cudf pack metadata in the
//!   body (empty on eos or a zero-row batch). The GPU payload itself is a NIXL WRITE, not
//!   this body.
//!
//! Blocking clients (`post_nixl_md`, `post_staging_lease`, `post_exchange`) use
//! `std::net::TcpStream` so the dedicated NIXL transport thread can call them without a
//! tokio runtime.

use std::collections::HashMap;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader as AsyncBufReader};
use tokio::net::{TcpListener, TcpStream as TokioTcpStream};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::fragment_executor::StagedBatch;
use crate::local_exchange::{ExchangeKey, LocalExchange, ReadyFragment};
use crate::result_store::FragmentInstanceId;

/// Callback invoked when a remote hop completes a receiver's sender set.
pub type ReadyReceiver = Arc<dyn Fn(ReadyFragment) + Send + Sync>;

/// One packed hop frame on `POST /exchange`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct PackedExchangeFrame {
    pub(crate) fragment_instance_id: FragmentInstanceId,
    pub(crate) dest_stream: i32,
    pub(crate) sender_id: i32,
    pub(crate) seq: i64,
    pub(crate) eos: bool,
    pub(crate) names: Vec<String>,
    pub(crate) offset: u64,
    pub(crate) length: u64,
    pub(crate) rows: Option<u64>,
    /// Cudf pack metadata. Empty when `eos` or `length == 0`.
    pub(crate) metadata: Vec<u8>,
}

/// Receiver-side arena lease returned by `POST /staging-lease`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RemoteLease {
    pub(crate) remote_addr: u64,
    pub(crate) offset: u64,
}

/// Loads a peer's NIXL metadata and returns this CN's cached local blob.
pub trait NixlMdHandler: Send + Sync + std::fmt::Debug {
    fn on_peer_md(&self, peer_metadata: &[u8]) -> Result<Vec<u8>, String>;
}

/// Grants a lease of this CN's staging arena for a peer WRITE.
pub trait StagingLeaseHandler: Send + Sync + std::fmt::Debug {
    fn lease(&self, length: u64) -> Result<RemoteLease, String>;
    /// Returns the lease at `offset`. Used by the log-only bandwidth canary so the
    /// remote probe does not sit in the exchange rendezvous.
    fn release(&self, offset: u64) -> Result<(), String>;
}

impl StagingLeaseHandler for Arc<dyn crate::fragment_executor::FragmentExecutor> {
    fn lease(&self, length: u64) -> Result<RemoteLease, String> {
        let (base, _) = self.staging_info()?;
        let offset = self.staging_lease(length)?;
        Ok(RemoteLease {
            remote_addr: base + offset,
            offset,
        })
    }

    fn release(&self, offset: u64) -> Result<(), String> {
        self.staging_release(offset)
    }
}

/// Listener that accepts the three hop routes and records `/exchange` frames on a
/// [`LocalExchange`].
pub struct ExchangeHttpServer {
    local_addr: SocketAddr,
    shutdown: CancellationToken,
    join: JoinHandle<()>,
}

impl ExchangeHttpServer {
    /// Binds `bind` and serves until [`Self::shutdown`].
    pub async fn start(
        bind: SocketAddr,
        exchange: Arc<LocalExchange>,
        md: Option<Arc<dyn NixlMdHandler>>,
        leases: Option<Arc<dyn StagingLeaseHandler>>,
        on_ready: Option<ReadyReceiver>,
    ) -> Result<Self, String> {
        let listener = TcpListener::bind(bind)
            .await
            .map_err(|err| format!("exchange http bind {bind}: {err}"))?;
        let local_addr = listener
            .local_addr()
            .map_err(|err| format!("exchange http local_addr: {err}"))?;
        let shutdown = CancellationToken::new();
        let server_shutdown = shutdown.clone();
        info!(%local_addr, "starting packed exchange HTTP server");
        let join = tokio::spawn(async move {
            serve_loop(listener, exchange, md, leases, on_ready, server_shutdown).await;
        });
        Ok(Self {
            local_addr,
            shutdown,
            join,
        })
    }

    /// The bound address, including the ephemeral port when `bind` used port 0.
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// Stops accept and waits for the serve task.
    pub async fn shutdown(self) {
        self.shutdown.cancel();
        let _ = self.join.await;
    }
}

/// Peer HTTP port from a destination brpc port, using this CN's advertised http/brpc offset.
/// Default CN ports are http 8040 / brpc 8060 (offset −20). The 2-CN study script may set
/// `http_port = brpc_port + 1` instead; the wrapping arithmetic covers both.
pub fn peer_http_port(dest_brpc: u16, local_http: u16, local_brpc: u16) -> u16 {
    dest_brpc.wrapping_add(local_http.wrapping_sub(local_brpc))
}

/// POSTs this CN's agent metadata and returns the peer's.
pub fn post_nixl_md(peer: SocketAddr, local_md: &[u8]) -> Result<Vec<u8>, String> {
    let (status, _headers, body) = http_post(peer, "/nixl-md", &[], local_md)?;
    if status != 200 {
        return Err(format!(
            "POST /nixl-md {peer} returned {status}: {}",
            String::from_utf8_lossy(&body)
        ));
    }
    Ok(body)
}

/// Leases `length` bytes of the peer's staging arena.
pub fn post_staging_lease(peer: SocketAddr, length: u64) -> Result<RemoteLease, String> {
    let extra = vec![("X-Length".to_string(), length.to_string())];
    let (status, headers, body) = http_post(peer, "/staging-lease", &extra, &[])?;
    if status != 200 {
        return Err(format!(
            "POST /staging-lease {peer} returned {status}: {}",
            String::from_utf8_lossy(&body)
        ));
    }
    let remote_addr = required_header(&headers, "x-remote-addr")?
        .parse::<u64>()
        .map_err(|err| format!("X-Remote-Addr: {err}"))?;
    let offset = required_header(&headers, "x-offset")?
        .parse::<u64>()
        .map_err(|err| format!("X-Offset: {err}"))?;
    Ok(RemoteLease {
        remote_addr,
        offset,
    })
}

/// POSTs one packed hop frame to `peer`.
pub fn post_exchange(peer: SocketAddr, frame: &PackedExchangeFrame) -> Result<(), String> {
    let names = frame.names.join(",");
    let mut extra = vec![
        (
            "X-Fragment-Instance-Id".to_string(),
            frame.fragment_instance_id.to_string(),
        ),
        ("X-Dest-Stream".to_string(), frame.dest_stream.to_string()),
        ("X-Sender-Id".to_string(), frame.sender_id.to_string()),
        ("X-Seq".to_string(), frame.seq.to_string()),
        (
            "X-Eos".to_string(),
            if frame.eos { "1" } else { "0" }.to_string(),
        ),
        ("X-Column-Names".to_string(), names),
        ("X-Offset".to_string(), frame.offset.to_string()),
        ("X-Length".to_string(), frame.length.to_string()),
    ];
    if let Some(rows) = frame.rows {
        extra.push(("X-Rows".to_string(), rows.to_string()));
    }
    let (status, _headers, body) = http_post(peer, "/exchange", &extra, &frame.metadata)?;
    if status != 200 {
        return Err(format!(
            "POST /exchange {peer} returned {status}: {}",
            String::from_utf8_lossy(&body)
        ));
    }
    Ok(())
}

/// Releases a receiver-side canary lease without touching the exchange rendezvous.
pub(crate) fn post_canary_release(peer: SocketAddr, offset: u64) -> Result<(), String> {
    let extra = vec![
        ("X-Canary".to_string(), "1".to_string()),
        ("X-Offset".to_string(), offset.to_string()),
    ];
    let (status, _headers, body) = http_post(peer, "/exchange", &extra, &[])?;
    if status != 200 {
        return Err(format!(
            "POST /exchange canary-release {peer} returned {status}: {}",
            String::from_utf8_lossy(&body)
        ));
    }
    Ok(())
}

fn http_post(
    peer: SocketAddr,
    path: &str,
    extra_headers: &[(String, String)],
    body: &[u8],
) -> Result<(u16, HashMap<String, String>, Vec<u8>), String> {
    let mut request = format!(
        "POST {path} HTTP/1.1\r\nHost: {peer}\r\nContent-Length: {}\r\nConnection: close\r\n",
        body.len()
    );
    for (name, value) in extra_headers {
        request.push_str(&format!("{name}: {value}\r\n"));
    }
    request.push_str("\r\n");

    let mut stream =
        TcpStream::connect(peer).map_err(|err| format!("exchange http connect {peer}: {err}"))?;
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .map_err(|err| format!("set read timeout: {err}"))?;
    stream
        .set_write_timeout(Some(Duration::from_secs(5)))
        .map_err(|err| format!("set write timeout: {err}"))?;
    stream
        .write_all(request.as_bytes())
        .map_err(|err| format!("exchange http write headers: {err}"))?;
    stream
        .write_all(body)
        .map_err(|err| format!("exchange http write body: {err}"))?;
    stream
        .flush()
        .map_err(|err| format!("exchange http flush: {err}"))?;

    let mut reader = BufReader::new(stream);
    let mut status_line = String::new();
    reader
        .read_line(&mut status_line)
        .map_err(|err| format!("exchange http read status: {err}"))?;
    let status = parse_status_code(&status_line)?;
    let mut headers = HashMap::<String, String>::new();
    loop {
        let mut line = String::new();
        reader
            .read_line(&mut line)
            .map_err(|err| format!("exchange http read header: {err}"))?;
        let line = line.trim_end();
        if line.is_empty() {
            break;
        }
        let Some((name, value)) = line.split_once(':') else {
            return Err(format!("malformed response header: {line}"));
        };
        headers.insert(name.trim().to_ascii_lowercase(), value.trim().to_string());
    }
    let content_length = headers
        .get("content-length")
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(0);
    let mut response_body = vec![0; content_length];
    if content_length > 0 {
        reader
            .read_exact(&mut response_body)
            .map_err(|err| format!("exchange http read body: {err}"))?;
    }
    Ok((status, headers, response_body))
}

fn parse_status_code(status_line: &str) -> Result<u16, String> {
    let mut parts = status_line.split_whitespace();
    let _http = parts.next();
    parts
        .next()
        .ok_or_else(|| format!("missing status code in {status_line:?}"))?
        .parse::<u16>()
        .map_err(|err| format!("status code: {err}"))
}

fn required_header<'a>(
    headers: &'a HashMap<String, String>,
    name: &str,
) -> Result<&'a String, String> {
    headers
        .get(name)
        .ok_or_else(|| format!("missing {name} header"))
}

async fn serve_loop(
    listener: TcpListener,
    exchange: Arc<LocalExchange>,
    md: Option<Arc<dyn NixlMdHandler>>,
    leases: Option<Arc<dyn StagingLeaseHandler>>,
    on_ready: Option<ReadyReceiver>,
    shutdown: CancellationToken,
) {
    loop {
        tokio::select! {
            _ = shutdown.cancelled() => break,
            accepted = listener.accept() => {
                match accepted {
                    Ok((stream, _)) => {
                        let exchange = exchange.clone();
                        let md = md.clone();
                        let leases = leases.clone();
                        let on_ready = on_ready.clone();
                        tokio::spawn(async move {
                            if let Err(err) = handle_connection(
                                stream, exchange, md, leases, on_ready,
                            ).await {
                                warn!(error = %err, "exchange http request failed");
                            }
                        });
                    }
                    Err(err) => {
                        warn!(error = %err, "exchange http accept failed");
                    }
                }
            }
        }
    }
}

async fn handle_connection(
    stream: TokioTcpStream,
    exchange: Arc<LocalExchange>,
    md: Option<Arc<dyn NixlMdHandler>>,
    leases: Option<Arc<dyn StagingLeaseHandler>>,
    on_ready: Option<ReadyReceiver>,
) -> Result<(), String> {
    let mut reader = AsyncBufReader::new(stream);
    let mut request_line = String::new();
    reader
        .read_line(&mut request_line)
        .await
        .map_err(|err| format!("read request line: {err}"))?;
    let path = request_path(&request_line);
    let mut headers = HashMap::<String, String>::new();
    loop {
        let mut line = String::new();
        reader
            .read_line(&mut line)
            .await
            .map_err(|err| format!("read header: {err}"))?;
        let line = line.trim_end();
        if line.is_empty() {
            break;
        }
        let Some((name, value)) = line.split_once(':') else {
            write_http_response(reader.get_mut(), 400, &[], b"malformed header").await?;
            return Ok(());
        };
        headers.insert(name.trim().to_ascii_lowercase(), value.trim().to_string());
    }
    let content_length = headers
        .get("content-length")
        .ok_or_else(|| "missing Content-Length".to_string())?
        .parse::<usize>()
        .map_err(|err| format!("Content-Length: {err}"))?;
    let mut body = vec![0; content_length];
    reader
        .read_exact(&mut body)
        .await
        .map_err(|err| format!("read body: {err}"))?;

    match path {
        Some("/exchange") => {
            handle_exchange(
                reader.get_mut(),
                &headers,
                &body,
                exchange,
                leases.as_ref(),
                on_ready.as_ref(),
            )
            .await
        }
        Some("/nixl-md") => handle_nixl_md(reader.get_mut(), md.as_ref(), &body).await,
        Some("/staging-lease") => {
            handle_staging_lease(reader.get_mut(), &headers, leases.as_ref()).await
        }
        _ => {
            write_http_response(
                reader.get_mut(),
                404,
                &[],
                b"expected POST /exchange, /nixl-md, or /staging-lease",
            )
            .await
        }
    }
}

fn request_path(request_line: &str) -> Option<&str> {
    let mut parts = request_line.split_whitespace();
    let method = parts.next()?;
    let path = parts.next()?;
    (method == "POST").then_some(path)
}

async fn handle_exchange(
    stream: &mut TokioTcpStream,
    headers: &HashMap<String, String>,
    body: &[u8],
    exchange: Arc<LocalExchange>,
    leases: Option<&Arc<dyn StagingLeaseHandler>>,
    on_ready: Option<&ReadyReceiver>,
) -> Result<(), String> {
    let canary = matches!(
        headers.get("x-canary").map(String::as_str),
        Some("1") | Some("true")
    );
    if canary {
        let offset = match headers.get("x-offset") {
            Some(value) => match value.parse::<u64>() {
                Ok(offset) => offset,
                Err(err) => {
                    write_http_response(stream, 400, &[], format!("X-Offset: {err}").as_bytes())
                        .await?;
                    return Ok(());
                }
            },
            None => {
                write_http_response(stream, 400, &[], b"canary frame missing X-Offset").await?;
                return Ok(());
            }
        };
        let Some(leases) = leases else {
            write_http_response(stream, 501, &[], b"staging-lease is not configured").await?;
            return Ok(());
        };
        match leases.release(offset) {
            Ok(()) => write_http_response(stream, 200, &[], b"ok").await?,
            Err(err) => write_http_response(stream, 500, &[], err.as_bytes()).await?,
        }
        return Ok(());
    }
    let frame = match parse_exchange_frame(headers, body) {
        Ok(frame) => frame,
        Err(err) => {
            write_http_response(stream, 400, &[], err.as_bytes()).await?;
            return Ok(());
        }
    };
    let key = ExchangeKey {
        fragment_instance_id: frame.fragment_instance_id,
        node_id: frame.dest_stream,
    };
    // A pure eos frame (no payload) is `batch = None`. A zero-row data frame still
    // carries a StagedBatch with len == 0 so the receiver can bind schema.
    let batch = if frame.length == 0 && frame.metadata.is_empty() && frame.eos {
        None
    } else {
        Some(StagedBatch {
            metadata: frame.metadata,
            offset: frame.offset,
            len: frame.length,
            rows: frame.rows,
        })
    };
    match exchange.push_remote_frame(
        key,
        frame.sender_id,
        frame.seq,
        frame.eos,
        frame.names,
        batch,
    ) {
        Ok(ready) => {
            write_http_response(stream, 200, &[], b"ok").await?;
            if let (Some(ready), Some(on_ready)) = (ready, on_ready) {
                on_ready(ready);
            }
        }
        Err(err) => write_http_response(stream, 500, &[], err.as_bytes()).await?,
    }
    Ok(())
}

async fn handle_nixl_md(
    stream: &mut TokioTcpStream,
    md: Option<&Arc<dyn NixlMdHandler>>,
    body: &[u8],
) -> Result<(), String> {
    let Some(md) = md else {
        write_http_response(stream, 501, &[], b"nixl-md is not configured").await?;
        return Ok(());
    };
    match md.on_peer_md(body) {
        Ok(local) => write_http_response(stream, 200, &[], &local).await?,
        Err(err) => write_http_response(stream, 500, &[], err.as_bytes()).await?,
    }
    Ok(())
}

async fn handle_staging_lease(
    stream: &mut TokioTcpStream,
    headers: &HashMap<String, String>,
    leases: Option<&Arc<dyn StagingLeaseHandler>>,
) -> Result<(), String> {
    let Some(leases) = leases else {
        write_http_response(stream, 501, &[], b"staging-lease is not configured").await?;
        return Ok(());
    };
    let length = match required_header(headers, "x-length") {
        Ok(value) => match value.parse::<u64>() {
            Ok(length) => length,
            Err(err) => {
                write_http_response(stream, 400, &[], format!("X-Length: {err}").as_bytes())
                    .await?;
                return Ok(());
            }
        },
        Err(err) => {
            write_http_response(stream, 400, &[], err.as_bytes()).await?;
            return Ok(());
        }
    };
    match leases.lease(length) {
        Ok(lease) => {
            let extra = [
                ("X-Remote-Addr", lease.remote_addr.to_string()),
                ("X-Offset", lease.offset.to_string()),
            ];
            write_http_response(stream, 200, &extra, b"").await?;
        }
        Err(err) => write_http_response(stream, 500, &[], err.as_bytes()).await?,
    }
    Ok(())
}

fn parse_exchange_frame(
    headers: &HashMap<String, String>,
    body: &[u8],
) -> Result<PackedExchangeFrame, String> {
    let fragment_instance_id =
        parse_instance_id(required_header(headers, "x-fragment-instance-id")?)?;
    let dest_stream = parse_i32(required_header(headers, "x-dest-stream")?, "X-Dest-Stream")?;
    let sender_id = parse_i32(required_header(headers, "x-sender-id")?, "X-Sender-Id")?;
    let seq = required_header(headers, "x-seq")?
        .parse::<i64>()
        .map_err(|err| format!("X-Seq: {err}"))?;
    let eos = match required_header(headers, "x-eos")?.as_str() {
        "1" | "true" => true,
        "0" | "false" => false,
        other => return Err(format!("X-Eos must be 0 or 1, got {other}")),
    };
    let names = required_header(headers, "x-column-names")?
        .split(',')
        .filter(|name| !name.is_empty())
        .map(str::to_string)
        .collect::<Vec<_>>();
    let offset = headers
        .get("x-offset")
        .map(|value| {
            value
                .parse::<u64>()
                .map_err(|err| format!("X-Offset: {err}"))
        })
        .transpose()?
        .unwrap_or(0);
    let length = headers
        .get("x-length")
        .map(|value| {
            value
                .parse::<u64>()
                .map_err(|err| format!("X-Length: {err}"))
        })
        .transpose()?
        .unwrap_or(0);
    let rows = match headers.get("x-rows").map(String::as_str) {
        None | Some("") => None,
        Some(value) => Some(
            value
                .parse::<u64>()
                .map_err(|err| format!("X-Rows: {err}"))?,
        ),
    };
    Ok(PackedExchangeFrame {
        fragment_instance_id,
        dest_stream,
        sender_id,
        seq,
        eos,
        names,
        offset,
        length,
        rows,
        metadata: body.to_vec(),
    })
}

fn parse_i32(value: &str, name: &str) -> Result<i32, String> {
    value.parse::<i32>().map_err(|err| format!("{name}: {err}"))
}

fn parse_instance_id(value: &str) -> Result<FragmentInstanceId, String> {
    let uuid =
        uuid::Uuid::parse_str(value).map_err(|err| format!("fragment instance id: {err}"))?;
    let (hi, lo) = uuid.as_u64_pair();
    Ok(FragmentInstanceId::from_halves(hi as i64, lo as i64))
}

async fn write_http_response(
    stream: &mut TokioTcpStream,
    status: u16,
    extra_headers: &[(&str, String)],
    body: &[u8],
) -> Result<(), String> {
    let reason = match status {
        200 => "OK",
        400 => "Bad Request",
        404 => "Not Found",
        501 => "Not Implemented",
        _ => "Internal Server Error",
    };
    let mut response = format!(
        "HTTP/1.1 {status} {reason}\r\nContent-Type: application/octet-stream\r\nContent-Length: {}\r\nConnection: close\r\n",
        body.len()
    );
    for (name, value) in extra_headers {
        response.push_str(&format!("{name}: {value}\r\n"));
    }
    response.push_str("\r\n");
    stream
        .write_all(response.as_bytes())
        .await
        .map_err(|err| format!("write response: {err}"))?;
    stream
        .write_all(body)
        .await
        .map_err(|err| format!("write response body: {err}"))?;
    stream
        .flush()
        .await
        .map_err(|err| format!("flush: {err}"))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use starrocks_thrift::internal_service::{InternalServiceVersion, TExecPlanFragmentParams};

    use super::*;
    use crate::local_exchange::SenderSource;

    fn params() -> TExecPlanFragmentParams {
        TExecPlanFragmentParams {
            protocol_version: InternalServiceVersion::V1,
            fragment: None,
            desc_tbl: None,
            params: None,
            coord: None,
            backend_num: None,
            query_globals: None,
            query_options: None,
            enable_profile: None,
            resource_info: None,
            import_label: None,
            db_name: None,
            load_job_id: None,
            load_error_hub_info: None,
            is_pipeline: None,
            pipeline_dop: None,
            per_scan_node_dop: None,
            workgroup: None,
            enable_resource_group: None,
            func_version: None,
            enable_shared_scan: None,
            is_stream_pipeline: None,
            adaptive_dop_param: None,
            group_execution_scan_dop: None,
            pred_tree_params: None,
            exec_stats_node_ids: None,
            arrow_flight_sql_version: None,
        }
    }

    #[derive(Debug)]
    struct FakeMd {
        mine: Vec<u8>,
        loaded: Mutex<Vec<Vec<u8>>>,
    }

    impl NixlMdHandler for FakeMd {
        fn on_peer_md(&self, peer_metadata: &[u8]) -> Result<Vec<u8>, String> {
            self.loaded.lock().unwrap().push(peer_metadata.to_vec());
            Ok(self.mine.clone())
        }
    }

    #[derive(Debug)]
    struct FakeLeases {
        base: u64,
        next: Mutex<u64>,
    }

    impl StagingLeaseHandler for FakeLeases {
        fn lease(&self, length: u64) -> Result<RemoteLease, String> {
            let mut next = self.next.lock().unwrap();
            let offset = *next;
            *next += length;
            Ok(RemoteLease {
                remote_addr: self.base + offset,
                offset,
            })
        }

        fn release(&self, _offset: u64) -> Result<(), String> {
            Ok(())
        }
    }

    #[test]
    fn peer_http_port_uses_the_advertised_offset() {
        assert_eq!(peer_http_port(8060, 8040, 8060), 8040);
        assert_eq!(peer_http_port(19060, 19061, 19060), 19061);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn http_hop_delivers_packed_batch_and_eos_into_the_rendezvous() {
        let exchange = Arc::new(LocalExchange::default());
        let server = ExchangeHttpServer::start(
            "127.0.0.1:0".parse().unwrap(),
            exchange.clone(),
            None,
            None,
            None,
        )
        .await
        .unwrap();
        let peer = server.local_addr();
        let instance = FragmentInstanceId::from_halves(11, 22);
        let metadata = b"pack-meta".to_vec();

        post_exchange(
            peer,
            &PackedExchangeFrame {
                fragment_instance_id: instance,
                dest_stream: 7,
                sender_id: 0,
                seq: 0,
                eos: false,
                names: vec!["id".to_string()],
                offset: 4096,
                length: 32,
                rows: Some(5),
                metadata: metadata.clone(),
            },
        )
        .unwrap();
        post_exchange(
            peer,
            &PackedExchangeFrame {
                fragment_instance_id: instance,
                dest_stream: 7,
                sender_id: 0,
                seq: 1,
                eos: true,
                names: vec!["id".to_string()],
                offset: 0,
                length: 0,
                rows: None,
                metadata: Vec::new(),
            },
        )
        .unwrap();

        let ready = exchange
            .register_receiver(instance, vec![(7, 1)], params())
            .unwrap()
            .expect("eos already arrived over HTTP");
        let SenderSource::Remote {
            names,
            sender_id,
            batches,
            closed,
        } = &ready.inputs[0].sources[0]
        else {
            panic!("expected a remote source");
        };
        assert_eq!(names, &["id".to_string()]);
        assert_eq!(*sender_id, 0);
        assert!(*closed);
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].metadata, metadata);
        assert_eq!(batches[0].offset, 4096);
        assert_eq!(batches[0].len, 32);
        assert_eq!(batches[0].rows, Some(5));
        server.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn http_md_and_lease_routes_use_injected_handlers() {
        let exchange = Arc::new(LocalExchange::default());
        let md = Arc::new(FakeMd {
            mine: b"local-md".to_vec(),
            loaded: Mutex::new(Vec::new()),
        });
        let leases = Arc::new(FakeLeases {
            base: 0x1000,
            next: Mutex::new(0),
        });
        let server = ExchangeHttpServer::start(
            "127.0.0.1:0".parse().unwrap(),
            exchange,
            Some(md.clone()),
            Some(leases),
            None,
        )
        .await
        .unwrap();
        let peer = server.local_addr();

        let reply = post_nixl_md(peer, b"peer-md").unwrap();
        assert_eq!(reply, b"local-md");
        assert_eq!(md.loaded.lock().unwrap().as_slice(), &[b"peer-md".to_vec()]);

        let lease = post_staging_lease(peer, 64).unwrap();
        assert_eq!(
            lease,
            RemoteLease {
                remote_addr: 0x1000,
                offset: 0
            }
        );
        let lease2 = post_staging_lease(peer, 16).unwrap();
        assert_eq!(lease2.offset, 64);
        assert_eq!(lease2.remote_addr, 0x1000 + 64);
        server.shutdown().await;
    }
}
