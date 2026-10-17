//! The ring transport over NIXL and the GPU engine: the [`Fabric`] that WRITEs frames between
//! registered rings, the [`RingEngine`] over the engine's `RingExchange`, and the [`Control`] that
//! rides `transmit_chunk`. The protocol itself is in [`crate::ring_transport`].

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

use nixl_sys::{
    Agent, MemType, MemoryRegion, NixlDescriptor, NotificationMap, OptArgs, RegistrationHandle,
    XferDescList, XferOp, XferRequest, XferStatus,
};
use tracing::info;

use crate::fragment_executor::SenderSlot;
use crate::nixl_chunk::{NixlEndpoint, NixlEnvelope};
use crate::nixl_transport::{call, check_single_visible_device, send_failed};
use crate::proto::starrocks::PTransmitChunkParams;
use crate::ring_transport::{
    Control, Fabric, Landing, RingEngine, RingSettings, RingTransport, WriteId,
};

/// Appended to bring-up errors, whose usual cause is the environment.
const ENV_HINT: &str = "source experimental/starrocks/scripts/cn-env.sh (NIXL_PREFIX, \
                        NIXL_PLUGIN_DIR, LD_LIBRARY_PATH) and set \
                        UCX_TLS=cuda_copy,cuda_ipc,tcp,self";

/// The engine's ring sizes for the ring exchange (`SIRIUS_CN_EXCHANGE=ring`, the default), from
/// the environment; `None` for the direct exchange (`SIRIUS_CN_EXCHANGE=direct`).
///
/// - `SIRIUS_CN_RING_FRAME_BYTES`: frame size, 64 MiB;
/// - `SIRIUS_CN_RING_SEND_BYTES` / `SIRIUS_CN_RING_RECV_BYTES`: ring sizes, 1 GiB / 1.5 GiB;
/// - `SIRIUS_CN_RING_GPU_LANDING_FRACTION`: share of the GPU space landed batches may hold, 0.1;
/// - `SIRIUS_CN_RING_HOST_LANDING_BYTES`: host bytes landed batches may hold, 48 GiB.
///
/// Sizes take a `KiB`, `MiB` or `GiB` suffix.
pub fn ring_options_from_env() -> Result<Option<sirius::RingOptions>, String> {
    let env = |name: &str| std::env::var(name).ok();
    match env("SIRIUS_CN_EXCHANGE").as_deref().map(str::trim) {
        Some("direct") => return Ok(None),
        None | Some("") | Some("ring") => {}
        Some(other) => {
            return Err(format!(
                "SIRIUS_CN_EXCHANGE must be ring or direct, not '{other}'"
            ));
        }
    }
    let bytes = |name: &str, default: u64| match env(name) {
        None => Ok(default),
        Some(value) => parse_bytes(&value)
            .filter(|&n| n > 0)
            .ok_or_else(|| format!("{name} must be a positive size, not '{value}'")),
    };
    let fraction = match env("SIRIUS_CN_RING_GPU_LANDING_FRACTION") {
        None => 0.1,
        Some(value) => value
            .trim()
            .parse::<f64>()
            .ok()
            .filter(|f| (0.0..=1.0).contains(f))
            .ok_or_else(|| {
                format!("SIRIUS_CN_RING_GPU_LANDING_FRACTION must be in [0, 1], not '{value}'")
            })?,
    };
    Ok(Some(sirius::RingOptions {
        frame_bytes: bytes("SIRIUS_CN_RING_FRAME_BYTES", 64 << 20)?,
        send_bytes: bytes("SIRIUS_CN_RING_SEND_BYTES", 1 << 30)?,
        recv_bytes: bytes("SIRIUS_CN_RING_RECV_BYTES", 3 << 29)?,
        gpu_landing_fraction: fraction,
        host_landing_bytes: bytes("SIRIUS_CN_RING_HOST_LANDING_BYTES", 48 << 30)?,
    }))
}

/// `123`, `64MiB`, `1.5GiB`.
fn parse_bytes(value: &str) -> Option<u64> {
    let value = value.trim();
    let (number, unit) = match value.find(|c: char| c.is_ascii_alphabetic()) {
        Some(at) => value.split_at(at),
        None => (value, ""),
    };
    let scale: u64 = match unit.trim() {
        "" | "B" => 1,
        "KiB" | "K" => 1 << 10,
        "MiB" | "M" => 1 << 20,
        "GiB" | "G" => 1 << 30,
        _ => return None,
    };
    let number: f64 = number.trim().parse().ok()?;
    (number >= 0.0).then(|| (number * scale as f64) as u64)
}

/// Starts the ring transport for agent `agent_name` over the engine's `ring`, landing batches in
/// `exchange`'s token table. Fails here, before any query, on a broken NIXL install.
pub fn start_ring_transport(
    agent_name: String,
    ring: sirius::RingExchange,
    exchange: Arc<sirius::DirectExchange>,
) -> Result<Arc<dyn NixlEndpoint>, String> {
    let settings = RingSettings::from_env()?;
    let engine = Arc::new(EngineRing { ring, exchange });
    let fabric = NixlFabric::bring_up(&agent_name, engine.as_ref())?;
    Ok(Arc::new(RingTransport::start(
        fabric,
        engine,
        Arc::new(PrpcControl),
        settings,
    )?))
}

// ---- engine --------------------------------------------------------------------------------

/// The engine's `RingExchange` and the direct exchange whose tokens landed batches take.
#[derive(Debug)]
struct EngineRing {
    ring: sirius::RingExchange,
    exchange: Arc<sirius::DirectExchange>,
}

fn text(err: impl std::fmt::Display) -> String {
    err.to_string()
}

impl RingEngine for EngineRing {
    fn frame_bytes(&self) -> u64 {
        self.ring.frame_bytes()
    }

    fn send_region(&self) -> (i32, usize, u64) {
        let (base, len) = self.ring.send_region();
        (self.ring.device(), base, len)
    }

    fn recv_region(&self) -> (i32, usize, u64) {
        let (base, len) = self.ring.recv_region();
        (self.ring.device(), base, len)
    }

    fn lease_send(&self) -> Result<Option<u32>, String> {
        self.ring.lease_send().map_err(text)
    }

    fn release_send(&self, frame: u32) -> Result<(), String> {
        self.ring.release_send(frame).map_err(text)
    }

    fn lease_recv(&self) -> Result<Option<u32>, String> {
        self.ring.lease_recv().map_err(text)
    }

    fn release_recv(&self, frame: u32) -> Result<(), String> {
        self.ring.release_recv(frame).map_err(text)
    }

    fn stage(&self, token: u64, k: u32, frame: u32) -> Result<u64, String> {
        self.ring.stage(token, k, frame).map_err(text)
    }

    fn release_job(&self, token: u64) {
        if let Err(err) = self.ring.release_job(token) {
            tracing::warn!(token, error = %err, "failed to release a ring job");
        }
    }

    fn begin(&self, header: &[u8]) -> Result<Landing, String> {
        let landing = self.ring.begin(header).map_err(text)?;
        Ok(Landing {
            id: landing.id,
            tier: landing.tier,
            rows: landing.rows,
            frames: landing.frames,
        })
    }

    fn copy_out(&self, landing: u64, k: u32, frame: u32, len: u64) -> Result<u64, String> {
        self.ring.copy_out(landing, k, frame, len).map_err(text)
    }

    fn seal(&self, landing: u64) -> Result<u64, String> {
        self.ring.seal(landing).map_err(text)
    }

    fn abort(&self, landing: u64) {
        if let Err(err) = self.ring.abort(landing) {
            tracing::warn!(landing, error = %err, "failed to abort a ring landing");
        }
    }

    fn done(&self, ticket: u64) -> Result<bool, String> {
        self.ring.done(ticket).map_err(text)
    }

    fn release_token(&self, token: u64) {
        if let Err(err) = self.exchange.release(token) {
            tracing::warn!(token, error = %err, "failed to release a landed batch");
        }
    }

    fn outstanding(&self) -> usize {
        self.exchange.outstanding().unwrap_or(0)
    }

    fn stats_line(&self) -> String {
        self.ring.stats_line()
    }
}

// ---- fabric --------------------------------------------------------------------------------

/// One ring as a NIXL memory region.
#[derive(Debug)]
struct RingRegion {
    base: usize,
    len: usize,
    device: u64,
}

impl MemoryRegion for RingRegion {
    unsafe fn as_ptr(&self) -> *const u8 {
        self.base as *const u8
    }

    fn size(&self) -> usize {
        self.len
    }
}

impl NixlDescriptor for RingRegion {
    fn mem_type(&self) -> MemType {
        MemType::Vram
    }

    fn device_id(&self) -> u64 {
        self.device
    }
}

/// NIXL with the UCX backend, the two rings registered and nothing else.
struct NixlFabric {
    // Dropped before the agent.
    _send: RegistrationHandle,
    _recv: RegistrationHandle,
    agent: Agent,
    name: String,
    md: Vec<u8>,
    device: u64,
    send_base: usize,
    frame_bytes: u64,
    writes: HashMap<WriteId, XferRequest>,
    next_write: WriteId,
    notes: Notes,
}

/// The notification map the fabric reads into.
struct Notes(NotificationMap);

// SAFETY: the map is a plain container the C API fills; the fabric that owns it moves to the
// transport thread once, before any use, and only that thread touches it.
unsafe impl Send for Notes {}

impl NixlFabric {
    fn bring_up(name: &str, engine: &dyn RingEngine) -> Result<Self, String> {
        if nixl_sys::is_stub() {
            return Err(format!(
                "nixl-sys was built as a stub ({ENV_HINT}); if it was built before sourcing \
                 cn-env.sh, run `cargo clean -p nixl-sys` and rebuild"
            ));
        }
        check_single_visible_device(std::env::var("CUDA_VISIBLE_DEVICES").ok().as_deref())?;
        let agent = Agent::new(name)
            .map_err(|err| format!("failed to create nixl agent '{name}': {err} ({ENV_HINT})"))?;
        let (_, params) = agent
            .get_plugin_params("UCX")
            .map_err(|err| format!("nixl UCX plugin unavailable: {err} ({ENV_HINT})"))?;
        let backend = agent
            .create_backend("UCX", &params)
            .map_err(|err| format!("failed to create the nixl UCX backend: {err} ({ENV_HINT})"))?;
        let mut options =
            OptArgs::new().map_err(|err| format!("failed to create nixl options: {err}"))?;
        options
            .add_backend(&backend)
            .map_err(|err| format!("failed to select the nixl UCX backend: {err}"))?;
        let register = |(device, base, len): (i32, usize, u64), which: &str| {
            agent
                .register_memory(
                    &RingRegion {
                        base,
                        len: len as usize,
                        device: device as u64,
                    },
                    Some(&options),
                )
                .map_err(|err| {
                    format!(
                        "failed to register the {len}-byte {which} ring with nixl: {err}; \
                         UCX_TLS must include cuda_copy ({ENV_HINT})"
                    )
                })
        };
        let send_region = engine.send_region();
        let recv_region = engine.recv_region();
        let send = register(send_region, "send")?;
        let recv = register(recv_region, "receive")?;
        let md = agent
            .get_local_md()
            .map_err(|err| format!("failed to serialize nixl agent metadata: {err}"))?;
        let notes =
            NotificationMap::new().map_err(|err| format!("failed to create a nixl map: {err}"))?;
        info!(
            agent = name,
            device = send_region.0,
            send_base = send_region.1,
            send_len = send_region.2,
            recv_base = recv_region.1,
            recv_len = recv_region.2,
            frame_bytes = engine.frame_bytes(),
            "nixl ring fabric ready; send and receive rings registered"
        );
        Ok(Self {
            _send: send,
            _recv: recv,
            agent,
            name: name.to_string(),
            md,
            device: send_region.0 as u64,
            send_base: send_region.1,
            frame_bytes: engine.frame_bytes(),
            writes: HashMap::new(),
            next_write: 1,
            notes: Notes(notes),
        })
    }
}

impl Fabric for NixlFabric {
    fn local_md(&self) -> Vec<u8> {
        self.md.clone()
    }

    fn local_name(&self) -> String {
        self.name.clone()
    }

    fn load_peer(&mut self, md: &[u8]) -> Result<String, String> {
        let name = self
            .agent
            .load_remote_md(md)
            .map_err(|err| format!("failed to load a peer's nixl metadata: {err}"))?;
        // Connect now, so the first frame does not pay for it.
        if let Err(err) = self.agent.make_connection(&name, None) {
            tracing::warn!(peer = %name, error = %err, "failed to pre-connect to a nixl peer");
        }
        Ok(name)
    }

    fn write(
        &mut self,
        agent: &str,
        frame: u32,
        dst: u64,
        device: i32,
        len: u64,
        note: &str,
    ) -> Result<WriteId, String> {
        let descriptors = || {
            XferDescList::new(MemType::Vram)
                .map_err(|err| format!("failed to create a nixl descriptor list: {err}"))
        };
        let (mut local, mut remote) = (descriptors()?, descriptors()?);
        local.add_desc(
            self.send_base + frame as usize * self.frame_bytes as usize,
            len as usize,
            self.device,
        );
        remote.add_desc(dst as usize, len as usize, device as u64);
        let mut options =
            OptArgs::new().map_err(|err| format!("failed to create nixl options: {err}"))?;
        options
            .set_notification_message(note.as_bytes())
            .and_then(|()| options.set_has_notification(true))
            .map_err(|err| format!("failed to attach a ring notification: {err}"))?;
        let request = self
            .agent
            .create_xfer_req(XferOp::Write, &local, &remote, agent, Some(&options))
            .map_err(|err| format!("failed to create a ring WRITE to agent '{agent}': {err}"))?;
        self.agent
            .post_xfer_req(&request, Some(&options))
            .map_err(|err| format!("failed to post a ring WRITE to agent '{agent}': {err}"))?;
        let id = self.next_write;
        self.next_write += 1;
        self.writes.insert(id, request);
        Ok(id)
    }

    fn poll_write(&mut self, id: WriteId) -> Result<bool, String> {
        let request = self
            .writes
            .get(&id)
            .ok_or_else(|| format!("unknown ring WRITE {id}"))?;
        match self.agent.get_xfer_status(request) {
            Ok(XferStatus::Success) => Ok(true),
            Ok(XferStatus::InProgress) => Ok(false),
            Err(err) => Err(format!("{err}")),
        }
    }

    fn forget_write(&mut self, id: WriteId) {
        self.writes.remove(&id);
    }

    fn notify(&mut self, agent: &str, note: &str) -> Result<(), String> {
        self.agent
            .send_notification(agent, note.as_bytes(), None)
            .map_err(|err| format!("failed to notify agent '{agent}': {err}"))
    }

    fn take_notes(&mut self) -> Result<Vec<(String, String)>, String> {
        self.agent
            .get_notifications(&mut self.notes.0, None)
            .map_err(|err| format!("{err}"))?;
        let notes = self.notes.0.take_notifs().map_err(|err| format!("{err}"))?;
        Ok(notes
            .into_iter()
            .flat_map(|(agent, texts)| texts.into_iter().map(move |text| (agent.clone(), text)))
            .collect())
    }
}

// ---- control -------------------------------------------------------------------------------

/// `transmit_chunk` round trips, as the direct transport makes them.
struct PrpcControl;

impl Control for PrpcControl {
    fn call(
        &self,
        peer: SocketAddr,
        params: PTransmitChunkParams,
        envelope: &NixlEnvelope,
    ) -> Result<Vec<u8>, String> {
        call(peer, params, envelope)
    }

    fn fail(&self, ends: Vec<(SocketAddr, SenderSlot)>, error: &str) {
        send_failed(ends, error);
    }
}

#[cfg(test)]
mod tests {
    use super::parse_bytes;

    #[test]
    fn sizes_take_binary_suffixes() {
        assert_eq!(parse_bytes("123"), Some(123));
        assert_eq!(parse_bytes("64MiB"), Some(64 << 20));
        assert_eq!(parse_bytes(" 1.5GiB "), Some(3 << 29));
        assert_eq!(parse_bytes("48G"), Some(48 << 30));
        assert_eq!(parse_bytes("2KiB"), Some(2048));
        assert_eq!(parse_bytes("lots"), None);
        assert_eq!(parse_bytes("5TB"), None);
    }
}
