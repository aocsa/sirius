//! Sirius envelope on unpatched `PInternalService.transmit_chunk`.
//!
//! Control for the NIXL packed hop uses the FE-advertised brpc port and the existing
//! `transmit_chunk` method. The protobuf names routing (`finst_id`, `node_id`, `sender_id`,
//! `sequence`, `eos`); the attachment is tagged `SRNX` plus a kind byte. GPU payload never
//! rides this RPC. Native `ChunkPB`, pass-through, and pipeline-level shuffle are rejected so
//! a stock BE cannot be misread as Sirius.

use crate::proto::starrocks::PTransmitChunkParams;
use crate::result_store::FragmentInstanceId;

/// ASCII `SRNX` (0x53524E58 as a big-endian u32). On the wire the four bytes are `S R N X`;
/// remaining integers in the envelope are little-endian.
pub const MAGIC: [u8; 4] = *b"SRNX";

/// Kind byte after [`MAGIC`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub(crate) enum NixlKind {
    Md = 1,
    Lease = 2,
    Packed = 3,
    CanaryRelease = 4,
}

impl NixlKind {
    fn from_u8(value: u8) -> Result<Self, String> {
        match value {
            1 => Ok(Self::Md),
            2 => Ok(Self::Lease),
            3 => Ok(Self::Packed),
            4 => Ok(Self::CanaryRelease),
            other => Err(format!("unknown SRNX kind {other}")),
        }
    }
}

/// Request attachment after `SRNX` + kind.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum NixlEnvelope {
    /// Sender `get_local_md()` blob. Reply attachment is the receiver's blob (no envelope).
    Md(Vec<u8>),
    /// Request a receiver-side arena lease of `length` bytes.
    Lease { length: u64 },
    /// Announce that a NIXL WRITE has filled `offset`/`length` in the receiver arena.
    Packed {
        offset: u64,
        length: u64,
        rows: u64,
        names: Vec<String>,
        metadata: Vec<u8>,
    },
    /// Release a canary lease without touching the exchange rendezvous.
    CanaryRelease { offset: u64 },
}

impl NixlEnvelope {
    /// Encodes `SRNX` + kind + kind-specific payload.
    pub(crate) fn encode(&self) -> Vec<u8> {
        let mut out = Vec::from(MAGIC);
        match self {
            Self::Md(blob) => {
                out.push(NixlKind::Md as u8);
                out.extend_from_slice(blob);
            }
            Self::Lease { length } => {
                out.push(NixlKind::Lease as u8);
                out.extend_from_slice(&length.to_le_bytes());
            }
            Self::Packed {
                offset,
                length,
                rows,
                names,
                metadata,
            } => {
                out.push(NixlKind::Packed as u8);
                out.extend_from_slice(&offset.to_le_bytes());
                out.extend_from_slice(&length.to_le_bytes());
                out.extend_from_slice(&rows.to_le_bytes());
                let name_count = u32::try_from(names.len()).unwrap_or(u32::MAX);
                out.extend_from_slice(&name_count.to_le_bytes());
                for name in names {
                    let bytes = name.as_bytes();
                    let len = u32::try_from(bytes.len()).unwrap_or(u32::MAX);
                    out.extend_from_slice(&len.to_le_bytes());
                    out.extend_from_slice(bytes);
                }
                let meta_len = u32::try_from(metadata.len()).unwrap_or(u32::MAX);
                out.extend_from_slice(&meta_len.to_le_bytes());
                out.extend_from_slice(metadata);
            }
            Self::CanaryRelease { offset } => {
                out.push(NixlKind::CanaryRelease as u8);
                out.extend_from_slice(&offset.to_le_bytes());
            }
        }
        out
    }

    /// Decodes a request attachment. Does not inspect protobuf routing fields.
    pub(crate) fn decode(bytes: &[u8]) -> Result<Self, String> {
        if bytes.len() < 5 {
            return Err("transmit_chunk attachment is too short for an SRNX envelope".to_string());
        }
        if bytes[..4] != MAGIC {
            return Err(
                "transmit_chunk attachment is not an SRNX envelope; native ChunkPB must not \
                 ride this path"
                    .to_string(),
            );
        }
        let kind = NixlKind::from_u8(bytes[4])?;
        let rest = &bytes[5..];
        match kind {
            NixlKind::Md => Ok(Self::Md(rest.to_vec())),
            NixlKind::Lease => {
                let length = read_u64(rest, 0)?;
                if rest.len() != 8 {
                    return Err(
                        "SRNX Lease request must be exactly 8 bytes after the kind".to_string()
                    );
                }
                Ok(Self::Lease { length })
            }
            NixlKind::Packed => decode_packed(rest),
            NixlKind::CanaryRelease => {
                let offset = read_u64(rest, 0)?;
                if rest.len() != 8 {
                    return Err(
                        "SRNX CanaryRelease request must be exactly 8 bytes after the kind"
                            .to_string(),
                    );
                }
                Ok(Self::CanaryRelease { offset })
            }
        }
    }
}

fn decode_packed(rest: &[u8]) -> Result<NixlEnvelope, String> {
    let offset = read_u64(rest, 0)?;
    let length = read_u64(rest, 8)?;
    let rows = read_u64(rest, 16)?;
    let mut pos = 24;
    let name_count = read_u32(rest, pos)? as usize;
    pos += 4;
    let mut names = Vec::with_capacity(name_count);
    for _ in 0..name_count {
        let len = read_u32(rest, pos)? as usize;
        pos += 4;
        let end = pos
            .checked_add(len)
            .ok_or_else(|| "SRNX Packed name length overflow".to_string())?;
        let bytes = rest
            .get(pos..end)
            .ok_or_else(|| "SRNX Packed name truncated".to_string())?;
        names.push(
            String::from_utf8(bytes.to_vec())
                .map_err(|err| format!("SRNX Packed name is not utf-8: {err}"))?,
        );
        pos = end;
    }
    let meta_len = read_u32(rest, pos)? as usize;
    pos += 4;
    let end = pos
        .checked_add(meta_len)
        .ok_or_else(|| "SRNX Packed metadata length overflow".to_string())?;
    if end != rest.len() {
        return Err(
            "SRNX Packed attachment has trailing bytes or a truncated metadata blob".to_string(),
        );
    }
    let metadata = rest[pos..end].to_vec();
    Ok(NixlEnvelope::Packed {
        offset,
        length,
        rows,
        names,
        metadata,
    })
}

fn read_u64(bytes: &[u8], offset: usize) -> Result<u64, String> {
    let slice = bytes
        .get(offset..offset + 8)
        .ok_or_else(|| "SRNX envelope truncated reading u64".to_string())?;
    Ok(u64::from_le_bytes(slice.try_into().unwrap()))
}

fn read_u32(bytes: &[u8], offset: usize) -> Result<u32, String> {
    let slice = bytes
        .get(offset..offset + 4)
        .ok_or_else(|| "SRNX envelope truncated reading u32".to_string())?;
    Ok(u32::from_le_bytes(slice.try_into().unwrap()))
}

/// Lease reply attachment: `remote_addr` then `offset`, little-endian u64s. No `SRNX` prefix
/// — it is a two-integer grant, not a control kind.
pub(crate) fn encode_lease_reply(remote_addr: u64, offset: u64) -> Vec<u8> {
    let mut out = Vec::with_capacity(16);
    out.extend_from_slice(&remote_addr.to_le_bytes());
    out.extend_from_slice(&offset.to_le_bytes());
    out
}

/// Inverse of [`encode_lease_reply`].
pub(crate) fn decode_lease_reply(bytes: &[u8]) -> Result<(u64, u64), String> {
    if bytes.len() != 16 {
        return Err(format!(
            "lease reply attachment must be 16 bytes, got {}",
            bytes.len()
        ));
    }
    Ok((read_u64(bytes, 0)?, read_u64(bytes, 8)?))
}

/// Rejects StarRocks native shuffle so a stock BE `ChunkPB` cannot be treated as Sirius.
pub(crate) fn reject_native_chunk(params: &PTransmitChunkParams) -> Result<(), String> {
    if params.use_pass_through.unwrap_or(false) {
        return Err("pass-through transmit_chunk is not supported".to_string());
    }
    if params.is_pipeline_level_shuffle.unwrap_or(false) {
        return Err("pipeline-level shuffle transmit_chunk is not supported".to_string());
    }
    if !params.chunks.is_empty() {
        return Err(
            "transmit_chunk ChunkPB payloads are not supported; Sirius NIXL control must be in \
             the SRNX attachment"
                .to_string(),
        );
    }
    Ok(())
}

/// Protobuf for Md / Lease / CanaryRelease: empty chunks, no pass-through, no pipeline.
/// Routing fields are unset so these frames cannot be ingested as shuffle.
pub(crate) fn control_params() -> PTransmitChunkParams {
    PTransmitChunkParams {
        finst_id: None,
        node_id: None,
        sender_id: None,
        be_number: Some(0),
        eos: Some(false),
        sequence: Some(0),
        chunks: Vec::new(),
        query_statistics: None,
        use_pass_through: Some(false),
        is_pipeline_level_shuffle: Some(false),
        driver_sequences: Vec::new(),
    }
}

/// Protobuf for a Packed (or Packed-EOS) frame. Attachment carries the SRNX Packed body.
pub(crate) fn packed_params(
    fragment_instance_id: FragmentInstanceId,
    dest_stream: i32,
    sender_id: i32,
    seq: i64,
    eos: bool,
) -> PTransmitChunkParams {
    PTransmitChunkParams {
        finst_id: Some(fragment_instance_id.to_proto()),
        node_id: Some(dest_stream),
        sender_id: Some(sender_id),
        be_number: Some(0),
        eos: Some(eos),
        sequence: Some(seq),
        chunks: Vec::new(),
        query_statistics: None,
        use_pass_through: Some(false),
        is_pipeline_level_shuffle: Some(false),
        driver_sequences: Vec::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::starrocks::ChunkPb;

    #[test]
    fn md_lease_packed_and_canary_round_trip() {
        let md = NixlEnvelope::Md(b"agent-md-blob".to_vec());
        assert_eq!(NixlEnvelope::decode(&md.encode()).unwrap(), md);

        let lease = NixlEnvelope::Lease { length: 1_048_576 };
        assert_eq!(NixlEnvelope::decode(&lease.encode()).unwrap(), lease);

        let packed = NixlEnvelope::Packed {
            offset: 0x2000,
            length: 1_048_576,
            rows: 7,
            names: vec!["region".into(), "sum".into()],
            metadata: b"pack-meta".to_vec(),
        };
        assert_eq!(NixlEnvelope::decode(&packed.encode()).unwrap(), packed);

        let canary = NixlEnvelope::CanaryRelease { offset: 0x1000 };
        assert_eq!(NixlEnvelope::decode(&canary.encode()).unwrap(), canary);
    }

    #[test]
    fn magic_is_ascii_srnx() {
        let encoded = NixlEnvelope::Md(Vec::new()).encode();
        assert_eq!(&encoded[..4], b"SRNX");
        assert_eq!(u32::from_be_bytes(MAGIC), 0x5352_4E58);
    }

    #[test]
    fn lease_reply_is_two_little_endian_u64s() {
        let bytes = encode_lease_reply(0xB000_2000, 0x2000);
        assert_eq!(decode_lease_reply(&bytes).unwrap(), (0xB000_2000, 0x2000));
        assert!(decode_lease_reply(&[0u8; 8]).is_err());
    }

    #[test]
    fn reject_wrong_magic_and_truncated() {
        assert!(NixlEnvelope::decode(b"PRPC").is_err());
        assert!(NixlEnvelope::decode(b"SRNX").is_err());
        let mut short_lease = Vec::from(MAGIC);
        short_lease.push(NixlKind::Lease as u8);
        short_lease.extend_from_slice(&1u32.to_le_bytes());
        assert!(NixlEnvelope::decode(&short_lease).is_err());
    }

    #[test]
    fn reject_native_chunk_guards() {
        assert!(reject_native_chunk(&control_params()).is_ok());

        let mut pass = control_params();
        pass.use_pass_through = Some(true);
        assert!(
            reject_native_chunk(&pass)
                .unwrap_err()
                .contains("pass-through")
        );

        let mut pipeline = control_params();
        pipeline.is_pipeline_level_shuffle = Some(true);
        assert!(
            reject_native_chunk(&pipeline)
                .unwrap_err()
                .contains("pipeline")
        );

        let mut chunks = control_params();
        chunks.chunks.push(ChunkPb::default());
        assert!(
            reject_native_chunk(&chunks)
                .unwrap_err()
                .contains("ChunkPB")
        );
    }

    #[test]
    fn packed_params_carry_routing_and_empty_chunks() {
        let id = FragmentInstanceId::from_halves(11, 22);
        let params = packed_params(id, 2, 0, 3, true);
        assert!(reject_native_chunk(&params).is_ok());
        assert_eq!(params.node_id, Some(2));
        assert_eq!(params.sender_id, Some(0));
        assert_eq!(params.sequence, Some(3));
        assert_eq!(params.eos, Some(true));
        assert!(params.chunks.is_empty());
        let decoded = FragmentInstanceId::from(params.finst_id.as_ref().unwrap());
        assert_eq!(decoded, id);
    }
}
