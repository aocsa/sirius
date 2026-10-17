//! Messages of the ring exchange.
//!
//! Batches travel between CNs as frames: fixed-size slices of a batch's byte stream, written by
//! NIXL from the sender's send ring into a receive-ring frame the receiver credited to the lane.
//! Three kinds of NIXL notification carry the control that goes with them, so no batch needs an
//! RPC of its own:
//!
//! | Kind | From | Body |
//! |---|---|---|
//! | `Frame` | sender, with each WRITE | `u64 lane, u64 batch, u32 k, u32 frame, u64 len, u32 n, n x header byte` |
//! | `Credit` | receiver, once a frame was copied out | `u64 lane, u32 n, n x u32 frame` |
//! | `Refuse` | receiver, when a lane cannot go on | `u64 lane, utf-8 error` |
//!
//! `Frame` k = 0 carries the batch's header (the engine's wire header); the others carry none.
//! A notification is the `RNG1` magic, a kind byte and the little-endian body, hex-encoded:
//! NIXL hands notifications over as UTF-8 strings.
//!
//! Lanes are set up and torn down over `transmit_chunk` (see [`crate::nixl_chunk`]): `RingOpen`
//! carries the sender's agent metadata and the hop's column names, and its reply grants the lane
//! its standing credits; `RingClose` returns them once the lane is done. EOS and failure frames
//! stay as they are.

const MAGIC: [u8; 4] = *b"RNG1";
const FRAME: u8 = 1;
const CREDIT: u8 = 2;
const REFUSE: u8 = 3;

/// One ring notification.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum RingNote {
    /// Frame `k` of batch `batch` on `lane` landed in receive frame `frame`, `len` bytes.
    Frame {
        lane: u64,
        batch: u64,
        k: u32,
        frame: u32,
        len: u64,
        /// The batch's header, on frame 0 only.
        header: Vec<u8>,
    },
    /// The receiver copied these frames out: `lane` may write into them again.
    Credit { lane: u64, frames: Vec<u32> },
    /// The receiver cannot go on with `lane`, for `error`.
    Refuse { lane: u64, error: String },
}

impl RingNote {
    pub(crate) fn encode(&self) -> String {
        let mut out = Vec::from(MAGIC);
        match self {
            Self::Frame {
                lane,
                batch,
                k,
                frame,
                len,
                header,
            } => {
                out.push(FRAME);
                out.extend_from_slice(&lane.to_le_bytes());
                out.extend_from_slice(&batch.to_le_bytes());
                out.extend_from_slice(&k.to_le_bytes());
                out.extend_from_slice(&frame.to_le_bytes());
                out.extend_from_slice(&len.to_le_bytes());
                out.extend_from_slice(&(header.len() as u32).to_le_bytes());
                out.extend_from_slice(header);
            }
            Self::Credit { lane, frames } => {
                out.push(CREDIT);
                out.extend_from_slice(&lane.to_le_bytes());
                out.extend_from_slice(&(frames.len() as u32).to_le_bytes());
                for frame in frames {
                    out.extend_from_slice(&frame.to_le_bytes());
                }
            }
            Self::Refuse { lane, error } => {
                out.push(REFUSE);
                out.extend_from_slice(&lane.to_le_bytes());
                out.extend_from_slice(error.as_bytes());
            }
        }
        hex_encode(&out)
    }

    pub(crate) fn decode(text: &str) -> Result<Self, String> {
        let bytes = hex_decode(text)?;
        let mut reader = Reader(&bytes);
        if reader.take(4)? != MAGIC {
            return Err("not a ring notification".to_string());
        }
        let note = match reader.u8()? {
            FRAME => {
                let lane = reader.u64()?;
                let batch = reader.u64()?;
                let k = reader.u32()?;
                let frame = reader.u32()?;
                let len = reader.u64()?;
                let n = reader.u32()? as usize;
                Self::Frame {
                    lane,
                    batch,
                    k,
                    frame,
                    len,
                    header: reader.take(n)?.to_vec(),
                }
            }
            CREDIT => {
                let lane = reader.u64()?;
                let n = reader.u32()? as usize;
                if n > reader.0.len() / 4 {
                    return Err("ring credit count out of range".to_string());
                }
                let frames = (0..n).map(|_| reader.u32()).collect::<Result<_, _>>()?;
                Self::Credit { lane, frames }
            }
            REFUSE => {
                let lane = reader.u64()?;
                let error = String::from_utf8(reader.rest())
                    .map_err(|err| format!("ring refusal is not utf-8: {err}"))?;
                Self::Refuse { lane, error }
            }
            kind => return Err(format!("unknown ring notification kind {kind}")),
        };
        reader.finish()?;
        Ok(note)
    }
}

/// What a `RingOpen` reply grants: a lane id, where its receive frames are, and the frames
/// themselves as standing credits. No frames means the receiver is busy; the sender asks again.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct LaneGrant {
    pub lane: u64,
    /// The receiver's CUDA device and receive ring.
    pub device: i32,
    pub recv_base: u64,
    pub frame_bytes: u64,
    /// The receiver's agent name, which the sender WRITEs to.
    pub agent: String,
    pub frames: Vec<u32>,
}

impl LaneGrant {
    pub(crate) fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(&self.lane.to_le_bytes());
        out.extend_from_slice(&self.device.to_le_bytes());
        out.extend_from_slice(&self.recv_base.to_le_bytes());
        out.extend_from_slice(&self.frame_bytes.to_le_bytes());
        out.extend_from_slice(&(self.agent.len() as u32).to_le_bytes());
        out.extend_from_slice(self.agent.as_bytes());
        out.extend_from_slice(&(self.frames.len() as u32).to_le_bytes());
        for frame in &self.frames {
            out.extend_from_slice(&frame.to_le_bytes());
        }
        out
    }

    pub(crate) fn decode(bytes: &[u8]) -> Result<Self, String> {
        let mut reader = Reader(bytes);
        let lane = reader.u64()?;
        let device = reader.u32()? as i32;
        let recv_base = reader.u64()?;
        let frame_bytes = reader.u64()?;
        let n = reader.u32()? as usize;
        let agent = String::from_utf8(reader.take(n)?.to_vec())
            .map_err(|err| format!("ring lane agent is not utf-8: {err}"))?;
        let n = reader.u32()? as usize;
        if n > reader.0.len() / 4 {
            return Err("ring lane frame count out of range".to_string());
        }
        let frames = (0..n).map(|_| reader.u32()).collect::<Result<_, _>>()?;
        reader.finish()?;
        Ok(Self {
            lane,
            device,
            recv_base,
            frame_bytes,
            agent,
            frames,
        })
    }
}

/// The body of a `RingOpen` request: the sender's agent metadata, so the receiver can send it
/// credits, and the hop's column names, which every batch the lane delivers carries.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct LaneOpen {
    pub md: Vec<u8>,
    pub names: Vec<String>,
}

impl LaneOpen {
    pub(crate) fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(&(self.md.len() as u32).to_le_bytes());
        out.extend_from_slice(&self.md);
        out.extend_from_slice(&(self.names.len() as u32).to_le_bytes());
        for name in &self.names {
            out.extend_from_slice(&(name.len() as u32).to_le_bytes());
            out.extend_from_slice(name.as_bytes());
        }
        out
    }

    pub(crate) fn decode(bytes: &[u8]) -> Result<Self, String> {
        let mut reader = Reader(bytes);
        let n = reader.u32()? as usize;
        let md = reader.take(n)?.to_vec();
        let count = reader.u32()? as usize;
        if count > reader.0.len() / 4 {
            return Err("ring lane name count out of range".to_string());
        }
        let names = (0..count)
            .map(|_| {
                let len = reader.u32()? as usize;
                String::from_utf8(reader.take(len)?.to_vec())
                    .map_err(|err| format!("ring lane name is not utf-8: {err}"))
            })
            .collect::<Result<_, _>>()?;
        reader.finish()?;
        Ok(Self { md, names })
    }
}

fn hex_encode(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for &b in bytes {
        out.push(DIGITS[(b >> 4) as usize] as char);
        out.push(DIGITS[(b & 15) as usize] as char);
    }
    out
}

fn hex_decode(text: &str) -> Result<Vec<u8>, String> {
    let digit = |c: u8| match c {
        b'0'..=b'9' => Ok(c - b'0'),
        b'a'..=b'f' => Ok(c - b'a' + 10),
        _ => Err(format!("ring notification has a non-hex byte {c:#x}")),
    };
    let bytes = text.as_bytes();
    if bytes.len() % 2 != 0 {
        return Err("ring notification has an odd length".to_string());
    }
    bytes
        .chunks_exact(2)
        .map(|pair| Ok(digit(pair[0])? << 4 | digit(pair[1])?))
        .collect()
}

/// Little-endian cursor.
struct Reader<'a>(&'a [u8]);

impl<'a> Reader<'a> {
    fn take(&mut self, len: usize) -> Result<&'a [u8], String> {
        if self.0.len() < len {
            return Err("ring message is truncated".to_string());
        }
        let (head, rest) = self.0.split_at(len);
        self.0 = rest;
        Ok(head)
    }

    fn u8(&mut self) -> Result<u8, String> {
        Ok(self.take(1)?[0])
    }

    fn u32(&mut self) -> Result<u32, String> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().unwrap()))
    }

    fn u64(&mut self) -> Result<u64, String> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }

    fn rest(&mut self) -> Vec<u8> {
        std::mem::take(&mut self.0).to_vec()
    }

    fn finish(&self) -> Result<(), String> {
        if self.0.is_empty() {
            Ok(())
        } else {
            Err(format!("ring message has {} trailing bytes", self.0.len()))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn notes_round_trip() {
        for note in [
            RingNote::Frame {
                lane: 7,
                batch: 3,
                k: 0,
                frame: 11,
                len: 64 << 20,
                header: vec![1, 2, 3, 255],
            },
            RingNote::Frame {
                lane: u64::MAX,
                batch: 0,
                k: 9,
                frame: 0,
                len: 1,
                header: Vec::new(),
            },
            RingNote::Credit {
                lane: 2,
                frames: vec![0, 5, 23],
            },
            RingNote::Credit {
                lane: 2,
                frames: Vec::new(),
            },
            RingNote::Refuse {
                lane: 1,
                error: "ring exchange: no tier has room".to_string(),
            },
        ] {
            let text = note.encode();
            assert!(text.is_ascii());
            assert_eq!(RingNote::decode(&text).unwrap(), note);
        }
    }

    #[test]
    fn notes_refuse_garbage() {
        assert!(RingNote::decode("zz").is_err());
        assert!(RingNote::decode("abc").is_err());
        assert!(RingNote::decode(&hex_encode(b"RNG2\x01")).is_err());
        assert!(RingNote::decode(&hex_encode(b"RNG1\x09")).is_err());
        let mut credit = RingNote::Credit {
            lane: 1,
            frames: vec![1],
        }
        .encode();
        credit.push_str("00");
        assert!(RingNote::decode(&credit).unwrap_err().contains("trailing"));
        let forged = {
            let mut b = Vec::from(MAGIC);
            b.push(CREDIT);
            b.extend_from_slice(&1u64.to_le_bytes());
            b.extend_from_slice(&u32::MAX.to_le_bytes());
            hex_encode(&b)
        };
        assert!(
            RingNote::decode(&forged)
                .unwrap_err()
                .contains("out of range")
        );
    }

    #[test]
    fn lane_messages_round_trip() {
        let grant = LaneGrant {
            lane: 42,
            device: 3,
            recv_base: 0x7f00_0000_0000,
            frame_bytes: 64 << 20,
            agent: "10.0.0.1:8060".to_string(),
            frames: vec![4, 5, 6, 7],
        };
        assert_eq!(LaneGrant::decode(&grant.encode()).unwrap(), grant);
        let open = LaneOpen {
            md: vec![9; 300],
            names: vec!["l_orderkey".to_string(), String::new()],
        };
        assert_eq!(LaneOpen::decode(&open.encode()).unwrap(), open);
        assert!(LaneOpen::decode(&open.encode()[..10]).is_err());
    }
}
