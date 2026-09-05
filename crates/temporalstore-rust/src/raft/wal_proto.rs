// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Encode a WAL record as a compact binary message rather than text.
//!
//! The text encoding spells out every field name on every record and has no byte-string type, so
//! a binary value becomes an array of decimal numbers. Measured: a record carrying a 20-byte
//! payload cost 782 bytes, and one carrying a 1,034-byte payload cost 3,823 -- roughly three
//! bytes written for every byte of data. Every one of those bytes is paid twice: once writing it,
//! and again on each replay that parses it back.
//!
//! **Fidelity comes before compactness here.** This is the durability path, so a command this
//! module does not model explicitly is carried byte for byte in the previous encoding rather than
//! approximated by the nearest match. Modelled commands can then be added one at a time, and a
//! command this module has never heard of still replays exactly. The same applies to the record's
//! rarely-set fields, which travel whole in `rest`.

use std::io;

use prost::Message;

use super::*;
use crate::sdk::v1;

/// The record fields that sit at their default on nearly every record.
///
/// They are carried together in their existing encoding rather than modelled field by field:
/// modelling them would add a conversion to get wrong for each one, and save nothing on the
/// records that actually dominate the log, where every one of them is absent.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
struct RaftWalRecordRest {
    #[serde(default, skip_serializing_if = "RaftReplicaRole::is_default")]
    replica_role: RaftReplicaRole,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    joint_membership: Option<JointConsensusMembership>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    latest_external_snapshot_ref: Option<RaftExternalSnapshotRef>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    installed_snapshot: Option<RaftSnapshot>,
    #[serde(default, skip_serializing_if = "RaftPeerPipelineRuntimeState::is_default")]
    pipeline_state: RaftPeerPipelineRuntimeState,
    #[serde(default, skip_serializing_if = "RaftReadSafetyRuntimeState::is_default")]
    read_safety_state: RaftReadSafetyRuntimeState,
    #[serde(default, skip_serializing_if = "RaftMembershipRuntimeEvidence::is_default")]
    membership_evidence: RaftMembershipRuntimeEvidence,
}

impl RaftWalRecordRest {
    fn is_empty(&self) -> bool {
        *self == RaftWalRecordRest::default()
    }
}

/// Split a checksum into the bytes to store and whether they are its text rather than its digest.
///
/// A checksum is a hex digest in practice, and storing the digest costs half what its text does.
/// Anything that is not a clean round-trip through hex is kept as its own bytes instead, so this
/// never has to assume what a checksum looks like in order to return it unchanged.
fn checksum_to_bytes(checksum: &str) -> (Vec<u8>, bool) {
    match decode_hex(checksum) {
        Some(digest) if encode_hex(&digest) == checksum => (digest, false),
        _ => (checksum.as_bytes().to_vec(), true),
    }
}

fn checksum_from_bytes(bytes: &[u8], is_text: bool) -> String {
    if is_text {
        String::from_utf8_lossy(bytes).into_owned()
    } else {
        encode_hex(bytes)
    }
}

fn decode_hex(text: &str) -> Option<Vec<u8>> {
    if text.len() % 2 != 0 || text.is_empty() {
        return None;
    }
    let bytes = text.as_bytes();
    let mut out = Vec::with_capacity(text.len() / 2);
    for pair in bytes.chunks(2) {
        let hi = (pair[0] as char).to_digit(16)?;
        let lo = (pair[1] as char).to_digit(16)?;
        out.push(((hi << 4) | lo) as u8);
    }
    Some(out)
}

fn encode_hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        let _ = write!(out, "{byte:02x}");
    }
    out
}

/// Map a command onto its modelled form, or carry it verbatim.
///
/// A modelled arm is used ONLY when it can reproduce the command exactly. `StringSetEx` with a
/// zero TTL is the trap: the modelled form distinguishes the two commands by whether the TTL is
/// non-zero, so a zero-TTL `StringSetEx` would come back as a plain `StringSet`. It goes verbatim
/// instead -- a command that cannot round-trip through a modelled arm must not use one.
pub(crate) fn command_to_proto(command: &Command) -> io::Result<v1::wal_command::Kind> {
    Ok(match command_encoding(command)? {
        CommandEncoding::StringSet { key, value, ttl_ms } => {
            v1::wal_command::Kind::StringSet(v1::StringSet {
                key: key.to_owned(),
                value: value.to_vec(),
                ttl_ms,
            })
        }
        CommandEncoding::HashSet { key, field, value } => {
            v1::wal_command::Kind::HashSet(v1::HashSet {
                key: key.to_owned(),
                field: field.to_owned(),
                value: value.to_vec(),
            })
        }
        CommandEncoding::Owned(command) => command.kind.expect("verbatim arm always sets a kind"),
    })
}

/// A command in the shape it will be written in, borrowing its payload instead of owning a copy.
///
/// Building the owned message cost the payload twice on every write: once to fill the message and
/// once to serialise it. At a four-kilobyte value that is four kilobytes spent to describe four
/// kilobytes. The three modelled arms borrow their key and value here, so the write
/// copies them only into the buffer that goes to disk.
///
/// The verbatim arm still owns its bytes, and that is not an oversight: its JSON does not exist
/// until it is built, so there is nothing to borrow.
pub(crate) enum CommandEncoding<'a> {
    StringSet {
        key: &'a str,
        value: &'a [u8],
        ttl_ms: u64,
    },
    HashSet {
        key: &'a str,
        field: &'a str,
        value: &'a [u8],
    },
    Owned(v1::WalCommand),
}

/// Which arm a command takes. This is the ONLY place that decides, and `command_to_proto` is
/// written on top of it, so the two cannot answer differently -- the zero-TTL `StringSetEx` trap
/// documented above is decided once.
pub(crate) fn command_encoding(command: &Command) -> io::Result<CommandEncoding<'_>> {
    Ok(match command {
        Command::StringSet { key, value } => CommandEncoding::StringSet {
            key,
            value,
            ttl_ms: 0,
        },
        Command::StringSetEx { key, value, ttl_ms } if *ttl_ms > 0 => CommandEncoding::StringSet {
            key,
            value,
            ttl_ms: *ttl_ms,
        },
        Command::HashSet { key, field, value } => CommandEncoding::HashSet { key, field, value },
        other => CommandEncoding::Owned(v1::WalCommand {
            kind: Some(v1::wal_command::Kind::Verbatim(
                serde_json::to_vec(other).map_err(io::Error::other)?,
            )),
        }),
    })
}

/// Bytes a length-delimited field occupies: its key, its length, and its payload.
pub(crate) fn len_delimited_len(tag: u32, payload_len: usize) -> usize {
    prost::encoding::key_len(tag)
        + prost::encoding::encoded_len_varint(payload_len as u64)
        + payload_len
}

/// Bytes a varint field occupies, or none at all when the value is zero.
///
/// proto3 omits a scalar field holding its default, and the generated encoder does the same. A
/// hand-written field that emitted the zero would still decode, but it would no longer produce
/// the same bytes as the message it replaces -- which is the property the tests check.
pub(crate) fn varint_field_len(tag: u32, value: u64) -> usize {
    if value == 0 {
        return 0;
    }
    prost::encoding::key_len(tag) + prost::encoding::encoded_len_varint(value)
}

pub(crate) fn put_varint_field(tag: u32, value: u64, out: &mut Vec<u8>) {
    if value == 0 {
        return;
    }
    prost::encoding::encode_key(tag, prost::encoding::WireType::Varint, out);
    prost::encoding::encode_varint(value, out);
}

/// Write a staged page as field `tag`, its bytes borrowed straight into `out`.
pub(crate) fn put_staged_block(tag: u32, page: &crate::wal::StagedPage, out: &mut Vec<u8>) {
    let body = varint_field_len(1, page.object_id)
        + if page.bytes.is_empty() {
            0
        } else {
            len_delimited_len(2, page.bytes.len())
        };
    prost::encoding::encode_key(tag, prost::encoding::WireType::LengthDelimited, out);
    prost::encoding::encode_varint(body as u64, out);
    put_varint_field(1, page.object_id, out);
    put_len_delimited(2, &page.bytes, out);
}

fn put_len_delimited(tag: u32, payload: &[u8], out: &mut Vec<u8>) {
    if payload.is_empty() {
        return;
    }
    prost::encoding::encode_key(tag, prost::encoding::WireType::LengthDelimited, out);
    prost::encoding::encode_varint(payload.len() as u64, out);
    out.extend_from_slice(payload);
}

fn len_delimited_len_or_none(tag: u32, payload_len: usize) -> usize {
    if payload_len == 0 {
        return 0;
    }
    len_delimited_len(tag, payload_len)
}

impl CommandEncoding<'_> {
    /// Bytes of the `WalCommand` body -- the single oneof field, with no outer length.
    fn body_len(&self) -> usize {
        match self {
            Self::StringSet { key, value, ttl_ms } => {
                let inner = len_delimited_len_or_none(1, key.len())
                    + len_delimited_len_or_none(2, value.len())
                    + varint_field_len(3, *ttl_ms);
                len_delimited_len(1, inner)
            }
            Self::HashSet { key, field, value } => {
                let inner = len_delimited_len_or_none(1, key.len())
                    + len_delimited_len_or_none(2, field.len())
                    + len_delimited_len_or_none(3, value.len());
                len_delimited_len(2, inner)
            }
            Self::Owned(command) => prost::Message::encoded_len(command),
        }
    }

    /// Bytes this command occupies as field `tag` of the record that carries it.
    pub(crate) fn encoded_len_at(&self, tag: u32) -> usize {
        len_delimited_len(tag, self.body_len())
    }

    fn put_body(&self, out: &mut Vec<u8>) {
        match self {
            Self::StringSet { key, value, ttl_ms } => {
                let inner = len_delimited_len_or_none(1, key.len())
                    + len_delimited_len_or_none(2, value.len())
                    + varint_field_len(3, *ttl_ms);
                prost::encoding::encode_key(1, prost::encoding::WireType::LengthDelimited, out);
                prost::encoding::encode_varint(inner as u64, out);
                put_len_delimited(1, key.as_bytes(), out);
                put_len_delimited(2, value, out);
                put_varint_field(3, *ttl_ms, out);
            }
            Self::HashSet { key, field, value } => {
                let inner = len_delimited_len_or_none(1, key.len())
                    + len_delimited_len_or_none(2, field.len())
                    + len_delimited_len_or_none(3, value.len());
                prost::encoding::encode_key(2, prost::encoding::WireType::LengthDelimited, out);
                prost::encoding::encode_varint(inner as u64, out);
                put_len_delimited(1, key.as_bytes(), out);
                put_len_delimited(2, field.as_bytes(), out);
                put_len_delimited(3, value, out);
            }
            Self::Owned(command) => {
                prost::Message::encode_raw(command, out);
            }
        }
    }

    /// Write this command as field `tag`, payload borrowed straight into `out`.
    pub(crate) fn put_at(&self, tag: u32, out: &mut Vec<u8>) {
        prost::encoding::encode_key(tag, prost::encoding::WireType::LengthDelimited, out);
        prost::encoding::encode_varint(self.body_len() as u64, out);
        self.put_body(out);
    }
}

pub(crate) fn command_from_proto(kind: v1::wal_command::Kind) -> io::Result<Command> {
    Ok(match kind {
        v1::wal_command::Kind::StringSet(command) => {
            if command.ttl_ms > 0 {
                Command::StringSetEx {
                    key: command.key,
                    value: command.value,
                    ttl_ms: command.ttl_ms,
                }
            } else {
                Command::StringSet {
                    key: command.key,
                    value: command.value,
                }
            }
        }
        v1::wal_command::Kind::HashSet(command) => Command::HashSet {
            key: command.key,
            field: command.field,
            value: command.value,
        },
        v1::wal_command::Kind::Verbatim(bytes) => {
            serde_json::from_slice(&bytes).map_err(io::Error::other)?
        }
    })
}

fn entry_to_proto(entry: &RaftLogEntry) -> io::Result<v1::WalLogEntry> {
    Ok(v1::WalLogEntry {
        term: entry.term,
        index: entry.index,
        shard_id: entry.shard_id,
        command: Some(v1::WalCommand {
            kind: Some(command_to_proto(&entry.command)?),
        }),
    })
}

fn entry_from_proto(entry: v1::WalLogEntry) -> io::Result<RaftLogEntry> {
    let kind = entry
        .command
        .and_then(|command| command.kind)
        .ok_or_else(|| io::Error::other("wal log entry is missing its command"))?;
    Ok(RaftLogEntry {
        term: entry.term,
        index: entry.index,
        shard_id: entry.shard_id,
        command: command_from_proto(kind)?,
    })
}

/// Introduces a length-prefixed binary record. Chosen so it cannot be confused with either text
/// form: a record written before this change starts with `{`, and one wrapped in the integrity
/// envelope starts with `#`.
pub(super) const BINARY_RECORD_MAGIC: u8 = 0xA7;

/// Whether new records are written in the binary encoding. Off unless asked for: reading it is
/// safe from the moment the code lands, but writing it is a one-way step for any reader that
/// predates this, so the switch is deliberate.
pub(super) fn binary_records_enabled() -> bool {
    // Default ON since the encoding proved out on real hardware; the env var now
    // opts OUT. Old records read back fine: reads dispatch on the first byte.
    std::env::var("TS_RAFT_WAL_BINARY_RECORDS")
        .map(|value| !(value == "0" || value.eq_ignore_ascii_case("false")))
        .unwrap_or(true)
}

/// Encode one record as it should appear in a segment file.
pub(super) fn encode_record_line(
    envelope: &RaftWalEnvelope,
    binary: bool,
) -> io::Result<Vec<u8>> {
    if !binary {
        let mut out = serde_json::to_vec(envelope).map_err(io::Error::other)?;
        out.push(b'\n');
        return Ok(out);
    }
    let payload = encode_envelope(envelope)?;
    let mut out = Vec::with_capacity(payload.len() + 5);
    out.push(BINARY_RECORD_MAGIC);
    out.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    out.extend_from_slice(&payload);
    Ok(out)
}

/// Read the next record, in whichever encoding it was written.
///
/// Returns how many bytes it occupied along with the record. `None` means what follows is not a
/// complete, decodable record -- a torn tail from a crash mid-append -- and the caller truncates
/// there, exactly as it did when every record was text.
pub(super) fn next_envelope(bytes: &[u8]) -> Option<(usize, RaftWalEnvelope)> {
    match bytes.first()? {
        &BINARY_RECORD_MAGIC => {
            let length_end = 5usize;
            if bytes.len() < length_end {
                return None;
            }
            let length =
                u32::from_le_bytes(bytes[1..length_end].try_into().ok()?) as usize;
            let end = length_end.checked_add(length)?;
            // A length running past the end of what was written is a torn tail, not a record.
            if bytes.len() < end {
                return None;
            }
            Some((end, decode_envelope(&bytes[length_end..end]).ok()?))
        }
        _ => {
            let line_len = bytes
                .iter()
                .position(|byte| *byte == b'\n')
                .map(|position| position + 1)
                .unwrap_or(bytes.len());
            let raw_line = &bytes[..line_len];
            let line = raw_line.strip_suffix(b"\n").unwrap_or(raw_line);
            Some((
                line_len,
                serde_json::from_slice::<RaftWalEnvelope>(line).ok()?,
            ))
        }
    }
}

/// Encode a replicated batch for the wire, behind the same magic byte the log uses.
///
/// A receiver tells the two encodings apart by the first byte: a text body always starts with
/// `{`, so a body starting with the magic can only be binary.
pub(super) fn encode_append_entries(
    request: &AppendEntriesRequest,
) -> io::Result<Vec<u8>> {
    let message = v1::RaftAppendEntriesRequest {
        shard_id: request.shard_id,
        term: request.term,
        leader_id: request.leader_id,
        target_id: request.target_id,
        prev_log_index: request.prev_log_index,
        prev_log_term: request.prev_log_term,
        entries: request
            .entries
            .iter()
            .map(entry_to_proto)
            .collect::<io::Result<Vec<_>>>()?,
        leader_commit: request.leader_commit,
        rpc: match &request.rpc {
            Some(rpc) => serde_json::to_vec(rpc).map_err(io::Error::other)?,
            None => Vec::new(),
        },
    };
    let mut out = Vec::with_capacity(message.encoded_len() + 1);
    out.push(BINARY_RECORD_MAGIC);
    message.encode(&mut out).map_err(io::Error::other)?;
    Ok(out)
}

/// Whether a body is the binary encoding rather than text.
pub(crate) fn is_binary_rpc(body: &[u8]) -> bool {
    body.first() == Some(&BINARY_RECORD_MAGIC)
}

/// Decode a replicated batch that arrived in the binary encoding.
pub(crate) fn decode_append_entries(body: &[u8]) -> io::Result<AppendEntriesRequest> {
    let message = v1::RaftAppendEntriesRequest::decode(&body[1..]).map_err(io::Error::other)?;
    Ok(AppendEntriesRequest {
        rpc: if message.rpc.is_empty() {
            None
        } else {
            serde_json::from_slice(&message.rpc).map_err(io::Error::other)?
        },
        shard_id: message.shard_id,
        term: message.term,
        leader_id: message.leader_id,
        target_id: message.target_id,
        prev_log_index: message.prev_log_index,
        prev_log_term: message.prev_log_term,
        entries: message
            .entries
            .into_iter()
            .map(entry_from_proto)
            .collect::<io::Result<Vec<_>>>()?,
        leader_commit: message.leader_commit,
    })
}

/// Encode one envelope as a binary message.
pub(super) fn encode_envelope(envelope: &RaftWalEnvelope) -> io::Result<Vec<u8>> {
    let record = &envelope.record;
    // The snapshot gets first-class fields: its state image is slab and index BYTES, and in
    // the side blob those become text again -- re-inflating exactly what compaction bounds.
    let installed_snapshot = match &record.installed_snapshot {
        None => None,
        Some(snapshot) => {
            let mut sans_image = snapshot.clone();
            let image = sans_image.state_image.take();
            Some(v1::WalInstalledSnapshot {
                snapshot_sans_image: serde_json::to_vec(&sans_image)
                    .map_err(io::Error::other)?,
                state_image: image.map(|image| v1::WalStateImage {
                    index: image.index_bytes,
                    next_page_id: image.next_page_id,
                    slabs: image
                        .slabs
                        .into_iter()
                        .map(|slab| v1::WalStateImageSlab {
                            page_slab_id: slab.page_slab_id,
                            slab: slab.bytes,
                        })
                        .collect(),
                }),
            })
        }
    };
    let rest = RaftWalRecordRest {
        replica_role: record.replica_role,
        joint_membership: record.joint_membership.clone(),
        latest_external_snapshot_ref: record.latest_external_snapshot_ref.clone(),
        installed_snapshot: None,
        pipeline_state: record.pipeline_state.clone(),
        read_safety_state: record.read_safety_state.clone(),
        membership_evidence: record.membership_evidence.clone(),
    };
    // Omit the block entirely when every field in it is at its default, which is the common case.
    let rest_bytes = if rest.is_empty() {
        Vec::new()
    } else {
        serde_json::to_vec(&rest).map_err(io::Error::other)?
    };

    let (fence_checksum, fence_checksum_is_text) =
        checksum_to_bytes(&record.storage_apply_fence.checksum);
    let (checksum, checksum_is_text) = checksum_to_bytes(&envelope.checksum);

    let message = v1::WalEnvelope {
        sequence: envelope.sequence,
        checksum,
        checksum_is_text,
        record: Some(v1::WalRecord {
            hard_state: Some(v1::WalHardState {
                current_term: record.hard_state.current_term,
                voted_for: record.hard_state.voted_for,
                commit_index: record.hard_state.commit_index,
            }),
            membership: Some(v1::WalMembership {
                shard_id: record.membership.shard_id,
                voters: record.membership.voters.clone(),
                leader_id: record.membership.leader_id,
            }),
            apply_snapshot_fence: Some(v1::WalApplySnapshotFence {
                applied_index: record.apply_snapshot_fence.applied_index,
                commit_index: record.apply_snapshot_fence.commit_index,
                installed_snapshot_index: record.apply_snapshot_fence.installed_snapshot_index,
                first_retained_log_index: record.apply_snapshot_fence.first_retained_log_index,
            }),
            storage_apply_fence: Some(v1::WalStorageApplyFence {
                shard_id: record.storage_apply_fence.shard_id,
                raft_term: record.storage_apply_fence.raft_term,
                committed_index: record.storage_apply_fence.committed_index,
                applied_index: record.storage_apply_fence.applied_index,
                snapshot_id: record.storage_apply_fence.snapshot_id.clone(),
                storage_epoch: record.storage_apply_fence.storage_epoch,
                checksum: fence_checksum,
                checksum_is_text: fence_checksum_is_text,
            }),
            entries: record
                .entries
                .iter()
                .map(entry_to_proto)
                .collect::<io::Result<Vec<_>>>()?,
            installed_snapshot,
            rest: rest_bytes,
        }),
        delta: envelope.delta.as_ref().map(|delta| v1::WalEntryDelta {
            from_index: delta.from_index,
            log_first_index: delta.log_first_index,
            log_last_index: delta.log_last_index,
        }),
    };
    Ok(message.encode_to_vec())
}

/// Decode one binary message back into an envelope.
pub(super) fn decode_envelope(bytes: &[u8]) -> io::Result<RaftWalEnvelope> {
    let message = v1::WalEnvelope::decode(bytes).map_err(io::Error::other)?;
    let record = message
        .record
        .ok_or_else(|| io::Error::other("wal envelope is missing its record"))?;
    let hard_state = record.hard_state.unwrap_or_default();
    let membership = record.membership.unwrap_or_default();
    let apply_snapshot_fence = record.apply_snapshot_fence.unwrap_or_default();
    let storage_apply_fence = record.storage_apply_fence.unwrap_or_default();
    let rest: RaftWalRecordRest = if record.rest.is_empty() {
        RaftWalRecordRest::default()
    } else {
        serde_json::from_slice(&record.rest).map_err(io::Error::other)?
    };

    let installed_snapshot = match record.installed_snapshot {
        Some(snapshot) => {
            let mut decoded: RaftSnapshot =
                serde_json::from_slice(&snapshot.snapshot_sans_image).map_err(io::Error::other)?;
            decoded.state_image = snapshot.state_image.map(|image| RaftSnapshotStateImage {
                index_bytes: image.index,
                next_page_id: image.next_page_id,
                slabs: image
                    .slabs
                    .into_iter()
                    .map(|slab| RaftSnapshotStateImageSlab {
                        page_slab_id: slab.page_slab_id,
                        bytes: slab.slab,
                    })
                    .collect(),
            });
            Some(decoded)
        }
        // Records written before the native field carried the snapshot in the side blob.
        None => rest.installed_snapshot,
    };
    Ok(RaftWalEnvelope {
        sequence: message.sequence,
        checksum: checksum_from_bytes(&message.checksum, message.checksum_is_text),
        record: RaftWalRecord {
            hard_state: RaftHardState {
                current_term: hard_state.current_term,
                voted_for: hard_state.voted_for,
                commit_index: hard_state.commit_index,
            },
            membership: RaftMembership {
                shard_id: membership.shard_id,
                voters: membership.voters,
                leader_id: membership.leader_id,
            },
            replica_role: rest.replica_role,
            joint_membership: rest.joint_membership,
            latest_external_snapshot_ref: rest.latest_external_snapshot_ref,
            installed_snapshot,
            apply_snapshot_fence: RaftApplySnapshotFence {
                applied_index: apply_snapshot_fence.applied_index,
                commit_index: apply_snapshot_fence.commit_index,
                installed_snapshot_index: apply_snapshot_fence.installed_snapshot_index,
                first_retained_log_index: apply_snapshot_fence.first_retained_log_index,
            },
            storage_apply_fence: RaftStorageApplyFence {
                shard_id: storage_apply_fence.shard_id,
                raft_term: storage_apply_fence.raft_term,
                committed_index: storage_apply_fence.committed_index,
                applied_index: storage_apply_fence.applied_index,
                snapshot_id: storage_apply_fence.snapshot_id,
                storage_epoch: storage_apply_fence.storage_epoch,
                checksum: checksum_from_bytes(
                    &storage_apply_fence.checksum,
                    storage_apply_fence.checksum_is_text,
                ),
            },
            pipeline_state: rest.pipeline_state,
            read_safety_state: rest.read_safety_state,
            membership_evidence: rest.membership_evidence,
            entries: record
                .entries
                .into_iter()
                .map(entry_from_proto)
                .collect::<io::Result<Vec<_>>>()?,
        },
        delta: message.delta.map(|delta| RaftWalEntryDelta {
            from_index: delta.from_index,
            log_first_index: delta.log_first_index,
            log_last_index: delta.log_last_index,
        }),
    })
}


#[cfg(test)]
mod tests {
    use super::*;

    fn envelope_with(command: Command) -> RaftWalEnvelope {
        RaftWalEnvelope {
            sequence: 7,
            checksum: "9e0126c499690a34b92dc5cac030ea79a3b75a6d92cd9636afc7bfa457b34a01".into(),
            record: RaftWalRecord {
                hard_state: RaftHardState {
                    current_term: 3,
                    voted_for: Some(2),
                    commit_index: 41,
                },
                membership: RaftMembership {
                    shard_id: 1,
                    voters: vec![1, 2, 3],
                    leader_id: 1,
                },
                storage_apply_fence: RaftStorageApplyFence {
                    shard_id: 1,
                    raft_term: 3,
                    committed_index: 41,
                    applied_index: 41,
                    snapshot_id: None,
                    storage_epoch: 41,
                    checksum: "5d694524421e20e2c7be7c646b30d7a13b4266dfffd1231e2b4e10b3bbd4a6f7"
                        .into(),
                },
                entries: vec![RaftLogEntry {
                    term: 3,
                    index: 42,
                    shard_id: 1,
                    command,
                }],
                replica_role: RaftReplicaRole::default(),
                joint_membership: None,
                latest_external_snapshot_ref: None,
                installed_snapshot: None,
                apply_snapshot_fence: RaftApplySnapshotFence::default(),
                pipeline_state: RaftPeerPipelineRuntimeState::default(),
                read_safety_state: RaftReadSafetyRuntimeState::default(),
                membership_evidence: RaftMembershipRuntimeEvidence::default(),
            },
            delta: Some(RaftWalEntryDelta {
                from_index: 41,
                log_first_index: 1,
                log_last_index: 42,
            }),
        }
    }

    fn assert_round_trips(command: Command) {
        let envelope = envelope_with(command);
        let encoded = encode_envelope(&envelope).expect("encode");
        let decoded = decode_envelope(&encoded).expect("decode");
        assert_eq!(
            decoded, envelope,
            "a record must decode back to exactly what was encoded"
        );
    }

    #[test]
    fn a_modelled_command_round_trips() {
        assert_round_trips(Command::StringSet {
            key: "k".into(),
            value: b"hello".to_vec(),
        });
        assert_round_trips(Command::HashSet {
            key: "k".into(),
            field: "f".into(),
            value: b"hello".to_vec(),
        });
        assert_round_trips(Command::StringSetEx {
            key: "k".into(),
            value: b"hello".to_vec(),
            ttl_ms: 5_000,
        });
    }

    /// The modelled form tells `StringSet` and `StringSetEx` apart by whether the TTL is
    /// non-zero, so a zero-TTL `StringSetEx` cannot use it -- it would come back as a plain
    /// `StringSet`, quietly dropping the fact that an expiry was set at all. It must take the
    /// verbatim path instead.
    #[test]
    fn a_zero_ttl_expiring_set_does_not_decay_into_a_plain_set() {
        let command = Command::StringSetEx {
            key: "k".into(),
            value: b"hello".to_vec(),
            ttl_ms: 0,
        };
        assert_round_trips(command.clone());
        let encoded = encode_envelope(&envelope_with(command)).expect("encode");
        let decoded = decode_envelope(&encoded).expect("decode");
        assert!(
            matches!(
                decoded.record.entries[0].command,
                Command::StringSetEx { ttl_ms: 0, .. }
            ),
            "it must still be an expiring set after a round trip"
        );
    }

    /// A command this module does not model must still round-trip exactly, because the log has to
    /// replay commands the encoding has never been taught about.
    #[test]
    fn an_unmodelled_command_round_trips_verbatim() {
        assert_round_trips(Command::StringDelete { key: "k".into() });
        assert_round_trips(Command::SetAdd {
            key: "k".into(),
            member: b"a".to_vec(),
        });
    }

    /// Payload bytes must survive whatever they contain -- including bytes a text encoding would
    /// have to escape, and bytes that are not valid UTF-8 at all.
    #[test]
    fn arbitrary_payload_bytes_survive() {
        assert_round_trips(Command::StringSet {
            key: "k".into(),
            value: (0u8..=255).collect(),
        });
        assert_round_trips(Command::StringSet {
            key: "quote\"newline\n".into(),
            value: vec![0, 0x1b, 0xff, 0xfe],
        });
    }

    /// A checksum is a hex digest in practice and is stored as one, but the encoding must not
    /// assume that: anything else is kept as its own bytes and returned unchanged.
    #[test]
    fn a_checksum_that_is_not_a_hex_digest_is_returned_unchanged() {
        let mut envelope = envelope_with(Command::StringSet {
            key: "k".into(),
            value: b"v".to_vec(),
        });
        envelope.checksum = "not-a-digest".into();
        envelope.record.storage_apply_fence.checksum = "also/not/hex".into();
        let encoded = encode_envelope(&envelope).expect("encode");
        assert_eq!(decode_envelope(&encoded).expect("decode"), envelope);
    }

    /// A hex digest must actually be stored as its digest rather than its text, since that is
    /// where the saving comes from.
    #[test]
    fn a_hex_digest_is_stored_as_bytes_not_text() {
        let digest = "9e0126c499690a34b92dc5cac030ea79a3b75a6d92cd9636afc7bfa457b34a01";
        let (bytes, is_text) = checksum_to_bytes(digest);
        assert!(!is_text);
        assert_eq!(bytes.len(), 32, "64 hex characters are 32 bytes");
        assert_eq!(checksum_from_bytes(&bytes, is_text), digest);
    }

    /// The record's rarely-set fields travel whole, so a record that does set them still comes
    /// back intact.
    #[test]
    fn the_rarely_set_record_fields_survive() {
        let mut envelope = envelope_with(Command::StringSet {
            key: "k".into(),
            value: b"v".to_vec(),
        });
        envelope.record.membership_evidence.learner_add_count = 4;
        envelope.record.apply_snapshot_fence.applied_index = 9;
        let encoded = encode_envelope(&envelope).expect("encode");
        assert_eq!(decode_envelope(&encoded).expect("decode"), envelope);
    }

    /// The point of the exercise: the same record, both ways.
    #[test]
    fn encodes_far_smaller_than_the_text_form() {
        for payload in [10usize, 1024] {
            let envelope = envelope_with(Command::StringSet {
                key: "fmt-001999".into(),
                value: vec![0x41; payload],
            });
            let text = serde_json::to_vec(&envelope).expect("json");
            let binary = encode_envelope(&envelope).expect("proto");
            println!(
                "payload {payload:>5}B -> text {:>6}B, binary {:>6}B ({:.2}x smaller)",
                text.len(),
                binary.len(),
                text.len() as f64 / binary.len() as f64
            );
            assert!(
                binary.len() < text.len(),
                "the binary form must be smaller ({} vs {})",
                binary.len(),
                text.len()
            );
        }
    }

    /// What one AppendEntries costs on the wire, both ways. Replication sends this to every
    /// follower for every write, so the ratio here is paid once per follower per write.
    #[test]
    fn replication_body_cost() {
        for payload in [10usize, 1024] {
            let entries = vec![RaftLogEntry {
                term: 3,
                index: 42,
                shard_id: 1,
                command: Command::StringSet {
                    key: "k".into(),
                    value: vec![0x41; payload],
                },
            }];
            let request = AppendEntriesRequest {
                rpc: None,
                shard_id: 1,
                term: 3,
                leader_id: 1,
                target_id: 2,
                prev_log_index: 41,
                prev_log_term: 3,
                entries: entries.clone(),
                leader_commit: 41,
            };
            let text = serde_json::to_vec(&request).expect("json");
            // The entries are what scales; the rest of the request is a handful of integers.
            let binary: usize = entries
                .iter()
                .map(|entry| {
                    let proto = entry_to_proto(entry).expect("proto");
                    prost::Message::encoded_len(&proto)
                })
                .sum();
            println!(
                "append_entries payload {payload:>5}B -> json {:>6}B, entries as binary {:>6}B ({:.2}x)",
                text.len(),
                binary,
                text.len() as f64 / binary.max(1) as f64
            );
        }
    }

    /// A replicated batch must arrive as exactly what was sent. Anything lost here is a follower
    /// applying something different from its leader, which is the one thing consensus exists to
    /// prevent.
    #[test]
    fn a_replicated_batch_round_trips() {
        let request = AppendEntriesRequest {
            rpc: None,
            shard_id: 1,
            term: 3,
            leader_id: 1,
            target_id: 2,
            prev_log_index: 41,
            prev_log_term: 3,
            entries: vec![
                RaftLogEntry {
                    term: 3,
                    index: 42,
                    shard_id: 1,
                    command: Command::StringSet {
                        key: "k".into(),
                        // Bytes a text encoding would have to escape, and bytes that are not
                        // valid text at all.
                        value: (0u8..=255).collect(),
                    },
                },
                RaftLogEntry {
                    term: 3,
                    index: 43,
                    shard_id: 1,
                    // An unmodelled command still has to arrive intact.
                    command: Command::StringDelete { key: "gone".into() },
                },
            ],
            leader_commit: 41,
        };
        let encoded = encode_append_entries(&request).expect("encode");
        assert!(is_binary_rpc(&encoded), "a binary body must be recognisable as one");
        assert_eq!(decode_append_entries(&encoded).expect("decode"), request);
    }

    /// A text body must never be mistaken for a binary one -- they share a route.
    #[test]
    fn a_text_body_is_not_mistaken_for_binary() {
        let json = serde_json::to_vec(&AppendEntriesRequest {
            rpc: None,
            shard_id: 1,
            term: 1,
            leader_id: 1,
            target_id: 2,
            prev_log_index: 0,
            prev_log_term: 0,
            entries: Vec::new(),
            leader_commit: 0,
        })
        .unwrap();
        assert!(!is_binary_rpc(&json));
    }
}
