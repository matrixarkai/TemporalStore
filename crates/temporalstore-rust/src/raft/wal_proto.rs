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
/// They are carried together in one side message that is left out of a record that has none of
/// them, which is nearly all of them, rather than modelled on the record itself.
#[derive(Debug, Clone, Default, PartialEq)]
struct RaftWalRecordRest {
    replica_role: RaftReplicaRole,
    joint_membership: Option<JointConsensusMembership>,
    latest_external_snapshot_ref: Option<RaftExternalSnapshotRef>,
    installed_snapshot: Option<RaftSnapshot>,
    pipeline_state: RaftPeerPipelineRuntimeState,
    read_safety_state: RaftReadSafetyRuntimeState,
    membership_evidence: RaftMembershipRuntimeEvidence,
}

impl RaftWalRecordRest {
    fn is_empty(&self) -> bool {
        *self == RaftWalRecordRest::default()
    }
}

fn pipeline_state_to_proto(state: &RaftPeerPipelineRuntimeState) -> v1::WalPipelineState {
    let RaftPeerPipelineRuntimeState {
        match_index,
        next_index,
        append_requests,
        append_accepted,
        append_rejected,
        committed_overwrite_rejections,
        inflight_entries,
        inflight_bytes,
        append_queue_depth,
        last_accepted_append_ms,
        leader_reported_commit_index,
        apply_inflight_tasks,
        apply_queue_depth,
        apply_queue_max_depth,
        apply_backpressure_rejections,
        memory_backpressure_rejections,
        oversized_log_rejections,
        append_queue_max_depth,
        reorder_queue_depth,
        out_of_order_append_rejections,
        reorder_entries_accepted,
        reorder_entries_released,
        reorder_entries_rejected,
        reorder_entry_timeouts,
        reorder_dropped_packages,
        stale_term_rejections,
        append_send_failures,
        snapshot_sending,
        snapshot_installing,
        snapshot_installed_index,
        snapshot_send_attempts,
        snapshot_send_completed,
        snapshot_send_failed,
        snapshot_install_started,
        snapshot_install_completed,
        snapshot_install_rejected,
        snapshot_install_rolled_back,
        snapshot_install_received_chunks,
        snapshot_send_progress_mark,
        snapshot_install_total_chunks,
        snapshot_install_progress_per_mille,
        snapshot_retry_count,
        snapshot_chunk_retry_count,
        snapshot_backpressure_rejections,
        snapshot_rate_limit_rejections,
        snapshot_send_started_ms,
        snapshot_send_elapsed_ms,
        snapshot_send_timeouts,
        snapshot_during_membership_change,
        snapshot_rejoin_after_compacted_log,
        transfer_leader_target,
        transfer_leader_requests,
        transfer_leader_accepted,
        transfer_leader_rejected,
        transfer_leader_completed,
        transfer_leader_started_ms,
        transfer_leader_elapsed_ms,
        transfer_leader_timeouts,
        pre_vote_rejections,
        election_rejections,
        offline_since_ms,
        offline_elapsed_ms,
        offline_timeout_reached,
        offline_timeout_rejections,
        auto_promoted_from_learner,
    } = state;
    v1::WalPipelineState {
        match_index: *match_index,
        next_index: *next_index,
        append_requests: *append_requests,
        append_accepted: *append_accepted,
        append_rejected: *append_rejected,
        committed_overwrite_rejections: *committed_overwrite_rejections,
        inflight_entries: *inflight_entries,
        inflight_bytes: *inflight_bytes,
        append_queue_depth: *append_queue_depth,
        last_accepted_append_ms: *last_accepted_append_ms,
        leader_reported_commit_index: *leader_reported_commit_index,
        apply_inflight_tasks: *apply_inflight_tasks,
        apply_queue_depth: *apply_queue_depth,
        apply_queue_max_depth: *apply_queue_max_depth,
        apply_backpressure_rejections: *apply_backpressure_rejections,
        memory_backpressure_rejections: *memory_backpressure_rejections,
        oversized_log_rejections: *oversized_log_rejections,
        append_queue_max_depth: *append_queue_max_depth,
        reorder_queue_depth: *reorder_queue_depth,
        out_of_order_append_rejections: *out_of_order_append_rejections,
        reorder_entries_accepted: *reorder_entries_accepted,
        reorder_entries_released: *reorder_entries_released,
        reorder_entries_rejected: *reorder_entries_rejected,
        reorder_entry_timeouts: *reorder_entry_timeouts,
        reorder_dropped_packages: *reorder_dropped_packages,
        stale_term_rejections: *stale_term_rejections,
        append_send_failures: *append_send_failures,
        snapshot_sending: *snapshot_sending,
        snapshot_installing: *snapshot_installing,
        snapshot_installed_index: *snapshot_installed_index,
        snapshot_send_attempts: *snapshot_send_attempts,
        snapshot_send_completed: *snapshot_send_completed,
        snapshot_send_failed: *snapshot_send_failed,
        snapshot_install_started: *snapshot_install_started,
        snapshot_install_completed: *snapshot_install_completed,
        snapshot_install_rejected: *snapshot_install_rejected,
        snapshot_install_rolled_back: *snapshot_install_rolled_back,
        snapshot_install_received_chunks: *snapshot_install_received_chunks,
        snapshot_send_progress_mark: *snapshot_send_progress_mark,
        snapshot_install_total_chunks: *snapshot_install_total_chunks,
        snapshot_install_progress_per_mille: *snapshot_install_progress_per_mille,
        snapshot_retry_count: *snapshot_retry_count,
        snapshot_chunk_retry_count: *snapshot_chunk_retry_count,
        snapshot_backpressure_rejections: *snapshot_backpressure_rejections,
        snapshot_rate_limit_rejections: *snapshot_rate_limit_rejections,
        snapshot_send_started_ms: *snapshot_send_started_ms,
        snapshot_send_elapsed_ms: *snapshot_send_elapsed_ms,
        snapshot_send_timeouts: *snapshot_send_timeouts,
        snapshot_during_membership_change: *snapshot_during_membership_change,
        snapshot_rejoin_after_compacted_log: *snapshot_rejoin_after_compacted_log,
        transfer_leader_target: *transfer_leader_target,
        transfer_leader_requests: *transfer_leader_requests,
        transfer_leader_accepted: *transfer_leader_accepted,
        transfer_leader_rejected: *transfer_leader_rejected,
        transfer_leader_completed: *transfer_leader_completed,
        transfer_leader_started_ms: *transfer_leader_started_ms,
        transfer_leader_elapsed_ms: *transfer_leader_elapsed_ms,
        transfer_leader_timeouts: *transfer_leader_timeouts,
        pre_vote_rejections: *pre_vote_rejections,
        election_rejections: *election_rejections,
        offline_since_ms: *offline_since_ms,
        offline_elapsed_ms: *offline_elapsed_ms,
        offline_timeout_reached: *offline_timeout_reached,
        offline_timeout_rejections: *offline_timeout_rejections,
        auto_promoted_from_learner: *auto_promoted_from_learner,
    }
}

fn pipeline_state_from_proto(message: v1::WalPipelineState) -> RaftPeerPipelineRuntimeState {
    RaftPeerPipelineRuntimeState {
        match_index: message.match_index,
        next_index: message.next_index,
        append_requests: message.append_requests,
        append_accepted: message.append_accepted,
        append_rejected: message.append_rejected,
        committed_overwrite_rejections: message.committed_overwrite_rejections,
        inflight_entries: message.inflight_entries,
        inflight_bytes: message.inflight_bytes,
        append_queue_depth: message.append_queue_depth,
        last_accepted_append_ms: message.last_accepted_append_ms,
        leader_reported_commit_index: message.leader_reported_commit_index,
        apply_inflight_tasks: message.apply_inflight_tasks,
        apply_queue_depth: message.apply_queue_depth,
        apply_queue_max_depth: message.apply_queue_max_depth,
        apply_backpressure_rejections: message.apply_backpressure_rejections,
        memory_backpressure_rejections: message.memory_backpressure_rejections,
        oversized_log_rejections: message.oversized_log_rejections,
        append_queue_max_depth: message.append_queue_max_depth,
        reorder_queue_depth: message.reorder_queue_depth,
        out_of_order_append_rejections: message.out_of_order_append_rejections,
        reorder_entries_accepted: message.reorder_entries_accepted,
        reorder_entries_released: message.reorder_entries_released,
        reorder_entries_rejected: message.reorder_entries_rejected,
        reorder_entry_timeouts: message.reorder_entry_timeouts,
        reorder_dropped_packages: message.reorder_dropped_packages,
        stale_term_rejections: message.stale_term_rejections,
        append_send_failures: message.append_send_failures,
        snapshot_sending: message.snapshot_sending,
        snapshot_installing: message.snapshot_installing,
        snapshot_installed_index: message.snapshot_installed_index,
        snapshot_send_attempts: message.snapshot_send_attempts,
        snapshot_send_completed: message.snapshot_send_completed,
        snapshot_send_failed: message.snapshot_send_failed,
        snapshot_install_started: message.snapshot_install_started,
        snapshot_install_completed: message.snapshot_install_completed,
        snapshot_install_rejected: message.snapshot_install_rejected,
        snapshot_install_rolled_back: message.snapshot_install_rolled_back,
        snapshot_install_received_chunks: message.snapshot_install_received_chunks,
        snapshot_send_progress_mark: message.snapshot_send_progress_mark,
        snapshot_install_total_chunks: message.snapshot_install_total_chunks,
        snapshot_install_progress_per_mille: message.snapshot_install_progress_per_mille,
        snapshot_retry_count: message.snapshot_retry_count,
        snapshot_chunk_retry_count: message.snapshot_chunk_retry_count,
        snapshot_backpressure_rejections: message.snapshot_backpressure_rejections,
        snapshot_rate_limit_rejections: message.snapshot_rate_limit_rejections,
        snapshot_send_started_ms: message.snapshot_send_started_ms,
        snapshot_send_elapsed_ms: message.snapshot_send_elapsed_ms,
        snapshot_send_timeouts: message.snapshot_send_timeouts,
        snapshot_during_membership_change: message.snapshot_during_membership_change,
        snapshot_rejoin_after_compacted_log: message.snapshot_rejoin_after_compacted_log,
        transfer_leader_target: message.transfer_leader_target,
        transfer_leader_requests: message.transfer_leader_requests,
        transfer_leader_accepted: message.transfer_leader_accepted,
        transfer_leader_rejected: message.transfer_leader_rejected,
        transfer_leader_completed: message.transfer_leader_completed,
        transfer_leader_started_ms: message.transfer_leader_started_ms,
        transfer_leader_elapsed_ms: message.transfer_leader_elapsed_ms,
        transfer_leader_timeouts: message.transfer_leader_timeouts,
        pre_vote_rejections: message.pre_vote_rejections,
        election_rejections: message.election_rejections,
        offline_since_ms: message.offline_since_ms,
        offline_elapsed_ms: message.offline_elapsed_ms,
        offline_timeout_reached: message.offline_timeout_reached,
        offline_timeout_rejections: message.offline_timeout_rejections,
        auto_promoted_from_learner: message.auto_promoted_from_learner,
    }
}

fn read_safety_state_to_proto(state: &RaftReadSafetyRuntimeState) -> v1::WalReadSafetyState {
    let RaftReadSafetyRuntimeState {
        read_index_requests,
        read_index_accepted,
        read_index_rejected,
        lease_read_requests,
        lease_read_accepted,
        lease_read_rejected,
        pre_vote_requests,
        pre_vote_accepted,
        pre_vote_rejected,
        bounded_stale_read_requests,
        bounded_stale_read_accepted,
        bounded_stale_read_rejected,
        stale_leader_lease_rejected,
        lagging_follower_read_rejected,
        stale_follower_write_rejected,
        minority_partition_read_rejected,
        minority_partition_write_rejected,
        healed_follower_catchup_observed,
    } = state;
    v1::WalReadSafetyState {
        read_index_requests: *read_index_requests,
        read_index_accepted: *read_index_accepted,
        read_index_rejected: *read_index_rejected,
        lease_read_requests: *lease_read_requests,
        lease_read_accepted: *lease_read_accepted,
        lease_read_rejected: *lease_read_rejected,
        pre_vote_requests: *pre_vote_requests,
        pre_vote_accepted: *pre_vote_accepted,
        pre_vote_rejected: *pre_vote_rejected,
        bounded_stale_read_requests: *bounded_stale_read_requests,
        bounded_stale_read_accepted: *bounded_stale_read_accepted,
        bounded_stale_read_rejected: *bounded_stale_read_rejected,
        stale_leader_lease_rejected: *stale_leader_lease_rejected,
        lagging_follower_read_rejected: *lagging_follower_read_rejected,
        stale_follower_write_rejected: *stale_follower_write_rejected,
        minority_partition_read_rejected: *minority_partition_read_rejected,
        minority_partition_write_rejected: *minority_partition_write_rejected,
        healed_follower_catchup_observed: *healed_follower_catchup_observed,
    }
}

fn read_safety_state_from_proto(message: v1::WalReadSafetyState) -> RaftReadSafetyRuntimeState {
    RaftReadSafetyRuntimeState {
        read_index_requests: message.read_index_requests,
        read_index_accepted: message.read_index_accepted,
        read_index_rejected: message.read_index_rejected,
        lease_read_requests: message.lease_read_requests,
        lease_read_accepted: message.lease_read_accepted,
        lease_read_rejected: message.lease_read_rejected,
        pre_vote_requests: message.pre_vote_requests,
        pre_vote_accepted: message.pre_vote_accepted,
        pre_vote_rejected: message.pre_vote_rejected,
        bounded_stale_read_requests: message.bounded_stale_read_requests,
        bounded_stale_read_accepted: message.bounded_stale_read_accepted,
        bounded_stale_read_rejected: message.bounded_stale_read_rejected,
        stale_leader_lease_rejected: message.stale_leader_lease_rejected,
        lagging_follower_read_rejected: message.lagging_follower_read_rejected,
        stale_follower_write_rejected: message.stale_follower_write_rejected,
        minority_partition_read_rejected: message.minority_partition_read_rejected,
        minority_partition_write_rejected: message.minority_partition_write_rejected,
        healed_follower_catchup_observed: message.healed_follower_catchup_observed,
    }
}

fn membership_evidence_to_proto(state: &RaftMembershipRuntimeEvidence) -> v1::WalMembershipEvidence {
    let RaftMembershipRuntimeEvidence {
        learner_add_count,
        learner_catchup_count,
        learner_promote_count,
        voter_remove_count,
        witness_add_count,
        auto_promote_count,
        leader_transfer_write_count,
        leader_transfer_exact_once_commit_count,
        leader_transfer_exact_once_commit_ids,
        pending_joint_consensus_persist_count,
        pending_joint_consensus_restore_count,
    } = state;
    v1::WalMembershipEvidence {
        learner_add_count: *learner_add_count,
        learner_catchup_count: *learner_catchup_count,
        learner_promote_count: *learner_promote_count,
        voter_remove_count: *voter_remove_count,
        witness_add_count: *witness_add_count,
        auto_promote_count: *auto_promote_count,
        leader_transfer_write_count: *leader_transfer_write_count,
        leader_transfer_exact_once_commit_count: *leader_transfer_exact_once_commit_count,
        leader_transfer_exact_once_commit_ids: leader_transfer_exact_once_commit_ids.clone(),
        pending_joint_consensus_persist_count: *pending_joint_consensus_persist_count,
        pending_joint_consensus_restore_count: *pending_joint_consensus_restore_count,
    }
}

fn membership_evidence_from_proto(message: v1::WalMembershipEvidence) -> RaftMembershipRuntimeEvidence {
    RaftMembershipRuntimeEvidence {
        learner_add_count: message.learner_add_count,
        learner_catchup_count: message.learner_catchup_count,
        learner_promote_count: message.learner_promote_count,
        voter_remove_count: message.voter_remove_count,
        witness_add_count: message.witness_add_count,
        auto_promote_count: message.auto_promote_count,
        leader_transfer_write_count: message.leader_transfer_write_count,
        leader_transfer_exact_once_commit_count: message.leader_transfer_exact_once_commit_count,
        leader_transfer_exact_once_commit_ids: message.leader_transfer_exact_once_commit_ids,
        pending_joint_consensus_persist_count: message.pending_joint_consensus_persist_count,
        pending_joint_consensus_restore_count: message.pending_joint_consensus_restore_count,
    }
}

fn joint_membership_to_proto(state: &JointConsensusMembership) -> v1::WalJointMembership {
    let JointConsensusMembership {
        old_voters,
        new_voters,
    } = state;
    v1::WalJointMembership {
        old_voters: old_voters.clone(),
        new_voters: new_voters.clone(),
    }
}

fn joint_membership_from_proto(message: v1::WalJointMembership) -> JointConsensusMembership {
    JointConsensusMembership {
        old_voters: message.old_voters,
        new_voters: message.new_voters,
    }
}

fn external_snapshot_ref_to_proto(state: &RaftExternalSnapshotRef) -> v1::WalExternalSnapshotRef {
    let RaftExternalSnapshotRef {
        uri,
        checksum,
        byte_size,
    } = state;
    v1::WalExternalSnapshotRef {
        uri: uri.clone(),
        checksum: checksum.clone(),
        byte_size: *byte_size,
    }
}

fn external_snapshot_ref_from_proto(message: v1::WalExternalSnapshotRef) -> RaftExternalSnapshotRef {
    RaftExternalSnapshotRef {
        uri: message.uri,
        checksum: message.checksum,
        byte_size: message.byte_size,
    }
}

fn rest_to_proto(rest: &RaftWalRecordRest) -> v1::WalRecordRest {
    v1::WalRecordRest {
        replica_role: match rest.replica_role {
            RaftReplicaRole::Voter => 0,
            RaftReplicaRole::Learner => 1,
            RaftReplicaRole::Witness => 2,
        },
        joint_membership: rest.joint_membership.as_ref().map(joint_membership_to_proto),
        latest_external_snapshot_ref: rest
            .latest_external_snapshot_ref
            .as_ref()
            .map(external_snapshot_ref_to_proto),
        pipeline_state: (!rest.pipeline_state.is_default())
            .then(|| pipeline_state_to_proto(&rest.pipeline_state)),
        read_safety_state: (!rest.read_safety_state.is_default())
            .then(|| read_safety_state_to_proto(&rest.read_safety_state)),
        membership_evidence: (!rest.membership_evidence.is_default())
            .then(|| membership_evidence_to_proto(&rest.membership_evidence)),
    }
}

fn rest_from_proto(message: v1::WalRecordRest) -> io::Result<RaftWalRecordRest> {
    let replica_role = match message.replica_role {
        0 => RaftReplicaRole::Voter,
        1 => RaftReplicaRole::Learner,
        2 => RaftReplicaRole::Witness,
        other => {
            return Err(io::Error::other(format!(
                "wal record names replica role {other}, which this build does not know"
            )))
        }
    };
    Ok(RaftWalRecordRest {
        replica_role,
        joint_membership: message.joint_membership.map(joint_membership_from_proto),
        latest_external_snapshot_ref: message
            .latest_external_snapshot_ref
            .map(external_snapshot_ref_from_proto),
        installed_snapshot: None,
        pipeline_state: message
            .pipeline_state
            .map(pipeline_state_from_proto)
            .unwrap_or_default(),
        read_safety_state: message
            .read_safety_state
            .map(read_safety_state_from_proto)
            .unwrap_or_default(),
        membership_evidence: message
            .membership_evidence
            .map(membership_evidence_from_proto)
            .unwrap_or_default(),
    })
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
        CommandEncoding::Owned(command) => command.kind.expect("the owned arm always sets a kind"),
    })
}

/// A command in the shape it will be written in, borrowing its payload instead of owning a copy.
///
/// Building the owned message cost the payload twice on every write: once to fill the message and
/// once to serialise it. At a four-kilobyte value that is four kilobytes spent to describe four
/// kilobytes. The three modelled arms borrow their key and value here, so the write
/// copies them only into the buffer that goes to disk.
///
/// The owned arm still owns its bytes, and that is not an oversight: its protobuf encoding does not
/// exist until it is built, so there is nothing to borrow.
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
            kind: Some(v1::wal_command::Kind::SdkCommand(
                crate::shared_store::command_protobuf_bytes(other)?,
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

/// A field whose value is a hash, and so has no small values to save on.
///
/// A varint is a saving only when the high bits are usually clear. An object id here is a hash:
/// measured over 10,000 records, every one of them needed 9 or 10 varint bytes, where fixed64
/// needs 8. For these fields the varint is a cost, not a saving.
pub(crate) fn fixed64_field_len(tag: u32) -> usize {
    prost::encoding::key_len(tag) + 8
}

pub(crate) fn put_fixed64_field(tag: u32, value: u64, out: &mut Vec<u8>) {
    prost::encoding::encode_key(tag, prost::encoding::WireType::SixtyFourBit, out);
    out.extend_from_slice(&value.to_le_bytes());
}

pub(crate) fn put_varint_field(tag: u32, value: u64, out: &mut Vec<u8>) {
    if value == 0 {
        return;
    }
    prost::encoding::encode_key(tag, prost::encoding::WireType::Varint, out);
    prost::encoding::encode_varint(value, out);
}

/// Write a staged block as field `tag`, its bytes borrowed straight into `out`.
pub(crate) fn put_staged_block(
    tag: u32,
    block: &crate::wal::StagedBlock,
    implied_object_id: Option<u64>,
    out: &mut Vec<u8>,
) {
    let object_id = staged_block_object_id(block, implied_object_id);
    let body = staged_block_body_len(block, implied_object_id);
    prost::encoding::encode_key(tag, prost::encoding::WireType::LengthDelimited, out);
    prost::encoding::encode_varint(body as u64, out);
    if let Some(object_id) = object_id {
        put_fixed64_field(1, object_id, out);
    }
    put_len_delimited(2, &block.bytes, out);
    // Ascending field order, the order prost would have written them, which is the property
    // `the_borrowing_encoder_writes_the_same_bytes` checks. Written unconditionally when present
    // rather than elided against an implied value the way the object id is: there is no second
    // copy of the element name anywhere in the record to imply it from.
    if let Some(element_name) = block.element_name.as_deref() {
        // NOT `put_len_delimited`, which skips an empty payload. This field is `optional` in the
        // schema, so prost writes it for `Some("")` and omits it only for `None` -- and the
        // difference is load-bearing rather than cosmetic: an element name that decoded back as `None`
        // would make an element block indistinguishable from the whole-object block of the same key,
        // which is the exact confusion this field exists to remove.
        prost::encoding::encode_key(4, prost::encoding::WireType::LengthDelimited, out);
        prost::encoding::encode_varint(element_name.len() as u64, out);
        out.extend_from_slice(element_name.as_bytes());
    }
}

/// The object id a staged block has to write, if any.
///
/// A record that carries one block for one outcome already says the object id once, in the
/// outcome. Writing it again in the block costs a second copy of a 64-bit hash on every write.
/// It is omitted only when there is exactly one of each and the two agree, so a reader never
/// has to guess which outcome an unlabelled block belongs to.
pub(crate) fn staged_block_object_id(
    block: &crate::wal::StagedBlock,
    implied_object_id: Option<u64>,
) -> Option<u64> {
    if implied_object_id == Some(block.object_id) {
        return None;
    }
    Some(block.object_id)
}

pub(crate) fn staged_block_body_len(
    block: &crate::wal::StagedBlock,
    implied_object_id: Option<u64>,
) -> usize {
    let id_len = match staged_block_object_id(block, implied_object_id) {
        Some(_) => fixed64_field_len(1),
        None => 0,
    };
    // This length RESERVES the buffer `put_staged_block` then fills, so the two must agree field
    // for field or the record is written into the wrong number of bytes -- which is exactly how
    // they disagreed once before, when this measured a varint after the writer had moved to a
    // fixed field. Every arm here mirrors a line there.
    id_len
        + if block.bytes.is_empty() {
            0
        } else {
            len_delimited_len(2, block.bytes.len())
        }
        + match block.element_name.as_deref() {
            // Counted for `Some("")` as well, because the writer above emits it: an `optional`
            // field is present or absent, not empty or absent.
            Some(element_name) => len_delimited_len(4, element_name.len()),
            None => 0,
        }
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
        v1::wal_command::Kind::SdkCommand(bytes) => {
            crate::shared_store::command_from_protobuf_bytes(&bytes)?
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
        // Carried in BOTH directions here and in `entry_from_proto`. A binary record that dropped
        // it would restore a log whose entries resolve their deadlines against the restarting
        // node's clock, which is the divergence the field exists to close -- and it would do so
        // silently, because every other field round-trips.
        leader_time_ms: entry.leader_time_ms,
    })
}

fn entry_from_proto(entry: v1::WalLogEntry) -> io::Result<RaftLogEntry> {
    let kind = entry
        .command
        .and_then(|command| command.kind)
        .ok_or_else(|| io::Error::other("wal log entry is missing its command"))?;
    Ok(RaftLogEntry {
        leader_time_ms: entry.leader_time_ms,
        term: entry.term,
        index: entry.index,
        shard_id: entry.shard_id,
        command: command_from_proto(kind)?,
    })
}

/// Introduces a length-prefixed binary record.
pub(super) const BINARY_RECORD_MAGIC: u8 = 0xA7;

/// Encode one record as it should appear in a segment file: the magic, a little-endian length and
/// the protobuf envelope. There is no other encoding of a record.
pub(super) fn encode_record_line(envelope: &RaftWalEnvelope) -> io::Result<Vec<u8>> {
    let payload = encode_envelope(envelope)?;
    let mut out = Vec::with_capacity(payload.len() + 5);
    out.push(BINARY_RECORD_MAGIC);
    out.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    out.extend_from_slice(&payload);
    Ok(out)
}

/// Read the next record.
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
        // Anything that does not open with the magic is not a record: a torn tail or another file.
        _ => None,
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
pub(super) fn record_to_proto(record: &RaftWalRecord) -> io::Result<v1::WalRecord> {
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
                    next_block_id: image.next_block_id,
                    slabs: image
                        .slabs
                        .into_iter()
                        .map(|slab| v1::WalStateImageSlab {
                            block_slab_id: slab.block_slab_id,
                            slab: slab.bytes,
                        })
                        .collect(),
                    start_routing_bucket: image
                        .routing_range
                        .map(|range| range.start_routing_bucket),
                    end_routing_bucket: image
                        .routing_range
                        .map(|range| range.end_routing_bucket),
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
    let rest_message = if rest.is_empty() {
        None
    } else {
        Some(rest_to_proto(&rest))
    };

    let (fence_checksum, fence_checksum_is_text) =
        checksum_to_bytes(&record.storage_apply_fence.checksum);

    Ok(v1::WalRecord {
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
            rest: rest_message,
    })
}

/// The bytes a record's checksum covers: the record message exactly as it is written.
pub(super) fn encode_record(record: &RaftWalRecord) -> io::Result<Vec<u8>> {
    Ok(record_to_proto(record)?.encode_to_vec())
}

/// Encode one envelope as a binary message.
pub(super) fn encode_envelope(envelope: &RaftWalEnvelope) -> io::Result<Vec<u8>> {
    let message = v1::WalEnvelope {
        sequence: envelope.sequence,
        checksum: envelope.checksum,
        record: Some(record_to_proto(&envelope.record)?),
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
    let rest = match record.rest {
        Some(rest) => rest_from_proto(rest)?,
        None => RaftWalRecordRest::default(),
    };

    let installed_snapshot = match record.installed_snapshot {
        Some(snapshot) => {
            let mut decoded: RaftSnapshot =
                serde_json::from_slice(&snapshot.snapshot_sans_image).map_err(io::Error::other)?;
            decoded.state_image = snapshot.state_image.map(|image| RaftSnapshotStateImage {
                // Both ends or neither, as on the other decode: half a range is not a range.
                routing_range: match (image.start_routing_bucket, image.end_routing_bucket) {
                    (Some(start_routing_bucket), Some(end_routing_bucket)) => {
                        Some(crate::engine::routing_range_stamp::RoutingRangeStamp {
                            start_routing_bucket,
                            end_routing_bucket,
                        })
                    }
                    _ => None,
                },
                index_bytes: image.index,
                next_block_id: image.next_block_id,
                slabs: image
                    .slabs
                    .into_iter()
                    .map(|slab| RaftSnapshotStateImageSlab {
                        block_slab_id: slab.block_slab_id,
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
        checksum: message.checksum,
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

/// THE RAFT WAL CARRIES EVERY COMMAND AS PROTOBUF.
///
/// Three commands are modelled field by field. Every other one rides the `sdk_command` arm as its
/// SDK protobuf encoding, through the same mapping the shared WAL publishes with, and a command that
/// has no mapping is refused rather than written as JSON. No arm writes or reads JSON.
///
/// `command_encoding` is the only place that decides. The roster below states the three, and the
/// test checks it from both ends: the source text that defines the arms, and the real encoder driven
/// with real commands.
const COMMANDS_THE_RAFT_WAL_MODELS: &[&str] = &["HashSet", "StringSet", "StringSetEx"];

#[test]
fn every_command_the_raft_wal_carries_is_protobuf() {
    // ---------- the source-text half: which commands `command_encoding` names at all.
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    let text = std::fs::read_to_string(root.join("src/raft/wal_proto.rs"))
        .expect("this guard reads src/raft/wal_proto.rs");
    let body = text
        .split_once("pub(crate) fn command_encoding")
        .expect("command_encoding must be in this file")
        .1;
    let encoder = &body[..body.find("\n}").expect("command_encoding must close")];
    let mut modelled: Vec<String> = encoder
        .split("Command::")
        .skip(1)
        .map(|rest| {
            rest.chars()
                .take_while(|c| c.is_alphanumeric() || *c == '_')
                .collect()
        })
        .collect();
    modelled.sort();
    modelled.dedup();

    // DENOMINATOR FIRST. This parses SOURCE TEXT, which silently returns empty once a refactor
    // moves the function -- and an empty parse would pass against an empty roster.
    assert!(
        !modelled.is_empty(),
        "parsed no Command names out of command_encoding, so this guard proved nothing"
    );
    assert_eq!(
        modelled, COMMANDS_THE_RAFT_WAL_MODELS,
        "the commands command_encoding models have changed. Add the new name to \
         COMMANDS_THE_RAFT_WAL_MODELS in the same change that modelled it, or delete the name \
         that is no longer modelled"
    );

    // ---------- no JSON on either side of the codec.
    let uncommented = |text: &str| -> String {
        text.lines()
            .filter(|line| !line.trim_start().starts_with("//"))
            .collect::<Vec<_>>()
            .join("\n")
    };
    let decoder = body
        .split_once("pub(crate) fn command_from_proto")
        .expect("command_from_proto must be in this file")
        .1;
    let decoder = &decoder[..decoder.find("\n}").expect("command_from_proto must close")];
    for (name, code) in [("command_encoding", encoder), ("command_from_proto", decoder)] {
        assert!(code.len() > 100, "{name}: the scan found almost nothing");
        assert!(
            !uncommented(code).contains("serde_json"),
            "{name} reaches for serde_json: a JSON arm survives in the Raft WAL command codec"
        );
    }
    // THE CONTROL: the same scan over a body that does call serde_json must see it.
    assert!(
        uncommented("fn a() { serde_json::to_vec(&x) }").contains("serde_json"),
        "the control no longer sees serde_json, so this scan could pass over a JSON arm"
    );

    // ---------- the behaviour half. A LIST OF NAMES IS NOT A FORMAT: the arm is chosen by
    // ---------- `command_encoding` at run time, so each case goes through the real encoder.
    let carried_by_the_sdk_arm: Vec<(&str, Command)> = vec![
        (
            "SetAdd",
            Command::SetAdd {
                key: "k".into(),
                member: b"m".to_vec(),
            },
        ),
        (
            "StringDelete",
            Command::StringDelete { key: "k".into() },
        ),
    ];
    assert_eq!(
        carried_by_the_sdk_arm.len(),
        2,
        "two cases, or the loop below asserts nothing"
    );
    for (name, command) in carried_by_the_sdk_arm {
        match command_to_proto(&command).expect("the command must encode") {
            v1::wal_command::Kind::SdkCommand(bytes) => {
                assert!(
                    serde_json::from_slice::<serde_json::Value>(&bytes).is_err(),
                    "{name} took the SDK arm but its payload parses as JSON"
                );
                let back = command_from_proto(v1::wal_command::Kind::SdkCommand(bytes))
                    .expect("the SDK arm must decode");
                assert_eq!(back, command, "{name} did not survive the round trip");
            }
            _ => panic!(
                "{name} now takes a modelled arm -- add it to COMMANDS_THE_RAFT_WAL_MODELS in \
                 this same change"
            ),
        }
    }

    // ---------- THE TRAP, pinned: a zero-TTL StringSetEx means "expires now", and neither the
    // ---------- modelled arm nor the SDK encoding can say that -- both would bring it back as a
    // ---------- plain StringSet, a write with no expiry. It is refused.
    let zero_ttl = Command::StringSetEx {
        key: "k".into(),
        value: b"v".to_vec(),
        ttl_ms: 0,
    };
    assert!(
        command_to_proto(&zero_ttl).is_err(),
        "a zero-TTL StringSetEx was encoded, and would read back as a write with no expiry"
    );

    // ---------- THE CONTROL. Without this, a `command_to_proto` that returned the SDK arm for
    // ---------- EVERYTHING would satisfy every assertion above.
    let modelled_command = Command::HashSet {
        key: "k".into(),
        field: "f".into(),
        value: b"v".to_vec(),
    };
    match command_to_proto(&modelled_command).expect("HashSet must encode") {
        v1::wal_command::Kind::HashSet(_) => {}
        _ => panic!("HashSet is on the modelled roster but did not take its modelled arm"),
    }
}

    fn envelope_with(command: Command) -> RaftWalEnvelope {
        RaftWalEnvelope {
            sequence: 7,
            checksum: 0x9e01_26c4,
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
                    leader_time_ms: 0,
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

    /// A zero-TTL `StringSetEx` means "expires now", and nothing the log can carry says so: the
    /// modelled form and the SDK encoding both tell the two string writes apart by a non-zero TTL,
    /// so it would come back as a plain `StringSet` and quietly lose its expiry. It is refused.
    #[test]
    fn a_zero_ttl_expiring_set_is_refused_rather_than_decayed_into_a_plain_set() {
        let command = Command::StringSetEx {
            key: "k".into(),
            value: b"hello".to_vec(),
            ttl_ms: 0,
        };
        assert!(
            encode_envelope(&envelope_with(command)).is_err(),
            "a zero-TTL expiring set was written, and would replay as a plain set"
        );
    }

    /// EVERY FIELD OF THE SIDE MESSAGE, A DIFFERENT VALUE, ROUND-TRIPPED. The conversions are
    /// written field by field and the struct is destructured with no `..`, so a field added to one
    /// of these structs fails to compile -- and a field copied to the wrong place changes the answer
    /// here, because no two fields hold the same number.
    #[test]
    fn the_side_message_round_trips_with_every_field_distinct() {
        let rest = RaftWalRecordRest {
            replica_role: RaftReplicaRole::Witness,
            joint_membership: Some(JointConsensusMembership {
            old_voters: vec![201, 301],
            new_voters: vec![202, 302],
        }),
            latest_external_snapshot_ref: Some(RaftExternalSnapshotRef {
            uri: "v1".to_string(),
            checksum: "v2".to_string(),
            byte_size: 103,
        }),
            installed_snapshot: None,
            pipeline_state: RaftPeerPipelineRuntimeState {
            match_index: 101,
            next_index: 102,
            append_requests: 103,
            append_accepted: 104,
            append_rejected: 105,
            committed_overwrite_rejections: 106,
            inflight_entries: 107,
            inflight_bytes: 108,
            append_queue_depth: 109,
            last_accepted_append_ms: 110,
            leader_reported_commit_index: 111,
            apply_inflight_tasks: 112,
            apply_queue_depth: 113,
            apply_queue_max_depth: 114,
            apply_backpressure_rejections: 115,
            memory_backpressure_rejections: 116,
            oversized_log_rejections: 117,
            append_queue_max_depth: 118,
            reorder_queue_depth: 119,
            out_of_order_append_rejections: 120,
            reorder_entries_accepted: 121,
            reorder_entries_released: 122,
            reorder_entries_rejected: 123,
            reorder_entry_timeouts: 124,
            reorder_dropped_packages: 125,
            stale_term_rejections: 126,
            append_send_failures: 127,
            snapshot_sending: true,
            snapshot_installing: true,
            snapshot_installed_index: 130,
            snapshot_send_attempts: 131,
            snapshot_send_completed: 132,
            snapshot_send_failed: 133,
            snapshot_install_started: 134,
            snapshot_install_completed: 135,
            snapshot_install_rejected: 136,
            snapshot_install_rolled_back: 137,
            snapshot_install_received_chunks: 138,
            snapshot_send_progress_mark: 139,
            snapshot_install_total_chunks: 140,
            snapshot_install_progress_per_mille: 141,
            snapshot_retry_count: 142,
            snapshot_chunk_retry_count: 143,
            snapshot_backpressure_rejections: 144,
            snapshot_rate_limit_rejections: 145,
            snapshot_send_started_ms: Some(146),
            snapshot_send_elapsed_ms: 147,
            snapshot_send_timeouts: 148,
            snapshot_during_membership_change: true,
            snapshot_rejoin_after_compacted_log: true,
            transfer_leader_target: true,
            transfer_leader_requests: 152,
            transfer_leader_accepted: 153,
            transfer_leader_rejected: 154,
            transfer_leader_completed: 155,
            transfer_leader_started_ms: Some(156),
            transfer_leader_elapsed_ms: 157,
            transfer_leader_timeouts: 158,
            pre_vote_rejections: 159,
            election_rejections: 160,
            offline_since_ms: Some(161),
            offline_elapsed_ms: 162,
            offline_timeout_reached: true,
            offline_timeout_rejections: 164,
            auto_promoted_from_learner: true,
        },
            read_safety_state: RaftReadSafetyRuntimeState {
            read_index_requests: 101,
            read_index_accepted: 102,
            read_index_rejected: 103,
            lease_read_requests: 104,
            lease_read_accepted: 105,
            lease_read_rejected: 106,
            pre_vote_requests: 107,
            pre_vote_accepted: 108,
            pre_vote_rejected: 109,
            bounded_stale_read_requests: 110,
            bounded_stale_read_accepted: 111,
            bounded_stale_read_rejected: 112,
            stale_leader_lease_rejected: 113,
            lagging_follower_read_rejected: 114,
            stale_follower_write_rejected: 115,
            minority_partition_read_rejected: 116,
            minority_partition_write_rejected: 117,
            healed_follower_catchup_observed: 118,
        },
            membership_evidence: RaftMembershipRuntimeEvidence {
            learner_add_count: 101,
            learner_catchup_count: 102,
            learner_promote_count: 103,
            voter_remove_count: 104,
            witness_add_count: 105,
            auto_promote_count: 106,
            leader_transfer_write_count: 107,
            leader_transfer_exact_once_commit_count: 108,
            leader_transfer_exact_once_commit_ids: vec![209, 309],
            pending_joint_consensus_persist_count: 110,
            pending_joint_consensus_restore_count: 111,
        },
        };
        // DENOMINATOR: nothing here is at its default, or a message that dropped a field would
        // still compare equal to one that carried it.
        assert!(!rest.pipeline_state.is_default());
        assert!(!rest.read_safety_state.is_default());
        assert!(!rest.membership_evidence.is_default());
        let message = rest_to_proto(&rest);
        let back = rest_from_proto(message).expect("the side message decodes");
        assert_eq!(back, rest);
    }

    /// A command this module does not model must still round-trip exactly, because the log has to
    /// replay commands the encoding has never been taught about.
    #[test]
    fn an_unmodelled_command_round_trips_through_the_sdk_arm() {
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
        envelope.checksum = u32::MAX;
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

    /// What one AppendEntries costs on the wire, both ways. Replication sends this to every
    /// follower for every write, so the ratio here is paid once per follower per write.
    #[test]
    fn replication_body_cost() {
        for payload in [10usize, 1024] {
            let entries = vec![RaftLogEntry {
                leader_time_ms: 0,
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
                    leader_time_ms: 0,
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
                    leader_time_ms: 0,
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
