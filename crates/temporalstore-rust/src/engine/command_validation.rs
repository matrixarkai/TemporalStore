// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Command inspection, admission control, and precondition validation helpers, split from engine.rs.
use super::*;
// FOR THE SLOT'S SERIES SURFACE: `ObjectSlot` implements these, and the names it keeps
// (`len`, `get`, `is_empty`) already matched the call sites -- only the trait was out of
// scope here. Anonymous, because only the methods are wanted.
use crate::engine::timestamped_series::TimestampedSeries as _;

/// The key a command READS (does not write), for LRU recency tracking. Covers the
/// key-addressed read commands; the hash-addressed context queries are omitted (their
/// buckets refresh recency on the next write -- acceptable, eviction is memory-only).
///
/// ONE KEY, NOT A LIST OF THEM. Every arm of this match yielded exactly one key or none, so
/// the `Vec<String>` this used to return carried at most one element and cloned it to do so.
/// A read command's key is owned by the command for as long as the recency stamp needs it,
/// which is why the borrow is sound and the clone was not buying anything.
pub(super) fn command_read_key(command: &Command) -> Option<&str> {
    match command {
        Command::CommonTtl { key, .. }
        | Command::CommonExists { key, .. }
        | Command::StringGet { key, .. }
        | Command::HashGet { key, .. }
        | Command::HashMultiGet { key, .. }
        | Command::HashGetAll { key, .. }
        | Command::HashLen { key, .. }
        | Command::SetMembers { key, .. }
        | Command::SetScan { key, .. }
        | Command::FeatureQuery { key, .. }
        | Command::FeatureAggQuery { key, .. }
        | Command::FeatureProtoAggQuery { key, .. }
        | Command::SequenceQuery { key, .. }
        | Command::CellRankQuery { key, .. } => Some(key.as_str()),
        _ => None,
    }
}

/// Hand each key a command touched, read or write, to `visit` for the per-bucket LRU stamp.
///
/// VISITED RATHER THAN COLLECTED. Execution already derives owned write keys for durability and
/// index maintenance, while a read can borrow its key from the command. Recency retains neither,
/// so deriving another vector solely for this walk creates a second vector and a second copy of
/// each ordinary feature key.
pub(super) fn for_each_precomputed_touched_key(
    command: &Command,
    object_keys: &[String],
    mut visit: impl FnMut(&str),
) {
    if object_keys.is_empty() {
        if let Some(key) = command_read_key(command) {
            visit(key);
        }
    } else {
        for key in object_keys {
            visit(key);
        }
    }
}

#[derive(Clone)]
pub(crate) enum CommandObjectKeys {
    One(String),
    Many(Vec<String>),
}

impl CommandObjectKeys {
    fn one(key: String) -> Self {
        Self::One(key)
    }

    fn many(keys: Vec<String>) -> Self {
        Self::Many(keys)
    }

    fn as_slice(&self) -> &[String] {
        match self {
            Self::One(key) => std::slice::from_ref(key),
            Self::Many(keys) => keys,
        }
    }
}

impl std::ops::Deref for CommandObjectKeys {
    type Target = [String];

    fn deref(&self) -> &Self::Target {
        self.as_slice()
    }
}

pub(crate) enum CommandObjectKeysIntoIter {
    One(std::iter::Once<String>),
    Many(std::vec::IntoIter<String>),
}

impl Iterator for CommandObjectKeysIntoIter {
    type Item = String;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::One(key) => key.next(),
            Self::Many(keys) => keys.next(),
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        match self {
            Self::One(key) => key.size_hint(),
            Self::Many(keys) => keys.size_hint(),
        }
    }
}

impl ExactSizeIterator for CommandObjectKeysIntoIter {}

impl IntoIterator for CommandObjectKeys {
    type Item = String;
    type IntoIter = CommandObjectKeysIntoIter;

    fn into_iter(self) -> Self::IntoIter {
        match self {
            Self::One(key) => CommandObjectKeysIntoIter::One(std::iter::once(key)),
            Self::Many(keys) => CommandObjectKeysIntoIter::Many(keys.into_iter()),
        }
    }
}

impl<'a> IntoIterator for &'a CommandObjectKeys {
    type Item = &'a String;
    type IntoIter = std::slice::Iter<'a, String>;

    fn into_iter(self) -> Self::IntoIter {
        self.as_slice().iter()
    }
}

pub(crate) fn command_object_keys(command: &Command) -> CommandObjectKeys {
    match command {
        // Touches no object, so it holds no object keys.
        Command::LeaderEstablish => CommandObjectKeys::many(Vec::new()),
        Command::CommonDelete { key } => CommandObjectKeys::many(associated_record_keys(key)),
        Command::CommonPersist { key } => CommandObjectKeys::many(associated_record_keys(key)),
        Command::CommonExpire { key, .. }
        | Command::StringSet { key, .. }
        | Command::StringSetEx { key, .. }
        | Command::StringSetConditional { key, .. }
        | Command::StringDelete { key }
        | Command::HashSet { key, .. }
        | Command::HashMultiSet { key, .. }
        | Command::HashMultiSetIfTimestampOlder { key, .. }
        | Command::HashIncrBy { key, .. }
        | Command::HashDelete { key, .. }
        | Command::SetAdd { key, .. }
        | Command::SetRemove { key, .. }
        | Command::ListPush { key, .. }
        | Command::ListPop { key, .. }
        | Command::ListRange { key, .. }
        | Command::ListLen { key }
        | Command::ZSetAdd { key, .. }
        | Command::ZSetScore { key, .. }
        | Command::ZSetRemove { key, .. }
        | Command::ZSetCard { key }
        | Command::ZSetRange { key, .. }
        | Command::ZSetRangeByScore { key, .. }
        | Command::ZSetIncrBy { key, .. }
        | Command::ZSetPop { key, .. }
        | Command::ZSetRank { key, .. }
        | Command::BucketTake { key, .. }
        | Command::BucketPeek { key, .. }
        | Command::SeenCheck { key, .. }
        | Command::SeenCard { key }
        | Command::FeatureAppend { key, .. }
        | Command::FeatureAppendWithPolicy { key, .. }
        | Command::FeatureReplace { key, .. }
        | Command::FeatureDelete { key }
        | Command::SequenceAdd { key, .. }
        | Command::ControlStateIncrement { key, .. }
        | Command::ControlStateIncrementWithOptions { key, .. }
        | Command::ControlStateChangeAdd { key, .. }
        | Command::ControlStateSelectionSet { key, .. } => CommandObjectKeys::one(key.clone()),
        Command::ControlStateSet { family, key, .. }
        | Command::ControlStateSetAndGet { family, key, .. }
        | Command::ControlStateSetAndGetWithOptions { family, key, .. } => {
            CommandObjectKeys::one(control_state_family_key(*family, key))
        }
        Command::ContextUpsertNode { tenant_hash, node } => {
            CommandObjectKeys::one(context_node_key(*tenant_hash, node.node_hash))
        }
        Command::ContextWriteEvent {
            tenant_hash,
            node_hash,
            ..
        } => CommandObjectKeys::one(context_event_key(*tenant_hash, *node_hash)),
        Command::ContextWriteExtractedEvent {
            tenant_hash,
            node_hash,
            event,
            indexes,
            ..
        } => {
            let mut keys = vec![context_event_key(*tenant_hash, *node_hash)];
            if !context_index_disabled(indexes, InternalContextIndex::EventKind) {
                keys.push(context_index_key(
                    *tenant_hash,
                    "event_kind",
                    context_event_kind_hash(event),
                    indexes.scope_hash,
                ));
            }
            if !context_index_disabled(indexes, InternalContextIndex::Status)
                && indexes.status_hash != 0
            {
                keys.push(context_index_key(
                    *tenant_hash,
                    "status",
                    indexes.status_hash,
                    indexes.scope_hash,
                ));
            }
            if !context_index_disabled(indexes, InternalContextIndex::Source)
                && indexes.source_hash != 0
            {
                keys.push(context_index_key(
                    *tenant_hash,
                    "source",
                    indexes.source_hash,
                    indexes.scope_hash,
                ));
            }
            if !context_index_disabled(indexes, InternalContextIndex::EventTimeBucket)
                && indexes.event_time_bucket_ms != 0
            {
                keys.push(context_index_key(
                    *tenant_hash,
                    "event_time_bucket",
                    indexes.event_time_bucket_ms,
                    indexes.scope_hash,
                ));
            }
            if !context_index_disabled(indexes, InternalContextIndex::Entity) {
                keys.extend(
                    indexes
                        .entity_hashes
                        .iter()
                        .copied()
                        .filter(|hash| *hash != 0)
                        .map(|entity_hash| {
                            context_index_key(
                                *tenant_hash,
                                "entity",
                                entity_hash,
                                indexes.scope_hash,
                            )
                        }),
                );
            }
            CommandObjectKeys::many(keys)
        }
        Command::ContextWriteIndexRef {
            tenant_hash,
            index_name,
            index_value_hash,
            scope_hash,
            ..
        } => CommandObjectKeys::one(context_index_key(
            *tenant_hash,
            index_name,
            *index_value_hash,
            *scope_hash,
        )),
        Command::ContextWritePackAudit { tenant_hash, audit } => {
            CommandObjectKeys::one(context_audit_key(*tenant_hash, audit.session_hash))
        }
        Command::ContextMarkSummaryDirty {
            tenant_hash,
            node_hash,
            ..
        } => CommandObjectKeys::one(context_dirty_key(*tenant_hash, *node_hash)),
        Command::ContextMarkEmbeddingDirty {
            tenant_hash,
            node_hash,
            ..
        } => CommandObjectKeys::one(context_embedding_dirty_key(*tenant_hash, *node_hash)),
        Command::ContextUpsertEntity {
            tenant_hash,
            entity,
        } => CommandObjectKeys::one(context_entity_key(
            *tenant_hash,
            entity.node_hash,
            entity.entity_hash,
        )),
        Command::ContextUpsertChildRef {
            tenant_hash,
            child_ref,
        } => CommandObjectKeys::one(context_child_key(*tenant_hash, child_ref.parent_hash)),
        Command::ContextSetNodeEmbedding {
            tenant_hash,
            node_hash,
            ..
        } => CommandObjectKeys::one(context_node_key(*tenant_hash, *node_hash)),
        Command::ContextQueryNodeEmbeddings {
            tenant_hash,
            node_hashes,
        } => CommandObjectKeys::many(
            node_hashes
                .iter()
                .map(|node_hash| context_node_key(*tenant_hash, *node_hash))
                .collect(),
        ),
        Command::ContextQueryNodeVectorProjections {
            tenant_hash,
            node_hashes,
        } => CommandObjectKeys::many(
            node_hashes
                .iter()
                .map(|node_hash| context_node_key(*tenant_hash, *node_hash))
                .collect(),
        ),
        Command::ContextUpsertSummary {
            tenant_hash,
            summary,
        } => CommandObjectKeys::one(context_summary_key(
            *tenant_hash,
            summary.node_hash,
            summary.level,
        )),
        Command::ContextWriteCompressionEvent { tenant_hash, event } => {
            CommandObjectKeys::one(context_compression_key(*tenant_hash, event.node_hash))
        }
        Command::ContextCompressEvents {
            tenant_hash,
            node_hash,
            ..
        } => CommandObjectKeys::one(context_compression_key(*tenant_hash, *node_hash)),
        Command::SequenceBatchQuery { .. }
        | Command::CommonTtl { .. }
        | Command::CommonExists { .. }
        | Command::StringGet { .. }
        | Command::HashGet { .. }
        | Command::HashMultiGet { .. }
        | Command::HashGetAll { .. }
        | Command::HashLen { .. }
        | Command::SetMembers { .. }
        | Command::SetScan { .. }
        | Command::FeatureQuery { .. }
        | Command::FeatureQueryFiltered { .. }
        | Command::FeatureAggQuery { .. }
        | Command::FeatureProtoAggQuery { .. }
        | Command::SequenceQuery { .. }
        | Command::CellRankQuery { .. }
        | Command::ControlStateCount { .. }
        | Command::ControlStateQuery { .. }
        | Command::ControlStateDetail { .. }
        | Command::ControlStateFamilyQuery { .. }
        | Command::ControlStateSelectionQuery { .. }
        | Command::ControlStateManager { .. }
        | Command::ControlStateDebug { .. }
        | Command::ContextGetNode { .. }
        | Command::ContextGetNodes { .. }
        | Command::ContextQueryEvents { .. }
        | Command::ContextQueryEventsByIds { .. }
        | Command::ContextQueryIndex { .. }
        | Command::ContextQueryIndexIntersection { .. }
        | Command::ContextQueryPackAudit { .. }
        | Command::ContextQuerySummaryDirty { .. }
        | Command::ContextQueryEmbeddingDirty { .. }
        | Command::ContextGetEntity { .. }
        | Command::ContextQueryEntities { .. }
        | Command::ContextQueryChildren { .. }
        | Command::ContextTraverseTree { .. }
        | Command::ContextQuerySummaries { .. }
        | Command::ContextQuerySummaryVectors { .. }
        | Command::ContextQueryCompressionEvents { .. }
        | Command::ContextResourceBlobBegin { .. }
        | Command::ContextResourceBlobAppend { .. }
        | Command::ContextResourceBlobCommit { .. }
        | Command::ContextResourceBlobPut { .. }
        | Command::ContextResourceBlobFetch { .. }
        | Command::ContextResourceBlobSweep { .. }
        | Command::ContextQueryNodeContext { .. } => CommandObjectKeys::many(Vec::new()),
    }
}

pub(super) fn command_updates_bucket_index_directly(command: &Command) -> bool {
    matches!(
        command,
        Command::CommonDelete { .. }
            | Command::StringDelete { .. }
            | Command::StringSet { .. }
            | Command::StringSetEx { .. }
            | Command::StringSetConditional { .. }
            | Command::HashSet { .. }
            | Command::HashMultiSet { .. }
            | Command::HashMultiSetIfTimestampOlder { .. }
            | Command::HashIncrBy { .. }
            | Command::HashDelete { .. }
            | Command::SetAdd { .. }
            | Command::SetRemove { .. }
            | Command::ListPush { .. }
            | Command::ListPop { .. }
            | Command::ZSetAdd { .. }
            | Command::ZSetRemove { .. }
            | Command::ZSetIncrBy { .. }
            | Command::ZSetPop { .. }
            | Command::ControlStateIncrement { .. }
            | Command::ControlStateIncrementWithOptions { .. }
            | Command::ControlStateSet { .. }
            | Command::ControlStateSetAndGet { .. }
            | Command::ControlStateSetAndGetWithOptions { .. }
            // These file their blocks through `sync_bucket_index_object_blocks`, which maintains
            // exactly what a rebuild would recompute. Without them here the post-command path took
            // the rebuild branch on EVERY call: measured at twice the shard's block count per call,
            // so a one-point feature append into a 1,024-key store cost 12,717 allocations, none of
            // it about the point being appended.
            | Command::FeatureAppend { .. }
            | Command::FeatureAppendWithPolicy { .. }
            | Command::FeatureReplace { .. }
            | Command::FeatureDelete { .. }
            | Command::SequenceAdd { .. }
    )
}

/// True when a command mutates only state that no block backs.
///
/// The seen sets, the control-state change and selection maps, and the token buckets live outside the
/// block-backed models, and their durability rides the index-log `key_states` blobs rather than a block
/// entry. So a rebuild of the block index after one of these can only recompute what it already held.
///
/// This is deliberately NOT folded into `command_updates_bucket_index_directly`: these commands do
/// not update that index, and saying they do to skip the rebuild would make the predicate assert
/// something false. They need their own reason.
pub(super) fn command_writes_no_block(command: &Command) -> bool {
    matches!(
        command,
        Command::SeenCheck { .. }
            | Command::ControlStateChangeAdd { .. }
            | Command::ControlStateSelectionSet { .. }
            | Command::BucketTake { .. }
    )
}

pub(crate) fn is_write_command(command: &Command) -> bool {
    matches!(
        command,
        Command::CommonDelete { .. }
            | Command::CommonExpire { .. }
            | Command::CommonPersist { .. }
            | Command::StringSet { .. }
            | Command::StringSetEx { .. }
            | Command::StringSetConditional { .. }
            | Command::StringDelete { .. }
            | Command::HashSet { .. }
            | Command::HashMultiSet { .. }
            | Command::HashMultiSetIfTimestampOlder { .. }
            | Command::HashIncrBy { .. }
            | Command::HashDelete { .. }
            | Command::SetAdd { .. }
            | Command::SetRemove { .. }
            | Command::ListPush { .. }
            | Command::ListPop { .. }
            | Command::ZSetAdd { .. }
            | Command::ZSetRemove { .. }
            | Command::ZSetIncrBy { .. }
            | Command::ZSetPop { .. }
            | Command::BucketTake { .. }
            | Command::SeenCheck { .. }
            | Command::FeatureAppend { .. }
            | Command::FeatureAppendWithPolicy { .. }
            | Command::FeatureReplace { .. }
            | Command::FeatureDelete { .. }
            | Command::SequenceAdd { .. }
            | Command::ControlStateIncrement { .. }
            | Command::ControlStateIncrementWithOptions { .. }
            | Command::ControlStateChangeAdd { .. }
            | Command::ControlStateSet { .. }
            | Command::ControlStateSetAndGet { .. }
            | Command::ControlStateSetAndGetWithOptions { .. }
            | Command::ControlStateSelectionSet { .. }
            | Command::ContextResourceBlobBegin { .. }
            | Command::ContextResourceBlobAppend { .. }
            | Command::ContextResourceBlobCommit { .. }
            | Command::ContextResourceBlobPut { .. }
            | Command::ContextResourceBlobSweep { .. }
            | Command::ContextUpsertNode { .. }
            | Command::ContextWriteEvent { .. }
            | Command::ContextWriteExtractedEvent { .. }
            | Command::ContextWriteIndexRef { .. }
            | Command::ContextWritePackAudit { .. }
            | Command::ContextMarkSummaryDirty { .. }
            | Command::ContextMarkEmbeddingDirty { .. }
            | Command::ContextUpsertEntity { .. }
            | Command::ContextUpsertChildRef { .. }
            | Command::ContextSetNodeEmbedding { .. }
            | Command::ContextUpsertSummary { .. }
            | Command::ContextWriteCompressionEvent { .. }
            | Command::ContextCompressEvents { .. }
    )
}

pub(super) fn admission_limits(
    shard_id: ShardId,
    write_command: bool,
    config: &Config,
    table_name: Option<&str>,
) -> Vec<AdmissionLimit> {
    let mut limits = Vec::new();
    // A configured qps of 0 means UNLIMITED on EVERY scope below, not deny-all. Zero is what
    // an unset knob reads as, so treating it as "refuse everything" turns an intended "no limit
    // here" into a total outage; denying traffic outright has its own controls.
    //
    // The filter has to stay uniform across the three scopes. It once guarded the shard knob
    // alone, so `write_qps: 0` meant no limit while `table_write_qps: 0` a few lines below denied
    // every write to that table -- two knobs of identical shape with opposite meanings.
    if let Some(limit) = (if write_command {
        config.write_qps
    } else {
        config.read_qps
    })
    .filter(|&limit| limit > 0)
    {
        limits.push(AdmissionLimit {
            scope: AdmissionScope::Shard(shard_id),
            limit,
            label: if write_command {
                "write_qps"
            } else {
                "read_qps"
            },
        });
    }
    if let Some(table_name) = table_name
        .map(str::trim)
        .filter(|table_name| !table_name.is_empty())
    {
        if let Some(limit) = (if write_command {
            config.table_write_qps
        } else {
            config.table_read_qps
        })
        .filter(|&limit| limit > 0)
        {
            limits.push(AdmissionLimit {
                scope: AdmissionScope::Table(table_name.to_string()),
                limit,
                label: if write_command {
                    "table_write_qps"
                } else {
                    "table_read_qps"
                },
            });
        }
    }
    if let Some(tenant_name) = config
        .tenant_name
        .as_deref()
        .map(str::trim)
        .filter(|tenant_name| !tenant_name.is_empty())
    {
        if let Some(limit) = (if write_command {
            config.tenant_write_qps
        } else {
            config.tenant_read_qps
        })
        .filter(|&limit| limit > 0)
        {
            limits.push(AdmissionLimit {
                scope: AdmissionScope::Tenant(tenant_name.to_string()),
                limit,
                label: if write_command {
                    "tenant_write_qps"
                } else {
                    "tenant_read_qps"
                },
            });
        }
    }
    limits
}

pub(super) fn reset_admission_window(admission: &mut AdmissionState, now_sec: u64) {
    if admission.window_epoch_sec != now_sec {
        admission.window_epoch_sec = now_sec;
        admission.read_count = 0;
        admission.write_count = 0;
    }
}

pub(super) fn admission_count(admission: &mut AdmissionState, write_command: bool) -> &mut u64 {
    if write_command {
        &mut admission.write_count
    } else {
        &mut admission.read_count
    }
}

pub(super) fn now_epoch_seconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or_default()
}

pub(super) fn validate_command_preconditions(
    cache: &MultiLayerCache,
    block_store: &BlockStore,
    shard_id: ShardId,
    shard: &ShardState,
    command: &Command,
) -> Result<(), Status> {
    match command {
        Command::SetScan { limit, .. } if *limit == 0 || *limit > 10_000 => {
            return Err(Status::error(
                "invalid_argument",
                "set scan limit must be between 1 and 10000",
            ));
        }
        Command::HashMultiSetIfTimestampOlder {
            timestamp_nanos, ..
        } if !(0..1_000_000_000).contains(timestamp_nanos) => {
            return Err(Status::error(
                "invalid_argument",
                "timestamp_nanos must be between 0 and 999999999",
            ));
        }
        // A ranked cell read is refused at admission for everything that needs no snapshot: the
        // bounds, the window, the score parameters. Names are checked against the key's header when
        // the read runs.
        Command::CellRankQuery { spec, .. } => {
            if let Err(why) = super::cell_rank::validate_spec(spec) {
                return Err(Status::error("invalid_argument", why));
            }
        }
        // Expiry checks here use the SAME replay-aware clock as the executor's
        // `remove_if_expired`. Validation runs only on the leader today, where the two
        // clocks coincide, so this changes no production behaviour. It removes a split
        // that would matter the moment anything validates during replay: deciding
        // "is this key expired" against the restart clock is what makes a replayed
        // command fail its precondition and abort a shard load.
        Command::CommonExpire { key, .. } => {
            if shard
                .expires_at_ms()
                .get(key.as_str())
                .map(|expires_at| *expires_at <= resolve_now_ms())
                .unwrap_or(false)
                || !record_exists(shard, key)
            {
                return Err(Status::error("not_found", "key not found"));
            }
        }
        Command::FeatureAppend { key, points } => {
            let current = shard
                .features()
                .get(&key[..])
                .map(|series| series.len())
                .unwrap_or(0);
            if current.saturating_add(points.len()) > FEATURE_ADD_HARD_MAX_SIZE {
                return Err(Status::error(
                    "invalid_argument",
                    format!("{key} size bigger than {FEATURE_ADD_HARD_MAX_SIZE}"),
                ));
            }
        }
        Command::FeatureAppendWithPolicy {
            key,
            points,
            policy,
        } => {
            if *policy == FeatureWritePolicy::Block {
                return Err(Status::error("invalid_argument", "Invalid write policy"));
            }
            let current = shard
                .features()
                .get(&key[..])
                .map(|series| series.len())
                .unwrap_or(0);
            if current.saturating_add(points.len()) > FEATURE_ADD_HARD_MAX_SIZE {
                return Err(Status::error(
                    "invalid_argument",
                    format!("{key} size bigger than {FEATURE_ADD_HARD_MAX_SIZE}"),
                ));
            }
            // Merge-reduce is refused HERE, like `Block`, so a bad request writes nothing and
            // reaches no log. The plan is thrown away; execution plans again (see the module note).
            if *policy == FeatureWritePolicy::MergeReduce {
                let (start_routing_bucket, end_routing_bucket) = shard.routing_range();
                // A key past its deadline reads as absent, as it does everywhere else.
                let expired = shard
                    .expires_at_ms()
                    .get(key.as_str())
                    .map(|expires_at| *expires_at <= resolve_now_ms())
                    .unwrap_or(false);
                crate::engine::cell_merge_append::plan_cell_merge(
                    cache,
                    block_store,
                    shard_id,
                    shard.features().get(&key[..]).filter(|_| !expired),
                    key,
                    crate::engine::hashing::block_routing_bucket(
                        key,
                        start_routing_bucket,
                        end_routing_bucket,
                    ),
                    points,
                )?;
            }
        }
        Command::FeatureReplace { key, points, .. } => {
            let current = shard
                .features()
                .get(&key[..])
                .map(|series| series.len())
                .unwrap_or(0);
            if current.saturating_add(points.len()) > FEATURE_ADD_HARD_MAX_SIZE {
                return Err(Status::error(
                    "invalid_argument",
                    format!("{key} size bigger than {FEATURE_ADD_HARD_MAX_SIZE}"),
                ));
            }
        }
        // THE SAME SERIES, SO THE SAME CEILING. `SequenceAdd` appends into `features` exactly as
        // the three arms above do -- `execute_on_shard` reaches for `features_mut()` under both
        // names -- but it was not in this block, so the ceiling the other three enforce was
        // reachable around: a key the feature commands refuse to grow could be grown past it by
        // sending rows instead of points.
        //
        // The cost of the omission is not only the ceiling. Validation runs BEFORE execution, so a
        // refused command writes nothing; an accepted one serialises every row to JSON and appends
        // a block per row before `trim_timestamped_series` discards everything past
        // `feature_max_size`. An unbounded batch therefore bought an unbounded amount of durable
        // work for data that does not survive the same command.
        Command::SequenceAdd { key, rows } => {
            let current = shard
                .features()
                .get(&key[..])
                .map(|series| series.len())
                .unwrap_or(0);
            if current.saturating_add(rows.len()) > FEATURE_ADD_HARD_MAX_SIZE {
                return Err(Status::error(
                    "invalid_argument",
                    format!("{key} size bigger than {FEATURE_ADD_HARD_MAX_SIZE}"),
                ));
            }
        }
        Command::ContextUpsertNode { tenant_hash, node } => {
            validate_context_required(*tenant_hash != 0, "tenant_hash is required")?;
            validate_context_node(node)?;
        }
        Command::ContextGetNode {
            tenant_hash,
            node_hash,
        } => {
            validate_context_required(*tenant_hash != 0, "tenant_hash is required")?;
            validate_context_required(*node_hash != 0, "node_hash is required")?;
        }
        Command::ContextGetNodes {
            tenant_hash,
            node_hashes,
        } => {
            validate_context_required(*tenant_hash != 0, "tenant_hash is required")?;
            validate_context_required(!node_hashes.is_empty(), "node_hashes are required")?;
            for node_hash in node_hashes {
                validate_context_required(*node_hash != 0, "node_hash is required")?;
            }
        }
        Command::ContextQuerySummaryVectors {
            tenant_hash,
            node_hashes,
            level,
            as_of_ms,
        } => {
            validate_context_required(*tenant_hash != 0, "tenant_hash is required")?;
            validate_context_required(!node_hashes.is_empty(), "node_hashes are required")?;
            validate_context_required(*level != 0, "level is required")?;
            validate_context_required(*as_of_ms != 0, "as_of_ms is required")?;
            for node_hash in node_hashes {
                validate_context_required(*node_hash != 0, "node_hash is required")?;
            }
        }
        Command::ContextWriteEvent {
            tenant_hash,
            node_hash,
            event,
            ..
        } => {
            validate_context_required(*tenant_hash != 0, "tenant_hash is required")?;
            validate_context_required(*node_hash != 0, "node_hash is required")?;
            validate_context_event(event)?;
        }
        Command::ContextWriteExtractedEvent {
            tenant_hash,
            node_hash,
            event,
            indexes,
            ..
        } => {
            validate_context_required(*tenant_hash != 0, "tenant_hash is required")?;
            validate_context_required(*node_hash != 0, "node_hash is required")?;
            validate_context_event(event)?;
            validate_context_extracted_indexes(event, indexes)?;
        }
        Command::ContextQueryEvents {
            tenant_hash,
            node_hash,
            start_time_ms,
            end_time_ms,
            limit,
            max_scan,
            kinds,
            statuses,
            min_confidence,
            min_importance,
            ..
        } => {
            validate_context_required(*tenant_hash != 0, "tenant_hash is required")?;
            validate_context_required(*node_hash != 0, "node_hash is required")?;
            validate_context_limit(*limit)?;
            validate_context_limit(*max_scan)?;
            validate_context_range(*start_time_ms, *end_time_ms)?;
            validate_context_filters(kinds, statuses, *min_confidence, *min_importance)?;
        }
        Command::ContextQueryEventsByIds {
            tenant_hash,
            node_hash,
            event_id_hashes,
            limit,
        } => {
            validate_context_required(*tenant_hash != 0, "tenant_hash is required")?;
            validate_context_required(*node_hash != 0, "node_hash is required")?;
            validate_context_required(!event_id_hashes.is_empty(), "event ids are required")?;
            validate_context_limit(*limit)?;
            for event_id_hash in event_id_hashes {
                validate_context_required(*event_id_hash != 0, "event id is required")?;
            }
        }
        Command::ContextWriteIndexRef {
            tenant_hash,
            index_name,
            index_value_hash,
            event_time_ms,
            index_ref,
            ..
        } => {
            validate_context_required(*tenant_hash != 0, "tenant_hash is required")?;
            validate_context_index_name(index_name)?;
            validate_context_required(*index_value_hash != 0, "index_value_hash is required")?;
            validate_context_required(*event_time_ms != 0, "event_time_ms is required")?;
            validate_context_timestamp(*event_time_ms)?;
            validate_context_index_ref(index_ref)?;
        }
        Command::ContextQueryIndex {
            tenant_hash,
            index_name,
            index_value_hash,
            start_time_ms,
            end_time_ms,
            limit,
            ..
        } => {
            validate_context_required(*tenant_hash != 0, "tenant_hash is required")?;
            validate_context_index_name(index_name)?;
            validate_context_required(*index_value_hash != 0, "index_value_hash is required")?;
            validate_context_limit(*limit)?;
            validate_context_range(*start_time_ms, *end_time_ms)?;
        }
        Command::ContextQueryIndexIntersection {
            tenant_hash,
            predicates,
            limit,
        } => {
            validate_context_required(*tenant_hash != 0, "tenant_hash is required")?;
            validate_context_required(!predicates.is_empty(), "predicates are required")?;
            validate_context_limit(*limit)?;
            for predicate in predicates {
                validate_context_index_lookup(predicate)?;
            }
        }
        Command::ContextWritePackAudit { tenant_hash, audit } => {
            validate_context_required(*tenant_hash != 0, "tenant_hash is required")?;
            validate_context_pack_audit(audit)?;
        }
        Command::ContextQueryPackAudit {
            tenant_hash,
            session_hash,
            start_time_ms,
            end_time_ms,
            limit,
            ..
        } => {
            validate_context_required(*tenant_hash != 0, "tenant_hash is required")?;
            validate_context_required(*session_hash != 0, "session_hash is required")?;
            validate_context_limit(*limit)?;
            validate_context_range(*start_time_ms, *end_time_ms)?;
        }
        Command::ContextMarkSummaryDirty {
            tenant_hash,
            node_hash,
            event_time_ms,
            reason,
            propagate_depth,
            ..
        } => {
            validate_context_required(*tenant_hash != 0, "tenant_hash is required")?;
            validate_context_dirty_node(*node_hash, *event_time_ms, *reason, *propagate_depth)?;
        }
        Command::ContextQuerySummaryDirty {
            tenant_hash,
            node_hash,
            start_time_ms,
            end_time_ms,
            limit,
            ..
        } => {
            validate_context_required(*tenant_hash != 0, "tenant_hash is required")?;
            validate_context_required(*node_hash != 0, "node_hash is required")?;
            validate_context_limit(*limit)?;
            validate_context_range(*start_time_ms, *end_time_ms)?;
        }
        Command::ContextMarkEmbeddingDirty {
            tenant_hash,
            node_hash,
            event_time_ms,
            reason,
            propagate_depth,
            ..
        } => {
            validate_context_required(*tenant_hash != 0, "tenant_hash is required")?;
            validate_context_dirty_node(*node_hash, *event_time_ms, *reason, *propagate_depth)?;
        }
        Command::ContextQueryEmbeddingDirty {
            start_time_ms,
            end_time_ms,
            limit,
            ..
        } => {
            // Both node_hash == 0 (all pending nodes) and tenant_hash == 0 (all
            // tenants on the shard) are permitted: together they are the drainer's
            // O(pending) all-pending scan.
            validate_context_limit(*limit)?;
            validate_context_range(*start_time_ms, *end_time_ms)?;
        }
        Command::ContextUpsertEntity {
            tenant_hash,
            entity,
        } => {
            validate_context_required(*tenant_hash != 0, "tenant_hash is required")?;
            validate_context_entity(entity)?;
        }
        Command::ContextGetEntity {
            tenant_hash,
            node_hash,
            entity_hash,
        } => {
            validate_context_required(*tenant_hash != 0, "tenant_hash is required")?;
            validate_context_required(*node_hash != 0, "node_hash is required")?;
            validate_context_required(*entity_hash != 0, "entity_hash is required")?;
        }
        Command::ContextQueryEntities {
            tenant_hash,
            node_hash,
            entity_hashes,
            limit,
        } => {
            validate_context_required(*tenant_hash != 0, "tenant_hash is required")?;
            validate_context_required(*node_hash != 0, "node_hash is required")?;
            validate_context_limit(*limit)?;
            if entity_hashes.len() > CONTEXT_MAX_LIMIT {
                return Err(Status::error(
                    "invalid_argument",
                    "entity_hashes exceeds maximum",
                ));
            }
            if entity_hashes.iter().any(|hash| *hash == 0) {
                return Err(Status::error(
                    "invalid_argument",
                    "entity_hashes must be non-zero",
                ));
            }
        }
        Command::ContextUpsertChildRef {
            tenant_hash,
            child_ref,
        } => {
            validate_context_required(*tenant_hash != 0, "tenant_hash is required")?;
            validate_context_child_ref(child_ref)?;
        }
        Command::ContextQueryChildren {
            tenant_hash,
            parent_hash,
            limit,
        } => {
            validate_context_required(*tenant_hash != 0, "tenant_hash is required")?;
            validate_context_required(*parent_hash != 0, "parent_hash is required")?;
            validate_context_limit(*limit)?;
        }
        Command::ContextTraverseTree {
            tenant_hash,
            start_node_hash,
            query_vector,
            max_depth,
            top_k_per_depth,
            max_children_scored_per_parent,
            max_candidate_nodes,
            ..
        } => {
            validate_context_required(*tenant_hash != 0, "tenant_hash is required")?;
            validate_context_required(*start_node_hash != 0, "start_node_hash is required")?;
            validate_context_required(!query_vector.is_empty(), "query_vector is required")?;
            validate_context_embedding_vector("query_vector", query_vector)?;
            if max_depth.unwrap_or_default() > CONTEXT_MAX_TRAVERSAL_DEPTH {
                return Err(Status::error(
                    "invalid_argument",
                    "max_depth exceeds maximum",
                ));
            }
            for limit in [
                *top_k_per_depth,
                *max_children_scored_per_parent,
                *max_candidate_nodes,
            ] {
                validate_context_limit(limit)?;
            }
        }
        Command::ContextUpsertSummary {
            tenant_hash,
            summary,
        } => {
            validate_context_required(*tenant_hash != 0, "tenant_hash is required")?;
            validate_context_summary(summary)?;
        }
        Command::ContextQuerySummaries {
            tenant_hash,
            node_hash,
            level,
            as_of_ms,
            limit,
        } => {
            validate_context_required(*tenant_hash != 0, "tenant_hash is required")?;
            validate_context_required(*node_hash != 0, "node_hash is required")?;
            validate_context_required(*level != 0, "level is required")?;
            validate_context_required(*as_of_ms != 0, "as_of_ms is required")?;
            validate_context_timestamp(*as_of_ms)?;
            validate_context_limit(*limit)?;
        }
        Command::ContextWriteCompressionEvent { tenant_hash, event } => {
            validate_context_required(*tenant_hash != 0, "tenant_hash is required")?;
            validate_context_compression_event(event)?;
        }
        Command::ContextQueryCompressionEvents {
            tenant_hash,
            node_hashes,
            start_time_ms,
            end_time_ms,
            limit,
        } => {
            validate_context_required(*tenant_hash != 0, "tenant_hash is required")?;
            validate_context_limit(*limit)?;
            validate_context_range(*start_time_ms, *end_time_ms)?;
            if node_hashes.len() > CONTEXT_MAX_FILTER_VALUES {
                return Err(Status::error("invalid_argument", "too many node_hashes"));
            }
        }
        Command::ContextCompressEvents {
            tenant_hash,
            node_hash,
            source_start_ms,
            source_end_ms,
            compressed_time_ms,
            max_source_events,
            min_confidence,
            min_importance,
        } => {
            validate_context_required(*tenant_hash != 0, "tenant_hash is required")?;
            validate_context_required(*node_hash != 0, "node_hash is required")?;
            validate_context_range(*source_start_ms, *source_end_ms)?;
            validate_context_limit(*max_source_events)?;
            validate_context_score("min_confidence", *min_confidence)?;
            validate_context_score("min_importance", *min_importance)?;
            if *compressed_time_ms != 0 {
                validate_context_timestamp(*compressed_time_ms)?;
            }
        }
        Command::ContextQueryNodeContext {
            tenant_hash,
            node_hash,
            summary_level,
            as_of_ms,
            cold_start_time_ms,
            cold_end_time_ms,
            compression_limit,
        } => {
            validate_context_required(*tenant_hash != 0, "tenant_hash is required")?;
            validate_context_required(*node_hash != 0, "node_hash is required")?;
            validate_context_required(*as_of_ms != 0, "as_of_ms is required")?;
            validate_context_timestamp(*as_of_ms)?;
            if summary_level.unwrap_or(1) == 0 {
                return Err(Status::error(
                    "invalid_argument",
                    "summary_level is required",
                ));
            }
            validate_context_limit(*compression_limit)?;
            if *cold_start_time_ms != 0 || *cold_end_time_ms != 0 {
                validate_context_range(*cold_start_time_ms, *cold_end_time_ms)?;
            }
        }
        _ => {}
    }

    if let Command::HashIncrBy {
        key,
        field,
        increment,
    } = command
    {
        if shard
            .expires_at_ms()
            .get(key.as_str())
            .map(|expires_at| *expires_at <= resolve_now_ms())
            .unwrap_or(false)
        {
            return Ok(());
        }
        let Some(bytes) = shard
            .bucket_index.hashes
            .get(key)
            .and_then(|entries| entries.get(field))
            .and_then(|address| {
                let (start_routing_bucket, end_routing_bucket) = shard.routing_range();
                read_block_bytes(
                    cache,
                    block_store,
                    shard_id,
                    address,
                    BlockIdentity::of(shard_id, "hash", key, Some(field.as_str())),
                    Some(crate::engine::hashing::block_routing_bucket(
                        key,
                        start_routing_bucket,
                        end_routing_bucket,
                    )),
                )
            })
        else {
            return 0_i64
                .checked_add(*increment)
                .map(|_| ())
                .ok_or_else(|| Status::error("out_of_range", "hash increment overflows i64"));
        };
        let current = parse_i64(&bytes)
            .ok_or_else(|| Status::error("unmatched", "hash value is not an integer"))?;
        current
            .checked_add(*increment)
            .map(|_| ())
            .ok_or_else(|| Status::error("out_of_range", "hash increment overflows i64"))?;
    }
    Ok(())
}

/// Encode a cached response.
///
/// msgpack, not JSON. `CommandResponse::Bytes` carries a `Vec<u8>`, and JSON writes that as an
/// ARRAY OF DECIMAL NUMBERS -- about four characters a byte. Measured: a 4 KiB value cached as
/// 16,410 bytes of text against 4,117 packed, and every HIT paid to parse those characters back.
/// That was the bulk of what a warm read allocated: 15 allocations and 2,788 bytes to serve a
/// 64-byte value, a cost that tracked the ENCODING rather than the value.
///
/// Nothing on disk depends on this: these entries are `put_memory_only`, so the choice is
/// contained to the pair of functions here. A stored entry that does not decode is dropped and
/// recomputed, which is what the JSON path already did -- so an entry written by an older build
/// in the same process costs one recompute, not an error.
/// Reading a cached answer without building it one byte at a time.
///
/// `CommandResponse` is an internally tagged enum (`#[serde(tag = "kind")]`). Serde cannot pick the
/// variant until it has read the tag, so reading one back buffers the whole content first, and a
/// `Vec<u8>` buffers as one element per BYTE at 32 bytes each. Measured on a hit, decoding a stored
/// answer for a 4,096-byte value allocated 135,296 bytes, of which 131,072 is exactly 4,096 x 32.
/// That was 97% of everything a warm read allocated.
///
/// None of it shows in what is stored: the same answer packs into 4,117 bytes, a 1.005x encoding.
/// The cost is entirely on the read side.
///
/// So the tag is read on its own first and borrowed from the packed input, and the payload is then
/// read into a plain struct that carries no tag and so needs no buffering. Matching the tag needs
/// no owned `String`; the returned payload remains the only allocation. What is STORED does not
/// change at all -- these are the same bytes the encoder already wrote -- which matters because
/// the cache admits and evicts by size, and an entry of a different size lives a different life.
#[derive(serde::Deserialize)]
struct CachedKind<'a> {
    #[serde(borrow)]
    kind: &'a str,
}

#[derive(serde::Deserialize)]
struct CachedBytes {
    /// PAIRED WITH `CommandResponse::Bytes::value` AND NOT INDEPENDENT OF IT.
    ///
    /// The decoder reads the tag alone and then re-reads the payload through this narrow mirror, so
    /// this field and the one on the enum are two declarations of a single wire field. If only one
    /// carries the byte-string annotation, this path decodes a shape the encoder never wrote. The
    /// pairing is derived from the codec, not from the field happening to share a name.
    #[serde(with = "crate::types::cached_payload_bytes")]
    value: Option<Vec<u8>>,
}

#[derive(serde::Deserialize)]
struct CachedMembers {
    members: Vec<Vec<u8>>,
}

#[derive(serde::Deserialize)]
struct CachedValues {
    values: Vec<Option<Vec<u8>>>,
}

/// Encode a cached response.
///
/// Nothing on disk depends on this being msgpack rather than anything else, but it does depend on
/// it staying the SAME: these entries are sized by the cache, so changing the encoding changes
/// which of them survive pressure. This writes what it has always written.
pub(super) fn encode_cached_response(response: &CommandResponse) -> Option<Vec<u8>> {
    let mut packed = Vec::new();
    let mut serializer = rmp_serde::Serializer::new(&mut packed).with_struct_map();
    // struct-as-MAP: the internal tag needs the field names present to read back. The array form
    // would drop them.
    serde::Serialize::serialize(response, &mut serializer).ok()?;
    Some(packed)
}

pub(super) fn decode_cached_response(bytes: &[u8]) -> Option<CommandResponse> {
    // Reading the tag on its own steps over the payload instead of building it: the fields this
    // struct does not name are skipped, not materialised.
    let kind = rmp_serde::from_slice::<CachedKind<'_>>(bytes).ok()?.kind;
    match kind {
        "bytes" => Some(CommandResponse::Bytes {
            value: rmp_serde::from_slice::<CachedBytes>(bytes).ok()?.value,
        }),
        "members" => Some(CommandResponse::Members {
            members: rmp_serde::from_slice::<CachedMembers>(bytes).ok()?.members,
        }),
        "values" => Some(CommandResponse::Values {
            values: rmp_serde::from_slice::<CachedValues>(bytes).ok()?.values,
        }),
        // Every other shape carries no bulk payload, so the buffering costs nothing worth avoiding
        // and the whole enum reads back as itself. A variant added later lands here: slower than it
        // could be, never wrong.
        _ => rmp_serde::from_slice::<CommandResponse>(bytes).ok(),
    }
}

pub(super) fn cached_response(
    cache: &MultiLayerCache,
    key: CacheKey,
    source: impl FnOnce() -> CommandResponse,
) -> CommandResponse {
    let cached = crate::alloc_probe::in_class(crate::alloc_probe::AllocClass::CacheRead, || {
        cache.get(&key)
    });
    if let Ok(Some(bytes)) = cached {
        if let Some(response) = decode_cached_response(&bytes) {
            return response;
        }
        let _ = cache.invalidate(&key);
    }
    let response = source();
    if let Some(bytes) = encode_cached_response(&response) {
        cache.put_memory_only(key, bytes);
    }
    response
}

pub(super) fn invalidate_if_cached(cache: &MultiLayerCache, key: CacheKey) {
    if cache.peek(&key) {
        let _ = cache.invalidate(&key);
    }
}
