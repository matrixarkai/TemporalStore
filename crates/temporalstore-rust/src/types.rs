use serde::{Deserialize, Deserializer, Serialize};

pub type ShardId = u64;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Status {
    pub ok: bool,
    pub code: String,
    pub message: String,
}

impl Status {
    pub fn ok() -> Self {
        Self {
            ok: true,
            code: "ok".to_string(),
            message: String::new(),
        }
    }

    pub fn error(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            ok: false,
            code: code.into(),
            message: message.into(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct FeaturePoint {
    #[serde(alias = "ts", alias = "timestamp")]
    pub timestamp_ms: u64,
    #[serde(deserialize_with = "bytes_from_json")]
    pub value: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SequenceFeatureRow {
    #[serde(alias = "ts", alias = "timestamp")]
    pub timestamp_ms: u64,
    pub gid: u64,
    pub action_type: u32,
    pub duration: u32,
    pub author_id: u64,
}

impl SequenceFeatureRow {
    pub fn encode_cpp_feature_value(&self) -> Vec<u8> {
        let mut out = Vec::new();
        encode_varint_field(&mut out, 1, self.gid);
        encode_varint_field(&mut out, 2, self.action_type as u64);
        encode_varint_field(&mut out, 3, self.duration as u64);
        encode_varint_field(&mut out, 4, self.author_id);
        out
    }

    pub fn decode_cpp_feature_value(timestamp_ms: u64, value: &[u8]) -> Option<Self> {
        let mut cursor = 0;
        let mut gid = None;
        let mut action_type = None;
        let mut duration = None;
        let mut author_id = None;
        while cursor < value.len() {
            let tag = decode_varint(value, &mut cursor)?;
            let field = tag >> 3;
            let wire_type = tag & 0x7;
            match (field, wire_type) {
                (1, 0) => gid = Some(decode_varint(value, &mut cursor)?),
                (2, 0) => action_type = u32::try_from(decode_varint(value, &mut cursor)?).ok(),
                (3, 0) => duration = u32::try_from(decode_varint(value, &mut cursor)?).ok(),
                (4, 0) => author_id = Some(decode_varint(value, &mut cursor)?),
                (_, 0) => {
                    let _ = decode_varint(value, &mut cursor)?;
                }
                (_, 1) => cursor = cursor.checked_add(8)?,
                (_, 2) => {
                    let len = usize::try_from(decode_varint(value, &mut cursor)?).ok()?;
                    cursor = cursor.checked_add(len)?;
                }
                (_, 5) => cursor = cursor.checked_add(4)?,
                _ => return None,
            }
            if cursor > value.len() {
                return None;
            }
        }
        Some(Self {
            timestamp_ms,
            gid: gid?,
            action_type: action_type?,
            duration: duration?,
            author_id: author_id?,
        })
    }
}

fn bytes_from_json<'de, D>(deserializer: D) -> Result<Vec<u8>, D::Error>
where
    D: Deserializer<'de>,
{
    let value = serde_json::Value::deserialize(deserializer)?;
    bytes_value(value).map_err(serde::de::Error::custom)
}

fn bytes_value(value: serde_json::Value) -> Result<Vec<u8>, String> {
    match value {
        serde_json::Value::Null => Ok(Vec::new()),
        serde_json::Value::String(value) => Ok(value.into_bytes()),
        serde_json::Value::Array(_) => serde_json::from_value(value).map_err(|err| err.to_string()),
        serde_json::Value::Object(mut object) => {
            if let Some(bytes) = object.remove("bytes") {
                serde_json::from_value(bytes).map_err(|err| err.to_string())
            } else if let Some(value) = object.remove("value") {
                bytes_value(value)
            } else {
                serde_json::to_vec(&serde_json::Value::Object(object))
                    .map_err(|err| err.to_string())
            }
        }
        value => serde_json::to_vec(&value).map_err(|err| err.to_string()),
    }
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum FeatureFilterOp {
    Equal,
    NotEqual,
    GreaterThan,
    GreaterOrEqual,
    LessThan,
    LessOrEqual,
}

impl<'de> Deserialize<'de> for FeatureFilterOp {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = serde_json::Value::deserialize(deserializer)?;
        match value {
            serde_json::Value::Number(number) => match number.as_i64() {
                Some(0) => Ok(Self::Equal),
                Some(1) => Ok(Self::NotEqual),
                Some(2) => Ok(Self::GreaterThan),
                Some(3) => Ok(Self::LessThan),
                Some(4) => Ok(Self::GreaterOrEqual),
                Some(5) => Ok(Self::LessOrEqual),
                _ => Err(serde::de::Error::custom("unknown FeatureFilterOp value")),
            },
            serde_json::Value::String(value) => match value.as_str() {
                "equal" | "Equal" | "EQUAL" | "=" | "==" | "0" => Ok(Self::Equal),
                "not_equal" | "NotEqual" | "NOT_EQUAL" | "!=" | "1" => Ok(Self::NotEqual),
                "greater_than" | "GreaterThan" | "GREATER_THAN" | ">" | "2" => {
                    Ok(Self::GreaterThan)
                }
                "less_than" | "LessThan" | "LESS_THAN" | "<" | "3" => Ok(Self::LessThan),
                "greater_or_equal" | "GreaterOrEqual" | "GREATER_OR_EQUAL" | ">=" | "4" => {
                    Ok(Self::GreaterOrEqual)
                }
                "less_or_equal" | "LessOrEqual" | "LESS_OR_EQUAL" | "<=" | "5" => {
                    Ok(Self::LessOrEqual)
                }
                _ => Err(serde::de::Error::custom("unknown FeatureFilterOp value")),
            },
            _ => Err(serde::de::Error::custom(
                "FeatureFilterOp must be a string or integer",
            )),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum StringSetCondition {
    Always,
    IfExists,
    IfNotExists,
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum FeatureWritePolicy {
    Upsert,
    InsertIfAbsent,
    ReplaceExisting,
    Block,
}

impl<'de> Deserialize<'de> for FeatureWritePolicy {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = serde_json::Value::deserialize(deserializer)?;
        match value {
            serde_json::Value::Number(number) => match number.as_i64() {
                Some(0) => Ok(Self::Upsert),
                Some(1) => Ok(Self::Block),
                Some(2) => Ok(Self::InsertIfAbsent),
                Some(3) => Ok(Self::ReplaceExisting),
                _ => Err(serde::de::Error::custom("unknown WritePolicy value")),
            },
            serde_json::Value::String(value) => match value.as_str() {
                "UPSERT" | "upsert" | "Upsert" | "0" => Ok(Self::Upsert),
                "BLOCK" | "block" | "Block" | "1" => Ok(Self::Block),
                "FIRST" | "first" | "First" | "NX" | "nx" | "2" => Ok(Self::InsertIfAbsent),
                "UPDATE" | "update" | "Update" | "XX" | "xx" | "3" => Ok(Self::ReplaceExisting),
                _ => Err(serde::de::Error::custom("unknown WritePolicy value")),
            },
            _ => Err(serde::de::Error::custom(
                "WritePolicy must be a string or integer",
            )),
        }
    }
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct FeatureFilter {
    pub field: String,
    pub op: FeatureFilterOp,
    pub value: u64,
}

impl FeatureFilter {
    pub fn parse_cpp_filter(value: &str) -> Result<Self, String> {
        let mut parts = value.split_whitespace();
        let field = parts
            .next()
            .ok_or_else(|| "filter must be '<field> <op> <value>'".to_string())?;
        let op = parts
            .next()
            .ok_or_else(|| "filter must be '<field> <op> <value>'".to_string())?;
        let raw_value = parts
            .next()
            .ok_or_else(|| "filter must be '<field> <op> <value>'".to_string())?;
        if parts.next().is_some() {
            return Err("filter must be '<field> <op> <value>'".to_string());
        }
        if !matches!(field, "gid" | "action_type" | "duration" | "author_id") {
            return Err(format!("unknown feature field '{field}'"));
        }
        let op = match op {
            "=" | "==" => FeatureFilterOp::Equal,
            "!=" => FeatureFilterOp::NotEqual,
            ">" => FeatureFilterOp::GreaterThan,
            ">=" => FeatureFilterOp::GreaterOrEqual,
            "<" => FeatureFilterOp::LessThan,
            "<=" => FeatureFilterOp::LessOrEqual,
            _ => return Err(format!("unsupported feature filter op '{op}'")),
        };
        let value = raw_value
            .parse::<u64>()
            .map_err(|_| format!("feature filter value '{raw_value}' is not uint64"))?;
        Ok(Self {
            field: field.to_string(),
            op,
            value,
        })
    }
}

impl<'de> Deserialize<'de> for FeatureFilter {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct StructuredFeatureFilter {
            field: String,
            op: FeatureFilterOp,
            value: u64,
        }

        let value = serde_json::Value::deserialize(deserializer)?;
        match value {
            serde_json::Value::String(filter) => {
                Self::parse_cpp_filter(&filter).map_err(serde::de::Error::custom)
            }
            value => {
                let filter: StructuredFeatureFilter =
                    serde_json::from_value(value).map_err(serde::de::Error::custom)?;
                Ok(Self {
                    field: filter.field,
                    op: filter.op,
                    value: filter.value,
                })
            }
        }
    }
}

pub fn parse_cpp_feature_filters<'a>(
    filters: impl IntoIterator<Item = &'a str>,
) -> Result<Vec<FeatureFilter>, String> {
    let mut parsed = Vec::new();
    for filter in filters
        .into_iter()
        .filter(|filter| !filter.trim().is_empty())
    {
        let filter = FeatureFilter::parse_cpp_filter(filter)?;
        if let Some(index) = parsed
            .iter()
            .position(|existing: &FeatureFilter| existing.field == filter.field)
        {
            parsed[index] = filter;
        } else {
            parsed.push(filter);
        }
    }
    Ok(parsed)
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SequenceQuerySpec {
    pub key: String,
    #[serde(alias = "start_ts")]
    pub start_ms: u64,
    #[serde(alias = "end_ts")]
    pub end_ms: u64,
    #[serde(alias = "limit")]
    pub count: usize,
    #[serde(default)]
    pub filters: Vec<FeatureFilter>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct IpsStats {
    pub total: u64,
    pub first_timestamp_ms: Option<u64>,
    pub last_timestamp_ms: Option<u64>,
    pub action_type_counts: Vec<(u32, u64)>,
    pub table_id_counts: Vec<(u64, u64)>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct IpsSnapshotReport {
    pub key: String,
    pub start_ms: u64,
    pub end_ms: u64,
    pub requested_count: Option<usize>,
    pub returned_count: usize,
    pub total_in_range: u64,
    pub first_timestamp_ms: Option<u64>,
    pub last_timestamp_ms: Option<u64>,
    pub action_type_counts: Vec<(u32, u64)>,
    pub table_id_counts: Vec<(u64, u64)>,
    pub unique_page_ref_count: usize,
    pub packed_timestamped_page_count: usize,
    pub page_segment_ids: Vec<u64>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RiskFamily {
    H,
    Cpc,
    Fol,
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RiskFolType {
    First,
    Last,
}

impl<'de> Deserialize<'de> for RiskFolType {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = serde_json::Value::deserialize(deserializer)?;
        match value {
            serde_json::Value::Number(number) => match number.as_i64() {
                Some(0) => Ok(Self::First),
                Some(1) => Ok(Self::Last),
                _ => Err(serde::de::Error::custom("unknown RiskFolType value")),
            },
            serde_json::Value::String(value) => match value.as_str() {
                "FIRST" | "first" | "First" | "0" => Ok(Self::First),
                "LAST" | "last" | "Last" | "1" => Ok(Self::Last),
                _ => Err(serde::de::Error::custom("unknown RiskFolType value")),
            },
            _ => Err(serde::de::Error::custom(
                "RiskFolType must be a string or integer",
            )),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ContextNode {
    pub node_hash: u64,
    #[serde(default)]
    pub parent_hash: u64,
    #[serde(default)]
    pub kind: u32,
    #[serde(default)]
    pub canonical_name: String,
    #[serde(default)]
    pub l0: String,
    // Deprecated hot-schema field: retained for legacy Rust inputs only.
    #[serde(default, skip_serializing)]
    pub status: u32,
    #[serde(default)]
    pub last_event_time_ms: u64,
    // Deprecated hot-schema field: use ContextSummaryDirtyMarker instead.
    #[serde(default, skip_serializing)]
    pub summary_dirty: bool,
    // Deprecated hot-schema field: use ContextSummary records instead.
    #[serde(default, skip_serializing)]
    pub l1_ref: String,
    // Deprecated hot-schema field: use resource/provenance sidecars instead.
    #[serde(default, skip_serializing)]
    pub raw_metadata_ref: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ContextEvent {
    pub event_id_hash: u64,
    pub event_time_ms: u64,
    #[serde(default)]
    pub ingestion_time_ms: u64,
    // Deprecated hot-schema field: use type/event_type instead.
    #[serde(default, skip_serializing)]
    pub kind: u32,
    #[serde(default, rename = "type", alias = "event_type")]
    pub event_type: u32,
    // Deprecated hot-schema field: C++ reserves this field.
    #[serde(default, skip_serializing)]
    pub actor_hash: u64,
    // Deprecated hot-schema field: use secondary index status_hash instead.
    #[serde(default, skip_serializing)]
    pub status: u32,
    // Deprecated hot-schema field: C++ reserves this field.
    #[serde(default, skip_serializing)]
    pub valid_until_ms: u64,
    #[serde(default)]
    pub confidence: f32,
    #[serde(default)]
    pub importance: f32,
    #[serde(default)]
    pub text: String,
    // Deprecated hot-schema field: use source/resource secondary indexes instead.
    #[serde(default, skip_serializing)]
    pub source_ref: String,
    // Deprecated hot-schema field: use ContextChildRef/link sidecars instead.
    #[serde(default, skip_serializing)]
    pub related_node_hashes: Vec<u64>,
    // Deprecated hot-schema field: use ContextCompressionEvent/debug sidecars instead.
    #[serde(default, skip_serializing)]
    pub compact_attrs: Vec<u8>,
}

impl ContextEvent {
    pub fn primary_time_ms(&self) -> u64 {
        if self.ingestion_time_ms != 0 {
            self.ingestion_time_ms
        } else {
            self.event_time_ms
        }
    }

    pub fn event_type_code(&self) -> u32 {
        #[allow(deprecated)]
        if self.event_type != 0 {
            self.event_type
        } else {
            self.kind
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ContextIndexRef {
    pub primary_node_hash: u64,
    pub primary_event_time_ms: u64,
    #[serde(default)]
    pub event_id_hash: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ContextIndexLookup {
    pub index_name: String,
    pub index_value_hash: u64,
    #[serde(default)]
    pub scope_hash: u64,
    pub start_time_ms: u64,
    pub end_time_ms: u64,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum InternalContextIndex {
    EventKind,
    Entity,
    Status,
    Source,
    EventTimeBucket,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct ContextExtractedEventIndexes {
    #[serde(default)]
    pub scope_hash: u64,
    #[serde(default)]
    pub entity_hashes: Vec<u64>,
    #[serde(default)]
    pub status_hash: u64,
    #[serde(default)]
    pub source_hash: u64,
    #[serde(default)]
    pub event_time_bucket_ms: u64,
    #[serde(default)]
    pub disabled_indexes: Vec<InternalContextIndex>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ContextAuditRef {
    pub node_hash: u64,
    pub event_time_ms: u64,
    #[serde(default)]
    pub reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ContextPackAudit {
    pub query_id: String,
    pub session_hash: u64,
    pub request_time_ms: u64,
    #[serde(default)]
    pub query_hash: u64,
    #[serde(default)]
    pub max_prompt_tokens: u32,
    #[serde(default)]
    pub selected_tokens: u32,
    #[serde(default)]
    pub selected_refs: Vec<ContextAuditRef>,
    #[serde(default)]
    pub blocked_refs: Vec<ContextAuditRef>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ContextSummaryDirtyMarker {
    pub node_hash: u64,
    pub event_time_ms: u64,
    #[serde(default)]
    pub reason: u32,
    #[serde(default)]
    pub propagate_depth: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ContextEntity {
    pub entity_hash: u64,
    pub node_hash: u64,
    #[serde(default, rename = "type", alias = "entity_type")]
    pub entity_type: u32,
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub value: String,
    pub updated_at_ms: u64,
    #[serde(default)]
    pub valid_from_ms: u64,
    #[serde(default)]
    pub confidence: f32,
    #[serde(default)]
    pub source_event_hashes: Vec<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ContextChildRef {
    pub parent_hash: u64,
    pub child_hash: u64,
    pub updated_at_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ContextEmbedding {
    pub ref_hash: u64,
    pub level: u32,
    #[serde(default)]
    pub model_hash: u64,
    #[serde(default)]
    pub vector: Vec<f32>,
    pub updated_at_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ContextSummary {
    pub node_hash: u64,
    pub level: u32,
    #[serde(default)]
    pub text: String,
    pub valid_from_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ContextCompressionEvent {
    pub compression_id_hash: u64,
    pub node_hash: u64,
    pub source_start_ms: u64,
    pub source_end_ms: u64,
    pub compressed_time_ms: u64,
    #[serde(default)]
    pub summary: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ContextTraversedNode {
    pub node_hash: u64,
    pub depth: u32,
    pub score: f32,
}

pub type ContextNodeModel = ContextNode;
pub type ContextEventModel = ContextEvent;
pub type ContextSegment = ContextEvent;
pub type ContextIndexModel = ContextIndexRef;
pub type ContextAuditModel = ContextPackAudit;
pub type ContextDirtyModel = ContextSummaryDirtyMarker;
pub type ContextChildModel = ContextChildRef;
pub type ContextEmbeddingModel = ContextEmbedding;
pub type ContextSummaryModel = ContextSummary;
pub type ContextCompressionModel = ContextCompressionEvent;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ContextModelDescriptor {
    pub model_id: u8,
    pub name: String,
    pub key_family: String,
    pub page_primitive: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub aliases: Vec<String>,
}

pub const CONTEXT_NODE_MODEL_ID: u8 = 9;
pub const CONTEXT_EVENT_MODEL_ID: u8 = 10;
pub const CONTEXT_INDEX_MODEL_ID: u8 = 11;
pub const CONTEXT_AUDIT_MODEL_ID: u8 = 12;
pub const CONTEXT_DIRTY_MODEL_ID: u8 = 13;
pub const CONTEXT_CHILD_MODEL_ID: u8 = 14;
pub const CONTEXT_EMBEDDING_MODEL_ID: u8 = 15;
pub const CONTEXT_SUMMARY_MODEL_ID: u8 = 16;
pub const CONTEXT_COMPRESSION_MODEL_ID: u8 = 17;
pub const CONTEXT_ENTITY_MODEL_ID: u8 = 18;

fn context_model_descriptor_entry(
    model_id: u8,
    name: &str,
    key_family: &str,
    page_primitive: &str,
    aliases: &[&str],
) -> ContextModelDescriptor {
    ContextModelDescriptor {
        model_id,
        name: name.to_string(),
        key_family: key_family.to_string(),
        page_primitive: page_primitive.to_string(),
        aliases: aliases.iter().map(|alias| alias.to_string()).collect(),
    }
}

pub fn context_model_descriptors() -> Vec<ContextModelDescriptor> {
    vec![
        context_model_descriptor_entry(
            CONTEXT_NODE_MODEL_ID,
            "ContextNodeModel",
            "ctx:node",
            "HashOrSet<std::string,std::string>",
            &["ContextNode", "ctxnode"],
        ),
        context_model_descriptor_entry(
            CONTEXT_EVENT_MODEL_ID,
            "ContextEventModel",
            "ctx:event",
            "FeatureOrSet",
            &["ContextEvent", "ContextSegment", "ctxevent", "ctxsegment"],
        ),
        context_model_descriptor_entry(
            CONTEXT_INDEX_MODEL_ID,
            "ContextIndexModel",
            "ctxidx",
            "FeatureOrSet",
            &["ContextIndex", "ContextIndexRef", "ctx:index"],
        ),
        context_model_descriptor_entry(
            CONTEXT_AUDIT_MODEL_ID,
            "ContextAuditModel",
            "ctx:audit",
            "FeatureOrSet",
            &["ContextAudit", "ContextPackAudit"],
        ),
        context_model_descriptor_entry(
            CONTEXT_DIRTY_MODEL_ID,
            "ContextDirtyModel",
            "ctx:dirty",
            "FeatureOrSet",
            &["ContextDirty", "ContextSummaryDirtyMarker"],
        ),
        context_model_descriptor_entry(
            CONTEXT_CHILD_MODEL_ID,
            "ContextChildModel",
            "ctx:child",
            "FeatureOrSet",
            &["ContextChild", "ContextChildRef"],
        ),
        context_model_descriptor_entry(
            CONTEXT_EMBEDDING_MODEL_ID,
            "ContextEmbeddingModel",
            "ctx:embedding",
            "HashOrSet<std::string,std::string>",
            &["ContextEmbedding"],
        ),
        context_model_descriptor_entry(
            CONTEXT_SUMMARY_MODEL_ID,
            "ContextSummaryModel",
            "ctx:summary",
            "FeatureOrSet",
            &["ContextSummary"],
        ),
        context_model_descriptor_entry(
            CONTEXT_COMPRESSION_MODEL_ID,
            "ContextCompressionModel",
            "ctx:compress",
            "FeatureOrSet",
            &[
                "ContextCompression",
                "ContextCompressionEvent",
                "ctx:compression",
            ],
        ),
        context_model_descriptor_entry(
            CONTEXT_ENTITY_MODEL_ID,
            "ContextEntityModel",
            "ctx:entity",
            "HashOrSet<std::string,std::string>",
            &["ContextEntity"],
        ),
    ]
}

pub fn context_model_descriptor(selector: &str) -> Option<ContextModelDescriptor> {
    let selector = selector.trim();
    let selector_lower = selector.to_ascii_lowercase();
    let selector_model_id = selector.parse::<u8>().ok();
    context_model_descriptors().into_iter().find(|descriptor| {
        Some(descriptor.model_id) == selector_model_id
            || descriptor.name.eq_ignore_ascii_case(selector)
            || descriptor.key_family.eq_ignore_ascii_case(selector)
            || descriptor
                .aliases
                .iter()
                .any(|alias| alias.eq_ignore_ascii_case(selector))
            || descriptor
                .aliases
                .iter()
                .any(|alias| alias.to_ascii_lowercase() == selector_lower)
    })
}

pub trait ContextWire: Sized + Serialize + for<'de> Deserialize<'de> {
    fn encode_cpp_context_value(&self) -> Vec<u8>;
    fn decode_cpp_context_value(bytes: &[u8]) -> Option<Self>;

    fn encode_context_value(&self) -> Vec<u8> {
        self.encode_cpp_context_value()
    }

    fn decode_context_value(bytes: &[u8]) -> Option<Self> {
        Self::decode_cpp_context_value(bytes).or_else(|| serde_json::from_slice(bytes).ok())
    }
}

impl ContextWire for ContextNode {
    fn encode_cpp_context_value(&self) -> Vec<u8> {
        let mut out = Vec::new();
        encode_varint_field(&mut out, 1, self.node_hash);
        encode_varint_field(&mut out, 2, self.parent_hash);
        encode_varint_field(&mut out, 3, u64::from(self.kind));
        encode_bytes_field(&mut out, 4, self.canonical_name.as_bytes());
        encode_bytes_field(&mut out, 5, self.l0.as_bytes());
        encode_varint_field(&mut out, 6, self.last_event_time_ms);
        if !self.raw_metadata_ref.is_empty() {
            encode_bytes_field(&mut out, 10, self.raw_metadata_ref.as_bytes());
        }
        out
    }

    fn decode_cpp_context_value(bytes: &[u8]) -> Option<Self> {
        let mut cursor = 0;
        let mut value = Self {
            node_hash: 0,
            parent_hash: 0,
            kind: 0,
            canonical_name: String::new(),
            l0: String::new(),
            status: 0,
            last_event_time_ms: 0,
            summary_dirty: false,
            l1_ref: String::new(),
            raw_metadata_ref: String::new(),
        };
        while cursor < bytes.len() {
            let tag = decode_varint(bytes, &mut cursor)?;
            match (tag >> 3, tag & 0x7) {
                (1, 0) => value.node_hash = decode_varint(bytes, &mut cursor)?,
                (2, 0) => value.parent_hash = decode_varint(bytes, &mut cursor)?,
                (3, 0) => value.kind = u32::try_from(decode_varint(bytes, &mut cursor)?).ok()?,
                (4, 2) => value.canonical_name = decode_string(bytes, &mut cursor)?,
                (5, 2) => value.l0 = decode_string(bytes, &mut cursor)?,
                (6, 0) => value.last_event_time_ms = decode_varint(bytes, &mut cursor)?,
                // Legacy Rust-only node fields from pre-trim builds.
                (7, 0) => value.last_event_time_ms = decode_varint(bytes, &mut cursor)?,
                (8, 0) => value.summary_dirty = decode_varint(bytes, &mut cursor)? != 0,
                (9, 2) => value.l1_ref = decode_string(bytes, &mut cursor)?,
                (10, 2) => value.raw_metadata_ref = decode_string(bytes, &mut cursor)?,
                (_, wire_type) => skip_proto_field(bytes, &mut cursor, wire_type)?,
            }
        }
        Some(value)
    }
}

impl ContextWire for ContextEvent {
    fn encode_cpp_context_value(&self) -> Vec<u8> {
        let mut out = Vec::new();
        encode_varint_field(&mut out, 1, self.event_id_hash);
        encode_varint_field(&mut out, 2, self.event_time_ms);
        encode_varint_field(&mut out, 3, u64::from(self.event_type_code()));
        encode_fixed32_field(&mut out, 6, self.confidence.to_bits());
        encode_fixed32_field(&mut out, 7, self.importance.to_bits());
        encode_bytes_field(&mut out, 8, self.text.as_bytes());
        encode_varint_field(&mut out, 9, self.primary_time_ms());
        out
    }

    fn decode_cpp_context_value(bytes: &[u8]) -> Option<Self> {
        let mut cursor = 0;
        let mut value = Self {
            event_id_hash: 0,
            event_time_ms: 0,
            ingestion_time_ms: 0,
            kind: 0,
            event_type: 0,
            actor_hash: 0,
            status: 0,
            valid_until_ms: 0,
            confidence: 0.0,
            importance: 0.0,
            text: String::new(),
            source_ref: String::new(),
            related_node_hashes: Vec::new(),
            compact_attrs: Vec::new(),
        };
        while cursor < bytes.len() {
            let tag = decode_varint(bytes, &mut cursor)?;
            match (tag >> 3, tag & 0x7) {
                (1, 0) => value.event_id_hash = decode_varint(bytes, &mut cursor)?,
                (2, 0) => value.event_time_ms = decode_varint(bytes, &mut cursor)?,
                (3, 0) => {
                    let event_type = u32::try_from(decode_varint(bytes, &mut cursor)?).ok()?;
                    value.event_type = event_type;
                }
                // Legacy Rust-only event fields from pre-trim builds.
                (4, 0) => {
                    value.event_type = u32::try_from(decode_varint(bytes, &mut cursor)?).ok()?
                }
                (5, 0) => value.actor_hash = decode_varint(bytes, &mut cursor)?,
                (6, 0) => value.status = u32::try_from(decode_varint(bytes, &mut cursor)?).ok()?,
                (6, 5) => value.confidence = f32::from_bits(decode_fixed32(bytes, &mut cursor)?),
                (7, 0) => value.valid_until_ms = decode_varint(bytes, &mut cursor)?,
                (7, 5) => value.importance = f32::from_bits(decode_fixed32(bytes, &mut cursor)?),
                (8, 2) => value.text = decode_string(bytes, &mut cursor)?,
                (8, 5) => value.confidence = f32::from_bits(decode_fixed32(bytes, &mut cursor)?),
                (9, 0) => value.ingestion_time_ms = decode_varint(bytes, &mut cursor)?,
                (9, 5) => value.importance = f32::from_bits(decode_fixed32(bytes, &mut cursor)?),
                (10, 2) => value.text = decode_string(bytes, &mut cursor)?,
                (11, 2) => value.source_ref = decode_string(bytes, &mut cursor)?,
                (12, 0) => value
                    .related_node_hashes
                    .push(decode_varint(bytes, &mut cursor)?),
                (12, 2) => {
                    let packed = decode_bytes(bytes, &mut cursor)?;
                    let mut packed_cursor = 0;
                    while packed_cursor < packed.len() {
                        value
                            .related_node_hashes
                            .push(decode_varint(&packed, &mut packed_cursor)?);
                    }
                }
                (13, 2) => value.compact_attrs = decode_bytes(bytes, &mut cursor)?,
                (_, wire_type) => skip_proto_field(bytes, &mut cursor, wire_type)?,
            }
        }
        if value.ingestion_time_ms == 0 {
            value.ingestion_time_ms = value.event_time_ms;
        }
        if value.event_type == 0 {
            value.event_type = value.kind;
        }
        Some(value)
    }
}

impl ContextWire for ContextIndexRef {
    fn encode_cpp_context_value(&self) -> Vec<u8> {
        let mut out = Vec::new();
        encode_varint_field(&mut out, 1, self.primary_node_hash);
        encode_varint_field(&mut out, 2, self.primary_event_time_ms);
        encode_varint_field(&mut out, 3, self.event_id_hash);
        out
    }

    fn decode_cpp_context_value(bytes: &[u8]) -> Option<Self> {
        let mut cursor = 0;
        let mut value = Self {
            primary_node_hash: 0,
            primary_event_time_ms: 0,
            event_id_hash: 0,
        };
        while cursor < bytes.len() {
            let tag = decode_varint(bytes, &mut cursor)?;
            match (tag >> 3, tag & 0x7) {
                (1, 0) => value.primary_node_hash = decode_varint(bytes, &mut cursor)?,
                (2, 0) => value.primary_event_time_ms = decode_varint(bytes, &mut cursor)?,
                (3, 0) => value.event_id_hash = decode_varint(bytes, &mut cursor)?,
                (_, wire_type) => skip_proto_field(bytes, &mut cursor, wire_type)?,
            }
        }
        Some(value)
    }
}

impl ContextWire for ContextAuditRef {
    fn encode_cpp_context_value(&self) -> Vec<u8> {
        let mut out = Vec::new();
        encode_varint_field(&mut out, 1, self.node_hash);
        encode_varint_field(&mut out, 2, self.event_time_ms);
        encode_bytes_field(&mut out, 3, self.reason.as_bytes());
        out
    }

    fn decode_cpp_context_value(bytes: &[u8]) -> Option<Self> {
        let mut cursor = 0;
        let mut value = Self {
            node_hash: 0,
            event_time_ms: 0,
            reason: String::new(),
        };
        while cursor < bytes.len() {
            let tag = decode_varint(bytes, &mut cursor)?;
            match (tag >> 3, tag & 0x7) {
                (1, 0) => value.node_hash = decode_varint(bytes, &mut cursor)?,
                (2, 0) => value.event_time_ms = decode_varint(bytes, &mut cursor)?,
                (3, 2) => value.reason = decode_string(bytes, &mut cursor)?,
                (_, wire_type) => skip_proto_field(bytes, &mut cursor, wire_type)?,
            }
        }
        Some(value)
    }
}

impl ContextWire for ContextPackAudit {
    fn encode_cpp_context_value(&self) -> Vec<u8> {
        let mut out = Vec::new();
        encode_bytes_field(&mut out, 1, self.query_id.as_bytes());
        encode_varint_field(&mut out, 2, self.session_hash);
        encode_varint_field(&mut out, 3, self.request_time_ms);
        encode_varint_field(&mut out, 4, self.query_hash);
        encode_varint_field(&mut out, 5, u64::from(self.max_prompt_tokens));
        encode_varint_field(&mut out, 6, u64::from(self.selected_tokens));
        for selected in &self.selected_refs {
            encode_bytes_field(&mut out, 7, &selected.encode_cpp_context_value());
        }
        for blocked in &self.blocked_refs {
            encode_bytes_field(&mut out, 8, &blocked.encode_cpp_context_value());
        }
        out
    }

    fn decode_cpp_context_value(bytes: &[u8]) -> Option<Self> {
        let mut cursor = 0;
        let mut value = Self {
            query_id: String::new(),
            session_hash: 0,
            request_time_ms: 0,
            query_hash: 0,
            max_prompt_tokens: 0,
            selected_tokens: 0,
            selected_refs: Vec::new(),
            blocked_refs: Vec::new(),
        };
        while cursor < bytes.len() {
            let tag = decode_varint(bytes, &mut cursor)?;
            match (tag >> 3, tag & 0x7) {
                (1, 2) => value.query_id = decode_string(bytes, &mut cursor)?,
                (2, 0) => value.session_hash = decode_varint(bytes, &mut cursor)?,
                (3, 0) => value.request_time_ms = decode_varint(bytes, &mut cursor)?,
                (4, 0) => value.query_hash = decode_varint(bytes, &mut cursor)?,
                (5, 0) => {
                    value.max_prompt_tokens =
                        u32::try_from(decode_varint(bytes, &mut cursor)?).ok()?
                }
                (6, 0) => {
                    value.selected_tokens =
                        u32::try_from(decode_varint(bytes, &mut cursor)?).ok()?
                }
                (7, 2) => value
                    .selected_refs
                    .push(ContextAuditRef::decode_context_value(&decode_bytes(
                        bytes,
                        &mut cursor,
                    )?)?),
                (8, 2) => value
                    .blocked_refs
                    .push(ContextAuditRef::decode_context_value(&decode_bytes(
                        bytes,
                        &mut cursor,
                    )?)?),
                (_, wire_type) => skip_proto_field(bytes, &mut cursor, wire_type)?,
            }
        }
        Some(value)
    }
}

impl ContextWire for ContextSummaryDirtyMarker {
    fn encode_cpp_context_value(&self) -> Vec<u8> {
        let mut out = Vec::new();
        encode_varint_field(&mut out, 1, self.node_hash);
        encode_varint_field(&mut out, 2, self.event_time_ms);
        encode_varint_field(&mut out, 3, u64::from(self.reason));
        encode_varint_field(&mut out, 4, u64::from(self.propagate_depth));
        out
    }

    fn decode_cpp_context_value(bytes: &[u8]) -> Option<Self> {
        let mut cursor = 0;
        let mut value = Self {
            node_hash: 0,
            event_time_ms: 0,
            reason: 0,
            propagate_depth: 0,
        };
        while cursor < bytes.len() {
            let tag = decode_varint(bytes, &mut cursor)?;
            match (tag >> 3, tag & 0x7) {
                (1, 0) => value.node_hash = decode_varint(bytes, &mut cursor)?,
                (2, 0) => value.event_time_ms = decode_varint(bytes, &mut cursor)?,
                (3, 0) => value.reason = u32::try_from(decode_varint(bytes, &mut cursor)?).ok()?,
                (4, 0) => {
                    value.propagate_depth =
                        u32::try_from(decode_varint(bytes, &mut cursor)?).ok()?
                }
                (_, wire_type) => skip_proto_field(bytes, &mut cursor, wire_type)?,
            }
        }
        Some(value)
    }
}

impl ContextWire for ContextEntity {
    fn encode_cpp_context_value(&self) -> Vec<u8> {
        let mut out = Vec::new();
        encode_varint_field(&mut out, 1, self.entity_hash);
        encode_varint_field(&mut out, 2, self.node_hash);
        encode_varint_field(&mut out, 3, u64::from(self.entity_type));
        encode_bytes_field(&mut out, 4, self.name.as_bytes());
        encode_bytes_field(&mut out, 5, self.value.as_bytes());
        encode_varint_field(&mut out, 6, self.updated_at_ms);
        encode_varint_field(&mut out, 7, self.valid_from_ms);
        encode_fixed32_field(&mut out, 8, self.confidence.to_bits());
        for source_event_hash in &self.source_event_hashes {
            encode_varint_field(&mut out, 9, *source_event_hash);
        }
        out
    }

    fn decode_cpp_context_value(bytes: &[u8]) -> Option<Self> {
        let mut cursor = 0;
        let mut value = Self {
            entity_hash: 0,
            node_hash: 0,
            entity_type: 0,
            name: String::new(),
            value: String::new(),
            updated_at_ms: 0,
            valid_from_ms: 0,
            confidence: 0.0,
            source_event_hashes: Vec::new(),
        };
        while cursor < bytes.len() {
            let tag = decode_varint(bytes, &mut cursor)?;
            match (tag >> 3, tag & 0x7) {
                (1, 0) => value.entity_hash = decode_varint(bytes, &mut cursor)?,
                (2, 0) => value.node_hash = decode_varint(bytes, &mut cursor)?,
                (3, 0) => {
                    value.entity_type = u32::try_from(decode_varint(bytes, &mut cursor)?).ok()?
                }
                (4, 2) => value.name = decode_string(bytes, &mut cursor)?,
                (5, 2) => value.value = decode_string(bytes, &mut cursor)?,
                (6, 0) => value.updated_at_ms = decode_varint(bytes, &mut cursor)?,
                (7, 0) => value.valid_from_ms = decode_varint(bytes, &mut cursor)?,
                (8, 5) => value.confidence = f32::from_bits(decode_fixed32(bytes, &mut cursor)?),
                (9, 0) => value
                    .source_event_hashes
                    .push(decode_varint(bytes, &mut cursor)?),
                (9, 2) => {
                    let packed = decode_bytes(bytes, &mut cursor)?;
                    let mut packed_cursor = 0;
                    while packed_cursor < packed.len() {
                        value
                            .source_event_hashes
                            .push(decode_varint(&packed, &mut packed_cursor)?);
                    }
                }
                (_, wire_type) => skip_proto_field(bytes, &mut cursor, wire_type)?,
            }
        }
        Some(value)
    }
}

impl ContextWire for ContextChildRef {
    fn encode_cpp_context_value(&self) -> Vec<u8> {
        let mut out = Vec::new();
        encode_varint_field(&mut out, 1, self.parent_hash);
        encode_varint_field(&mut out, 2, self.child_hash);
        encode_varint_field(&mut out, 5, self.updated_at_ms);
        out
    }

    fn decode_cpp_context_value(bytes: &[u8]) -> Option<Self> {
        let mut cursor = 0;
        let mut value = Self {
            parent_hash: 0,
            child_hash: 0,
            updated_at_ms: 0,
        };
        while cursor < bytes.len() {
            let tag = decode_varint(bytes, &mut cursor)?;
            match (tag >> 3, tag & 0x7) {
                (1, 0) => value.parent_hash = decode_varint(bytes, &mut cursor)?,
                (2, 0) => value.child_hash = decode_varint(bytes, &mut cursor)?,
                (5, 0) => value.updated_at_ms = decode_varint(bytes, &mut cursor)?,
                (_, wire_type) => skip_proto_field(bytes, &mut cursor, wire_type)?,
            }
        }
        Some(value)
    }
}

impl ContextWire for ContextEmbedding {
    fn encode_cpp_context_value(&self) -> Vec<u8> {
        let mut out = Vec::new();
        encode_varint_field(&mut out, 1, self.ref_hash);
        encode_varint_field(&mut out, 2, u64::from(self.level));
        encode_varint_field(&mut out, 3, self.model_hash);
        for value in &self.vector {
            encode_fixed32_field(&mut out, 4, value.to_bits());
        }
        encode_varint_field(&mut out, 5, self.updated_at_ms);
        out
    }

    fn decode_cpp_context_value(bytes: &[u8]) -> Option<Self> {
        let mut cursor = 0;
        let mut value = Self {
            ref_hash: 0,
            level: 0,
            model_hash: 99,
            vector: Vec::new(),
            updated_at_ms: 0,
        };
        while cursor < bytes.len() {
            let tag = decode_varint(bytes, &mut cursor)?;
            match (tag >> 3, tag & 0x7) {
                (1, 0) => value.ref_hash = decode_varint(bytes, &mut cursor)?,
                (2, 0) => value.level = u32::try_from(decode_varint(bytes, &mut cursor)?).ok()?,
                (3, 0) => value.model_hash = decode_varint(bytes, &mut cursor)?,
                (4, 5) => value
                    .vector
                    .push(f32::from_bits(decode_fixed32(bytes, &mut cursor)?)),
                (4, 2) => {
                    let packed = decode_bytes(bytes, &mut cursor)?;
                    let mut packed_cursor = 0;
                    while packed_cursor < packed.len() {
                        value
                            .vector
                            .push(f32::from_bits(decode_fixed32(&packed, &mut packed_cursor)?));
                    }
                }
                (5, 0) => value.updated_at_ms = decode_varint(bytes, &mut cursor)?,
                (_, wire_type) => skip_proto_field(bytes, &mut cursor, wire_type)?,
            }
        }
        Some(value)
    }
}

impl ContextWire for ContextSummary {
    fn encode_cpp_context_value(&self) -> Vec<u8> {
        let mut out = Vec::new();
        encode_varint_field(&mut out, 2, self.node_hash);
        encode_varint_field(&mut out, 3, u64::from(self.level));
        encode_bytes_field(&mut out, 4, self.text.as_bytes());
        encode_varint_field(&mut out, 5, self.valid_from_ms);
        out
    }

    fn decode_cpp_context_value(bytes: &[u8]) -> Option<Self> {
        let mut cursor = 0;
        let mut value = Self {
            node_hash: 0,
            level: 0,
            text: String::new(),
            valid_from_ms: 0,
        };
        while cursor < bytes.len() {
            let tag = decode_varint(bytes, &mut cursor)?;
            match (tag >> 3, tag & 0x7) {
                (2, 0) => value.node_hash = decode_varint(bytes, &mut cursor)?,
                (3, 0) => value.level = u32::try_from(decode_varint(bytes, &mut cursor)?).ok()?,
                (4, 2) => value.text = decode_string(bytes, &mut cursor)?,
                (5, 0) => value.valid_from_ms = decode_varint(bytes, &mut cursor)?,
                (_, wire_type) => skip_proto_field(bytes, &mut cursor, wire_type)?,
            }
        }
        Some(value)
    }
}

impl ContextWire for ContextCompressionEvent {
    fn encode_cpp_context_value(&self) -> Vec<u8> {
        let mut out = Vec::new();
        encode_varint_field(&mut out, 1, self.compression_id_hash);
        encode_varint_field(&mut out, 2, self.node_hash);
        encode_varint_field(&mut out, 3, self.source_start_ms);
        encode_varint_field(&mut out, 4, self.source_end_ms);
        encode_varint_field(&mut out, 5, self.compressed_time_ms);
        encode_bytes_field(&mut out, 6, self.summary.as_bytes());
        out
    }

    fn decode_cpp_context_value(bytes: &[u8]) -> Option<Self> {
        let mut cursor = 0;
        let mut value = Self {
            compression_id_hash: 0,
            node_hash: 0,
            source_start_ms: 0,
            source_end_ms: 0,
            compressed_time_ms: 0,
            summary: String::new(),
        };
        while cursor < bytes.len() {
            let tag = decode_varint(bytes, &mut cursor)?;
            match (tag >> 3, tag & 0x7) {
                (1, 0) => value.compression_id_hash = decode_varint(bytes, &mut cursor)?,
                (2, 0) => value.node_hash = decode_varint(bytes, &mut cursor)?,
                (3, 0) => value.source_start_ms = decode_varint(bytes, &mut cursor)?,
                (4, 0) => value.source_end_ms = decode_varint(bytes, &mut cursor)?,
                (5, 0) => value.compressed_time_ms = decode_varint(bytes, &mut cursor)?,
                (6, 2) => value.summary = decode_string(bytes, &mut cursor)?,
                (_, wire_type) => skip_proto_field(bytes, &mut cursor, wire_type)?,
            }
        }
        Some(value)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Command {
    CommonDelete {
        key: String,
    },
    CommonExpire {
        key: String,
        ttl_ms: u64,
    },
    CommonTtl {
        key: String,
    },
    CommonExists {
        key: String,
    },
    StringSet {
        key: String,
        value: Vec<u8>,
    },
    StringSetEx {
        key: String,
        value: Vec<u8>,
        ttl_ms: u64,
    },
    StringSetConditional {
        key: String,
        value: Vec<u8>,
        #[serde(default)]
        ttl_ms: Option<u64>,
        condition: StringSetCondition,
        return_old: bool,
    },
    StringGet {
        key: String,
    },
    StringDelete {
        key: String,
    },
    HashSet {
        key: String,
        field: String,
        value: Vec<u8>,
    },
    HashGet {
        key: String,
        field: String,
    },
    HashMultiGet {
        key: String,
        fields: Vec<String>,
    },
    HashMultiSet {
        key: String,
        entries: Vec<(String, Vec<u8>)>,
    },
    HashIncrBy {
        key: String,
        field: String,
        increment: i64,
    },
    HashGetAll {
        key: String,
    },
    HashLen {
        key: String,
    },
    HashDelete {
        key: String,
        field: String,
    },
    SetAdd {
        key: String,
        member: Vec<u8>,
    },
    SetMembers {
        key: String,
    },
    SetRemove {
        key: String,
        member: Vec<u8>,
    },
    FeatureAppend {
        key: String,
        points: Vec<FeaturePoint>,
    },
    FeatureAppendWithPolicy {
        key: String,
        points: Vec<FeaturePoint>,
        policy: FeatureWritePolicy,
    },
    FeatureQuery {
        key: String,
        start_ms: u64,
        end_ms: u64,
        #[serde(default)]
        count: Option<usize>,
    },
    FeatureQueryFiltered {
        key: String,
        start_ms: u64,
        end_ms: u64,
        #[serde(default)]
        count: Option<usize>,
        #[serde(default)]
        filters: Vec<FeatureFilter>,
    },
    FeatureReplace {
        key: String,
        start_ms: u64,
        end_ms: u64,
        points: Vec<FeaturePoint>,
    },
    FeatureDelete {
        key: String,
    },
    FeatureAggQuery {
        key: String,
        start_ms: u64,
        end_ms: u64,
        aggregator: String,
        #[serde(default)]
        count: Option<usize>,
    },
    SequenceAdd {
        key: String,
        rows: Vec<SequenceFeatureRow>,
    },
    SequenceAddWithPolicy {
        key: String,
        rows: Vec<SequenceFeatureRow>,
        policy: FeatureWritePolicy,
    },
    SequenceQuery {
        key: String,
        start_ms: u64,
        end_ms: u64,
        count: usize,
        #[serde(default)]
        filters: Vec<FeatureFilter>,
    },
    SequenceBatchQuery {
        queries: Vec<SequenceQuerySpec>,
    },
    IpsAdd {
        key: String,
        timestamp_ms: u64,
        instance: Vec<u8>,
    },
    IpsAddWithOptions {
        key: String,
        timestamp_ms: u64,
        instance: Vec<u8>,
        #[serde(default)]
        action_type: Option<u32>,
        #[serde(default)]
        table_id: Option<u64>,
        #[serde(default)]
        request_id: Option<String>,
    },
    IpsLoad {
        key: String,
        points: Vec<FeaturePoint>,
    },
    IpsQueryLast {
        key: String,
        count: usize,
    },
    IpsQueryRange {
        key: String,
        start_ms: u64,
        end_ms: u64,
        #[serde(default)]
        count: Option<usize>,
    },
    IpsBatchQueryLast {
        keys: Vec<String>,
        count: usize,
    },
    IpsRemove {
        key: String,
        timestamp_ms: u64,
    },
    IpsDelete {
        key: String,
    },
    IpsCount {
        key: String,
        start_ms: u64,
        end_ms: u64,
    },
    IpsQueryRangeWithOptions {
        key: String,
        start_ms: u64,
        end_ms: u64,
        #[serde(default)]
        count: Option<usize>,
        #[serde(default)]
        action_type: Option<u32>,
        #[serde(default)]
        table_id: Option<u64>,
    },
    IpsSnapshot {
        key: String,
        start_ms: u64,
        end_ms: u64,
        #[serde(default)]
        count: Option<usize>,
    },
    IpsSnapshotReport {
        key: String,
        start_ms: u64,
        end_ms: u64,
        #[serde(default)]
        count: Option<usize>,
    },
    IpsStat {
        key: String,
        start_ms: u64,
        end_ms: u64,
    },
    IpsFilter {
        key: String,
        start_ms: u64,
        end_ms: u64,
        #[serde(default)]
        count: Option<usize>,
        #[serde(default)]
        action_type: Option<u32>,
        #[serde(default)]
        table_id: Option<u64>,
    },
    RiskIncrement {
        key: String,
        timestamp_ms: u64,
        amount: i64,
    },
    RiskIncrementWithOptions {
        key: String,
        timestamp_ms: u64,
        amount: i64,
        #[serde(default)]
        precision_ms: Option<u64>,
        #[serde(default)]
        ttl_ms: Option<u64>,
    },
    RiskChangeAdd {
        key: String,
        timestamp_ms: u64,
        value: Vec<u8>,
        #[serde(default)]
        precision_ms: Option<u64>,
        #[serde(default)]
        ttl_ms: Option<u64>,
    },
    RiskCount {
        key: String,
        start_ms: u64,
        end_ms: u64,
    },
    RiskQuery {
        key: String,
        start_ms: u64,
        end_ms: u64,
        aggregator: String,
    },
    RiskDetail {
        key: String,
        start_ms: u64,
        end_ms: u64,
        #[serde(default)]
        count: Option<usize>,
    },
    RiskSet {
        family: RiskFamily,
        key: String,
        timestamp_ms: u64,
        amount: i64,
    },
    RiskSetAndGet {
        family: RiskFamily,
        key: String,
        timestamp_ms: u64,
        amount: i64,
        start_ms: u64,
        end_ms: u64,
        aggregator: String,
    },
    RiskFamilyQuery {
        family: RiskFamily,
        key: String,
        start_ms: u64,
        end_ms: u64,
        aggregator: String,
    },
    RiskFolSet {
        key: String,
        value: Vec<u8>,
        occur_time_ms: u64,
        ttl_ms: u64,
        fol_type: RiskFolType,
    },
    RiskFolQuery {
        key: String,
    },
    RiskManager {
        key: String,
    },
    RiskDebug {
        key: String,
        start_ms: u64,
        end_ms: u64,
    },
    ContextUpsertNode {
        tenant_hash: u64,
        node: ContextNode,
    },
    ContextGetNode {
        tenant_hash: u64,
        node_hash: u64,
    },
    ContextGetNodes {
        tenant_hash: u64,
        node_hashes: Vec<u64>,
    },
    ContextWriteEvent {
        tenant_hash: u64,
        node_hash: u64,
        event: ContextEvent,
        #[serde(default)]
        first_write_only: bool,
        #[serde(default)]
        cold_storage: bool,
    },
    ContextWriteExtractedEvent {
        tenant_hash: u64,
        node_hash: u64,
        event: ContextEvent,
        #[serde(default)]
        indexes: ContextExtractedEventIndexes,
        #[serde(default)]
        first_write_only: bool,
        #[serde(default)]
        cold_storage: bool,
    },
    ContextQueryEvents {
        tenant_hash: u64,
        node_hash: u64,
        start_time_ms: u64,
        end_time_ms: u64,
        #[serde(default)]
        limit: Option<usize>,
        #[serde(default)]
        current_valid_only: bool,
        #[serde(default)]
        as_of_ms: u64,
        #[serde(default)]
        kinds: Vec<u32>,
        #[serde(default)]
        statuses: Vec<u32>,
        #[serde(default)]
        min_confidence: f32,
        #[serde(default)]
        min_importance: f32,
    },
    ContextWriteIndexRef {
        tenant_hash: u64,
        index_name: String,
        index_value_hash: u64,
        #[serde(default)]
        scope_hash: u64,
        event_time_ms: u64,
        index_ref: ContextIndexRef,
    },
    ContextQueryIndex {
        tenant_hash: u64,
        index_name: String,
        index_value_hash: u64,
        #[serde(default)]
        scope_hash: u64,
        start_time_ms: u64,
        end_time_ms: u64,
        #[serde(default)]
        limit: Option<usize>,
    },
    ContextQueryIndexIntersection {
        tenant_hash: u64,
        predicates: Vec<ContextIndexLookup>,
        #[serde(default)]
        limit: Option<usize>,
    },
    ContextWritePackAudit {
        tenant_hash: u64,
        audit: ContextPackAudit,
    },
    ContextQueryPackAudit {
        tenant_hash: u64,
        session_hash: u64,
        start_time_ms: u64,
        end_time_ms: u64,
        #[serde(default)]
        limit: Option<usize>,
    },
    ContextMarkSummaryDirty {
        tenant_hash: u64,
        marker: ContextSummaryDirtyMarker,
    },
    ContextQuerySummaryDirty {
        tenant_hash: u64,
        node_hash: u64,
        start_time_ms: u64,
        end_time_ms: u64,
        #[serde(default)]
        limit: Option<usize>,
    },
    ContextUpsertEntity {
        tenant_hash: u64,
        entity: ContextEntity,
    },
    ContextGetEntity {
        tenant_hash: u64,
        node_hash: u64,
        entity_hash: u64,
    },
    ContextQueryEntities {
        tenant_hash: u64,
        node_hash: u64,
        entity_hashes: Vec<u64>,
        #[serde(default)]
        limit: Option<usize>,
    },
    ContextUpsertChildRef {
        tenant_hash: u64,
        child_ref: ContextChildRef,
    },
    ContextQueryChildren {
        tenant_hash: u64,
        parent_hash: u64,
        #[serde(default)]
        limit: Option<usize>,
    },
    ContextUpsertEmbedding {
        tenant_hash: u64,
        embedding: ContextEmbedding,
    },
    ContextQueryEmbeddings {
        tenant_hash: u64,
        ref_hashes: Vec<u64>,
        #[serde(default)]
        limit: Option<usize>,
    },
    ContextTraverseTree {
        tenant_hash: u64,
        start_node_hash: u64,
        query_vector: Vec<f32>,
        #[serde(default)]
        max_depth: Option<u32>,
        #[serde(default)]
        top_k_per_depth: Option<usize>,
        #[serde(default)]
        max_children_scored_per_parent: Option<usize>,
        #[serde(default)]
        max_candidate_nodes: Option<usize>,
        #[serde(default)]
        leaf_only: bool,
    },
    ContextUpsertSummary {
        tenant_hash: u64,
        summary: ContextSummary,
    },
    ContextQuerySummaries {
        tenant_hash: u64,
        node_hash: u64,
        level: u32,
        as_of_ms: u64,
        #[serde(default)]
        limit: Option<usize>,
    },
    ContextWriteCompressionEvent {
        tenant_hash: u64,
        event: ContextCompressionEvent,
    },
    ContextQueryCompressionEvents {
        tenant_hash: u64,
        node_hashes: Vec<u64>,
        start_time_ms: u64,
        end_time_ms: u64,
        #[serde(default)]
        limit: Option<usize>,
    },
    ContextCompressEvents {
        tenant_hash: u64,
        node_hash: u64,
        source_start_ms: u64,
        source_end_ms: u64,
        compressed_time_ms: u64,
        #[serde(default)]
        max_source_events: Option<usize>,
        #[serde(default)]
        min_confidence: f32,
        #[serde(default)]
        min_importance: f32,
    },
    ContextQueryNodeContext {
        tenant_hash: u64,
        node_hash: u64,
        #[serde(default)]
        summary_level: Option<u32>,
        as_of_ms: u64,
        #[serde(default)]
        cold_start_time_ms: u64,
        #[serde(default)]
        cold_end_time_ms: u64,
        #[serde(default)]
        compression_limit: Option<usize>,
    },
}

fn encode_varint_field(out: &mut Vec<u8>, field_number: u64, value: u64) {
    encode_varint(out, field_number << 3);
    encode_varint(out, value);
}

fn encode_fixed32_field(out: &mut Vec<u8>, field_number: u64, value: u32) {
    encode_varint(out, (field_number << 3) | 5);
    out.extend(value.to_le_bytes());
}

fn encode_bytes_field(out: &mut Vec<u8>, field_number: u64, value: &[u8]) {
    if value.is_empty() {
        return;
    }
    encode_varint(out, (field_number << 3) | 2);
    encode_varint(out, value.len() as u64);
    out.extend(value);
}

fn encode_varint(out: &mut Vec<u8>, mut value: u64) {
    while value >= 0x80 {
        out.push((value as u8) | 0x80);
        value >>= 7;
    }
    out.push(value as u8);
}

fn decode_varint(bytes: &[u8], cursor: &mut usize) -> Option<u64> {
    let mut value = 0_u64;
    for shift in (0..64).step_by(7) {
        let byte = *bytes.get(*cursor)?;
        *cursor += 1;
        value |= u64::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            return Some(value);
        }
    }
    None
}

fn decode_fixed32(bytes: &[u8], cursor: &mut usize) -> Option<u32> {
    let end = cursor.checked_add(4)?;
    let slice = bytes.get(*cursor..end)?;
    *cursor = end;
    Some(u32::from_le_bytes(slice.try_into().ok()?))
}

fn decode_bytes(bytes: &[u8], cursor: &mut usize) -> Option<Vec<u8>> {
    let len = usize::try_from(decode_varint(bytes, cursor)?).ok()?;
    let end = cursor.checked_add(len)?;
    let slice = bytes.get(*cursor..end)?;
    *cursor = end;
    Some(slice.to_vec())
}

fn decode_string(bytes: &[u8], cursor: &mut usize) -> Option<String> {
    String::from_utf8(decode_bytes(bytes, cursor)?).ok()
}

fn skip_proto_field(bytes: &[u8], cursor: &mut usize, wire_type: u64) -> Option<()> {
    match wire_type {
        0 => {
            let _ = decode_varint(bytes, cursor)?;
        }
        1 => *cursor = cursor.checked_add(8)?,
        2 => {
            let len = usize::try_from(decode_varint(bytes, cursor)?).ok()?;
            *cursor = cursor.checked_add(len)?;
        }
        5 => *cursor = cursor.checked_add(4)?,
        _ => return None,
    }
    (*cursor <= bytes.len()).then_some(())
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum CommandResponse {
    Empty,
    Bytes {
        value: Option<Vec<u8>>,
    },
    Integer {
        value: i64,
    },
    Members {
        members: Vec<Vec<u8>>,
    },
    Values {
        values: Vec<Option<Vec<u8>>>,
    },
    HashEntries {
        entries: Vec<(String, Vec<u8>)>,
    },
    FeaturePoints {
        points: Vec<FeaturePoint>,
    },
    FeaturePointGroups {
        groups: Vec<(String, Vec<FeaturePoint>)>,
    },
    Aggregate {
        value: i64,
    },
    SequenceRows {
        rows: Vec<SequenceFeatureRow>,
    },
    SequenceRowGroups {
        groups: Vec<(String, Vec<SequenceFeatureRow>)>,
    },
    IpsStats {
        stats: IpsStats,
    },
    IpsSnapshotReport {
        report: IpsSnapshotReport,
    },
    ContextNode {
        object_key: String,
        node: Option<ContextNode>,
    },
    ContextNodes {
        nodes: Vec<ContextNode>,
    },
    ContextObjectKey {
        object_key: String,
    },
    ContextExtractedEventWrite {
        event_object_key: String,
        index_object_keys: Vec<String>,
        written_index_count: usize,
    },
    ContextEvents {
        object_key: String,
        events: Vec<ContextEvent>,
    },
    ContextIndexRefs {
        object_key: String,
        refs: Vec<ContextIndexRef>,
    },
    ContextIndexIntersection {
        refs: Vec<ContextIndexRef>,
        matched_index_count: usize,
        scanned_ref_count: usize,
        deduped_ref_count: usize,
    },
    ContextPackAudits {
        object_key: String,
        audits: Vec<ContextPackAudit>,
    },
    ContextSummaryDirtyMarkers {
        object_key: String,
        markers: Vec<ContextSummaryDirtyMarker>,
    },
    ContextEntity {
        object_key: String,
        entity: Option<ContextEntity>,
    },
    ContextEntities {
        object_key: String,
        entities: Vec<ContextEntity>,
    },
    ContextChildRefs {
        object_key: String,
        refs: Vec<ContextChildRef>,
        #[serde(default)]
        created: Option<bool>,
    },
    ContextEmbeddings {
        embeddings: Vec<ContextEmbedding>,
    },
    ContextTraversedNodes {
        nodes: Vec<ContextTraversedNode>,
    },
    ContextSummaries {
        object_key: String,
        summaries: Vec<ContextSummary>,
    },
    ContextCompressionEvents {
        object_key: String,
        events: Vec<ContextCompressionEvent>,
        #[serde(default)]
        source_event_count: Option<u32>,
        #[serde(default)]
        truncated_source_events: Option<bool>,
    },
    ContextNodeContext {
        node_exists: bool,
        node: Option<ContextNode>,
        overall_summary_exists: bool,
        overall_summary: Option<ContextSummary>,
        cold_window_summaries: Vec<ContextCompressionEvent>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ExecuteRequest {
    pub shard_id: ShardId,
    pub command: Command,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ExecuteResponse {
    pub status: Status,
    pub response: CommandResponse,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum EventReplicationMode {
    #[default]
    Inherit,
    AsyncStorage,
    SyncStorage,
    Raft,
}

impl EventReplicationMode {
    pub fn requires_restart(self) -> bool {
        false
    }

    pub fn is_explicit(self) -> bool {
        self != Self::Inherit
    }
}

impl ExecuteRequest {
    pub fn with_replication_mode(
        self,
        replication_mode: EventReplicationMode,
    ) -> ReplicatedExecuteRequest {
        ReplicatedExecuteRequest {
            shard_id: self.shard_id,
            command: self.command,
            replication_mode,
        }
    }

    pub fn with_async_storage(self) -> ReplicatedExecuteRequest {
        self.with_replication_mode(EventReplicationMode::AsyncStorage)
    }

    pub fn with_sync_storage(self) -> ReplicatedExecuteRequest {
        self.with_replication_mode(EventReplicationMode::SyncStorage)
    }

    pub fn with_raft(self) -> ReplicatedExecuteRequest {
        self.with_replication_mode(EventReplicationMode::Raft)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct EventReplicationSelectionReport {
    pub requested_mode: EventReplicationMode,
    pub effective_mode: EventReplicationMode,
    pub write_command: bool,
    pub accepted: bool,
    pub restart_required: bool,
    pub reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ReplicatedExecuteRequest {
    pub shard_id: ShardId,
    pub command: Command,
    #[serde(default)]
    pub replication_mode: EventReplicationMode,
}

impl ReplicatedExecuteRequest {
    pub fn new(
        shard_id: ShardId,
        command: Command,
        replication_mode: EventReplicationMode,
    ) -> Self {
        Self {
            shard_id,
            command,
            replication_mode,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ReplicatedCommand {
    pub command: Command,
    #[serde(default)]
    pub replication_mode: EventReplicationMode,
}

impl ReplicatedCommand {
    pub fn new(command: Command, replication_mode: EventReplicationMode) -> Self {
        Self {
            command,
            replication_mode,
        }
    }

    pub fn async_storage(command: Command) -> Self {
        Self::new(command, EventReplicationMode::AsyncStorage)
    }

    pub fn sync_storage(command: Command) -> Self {
        Self::new(command, EventReplicationMode::SyncStorage)
    }

    pub fn raft(command: Command) -> Self {
        Self::new(command, EventReplicationMode::Raft)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ReplicatedBatchExecuteRequest {
    pub shard_id: ShardId,
    pub commands: Vec<ReplicatedCommand>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ReplicatedBatchExecuteResponse {
    pub status: Status,
    pub responses: Vec<ExecuteResponse>,
    pub replication: Vec<EventReplicationSelectionReport>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct BatchExecuteRequest {
    pub shard_id: ShardId,
    pub commands: Vec<Command>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct BatchExecuteResponse {
    pub status: Status,
    pub responses: Vec<ExecuteResponse>,
}

#[cfg(test)]
mod tests {
    use super::*;

    // shared-corpus: dynamic_event_replication_mode_selection
    #[test]
    fn caller_can_select_event_replication_mode_with_helpers() {
        let request = ExecuteRequest {
            shard_id: 7,
            command: Command::StringSet {
                key: "caller-choice".to_string(),
                value: b"raft".to_vec(),
            },
        }
        .with_raft();
        assert_eq!(request.replication_mode, EventReplicationMode::Raft);

        let batch_command = ReplicatedCommand::sync_storage(Command::HashSet {
            key: "caller-choice".to_string(),
            field: "mode".to_string(),
            value: b"sync".to_vec(),
        });
        assert_eq!(
            batch_command.replication_mode,
            EventReplicationMode::SyncStorage
        );
        assert!(!EventReplicationMode::AsyncStorage.requires_restart());
        assert!(!EventReplicationMode::SyncStorage.requires_restart());
        assert!(!EventReplicationMode::Raft.requires_restart());
    }

    // shared-corpus: context_cpp_wire_model_descriptor_roundtrip
    #[test]
    fn context_models_round_trip_cpp_wire_payloads_and_type_alias() {
        assert_eq!(
            context_model_descriptors()
                .iter()
                .map(|descriptor| {
                    (
                        descriptor.name.as_str(),
                        descriptor.model_id,
                        descriptor.key_family.as_str(),
                    )
                })
                .collect::<Vec<_>>(),
            vec![
                ("ContextNodeModel", 9, "ctx:node"),
                ("ContextEventModel", 10, "ctx:event"),
                ("ContextIndexModel", 11, "ctxidx"),
                ("ContextAuditModel", 12, "ctx:audit"),
                ("ContextDirtyModel", 13, "ctx:dirty"),
                ("ContextChildModel", 14, "ctx:child"),
                ("ContextEmbeddingModel", 15, "ctx:embedding"),
                ("ContextSummaryModel", 16, "ctx:summary"),
                ("ContextCompressionModel", 17, "ctx:compress"),
                ("ContextEntityModel", 18, "ctx:entity"),
            ]
        );
        assert_eq!(
            context_model_descriptor("ContextEventModel").map(|descriptor| descriptor.model_id),
            Some(CONTEXT_EVENT_MODEL_ID)
        );
        assert_eq!(
            context_model_descriptor("ContextSegment").map(|descriptor| descriptor.model_id),
            Some(CONTEXT_EVENT_MODEL_ID)
        );
        assert_eq!(
            context_model_descriptor("ctx:event").map(|descriptor| descriptor.model_id),
            Some(CONTEXT_EVENT_MODEL_ID)
        );
        assert_eq!(
            context_model_descriptor("10").map(|descriptor| descriptor.name),
            Some("ContextEventModel".to_string())
        );
        assert_eq!(
            context_model_descriptor("ctx:compression").map(|descriptor| descriptor.model_id),
            Some(CONTEXT_COMPRESSION_MODEL_ID)
        );
        assert!(context_model_descriptor("ContextSegment")
            .unwrap()
            .aliases
            .iter()
            .any(|alias| alias == "ContextSegment"));

        let node = ContextNode {
            node_hash: 42,
            parent_hash: 7,
            kind: 3,
            canonical_name: "checkout".to_string(),
            l0: "service".to_string(),
            status: 1,
            last_event_time_ms: 123,
            summary_dirty: true,
            l1_ref: "l1".to_string(),
            raw_metadata_ref: "raw".to_string(),
        };
        let cpp_node = ContextNode {
            status: 0,
            summary_dirty: false,
            l1_ref: String::new(),
            raw_metadata_ref: String::new(),
            ..node.clone()
        };
        assert_eq!(
            ContextNode::decode_cpp_context_value(&node.encode_cpp_context_value()),
            Some(cpp_node)
        );

        let entity = ContextEntity {
            entity_hash: 7001,
            node_hash: 42,
            entity_type: 1,
            name: "gpu_purchase_request".to_string(),
            value: "approved".to_string(),
            updated_at_ms: 1_000,
            valid_from_ms: 1_000,
            confidence: 0.97,
            source_event_hashes: vec![5, 6],
        };
        assert_eq!(
            ContextEntity::decode_cpp_context_value(&entity.encode_cpp_context_value()),
            Some(entity)
        );

        let child = ContextChildRef {
            parent_hash: 10,
            child_hash: 20,
            updated_at_ms: 1_000,
        };
        assert_eq!(
            ContextChildRef::decode_cpp_context_value(&child.encode_cpp_context_value()),
            Some(child)
        );
        let embedding = ContextEmbedding {
            ref_hash: 20,
            level: 1,
            model_hash: 0,
            vector: vec![1.0, 0.0],
            updated_at_ms: 1_000,
        };
        assert_eq!(
            ContextEmbedding::decode_cpp_context_value(&embedding.encode_cpp_context_value()),
            Some(embedding)
        );
        let summary = ContextSummary {
            node_hash: 20,
            level: 1,
            text: "summary".to_string(),
            valid_from_ms: 1_000,
        };
        assert_eq!(
            ContextSummary::decode_cpp_context_value(&summary.encode_cpp_context_value()),
            Some(summary)
        );
        let compression = ContextCompressionEvent {
            compression_id_hash: 5001,
            node_hash: 20,
            source_start_ms: 900,
            source_end_ms: 1_000,
            compressed_time_ms: 1_000,
            summary: "compressed".to_string(),
        };
        assert_eq!(
            ContextCompressionEvent::decode_cpp_context_value(
                &compression.encode_cpp_context_value()
            ),
            Some(compression)
        );

        let event_json = br#"{
            "event_id_hash":5,
            "event_time_ms":1000,
            "ingestion_time_ms":1001,
            "kind":9,
            "type":2,
            "actor_hash":77,
            "status":1,
            "valid_until_ms":0,
            "confidence":0.75,
            "importance":0.5,
            "text":"hello",
            "source_ref":"src",
            "related_node_hashes":[42,43],
            "compact_attrs":[1,2,3]
        }"#;
        let event: ContextEvent = serde_json::from_slice(event_json).unwrap();
        assert_eq!(event.event_type, 2);
        assert_eq!(event.primary_time_ms(), 1001);
        let cpp_event = ContextEvent {
            ingestion_time_ms: 1001,
            kind: 0,
            actor_hash: 0,
            status: 0,
            valid_until_ms: 0,
            source_ref: String::new(),
            related_node_hashes: Vec::new(),
            compact_attrs: Vec::new(),
            ..event.clone()
        };
        assert_eq!(
            ContextEvent::decode_cpp_context_value(&event.encode_cpp_context_value()),
            Some(cpp_event)
        );

        let index = ContextIndexRef {
            primary_node_hash: 42,
            primary_event_time_ms: 1000,
            event_id_hash: 5,
        };
        assert_eq!(
            ContextIndexRef::decode_cpp_context_value(&index.encode_cpp_context_value()),
            Some(index)
        );

        let audit_ref = ContextAuditRef {
            node_hash: 42,
            event_time_ms: 1000,
            reason: "ranked".to_string(),
        };
        let audit = ContextPackAudit {
            query_id: "q1".to_string(),
            session_hash: 99,
            request_time_ms: 2000,
            query_hash: 111,
            max_prompt_tokens: 4096,
            selected_tokens: 128,
            selected_refs: vec![audit_ref],
            blocked_refs: Vec::new(),
        };
        assert_eq!(
            ContextPackAudit::decode_cpp_context_value(&audit.encode_cpp_context_value()),
            Some(audit)
        );

        let dirty = ContextSummaryDirtyMarker {
            node_hash: 42,
            event_time_ms: 3000,
            reason: 4,
            propagate_depth: 2,
        };
        assert_eq!(
            ContextSummaryDirtyMarker::decode_cpp_context_value(&dirty.encode_cpp_context_value()),
            Some(dirty)
        );
    }
}
