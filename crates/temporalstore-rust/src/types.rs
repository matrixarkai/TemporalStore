use serde::{Deserialize, Serialize};

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
    pub timestamp_ms: u64,
    pub value: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SequenceFeatureRow {
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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum FeatureFilterOp {
    Equal,
    NotEqual,
    GreaterThan,
    LessThan,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum StringSetCondition {
    Always,
    IfExists,
    IfNotExists,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum FeatureWritePolicy {
    Upsert,
    InsertIfAbsent,
    ReplaceExisting,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FeatureFilter {
    pub field: String,
    pub op: FeatureFilterOp,
    pub value: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SequenceQuerySpec {
    pub key: String,
    pub start_ms: u64,
    pub end_ms: u64,
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
}

fn encode_varint_field(out: &mut Vec<u8>, field_number: u64, value: u64) {
    encode_varint(out, field_number << 3);
    encode_varint(out, value);
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
