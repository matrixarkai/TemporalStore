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
