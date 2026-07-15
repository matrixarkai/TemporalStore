#[derive(Clone, Copy, Debug)]
#[cfg_attr(feature = "proxy", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "proxy", serde(rename_all = "snake_case"))]
pub enum FeatureFilterOp {
    Equal = 0,
    NotEqual = 1,
    GreaterThan = 2,
    LessThan = 3,
    GreaterOrEqual = 4,
    LessOrEqual = 5,
}

#[derive(Clone, Copy, Debug)]
#[repr(i32)]
#[cfg_attr(feature = "proxy", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "proxy", serde(rename_all = "snake_case"))]
pub enum FeatureWritePolicy {
    Upsert = 0,
    Block = 1,
    First = 2,
    Update = 3,
}

#[derive(Clone, Debug)]
#[cfg_attr(feature = "proxy", derive(serde::Serialize, serde::Deserialize))]
pub struct FeatureFilter {
    pub field: String,
    pub op: FeatureFilterOp,
    pub value: u64,
}

#[derive(Clone, Debug)]
#[cfg_attr(feature = "proxy", derive(serde::Serialize, serde::Deserialize))]
pub struct FeaturePoint {
    pub timestamp_ms: u64,
    pub value: Vec<u8>,
}

#[derive(Clone, Copy, Debug)]
#[repr(i32)]
pub enum ControlStatePrecision {
    OneSecond = 0,
    FiveSeconds = 1,
    TenSeconds = 2,
    OneMinute = 3,
    FiveMinutes = 4,
    TenMinutes = 5,
    OneHour = 6,
    OneDay = 7,
    OneMonth = 8,
}

#[derive(Clone, Copy, Debug)]
#[repr(i32)]
pub enum ControlStateWindowUnit {
    Second = 0,
    Minute = 1,
    Hour = 2,
    Day = 3,
}

#[derive(Clone, Copy, Debug)]
pub struct ControlStateWindow {
    pub start: i64,
    pub end: i64,
    pub unit: ControlStateWindowUnit,
}

impl Default for ControlStateWindow {
    fn default() -> Self {
        Self {
            start: -1,
            end: 0,
            unit: ControlStateWindowUnit::Hour,
        }
    }
}

#[derive(Clone, Copy, Debug)]
#[cfg_attr(feature = "proxy", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "proxy", serde(rename_all = "snake_case"))]
pub enum ControlStateHType {
    Count,
    Min,
    Max,
    Change,
}

#[derive(Clone, Copy, Debug)]
#[cfg_attr(feature = "proxy", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "proxy", serde(rename_all = "snake_case"))]
pub enum ControlStateFolType {
    First,
    Last,
}

#[derive(Clone, Copy, Debug)]
#[cfg_attr(feature = "proxy", derive(serde::Serialize, serde::Deserialize))]
pub struct SequenceFeatureRow {
    pub timestamp: u64,
    pub gid: u64,
    pub action_type: u32,
    pub duration: u32,
    pub author_id: u64,
}

#[derive(Clone, Copy, Debug)]
#[cfg_attr(feature = "proxy", derive(serde::Serialize, serde::Deserialize))]
pub struct IpsFeatureStat {
    pub id: i64,
    pub slot: i32,
    pub has_slot: bool,
    #[cfg_attr(feature = "proxy", serde(rename = "type"))]
    pub kind: i32,
    pub v1: i32,
    pub v2: i32,
}

#[derive(Clone, Debug)]
pub struct IpsInstance {
    pub table: String,
    pub uid: i64,
    pub timestamp_us: i64,
    pub action_type: i32,
    pub logical_table: i32,
    pub features: Vec<IpsFeatureStat>,
}

impl Default for IpsInstance {
    fn default() -> Self {
        Self {
            table: "table_compress".to_string(),
            uid: 0,
            timestamp_us: 0,
            action_type: 0,
            logical_table: 0,
            features: Vec::new(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct IpsLastQuery {
    pub table: String,
    pub uid: i64,
    pub action_type: i32,
    pub logical_table: i32,
    pub slot: i32,
    pub top_k: i32,
    pub last_instances: i64,
}

impl Default for IpsLastQuery {
    fn default() -> Self {
        Self {
            table: "table_compress".to_string(),
            uid: 0,
            action_type: 0,
            logical_table: 0,
            slot: 0,
            top_k: 20,
            last_instances: 10,
        }
    }
}
