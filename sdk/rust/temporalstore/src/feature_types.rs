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
#[cfg_attr(feature = "proxy", derive(serde::Serialize, serde::Deserialize))]
pub struct SequenceFeatureRow {
    pub timestamp: u64,
    pub gid: u64,
    pub action_type: u32,
    pub duration: u32,
    pub author_id: u64,
}
