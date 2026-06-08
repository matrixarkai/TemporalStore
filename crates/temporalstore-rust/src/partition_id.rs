use serde::{Deserialize, Serialize};
use thiserror::Error;

pub const SLOT_COUNT: u32 = 1 << 30;
pub const SLOT_MASK: u32 = SLOT_COUNT - 1;
pub const MIN_SLOTS_PER_PARTITION: u32 = 1 << 14;
pub const PARTITION_VERSION_MASK: u32 = 0xFFFF;
pub const MAX_TABLE_ID: u32 = 0xFFFF;
pub const PARTITION_INDEX_MASK: u32 = 0xFF;
pub const MAX_PARTITION_SET_INDEX: u32 = 0xFFFF;

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum PartitionIdError {
    #[error("table_id out of C++ partition id range: {0}")]
    TableIdOutOfRange(u64),
    #[error("partition_set_index out of C++ partition id range: {0}")]
    PartitionSetIndexOutOfRange(u64),
    #[error("partition_index out of C++ partition id range: {0}")]
    PartitionIndexOutOfRange(u64),
    #[error("partition_version out of C++ partition id range: {0}")]
    PartitionVersionOutOfRange(u64),
    #[error("partition set count must be > 0 and <= {max}: {value}")]
    InvalidPartitionSetCount { value: u32, max: u32 },
    #[error("partition count per set must be > 0 and <= 255: {0}")]
    InvalidPartitionCountPerSet(u32),
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PartitionId {
    id: u64,
}

impl PartitionId {
    pub fn new(
        table_id: u64,
        partition_set_index: u64,
        partition_index: u64,
        partition_version: u64,
    ) -> Result<Self, PartitionIdError> {
        validate_u16(table_id, PartitionIdError::TableIdOutOfRange)?;
        validate_u16(
            partition_set_index,
            PartitionIdError::PartitionSetIndexOutOfRange,
        )?;
        if partition_index > PARTITION_INDEX_MASK as u64 {
            return Err(PartitionIdError::PartitionIndexOutOfRange(partition_index));
        }
        validate_u16(
            partition_version,
            PartitionIdError::PartitionVersionOutOfRange,
        )?;
        let id = (((table_id << 16) | partition_set_index) << 8 | partition_index) << 16
            | partition_version;
        Ok(Self { id })
    }

    pub fn from_raw(id: u64) -> Self {
        Self { id }
    }

    pub fn id(self) -> u64 {
        self.id
    }

    pub fn table_id(self) -> u32 {
        ((self.id >> 40) & 0xFFFF) as u32
    }

    pub fn partition_set_id(self) -> u64 {
        self.id >> 24
    }

    pub fn partition_set_index(self) -> u32 {
        ((self.id >> 24) & 0xFFFF) as u32
    }

    pub fn partition_index(self) -> u32 {
        ((self.id >> 16) & 0xFF) as u32
    }

    pub fn partition_version(self) -> u32 {
        (self.id & 0xFFFF) as u32
    }

    pub fn with_partition_set_id(self, partition_set_id: u64) -> Result<Self, PartitionIdError> {
        if partition_set_id > 0xFFFF_FFFF {
            return Err(PartitionIdError::PartitionSetIndexOutOfRange(
                partition_set_id,
            ));
        }
        Ok(Self {
            id: (self.id & 0xFF_FFFF) | (partition_set_id << 24),
        })
    }

    pub fn with_partition_index(self, partition_index: u64) -> Result<Self, PartitionIdError> {
        if partition_index > PARTITION_INDEX_MASK as u64 {
            return Err(PartitionIdError::PartitionIndexOutOfRange(partition_index));
        }
        Ok(Self {
            id: (self.id & !0xFF_0000) | (partition_index << 16),
        })
    }

    pub fn with_partition_version(self, partition_version: u64) -> Result<Self, PartitionIdError> {
        validate_u16(
            partition_version,
            PartitionIdError::PartitionVersionOutOfRange,
        )?;
        Ok(Self {
            id: ((self.id >> 16) << 16) | partition_version,
        })
    }
}

impl std::fmt::Display for PartitionId {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "{}({}|{}|{}|{})",
            self.id,
            self.table_id(),
            self.partition_set_id(),
            self.partition_index(),
            self.partition_version()
        )
    }
}

pub fn validate_partition_set_count(value: u32) -> Result<(), PartitionIdError> {
    let max = SLOT_COUNT / MIN_SLOTS_PER_PARTITION;
    if value == 0 || value > max {
        return Err(PartitionIdError::InvalidPartitionSetCount { value, max });
    }
    Ok(())
}

pub fn validate_partition_count_per_set(value: u32) -> Result<(), PartitionIdError> {
    if value == 0 || value > PARTITION_INDEX_MASK {
        return Err(PartitionIdError::InvalidPartitionCountPerSet(value));
    }
    Ok(())
}

fn validate_u16(
    value: u64,
    error: impl FnOnce(u64) -> PartitionIdError,
) -> Result<(), PartitionIdError> {
    if value > PARTITION_VERSION_MASK as u64 {
        return Err(error(value));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn partition_id_matches_cpp_bit_layout_and_display() {
        let id = PartitionId::new(42, 7, 3, 11).unwrap();
        let expected = (((42_u64 << 16) | 7) << 8 | 3) << 16 | 11;
        assert_eq!(id.id(), expected);
        assert_eq!(id.table_id(), 42);
        assert_eq!(id.partition_set_id(), (42_u64 << 16) | 7);
        assert_eq!(id.partition_set_index(), 7);
        assert_eq!(id.partition_index(), 3);
        assert_eq!(id.partition_version(), 11);
        assert_eq!(id.to_string(), format!("{expected}(42|2752519|3|11)"));
    }

    #[test]
    fn partition_id_setters_match_cpp_masks() {
        let id = PartitionId::new(1, 2, 3, 4)
            .unwrap()
            .with_partition_set_id(0x000A_000B)
            .unwrap()
            .with_partition_index(0xCC)
            .unwrap()
            .with_partition_version(0xDDDD)
            .unwrap();
        assert_eq!(id.table_id(), 0x000A);
        assert_eq!(id.partition_set_index(), 0x000B);
        assert_eq!(id.partition_index(), 0xCC);
        assert_eq!(id.partition_version(), 0xDDDD);
    }

    #[test]
    fn partition_id_validates_cpp_ranges() {
        assert_eq!(
            PartitionId::new(0x1_0000, 0, 0, 0).unwrap_err(),
            PartitionIdError::TableIdOutOfRange(0x1_0000)
        );
        assert_eq!(
            PartitionId::new(0, 0x1_0000, 0, 0).unwrap_err(),
            PartitionIdError::PartitionSetIndexOutOfRange(0x1_0000)
        );
        assert_eq!(
            PartitionId::new(0, 0, 0x100, 0).unwrap_err(),
            PartitionIdError::PartitionIndexOutOfRange(0x100)
        );
        assert_eq!(
            PartitionId::new(0, 0, 0, 0x1_0000).unwrap_err(),
            PartitionIdError::PartitionVersionOutOfRange(0x1_0000)
        );
        assert!(validate_partition_set_count(SLOT_COUNT / MIN_SLOTS_PER_PARTITION).is_ok());
        assert!(validate_partition_set_count(0).is_err());
        assert!(validate_partition_count_per_set(255).is_ok());
        assert!(validate_partition_count_per_set(256).is_err());
    }
}
