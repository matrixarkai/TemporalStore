//! Preferred WAL-facing names for the local mutation log.
//!
//! The older `oplog` module remains available for compatibility, but new code
//! should import these WAL names.

pub use crate::oplog::{
    LocalWalStore, OplogError as WalError, OplogGcReport as WalGcReport, OplogRecord as WalRecord,
    OplogStats as WalStats,
};
