//! Compatibility facade for the standalone rustmtcache library.
//!
//! TemporalStore keeps this module path stable while the cache implementation
//! now lives in `crates/rustmtcache`, matching the external-library direction
//! used for RustRaft.

pub use rustmtcache::*;
