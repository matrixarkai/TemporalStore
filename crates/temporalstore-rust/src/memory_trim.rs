// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Returning freed heap to the operating system.
//!
//! Rust's `free` hands memory back to the allocator, not to the kernel. On glibc -- what this runs
//! on, since the only `#[global_allocator]` here is a test-only probe -- freed chunks stay in
//! per-thread arenas, and the process keeps its resident pages until the heap top happens to be
//! free or something calls `malloc_trim`.
//!
//! Measured in process: dropping 46 MB of live data returned 2.3% of it; one `malloc_trim(0)`
//! returned 95% of what was retained. A proxy holding 128.6 MB of live data measured 444.9 MB
//! resident, which is the same effect at deployment scale.

/// Ask the allocator to return free heap to the operating system.
///
/// Returns whether anything was released. `false` covers three different things and deliberately
/// does not distinguish them at the call site: the allocator had nothing to give back, the trim is
/// switched off, or this is not a glibc target.
///
/// Not on the serving path. A trim walks the free lists, so it belongs where a lot has just been
/// released at once and the next request is not waiting on it.
pub fn release_free_heap_to_os() -> bool {
    if !trim_enabled() {
        return false;
    }
    trim()
}

/// `TS_MALLOC_TRIM=0` (or `false`/`no`/`off`) switches the trim off.
///
/// On by default: the measurement is one-sided, in that what is returned is memory the process is
/// not using. The escape hatch exists because a trim costs a walk of the free lists, and an
/// operator who measures that as expensive on their heap should not need a build to stop it.
fn trim_enabled() -> bool {
    !matches!(
        std::env::var("TS_MALLOC_TRIM")
            .unwrap_or_default()
            .trim()
            .to_ascii_lowercase()
            .as_str(),
        "0" | "false" | "no" | "off"
    )
}

#[cfg(all(target_os = "linux", target_env = "gnu"))]
fn trim() -> bool {
    extern "C" {
        /// glibc: release free heap above `pad` back to the OS. Non-zero if it freed any.
        fn malloc_trim(pad: usize) -> i32;
    }
    // Safe: no arguments derived from Rust memory, no pointers handed over, and the allocator's own
    // bookkeeping is what it walks. It cannot invalidate a live allocation -- only unused pages go
    // back.
    unsafe { malloc_trim(0) != 0 }
}

#[cfg(not(all(target_os = "linux", target_env = "gnu")))]
fn trim() -> bool {
    // No portable equivalent. Reporting "nothing released" is the honest answer rather than
    // pretending the platform did something.
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The switch has to be read at the call, not cached, or an operator turning it off mid-run
    /// finds it still trimming.
    #[test]
    fn the_trim_switch_is_read_every_time() {
        std::env::set_var("TS_MALLOC_TRIM", "0");
        assert!(!trim_enabled(), "0 must switch the trim off");
        for off in ["false", "NO", "Off", " 0 "] {
            std::env::set_var("TS_MALLOC_TRIM", off);
            assert!(!trim_enabled(), "{off:?} must switch the trim off");
        }
        std::env::set_var("TS_MALLOC_TRIM", "1");
        assert!(trim_enabled(), "1 must leave it on");
        std::env::remove_var("TS_MALLOC_TRIM");
        assert!(trim_enabled(), "unset must leave it on, which is the default");
    }

    /// Off means off: the call must do nothing and say so.
    #[test]
    fn a_disabled_trim_releases_nothing() {
        std::env::set_var("TS_MALLOC_TRIM", "0");
        assert!(!release_free_heap_to_os(), "a disabled trim must not report a release");
        std::env::remove_var("TS_MALLOC_TRIM");
    }
}
