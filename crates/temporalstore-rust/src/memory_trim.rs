// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Returning freed heap to the operating system.
//!
//! Rust's `free` hands memory back to the allocator, not to the kernel. On glibc -- what this runs
//! on, since the only `#[global_allocator]` here is a test-only probe -- freed chunks stay in
//! per-thread arenas, and the process keeps its resident blocks until the heap top happens to be
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
    // bookkeeping is what it walks. It cannot invalidate a live allocation -- only unused blocks go
    // back.
    unsafe { malloc_trim(0) != 0 }
}

#[cfg(not(all(target_os = "linux", target_env = "gnu")))]
fn trim() -> bool {
    // No portable equivalent. Reporting "nothing released" is the honest answer rather than
    // pretending the platform did something.
    false
}

/// How many malloc arenas the server allows itself when the operator has said nothing.
///
/// glibc gives every thread that allocates its own arena, up to eight per core, and a freed chunk
/// stays in the arena that made it. The server runs a thread per connection plus background
/// workers -- about 43 under a 32-connection load -- and a write allocates on one thread and frees
/// on another, so chunks end up scattered across arenas where no later allocation reuses them.
///
/// MEASURED on the matched fixture (100,000 keys, 144 B values, 80/20 get/set, 40 minutes, three
/// servers differing only in this setting, one run each): resident memory from minute 5 grew
/// 1.00 MB/min at glibc's default, 0.28 at four arenas and 0.07 at two, with throughput unchanged
/// to slightly better (sets 952 -> 976 per second, gets 3,641 -> 3,738). The LIVE heap was flat
/// throughout -- valgrind massif over 435,000 sets read 6.2 MB at the start and 7.0 MB at the
/// end -- so what grew was retention, not data.
///
/// Much of the growth is warm-up and slows later: the default's slope was +2.05 MB/min in the
/// first half of that run and +0.75 in the second, and in a 30-minute write-only run the second
/// half was flat. It does not always stop: one 118-minute run of the uncapped server was still
/// rising at 1.4 MB/min in its second half. So the cap lowers how far and how fast resident memory
/// climbs; this is not a claim that uncapped memory grows per write without bound.
pub const DEFAULT_ARENA_MAX: i32 = 2;

/// Cap the malloc arenas, once, at process start, before the threads that allocate exist.
///
/// Returns the cap applied, or `None` when nothing was changed: the operator set
/// `MALLOC_ARENA_MAX` (glibc has already read it, and theirs wins), `TS_MALLOC_ARENA_MAX` is `0`
/// or unparsable, or this is not a glibc target. `TS_MALLOC_ARENA_MAX=<n>` picks another cap.
pub fn cap_malloc_arenas() -> Option<i32> {
    if std::env::var_os("MALLOC_ARENA_MAX").is_some() {
        return None;
    }
    let wanted = match std::env::var("TS_MALLOC_ARENA_MAX") {
        Ok(value) => value.trim().parse::<i32>().ok()?,
        Err(_) => DEFAULT_ARENA_MAX,
    };
    apply_arena_cap(wanted)
}

#[cfg(all(target_os = "linux", target_env = "gnu"))]
fn apply_arena_cap(wanted: i32) -> Option<i32> {
    if wanted <= 0 {
        return None;
    }
    extern "C" {
        /// glibc: set a malloc tunable. Returns 1 on success, 0 on error.
        fn mallopt(param: i32, value: i32) -> i32;
    }
    // `M_ARENA_MAX` from <malloc.h>.
    const M_ARENA_MAX: i32 = -8;
    // Safe: two integers in, no pointers, and it only changes how many arenas glibc will create
    // from now on. Arenas that already exist keep serving the threads that hold them.
    (unsafe { mallopt(M_ARENA_MAX, wanted) } == 1).then_some(wanted)
}

#[cfg(not(all(target_os = "linux", target_env = "gnu")))]
fn apply_arena_cap(_wanted: i32) -> Option<i32> {
    None
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

    /// The cap is a positive count, zero and negatives leave glibc's default, and an operator's own
    /// `MALLOC_ARENA_MAX` always wins. The value is a generous 1,024 so applying it in a test
    /// process that runs many threads changes nothing about how those threads allocate.
    #[test]
    fn the_arena_cap_applies_a_positive_value_and_leaves_the_rest_alone() {
        assert_eq!(DEFAULT_ARENA_MAX, 2, "the measured default is two arenas");
        assert_eq!(apply_arena_cap(0), None, "0 must leave glibc's default");
        assert_eq!(apply_arena_cap(-3), None, "a negative must leave glibc's default");
        #[cfg(all(target_os = "linux", target_env = "gnu"))]
        assert_eq!(apply_arena_cap(1024), Some(1024), "a positive cap must be applied");
        #[cfg(not(all(target_os = "linux", target_env = "gnu")))]
        assert_eq!(apply_arena_cap(1024), None, "no portable equivalent off glibc");

        std::env::set_var("MALLOC_ARENA_MAX", "4");
        assert_eq!(cap_malloc_arenas(), None, "the operator's setting must win");
        std::env::remove_var("MALLOC_ARENA_MAX");
    }

    /// Off means off: the call must do nothing and say so.
    #[test]
    fn a_disabled_trim_releases_nothing() {
        std::env::set_var("TS_MALLOC_TRIM", "0");
        assert!(!release_free_heap_to_os(), "a disabled trim must not report a release");
        std::env::remove_var("TS_MALLOC_TRIM");
    }
}
