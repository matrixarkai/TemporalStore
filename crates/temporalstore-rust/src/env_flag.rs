// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! One vocabulary for boolean environment flags.
//!
//! Six hand-rolled readers disagreed about what an operator had written. Four of them
//! (`metaserver`, `server`, `raft_node`, and `storage_backend::env_truthy`) matched the raw
//! string against `"1" | "true" | "TRUE" | "yes" | "YES"` with no trim and no lowercasing, and
//! treated everything else as `false` rather than as "not specified". For a flag whose default
//! is `true` that is the dangerous direction, because the value an operator is most likely to
//! write in order to KEEP it on turns it off:
//!
//! | written | that reader | this one |
//! |---|---|---|
//! | `on` / `On` / `ON` | `false` | `true` |
//! | `True` | `false` | `true` |
//! | `" 1"` (a stray space, as a unit file or heredoc leaves) | `false` | `true` |
//! | `wat` (anything unrecognised) | `false` | the default |
//! | `""` (set but empty) | `false` | the default |
//!
//! Every default-on flag in the tree reached one of those readers:
//! `TS_META_AUTO_REBALANCE_BALANCE`, `TS_META_REBALANCE_LOCATION_SCOPED`,
//! `TS_META_REBALANCE_PER_TABLE`, `TS_MATRIXOBJECT_CHECKPOINT_ON_START`,
//! `TS_MATRIXOBJECT_NETWORKED_CHECKPOINT_ON_START` and `TS_RAFT_ALLOW_PLAINTEXT`.
//!
//! The python side of this repository already settled the same argument and wrote it down in
//! `tools/test_env_flag_vocabulary.py`: "Boolean flags were parsed in six different
//! vocabularies. They disagreed on the two words an operator is most likely to reach for."
//! This is that vocabulary, for rust.

/// The words a boolean flag understands, or `None` when the value is not one of them.
///
/// `None` is deliberately distinct from `Some(false)`: a value nobody can read is not a
/// request to turn something off, and the caller falls back to the flag's own default.
pub fn parse_bool(raw: &str) -> Option<bool> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Some(true),
        "0" | "false" | "no" | "off" => Some(false),
        _ => None,
    }
}

/// Read `name` as a boolean, falling back to `default` when it is unset, empty, or holds
/// something outside the vocabulary.
pub fn env_bool(name: &str, default: bool) -> bool {
    std::env::var(name)
        .ok()
        .and_then(|raw| parse_bool(&raw))
        .unwrap_or(default)
}

/// Read `name` as a number, falling back to `default` when it is unset, empty, or holds
/// something that does not parse.
///
/// The numeric half of the same argument [`env_bool`] settles. `usize::from_str` rejects
/// surrounding whitespace outright -- `" 64 ".parse::<usize>()` is an `Err` -- so a reader that
/// hands it the raw value discards what the operator set and falls back to the default with no
/// message. A shell export, a systemd `Environment=` line, a heredoc and a `.env` file all leave
/// whitespace behind, and the boolean reader directly above already tolerates every one of them:
///
/// | written | a raw `value.parse()` | this one |
/// |---|---|---|
/// | `64` | `64` | `64` |
/// | `" 64"` / `"64 "` / `"\t64\n"` | the default | `64` |
/// | `wat` | the default | the default |
///
/// Fourteen local copies of this function existed, in seven files, and none of them trimmed. The
/// crate had settled the vocabulary question for booleans and left the numeric twin sitting
/// directly underneath it in the same files.
pub fn env_number<T: std::str::FromStr>(name: &str, default: T) -> T {
    std::env::var(name)
        .ok()
        .and_then(|raw| raw.trim().parse::<T>().ok())
        .unwrap_or(default)
}

/// The value of a numeric flag, or `None` when it is not one.
///
/// `None` is distinct from a zero the same way [`parse_bool`]'s is from `Some(false)`: a value
/// nobody can read is not a request for zero. Callers that treat zero as "unset" keep doing that
/// themselves -- `env_usize_any` filters it out on purpose -- rather than having it decided here.
pub fn parse_number<T: std::str::FromStr>(raw: &str) -> Option<T> {
    raw.trim().parse::<T>().ok()
}

/// The value of `name`, or `None` when it is unset OR set to nothing.
///
/// `std::env::var` answers `Ok("")` for a variable that is present and empty -- which is what
/// `export NEW=$UNSET` leaves behind, and what clearing a field means. Read through `or_else`,
/// that `Ok("")` is the newer spelling WINNING with nothing in it, and the older spelling it was
/// meant to replace is never consulted however correctly a deployment set it:
///
/// ```text
/// TS_BLOCK_SLAB_TARGET_BYTES=""        the previous name held 2 MiB
/// -> neither honoured; the built-in 1 GiB default applied
///
/// TS_DATA_RAFT_READ_MODE=""            TS_SERVER_RAFT_READ_MODE=linearizable
/// -> panic!("invalid TS_DATA_RAFT_READ_MODE"), and the data node exits at startup
/// ```
///
/// The python side settled this and wrote it down in
/// `tools/test_a_blank_flag_falls_through_to_the_older_spelling.py`, and two chains in
/// `context_workflow/model_provider.rs` already spell the filter out by hand. This is that rule,
/// in one place, for the rest of them.
///
/// Whitespace-only counts as nothing, for the same reason [`env_number`] trims: a value a shell
/// leaves as `" "` is not a value.
pub fn env_value(name: &str) -> Option<String> {
    std::env::var(name)
        .ok()
        .filter(|value| !value.trim().is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_number_survives_the_whitespace_a_launcher_leaves_behind() {
        // The failure this half of the module exists to prevent. Every one of these is an
        // `Err` from `usize::from_str`, so a reader that skips the trim answers with its
        // default and says nothing.
        for written in ["64", " 64", "64 ", " 64 ", "\t64\n", "  64  "] {
            assert_eq!(
                Some(64usize),
                parse_number::<usize>(written),
                "{written:?} should read as 64"
            );
        }
    }

    #[test]
    fn a_number_that_is_not_one_leaves_the_default_alone() {
        for written in ["", "   ", "wat", "6 4", "64x", "-1"] {
            assert_eq!(
                None,
                parse_number::<usize>(written),
                "{written:?} should not parse as a usize"
            );
        }
    }

    #[test]
    fn zero_is_a_value_and_not_an_absence() {
        // `env_usize_any` in the proxy treats 0 as "unset" and that is its decision to make.
        // This reader does not make it for everyone: a cap of 0 that silently became the
        // default would be the same silent discard, one layer up.
        assert_eq!(Some(0usize), parse_number::<usize>(" 0 "));
    }

    #[test]
    fn the_numeric_default_is_what_survives_an_unset_or_unreadable_variable() {
        let unset = "TS_ENV_FLAG_NUMBER_NAME_THAT_IS_NEVER_SET";
        assert_eq!(7usize, env_number(unset, 7usize));
        assert_eq!(7u64, env_number(unset, 7u64));
    }

    #[test]
    fn a_blank_value_is_not_a_value() {
        let name = "TS_ENV_FLAG_BLANK_PROBE";
        for written in ["", " ", "\t", "  \n "] {
            std::env::set_var(name, written);
            assert_eq!(None, env_value(name), "{written:?} should read as absent");
        }
        std::env::set_var(name, " codex ");
        assert_eq!(Some(" codex ".to_string()), env_value(name),
                   "a real value is returned as written, trimming is the caller's business");
        std::env::remove_var(name);
        assert_eq!(None, env_value(name));
    }

    #[test]
    fn a_blank_newer_spelling_falls_through_to_the_older_one() {
        // The whole point. Read with `std::env::var(..).or_else(..)` the second name is never
        // consulted here, because the first answered Ok("").
        let new = "TS_ENV_FLAG_CHAIN_NEW";
        let old = "TS_ENV_FLAG_CHAIN_OLD";
        std::env::set_var(new, "");
        std::env::set_var(old, "2097152");
        assert_eq!(
            Some("2097152".to_string()),
            env_value(new).or_else(|| env_value(old)),
            "a blank newer spelling must not shadow the older one"
        );
        std::env::set_var(new, "4194304");
        assert_eq!(Some("4194304".to_string()), env_value(new).or_else(|| env_value(old)),
                   "and a real newer value still wins");
        std::env::remove_var(new);
        std::env::remove_var(old);
    }

    #[test]
    fn both_halves_of_the_vocabulary_are_understood() {
        for on in ["1", "true", "yes", "on"] {
            assert_eq!(Some(true), parse_bool(on), "{on} should read as on");
        }
        for off in ["0", "false", "no", "off"] {
            assert_eq!(Some(false), parse_bool(off), "{off} should read as off");
        }
    }

    #[test]
    fn case_and_surrounding_space_do_not_change_the_answer() {
        // A unit file, a heredoc and a shell export all leave these behind.
        for written in ["On", "ON", "True", " 1", "1 ", "\tYES\n", "  on  "] {
            assert_eq!(
                Some(true),
                parse_bool(written),
                "{written:?} should read as on"
            );
        }
        for written in ["Off", "OFF", "False", " 0 ", "\tNO\n"] {
            assert_eq!(
                Some(false),
                parse_bool(written),
                "{written:?} should read as off"
            );
        }
    }

    #[test]
    fn an_unreadable_value_is_not_a_request_to_turn_something_off() {
        // The failure this module exists to prevent: a default-on flag whose value nobody can
        // read used to come back false, so a typo silently disabled the behaviour instead of
        // leaving it where the deployment had it.
        for written in ["", "wat", "2", "enabled", "disable"] {
            assert_eq!(None, parse_bool(written), "{written:?} should not parse");
        }
    }

    #[test]
    fn the_default_is_what_survives_an_unset_or_unreadable_variable() {
        // A name no test sets, so this reads the unset path without racing a sibling.
        let unset = "TS_ENV_FLAG_VOCABULARY_NAME_THAT_IS_NEVER_SET";
        assert!(env_bool(unset, true), "unset must fall back to the default");
        assert!(!env_bool(unset, false), "unset must fall back to the default");
    }
}
