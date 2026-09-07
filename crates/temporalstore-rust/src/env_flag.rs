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

#[cfg(test)]
mod tests {
    use super::*;

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
