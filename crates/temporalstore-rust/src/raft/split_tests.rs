// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Splitting an oversized `SequenceAdd` to fit the raft entry limit.
//!
//! The split used to build and serialize the whole candidate command for every row, to ask whether
//! one more row still fit: quadratic in the rows. It now keeps a running total. The total has to be
//! the same number the whole-command measure gives, at every boundary, because a row that fits or
//! does not decides where the chunks break and what the error reports. The old function is kept
//! here, unchanged apart from its name, as the oracle.

use super::*;
use crate::types::{Command, SequenceFeatureRow};

fn the_split_that_built_every_candidate(command: Command, limit: u64) -> Result<Vec<Command>, RaftError> {
    let bytes = command_size_bytes(&command);
    if bytes <= limit {
        return Ok(vec![command]);
    }
    match command {
        Command::SequenceAdd { key, rows } if rows.len() > 1 => {
            let mut chunks = Vec::new();
            let mut current = Vec::new();
            for row in rows {
                let mut candidate = current.clone();
                candidate.push(row.clone());
                let candidate_command = Command::SequenceAdd {
                    key: key.clone(),
                    rows: candidate,
                };
                let candidate_bytes = command_size_bytes(&candidate_command);
                if candidate_bytes <= limit {
                    current.push(row);
                    continue;
                }
                if current.is_empty() {
                    return Err(RaftError::LogEntryTooLarge {
                        bytes: candidate_bytes,
                        limit,
                    });
                }
                chunks.push(Command::SequenceAdd {
                    key: key.clone(),
                    rows: std::mem::take(&mut current),
                });
                let single_command = Command::SequenceAdd {
                    key: key.clone(),
                    rows: vec![row.clone()],
                };
                let single_bytes = command_size_bytes(&single_command);
                if single_bytes > limit {
                    return Err(RaftError::LogEntryTooLarge {
                        bytes: single_bytes,
                        limit,
                    });
                }
                current.push(row);
            }
            if !current.is_empty() {
                chunks.push(Command::SequenceAdd { key, rows: current });
            }
            Ok(chunks)
        }
        other => Err(RaftError::LogEntryTooLarge {
            bytes: command_size_bytes(&other),
            limit,
        }),
    }
}

fn rows(count: usize, wide: bool) -> Vec<SequenceFeatureRow> {
    (0..count)
        .map(|offset| SequenceFeatureRow {
            timestamp_ms: if wide { u64::MAX - offset as u64 } else { offset as u64 },
            gid: if wide { u64::MAX } else { (offset % 3) as u64 },
            action_type: if wide { u32::MAX } else { (offset % 8) as u32 },
            duration: if wide { u32::MAX } else { (offset % 600) as u32 },
            author_id: if wide { u64::MAX - 1 } else { 7 },
        })
        .collect()
}

/// Reduce a split to something comparable: the chunks as JSON, or the error's figures.
fn outcome(result: Result<Vec<Command>, RaftError>) -> String {
    match result {
        Ok(chunks) => chunks
            .iter()
            .map(|chunk| serde_json::to_string(chunk).expect("serializes"))
            .collect::<Vec<_>>()
            .join("|"),
        Err(RaftError::LogEntryTooLarge { bytes, limit }) => format!("too-large {bytes} {limit}"),
        Err(other) => format!("other {other:?}"),
    }
}

#[test]
fn a_split_breaks_where_the_whole_candidate_measure_broke_at_every_limit() {
    let mut compared = 0usize;
    let mut split_into_many = 0usize;
    let mut too_large = 0usize;
    for key in ["s", "a longer key with \"quotes\" and \u{e9}\u{4e2d}"] {
        for wide in [false, true] {
            for count in [2usize, 3, 7, 40, 200] {
                let command = Command::SequenceAdd {
                    key: key.to_string(),
                    rows: rows(count, wide),
                };
                let whole = command_size_bytes(&command);
                let one = command_size_bytes(&Command::SequenceAdd {
                    key: key.to_string(),
                    rows: rows(1, wide),
                });
                let two = command_size_bytes(&Command::SequenceAdd {
                    key: key.to_string(),
                    rows: rows(2, wide),
                });
                // Limits at and either side of every size where the answer can change: the whole
                // command, one row, two rows, a row's width, and a sweep through the middle.
                let mut limits = vec![0, 1, one.saturating_sub(1), one, one + 1, two - 1, two, two + 1];
                limits.extend([whole.saturating_sub(1), whole, whole + 1]);
                limits.extend((1..=20).map(|step| one + (whole - one) * step / 20));
                for limit in limits {
                    let new = outcome(split_command_for_raft_limit(command.clone(), limit));
                    let old = outcome(the_split_that_built_every_candidate(command.clone(), limit));
                    assert_eq!(new, old, "key {key:?} wide {wide} rows {count} limit {limit}");
                    compared += 1;
                    if new.contains('|') {
                        split_into_many += 1;
                    }
                    if new.starts_with("too-large") {
                        too_large += 1;
                    }
                }
            }
        }
    }
    assert!(compared > 300, "compared only {compared}");
    assert!(split_into_many > 50, "too few splits were exercised: {split_into_many}");
    assert!(too_large > 20, "too few errors were exercised: {too_large}");
}

#[test]
fn a_command_that_is_not_a_sequence_add_is_still_refused_with_its_whole_size() {
    let command = Command::StringSet {
        key: "k".to_string(),
        value: vec![7; 500],
    };
    let size = command_size_bytes(&command);
    assert_eq!(
        outcome(split_command_for_raft_limit(command.clone(), size - 1)),
        outcome(the_split_that_built_every_candidate(command, size - 1))
    );
}
