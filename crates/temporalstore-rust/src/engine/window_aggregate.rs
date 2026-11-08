// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! The one home of the reductions a windowed read applies to a series of numbers.
//!
//! # WHAT USED TO BE SPREAD OUT
//!
//! Three read paths each carried a private copy of "sum / min / max / first / last / count" over a
//! window: the feature aggregate query (`aggregate_feature_values`), the counter-family query
//! (`aggregate_control_state_values`), and the plain-scan arm of the windowed counter sum. They
//! agree on the verbs and disagree on the fallbacks, and those disagreements are answers a caller
//! may depend on, so this module keeps them exactly:
//!
//! | name                      | feature query              | counter query              |
//! |---------------------------|----------------------------|----------------------------|
//! | `sum`                     | sum of the parseable values| sum                        |
//! | `avg` / `average`         | truncating mean            | (unknown) sum              |
//! | `min` / `max`             | over the parseable values  | over the values            |
//! | `first` / `last`          | the first / last VALUE     | the first / last value     |
//! | `count`                   | number of values           | sum                        |
//! | `events` / `len`          | number of values           | number of values           |
//! | empty name                | number of values           | sum                        |
//! | any other name            | number of values           | sum                        |
//!
//! The lenient fallbacks belong to those two query doors and are NOT widened here: a door decides
//! what an unknown name means, [`AggName::parse`] only says whether the name is one this module
//! knows. A new read that wants unknown names refused calls [`AggName::parse`] and refuses `None`.
//!
//! # OVERFLOW
//!
//! The legacy reductions ([`sum_legacy`], [`mean_truncated`]) keep `Iterator::sum`'s behaviour
//! (a panic in a debug build, a wrap in a release build) because changing it would change an
//! answer. Nothing new should call them.

/// A reduction name this module knows. Matched ASCII case-insensitively.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum AggName {
    Sum,
    Avg,
    Min,
    Max,
    First,
    Last,
    /// `count`, and the empty name.
    Count,
    Events,
    Len,
}

impl AggName {
    /// `Some` for a name this module knows, `None` for anything else. Does not allocate.
    pub(crate) fn parse(name: &str) -> Option<AggName> {
        const TABLE: [(&str, AggName); 10] = [
            ("sum", AggName::Sum),
            ("avg", AggName::Avg),
            ("average", AggName::Avg),
            ("min", AggName::Min),
            ("max", AggName::Max),
            ("first", AggName::First),
            ("last", AggName::Last),
            ("count", AggName::Count),
            ("events", AggName::Events),
            ("len", AggName::Len),
        ];
        if name.is_empty() {
            return Some(AggName::Count);
        }
        TABLE
            .iter()
            .find(|(known, _)| known.eq_ignore_ascii_case(name))
            .map(|(_, parsed)| *parsed)
    }
}

/// Sum with `Iterator::sum`'s overflow behaviour (see the module note).
pub(crate) fn sum_legacy(values: impl Iterator<Item = i64>) -> i64 {
    values.sum()
}

/// Truncating integer mean; `0` for no values. The sum has `Iterator::sum`'s overflow behaviour.
pub(crate) fn mean_truncated(values: impl Iterator<Item = i64>) -> i64 {
    let (sum, count) = values.fold((0i64, 0i64), |(sum, count), value| (sum + value, count + 1));
    if count == 0 {
        0
    } else {
        sum / count
    }
}

/// Smallest value, `0` for no values.
pub(crate) fn min_or_zero(values: impl Iterator<Item = i64>) -> i64 {
    values.min().unwrap_or_default()
}

/// Largest value, `0` for no values.
pub(crate) fn max_or_zero(values: impl Iterator<Item = i64>) -> i64 {
    values.max().unwrap_or_default()
}

/// First value, `0` for no values.
pub(crate) fn first_or_zero(mut values: impl Iterator<Item = i64>) -> i64 {
    values.next().unwrap_or_default()
}

/// Last value, `0` for no values.
pub(crate) fn last_or_zero(values: impl Iterator<Item = i64>) -> i64 {
    values.last().unwrap_or_default()
}

/// The feature aggregate query's reduction over `values`, each already parsed to a number or
/// `None` when it is not one. `total` is the number of values INCLUDING the unparseable ones,
/// which is what the count verbs report.
pub(crate) fn feature_legacy(
    name: &str,
    values: impl Iterator<Item = Option<i64>>,
    total: usize,
) -> i64 {
    match AggName::parse(name) {
        Some(AggName::Sum) => sum_legacy(values.flatten()),
        Some(AggName::Avg) => mean_truncated(values.flatten()),
        Some(AggName::Min) => min_or_zero(values.flatten()),
        Some(AggName::Max) => max_or_zero(values.flatten()),
        // First and last name the first and last VALUE, parseable or not.
        Some(AggName::First) => first_or_zero(values.take(1).flatten()),
        Some(AggName::Last) => last_or_zero(values.last().into_iter().flatten()),
        _ => total as i64,
    }
}

/// The counter-family query's reduction over `values`. `Avg` is not a counter verb, so it falls
/// to the sum like every unknown name.
pub(crate) fn counter_legacy(name: &str, values: &[i64]) -> i64 {
    match AggName::parse(name) {
        Some(AggName::Events) | Some(AggName::Len) => values.len() as i64,
        Some(AggName::Min) => min_or_zero(values.iter().copied()),
        Some(AggName::Max) => max_or_zero(values.iter().copied()),
        Some(AggName::First) => first_or_zero(values.iter().copied()),
        Some(AggName::Last) => last_or_zero(values.iter().copied()),
        _ => sum_legacy(values.iter().copied()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // The two functions below are the bodies this module replaced, copied verbatim and kept as the
    // oracle: every answer of the shared door must equal them on every input.
    fn legacy_parse(bytes: &Vec<u8>) -> Option<i64> {
        std::str::from_utf8(bytes)
            .ok()?
            .trim_start_matches(|c: char| c.is_ascii_whitespace())
            .parse()
            .ok()
    }

    fn oracle_feature(values: &[Vec<u8>], aggregator: &str) -> i64 {
        match aggregator.to_ascii_lowercase().as_str() {
            "sum" => values.iter().filter_map(legacy_parse).sum(),
            "avg" | "average" => {
                let numeric = values.iter().filter_map(legacy_parse).collect::<Vec<_>>();
                if numeric.is_empty() {
                    0
                } else {
                    numeric.iter().sum::<i64>() / numeric.len() as i64
                }
            }
            "min" => values.iter().filter_map(legacy_parse).min().unwrap_or_default(),
            "max" => values.iter().filter_map(legacy_parse).max().unwrap_or_default(),
            "first" => values.first().and_then(legacy_parse).unwrap_or_default(),
            "last" => values.last().and_then(legacy_parse).unwrap_or_default(),
            "count" | "events" | "" => values.len() as i64,
            _ => values.len() as i64,
        }
    }

    fn oracle_counter(values: &[i64], aggregator: &str) -> i64 {
        match aggregator.to_ascii_lowercase().as_str() {
            "sum" | "count" | "" => values.iter().sum(),
            "events" | "len" => values.len() as i64,
            "min" => values.iter().copied().min().unwrap_or_default(),
            "max" => values.iter().copied().max().unwrap_or_default(),
            "first" => values.first().copied().unwrap_or_default(),
            "last" => values.last().copied().unwrap_or_default(),
            _ => values.iter().sum(),
        }
    }

    fn door_feature(values: &[Vec<u8>], aggregator: &str) -> i64 {
        feature_legacy(
            aggregator,
            values.iter().map(legacy_parse),
            values.len(),
        )
    }

    const NAMES: [&str; 16] = [
        "sum", "SUM", "Avg", "average", "min", "MAX", "first", "Last", "count", "events", "len",
        "", "p99", "median", "sum ", "rate",
    ];

    struct Rng(u64);
    impl Rng {
        fn next(&mut self) -> u64 {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            self.0
        }
    }

    fn sample_bytes(rng: &mut Rng) -> Vec<u8> {
        match rng.next() % 6 {
            0 => b"not a number".to_vec(),
            1 => vec![0xff, 0xfe],
            2 => format!("  {}", (rng.next() % 1000) as i64 - 500).into_bytes(),
            3 => Vec::new(),
            _ => ((rng.next() % 2000) as i64 - 1000).to_string().into_bytes(),
        }
    }

    #[test]
    fn the_shared_doors_answer_what_the_bodies_they_replaced_answered() {
        let mut rng = Rng(0x9E37_79B9_7F4A_7C15);
        let mut compared = 0usize;
        for round in 0..400 {
            let n = (rng.next() % 12) as usize; // includes the empty window
            let raw: Vec<Vec<u8>> = (0..n).map(|_| sample_bytes(&mut rng)).collect();
            let numbers: Vec<i64> = (0..n).map(|_| (rng.next() % 2000) as i64 - 1000).collect();
            for name in NAMES {
                assert_eq!(
                    door_feature(&raw, name),
                    oracle_feature(&raw, name),
                    "feature `{name}` round {round} over {raw:?}"
                );
                assert_eq!(
                    counter_legacy(name, &numbers),
                    oracle_counter(&numbers, name),
                    "counter `{name}` round {round} over {numbers:?}"
                );
                compared += 2;
            }
        }
        // A control that must come out right: the loop really compared, and the two doors really
        // disagree on `count`, so a door that collapsed them would fail above.
        assert_eq!(compared, 400 * NAMES.len() * 2);
        assert_eq!(door_feature(&[b"4".to_vec(), b"x".to_vec(), b"5".to_vec()], "count"), 3);
        assert_eq!(counter_legacy("count", &[4, 5]), 9);
    }

    #[test]
    fn a_name_is_known_or_refused_and_the_table_is_case_insensitive() {
        for known in ["sum", "Sum", "AVG", "average", "min", "max", "first", "last", "count", "events", "len", ""] {
            assert!(AggName::parse(known).is_some(), "`{known}` should be known");
        }
        for unknown in ["p99", "median", " sum", "sum ", "rate", "avgg"] {
            assert_eq!(AggName::parse(unknown), None, "`{unknown}` should be refused");
        }
    }

    #[test]
    fn edge_answers_are_the_documented_ones() {
        assert_eq!(mean_truncated(std::iter::empty()), 0);
        assert_eq!(mean_truncated([7, 8].into_iter()), 7); // truncates toward zero
        assert_eq!(mean_truncated([-7, -8].into_iter()), -7);
        assert_eq!(min_or_zero(std::iter::empty()), 0);
        assert_eq!(first_or_zero([3, 4].into_iter()), 3);
        assert_eq!(last_or_zero([3, 4].into_iter()), 4);
        // `first` of the feature query is the first VALUE: an unparseable one answers 0.
        assert_eq!(door_feature(&[b"x".to_vec(), b"9".to_vec()], "first"), 0);
    }
}
