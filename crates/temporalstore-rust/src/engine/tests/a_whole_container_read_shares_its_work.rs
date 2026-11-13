// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A READ OF A WHOLE LIST OR HASH SELECTS EACH ELEMENT OUT OF THE CACHE FRAME ITSELF, AND A LIST
//! RANGE BUILDS ITS PAGE KEYS FROM ONE RECORD KEY PER SLAB.
//!
//! A warm read of one element used to copy the whole cached frame into a `Vec`, copy the element
//! value out of it, and drop the frame. A list range also built each entry page key from scratch
//! (the slab record-key string, an `Arc` copy of it, and the selector) and formatted the entry
//! element name into a `String`. Now the frame is selected from in place, the list range shares its
//! record key through `PageKeys` as the whole-hash read does, and the element name is rendered into
//! a stack buffer.
//!
//! THE ANSWER IS THE SAME BYTES. Two default-build tests:
//!   * `a_list_range_answers_the_pushes_it_was_given` -- every window of a list against a model
//!     of the list kept beside it, over left and right pushes, empty and binary values, a pop at
//!     each end, a warm second read, and a reload that reads every block cold.
//!   * `a_list_element_name_spells_what_the_formatted_name_spells` -- the stack-rendered name
//!     against `format!`, over the extremes and a spread of sequences.
//! The whole-hash tests in `a_hash_get_all_shares_its_page_keys` run over the same read.
//! And two measurements, `probe::*`, which need `alloc-probe` and `--ignored`.
//!
//! rust-internal: measures this crate's own request paths, no product behaviour

#![allow(clippy::all)]
use super::*;

fn engine_on(dir: &std::path::Path) -> TemporalEngine {
    let engine = TemporalEngine::with_local_dirs(
        64 * 1024 * 1024,
        dir.join("cache"),
        dir.join("blocks"),
        dir.join("indexes"),
    );
    engine.load_shard(1);
    engine
}

fn run(engine: &TemporalEngine, command: Command) -> CommandResponse {
    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command,
    });
    assert!(response.status.ok, "{:?}", response.status);
    response.response
}

fn push(engine: &TemporalEngine, key: &str, member: Vec<u8>, left: bool) {
    run(
        engine,
        Command::ListPush {
            key: key.to_string(),
            member,
            left,
        },
    );
}

fn range(engine: &TemporalEngine, key: &str, start: i64, stop: i64) -> Vec<Vec<u8>> {
    match run(
        engine,
        Command::ListRange {
            key: key.to_string(),
            start,
            stop,
        },
    ) {
        CommandResponse::Members { members } => members,
        other => panic!("not members: {other:?}"),
    }
}

/// What a Redis-style inclusive range of `model` is, computed here and not by the engine.
fn expected(model: &[Vec<u8>], start: i64, stop: i64) -> Vec<Vec<u8>> {
    let length = model.len() as i64;
    let resolve = |index: i64| if index < 0 { length + index } else { index };
    let from = resolve(start).max(0);
    let to = resolve(stop).min(length - 1);
    if from > to || length == 0 {
        return Vec::new();
    }
    model[from as usize..=to as usize].to_vec()
}

#[test]
fn a_list_range_answers_the_pushes_it_was_given() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());

    let mut model: Vec<Vec<u8>> = Vec::new();
    for index in 0..48usize {
        let value = match index % 4 {
            0 => Vec::new(),
            1 => vec![0u8, 255, 1, 254, index as u8],
            2 => vec![b'x'; 1 + index * 37],
            _ => format!("value-{index}").into_bytes(),
        };
        let left = index % 3 == 0;
        push(&engine, "list", value.clone(), left);
        if left {
            model.insert(0, value);
        } else {
            model.push(value);
        }
    }
    // A pop at each end removes the entry a range must then not name.
    for left in [true, false] {
        run(
            &engine,
            Command::ListPop {
                key: "list".to_string(),
                left,
            },
        );
        if left {
            model.remove(0);
        } else {
            model.pop();
        }
    }
    push(&engine, "single", b"only".to_vec(), false);

    let windows: [(i64, i64); 9] = [
        (0, -1),
        (0, 0),
        (3, 9),
        (-5, -1),
        (-1, -1),
        (10, 4),
        (40, 400),
        (-400, 3),
        (0, 400),
    ];
    let check = |engine: &TemporalEngine, label: &str| -> usize {
        assert!(range(engine, "never-written", 0, -1).is_empty(), "{label}");
        assert_eq!(
            vec![b"only".to_vec()],
            range(engine, "single", 0, -1),
            "{label}"
        );
        let mut compared = 0;
        for (start, stop) in windows {
            let answer = range(engine, "list", start, stop);
            assert_eq!(
                expected(&model, start, stop),
                answer,
                "{label}: window {start}..{stop}"
            );
            // A SECOND READ is a warm one for the first keys, and answers the same.
            assert_eq!(answer, range(engine, "list", start, stop), "{label}: warm");
            compared += answer.len();
        }
        compared
    };

    let live = check(&engine, "live");
    drop(engine);
    // AFTER A RELOAD every block is read cold, which is the arm that stores a key.
    let reloaded = engine_on(dir.path());
    let cold = check(&reloaded, "reloaded");
    // DENOMINATOR: the windows named elements, both times, and the list is the one pushed.
    assert_eq!(live, cold);
    assert!(live > 100, "the windows compared {live} elements");
    assert_eq!(46, model.len());
}

#[test]
fn a_list_element_name_spells_what_the_formatted_name_spells() {
    let spelled = |seq: i64| format!("{:016x}", (seq as u64).wrapping_sub(i64::MIN as u64));
    let mut buf = [0u8; 16];
    let mut checked = 0;
    let mut state = 0x9e37_79b9_7f4a_7c15u64;
    let mut seqs = vec![
        0i64,
        1,
        -1,
        i64::MIN,
        i64::MAX,
        i64::MIN + 1,
        i64::MAX - 1,
        255,
        256,
        -256,
    ];
    for _ in 0..2000 {
        state = state
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        seqs.push((state >> (state % 64)) as i64);
    }
    for seq in seqs {
        assert_eq!(
            spelled(seq),
            crate::engine::execute_on_shard::list_element_name(seq, &mut buf),
            "{seq}"
        );
        checked += 1;
    }
    assert_eq!(2010, checked);
}

#[cfg(feature = "alloc-probe")]
mod probe {
    use super::*;
    use crate::alloc_probe::Probe;

    const OPS: usize = 20;

    fn per_read(model: &str, elements: usize) -> f64 {
        let dir = tempfile::tempdir().expect("tempdir");
        let engine = engine_on(dir.path());
        for index in 0..elements {
            if model == "list" {
                push(&engine, "k", format!("m{index:05}").into_bytes(), false);
            } else {
                run(
                    &engine,
                    Command::HashSet {
                        key: "k".to_string(),
                        field: format!("f{index:05}"),
                        value: vec![b'v'; 64],
                    },
                );
            }
        }
        let read = || match model {
            "list" => Command::ListRange {
                key: "k".to_string(),
                start: 0,
                stop: -1,
            },
            _ => Command::HashGetAll { key: "k".to_string() },
        };
        // One warm-up so the span measures the resident answer, not the first fill.
        let answered = match run(&engine, read()) {
            CommandResponse::Members { members } => members.len(),
            CommandResponse::HashEntries { entries } => entries.len(),
            other => panic!("not a whole read: {other:?}"),
        };
        assert_eq!(elements, answered);
        let commands: Vec<Command> = (0..OPS).map(|_| read()).collect();
        let probe = Probe::start();
        for command in commands {
            let response = engine.execute(ExecuteRequest {
                shard_id: 1,
                command,
            });
            assert!(response.status.ok);
        }
        probe.stop().allocs as f64 / OPS as f64
    }

    fn slope(model: &str) -> f64 {
        let short = per_read(model, 20);
        let long = per_read(model, 200);
        let slope = (long - short) / 180.0;
        println!("MEAS {model} range: {short:.1} @20, {long:.1} @200, {slope:.2}/element");
        slope
    }

    const LIST_LIMIT: f64 = 3.5;
    const HASH_LIMIT: f64 = 5.5;

    /// Allocations per ELEMENT of a warm list range, as the slope between two list sizes.
    ///
    /// Before this change the slope was 7.01: a `String` for the entry name, three allocations for
    /// its page key, the copy of the cached frame, and the value. Now it is the selector and the
    /// value.
    #[test]
    #[ignore = "measurement: run with --features alloc-probe --ignored"]
    fn a_list_range_does_not_pay_for_its_key_name_and_frame_copy_per_element() {
        let slope = slope("list");
        assert!(
            slope > 1.0,
            "the list range spent {slope:.2} allocations per element, so the probe cannot see work \
             that grows with the list"
        );
        assert!(
            slope <= LIST_LIMIT,
            "the list range spent {slope:.2} allocations per element; it spent 7.01 when each \
             entry formatted its name, built its page key from scratch and copied its frame"
        );
    }

    /// Allocations per FIELD of a warm whole-hash read. 6.03 before this change, which copied the
    /// cached frame for each field; the selector, the value and the name remain.
    #[test]
    #[ignore = "measurement: run with --features alloc-probe --ignored"]
    fn a_whole_hash_read_does_not_copy_the_frame_for_each_field() {
        let slope = slope("hash");
        assert!(slope > 1.0, "the whole-hash read spent {slope:.2} per field");
        assert!(
            slope <= HASH_LIMIT,
            "the whole-hash read spent {slope:.2} allocations per field; it spent 6.03 when each \
             field copied the cached frame"
        );
    }
}
