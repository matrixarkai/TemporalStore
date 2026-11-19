// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A LIST FINDS AN ELEMENT BY POSITION WITHOUT WALKING TO IT, AND REMOVES WITHOUT COPYING THE LIST.
//!
//! `ListSet` and `ListRange` reached the element at an index by advancing an iterator over every
//! element before it, and `ListRemove` copied every address of the list before comparing the first
//! one. The position is now the first sequence word plus the index while the list holds a contiguous
//! run of words, and a removal compares in place. What must not move is the answer: the same
//! elements at the same positions, the same removals for a positive, a negative and a zero count, and
//! the same answers after an interior removal has left a hole in the run, where the contiguous
//! shortcut no longer applies. This file drives both shapes against a model that is a plain `Vec`.
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

fn exec(engine: &TemporalEngine, command: Command) -> CommandResponse {
    let response = engine.execute(ExecuteRequest { shard_id: 1, command });
    assert!(response.status.ok, "{:?}", response.status);
    response.response
}

fn whole(engine: &TemporalEngine) -> Vec<Vec<u8>> {
    match exec(engine, Command::ListRange { key: "l".into(), start: 0, stop: -1 }) {
        CommandResponse::Members { members } => members,
        other => panic!("members expected: {other:?}"),
    }
}

fn window(engine: &TemporalEngine, start: i64, stop: i64) -> Vec<Vec<u8>> {
    match exec(engine, Command::ListRange { key: "l".into(), start, stop }) {
        CommandResponse::Members { members } => members,
        other => panic!("members expected: {other:?}"),
    }
}

fn count_of(response: CommandResponse) -> i64 {
    match response {
        CommandResponse::Integer { value } => value,
        other => panic!("count expected: {other:?}"),
    }
}

fn element(n: usize) -> Vec<u8> {
    // Every fifth element repeats, so a removal by value has several matches to choose between.
    if n % 5 == 0 { b"dup".to_vec() } else { format!("e-{n:04}").into_bytes() }
}

/// Every window shape against the model, and every index through `ListSet` and back.
fn assert_positions(engine: &TemporalEngine, model: &mut Vec<Vec<u8>>, label: &str) {
    assert_eq!(&whole(engine), model, "{label}: the whole list");
    let n = model.len() as i64;
    for (start, stop) in [(0, 0), (0, 3), (1, 1), (n / 2, n / 2 + 2), (-3, -1), (-1, -1), (n - 1, n + 5), (2, 1)] {
        let resolve = |i: i64| if i < 0 { n + i } else { i };
        let from = resolve(start).max(0);
        let to = resolve(stop).min(n - 1);
        let want: Vec<Vec<u8>> = if from > to || n == 0 { Vec::new() } else { model[from as usize..=to as usize].to_vec() };
        assert_eq!(window(engine, start, stop), want, "{label}: window {start}..{stop}");
    }
    // Overwrite a spread of positions, negative ones included, and read each back through the model.
    for index in [0i64, 1, n / 2, n - 2, n - 1, -1, -2, -n] {
        if n == 0 {
            break;
        }
        let resolved = if index < 0 { n + index } else { index };
        if resolved < 0 || resolved >= n {
            continue;
        }
        let value = format!("set-{label}-{index}").into_bytes();
        let answer = count_of(exec(engine, Command::ListSet { key: "l".into(), index, member: value.clone() }));
        assert_eq!(answer, 1, "{label}: ListSet at {index}");
        model[resolved as usize] = value;
        assert_eq!(&whole(engine), model, "{label}: after ListSet at {index}");
    }
    // Out of range answers 0 and changes nothing.
    assert_eq!(count_of(exec(engine, Command::ListSet { key: "l".into(), index: n, member: b"x".to_vec() })), 0);
    assert_eq!(count_of(exec(engine, Command::ListSet { key: "l".into(), index: -n - 1, member: b"x".to_vec() })), 0);
    assert_eq!(&whole(engine), model, "{label}: out-of-range sets change nothing");
}

#[test]
fn positions_and_removals_answer_what_a_walk_answered_with_and_without_a_hole() {
    for count in [1usize, 2, 9, 40] {
        let dir = tempfile::tempdir().unwrap();
        let engine = engine_on(dir.path());
        let mut model: Vec<Vec<u8>> = Vec::new();
        for n in 0..count {
            exec(&engine, Command::ListPush { key: "l".into(), member: element(n), left: false });
            model.push(element(n));
        }
        // A few pushed to the head and popped from both ends: still one contiguous run of words.
        for n in 0..3 {
            let value = format!("head-{n}").into_bytes();
            exec(&engine, Command::ListPush { key: "l".into(), member: value.clone(), left: true });
            model.insert(0, value);
        }
        exec(&engine, Command::ListPop { key: "l".into(), left: false });
        model.pop();
        assert_positions(&engine, &mut model, &format!("contiguous, {count} elements"));

        // An interior removal leaves a hole in the run; every answer must survive it.
        let victim = model[model.len() / 2].clone();
        let removed = count_of(exec(&engine, Command::ListRemove { key: "l".into(), count: 1, member: victim.clone() }));
        let at = model.iter().position(|e| *e == victim).unwrap();
        model.remove(at);
        assert_eq!(removed, 1);
        assert_positions(&engine, &mut model, &format!("holed, {count} elements"));

        // Removal by a negative count takes from the tail, a zero count takes every match, and a
        // member that is not there takes nothing.
        let dup = b"dup".to_vec();
        if model.iter().filter(|e| **e == dup).count() > 1 {
            let last = model.iter().rposition(|e| *e == dup).unwrap();
            assert_eq!(count_of(exec(&engine, Command::ListRemove { key: "l".into(), count: -1, member: dup.clone() })), 1);
            model.remove(last);
            assert_eq!(&whole(&engine), &model, "a negative count removes the last match");
        }
        let all = model.iter().filter(|e| **e == dup).count() as i64;
        assert_eq!(count_of(exec(&engine, Command::ListRemove { key: "l".into(), count: 0, member: dup.clone() })), all);
        model.retain(|e| *e != dup);
        assert_eq!(&whole(&engine), &model, "a zero count removes every match");
        assert_eq!(count_of(exec(&engine, Command::ListRemove { key: "l".into(), count: 0, member: b"absent".to_vec() })), 0);
        assert_eq!(&whole(&engine), &model);
        if !model.is_empty() {
            assert_positions(&engine, &mut model, &format!("after removals, {count} elements"));
        }
    }
}
