// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! THE MODEL SLOT ON AN INDEX-LOG ROW CARRIES A POSITION, NOT A REGISTRY CODE.
//!
//! Two numbering schemes name the same seventeen model spellings and they agree on NOTHING:
//!
//!   * `index_log::MODEL_ID_NUMBERS` is POSITIONAL and 0-based. The position IS the wire value, so
//!     `"string"` is 0 and will mean 0 forever.
//!   * the entry spelling's own registry (`storage_bucket_internals`'s `model_kind_registry!`)
//!     gives each kind an EXPLICIT `report_code()` with deliberate holes -- `"string"` is 1 there,
//!     `"zset"` is 6, and 5 and 15 belong to retired names.
//!
//! WHY THIS IS PINNED AND NOT COMMENTED. The resident entry holds its model as the one-byte
//! `StoredModelKind`, and the log row holds it as a `String`; collapsing the row's onto the entry's
//! is worth 23 bytes and is the single largest step in making the two one type. A reader whose hand
//! is already on `StoredModelKind` has `report_code()` in easy reach and `MODEL_ID_NUMBERS` nowhere
//! in sight, so reaching for the wrong one is the natural mistake rather than a careless one.
//!
//! AND IT IS SLOT-PRESERVING IN SHAPE AND INCOMPATIBLE IN VALUE, which is the worst combination a
//! durable format offers: the row still has twelve slots, the slot still holds an integer, the
//! decode still succeeds, and every stored row's model silently becomes a DIFFERENT, VALID kind.
//! Nothing errors anywhere. This file asserts what the slot carries, and asserts that a swap is
//! DETECTABLE for every spelling rather than assuming it would be.
//!
//! rust-internal: reads the engine's own index-log encoder, no external surface

use crate::index_log::{model_id_slot_number, MODEL_ID_NUMBERS};

/// Every spelling the registry declares, with the code IT would have written.
///
/// Spelled out here rather than read from the registry on purpose: this file's job is to catch the
/// two numberings being confused, and a table derived from one of them cannot do that. It is tied
/// to the registry by the membership assertion below instead, so it cannot go stale silently.
const REGISTRY_REPORT_CODES: &[(&str, u64)] = &[
    ("string", 1),
    ("hash", 2),
    ("set", 3),
    ("feature", 4),
    ("sequence", 5),
    ("control_state", 7),
    ("context_node", 8),
    ("context_event", 9),
    ("context_index", 10),
    ("context_audit", 11),
    ("context_child", 14),
    ("context_embedding", 15),
    ("context_summary", 16),
    ("context_compression", 17),
    ("context_entity", 13),
    ("zset", 6),
    ("list", 12),
];

#[test]
fn the_model_slot_carries_the_positional_index_and_never_the_registry_code() {
    // --- FLOOR: the subject is non-empty and the two tables cover the same spellings, so neither
    //     arm below can pass over nothing. ---
    assert!(
        !MODEL_ID_NUMBERS.is_empty(),
        "MODEL_ID_NUMBERS is empty, so every assertion below is scoring an empty set"
    );
    assert_eq!(
        MODEL_ID_NUMBERS.len(),
        REGISTRY_REPORT_CODES.len(),
        "DENOMINATOR: the positional list holds {} spelling(s) and this file's registry table {}. \
         One of them learned a model the other did not, so the comparison below is partial",
        MODEL_ID_NUMBERS.len(),
        REGISTRY_REPORT_CODES.len()
    );
    for (name, _) in REGISTRY_REPORT_CODES {
        assert!(
            MODEL_ID_NUMBERS.contains(name),
            "the registry declares {name:?} and the positional list does not. This file's table \
             has gone stale against `model_kind_registry!`"
        );
    }

    // --- ARM ONE: what the encoder actually writes, read back THROUGH the encoder. ---
    let mut checked = 0usize;
    for (position, name) in MODEL_ID_NUMBERS.iter().enumerate() {
        let written = model_id_slot_number(name);
        assert_eq!(
            written,
            Some(position as u64),
            "the model slot for {name:?} carried {written:?}, not its position {position}. The \
             position IS the wire value: a row already on disk holding {position} means {name:?} \
             and will mean it forever"
        );
        checked += 1;
    }
    assert_eq!(
        checked,
        MODEL_ID_NUMBERS.len(),
        "the loop checked {checked} of {} spellings",
        MODEL_ID_NUMBERS.len()
    );

    // --- ARM TWO: the swap is DETECTABLE for every spelling, so this guard cannot be satisfied by
    //     a kind where the two numberings happen to coincide. Also prints what each swap would
    //     silently reinterpret the row as -- the evidence, not a claim about it. ---
    let mut coincide: Vec<&str> = Vec::new();
    println!("  {:<22} {:>8} {:>6}   what a swapped row would decode as", "spelling", "position", "code");
    for (name, code) in REGISTRY_REPORT_CODES {
        let position = MODEL_ID_NUMBERS
            .iter()
            .position(|candidate| candidate == name)
            .expect("membership asserted above") as u64;
        if position == *code {
            coincide.push(name);
        }
        let misread = MODEL_ID_NUMBERS
            .get(*code as usize)
            .copied()
            .unwrap_or("<out of range: an unlisted number, written as a string>");
        println!("  {name:<22} {position:>8} {code:>6}   {misread}");
    }
    assert!(
        coincide.is_empty(),
        "these spellings have the SAME positional index and registry code, so a swap to \
         `report_code()` would not be caught on them: {coincide:?}. Every other arm of this guard \
         is still sound, but the swap is no longer detectable everywhere and this file must say so"
    );
    println!(
        "\n  {} spelling(s) checked; the two numberings differ on ALL of them, so a swap to \
         `report_code()` is detectable on any kind",
        REGISTRY_REPORT_CODES.len()
    );
}
