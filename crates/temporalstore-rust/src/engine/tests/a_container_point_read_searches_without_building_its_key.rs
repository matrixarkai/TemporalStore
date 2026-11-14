// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A POINT READ OF ONE ELEMENT OF A CONTAINER BLOCK DOES NOT BUILD A KEY TO SEARCH WITH, AND A
//! WHOLE-HASH READ SPELLS EACH FIELD NAME ONCE.
//!
//! Reading one element out of a framed block started by turning the element name back into the
//! key the block stores it under, as a `Vec<u8>`: for a hash field that is a copy of the name's own
//! bytes, for a list element eight bytes parsed out of sixteen hex digits. Neither needs the heap.
//! The key is now borrowed (a hash field) or written into eight bytes on the stack (a list word);
//! only a set or zset member, whose name has to be decoded, still builds one.
//!
//! The whole-hash read also copied every field name out of the container before reading a block,
//! then wrote each name a second time as an `Arc<str>` to say which element the block was for. The
//! container is now read in place and the name is lent; the one `String` per field is the answer's.
//!
//! WHAT DID NOT MOVE. The key every spelling builds, the element a block selects, the order of the
//! fields a whole-hash read answers, the divergence counters it keeps, and every stored byte: this
//! is a read path.
//!
//! Three arms in the default build, which compare against an oracle that restates each spelling
//! independently of the code under test, and two in `probe`, which need `alloc-probe` and
//! `--ignored`:
//!   * `the_borrowed_key_is_the_key_each_spelling_has_always_built`
//!   * `a_point_selection_answers_what_it_always_answered`
//!   * `a_whole_hash_read_answers_the_fields_a_hash_holds_in_order`
//!   * `probe::a_block_lookup_allocates_only_its_answer`
//!   * `probe::a_whole_hash_read_spends_no_allocation_on_a_name_it_does_not_return`
//!
//! rust-internal: measures this crate's own request paths, no product behaviour

#![allow(clippy::all)]
use super::*;
use crate::engine::container_blocks::{
    element_key_from_element_name, element_key_view, member_element_name,
    select_container_element, single_element_block, tombstone_block, ContainerElementRead,
    ElementKeySpelling,
};

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

/// What each spelling built before the key was borrowed, restated from the rules rather than from
/// the function: a hash field's own bytes, the member a hex or tagged name decodes to, eight zero
/// bytes then the member for a score-first spelling, and the big-endian word sixteen hex digits
/// spell. `None` wherever a spelling cannot produce a key from the name.
fn oracle(spelling: ElementKeySpelling, name: &str) -> Option<Vec<u8>> {
    match spelling {
        ElementKeySpelling::Utf8 => Some(name.as_bytes().to_vec()),
        ElementKeySpelling::Hex | ElementKeySpelling::Name => {
            crate::engine::container_blocks::member_from_element_name(name)
        }
        ElementKeySpelling::ScoreThenMember | ElementKeySpelling::ScoreThenMemberName => {
            let member = crate::engine::container_blocks::member_from_element_name(name)?;
            let mut key = vec![0_u8; 8];
            key.extend_from_slice(&member);
            Some(key)
        }
        ElementKeySpelling::BiasedWord => {
            if name.len() != 16 {
                return None;
            }
            u64::from_str_radix(name, 16)
                .ok()
                .map(|word| word.to_be_bytes().to_vec())
        }
    }
}

const SPELLINGS: [ElementKeySpelling; 6] = [
    ElementKeySpelling::Utf8,
    ElementKeySpelling::Hex,
    ElementKeySpelling::Name,
    ElementKeySpelling::ScoreThenMember,
    ElementKeySpelling::ScoreThenMemberName,
    ElementKeySpelling::BiasedWord,
];

fn names() -> Vec<String> {
    let mut names: Vec<String> = [
        "",
        "f",
        "field-000001",
        "h\u{e9}llo",
        "6162",
        "zz",
        "0",
        "+000000000000001",
        "0000000000000001",
        "ffffffffffffffff",
        "FFFFFFFFFFFFFFFF",
        "00000000000000010",
        "000000000000001",
        "8000000000000000",
    ]
    .iter()
    .map(|name| name.to_string())
    .collect();
    for member in [&b""[..], b"a", b"abc", &[0u8, 255, 7][..], b"\x01tag"] {
        names.push(member_element_name(member));
    }
    names
}

#[test]
fn the_borrowed_key_is_the_key_each_spelling_has_always_built() {
    let mut compared = 0;
    for spelling in SPELLINGS {
        for name in names() {
            let mut scratch = [0_u8; 8];
            let view = element_key_view(spelling, &name, &mut scratch).map(|key| key.into_owned());
            assert_eq!(oracle(spelling, &name), view, "{spelling:?} {name:?}");
            assert_eq!(
                oracle(spelling, &name),
                element_key_from_element_name(spelling, &name),
                "{spelling:?} {name:?}"
            );
            compared += 1;
        }
    }
    assert_eq!(SPELLINGS.len() * names().len(), compared);
    // THE ARMS THAT ARE ANSWERS, so the comparison above is not vacuous: a valid hash field, a
    // valid list word and a name no spelling can read.
    let mut scratch = [0_u8; 8];
    assert!(element_key_view(ElementKeySpelling::Utf8, "f", &mut scratch).is_some());
    assert!(element_key_view(ElementKeySpelling::BiasedWord, "8000000000000000", &mut scratch)
        .is_some());
    assert!(element_key_view(ElementKeySpelling::BiasedWord, "8000", &mut scratch).is_none());
}

#[test]
fn a_point_selection_answers_what_it_always_answered() {
    let hash_names = ["f", "", "h\u{e9}llo", "field-000001"];
    let list_names = ["8000000000000000", "8000000000000001", "ffffffffffffffff"];
    let member_names: Vec<String> = [&b"a"[..], b"abc", &[0u8, 255][..]]
        .iter()
        .map(|member| member_element_name(member))
        .collect();
    let mut selected = 0;
    for (kind, element_names) in [
        ("hash", hash_names.iter().map(|name| name.to_string()).collect::<Vec<_>>()),
        ("list", list_names.iter().map(|name| name.to_string()).collect()),
        ("set", member_names.clone()),
        ("zset", member_names.clone()),
    ] {
        for name in &element_names {
            let value = format!("value-of-{name}").into_bytes();
            let block = single_element_block(kind, name, &value);
            match select_container_element(&block, name) {
                ContainerElementRead::Found(found) => assert_eq!(value, found, "{kind} {name:?}"),
                other => panic!("{kind} {name:?}: expected the element, got {other:?}"),
            }
            // An element the block does not hold is absent, not an error and not another's value.
            let other = format!("{name}x");
            assert!(
                matches!(
                    select_container_element(&block, &other),
                    ContainerElementRead::Absent
                ),
                "{kind} {other:?}"
            );
            // A tombstone block states the removal.
            let removal = tombstone_block(kind, name).expect("a tombstone for a framed kind");
            assert!(
                matches!(
                    select_container_element(&removal, name),
                    ContainerElementRead::Removed
                ),
                "{kind} {name:?}"
            );
            selected += 1;
        }
    }
    assert_eq!(hash_names.len() + list_names.len() + 2 * member_names.len(), selected);
}

#[test]
fn a_whole_hash_read_answers_the_fields_a_hash_holds_in_order() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    // Written out of order, with an empty name, a non-ASCII name and binary values.
    let fields: Vec<(String, Vec<u8>)> = vec![
        ("b".to_string(), vec![0, 1, 2, 255]),
        ("".to_string(), b"empty-name".to_vec()),
        ("h\u{e9}llo".to_string(), b"non-ascii".to_vec()),
        ("a".to_string(), Vec::new()),
        ("c".to_string(), vec![b'v'; 300]),
    ];
    for (field, value) in &fields {
        run(
            &engine,
            Command::HashSet {
                key: "k".to_string(),
                field: field.clone(),
                value: value.clone(),
            },
        );
    }
    let all = |engine: &TemporalEngine| match run(
        engine,
        Command::HashGetAll {
            key: "k".to_string(),
        },
    ) {
        CommandResponse::HashEntries { entries } => entries,
        other => panic!("not entries: {other:?}"),
    };
    let mut expected = fields.clone();
    expected.sort();
    // Twice: the second answer is served from the cache the first filled.
    for _ in 0..2 {
        let got = all(&engine);
        assert_eq!(expected, got);
    }
    // And one field removed and one overwritten, which is what leaves tombstones in the index.
    run(
        &engine,
        Command::HashDelete {
            key: "k".to_string(),
            field: "b".to_string(),
        },
    );
    run(
        &engine,
        Command::HashSet {
            key: "k".to_string(),
            field: "a".to_string(),
            value: b"now-set".to_vec(),
        },
    );
    expected.retain(|(field, _)| field != "b");
    for (field, value) in expected.iter_mut() {
        if field == "a" {
            *value = b"now-set".to_vec();
        }
    }
    assert_eq!(expected, all(&engine));
    // A key that is not a hash answers nothing.
    assert!(matches!(
        run(&engine, Command::HashGetAll { key: "absent".to_string() }),
        CommandResponse::HashEntries { entries } if entries.is_empty()
    ));
}

#[cfg(feature = "alloc-probe")]
mod probe {
    use super::*;
    use crate::alloc_probe::Probe;

    fn spent(work: impl FnOnce()) -> (u64, u64) {
        let probe = Probe::start();
        work();
        let counts = probe.stop();
        (counts.allocs, counts.alloc_bytes)
    }

    /// Allocations of one lookup in a framed block, for a kind and a name that is, or is not, in it.
    ///
    /// A hash and a list build no key, so an absent element costs NOTHING and a present one costs
    /// the value copy alone. A set decodes its member, which is the control: the same call over a
    /// spelling that still builds a key must show that allocation, or the probe cannot see this
    /// function's allocations at all. The old code spent one more allocation on every row below.
    #[test]
    #[ignore = "measurement: run with --features alloc-probe --ignored"]
    fn a_block_lookup_allocates_only_its_answer() {
        let value = vec![b'v'; 64];
        let mut rows = Vec::new();
        for (kind, present, absent) in [
            ("hash", "field-000001".to_string(), "field-000002".to_string()),
            ("list", "8000000000000001".to_string(), "8000000000000002".to_string()),
            ("set", member_element_name(b"member-1"), member_element_name(b"member-2")),
        ] {
            let block = single_element_block(kind, &present, &value);
            // One warm call, so a lazy initialisation is not charged to the first row.
            let _ = select_container_element(&block, &present);
            let (found_allocs, found_bytes) = spent(|| {
                match select_container_element(&block, &present) {
                    ContainerElementRead::Found(found) => assert_eq!(value, found),
                    other => panic!("{kind}: {other:?}"),
                }
            });
            let (absent_allocs, _) = spent(|| {
                assert!(matches!(
                    select_container_element(&block, &absent),
                    ContainerElementRead::Absent
                ));
            });
            println!("MEAS {kind}: found {found_allocs} allocs {found_bytes} B, absent {absent_allocs}");
            rows.push((kind, found_allocs, found_bytes, absent_allocs));
        }
        let by = |kind: &str| *rows.iter().find(|row| row.0 == kind).unwrap();
        let (_, hash_found, hash_bytes, hash_absent) = by("hash");
        let (_, list_found, _, list_absent) = by("list");
        let (_, set_found, _, set_absent) = by("set");
        // The control: a member that has to be decoded still allocates, so the probe sees keys.
        assert!(set_absent >= 1, "a set lookup decodes its member into an allocation");
        assert_eq!(set_absent + 1, set_found, "that decode, and the value");
        // The change: nothing for the key.
        assert_eq!(0, hash_absent, "a hash lookup of an absent field built a key");
        assert_eq!(0, list_absent, "a list lookup of an absent element built a key");
        assert_eq!(1, hash_found, "a hash lookup spends the value copy and nothing else");
        assert_eq!(1, list_found, "a list lookup spends the value copy and nothing else");
        assert_eq!(value.len() as u64, hash_bytes, "the bytes are the value's own");
    }

    const OPS: usize = 20;

    fn per_read(fields: usize) -> f64 {
        let dir = tempfile::tempdir().expect("tempdir");
        let engine = engine_on(dir.path());
        for index in 0..fields {
            run(
                &engine,
                Command::HashSet {
                    key: "k".to_string(),
                    field: format!("f{index:05}"),
                    value: vec![b'v'; 64],
                },
            );
        }
        let read = || Command::HashGetAll {
            key: "k".to_string(),
        };
        // One warm-up so the span measures the resident answer, not the first fill.
        match run(&engine, read()) {
            CommandResponse::HashEntries { entries } => assert_eq!(fields, entries.len()),
            other => panic!("not entries: {other:?}"),
        }
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

    /// What one more field costs a warm whole-hash read, as the slope between two sizes.
    ///
    /// The answer needs a name, a value and the selector of its page key: three, and it spends
    /// 3.02. Undoing either change puts back exactly one allocation per field (4.02), and undoing
    /// both is the 5.02 it spent before: those three, a copy of the name made to read the container
    /// out, an `Arc<str>` of it to name the block, and the key the point selection searched with. The
    /// limit is the measured slope plus a margin, and the lower bound is the control: a read of the
    /// whole hash must still grow with the field count, or the probe cannot see that dimension.
    const WHOLE_HASH_LIMIT: f64 = 3.5;

    #[test]
    #[ignore = "measurement: run with --features alloc-probe --ignored"]
    fn a_whole_hash_read_spends_no_allocation_on_a_name_it_does_not_return() {
        let short = per_read(20);
        let long = per_read(200);
        let slope = (long - short) / 180.0;
        println!("MEAS whole hash: {short:.1} @20, {long:.1} @200, {slope:.2}/field");
        assert!(
            slope > 2.9,
            "the whole-hash read spent {slope:.2} allocations per field, fewer than the name, the \
             value and the selector the answer needs, so the probe is not measuring the read"
        );
        assert!(
            slope <= WHOLE_HASH_LIMIT,
            "the whole-hash read spent {slope:.2} allocations per field; it spent about 5.0 when each \
             field was copied out of the container, named again for its block and searched for \
             with a key of its own"
        );
    }
}
