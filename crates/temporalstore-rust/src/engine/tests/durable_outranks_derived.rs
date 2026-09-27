// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A DURABLE MAP MUST OUTRANK A NAME DERIVED FROM IT.
//!
//! # THE DEFECT
//!
//! `reconcile_secondary_views_from_bucket_index` rebuilds the model maps from the bucket index, and
//! for three kinds it recovers an element's identity by PARSING the component name the write path
//! spelled it into -- then assigned the result over the top:
//!
//! ```text
//!     if saw_lists { shard.lists = lists; }
//!     if saw_zsets { shard.zsets = zsets; }
//!     if saw_sets  { shard.sets  = sets;  }
//! ```
//!
//! All three of those maps are PERSISTED. So a stored value was being reconstructed from a second
//! copy of itself rendered as text, and the durable copy had no say. Driven below: a zset written at
//! score 7.5 comes back at 99.25 when only its NAME is changed.
//!
//! # WHAT EACH DURABLE MAP HOLDS, AND WHICH KINDS HAVE ONE
//!
//! ```text
//!     set     set_index_serde      (member bytes, address)                 the member's identity
//!     zset    zset_index_serde     (member bytes, (score, address))        identity AND score
//!     list    plain serde          (i64 sequence, address)                 the sequence
//!     string  #[serde(skip)]       nothing                 no component at all; the key IS identity
//!     hash    skip_serializing     nothing written         the component IS the caller's field name
//! ```
//!
//! So three kinds have a durable copy of what their name spells and two genuinely do not -- and for
//! the two, deriving is not a second copy of anything: a string has no component, and a hash field is
//! the caller's own text rather than something this engine rendered. They are left alone.
//!
//! # WHY THE RULE IS NOT THE ONE `control_state` USES
//!
//! The `control_state` arm keeps the persisted series wholesale for every key it has, saying why: "the
//! serialized i64 series is authoritative (the page is a copy of it)". Copying that rule here would be
//! wrong. `apply_key_states` folds `features` and the control-state maps out of the delta log and NOT
//! `sets`, `zsets` or `lists` -- so for these three the derived view is the ONLY path by which a
//! folded element arrives, and taking the durable map wholesale per key would drop exactly those.
//!
//! The rule is therefore per ELEMENT: the derived view decides which elements exist and which page
//! backs each, because it reflects the fold; the durable map supplies what the name merely re-spells,
//! and keeps any element the derived view could not produce.
//!
//! # AND THREE SILENT GUESSES BECOME SKIPS
//!
//! Each arm handled an unreadable name differently and none of them said so. A set's became
//! `unwrap_or_default()` -- the EMPTY member, a real value that then took a genuine element's address.
//! A list's became SEQUENCE ZERO, a real position whose entry it overwrote. A zset's was dropped. They
//! are skipped and counted now, and skipping is safe precisely because the durable map keeps the
//! element.
//!
//! # WHY THIS IS NOT THEORETICAL
//!
//! A list component is a single `u64` spelled in hexadecimal, and #1985 measured that 100% of list
//! names are also well-formed values of a different meaning. A view built by parsing names is one
//! ambiguity away from a wrong value, and the test below shows the wrong value winning over the
//! durable one.

#![allow(clippy::all)]
use super::*;

const OPERATOR_END: u32 = crate::DEFAULT_END_ROUTING_BUCKET;

fn engine_on(dir: &std::path::Path) -> TemporalEngine {
    TemporalEngine::with_local_dirs(
        64 * 1024 * 1024,
        dir.join("cache"),
        dir.join("pages"),
        dir.join("indexes"),
    )
}

fn load_on(engine: &TemporalEngine, end_routing_bucket: u32) {
    let response = engine.load_shard_with(crate::control::LoadShardRequest {
        shard_id: 1,
        table_name: "durable-outranks".to_string(),
        shard_uri: "local://durable-outranks/1".to_string(),
        start_routing_bucket: 0,
        end_routing_bucket,
        readonly: false,
        load_version: 1,
        local_node_id: Some(1),
    });
    assert!(
        response.status.ok,
        "the fixture could not load shard 1: {:?}",
        response.status
    );
}

fn write(engine: &TemporalEngine, command: Command) {
    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command,
    });
    assert!(response.status.ok, "the fixture write failed: {response:?}");
}

fn read(engine: &TemporalEngine, command: Command) -> crate::types::CommandResponse {
    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command,
    });
    assert!(response.status.ok, "a read failed: {response:?}");
    response.response
}

/// The component name a zset member is filed under, as the write path spells it.
fn zset_name(score: f64, member: &[u8]) -> String {
    format!(
        "{:016x}{}",
        crate::engine::execute_on_shard::zset_score_bits(score),
        hex::encode(member)
    )
}

/// Replace every occurrence of `from` with `to` inside a served index, whatever payload it carries.
///
/// `None` when the file holds no such text. Both codecs are handled and the JSON one is compressed,
/// so a byte search over the frame would find nothing and silently report success. EVERY occurrence,
/// because `block_index_written_key` renders the component into the map key the index is serialized
/// under as well as into the entry -- replacing one would leave the two naming different elements,
/// which is a corrupt index rather than a controlled experiment.
fn swap_in_index(bytes: &[u8], from: &str, to: &str) -> Option<(Vec<u8>, usize)> {
    const MAGIC: &[u8] = b"TSIDX\x01";
    let swap = |plain: &[u8]| -> Option<(Vec<u8>, usize)> {
        let needle = from.as_bytes();
        let mut out = Vec::with_capacity(plain.len());
        let mut swaps = 0usize;
        let mut at = 0usize;
        while at < plain.len() {
            if plain[at..].starts_with(needle) {
                out.extend_from_slice(to.as_bytes());
                at += needle.len();
                swaps += 1;
                continue;
            }
            out.push(plain[at]);
            at += 1;
        }
        (swaps > 0).then_some((out, swaps))
    };
    if !bytes.starts_with(MAGIC) {
        return swap(bytes);
    }
    let codec = *bytes.get(MAGIC.len())?;
    let payload = &bytes[MAGIC.len() + 1..];
    let (header, body) = match codec {
        1 => (0usize, payload),
        2 => (4usize, payload.get(4..)?),
        _ => return None,
    };
    let plain = zstd::stream::decode_all(body).ok()?;
    let (swapped, swaps) = swap(&plain)?;
    let recompressed = zstd::stream::encode_all(swapped.as_slice(), 3).ok()?;
    let mut out = Vec::with_capacity(MAGIC.len() + 1 + header + recompressed.len());
    out.extend_from_slice(MAGIC);
    out.push(codec);
    if header == 4 {
        out.extend_from_slice(&payload[..4]);
    }
    out.extend_from_slice(&recompressed);
    Some((out, swaps))
}

/// Swap a name across every index file, and answer how many occurrences moved.
fn swap_across_index_files(indexes: &std::path::Path, from: &str, to: &str) -> usize {
    let mut total = 0usize;
    for entry in std::fs::read_dir(indexes).expect("the index directory exists") {
        let path = entry.expect("a directory entry").path();
        if !path.is_file() {
            continue;
        }
        let bytes = std::fs::read(&path).expect("the index reads");
        if let Some((swapped, swaps)) = swap_in_index(&bytes, from, to) {
            std::fs::write(&path, &swapped).expect("the index rewrites");
            println!("  swapped {swaps} occurrence(s) in {:?}", path.file_name());
            total += swaps;
        }
    }
    total
}

// =============================================================================================
// 1. THE TEST THAT IS THE WHOLE POINT
// =============================================================================================

/// THE DURABLE SCORE WINS OVER A NAME THAT DISAGREES WITH IT.
///
/// This is the same experiment that found the defect, with the assertion the other way round. A zset
/// is written at 7.5; the component name in the served index is then changed to spell 99.25, and
/// nothing else is touched -- the persisted `zsets` map still holds 7.5 beside the member. On reload
/// the answer must be **7.5**.
///
/// Before this change it was 99.25: `shard.zsets = zsets` replaced the durable map with a view built
/// by parsing names, so a stored value lost to a second copy of itself rendered as text.
///
/// rust-internal: mutates the engine's own served index, no external surface
#[test]
fn a_durable_zset_score_outranks_a_component_name_that_disagrees() {
    let dir = tempfile::tempdir().unwrap();
    let indexes = dir.path().join("indexes");
    let member = b"outranked-member".to_vec();
    let durable_score = 7.5f64;
    let name_score = 99.25f64;

    {
        let engine = engine_on(dir.path());
        load_on(&engine, OPERATOR_END);
        write(
            &engine,
            Command::ZSetAdd {
                key: "do-zset".to_string(),
                member: member.clone(),
                score: durable_score,
            },
        );
        engine.unload_shard(1);
    }

    let from = zset_name(durable_score, &member);
    let to = zset_name(name_score, &member);
    assert_eq!(
        from.len(),
        to.len(),
        "the two names differ in length, so the swap would not be a pure substitution"
    );
    assert_ne!(from, to, "the two scores spell the same name");
    println!("[outrank] name {from:?} -> {to:?} (member half unchanged)");
    let swaps = swap_across_index_files(&indexes, &from, &to);
    assert!(
        swaps > 0,
        "the name was not found in any index file, so nothing was mutated and this test would pass \
         without testing anything"
    );

    let engine = engine_on(dir.path());
    load_on(&engine, OPERATOR_END);
    let answered = match read(
        &engine,
        Command::ZSetScore {
            key: "do-zset".to_string(),
            member: member.clone(),
        },
    ) {
        crate::types::CommandResponse::Bytes { value: Some(bytes) } => {
            String::from_utf8_lossy(&bytes).to_string()
        }
        other => panic!(
            "the member did not come back at all: {other:?}. That is a different defect than the \
             score's SOURCE -- investigate rather than adjusting this test."
        ),
    };
    let score: f64 = answered
        .parse()
        .unwrap_or_else(|_| panic!("a score came back as {answered:?}"));
    println!(
        "[outrank] wrote {durable_score}, changed the NAME to spell {name_score}, left the durable \
         map alone; the reload answered {score}"
    );
    assert!(
        (score - durable_score).abs() < 1e-9,
        "the reload answered {score}, not the durable {durable_score}. If it answered {name_score} \
         the derived name is authoritative again and this fix has been undone."
    );
}

/// AND THE SAME THING THROUGH THE OTHER DOOR: A RELEASE AND RELOAD, NOT A PROCESS RESTART.
///
/// `reconcile_secondary_views_from_bucket_index` is reached from five places -- two on the load path
/// in `persistence.rs`, and three on reconstruct (`lifecycle.rs`, `stream_batch_methods.rs`,
/// `engine.rs`). A fix proved on one door says nothing about the other, and a sibling has found these
/// two behaving differently before. This drives the reconstruct door by unloading and reloading the
/// shard inside ONE engine, so no file is re-read from scratch.
///
/// rust-internal: drives the engine's own unload/load cycle, no external surface
#[test]
fn the_durable_score_outranks_the_name_through_the_reconstruct_door_as_well() {
    let dir = tempfile::tempdir().unwrap();
    let member = b"reconstruct-member".to_vec();
    let engine = engine_on(dir.path());
    load_on(&engine, OPERATOR_END);
    write(
        &engine,
        Command::ZSetAdd {
            key: "rc-zset".to_string(),
            member: member.clone(),
            score: 3.25,
        },
    );

    // Unload and load again in the same engine: the reconstruct path, not a fresh process.
    engine.unload_shard(1);
    load_on(&engine, OPERATOR_END);
    let after_first = match read(
        &engine,
        Command::ZSetScore {
            key: "rc-zset".to_string(),
            member: member.clone(),
        },
    ) {
        crate::types::CommandResponse::Bytes { value: Some(bytes) } => {
            String::from_utf8_lossy(&bytes).to_string()
        }
        other => panic!("the member did not survive a release and reload: {other:?}"),
    };
    let score: f64 = after_first.parse().expect("a score parses");
    println!("[door] after a release and reload the score is {score}");
    assert!(
        (score - 3.25).abs() < 1e-9,
        "the reconstruct door answered {score}, not 3.25"
    );

    // And a SECOND cycle, because the first may have been served from a map that was never rebuilt.
    engine.unload_shard(1);
    load_on(&engine, OPERATOR_END);
    let again = match read(
        &engine,
        Command::ZSetScore {
            key: "rc-zset".to_string(),
            member: member.clone(),
        },
    ) {
        crate::types::CommandResponse::Bytes { value: Some(bytes) } => {
            String::from_utf8_lossy(&bytes).to_string()
        }
        other => panic!("the member did not survive a second cycle: {other:?}"),
    };
    let score: f64 = again.parse().expect("a score parses");
    assert!(
        (score - 3.25).abs() < 1e-9,
        "the second cycle answered {score}, not 3.25"
    );
    println!("[door] and again after a second cycle: {score}");
}

// =============================================================================================
// 2. ALL THREE KINDS, AND THE TWO THAT GENUINELY HAVE ONLY A NAME
// =============================================================================================

/// EVERY KIND WHOSE ELEMENT IDENTITY IS SPELLED INTO A NAME SURVIVES A RELOAD INTACT.
///
/// `set`, `zset` and `list` all have a durable map and all three used to be assigned over. `string`
/// and `hash` have none, and for them deriving is not a second copy of anything -- a string has no
/// component and a hash's component IS the caller's field name. Both are included so the fix is shown
/// not to have moved them.
///
/// rust-internal: drives a reload, no external surface
#[test]
fn all_three_spelled_kinds_and_the_two_without_a_durable_map_survive_a_reload() {
    let dir = tempfile::tempdir().unwrap();
    let members: Vec<Vec<u8>> = (0..7)
        .map(|element| format!("kind-member-{element:03}").into_bytes())
        .collect();

    {
        let engine = engine_on(dir.path());
        load_on(&engine, OPERATOR_END);
        for (element, member) in members.iter().enumerate() {
            write(
                &engine,
                Command::ZSetAdd {
                    key: "ak-zset".to_string(),
                    member: member.clone(),
                    score: element as f64 + 0.5,
                },
            );
            write(
                &engine,
                Command::SetAdd {
                    key: "ak-set".to_string(),
                    member: member.clone(),
                },
            );
            write(
                &engine,
                Command::ListPush {
                    key: "ak-list".to_string(),
                    member: format!("entry-{element}").into_bytes(),
                    left: false,
                },
            );
            write(
                &engine,
                Command::HashSet {
                    key: "ak-hash".to_string(),
                    field: format!("field-{element}"),
                    value: format!("value-{element}").into_bytes(),
                },
            );
        }
        write(
            &engine,
            Command::StringSet {
                key: "ak-string".to_string(),
                value: b"string-value".to_vec(),
            },
        );
        engine.unload_shard(1);
    }

    let engine = engine_on(dir.path());
    load_on(&engine, OPERATOR_END);

    // zset: identity AND score, both from the durable map.
    for (element, member) in members.iter().enumerate() {
        let answered = match read(
            &engine,
            Command::ZSetScore {
                key: "ak-zset".to_string(),
                member: member.clone(),
            },
        ) {
            crate::types::CommandResponse::Bytes { value: Some(bytes) } => {
                String::from_utf8_lossy(&bytes).to_string()
            }
            other => panic!("zset member {element} is gone after the reload: {other:?}"),
        };
        let score: f64 = answered.parse().expect("a score parses");
        assert!(
            (score - (element as f64 + 0.5)).abs() < 1e-9,
            "zset member {element} came back at {score}, not {}",
            element as f64 + 0.5
        );
    }

    // set: the member's identity. The empty member must NOT appear -- that is what an unreadable
    // name used to become.
    let set_members = match read(
        &engine,
        Command::SetMembers {
            key: "ak-set".to_string(),
        },
    ) {
        crate::types::CommandResponse::Members { members } => members,
        other => panic!("a set read answered {other:?}"),
    };
    assert_eq!(
        set_members.len(),
        members.len(),
        "the set came back with {} of {} members",
        set_members.len(),
        members.len()
    );
    assert!(
        !set_members.iter().any(|member| member.is_empty()),
        "the set came back holding the EMPTY member, which is what an unreadable name used to \
         become: {set_members:?}"
    );
    for member in &members {
        assert!(
            set_members.iter().any(|held| held == member),
            "the set lost {:?}",
            String::from_utf8_lossy(member)
        );
    }

    // list: the sequence, and in order. A name that could not be read used to become sequence ZERO
    // and overwrite whatever was there, so the ORDER is the thing to check.
    let list = match read(
        &engine,
        Command::ListRange {
            key: "ak-list".to_string(),
            start: 0,
            stop: -1,
        },
    ) {
        crate::types::CommandResponse::Members { members } => members,
        other => panic!("a list read answered {other:?}"),
    };
    assert_eq!(
        list.len(),
        members.len(),
        "the list came back with {} of {} entries",
        list.len(),
        members.len()
    );
    for (element, held) in list.iter().enumerate() {
        assert_eq!(
            held.as_slice(),
            format!("entry-{element}").as_bytes(),
            "list position {element} came back as {:?}",
            String::from_utf8_lossy(held)
        );
    }

    // hash: no durable map, and none needed -- the component IS the caller's field name.
    for element in 0..members.len() {
        let expected = format!("value-{element}").into_bytes();
        assert!(
            matches!(
                read(
                    &engine,
                    Command::HashGet {
                        key: "ak-hash".to_string(),
                        field: format!("field-{element}"),
                    },
                ),
                crate::types::CommandResponse::Bytes { value: Some(ref got) } if *got == expected
            ),
            "hash field {element} did not come back"
        );
    }

    // string: no component at all.
    assert!(
        matches!(
            read(&engine, Command::StringGet { key: "ak-string".to_string() }),
            crate::types::CommandResponse::Bytes { value: Some(ref got) } if got == b"string-value"
        ),
        "the string did not come back"
    );

    println!(
        "[kinds] {} zset members with their scores, {} set members with no empty member, {} list \
         entries in order, {} hash fields and the string all came back",
        members.len(),
        set_members.len(),
        list.len(),
        members.len()
    );
}

// =============================================================================================
// 3. THE FALLBACK: DURABLE WINS WHERE PRESENT, NOT "NEVER DERIVE"
// =============================================================================================

/// AN ELEMENT WITH NO DURABLE ENTRY STILL COMES BACK FROM ITS NAME.
///
/// The rule is "the durable map wins where it has the element", not "never derive". It has to be:
/// `apply_key_states` folds `features` and the control-state maps out of the delta log and NOT
/// `sets`, `zsets` or `lists`, so an element the fold added reaches these maps ONLY through the
/// derived view. A rule that preferred the durable map per KEY -- which is what the `control_state`
/// arm does, for a reason that holds there -- would drop every one of them.
///
/// Driven directly on the merge, because constructing a half-folded store through the public surface
/// would be a fixture with more moving parts than the property it checks.
///
/// rust-internal: reads the merge the load path uses, no product behaviour
#[test]
fn an_element_the_durable_map_does_not_hold_still_comes_back_from_its_name() {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine_on(dir.path());
    load_on(&engine, OPERATOR_END);

    // Two members of one zset, written normally, so both are durable and both have names.
    for (element, member) in [b"fb-one".to_vec(), b"fb-two".to_vec()].iter().enumerate() {
        write(
            &engine,
            Command::ZSetAdd {
                key: "fb-zset".to_string(),
                member: member.clone(),
                score: element as f64 + 1.0,
            },
        );
    }

    // Now DROP one of them from the durable map only, leaving its page entry and its name in the
    // bucket index. That is the shape a delta fold produces: an element the index knows about and the
    // persisted map does not.
    {
        let mut shards = engine.shards.write().expect("engine lock poisoned");
        let shard = shards.get_mut(&1).expect("shard is loaded");
        let removed = shard
            .zsets
            .get_mut("fb-zset")
            .expect("the zset is present")
            .remove(b"fb-two".as_slice());
        assert!(
            removed.is_some(),
            "the durable map did not hold fb-two, so this fixture cannot create the state it needs"
        );
        println!("[fallback] removed fb-two from the durable map, leaving its page entry");
    }

    // A release and reload runs the merge. The element the durable map no longer holds must come
    // back from its NAME rather than vanishing.
    engine.unload_shard(1);
    load_on(&engine, OPERATOR_END);

    for (member, expected) in [(b"fb-one".to_vec(), 1.0f64), (b"fb-two".to_vec(), 2.0f64)] {
        let answered = match read(
            &engine,
            Command::ZSetScore {
                key: "fb-zset".to_string(),
                member: member.clone(),
            },
        ) {
            crate::types::CommandResponse::Bytes { value: Some(bytes) } => {
                String::from_utf8_lossy(&bytes).to_string()
            }
            other => panic!(
                "{:?} did not come back: {other:?}. If it is fb-two, the merge is dropping elements \
                 the durable map does not hold -- which is the regression a per-KEY rule would have \
                 caused, and the whole reason this rule is per element.",
                String::from_utf8_lossy(&member)
            ),
        };
        let score: f64 = answered.parse().expect("a score parses");
        assert!(
            (score - expected).abs() < 1e-9,
            "{:?} came back at {score}, not {expected}",
            String::from_utf8_lossy(&member)
        );
        println!(
            "[fallback] {:?} came back at {score}",
            String::from_utf8_lossy(&member)
        );
    }
    println!(
        "[fallback] so the derived view remains the fallback: durable wins where present, and an \
         element it does not hold still arrives through its name"
    );
}

/// AN UNREADABLE NAME LOSES NOTHING, BECAUSE THE DURABLE MAP KEEPS THE ELEMENT.
///
/// This is what the merge is for, and the only case in which it is load-bearing -- a mutant that
/// reverted it to an assignment survived every other test here, because those fixtures leave the
/// element missing from the DURABLE map rather than from the derived view.
///
/// THE EXPERIMENT. Write a set, then corrupt one member's component name in the served index so
/// `hex::decode` cannot read it, keeping the length so nothing else shifts. On reload the derived view
/// skips that element; the durable map still holds it, and the merge puts it back.
///
/// TWO THINGS ARE ASSERTED, and the second is the older defect. The member must come back -- and the
/// EMPTY member must not appear, because the set arm used to end `.unwrap_or_default()`, which turned
/// a name it could not read into the empty member: a real value that then took this element's address.
///
/// rust-internal: mutates the engine's own served index, no external surface
#[test]
fn an_unreadable_component_name_is_skipped_and_the_durable_map_keeps_the_element() {
    let dir = tempfile::tempdir().unwrap();
    let indexes = dir.path().join("indexes");
    let kept = b"kept-member".to_vec();
    let corrupted = b"corrupted-member".to_vec();

    {
        let engine = engine_on(dir.path());
        load_on(&engine, OPERATOR_END);
        for member in [&kept, &corrupted] {
            write(
                &engine,
                Command::SetAdd {
                    key: "un-set".to_string(),
                    member: member.clone(),
                },
            );
        }
        engine.unload_shard(1);
    }

    // A set's component name is `hex::encode(member)`. Swapping one character for `z` keeps the
    // length -- so nothing else in the payload shifts -- and makes it undecodable.
    let from = hex::encode(&corrupted);
    let mut to = from.clone();
    to.replace_range(0..1, "z");
    assert_eq!(from.len(), to.len(), "the corruption changed the length");
    assert!(
        hex::decode(&to).is_err(),
        "{to:?} still decodes, so this fixture has not made an unreadable name"
    );
    println!("[unreadable] name {from:?} -> {to:?}");
    let swaps = swap_across_index_files(&indexes, &from, &to);
    assert!(
        swaps > 0,
        "the name was not found in any index file, so nothing was corrupted and this test would pass \
         without testing anything"
    );

    let engine = engine_on(dir.path());
    load_on(&engine, OPERATOR_END);
    let members = match read(
        &engine,
        Command::SetMembers {
            key: "un-set".to_string(),
        },
    ) {
        crate::types::CommandResponse::Members { members } => members,
        other => panic!("a set read answered {other:?}"),
    };
    println!(
        "[unreadable] the set came back with {} member(s): {:?}",
        members.len(),
        members
            .iter()
            .map(|member| String::from_utf8_lossy(member).to_string())
            .collect::<Vec<_>>()
    );
    assert!(
        !members.iter().any(|member| member.is_empty()),
        "the set came back holding the EMPTY member. That is what an unreadable name used to become \
         -- a real value taking a genuine element's address -- so this is the older defect returning."
    );
    assert!(
        members.iter().any(|member| member == &kept),
        "the member whose name was left alone is gone, which is a different failure than this test is \
         for"
    );
    assert!(
        members.iter().any(|member| member == &corrupted),
        "the member whose NAME was corrupted is gone. The derived view cannot read it and the durable \
         map still holds it, so the merge is what brings it back -- if this fails, the merge has been \
         reverted to an assignment."
    );
    assert_eq!(
        members.len(),
        2,
        "the set came back with {} members rather than 2",
        members.len()
    );
    println!(
        "[unreadable] both members came back: the unreadable name was skipped rather than defaulted, \
         and the durable map supplied the element the derived view could not"
    );

    // AND IT MUST STILL BE REMOVABLE, which is the door the merge is actually for.
    //
    // A set's member is SERVED from its page: `SetMembers` walks
    // `bucket_index_component_block_addresses` and reads the bytes, so it answers whether or not
    // `shard.sets` holds the element. `shard.sets` has exactly one reader in the command surface --
    // `SetRemove` -- so that is where a missing element shows. Without the merge the derived view has
    // skipped this member, the remove cannot find it, and the set holds a member nobody can delete.
    let removed = read(
        &engine,
        Command::SetRemove {
            key: "un-set".to_string(),
            member: corrupted.clone(),
        },
    );
    println!("[unreadable] removing the member whose name is unreadable answered {removed:?}");
    // Asserted on the EFFECT, not on the answer: `SetRemove` answers `Empty`, and what matters is
    // whether the member stops being served.
    let after = match read(
        &engine,
        Command::SetMembers {
            key: "un-set".to_string(),
        },
    ) {
        crate::types::CommandResponse::Members { members } => members,
        other => panic!("a set read answered {other:?}"),
    };
    assert!(
        !after.iter().any(|member| member == &corrupted),
        "the member whose NAME is unreadable is STILL SERVED after a remove: {after:?}. \
         `shard.sets` is the only map `SetRemove` consults, so without the merge the derived view's \
         skip leaves a member that is served from its page and cannot be deleted."
    );
    assert!(
        after.iter().any(|member| member == &kept),
        "the removal took the wrong member"
    );
    println!(
        "[unreadable] and it removed cleanly, leaving {} member(s) -- so the merge is what makes an \
         unreadable name a skip rather than a leak",
        after.len()
    );
}
