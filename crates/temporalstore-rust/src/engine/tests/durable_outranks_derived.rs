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
pub(super) fn swap_across_index_files(indexes: &std::path::Path, from: &str, to: &str) -> usize {
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

// =================================================================================================
// THE FOLD CARRIES ELEMENT IDENTITY, AND A SIXTEEN-CHARACTER COMPONENT IS A WHOLE ONE
// =================================================================================================

/// AN EMPTY ZSET MEMBER SPELLS EXACTLY SIXTEEN CHARACTERS, AND THE RECONCILE REFUSED TO READ IT.
///
/// `zset_component` is `{biased:016x}` followed by `hex::encode(member)`. A member of zero bytes --
/// which nothing on the write path rejects -- therefore spells EXACTLY sixteen characters, and the
/// reconcile's zset arm opened `if component.len() <= 16 { return None }`. So that component decoded
/// to nothing, the element was counted in `unreadable_names` and skipped, and on the one door where
/// the durable map does not already hold the element -- the delta fold, whose records carry the
/// elements written after the base snapshot -- the member was silently gone on reload.
///
/// THE ENGINE ALREADY SPELLS THE SAME BOUNDARY THE OTHER WAY. Both the insert arm and the removal
/// arm of `apply_outcome_item` ask `component.len() < 16`, so WAL replay accepted the very component
/// the reconcile refused. Two readers of one encoding disagreeing about its shortest legal form is
/// the defect, and this was the side that was wrong: sixteen characters is a complete score with an
/// empty member after it, and `hex::decode("")` is `Ok(vec![])`.
///
/// # THE EXPERIMENT, WHICH IS `an_element_the_durable_map_does_not_hold_still_comes_back_from_its_name`
/// # WITH AN EMPTY MEMBER
///
/// Two members, one of them empty. Both are written normally, so the fixture first PROVES an empty
/// member is reachable and durable rather than assuming it. Then the empty one is dropped from the
/// durable map only -- the shape a fold produces -- and the shard is reloaded.
///
///   * The non-empty member comes back. THE CONTROL: this arm is untouched by the boundary, so a
///     fixture that lost the whole key cannot pass as this finding.
///   * The empty member comes back too. Before the boundary was corrected it did not, and could
///     not: the reconcile skipped it and the durable map had nothing.
///
/// rust-internal: mutates the engine's own in-memory index, no external surface
#[test]
fn an_empty_zset_member_is_a_whole_component_and_survives_the_fold_shape() {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine_on(dir.path());
    load_on(&engine, OPERATOR_END);

    let empty: Vec<u8> = Vec::new();
    let control = b"mt-control".to_vec();
    for (member, score) in [(empty.clone(), 3.0f64), (control.clone(), 1.0f64)] {
        write(
            &engine,
            Command::ZSetAdd {
                key: "mt-zset".to_string(),
                member,
                score,
            },
        );
    }

    // THE DENOMINATOR, and the reachability claim. An empty member has to be accepted and durable,
    // or the loss below is about a state no caller can reach.
    let empty_component_len = {
        let shards = engine.shards.read().expect("engine lock poisoned");
        let shard = shards.get(&1).expect("shard is loaded");
        let members = shard.zsets.get("mt-zset").expect("the zset is present");
        assert_eq!(
            members.len(),
            2,
            "the fixture stored {} member(s), not the two it needs -- if the empty member was \
             rejected on the way in, this finding is about an unreachable state",
            members.len()
        );
        assert!(
            members.contains_key(empty.as_slice()),
            "the empty member is not in the durable map, so nothing here is about a live path"
        );
        zset_name(3.0, &empty).len()
    };
    assert_eq!(
        empty_component_len, 16,
        "an empty member's component is {empty_component_len} characters, not the sixteen this \
         finding rests on -- re-read `zset_component` before trusting the boundary below"
    );
    println!(
        "[empty-member] both members are durable, and the empty one's component is \
         {empty_component_len} characters"
    );

    // The shape a fold produces for ONE element: a page entry the index knows about, with no
    // durable map entry behind it.
    {
        let mut shards = engine.shards.write().expect("engine lock poisoned");
        let shard = shards.get_mut(&1).expect("shard is loaded");
        let removed = shard
            .zsets
            .get_mut("mt-zset")
            .expect("the zset is present")
            .remove(empty.as_slice());
        assert!(
            removed.is_some(),
            "the durable map did not hold the empty member, so this fixture cannot build the state \
             a fold produces"
        );
        println!("[empty-member] dropped the empty member from the durable map only");
    }

    engine.unload_shard(1);
    load_on(&engine, OPERATOR_END);

    let score_of = |member: &[u8]| -> Option<f64> {
        match read(
            &engine,
            Command::ZSetScore {
                key: "mt-zset".to_string(),
                member: member.to_vec(),
            },
        ) {
            crate::types::CommandResponse::Bytes { value: Some(bytes) } => {
                String::from_utf8_lossy(&bytes).parse::<f64>().ok()
            }
            _ => None,
        }
    };

    let control_score = score_of(&control);
    let empty_score = score_of(&empty);
    println!("[empty-member] after the reload: control={control_score:?} empty={empty_score:?}");

    // THE CONTROL, at the score it was written with and unaffected by the boundary.
    assert_eq!(
        control_score,
        Some(1.0),
        "the non-empty member did not come back either, so this fixture lost the whole key and says \
         nothing about the sixteen-character boundary"
    );

    // THE FINDING.
    assert_eq!(
        empty_score,
        Some(3.0),
        "the empty member came back as {empty_score:?}. Its component is exactly sixteen characters, \
         so a reconcile asking `component.len() <= 16` skips it -- and with no durable entry behind \
         it the member is silently gone. WAL replay accepts the same component at `< 16`."
    );
}

/// THE DELTA RECORD CARRIES THE MEMBER BYTES ITS COMPONENT MERELY SPELLS.
///
/// `apply_key_states` folded thirteen maps and `sets`, `zsets`, `lists` and `hashes` were not among
/// them, while `fold_delta_block_items` DID restore the page items -- so after a fold the bucket
/// index held pages for elements whose durable-map entry was never written, and the component its
/// page was filed under was the only copy of the member on that path. It is a DECODABLE copy on this
/// revision, so nothing was lost by it; #1996 had to rewrite a component into an ordinal shape
/// deliberately to lose one. This is the prerequisite that stops identity depending on that.
///
/// ASSERTED ON THE RECORD ON DISK, not on the function that builds it, because the question is what
/// a reload will find. The log is read back through the store's own reader and the carried bytes are
/// compared with the member the caller sent.
///
/// rust-internal: reads the engine's own index log, no external surface
#[test]
fn a_delta_record_carries_the_container_element_a_write_touched() {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine_on(dir.path());
    load_on(&engine, OPERATOR_END);

    let set_member = b"carried-set-member".to_vec();
    let zset_member = b"carried-zset-member".to_vec();
    write(
        &engine,
        Command::SetAdd {
            key: "carry-set".to_string(),
            member: set_member.clone(),
        },
    );
    write(
        &engine,
        Command::ZSetAdd {
            key: "carry-zset".to_string(),
            member: zset_member.clone(),
            score: 4.5,
        },
    );

    let records = engine
        .index_log_store
        .read_delta_records(1, 0)
        .expect("the delta log reads back");
    assert!(
        records.len() >= 2,
        "DENOMINATOR: the log holds {} delta record(s); with fewer than two the assertions below \
         are scoring an empty read",
        records.len()
    );

    // Gather every carried element across the log, keyed by the blob field that carried it.
    let mut carried_set: Vec<Vec<u8>> = Vec::new();
    let mut carried_zset: Vec<(Vec<u8>, u64)> = Vec::new();
    let mut blobs_seen = 0usize;
    for record in &records {
        for blob in &record.key_states {
            blobs_seen += 1;
            if let Some(elements) = blob.get("set_elements").and_then(|v| v.as_array()) {
                for element in elements {
                    if let Some(member) = element.get(0) {
                        if let Ok(bytes) = serde_json::from_value::<Vec<u8>>(member.clone()) {
                            carried_set.push(bytes);
                        }
                    }
                }
            }
            if let Some(elements) = blob.get("zset_elements").and_then(|v| v.as_array()) {
                for element in elements {
                    let member = element
                        .get(0)
                        .and_then(|m| serde_json::from_value::<Vec<u8>>(m.clone()).ok());
                    let score = element.get(1).and_then(|s| s.get(0)).and_then(|s| s.as_u64());
                    if let (Some(member), Some(score)) = (member, score) {
                        carried_zset.push((member, score));
                    }
                }
            }
        }
    }
    println!(
        "[carry] {} record(s), {blobs_seen} key-state blob(s), {} carried set element(s), {} \
         carried zset element(s)",
        records.len(),
        carried_set.len(),
        carried_zset.len()
    );

    assert!(
        carried_set.contains(&set_member),
        "no record carried the set member the caller sent. Carried: {carried_set:?}"
    );
    let expected_score = crate::engine::execute_on_shard::zset_score_bits(4.5);
    assert!(
        carried_zset.contains(&(zset_member.clone(), expected_score)),
        "no record carried the zset member with the score the durable map holds. Carried: \
         {carried_zset:?}, wanted {:?} at {expected_score}",
        String::from_utf8_lossy(&zset_member)
    );
    println!(
        "[carry] the record carries the member BYTES and the score the durable map holds, so a fold \
         does not have to decode the component to know either"
    );
}

/// A RECORD THAT CARRIES NO ELEMENTS CHANGES NOTHING, WHICH IS WHAT AN OLD LOG IS.
///
/// The carry rides in the delta record's existing opaque per-key state channel and is matched BY
/// NAME, so a record written before these fields existed simply does not have them. That is the
/// whole compatibility story and it is asserted rather than argued: nothing positional moves, no
/// reader has to be upgraded first, and `SHARD_INDEX_FORMAT_VERSION` does not change -- so an old
/// log is not refused, it folds exactly as it always did.
///
/// TWO ARMS, and the second is the one that could have been a silent disaster. Every one of the
/// thirteen maps `apply_key_states` folds is restored through `apply_key_state_field`, which treats
/// an ABSENT field as "this key had none" and REMOVES the entry. Had the container maps been folded
/// that way, an old record -- or any record for a key whose container this write did not touch --
/// would have DELETED the whole collection. `merge_container_elements` inserts and never removes,
/// and this asserts that at 0.00%: a blob naming only its key leaves all four maps exactly as they
/// were.
///
/// rust-internal: calls the engine's own fold directly, no external surface
#[test]
fn a_key_state_blob_that_carries_no_elements_leaves_the_container_maps_untouched() {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine_on(dir.path());
    load_on(&engine, OPERATOR_END);

    // A member in each of the four container maps, written normally.
    write(
        &engine,
        Command::SetAdd {
            key: "old-log".to_string(),
            member: b"set-member".to_vec(),
        },
    );
    write(
        &engine,
        Command::ZSetAdd {
            key: "old-log".to_string(),
            member: b"zset-member".to_vec(),
            score: 2.5,
        },
    );
    write(
        &engine,
        Command::ListPush {
            key: "old-log".to_string(),
            member: b"list-element".to_vec(),
            left: false,
        },
    );
    write(
        &engine,
        Command::HashSet {
            key: "old-log".to_string(),
            field: "hash-field".to_string(),
            value: b"hash-value".to_vec(),
        },
    );

    let mut shards = engine.shards.write().expect("engine lock poisoned");
    let shard = shards.get_mut(&1).expect("shard is loaded");
    let before = (
        shard.sets.clone(),
        shard.zsets.clone(),
        shard.lists.clone(),
        shard.hashes.clone(),
    );
    let populated: usize = before.0.len() + before.1.len() + before.2.len() + before.3.len();
    assert_eq!(
        populated, 4,
        "DENOMINATOR: {populated} of the four container maps hold this key; with fewer than four \
         an unchanged comparison below would be comparing empty maps"
    );

    // EXACTLY WHAT AN OLD RECORD'S BLOB IS: a key, and none of the fields that did not exist yet.
    // Through BOTH halves of the fold, because an old log meets both.
    let old_shape = vec![serde_json::json!({ "key": "old-log" })];
    super::apply_key_states(shard, &old_shape);
    super::fold_carried_container_elements(shard, &old_shape);

    let after = (
        shard.sets.clone(),
        shard.zsets.clone(),
        shard.lists.clone(),
        shard.hashes.clone(),
    );
    assert_eq!(
        before.0, after.0,
        "folding a blob with no carried elements changed `sets`"
    );
    assert_eq!(
        before.1, after.1,
        "folding a blob with no carried elements changed `zsets`"
    );
    assert_eq!(
        before.2, after.2,
        "folding a blob with no carried elements changed `lists`"
    );
    assert_eq!(
        before.3, after.3,
        "folding a blob with no carried elements changed `hashes`. An absent field must be a NO-OP \
         here and not a removal -- `apply_key_state_field`, which the other thirteen maps use, would \
         have deleted the whole collection."
    );
    println!(
        "[old-log] all four container maps unchanged at 0.00% across a blob that names only its key"
    );
}

/// THE FOLD RESTORES AN ELEMENT WHOSE COMPONENT COULD NOT NAME IT.
///
/// This is the finding, driven at the seam. A blob carrying one zset element and one set element is
/// folded onto a shard that holds neither, and there is NO component anywhere in the input -- so the
/// identity that arrives cannot have been decoded from one. That is the property the two blocked
/// changes need: a 2-byte element ordinal replacing `component: Option<Arc<str>>`, and batched pages
/// where element identity lives only in the page payload.
///
/// THE MUTANT THIS KILLS is the removal of the four `merge_container_elements` calls from
/// `apply_key_states`, which is the whole change on the apply side. Without them the maps stay empty
/// here, because nothing else in this test writes them.
///
/// AND A CONTROL WHERE THE CHANGE PREDICTS NOTHING: the same blob carries a `features` field, which
/// is one of the thirteen maps the fold already handled and which this change does not touch. It
/// must arrive exactly as it always did, so an `apply_key_states` that had stopped working
/// altogether cannot pass as this finding.
///
/// rust-internal: calls the engine's own fold directly, no external surface
#[test]
fn the_fold_restores_a_container_element_from_carried_identity_and_not_from_a_component() {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine_on(dir.path());
    load_on(&engine, OPERATOR_END);

    // A real address to carry, taken from a page this store actually wrote.
    write(
        &engine,
        Command::SetAdd {
            key: "seed".to_string(),
            member: b"seed-member".to_vec(),
        },
    );
    let mut shards = engine.shards.write().expect("engine lock poisoned");
    let shard = shards.get_mut(&1).expect("shard is loaded");
    let address = shard
        .sets
        .get("seed")
        .and_then(|members| members.get(b"seed-member".as_slice()))
        .cloned()
        .expect("the seed member has an address");

    // Bytes no component on this revision would spell: a component is hex, and these are not.
    let member = b"identity-not-in-a-name".to_vec();
    let score_bits = crate::engine::execute_on_shard::zset_score_bits(9.25);
    let blob = vec![serde_json::json!({
        "key": "folded",
        "set_elements": [[ &member, &address ]],
        "zset_elements": [[ &member, [score_bits, &address] ]],
    }),
    // THE CONTROL: one of the thirteen maps the fold already restored, through the unchanged path.
    //
    // A SEPARATE BLOB, because that is what a record actually carries. `capture_key_states` and
    // `capture_container_element_states` each produce their own blobs and the two are never merged,
    // and `apply_key_states` relies on exactly that: it SKIPS a blob carrying container fields,
    // because reading a container carry as a per-key capture would clear the key's deadline and
    // every series it holds (see `a_container_carry_does_not_clear_the_key_state_it_does_not_name`).
    // This fixture put both in one object, which no writer emits, and so was testing a shape that
    // cannot occur.
    serde_json::json!({
        "key": "folded",
        "features": { "7": &address },
    })];

    assert!(
        shard.sets.get("folded").is_none() && shard.zsets.get("folded").is_none(),
        "DENOMINATOR: the shard already holds this key, so an element found after the fold would \
         not have come from the fold"
    );

    // BOTH HALVES, because they do different jobs now: `apply_key_states` restores the thirteen
    // per-key maps (the `features` control below), and `fold_carried_container_elements` restores
    // the container elements -- the latter only where the page each one names is still there, which
    // for this seeded address it is.
    super::apply_key_states(shard, &blob);
    super::fold_carried_container_elements(shard, &blob);

    let folded_set = shard
        .sets
        .get("folded")
        .and_then(|members| members.get(member.as_slice()))
        .cloned();
    let folded_zset = shard
        .zsets
        .get("folded")
        .and_then(|members| members.get(member.as_slice()))
        .cloned();
    println!(
        "[carried] set={} zset={} for a member of {} bytes that no component in the input spells",
        folded_set.is_some(),
        folded_zset.is_some(),
        member.len()
    );

    assert!(
        folded_set.is_some(),
        "the fold did not restore the set element. There is no component in this input, so if this \
         is red the carry is not being applied and a folded element is once again known only by the \
         name its page was filed under."
    );
    let (folded_score, _) = folded_zset.expect(
        "the fold did not restore the zset element from its carried identity, so `shard.zsets` is \
         still a map the fold cannot write",
    );
    assert_eq!(
        folded_score, score_bits,
        "the zset element came back at score bits {folded_score}, not the {score_bits} the record \
         carried -- the score must come from the carried entry, not be re-derived"
    );

    // THE CONTROL, at the value it was given, through the path this change does not touch.
    let control = shard
        .features
        .get("folded")
        .and_then(|series| series.get(&7u64))
        .cloned();
    assert!(
        control.is_some(),
        "the `features` control did not arrive, so `apply_key_states` is not working at all and the \
         assertions above say nothing about the carry specifically"
    );
    println!(
        "[carried] and the `features` control arrived through the unchanged per-key path, so the \
         fold as a whole is working and the two results above are about the carry"
    );
}

/// A CARRIED ELEMENT WHOSE PAGE THE FOLD DID NOT LEAVE BEHIND IS NOT RESTORED.
///
/// This is the test that made the carry's shape what it is, and the first shape of it was wrong.
/// A fold replays a SUFFIX of the delta log, so ONE fold can both add an element and take it away
/// again -- add, then remove, between two base-index writes is entirely ordinary. Applying a
/// record's carried elements as that record was folded put the element into the durable map, and
/// `fill_absent_elements` -- which keeps every durable element the derived view could not produce,
/// #1989's rule -- then handed it back after the later record had removed its page. **The element
/// would be served after being deleted.** Nothing did that before the carry existed, because the
/// fold never wrote these four maps at all, so this would have been a regression introduced by the
/// fix.
///
/// THE MERGE ASKS THAT QUESTION OF ITS PERSISTED INPUT TOO NOW, which is a later change and not a
/// reason to relax this one. `resident_map_readers` carries the argument: the carry and the persisted
/// map are two inputs to one merge, the carry was applied in the wrong ORDER and the persisted map is
/// simply OLDER than the page index, and both now answer `live_page_key` against the finished index.
/// So this test's subject would be stripped downstream if it survived here. It must not survive
/// here: correctness at this stage is not the same as being cleaned up at the next one.
///
/// AND MATCHING THE RECORD'S TOMBSTONES DOES NOT SUBSTITUTE, which is why the obvious symmetric
/// half was removed rather than kept. `mark_bucket_index_block_deleted_with` -- which `SetRemove`,
/// `ZSetRemove`, `ListPop` and `HashDelete` all reach -- is named for a mark it does not make: its
/// body is a `retain` that DROPS the page. So by the time the delta record is built there is no
/// page left to describe and no `deleted` item is emitted; the removal is spelled as the ABSENCE of
/// a page under a covered key. A removal half reading tombstones is dead code on every typed
/// container removal.
///
/// (`collect_command_index_items_for`'s own doc still claims a typed removal "marks its page
/// deleted in the bucket index rather than dropping it, so the item this emits carries
/// `deleted: true`". That claim does not hold on this revision. Recorded here rather than acted on:
/// correcting it is not this change's business, but a removal half built on believing it would have
/// been.)
///
/// # THE EXPERIMENT
///
/// Two carried elements against the same live shard. One names the address of a page that IS in the
/// bucket index; the other names an address no page holds, which is exactly what a carry left over
/// from a record whose page a later record removed looks like.
///
///   * The live-page element is restored. THE CONTROL: a filter that rejected everything would
///     otherwise pass as this finding.
///   * The absent-page element is NOT restored.
///
/// rust-internal: calls the engine's own fold directly, no external surface
#[test]
fn a_carried_element_whose_page_the_fold_did_not_keep_is_not_restored() {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine_on(dir.path());
    load_on(&engine, OPERATOR_END);

    write(
        &engine,
        Command::SetAdd {
            key: "res-seed".to_string(),
            member: b"res-seed-member".to_vec(),
        },
    );

    let mut shards = engine.shards.write().expect("engine lock poisoned");
    let shard = shards.get_mut(&1).expect("shard is loaded");
    let live_address = shard
        .sets
        .get("res-seed")
        .and_then(|members| members.get(b"res-seed-member".as_slice()))
        .cloned()
        .expect("the seed member has an address");

    // An address no live page holds. Built by changing the length, which is part of what names a
    // page in the live set, so it is a well-formed address that simply is not in the index -- which
    // is what a removed page leaves behind in a carry.
    let mut absent_address = live_address.clone();
    absent_address.set_length(live_address.length() + 4_096);

    // THE DENOMINATOR: the two addresses must actually differ, or both arms measure the same thing.
    assert_ne!(
        live_address.length(),
        absent_address.length(),
        "the two addresses are identical, so this fixture cannot tell a live page from an absent one"
    );
    let live_pages: usize = shard
        .bucket_index
        .bucket_map
        .values()
        .map(|bucket| bucket.block_index.values().filter(|page| !page.deleted).count())
        .sum();
    assert!(
        live_pages >= 1,
        "DENOMINATOR: {live_pages} live pages; with none, every carried element is skipped for the \
         wrong reason and the finding below is vacuous"
    );

    let kept = b"kept-by-a-live-page".to_vec();
    let gone = b"removed-page-leftover".to_vec();
    let blob = vec![serde_json::json!({
        "key": "res",
        "set_elements": [
            [ &kept, &live_address ],
            [ &gone, &absent_address ],
        ],
    })];

    super::fold_carried_container_elements(shard, &blob);

    let restored: Vec<Vec<u8>> = shard
        .sets
        .get("res")
        .map(|members| members.keys().cloned().collect())
        .unwrap_or_default();
    println!(
        "[resurrect] {live_pages} live page(s); restored {} of 2 carried element(s)",
        restored.len()
    );

    // THE CONTROL.
    assert!(
        restored.contains(&kept),
        "the element whose page IS live was not restored either, so the filter rejects everything \
         and says nothing about the absent one"
    );

    // THE FINDING.
    assert!(
        !restored.contains(&gone),
        "an element naming a page the fold did not leave behind was restored anyway. That is how a \
         removed element comes back: `fill_absent_elements` keeps every durable element the derived \
         view could not produce, and the derived view cannot produce this one because its page is \
         gone. It would then be served after being deleted. (That merge now asks this same \
         live-page question of its persisted input, so it would strip this element afterwards -- \
         but the fold must not hand it a known-dead one to strip.)"
    );
    println!(
        "[resurrect] so the carry is applied against the FINISHED page index, and a removal needs \
         no tombstone to be honoured -- taking the page away is the whole answer"
    );
}

/// A CONTAINER CARRY MUST NOT BE READ AS A PER-KEY STATE CAPTURE.
///
/// This is a bug the carry introduced and this test is what it is for. Both kinds of blob ride in
/// the delta record's one `key_states` channel, and `apply_key_states` applies THIRTEEN maps from
/// every blob it meets through `apply_key_state_field` -- which treats an ABSENT field as "this key
/// had none" and REMOVES the entry. That is deliberate and load-bearing for a real capture: a blob
/// naming only its key means "this key is in none of the thirteen", and the removal is the point.
///
/// A container carry names only its key and its elements. Read as a capture it therefore says this
/// key has no deadline and no series at all -- so a plain `SetAdd` against a key that holds a TTL
/// would DROP THAT TTL on the next fold, turning an expiring key into a permanent one, with nothing
/// reporting it. The carry is now skipped by that loop.
///
/// # TWO ARMS, AND THE SECOND IS THE ONE THAT KEEPS THE FIRST HONEST
///
///   * A container-only blob leaves `expires_at_ms` and `features` alone.
///   * A GENUINE capture blob for the same key, with those fields absent, still REMOVES them. This
///     is the control: skipping too much -- or `apply_key_states` quietly doing nothing at all --
///     would pass the first arm and break the mechanism the thirteen maps depend on, and only this
///     arm can tell the two apart.
///
/// rust-internal: calls the engine's own fold directly, no external surface
#[test]
fn a_container_carry_does_not_clear_the_key_state_it_does_not_name() {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine_on(dir.path());
    load_on(&engine, OPERATOR_END);

    write(
        &engine,
        Command::SetAdd {
            key: "carry-ttl".to_string(),
            member: b"ttl-member".to_vec(),
        },
    );

    let mut shards = engine.shards.write().expect("engine lock poisoned");
    let shard = shards.get_mut(&1).expect("shard is loaded");
    let address = shard
        .sets
        .get("carry-ttl")
        .and_then(|members| members.get(b"ttl-member".as_slice()))
        .cloned()
        .expect("the member has an address");

    // State this key holds that a container carry says nothing about.
    shard
        .expires_at_ms
        .insert("carry-ttl".to_string(), 1_800_000_000_000u64);
    shard
        .features
        .entry("carry-ttl".to_string())
        .or_default()
        .insert(11u64, address.clone());

    // THE DENOMINATOR: both must actually be there, or "unchanged" below is comparing absences.
    assert!(
        shard.expires_at_ms.contains_key("carry-ttl")
            && shard.features.get("carry-ttl").is_some_and(|s| !s.is_empty()),
        "the fixture did not establish a deadline and a series, so neither arm below means anything"
    );

    // ARM ONE: a container-only carry, exactly what a `SetAdd` now writes.
    let carry = vec![serde_json::json!({
        "key": "carry-ttl",
        "set_elements": [[ b"another-member".to_vec(), &address ]],
    })];
    super::apply_key_states(shard, &carry);
    super::fold_carried_container_elements(shard, &carry);

    let deadline_after_carry = shard.expires_at_ms.get("carry-ttl").copied();
    let series_after_carry = shard.features.get("carry-ttl").map(|s| s.len()).unwrap_or(0);
    println!(
        "[carry-ttl] after a container-only carry: deadline={deadline_after_carry:?} \
         series_len={series_after_carry}"
    );
    assert_eq!(
        deadline_after_carry,
        Some(1_800_000_000_000u64),
        "a container carry cleared this key's deadline. Read as a per-key capture it says the key \
         has no deadline, so a plain SetAdd against a key with a TTL would drop the TTL on the next \
         fold."
    );
    assert_eq!(
        series_after_carry, 1,
        "a container carry cleared this key's feature series for the same reason"
    );
    // And it did do its own job.
    assert!(
        shard
            .sets
            .get("carry-ttl")
            .is_some_and(|members| members.contains_key(b"another-member".as_slice())),
        "the carry did not restore its own element, so arm one may be passing because nothing ran"
    );

    // ARM TWO, THE CONTROL: a genuine capture blob with those fields absent still removes them.
    let capture = vec![serde_json::json!({ "key": "carry-ttl" })];
    super::apply_key_states(shard, &capture);
    let deadline_after_capture = shard.expires_at_ms.get("carry-ttl").copied();
    let series_after_capture = shard.features.get("carry-ttl").map(|s| s.len()).unwrap_or(0);
    println!(
        "[carry-ttl] after a genuine capture that omits them: deadline={deadline_after_capture:?} \
         series_len={series_after_capture}"
    );
    assert_eq!(
        deadline_after_capture, None,
        "a real capture blob no longer removes an absent deadline. The skip is too broad -- it must \
         match only a container carry, or the thirteen maps stop being able to record a removal and \
         reconstruction resurrects what a write evicted."
    );
    assert_eq!(
        series_after_capture, 0,
        "a real capture blob no longer removes an absent series, for the same reason"
    );
    println!(
        "[carry-ttl] so the two blob kinds are told apart: a carry adds and never removes, a \
         capture still removes what it does not name"
    );
}
