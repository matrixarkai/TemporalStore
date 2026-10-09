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
//!     hash    serde(default)       the field name          the component IS the caller's field name
//! ```
//!
//! HASH HAS CHANGED GROUPS, AND THE RULE BELOW HAD TO BE RE-DERIVED RATHER THAN ITS ROW CORRECTED.
//! This table read `hash  skip_serializing  nothing written`, and the sentence under it -- "three
//! kinds have a durable copy of what their name spells and two genuinely do not" -- counted hash as
//! one of the two. `ShardState::hashes` carries `#[serde(default)]` now and is written, so hash has
//! a durable copy of its field names and the count is FOUR and one. Correcting the row alone would
//! have left a rule that no longer followed from its own table.
//!
//! What the re-derivation changes, and what it does not: a STRING still has no component, so for it
//! deriving is not a second copy of anything and it is still left alone. A HASH now has a durable
//! copy -- which is why `an_unnamed_hash_page_is_skipped_and_the_durable_map_keeps_the_field` below
//! can pass at all, and that test already says so, naming the six-fields-served-as-zero shape as
//! what the durability fixed. So the hash arm is in the same position as the three spelled kinds:
//! the derived view decides which elements exist, and the durable map keeps what a name cannot
//! supply. It is left alone for a DIFFERENT reason than a string is -- not "there is nothing to
//! outrank it with", but "the component is the caller's own text rather than something this engine
//! rendered, so there is no parse to get wrong."
//!
//! # WHY THE RULE IS NOT THE ONE `control_state` USES
//!
//! The `control_state` arm keeps the persisted series wholesale for every key it has, saying why: "the
//! serialized i64 series is authoritative (the page is a copy of it)". Copying that rule here would be
//! wrong. `apply_key_states` folds `features` and the control-state maps out of the delta log and NOT
//! `sets`, `zsets` or `lists`.
//!
//! THERE ARE TWO CHANNELS AND THAT SENTENCE DESCRIBES ONE OF THEM. It used to continue "so for these
//! three the derived view is the ONLY path by which a folded element arrives", which is true of
//! `apply_key_states` and false of the fold. The fold path also collects the record's
//! `CARRIED_CONTAINER_FIELDS` -- `set_elements`, `zset_elements`, `list_elements` and `hash_fields`
//! -- and `fold_carried_container_elements` merges all four into their durable maps, gated on the
//! pages the finished fold actually left behind. So a folded element of any of those four kinds
//! arrives by BOTH routes, and `engine::tests::fold_hash_map_completeness` measures that for hash:
//! after a fold the durable map and the page index name the same field set, in both directions.
//!
//! Naming one function's behaviour as the whole path's is how this was read as "the derived view is
//! the only path" by three separate documents. Both channels are named here so the next reader has
//! to meet the second one.
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
fn zset_name(member: &[u8]) -> String {
    hex::encode(member)
}

/// The component name a zset member USED TO be filed under, before the score left it. Kept only
/// for `a_durable_zset_score_outranks_a_component_name_that_disagrees`, to prove a needle shaped
/// like the retired encoding no longer appears anywhere in a served index.
fn zset_name_with_retired_score_prefix(score: f64, member: &[u8]) -> String {
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

/// THE DURABLE SCORE WINS OVER A NAME THAT DISAGREES WITH IT -- RESTATED, BECAUSE THE NAME NO
/// LONGER HAS A SCORE TO DISAGREE WITH.
///
/// This was the same experiment that found the defect, with the assertion the other way round: a
/// zset written at 7.5 had its served-index component name changed to spell 99.25, and the reload
/// had to answer 7.5 from the durable `zsets` map rather than 99.25 parsed back out of the name.
///
/// THAT EXPERIMENT IS NOW UNRUNNABLE AS WRITTEN, which is the finding this restatement drives
/// rather than papers over: a zset's component is `hex::encode(member)` now, with no score term
/// in it at all, so there is no score-shaped text anywhere in a served index for a corrupted name
/// to disagree with the durable map about. A score-shaped needle built the OLD way
/// (`zset_name_with_retired_score_prefix`) must swap ZERO times, which is the positive proof that
/// the attack surface this test used to drive is gone rather than merely untested. A MEMBER-shaped
/// needle (`zset_name`, what the component actually is now) must swap a NONZERO number of times
/// over the same files, which is the control proving the zero above means "not there" and not
/// "the swap mechanism found nothing in anything."
///
/// WHAT STILL PROVES "DURABLE OUTRANKS DERIVED": corrupting the member-shaped needle -- renaming
/// the served index's component for this element to a DIFFERENT member's hex -- and reading the
/// ORIGINAL member's score back unchanged, because `shard.zsets` is a separate, directly
/// persisted map (`zset_index_serde`) that a served-index text corruption cannot touch.
///
/// rust-internal: mutates the engine's own served index, no external surface
#[test]
fn a_durable_zset_score_outranks_a_component_name_that_disagrees() {
    let dir = tempfile::tempdir().unwrap();
    let indexes = dir.path().join("indexes");
    let member = b"outranked-member".to_vec();
    // SAME LENGTH AS `member`, sixteen bytes, for the same reason the retired-shape pair below
    // is checked for equal length: a pure substitution, not a resize of the frame.
    let other_member = b"a-different-mem1".to_vec();
    assert_eq!(member.len(), other_member.len(), "the two members must be the same length");
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

    // THE SCORE-SHAPED NEEDLE: built the way a component used to be spelled, before this change.
    // It must not appear anywhere, at either score -- there is nothing left shaped like it.
    let retired_from = zset_name_with_retired_score_prefix(durable_score, &member);
    let retired_to = zset_name_with_retired_score_prefix(name_score, &member);
    assert_eq!(
        retired_from.len(),
        retired_to.len(),
        "the two retired-shape names differ in length, so the swap would not be a pure substitution"
    );
    assert_ne!(retired_from, retired_to, "the two scores spell the same retired-shape name");
    let retired_swaps = swap_across_index_files(&indexes, &retired_from, &retired_to);
    assert_eq!(
        retired_swaps, 0,
        "a score-shaped needle matched {retired_swaps} time(s) in a served index -- the score is \
         supposed to have left the component entirely"
    );

    // THE CONTROL: a member-shaped needle, what the component actually is now, over the SAME
    // files. Nonzero proves the zero above means the score text is absent, not that nothing in
    // these files can ever be found by this mechanism.
    let member_from = zset_name(&member);
    let member_to = zset_name(&other_member);
    println!("[outrank] member-shaped needle {member_from:?} -> {member_to:?}");
    let member_swaps = swap_across_index_files(&indexes, &member_from, &member_to);
    assert!(
        member_swaps > 0,
        "the member-shaped needle was not found in any index file, so the zero above proves \
         nothing -- the control did not drive anything either"
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
        "[outrank] wrote {durable_score}, renamed the served index's component to a DIFFERENT \
         member's hex, left the durable map alone; the reload answered {score}"
    );
    assert!(
        (score - durable_score).abs() < 1e-9,
        "the reload answered {score}, not the durable {durable_score}. The served index's \
         component text was corrupted and the durable map still won, which is the property this \
         test is for."
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

    // hash: a durable map NOW, like the other three. This used to read "no durable map, and none
    // needed -- the component IS the caller's field name", and the second half is still true: the
    // component spells the field name exactly, so a readable name needs no help. What was wrong was
    // the inference that no help was ever needed, because a hash page can name NO field at all, and
    // then the name spells nothing and the durable map is the only record. That case is driven by
    // `an_unnamed_hash_page_is_skipped_and_the_durable_map_keeps_the_field` below.
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
// 3. THE FALLBACK: AN ELEMENT WITH NO DURABLE ENTRY IS STILL DERIVABLE
//    (and NOT "durable wins where present" -- the index-derived address wins a collision)
// =============================================================================================

/// AN ELEMENT WITH NO DURABLE ENTRY STILL COMES BACK FROM ITS NAME.
///
/// The rule is "an element missing from the durable map is still derivable", not "never derive",
/// and NOT -- as this paragraph said until it was measured -- "the durable map wins where it has
/// the element". It does not win: `fill_absent_elements` starts from the map built out of the
/// bucket index and calls `insert_element_if_absent`, so on a collision the INDEX-DERIVED address
/// is the one that survives and the durable map supplies only what the index could not name. This
/// test never exercised that direction, which is why the wrong claim sat here passing.
///
/// It has to allow derivation at all because `apply_key_states` folds `features` and the
/// control-state maps out of the delta log and NOT `sets`, `zsets` or `lists`, so an element the
/// fold added reaches these maps ONLY through the derived view. A rule that preferred the durable
/// map per KEY -- which is what the `control_state` arm does, for a reason that holds there --
/// would drop every one of them.
///
/// AND THE CLAIM IS NARROWER THAN IT LOOKS, which is worth stating because it was read too widely
/// for most of a campaign. What this pins is the REPAIR case: an element the durable map has LOST
/// comes back because the index entry names it. It is NOT evidence that the index must name every
/// element for a load to be correct -- a container element the index does not name at all survives
/// the reconcile untouched, measured in
/// `context_node_survives_reload::a_container_only_element_survives_the_reconcile_when_its_page_is_still_live`,
/// because the live filter keys on `(slab_id, offset, length)` and asks only whether the PAGE is
/// still there.
///
/// Driven directly on the merge, because constructing a half-folded store through the public surface
/// would be a fixture with more moving parts than the property it checks.
///
/// RESTATED FOR THE SCORE HALF, NOT THE MEMBER HALF. The member -- `fb-two`'s IDENTITY -- still
/// comes back from its name exactly as this test's title says: that is what the merge is for and
/// nothing about this change touches it. Its SCORE no longer can, because the name does not spell
/// one any more. So `fb-two`, the element the durable map lost, answers a PLACEHOLDER score
/// (`zset_score_bits(0.0)`, asserted exactly rather than merely "some value") on this path, where
/// it used to answer its true 2.0; `fb-one`, which the durable map never lost, is unaffected and
/// still answers its true 1.0 through the ordinary (non-fallback) path. This is the cost the
/// module doc for `reconcile_secondary_views_from_bucket_index`'s zset arm names explicitly: the
/// component's score was always a counted FALLBACK behind the durable map, never the primary
/// source, and a fallback that cannot recover a value it no longer has anywhere to read it from
/// is the finding, not a regression to chase.
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
            .elements_mut_for_test("fb-zset")
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

    // fb-one: the durable map never lost it, so it answers its TRUE score through the ordinary
    // path. fb-two: the durable map lost it, so it is recovered by IDENTITY from its name, but its
    // score is now a PLACEHOLDER -- zero, biased -- because the name no longer carries one.
    let placeholder = crate::engine::execute_on_shard::zset_score_bits(0.0);
    let placeholder_score = crate::engine::execute_on_shard::zset_score_from_bits(placeholder);
    for (member, expected) in [
        (b"fb-one".to_vec(), 1.0f64),
        (b"fb-two".to_vec(), placeholder_score),
    ] {
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
        "[fallback] so an element the durable map does not hold still arrives through its name. \
         This says nothing about a collision: the assertions above exercise only the ABSENT case, \
         and where both sources hold one element it is the index-derived address that survives"
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
    // IT PLANTS A CORRUPT ELEMENT NAME INTO AN INDEX FILE AND NEEDS TO FIND ONE THERE. A gated
    // live set entry carries none, so there is nothing to corrupt and the test's own floor says so
    // in as many words -- "the name was not found in any index file, so nothing was corrupted and
    // this test would pass without testing anything". That floor is the instrument working.
    //
    // The gated equivalent -- a derived view that cannot name an element while the durable map
    // still holds it -- is held by `gated_corpus_across_a_store_boundary`, whose durable floor is
    // asserted before any served count is read.
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

/// AN UNNAMED HASH PAGE LOSES NOTHING, BECAUSE THE DURABLE MAP KEEPS THE FIELD.
///
/// THE ARM THAT CHANGED IS THE ARM THAT HAD NO DRIVEN CASE. The merge was extended to `hashes` on
/// the claim that its mechanism is now byte-for-byte the other three's, and "closed by construction"
/// is exactly the kind of statement a test has repeatedly found to be false in this tree. So this is
/// the hash twin of `an_unreadable_component_name_is_skipped_and_the_durable_map_keeps_the_element`,
/// and it is the one test here that fails if `shard.hashes = hashes` comes back.
///
/// # RESTATED: THE SHAPE IS NO LONGER PLANTED, BECAUSE IT IS THE ONLY SHAPE THERE IS
///
/// This test used to BLANK the component on one field's page entry and keep a second field's name as
/// a control -- a named page and a nameless one, side by side. Under one entry a page NO live entry
/// of any container kind carries an element name, so there is nothing left to blank: the blanking
/// loop matched zero entries and the fixture's own floor refused it with `blanked 0, not one`. That
/// floor is why this reads as a restatement rather than as a test that quietly stopped testing.
///
/// THE CONTROL HAD TO GO WITH IT, AND THAT IS THE HONEST VERSION. `field-0` was the control because
/// its page still named it, so it arrived through the DERIVED view while `field-1` could only arrive
/// from the durable map. Neither is named now, so there is no named/nameless contrast to draw, and
/// keeping `field-0` as a "control" would be a second copy of the same case wearing the word
/// control. What replaces it is a DENOMINATOR on the other side of the claim: the index must be
/// shown to hold live hash entries for this key and to name NOTHING with them, or "the durable map
/// supplied it" is a conclusion about an index that was simply empty.
///
/// # WHAT IS ASSERTED, AND WHAT WOULD HAVE TO DISAGREE FOR IT TO FAIL
///
/// Both fields must be in `shard.hashes` after a reload, and the index must name neither. The two
/// independent artefacts are the index, which names zero fields, and the durable map, which names
/// two: if the merge stops consulting the map -- `shard.hashes = hashes`, or
/// `insert_element_if_absent` ceasing to insert -- the reload serves ZERO fields, because nothing
/// else can name one. That is a strictly larger failure than the single field the planted version
/// could lose, so the restatement did not weaken what this holds.
///
/// AND IT IS A TRIPWIRE IN THE OTHER DIRECTION TOO. If any live hash entry is ever found naming a
/// field again, the `named` floor below reddens and says so: the collapse would have been reverted,
/// and the arms in this module that assume a nameless index would all need re-reading.
///
/// Asserted on the resident map rather than through a command, deliberately: `HashGet` and
/// `HashGetAll` resolve through the resident map now, so reading them would be reading the same
/// source twice rather than the index against the map.
///
/// rust-internal: reads the engine's own in-memory index, no external surface
#[test]
fn an_unnamed_hash_page_is_skipped_and_the_durable_map_keeps_the_field() {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine_on(dir.path());
    load_on(&engine, OPERATOR_END);

    // Two fields of one hash, written normally.
    for element in 0..2 {
        write(
            &engine,
            Command::HashSet {
                key: "un-hash".to_string(),
                field: format!("field-{element}"),
                value: format!("value-{element}").into_bytes(),
            },
        );
    }

    // THE TWO DENOMINATORS, both read BEFORE the unload that writes the index.
    {
        let shards = engine.shards.write().expect("engine lock poisoned");
        let shard = shards.get(&1).expect("shard is loaded");
        assert_eq!(
            shard.hashes.get("un-hash").map(|fields| fields.len()),
            Some(2),
            "the durable map does not hold both fields, so this fixture cannot create the state it \
             needs and the assertions below would be about nothing"
        );
        let mut entries = 0usize;
        let mut named = 0usize;
        for bucket in shard.bucket_index.bucket_map.values() {
            for page in bucket.block_index.values() {
                if page.model_id.as_str() == "hash" && &*page.object_key == "un-hash" {
                    entries += 1;
                    if page.component.is_some() {
                        named += 1;
                    }
                }
            }
        }
        println!(
            "[unnamed-hash] the index holds {entries} hash entr(ies) for this key, {named} of \
             which name a field; the durable map names 2"
        );
        assert!(
            entries > 0,
            "THE INDEX HOLDS NO HASH ENTRY FOR THIS KEY AT ALL. Then the claim below would be a \
             statement about an EMPTY index rather than about a NAMELESS one, and this test would \
             pass without exercising the merge."
        );
        assert_eq!(
            named, 0,
            "{named} of {entries} hash entries NAME A FIELD. Under one entry a page none can: if \
             this reddens the collapse has been reverted, and every arm in this module that assumes \
             a nameless index has to be re-read rather than this floor relaxed."
        );
    }

    // The unload writes the index -- nameless entries and the durable map together -- and the
    // reload runs the merge over them.
    engine.unload_shard(1);
    load_on(&engine, OPERATOR_END);

    let shards = engine.shards.write().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard is loaded");
    // DELIBERATELY NOT AN `expect` ON THE KEY. The regression this arm guards against -- the
    // reconcile assigning the derived view WHOLESALE instead of merging the durable map into it --
    // takes the whole KEY with it, not one field of it. An `expect` here reddens with "the hash key
    // did not survive the reload", which reads as a broken fixture, and the mutation proving this
    // arm non-vacuous did exactly that. An absent key IS zero fields, so it is folded into the
    // field set and the assertion under test is the one that speaks.
    let field_names: std::collections::BTreeSet<String> = shard
        .hashes
        .get("un-hash")
        .map(|fields| fields.keys().map(|name| name.to_string()).collect())
        .unwrap_or_default();
    println!(
        "[unnamed-hash] reloaded holding {} field(s): {:?}",
        field_names.len(),
        field_names
    );
    for field in ["field-0", "field-1"] {
        assert!(
            field_names.contains(field),
            "{field} IS NOT SERVED after the reload; the key came back with {:?}. No page entry \
             names a field, so the derived view cannot produce either of them and the durable map \
             is the only thing that can. This reddens if `fill_absent_elements` stops filling -- \
             i.e. if the reconcile goes back to assigning the derived view wholesale.",
            field_names
        );
    }
    assert_eq!(
        field_names.len(),
        2,
        "the hash came back with {} field(s) rather than 2: {:?}",
        field_names.len(),
        field_names
    );
    println!(
        "[unnamed-hash] both fields came back from an index that names neither, so the durable map \
         supplied the whole field set rather than one field of it"
    );
}

// =================================================================================================
// THE FOLD CARRIES ELEMENT IDENTITY, AND AN EMPTY ZSET MEMBER IS A WHOLE COMPONENT TOO
// =================================================================================================

/// AN EMPTY ZSET MEMBER SPELLS EXACTLY ZERO CHARACTERS, AND THE RECONCILE ONCE REFUSED TO READ IT.
///
/// RESTATED: `zset_component` used to be `{biased:016x}` followed by `hex::encode(member)`, so a
/// member of zero bytes spelled exactly sixteen characters -- a complete score with an empty
/// member after it -- and the reconcile's zset arm opened `if component.len() <= 16 { return None
/// }`, refusing the one length a real, empty-member write could produce. That sixteen-character
/// boundary is gone along with the score: `zset_component` is `hex::encode(member)` alone now, so
/// an empty member spells the empty string, and there is no length left to special-case at all --
/// `hex::decode("")` was always `Ok(vec![])`, and the reconcile's arm no longer asks about length
/// before decoding. This test keeps its ORIGINAL subject, not the boundary that caused it: an
/// empty zset member must stay reachable and durable through the fold shape that strands an
/// element with a page in the index but no durable-map entry behind it.
///
/// Historically, `zset_component` is `{biased:016x}` followed by `hex::encode(member)`. A member
/// of zero bytes -- which nothing on the write path rejects -- therefore spelled EXACTLY sixteen
/// characters, and the reconcile's zset arm opened `if component.len() <= 16 { return None }`. So
/// that component decoded to nothing, the element was counted in `unreadable_names` and skipped,
/// and on the one door where
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
/// ITS SCORE IS A PLACEHOLDER NOW, same reasoning as the test this one mirrors: the durable map
/// is what this fixture dropped on purpose, so recovery falls back to the component, which no
/// longer carries a score for anything to fall back to. Presence is the finding this test is for;
/// score fidelity on this specific (durable-map-absent) path was always a secondary claim and is
/// the one thing this change costs.
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
        zset_name(&empty).len()
    };
    assert_eq!(
        empty_component_len, 0,
        "an empty member's component is {empty_component_len} characters, not the zero this \
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
            .elements_mut_for_test("mt-zset")
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
         nothing about the empty-member boundary"
    );

    // THE FINDING: PRESENCE, NOT SCORE FIDELITY. The empty member must still be THERE -- its
    // component is the empty string, which `hex::decode` has always read as `Ok(vec![])`, so
    // there is no length boundary left for a reconcile or a WAL replay arm to disagree about, and
    // that must stay true. Its SCORE is a placeholder on this path now, same reasoning and same
    // placeholder as `an_element_the_durable_map_does_not_hold_still_comes_back_from_its_name`:
    // the durable map lost it (this fixture dropped it on purpose, to build the fold shape), so
    // recovery falls back to the component -- which no longer has a score to fall back TO.
    let placeholder = crate::engine::execute_on_shard::zset_score_bits(0.0);
    let placeholder_score = crate::engine::execute_on_shard::zset_score_from_bits(placeholder);
    assert_eq!(
        empty_score,
        Some(placeholder_score),
        "the empty member came back as {empty_score:?}, not the placeholder {placeholder_score} \
         this path falls back to now that its component carries no score at all"
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
