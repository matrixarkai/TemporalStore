// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! WHAT A SET LISTING PAYS TO RECOVER BYTES IT IS ALREADY HOLDING.
//!
//! # THE CLAIM THIS MODULE WAS OPENED ON
//!
//! A set member's bytes appear in three places at once, and nobody had measured it:
//!
//! ```text
//!     sets: HashMap<String, BTreeMap<Vec<u8>, BlockAddress>>   the member IS the map key
//!     component = hex::encode(&member)                          the member again, two chars a byte
//!     append_value_of_object(.., &member, ..)                   the member again, as the payload
//! ```
//!
//! All three are real and all three are asserted below rather than read off the write arm. A
//! fourth site was NOT in the claim and is: `set_index_serde` persists the map as
//! `(member bytes, address)` pairs, so for a set the member reaches durable storage a second time,
//! in the index snapshot, beside the page that already holds it.
//!
//! # THE MEASUREMENT
//!
//! `Command::SetMembers` walks `bucket_index_component_block_addresses(shard, "set", &key)` and
//! reads ONE PAGE PER MEMBER to build its answer. The pairs it walks carry the component, and a
//! set's component IS `hex::encode(&member)` -- so every member the response returns was already
//! present, spelled in hex, in the tuple being iterated. `a_set_listing_reads_one_page_per_member`
//! counts the reads; `every_member_a_set_listing_returns_is_already_spelled_by_its_component`
//! proves the redundancy at byte level over a seeded shard rather than inferring it from the call
//! site.
//!
//! COUNTS, NOT TIMES. A timing ratio on this box reads 485x idle against 11x busy off identical
//! code, so the instrument is `maintenance_block_read_counts`, the counter inside the one place a
//! page read past the cache is noted (`note_block_read`).
//!
//! THE CONTROL IS THE ZSET, and it is a control that can fail. A zset's listing answers from
//! `shard.zsets` and reads no page at all, so it must report 0.00 reads per member WHILE STILL
//! RETURNING MEMBERS -- the returned bytes are asserted non-zero, because a listing over an empty
//! key reads zero pages too and would pass a weaker assertion without exercising anything.
//!
//! # WHAT THE PAGE READ BUYS, WHICH IS THE PART THAT DECIDES IT
//!
//! Two cheaper sources were put to the fixture and both are refused, for different reasons.
//!
//! ## Decoding the component instead of reading the page
//!
//! Byte-identical today -- that is the finding of
//! `every_member_a_set_listing_returns_is_already_spelled_by_its_component`. What it would cost is
//! not reads but the only validation a component has.
//!
//! THE SECOND HALF OF THAT SENTENCE USED TO READ "and nothing re-checks them; today a page that
//! disagrees with its name is served as the page", and `container_pages` made it false. A container
//! page now states which element it holds, and the read funnel asks the page for the element the
//! index named -- so a page that disagrees with its name is NOT served, it answers missing. The
//! component and the payload are still written together by one call site, and the check is now on the
//! element KEY rather than on the value, but a check exists where none did.
//!
//! What does not change is the verdict: decoding the component instead of reading the page would
//! trade an answer storage vouches for against one nothing does, and it would give up the element
//! check as well as the byte one. It is measured here and not taken.
//!
//! ## Serving from `shard.sets`
//!
//! The members ARE the keys of that map, resident and ordered, and walking them would read no page.
//! `the_resident_set_map_and_the_live_page_index_are_not_the_same_population` drove why that was
//! refused: `reconcile_secondary_views_from_bucket_index` installs
//! `shard.sets = fill_absent_elements(derived, persisted)`, whose documented job is to KEEP EVERY
//! DURABLE ELEMENT THE DERIVED VIEW COULD NOT PRODUCE, and it kept them without asking whether the
//! page each named was still there. The map held members the page index did not, and a listing served
//! from it would have returned them. `durable_outranks_derived`'s
//! `a_carried_element_whose_page_the_fold_did_not_keep_is_not_restored` records the same hazard from
//! the fold side and states the consequence in as many words: "It would then be served after being
//! deleted."
//!
//! THAT OBJECTION IS ANSWERED and this module's conclusion survives it on other grounds. The merge
//! asks the live-page question of the persisted map now, so the map cannot over-report. What still
//! stops a listing is the opposite direction: a live page whose component cannot be decoded is in the
//! index and in no map, so a map-served listing would MISS a member -- #1989's case, latent because
//! nothing writes a non-hex set component. `resident_map_readers` carries that argument and the
//! per-reader sweep. The 1.00 reads per member measured below are therefore UNBLOCKED rather than
//! recovered, and are a serving-path change with its own gate.
//!
//! # SO NO PRODUCTION CHANGE SHIPS FROM THIS MODULE
//!
//! The page read is load-bearing: it is the answer the storage vouches for, and both cheaper
//! sources are cheaper precisely because they skip that. The three copies are real, the redundancy
//! is real, and the reads are the price of the one answer that is checked against a page. This
//! module is the measurement and the written reason, and it asserts the facts so a later change
//! that breaks one of them fails the build.
//!
//! THE STORE PATH LENGTH is held equal across every arm and asserted equal: it moves allocation
//! bytes at about six bytes a character, and an arm on a longer temporary directory would read as a
//! heavier representation. It does not move the page-read COUNTS this module is quoted on, which is
//! why those are the headline and the bytes are not.
#![allow(clippy::all)]
use super::*;
use std::collections::BTreeSet;

/// Member widths the ladder is measured at, in bytes.
const WIDTHS: [usize; 5] = [8, 16, 32, 64, 256];

/// Members per container key. Two populations, so a per-member figure that is really a fixed cost
/// divided by the count shows up as a different number at each.
const SMALL_POPULATION: usize = 8;
const LARGE_POPULATION: usize = 64;

fn engine_on(dir: &std::path::Path) -> TemporalEngine {
    TemporalEngine::with_local_dirs(
        64 * 1024 * 1024,
        dir.join("cache"),
        dir.join("pages"),
        dir.join("indexes"),
    )
}

fn load_on(engine: &TemporalEngine) {
    let response = engine.load_shard_with(crate::control::LoadShardRequest {
        shard_id: 1,
        table_name: "set-listing-reads".to_string(),
        shard_uri: "local://set-listing-reads/1".to_string(),
        start_routing_bucket: 0,
        end_routing_bucket: 1023,
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

/// A member of exactly `width` bytes, distinct per index, and never all-equal: a fixture whose
/// members collide would file fewer pages than it wrote and every per-member figure below would be
/// divided by the wrong denominator.
fn member_of(width: usize, index: usize) -> Vec<u8> {
    let mut bytes = vec![0u8; width];
    let stamp = format!("{index:08}");
    for (slot, byte) in stamp.as_bytes().iter().enumerate() {
        if slot < width {
            bytes[slot] = *byte;
        }
    }
    // Fill the tail with something index-dependent so two members of the same width differ beyond
    // the stamp as well.
    for slot in stamp.len()..width {
        bytes[slot] = (index as u8).wrapping_mul(31).wrapping_add(slot as u8);
    }
    bytes
}

fn members_returned(response: &crate::types::CommandResponse) -> Vec<Vec<u8>> {
    match response {
        crate::types::CommandResponse::Members { members } => members.clone(),
        other => panic!("expected a Members response, got {other:?}"),
    }
}

/// Page reads past the cache performed by `work`, on this thread.

/// Every member the LIVE PAGES of one set key still state as present, decoded out of the element
/// key each page item is filed under.
///
/// THIS IS THE SOURCE THAT REPLACED THE COMPONENT, and its limit is stated here because one arm in
/// this module turns on it: the payload is where a member's bytes live now, but it is NOT an
/// authority on element LIVENESS. A removal writes a tombstone page and leaves the element's
/// original page live and still stating the element, so this OVER-REPORTS after a removal. For
/// "which members does this key hold" the serving path is the authority; for "are the bytes still
/// there" this is.
fn payload_members(engine: &TemporalEngine, object_key: &str) -> BTreeSet<Vec<u8>> {
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard is loaded");
    let mut stated = BTreeSet::new();
    for bucket in shard.bucket_index.bucket_map.values() {
        for page in bucket.block_index.values() {
            if page.model_id.as_str() != "set" || &*page.object_key != object_key || page.deleted {
                continue;
            }
            let Ok(bytes) = engine.block_store.read(&page.address) else {
                continue;
            };
            if let crate::engine::container_pages::ContainerPageDecode::Framed {
                spelling,
                items,
                ..
            } = crate::engine::container_pages::decode_container_page(&bytes)
            {
                for item in items {
                    if item.deleted {
                        continue;
                    }
                    if let Some(component) =
                        crate::engine::container_pages::component_from_element_key(
                            spelling, &item.key,
                        )
                    {
                        if let Ok(member) = hex::decode(&component) {
                            stated.insert(member);
                        }
                    }
                }
            }
        }
    }
    stated
}

fn page_reads_of<T>(work: impl FnOnce() -> T) -> (T, u64) {
    crate::engine::reset_maintenance_block_read_counts();
    let value = work();
    let counts = crate::engine::maintenance_block_read_counts();
    (value, counts.block_reads_total)
}

// =================================================================================================
// 1. THE COUNT
// =================================================================================================

/// ONE PAGE READ PER MEMBER FOR A SET LISTING, AND NONE FOR A ZSET'S.
///
/// The headline. `SetMembers` walks the page index and reads a page per entry; `ZSetRange` answers
/// out of `shard.zsets` and reads nothing. Both arms return members, and both return-populations
/// are asserted non-zero, so the zero row is a measured zero and not an unexercised one.
///
/// THE LISTING MUST BE COLD. `SetMembers` is wrapped in `cached_response`, which stores the whole
/// answer under `CacheKey::set_members`, so a second listing of the same key reads nothing at all.
/// Each key is listed exactly once here and
/// `a_second_set_listing_reads_no_pages_at_all_because_the_answer_is_cached` states the other half
/// explicitly, so this module cannot be read as claiming the cost is paid per call.
///
/// rust-internal: drives the engine's own command surface
#[test]
fn a_set_listing_reads_one_page_per_member_and_a_zset_listing_reads_none() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path_len = dir.path().as_os_str().len();
    let engine = engine_on(dir.path());
    load_on(&engine);

    println!("\n=== page reads per member, cold listing ===");
    println!("  store path {path_len} characters (equal for every arm in this test)");
    println!(
        "  {:>6}  {:>6}  {:>9}  {:>11}  {:>9}  {:>11}  {:>10}",
        "width", "members", "set reads", "set per mem", "zset rds", "zset per m", "bytes back"
    );

    let mut set_rows: Vec<(usize, usize, u64, f64)> = Vec::new();
    let mut zset_total_reads: u64 = 0;
    let mut zset_total_members: usize = 0;

    for population in [SMALL_POPULATION, LARGE_POPULATION] {
        for width in WIDTHS {
            let set_key = format!("s-{width}-{population}");
            let zset_key = format!("z-{width}-{population}");
            for index in 0..population {
                let member = member_of(width, index);
                write(
                    &engine,
                    Command::SetAdd {
                        key: set_key.clone(),
                        member: member.clone(),
                    },
                );
                write(
                    &engine,
                    Command::ZSetAdd {
                        key: zset_key.clone(),
                        member,
                        score: index as f64,
                    },
                );
            }

            let (set_response, set_reads) = page_reads_of(|| {
                read(
                    &engine,
                    Command::SetMembers {
                        key: set_key.clone(),
                    },
                )
            });
            let set_members = members_returned(&set_response);

            let (zset_response, zset_reads) = page_reads_of(|| {
                read(
                    &engine,
                    Command::ZSetRange {
                        key: zset_key.clone(),
                        start: 0,
                        stop: -1,
                        rev: false,
                    },
                )
            });
            let zset_returned = members_returned(&zset_response);

            // THE POPULATION THE FIXTURE CLAIMS, ASSERTED. A listing that answered short would
            // divide the reads by a denominator the store never held.
            assert_eq!(
                population,
                set_members.len(),
                "the set listing for {set_key} returned {} members, not the {population} written -- \
                 every per-member figure in this row would be divided by the wrong denominator",
                set_members.len()
            );
            // A zset listing is INTERLEAVED member/score, so the element count is half the length.
            assert_eq!(
                0,
                zset_returned.len() % 2,
                "the zset listing for {zset_key} returned an odd number of entries ({}), so \
                 halving it to an element count would be quietly wrong",
                zset_returned.len()
            );
            assert_eq!(
                population,
                zset_returned.len() / 2,
                "the zset control listed {} elements, not the {population} written -- a control \
                 over a short population is not the control this test claims",
                zset_returned.len() / 2
            );

            // THE CONTROL'S EXERCISED BYTES. A zero read count is only informative if the arm
            // actually produced member bytes.
            let zset_bytes: usize = zset_returned.iter().map(|entry| entry.len()).sum();
            assert!(
                zset_bytes > 0,
                "the zset control returned {zset_bytes} bytes, so its 0.00 reads per member is a \
                 control over nothing"
            );
            let set_bytes: usize = set_members.iter().map(|member| member.len()).sum();
            assert_eq!(
                population * width,
                set_bytes,
                "the set listing returned {set_bytes} bytes for {population} members of {width} \
                 bytes"
            );

            // THE FINDING, per row: one page read per member for the set.
            assert_eq!(
                population as u64, set_reads,
                "the set listing for {set_key} performed {set_reads} page reads for {population} \
                 members. The claim this module is quoted on is exactly one per member"
            );
            // THE CONTROL, per row: none for the zset.
            assert_eq!(
                0, zset_reads,
                "the zset listing for {zset_key} performed {zset_reads} page reads. A zset answers \
                 from `shard.zsets` and must read no page at all"
            );

            let per = set_reads as f64 / population as f64;
            println!(
                "  {width:>6}  {population:>7}  {set_reads:>9}  {per:>11.2}  {zset_reads:>9}  \
                 {:>10.2}  {set_bytes:>10}",
                0.0_f64
            );
            set_rows.push((width, population, set_reads, per));
            zset_total_reads += zset_reads;
            zset_total_members += population;
        }
    }

    // THE SLOPE ACROSS WIDTH. The page-read count is per MEMBER and not per byte, so it must be
    // flat across a 32x width change at a fixed population -- if it moved with width, the cost
    // would be something other than one read an element and the per-member figure would be a
    // coincidence of this fixture's sizes.
    for population in [SMALL_POPULATION, LARGE_POPULATION] {
        let at_narrowest = set_rows
            .iter()
            .find(|(width, pop, _, _)| *width == WIDTHS[0] && *pop == population)
            .map(|(_, _, reads, _)| *reads)
            .expect("the narrowest width was measured");
        let at_widest = set_rows
            .iter()
            .find(|(width, pop, _, _)| *width == WIDTHS[WIDTHS.len() - 1] && *pop == population)
            .map(|(_, _, reads, _)| *reads)
            .expect("the widest width was measured");
        assert_eq!(
            at_narrowest, at_widest,
            "at {population} members the read count moved from {at_narrowest} to {at_widest} \
             across a {}x width change. One read per member does not depend on member width",
            WIDTHS[WIDTHS.len() - 1] / WIDTHS[0]
        );
    }

    println!(
        "\n  set : 1.00 page reads per member at every width and both populations\n  zset: \
         {zset_total_reads} reads over {zset_total_members} members = 0.00 per member, with member \
         bytes returned"
    );
}

/// THE COST IS PER COLD LISTING, NOT PER CALL.
///
/// Stated separately so the count above is not read as a per-request figure. `cached_response`
/// stores the encoded answer under `CacheKey::set_members`, so the second listing of a key reads
/// nothing. The first is asserted non-zero as this test's own denominator: if the first read no
/// pages either, the second reading none would say nothing.
///
/// rust-internal: drives the engine's own command surface
#[test]
fn a_second_set_listing_reads_no_pages_at_all_because_the_answer_is_cached() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    load_on(&engine);

    let key = "cached-listing".to_string();
    const MEMBERS: usize = 16;
    for index in 0..MEMBERS {
        write(
            &engine,
            Command::SetAdd {
                key: key.clone(),
                member: member_of(32, index),
            },
        );
    }

    let (first_response, first_reads) =
        page_reads_of(|| read(&engine, Command::SetMembers { key: key.clone() }));
    let (second_response, second_reads) =
        page_reads_of(|| read(&engine, Command::SetMembers { key: key.clone() }));

    assert_eq!(MEMBERS, members_returned(&first_response).len());
    assert_eq!(MEMBERS, members_returned(&second_response).len());

    // THE DENOMINATOR.
    assert_eq!(
        MEMBERS as u64, first_reads,
        "the cold listing read {first_reads} pages for {MEMBERS} members; with zero, the warm \
         listing reading zero would be no finding"
    );
    assert_eq!(
        0, second_reads,
        "the warm listing read {second_reads} pages -- the whole answer is cached under \
         `CacheKey::set_members`"
    );
    println!(
        "\n  cold listing {first_reads} page reads, warm listing {second_reads} -- the per-member \
         cost is paid once per cache fill, not once per SMEMBERS"
    );
}

// =================================================================================================
// 2. THE REDUNDANCY, AT BYTE LEVEL
// =================================================================================================

/// THE INDEX ENTRY NO LONGER SPELLS THE MEMBER, AND AFTER A FOLD IT CANNOT EVEN COUNT THEM.
///
/// # THE COPY THIS ARM MEASURED HAS BEEN DELETED, NOT MOVED
///
/// This arm's subject was the third of the three copies in the module doc: `component =
/// hex::encode(&member)`, sitting beside the address in the walk `SetMembers` performs, so that the
/// page read recovered bytes the iteration was already holding. The entry stopped naming its
/// element, and that copy is now STRUCTURALLY UNREPRESENTABLE rather than merely absent.
/// `bucket_index_component_block_addresses` hardcodes `None` in the pair's first slot on ALL THREE
/// return paths. The old body asked the pair for its name and the `expect` fired.
///
/// AND IT IS NOT RESTATED AS `component.is_none()`, WHICH IS WHY THAT IS SAID HERE. Production
/// writes the `None` as a literal, so an `is_none()` assertion compares a constant against itself:
/// it would read like a guard over the deletion while being unable to fail. The
/// `MEMBERS * width * 2` name-byte assertion goes with the copy it measured, for the same reason:
/// the term is zero now, and zero is not measurable off a hardcoded `None`.
///
/// WHAT DOES GUARD THE DELETION IS A CONST-EVALUATED FIELD SUM, and it is named precisely because
/// the obvious candidate does not. `state.rs` reconstructs `BlockIndex`'s width from its seven
/// fields BY TYPE and asserts the sum is 40 and equal to `size_of::<BlockIndex>()`, so re-adding a
/// `component: Option<Arc<str>>` puts the sum at 56 and fails to COMPILE -- it cannot be a test
/// nothing runs. The signature tripwire in `element_ordinal_reuse`, which pins this function's
/// exact declaration text including `-> Vec<(Option<Arc<str>>, ElementEntry)>`, is a weaker and
/// different guard: it fires when the pair's SHAPE changes or a caller gains a component argument,
/// and it would NOT fire on the first slot merely starting to be populated. Both are cited rather
/// than one, because an earlier draft of this paragraph credited the tripwire with the whole job.
///
/// # WHAT IS ASSERTED INSTEAD, AND WHY IT CAN FAIL
///
/// TWO THINGS, both over sources that still exist and can disagree.
///
/// FIRST, THE MEMBER BYTES ARE STILL RECOVERABLE WITHOUT A PAGE READ -- just not from the index.
/// They are in the page PAYLOAD, and the two sides of the equality are TWO DIFFERENT FIELDS of it,
/// which is worth naming because the copies being distinct is the whole claim. The left is the
/// element KEY each item is filed under, decoded by `component_from_element_key`. The right is what
/// the listing serves, which is the item's stored VALUE -- `derive_membership` folds the pages and
/// hands back `derived.live.into_values()`. Both were written by one call site, which is what makes
/// them a redundancy and not a check.
///
/// THREE MUTATIONS WERE DRIVEN TO PROVE THEY ARE INDEPENDENT, AND THE FIRST TWO COULD NOT, so they
/// are recorded here rather than left for the next reader to repeat:
///
///   - truncating the component moved BOTH sides, because the serving path keys its fold by the
///     component: 12 members collided into 2 and the DENOMINATOR fired, not the equality;
///   - truncating the served VALUE collided them too, since this fixture's members differ only in
///     their last bytes -- again the denominator;
///   - flipping the first byte of the ELEMENT KEY is the one that isolates it. Every member here
///     starts with the same digit, so all twelve stay distinct, the listing still serves what was
///     written, and only the key-decoded copy moves. That reddens this equality and nothing above
///     it.
///
/// SECOND, AND THIS IS THE PART THE OLD ARM COULD NOT HAVE STATED: the walk now returns one pair
/// per PAGE rather than one per element, so after a FOLD it hands back strictly fewer pairs than
/// there are members. Before the collapse those were equal at every width -- which is what made
/// "the component beside the address already spells the member" sayable at all. A strict inequality
/// is asserted rather than a magnitude, because the number of pages a fold lands on is a property
/// of the batcher and moves with the fixture.
///
/// THE DENOMINATOR IS ASSERTED ON BOTH, as it was before: over an empty key every set here is
/// empty and the equalities would pass while exercising no member.
///
/// rust-internal: reads the engine's own page index and block store over a seeded store
#[test]
fn every_member_a_set_listing_returns_is_already_spelled_by_its_component() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    load_on(&engine);

    println!("\n=== the index entry no longer spells the member; the payload still does ===");
    println!(
        "  {:>6}  {:>8}  {:>10}  {:>12}  {:>12}",
        "width", "members", "payload", "pairs", "named pairs"
    );

    const MEMBERS: usize = 12;
    let mut total_pairs = 0usize;
    for width in WIDTHS {
        let key = format!("spelled-{width}");
        let mut written: BTreeSet<Vec<u8>> = BTreeSet::new();
        for index in 0..MEMBERS {
            let member = member_of(width, index);
            written.insert(member.clone());
            write(
                &engine,
                Command::SetAdd {
                    key: key.clone(),
                    member,
                },
            );
        }

        let from_pages: BTreeSet<Vec<u8>> =
            members_returned(&read(&engine, Command::SetMembers { key: key.clone() }))
                .into_iter()
                .collect();
        let from_payload = payload_members(&engine, &key);

        let (pairs, named) = {
            let shards = engine.shards.read().expect("engine lock poisoned");
            let shard = shards.get(&1).expect("shard is loaded");
            let walk = crate::engine::bucket_store::bucket_index_component_block_addresses(
                shard, "set", &key,
            );
            let named = walk.iter().filter(|(c, _)| c.is_some()).count();
            (walk.len(), named)
        };

        // THE DENOMINATOR.
        assert_eq!(
            MEMBERS,
            from_pages.len(),
            "the listing for {key} returned {} member(s), not the {MEMBERS} written -- an empty \
             or short answer makes the equalities below vacuous",
            from_pages.len()
        );
        assert!(
            pairs >= 1,
            "the page-index walk for {key} returned no pairs at all, so the pair count below is \
             not a count of anything"
        );

        assert_eq!(
            written, from_pages,
            "the listing for {key} did not return what was written"
        );
        // THE FINDING, FIRST HALF: the bytes are in the payload.
        assert_eq!(
            from_payload, from_pages,
            "the members decoded from the PAGE PAYLOAD differ from the members the listing \
             returns for {key}. The element key an item is filed under is the member in hex, so \
             these are the same bytes twice -- and since the entry stopped naming its element, the \
             payload is where that redundancy now lives"
        );

        println!(
            "  {width:>6}  {MEMBERS:>8}  {:>10}  {pairs:>12}  {named:>12}",
            from_payload.len()
        );
        total_pairs += pairs;
    }

    assert!(
        total_pairs >= WIDTHS.len(),
        "only {total_pairs} pair(s) were examined across {} width(s)",
        WIDTHS.len()
    );

    // THE FINDING, SECOND HALF: fold one key and the walk cannot even count its members.
    let folded_key = "spelled-folded".to_string();
    for index in 0..MEMBERS {
        write(
            &engine,
            Command::SetAdd {
                key: folded_key.clone(),
                member: member_of(WIDTHS[0], index),
            },
        );
    }
    crate::engine::reset_container_batch_counts();
    engine
        .compact_shard_blocks(1)
        .expect("the compaction round failed");
    let (batches, folded) = crate::engine::container_batch_counts();
    assert!(
        batches > 0 && folded > 0,
        "the folded arm folded nothing: {batches} batch(es), {folded} page(s), so the pair count \
         below is the unfolded one and says nothing"
    );
    let served_folded = members_returned(&read(
        &engine,
        Command::SetMembers {
            key: folded_key.clone(),
        },
    ))
    .len();
    let folded_pairs = {
        let shards = engine.shards.read().expect("engine lock poisoned");
        let shard = shards.get(&1).expect("shard is loaded");
        crate::engine::bucket_store::bucket_index_component_block_addresses(
            shard,
            "set",
            &folded_key,
        )
        .len()
    };
    assert_eq!(
        MEMBERS, served_folded,
        "the folded key serves {served_folded} member(s), not {MEMBERS}, so the comparison below \
         is against the wrong answer"
    );
    assert!(
        folded_pairs >= 1 && folded_pairs < served_folded,
        "the folded key's walk returned {folded_pairs} pair(s) for {served_folded} served \
         member(s). One entry a page is what makes this strict: before the collapse the walk \
         returned a pair per element and these were equal, which is what let this arm claim the \
         component beside the address already spelled the member"
    );
    println!(
        "\n  folded: {folded_pairs} walk pair(s) for {served_folded} served member(s) -- the index \
         no longer counts members, let alone names them; the payload does both"
    );
}

// =================================================================================================
// 3. WHY THE RESIDENT MAP IS NOT THE CHEAPER SOURCE
// =================================================================================================

/// THE RESIDENT MAP AND WHAT THE STORE ACTUALLY SERVES ARE NOT ONE POPULATION, AND THE LIVE PAGE
/// INDEX CAN NO LONGER BE ASKED WHICH MEMBERS A SET HOLDS AT ALL.
///
/// # WHY THIS IS RESTATED AND NOT RE-GOLDENED
///
/// This arm compared `shard.sets` against a member population DECODED OUT OF THE COMPONENT each
/// page is filed under. That population no longer exists.
/// `bucket_index_component_block_addresses` hardcodes `None` in the pair's first slot on ALL THREE
/// of its return paths -- the lookup arm, the released arm and the bucket walk -- so the walk hands
/// back addresses and nothing else. Measured here: resident map 2 member(s), live page index 0.
///
/// AND THE DERIVED SIDE OF THIS MERGE IS NOW EMPTY, which is the fact that makes the rest of it
/// matter. `reconcile_secondary_views_from_bucket_index` builds `derived` from the live page index,
/// and rebuilding a set's members from there needs the component the entry no longer carries.
/// Measured by handing the merge an EMPTY persisted map: `shard.sets` came back empty, so nothing
/// in it came from the derived side. The resident set map is therefore wholly the durable
/// snapshot's now, and the address test in `fill_absent_elements` is the only filter standing
/// between a stale snapshot and the map a reader trusts.
///
/// SO THE COMPARISON IS RE-ATTRIBUTED TO THE SOURCE THAT CAN STILL ANSWER, which is the serving
/// path. `SMEMBERS` is asked outright, with the shard lock DROPPED first, and its answer is the
/// population a listing walks. That is not a convenience: the page PAYLOAD cannot stand in for it,
/// because the payload OVER-REPORTS. Measured on this fixture, the removed member's page is still
/// live and still states the member as present, with the removal held in a SEPARATE tombstone page
/// whose entry is deleted. A population read off the payload therefore returns 2 and agrees with
/// the resident map, which would make the control below pass and the finding vanish.
///
/// # AND THE FINDING IS INVERTED BACK, WITH A NEW MECHANISM
///
/// This assertion has now been turned over twice, and the history is the point. It first read
/// `in_map.contains(&removed)` -- the merge resurrects. #2017's fix made `fill_absent_elements` ask
/// of every persisted element the question #2005 asks of every CARRIED one, "is there still a page
/// at this address?", and the assertion was inverted to `!in_map.contains(&removed)`.
///
/// THAT FIX'S PREMISE WAS ONE PAGE PER ELEMENT, AND THE COLLAPSE REMOVED IT. With one index entry a
/// page, a page SURVIVES the removal of one of its elements: the address stays live while the
/// element on it is dead. An address-liveness test therefore stopped being a proxy for
/// element-liveness at exactly that moment, and the merge resurrects again -- measured at map 2
/// against a served 1. The assertion is turned back rather than deleted, so that if the merge is
/// ever taught to ask about the ELEMENT the arm goes red and says so.
///
/// THE REFUSAL THIS MODULE EXISTS FOR IS THEREFORE RE-ESTABLISHED ON FRESH GROUNDS, and it is
/// stronger than it was: a listing served from `shard.sets` would hand back a member the store was
/// told to forget, and the only reason the store does not is that the listing is not served from
/// there. Both halves are asserted, because the divergence alone does not say which side is right.
///
/// THE CONTROL is the member that was never removed: it must be in BOTH populations, or the fixture
/// is one where the serving path is simply empty and the finding is an artefact.
///
/// rust-internal: calls the engine's own reconcile directly, then the command surface
#[test]
fn the_resident_set_map_and_the_live_page_index_are_not_the_same_population() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    load_on(&engine);

    let key = "divergence".to_string();
    let kept = b"kept-member-stays-live".to_vec();
    let removed = b"removed-member-page-gone".to_vec();

    write(
        &engine,
        Command::SetAdd {
            key: key.clone(),
            member: kept.clone(),
        },
    );
    write(
        &engine,
        Command::SetAdd {
            key: key.clone(),
            member: removed.clone(),
        },
    );

    // The durable map as a snapshot written BEFORE the removal holds it: both members, with their
    // addresses. Captured from the live map, which is exactly what `set_index_serde` serializes.
    let persisted_before_removal = {
        let shards = engine.shards.read().expect("engine lock poisoned");
        let shard = shards.get(&1).expect("shard is loaded");
        shard
            .sets
            .get(&key)
            .expect("the seeded key is in the resident map")
            .clone()
    };
    assert_eq!(
        2,
        persisted_before_removal.len(),
        "the pre-removal snapshot holds {} members, not the 2 written",
        persisted_before_removal.len()
    );

    write(
        &engine,
        Command::SetRemove {
            key: key.clone(),
            member: removed.clone(),
        },
    );

    // After the removal the command surface is self-consistent: this is the state it maintains.
    let listed_after_removal: BTreeSet<Vec<u8>> =
        members_returned(&read(&engine, Command::SetMembers { key: key.clone() }))
            .into_iter()
            .collect();
    assert_eq!(
        1,
        listed_after_removal.len(),
        "the listing after the removal returned {} members, not 1",
        listed_after_removal.len()
    );
    assert!(
        !listed_after_removal.contains(&removed),
        "the removed member is still listed, so this fixture never removed anything"
    );

    // THE REMOVED MEMBER'S PAGE OUTLIVES ITS ELEMENT, and that is the premise of everything below,
    // so it is measured rather than asserted from the source. One entry a page means a page cannot
    // be dropped because one of its elements went.
    let payload_population = payload_members(&engine, &key);
    assert!(
        payload_population.contains(&removed),
        "the live pages no longer state the removed member, so an address-liveness test would \
         refuse it and this arm has nothing to find. One entry a page is what keeps the page alive \
         through the removal of its element"
    );

    // Now put the durable map back as a snapshot predating the removal would, and run the load
    // path's reconcile over the live page index.
    let in_map: BTreeSet<Vec<u8>> = {
        let mut shards = engine.shards.write().expect("engine lock poisoned");
        let shard = shards.get_mut(&1).expect("shard is loaded");
        shard
            .sets
            .insert_elements_for_test(&key, persisted_before_removal);

        let live_pages: usize = shard
            .bucket_index
            .bucket_map
            .values()
            .map(|bucket| {
                bucket
                    .block_index
                    .values()
                    .filter(|page| !page.deleted)
                    .count()
            })
            .sum();
        assert!(
            live_pages >= 1,
            "DENOMINATOR: {live_pages} live pages, so the derived view is empty and the merge \
             below would keep everything for the wrong reason"
        );

        crate::engine::storage_bucket_internals::reconcile_secondary_views_from_bucket_index(
            &engine.block_store,
            shard,
            None,
        );

        // THE INDEX WALK, KEPT AS A MEASUREMENT RATHER THAN A POPULATION. It is what used to be
        // decoded into members; it is reported so that the reason this arm changed shape is a
        // number in the output and not only a paragraph above.
        let named =
            crate::engine::bucket_store::bucket_index_component_block_addresses(shard, "set", &key)
                .iter()
                .filter(|(component, _)| component.is_some())
                .count();
        println!("\n=== after a removal and a reconcile against a snapshot that predates it ===");
        println!("  index walk pairs carrying a member name: {named}");

        shard
            .sets
            .get(&key)
            .map(|members| members.keys().cloned().collect())
            .unwrap_or_default()
    };

    // THE SERVING PATH, asked with the lock released.
    let served: BTreeSet<Vec<u8>> =
        members_returned(&read(&engine, Command::SetMembers { key: key.clone() }))
            .into_iter()
            .collect();
    println!(
        "  resident map {} member(s), served {} member(s), pages state {} member(s)",
        in_map.len(),
        served.len(),
        payload_population.len()
    );

    // THE CONTROL.
    assert!(
        in_map.contains(&kept),
        "the member that was never removed is absent from the resident map, so the merge dropped \
         everything and the finding below is an artefact"
    );
    assert!(
        served.contains(&kept),
        "the member that was never removed is not served, so this fixture's serving path says \
         nothing and the comparison below is between one population and an empty one"
    );

    // THE FINDING, HALF ONE: the merge resurrects, because it tests the ADDRESS and the address is
    // still live.
    assert!(
        in_map.contains(&removed),
        "the resident map no longer holds the removed member. `fill_absent_elements` tests whether \
         a page is still live at the persisted element's address, and one entry a page keeps that \
         page alive through the removal of its element -- so this held at map 2 against a served 1. \
         If the merge has been taught to ask about the ELEMENT rather than the page, this arm is \
         the record of why it had to be, and the assertion should be turned over again with the \
         mechanism named"
    );

    // THE FINDING, HALF TWO: and the store is nonetheless right, because the listing is not served
    // from there. This is the half that says which side of the divergence is correct.
    assert!(
        !served.contains(&removed),
        "the store SERVED a member it was told to forget. This is the defect the refusal exists to \
         prevent: the resident map holds the removed member, so anything answering out of \
         `shard.sets` returns it"
    );
    assert_ne!(
        in_map, served,
        "the resident map and the served population are now one population, so the divergence this \
         arm reports has been fixed and the refusal it argues for needs re-deriving rather than \
         re-asserting"
    );
    println!(
        "  => the resident map over-reports by {}, and the refusal stands: a listing served from \
         `shard.sets` would hand back a removed member",
        in_map.len() - served.len()
    );
}
