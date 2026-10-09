// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! AN ELEMENT ORDINAL WOULD HAVE TO BE HANDED OUT BY A COUNTER, AND EVERY PLACE TO KEEP ONE IS
//! REBUILT FROM THE PAGES THAT LOSE IT.
//!
//! # WHAT WAS ASKED
//!
//! `BlockIndex.component` is `Option<Arc<str>>` -- sixteen bytes on every page entry, plus one heap
//! allocation per page for a name this engine RENDERED rather than received. For the container kinds
//! the name is content-derived:
//!
//! ```text
//!     zset    format!("{score_bits:016x}{}", hex::encode(member))
//!     set     hex::encode(member)
//!     list    format!("{:016x}", (seq as u64).wrapping_sub(i64::MIN as u64))
//!     hash    the caller's OWN field name
//!     string  no component at all
//! ```
//!
//! So: replace the name with a small per-element ordinal. #1985 refuted the idea on three grounds and
//! two are gone -- #1989 made the durable map outrank the name, and #1982 enumerates what replay
//! rebuilds. The third was that no per-element ordinal EXISTS for the kinds that would benefit, and
//! this module is that question: **can one be assigned and PERSISTED, with non-reuse ENFORCED rather
//! than coincidental?**
//!
//! It cannot, and it is refused TWICE OVER. The reasons are measured below rather than argued.
//!
//! # THE STOP CONDITION, WHICH IS WORSE THAN THE NON-REUSE ONE AND IS SETTLED FIRST
//!
//! `an_ordinal_loses_the_member_the_fold_delivers_without_a_durable_entry`. The case FOR an ordinal
//! rests on the member already being persisted twice, which the reconcile's own comment supports by
//! calling the name "a second copy of it rendered as text". But the same comment names the exception:
//! the name is "the fallback for a member the durable map does not have, which is how an element
//! folded out of the delta log arrives".
//!
//! `apply_key_states` folds THIRTEEN maps and `sets`, `zsets`, `lists` and `hashes` are **not** among
//! them, while `fold_delta_block_items` DOES restore the page items. So after a fold the index holds
//! pages for elements the durable map never received, and for those the component is the ONLY copy of
//! the member. Driven with #1989's own fixture and the name made ordinal-shaped: the control comes back
//! at 1.0 and the ordinal-named member comes back as `None`. Its bytes survive only in the page
//! PAYLOAD, which the reconcile deliberately does not read.
//!
//! **An ordinal here is a lossy migration, so it was not built.** For it to be safe the FOLD must carry
//! the member bytes -- a change to the delta record's outcome shape, which is #1982's territory.
//!
//! # THE THREE WAYS TO STOP A NUMBER BEING REUSED, AND WHAT EACH ONE HITS
//!
//! ## 1. A TOMBSTONE THAT KEEPS ITS NUMBER IN THE `max`
//!
//! **THIS SECTION HAS BEEN OVERTAKEN, AND WHAT OVERTOOK IT IS NOT THE ORDINAL.** It used to say there
//! was no tombstone to keep a number in, because EVERY delete path removed the page: the per-element
//! four went through `mark_bucket_index_block_deleted`, named for a mark it did not make, whose body
//! was `block_index.retain(.., |_, page| ..false..)`. That was correct, and it is the reason the
//! reservation this section is about could not be built.
//!
//! There IS a per-element tombstone now, and it arrived for an unrelated reason: a container's pages
//! became the statement of its membership, and a page nothing points at is a page no derivation can
//! read. So a removal keeps an entry pointing at the page that records it.
//!
//! **IT STILL DOES NOT RESERVE THE NUMBER, and that is deliberate rather than incidental.**
//! `container_page_ordinal` filters `deleted`, so the tombstone is invisible to the `max` and the
//! freed ordinal is still handed to the next element --
//! `the_element_ordinal_a_delete_frees_is_handed_straight_back_to_the_next_element` still drives that
//! and still passes. The filter was not free: without it the ordinal would climb once per element ever
//! written rather than once per live element, and a container churning distinct members would walk to
//! `MAX_ADDRESSABLE_BLOCK_ID` and fall off the ceiling. So the tombstone exists and the reuse this
//! module is about is unchanged; what a reader must not conclude is that a tombstone now reserves
//! anything.
//!
//! `every_per_element_delete_leaves_one_tombstone_and_the_whole_object_delete_leaves_none` drives the
//! new split over the same five arms the old claim covered.
//!
//! **THIS WIDENS #1990.** That issue names the whole-object path (`FeatureAppend`, `FeatureDelete`,
//! `FeatureAppend`) and says of a partial removal that it "does not free a number, because for the
//! container kinds no per-element number is assigned at all". True today, and it is true only because
//! nothing is assigned: the page it would have been assigned to is removed by the same `retain`. The
//! per-element path is the one an element ordinal would live on, and it is a removal too. It follows
//! that #1976's published mechanism -- "the absence of a `!block.deleted` filter is what reserves the
//! ordinal" -- is wrong on BOTH paths and not just the one #1990 corrected; `page.deleted` is written
//! by the upsert and read by five filters, and no delete command in this engine ever sets it.
//!
//! ## 2. THE ONE TOMBSTONE THIS STORE DOES KEEP, AND WHY IT IS STILL NOT A RESERVATION
//!
//! `BucketNode::deleted_object_index` is a real, persisted tombstone -- "THE TOMBSTONE SIDE OF A
//! BUCKET". It holds `stable_block_object_id(shard, kind, key)`, so under an ordinal it
//! would be keyed BY THE ORDINAL, which looks exactly like the reservation wanted.
//!
//! **THE FIRST DRAFT OF THIS MODULE REFUTED IT ON THE WRONG GROUND AND THE TEST WENT RED.** The claim
//! was that the reusing write clears it, reasoning off
//! `deleted_object_index.remove(&object_id)` sitting four statements before the page insert. That
//! call was in `sync_bucket_index_object_blocks_with_mode` -- the WHOLE-OBJECT restate path -- and
//! NOT in `upsert_bucket_index_block_inner`, the path every container ELEMENT write takes, which
//! did `object_index.insert(object_id)` and never touched the tombstone. Measured then: after
//! `ZSetAdd`, `ZSetRemove`, `ZSetAdd` of the same member the tombstone still held that page's
//! object id, and a LIVE page's object reported `deleted` through the public report. **That
//! asymmetry was a reporting defect and is fixed**; the element path clears the id it files a live
//! page for, and the test below holds the fixed arithmetic.
//!
//! The reuse idea is refuted on three grounds that the fix does not touch, all in
//! `the_element_rewrite_clears_the_tombstone_so_a_live_page_reads_as_hot`:
//!
//!   1. **IT IS A MEMBERSHIP SET, NOT A MARK.** It holds a HASH of the component. It answers "was
//!      ordinal N used?" and cannot answer "what is the highest?", so an assignment on it probes
//!      one candidate at a time and cannot reach a ceiling without 65,535 probes.
//!   2. **CLEARING IS WHAT KEEPS THE REPORT HONEST, AND A RESERVATION CANNOT ALLOW IT.** Both
//!      write paths now drop the id: the element one for the page it files, and a whole-object
//!      restate for every address it republishes -- so an unrelated whole-object write discards
//!      every element's reservation at once, and the element path discards its own on re-add.
//!   3. **A RESERVATION IS NEVER CLEARED, AND THAT INVERTS ITS OWN MEASURED ECONOMICS.** The field's
//!      doc chose its shape on a census -- 97.68% of buckets carry no tombstone, the widest carries a
//!      single id -- and named the turnover: "the shape wins while fewer than about a quarter of
//!      buckets carry one and loses above that, against a measured 2.32%". Kept for ever it is one id
//!      per removed element in every bucket that has ever had a removal.
//!
//! **AND THE STALE TOMBSTONE IS A DEFECT IN ITS OWN RIGHT, FOUND IN PASSING.**
//! `object_manager::runtime_report` reads `page.deleted || bucket.deleted() ||
//! bucket.deleted_object_index.contains(&page.object_id())`, so a LIVE page whose member was once
//! removed and re-added reports its object `deleted` and is counted as a deleted block ref instead of
//! a hot one. Reachable, not dead: `TemporalEngine::object_manager_runtime_report` is public and
//! `recovery_sweep_compact` calls it twice. Driven through that public surface. FOUND, NOT FIXED --
//! the missing `deleted_object_index.remove` belongs on the element write path, which is a change to
//! the write path and not to a test module.
//!
//! ## 3. A PERSISTED COUNTER -- WHICH IS THE ONE THIS STORE ALREADY REJECTED, TWICE
//!
//! Both rejections are in the tree and both are about this.
//!
//! `next_block_index_for_object`, which is the existing page ordinal, states the design:
//!
//! > Read from the blocks the object already has rather than from a counter, so it survives a restart
//! > without anything having to be persisted for it.
//!
//! And `block_index_handle` records what the counter COST:
//!
//! > Derived rather than assigned, because handles are written to disk inside the lookup's refs. Two
//! > processes holding the same page must compute the same handle or those refs point at nothing --
//! > which is what a counter did, silently, until a reload lost an object.
//!
//! **THE SECOND QUOTE IS NOT THE ONE THAT REFUTES THIS, AND SAYING SO IS THE POINT.** A handle is
//! assigned and never stored, so it must be recomputable; an element ordinal would be a STORED field
//! of the page, and a stored field does not have to be recomputed on load. Read carelessly the quote
//! closes the question, and it does not. What closes it is the third clause of the same sentence
//! applied to the ordinal's own home: **a reload lost an object because the number and the thing it
//! named were recovered from different places.**
//!
//! # SO WHERE COULD A HIGH-WATER MARK LIVE? THERE IS EXACTLY ONE CANDIDATE, AND THE REBUILD TAKES IT
//!
//! `a_high_water_mark_lives_only_where_the_rebuild_recomputes_it` drives a reload and enumerates the
//! three structures:
//!
//! ```text
//!     CoreIndex.bucket_map            SERIALIZED      the pages -- max over them is the derivation
//!                                                     #1990 shows dropping back
//!     CoreIndex.object_block_lookup   skip_serializing rebuilt from the bucket map on load
//!     ShardState.sets/zsets/lists     SERIALIZED      the durable maps #1989 made authoritative
//!     ShardState.hashes               skip_serializing rebuilt from the bucket map on load
//! ```
//!
//! So the durable maps are the candidate, and they genuinely survive a reload -- asserted, not
//! assumed. But the store's recovery contract is that **a stale index is treated exactly like an
//! absent one and rebuilt from the log**, and the rebuild door is
//! `reconcile_secondary_views_from_bucket_index`, which derives those same maps FROM THE PAGES and
//! merges. A mark held in the durable map is therefore recomputed as a `max` over the elements the
//! pages still hold -- which is the derivation the delete drops back. Driven below: the mark is 3
//! before the delete, 3 after it in memory, and 2 after the reload.
//!
//! **A counter that the rebuild can recompute from the data is not a counter. A counter the rebuild
//! cannot recompute cannot survive the rebuild this store requires.** That is the refutation, and it
//! is the same failure the `block_index_handle` comment names, reached from the other end.
//!
//! # AND THE ANSWER IS NOT ONE ANSWER -- IT IS FOUR, ONE PER KIND
//!
//! `what_an_element_ordinal_would_mean_for_each_kind` asserts the split in both directions, the way
//! #1982 asserted its two:
//!
//! ```text
//!     list    AN ORDINAL ALREADY EXISTS AND IS ALREADY PERSISTED. The component IS
//!             `format!("{:016x}", biased_seq)` and `shard.lists` is keyed by the `i64` itself. The
//!             only thing left to change is the SPELLING -- sixteen characters for eight bytes -- and
//!             that is not an ordinal proposal, it is #1976's fixed-width one with a narrower field.
//!     hash    REFUTED OUTRIGHT AND NOT ON PERFORMANCE. The component is the CALLER'S field name.
//!             An ordinal does not respell it, it destroys it -- and `hash` is the only kind the point
//!             read path reaches at all, twice, with that name as the argument.
//!     zset    THE ONLY TWO AN ORDINAL WOULD HELP, and the only two where it has to be ASSIGNED.
//!     set     Both already hold the member in a persisted map keyed BY the member, so the ordinal
//!             would be a third copy of an identity that already has two.
//! ```
//!
//! # THE READ PATH, COUNTED HERE RATHER THAN INHERITED
//!
//! #1985 established three call sites. Confirmed independently and at source level by
//! `the_point_read_path_is_three_call_sites_and_not_one_of_them_is_a_container`, with the input size
//! asserted before any count is believed: `bucket_index_block_address` has ONE non-doc caller in the
//! crate -- `read_bucket_index_value` -- and that has THREE, all in `execute_on_shard.rs`:
//! `StringGet` with `None`, `HashGet` with `Some(field)`, `HashIncrBy` with `Some(field)`. **No
//! container kind is reached by the point lookup at all.**
//!
//! `bucket_index_component_block_addresses` -- the reader #1986 found refutes ideas its neighbours
//! survive -- has three callers, `hash` twice and `set` once, and its signature is the whole of why:
//! `(shard, model_id, object_key)`. It takes no component and its caller has none to give, so an
//! ordinal answers it exactly as well as a name does. **It is not the obstacle here**, and that is
//! worth writing down because it was the obstacle twice before.
//!
//! # THE ORDERING PROPERTY, WHICH IS THE OBVIOUS REFUTATION AND IS NOT ONE
//!
//! A zset component is score-bits-then-member and a list component is an `i64::MIN`-biased sequence,
//! both so that lexical order IS the element's order; the write arm says the bias is "what lets
//! recovery and range reads walk the bucket index directly". An ordinal that did not preserve that
//! would force those readers to sort, and the sort could easily cost more than the bytes.
//!
//! **No reader on this revision takes its order from the component.**
//! `the_component_ordering_property_is_consumed_by_no_reader` classifies every consumer: the three
//! reconcile arms decode each component and insert into a `BTreeMap` keyed by the MEMBER BYTES or the
//! `i64` SEQUENCE, so order is re-established by the map rather than inherited from the walk; range
//! reads use `shard.zsets` (keyed by member, "derived per query -- V1 accepts the per-range sort") and
//! `shard.lists` (keyed by the `i64`); and the three readers that do sort by component need only a
//! TOTAL order, which an ordinal has. Of the three callers of
//! `bucket_index_component_block_addresses`, `SetMembers` discards the component, `HashLen` reads
//! `len()`, and `HashGetAll` keeps a field name an ordinal never touches.
//!
//! So the sixteen characters of score prefix on every zset element buy nothing that is consumed. That
//! is a point IN FAVOUR of the ordinal, recorded because a refutation must not collect support it has
//! not earned.
//!
//! # THE MIS-PARSE RATE, MEASURED
//!
//! #1976 assumed 0% and measured 1.935%. #1985 measured 100% for lists. An ordinal against a
//! hexadecimal name is the widest of the three, and
//! `the_mis_parse_rate_of_an_ordinal_against_the_names_this_store_already_holds` measures it per kind
//! over 20,000 names with the denominator printed:
//!
//! ```text
//!     list   20000 of 20000 well-formed ordinals of a DIFFERENT meaning   100.000%
//!     set     5007 of 20000                                               25.035%
//!     zset      20 of 20000                                                0.100%
//! ```
//!
//! **THE SET ROW IS THE ONE NOBODY PREDICTED: 25.035%.** A set component is `hex::encode(member)` and
//! nothing else, so every EIGHT-BYTE member spells exactly sixteen hexadecimal characters -- an
//! ordinary member length, not a corner. #1976's 1.935% was measured against a different spelling; a
//! sixteen-character ordinal against set names is thirteen times worse. The list row is the one that decides the stored
//! shape, and it is 100.00% by construction rather than by luck: a list component is SIXTEEN
//! hexadecimal characters and so is a sixteen-character ordinal, and `"0000000000000000"` is
//! simultaneously sequence `i64::MIN` and ordinal 0.
//!
//! # WHAT IT WOULD HAVE BEEN WORTH, AND IT IS NOT SIXTEEN BYTES -- THE MEMBER IS STORED TWICE
//!
//! The sixteen-byte pointer is the smaller half. `what_a_component_costs_an_element_at_five_member_widths`
//! measures the component against the member it names, per kind, at five widths, request and chunk,
//! as a HISTOGRAM with the store path held at 15 characters:
//!
//! ```text
//!     kind      n   pages  comp chars   REQUEST B    CHUNK B  chars/n
//!     set       8     100          16       48.00      68.64     2.00
//!     set      16     100          32       64.00      80.80     2.00
//!     set      32     100          64       96.00     114.40     2.00
//!     set      64     100         128      160.00     177.12     2.00
//!     set     256     100         512      544.00     560.16     2.00
//!     zset      8     100          32       64.00      80.80     4.00
//!     zset     16     100          48       80.00      96.16     3.00
//!     zset     32     100          80      112.00     130.56     2.50
//!     zset     64     100         144      176.00     192.16     2.25
//!     zset    256     100         528      560.00     576.32     2.06
//!     list      8     100          16       48.00      64.96     2.00   <- CONTROL, flat
//!     list    256     100          16       48.00      65.76     0.06   <- CONTROL, flat
//!     string  any     100        none        0.00       0.00        -   <- CONTROL, no component
//! ```
//!
//! **`chars/n` IS EXACTLY 2.00 FOR `set` AT EVERY WIDTH.** `hex::encode` spells the member at two
//! characters a byte, so a set element holds its member ONCE as the page payload and AGAIN, at double
//! width, as the name of the page. A zset adds a fixed sixteen characters of score bits on top -- and
//! that score is a third copy, because `zset_index_serde` persists it as a `u64` in the durable map.
//!
//! So at a sixteen-byte member a zset element pays **96.16 B of chunk for the name plus the 16-byte
//! fat pointer in the entry, to name sixteen bytes it already holds twice elsewhere.** At 256 bytes it
//! pays 576.32 + 16. That is far more than the sixteen-byte pointer this work was scoped around, and
//! it is why the refutation below is worth stating precisely rather than briefly.
//!
//! **TWO CONTROLS, BOTH AT THE PREDICTED ZERO EFFECT.** `list`'s component is sixteen characters
//! whatever the member is, and it reads 48.00 B a name at n=8 and 48.00 at n=256 -- flat across a 32x
//! change, asserted as an equality. `string` has no component and reads 0.00 at every width. A kind
//! that moved in either control would mean the measurement was picking up something other than the
//! member's spelling.
//!
//! **AND THE DOUBLING CAN BE ATTACKED WITHOUT AN ORDINAL, WHICH IS THE ACTIONABLE HALF OF THIS.** The
//! hex is there because the component is `Arc<str>` and a member is arbitrary bytes. A component that
//! held BYTES rather than characters halves it -- 2n back to n -- and needs no assignment, no
//! high-water mark and no non-reuse enforcement, because the name is still derived from the member.
//! That is a different change from this one and it is not refuted by anything below.
//!
//! The pointer half is measured separately by `what_an_element_ordinal_would_have_saved_a_page`, at
//! both routing ranges and two corpus sizes: 58.06-58.13 B requested and 69.20-69.32 B of chunk a
//! name over a container store whose mean name is 22.4 characters, against 0.00 on the routed control
//! where no page carries a component at all. Two populations and never a mean: routed p50 1 page an
//! object, containers p50 100.
//!
//! # THE WIDTH AND THE CEILING, IF IT EVER LANDS
//!
//! `the_width_an_element_ordinal_would_take_and_the_ceiling_that_implies` reads the widths from the
//! fields themselves and states the ceiling. The number that matters is not ours to choose freely:
//! #1994 narrowed `BlockAddress::block_id` to SIXTEEN BITS, so an ordinal folded into the existing
//! per-object ordinal caps at **65,535 elements an object** -- which is exactly the ceiling the
//! design that prompted this work has, against a measured container shape of p50 100 and one observed
//! case at 2,000. Thirty-two times the headroom, not sixty-five thousand.

#![allow(clippy::all)]
use super::*;

const OPERATOR_END: u32 = 1023;
const WHOLE_KEYSPACE_END: u32 = u32::MAX;

// =================================================================================================
// HARNESS
// =================================================================================================

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
        table_name: "element-ordinal".to_string(),
        shard_uri: "local://element-ordinal/1".to_string(),
        start_routing_bucket: 0,
        end_routing_bucket,
        readonly: false,
        load_version: 1,
        local_node_id: Some(1),
    });
    assert!(
        response.status.ok,
        "the fixture could not load shard 1 over 0..={end_routing_bucket}: {:?}",
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

/// Every page this object holds, as `(component, block_id)`, whatever its state.
///
/// Reads the SAME walk `next_block_index_for_object` reads -- every page of the object in the
/// bucket, with no filter on `deleted` -- so a count taken here is the count the assignment would
/// take.
/// THE OBJECT'S LIVE PAGES. Tombstone entries are excluded, and the exclusion is new.
///
/// WHAT THIS USED TO RETURN AND WHY THE CHANGE IS NOT A RELAXATION. It returned EVERY entry, with no
/// `deleted` filter -- and that was the same set, because nothing in this engine ever set
/// `BlockIndex::deleted` to true. Live entries and all entries were one quantity, so the distinction
/// could not be expressed and did not need to be. A container removal now leaves a tombstone entry
/// behind so the page recording the removal stays reachable, and the two quantities have come apart.
///
/// Every assertion in this module that uses this helper means the LIVE set: it is asking what the
/// object holds and what ordinal the next element would take. So the filter restores each of those
/// claims to the quantity it was always about, rather than weakening any of them.
/// [`tombstoned_pages_of`] is the other half, and the arms below assert BOTH.
fn pages_of(engine: &TemporalEngine, kind: &str, key: &str) -> Vec<(Option<String>, Option<u64>)> {
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard 1 is loaded");
    let mut held = Vec::new();
    for bucket in shard.bucket_index.bucket_map.values() {
        for (_, page) in bucket.block_index.iter() {
            if page.deleted {
                continue;
            }
            if page.model_id.as_str() == kind && &*page.object_key == key {
                held.push((
                    page.component.as_deref().map(str::to_string),
                    page.address.block_id(),
                ));
            }
        }
    }
    held.sort();
    held
}

/// The ordinals the object's TOMBSTONE entries carry, ascending.
///
/// Needed because a freed ordinal is now held by two entries for a while -- the tombstone that kept
/// it and the element that was handed it back -- and a guard about reuse has to be able to say which
/// is which rather than counting two and calling it corruption.
fn tombstone_ordinals_of(engine: &TemporalEngine, kind: &str, key: &str) -> Vec<Option<u64>> {
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard 1 is loaded");
    let mut held = Vec::new();
    for bucket in shard.bucket_index.bucket_map.values() {
        for (_, page) in bucket.block_index.iter() {
            if page.deleted && page.model_id.as_str() == kind && &*page.object_key == key {
                held.push(page.address.block_id());
            }
        }
    }
    held.sort();
    held
}

/// Pages of this object that carry `deleted == true`.
fn tombstoned_pages_of(engine: &TemporalEngine, kind: &str, key: &str) -> usize {
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard 1 is loaded");
    let mut count = 0usize;
    for bucket in shard.bucket_index.bucket_map.values() {
        for (_, page) in bucket.block_index.iter() {
            if page.model_id.as_str() == kind && &*page.object_key == key && page.deleted {
                count += 1;
            }
        }
    }
    count
}

/// The number an element-ordinal assignment would hand out next, derived the way this engine's
/// existing per-object ordinal is derived: one past the highest the object currently holds.
///
/// Counts ELEMENTS -- the pages of the object -- because that is what an element ordinal numbers.
/// A store where nothing is assigned yet answers with the page count, which is the same number a
/// first assignment would reach.
fn next_element_ordinal_would_be(engine: &TemporalEngine, kind: &str, key: &str) -> usize {
    pages_of(engine, kind, key).len()
}

/// Whether any bucket's persisted tombstone holds this object id.
fn tombstone_holds(engine: &TemporalEngine, object_id: u64) -> bool {
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard 1 is loaded");
    shard
        .bucket_index
        .bucket_map
        .values()
        .any(|bucket| bucket.deleted_object_index.contains(&object_id))
}

/// How many elements the durable map holds for this key, per kind.
fn durable_element_count(engine: &TemporalEngine, kind: &str, key: &str) -> usize {
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard 1 is loaded");
    match kind {
        "zset" => shard.zsets.get(key).map_or(0, |members| members.len()),
        "set" => shard.sets.get(key).map_or(0, |members| members.len()),
        "list" => shard.lists.get(key).map_or(0, |entries| entries.len()),
        "hash" => shard.hashes.get(key).map_or(0, |fields| fields.len()),
        other => panic!("no durable map is claimed for kind {other}"),
    }
}

fn zset_name(score: f64, member: &[u8]) -> String {
    crate::engine::execute_on_shard::zset_component(
        crate::engine::execute_on_shard::zset_score_bits(score),
        member,
    )
}

// =================================================================================================
// 1. A PER-ELEMENT DELETE NOW LEAVES A TOMBSTONE; A WHOLE-OBJECT DELETE STILL DOES NOT
// =================================================================================================

/// EVERY PER-ELEMENT DELETE PATH LEAVES EXACTLY ONE TOMBSTONE. THE WHOLE-OBJECT PATH LEAVES NONE.
///
/// # WHAT THIS ASSERTED BEFORE, AND WHY IT IS THE OPPOSITE NOW
///
/// It was called `FORMERLY(every_delete_path_removes_the_page_rather_than_tombstoning_it)` -- wrapped
/// so that neither a reader nor the citation check mistakes a name that no longer exists for a guard
/// that does, which is the same trap the unwritten-guard marker was for -- and it asserted
/// `tombstoned == 0` on all five arms. That was true and load-bearing: the per-element four went
/// through `mark_bucket_index_block_deleted`, whose name states a mark it does not make, and both its
/// body and the whole-object deleter's were a `retain` returning false. Nothing in this engine ever
/// set `BlockIndex::deleted` to true.
///
/// A container's PAGES are now the statement of its membership, which is what the fold this module
/// sits under exists to make possible, and a page nothing points at is a page no derivation can read.
/// So a per-element removal appends a page that records it and keeps an entry pointing at that page,
/// carrying `deleted`. The old claim is therefore FALSE BY INTENT for four of the five arms.
///
/// IT IS INVERTED HERE RATHER THAN RELAXED OR DELETED. Relaxing it -- dropping the `tombstoned == 0`
/// line -- would leave a module that no longer says anything about tombstones at all, and a guard
/// that silently swaps sides is indistinguishable from one that was wrong all along. So the count is
/// still asserted on every arm; what changed is the number it is asserted against, and that number is
/// now PER ARM rather than one constant, because the two kinds of delete differ.
///
/// # THE WHOLE-OBJECT ARM IS THE CONTROL, NOT AN EXEMPTION
///
/// `CommonDelete` drops every entry of the key, tombstones included, because there is no membership
/// left to keep true -- the object is gone. If that arm started leaving tombstones it would be
/// leaking an entry per deleted object forever, so its zero is asserted as firmly as the others' one.
///
/// THE DENOMINATOR IS PRINTED AND ASSERTED for every arm, because a delete of nothing and a delete
/// that tombstones produce the same page counts.
///
/// rust-internal: drives five commands, no external surface
#[test]
fn every_per_element_delete_leaves_one_tombstone_and_the_whole_object_delete_leaves_none() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    load_on(&engine, OPERATOR_END);

    // (kind, key, seed, delete, how many elements the seed makes)
    let member = b"member-00".to_vec();
    let seeds: Vec<(&str, &str, Command, Command)> = vec![
        (
            "zset",
            "eo-zset",
            Command::ZSetAdd {
                key: "eo-zset".to_string(),
                member: member.clone(),
                score: 1.5,
            },
            Command::ZSetRemove {
                key: "eo-zset".to_string(),
                member: member.clone(),
            },
        ),
        (
            "set",
            "eo-set",
            Command::SetAdd {
                key: "eo-set".to_string(),
                member: member.clone(),
            },
            Command::SetRemove {
                key: "eo-set".to_string(),
                member: member.clone(),
            },
        ),
        (
            "list",
            "eo-list",
            Command::ListPush {
                key: "eo-list".to_string(),
                member: member.clone(),
                left: false,
            },
            Command::ListPop {
                key: "eo-list".to_string(),
                left: false,
            },
        ),
        (
            "hash",
            "eo-hash",
            Command::HashSet {
                key: "eo-hash".to_string(),
                field: "f0".to_string(),
                value: member.clone(),
            },
            Command::HashDelete {
                key: "eo-hash".to_string(),
                field: "f0".to_string(),
            },
        ),
        (
            "zset",
            "eo-zset-whole",
            Command::ZSetAdd {
                key: "eo-zset-whole".to_string(),
                member: member.clone(),
                score: 1.5,
            },
            Command::CommonDelete {
                key: "eo-zset-whole".to_string(),
            },
        ),
    ];

    let mut arms_checked = 0usize;
    let mut per_element_arms = 0usize;
    let mut whole_object_arms = 0usize;
    for (kind, key, seed, delete) in seeds {
        // WHICH KIND OF DELETE THIS ARM IS, decided from the command rather than from the key's name.
        // Reading it off a hand-written list of which arms are whole-object would go stale the moment
        // a sixth arm was added, and nothing would fail.
        let whole_object = matches!(delete, Command::CommonDelete { .. });
        write(&engine, seed);
        let before = pages_of(&engine, kind, key);
        assert!(
            !before.is_empty(),
            "DENOMINATOR: {kind}/{key} holds no page after its seed, so the delete below would \
             prove nothing"
        );
        assert_eq!(
            0,
            tombstoned_pages_of(&engine, kind, key),
            "DENOMINATOR: {kind}/{key} already carries a tombstone before its delete, so the count \
             after it would not be attributable to the delete"
        );

        write(&engine, delete);
        let after = pages_of(&engine, kind, key);
        let tombstoned = tombstoned_pages_of(&engine, kind, key);

        println!(
            "  {kind:<5} {key:<14} {:<12} live pages {} -> {}, entries carrying deleted=true: {}",
            if whole_object { "whole-object" } else { "per-element" },
            before.len(),
            after.len(),
            tombstoned
        );
        // RESTATED FOR THE COLLAPSED PROJECTION, AS THE INVARIANT AND NOT AS A COUNT. A gated
        // per-element removal does not reduce the LIVE page count: the page it removes from still
        // holds the object's other elements, so the entry stays and a tombstone naming the removed
        // element is added beside it. What must fall is the live count OR -- under the collapse --
        // the tombstone count must rise, and the element must be gone either way.
        //
        // The removal's correctness under that arm is established by
        // `write_after_fold::a_gated_removal_leaves_every_other_member_whole_across_a_reload` and
        // `..::gated_removals_file_one_tombstone_per_distinct_element_and_do_not_accumulate`,
        // rather than by this count agreeing with whatever the code now does.
        // RESTATED FROM `kind == "set"`, WHICH WAS THE WHOLE COLLAPSED SET WHEN THIS WAS WRITTEN.
        //
        // `zset` and `list` are collapsed too now, so a hand-written kind here would leave their
        // per-element arms in the `else` branch below asserting that the LIVE page count FALLS --
        // which is exactly what a gated removal does not do, and the arm would have gone red for
        // the right reason with the wrong explanation. Asking the shared predicate means the kind
        // list lives in ONE place: a kind added to `index_entry_names_a_page` arrives here with
        // it, and a kind held OUT of it (hash, today) keeps the ungated expectation.
        let collapsed = !whole_object
            && crate::engine::storage_bucket_internals::index_entry_names_a_page(kind);
        if collapsed {
            assert!(
                tombstoned > 0,
                "{kind}/{key}: the gated per-element delete left {} live pages of {} and filed \
                 {tombstoned} tombstones, so it recorded the removal nowhere",
                after.len(),
                before.len()
            );
        } else {
            assert!(
                after.len() < before.len(),
                "{kind}/{key}: the delete left {} live pages of {}, so it did not remove",
                after.len(),
                before.len()
            );
        }
        if whole_object {
            whole_object_arms += 1;
            assert_eq!(
                0, tombstoned,
                "{kind}/{key}: a WHOLE-OBJECT delete left {tombstoned} tombstone entry(ies). There \
                 is no membership left to keep true once the object is gone, so an entry retained \
                 here is leaked for the life of the store rather than collected by a rewrite."
            );
        } else {
            per_element_arms += 1;
            assert_eq!(
                1, tombstoned,
                "{kind}/{key}: a PER-ELEMENT delete left {tombstoned} tombstone entry(ies), not one. \
                 Zero means the removal reached the index and NOT the pages, so a membership derived \
                 from the pages would put the element back -- which is the defect #2028 drove and \
                 this whole change exists to close. More than one means the removal filed a tombstone \
                 per bucket rather than per element."
            );
        }
        arms_checked += 1;
    }

    assert_eq!(
        arms_checked, 5,
        "DENOMINATOR: {arms_checked} delete paths were driven, not the five this claim covers"
    );
    // BOTH SIDES OF THE SPLIT ARE NON-EMPTY. Without this, five per-element arms and no whole-object
    // arm would satisfy every assertion above and the control would be untested.
    assert_eq!(
        4, per_element_arms,
        "DENOMINATOR: {per_element_arms} per-element arms were driven, not four"
    );
    assert_eq!(
        1, whole_object_arms,
        "DENOMINATOR: {whole_object_arms} whole-object arms were driven, not one"
    );
    println!(
        "\n  four per-element deletes left one tombstone each; the whole-object delete left none"
    );
}

// =================================================================================================
// 2. THE ORDINAL A DELETE FREES IS HANDED STRAIGHT BACK
// =================================================================================================

/// THE TWO ORDINARY COMMANDS #1990 NAMES, DRIVEN ON THE ELEMENT AXIS INSTEAD OF THE OBJECT AXIS.
///
/// #1990 drove `FeatureAppend`, `FeatureDelete`, `FeatureAppend` -- the whole-object path. The path an
/// element ordinal would live on is `ZSetAdd`, `ZSetRemove`, `ZSetAdd`, and it is the same story: the
/// removal takes the page away, the walk's `max` drops back, and the next element is handed the
/// number the removed one had.
///
/// **THIS IS THE TEST THAT WOULD FAIL IF NON-REUSE WERE ENFORCED**, and it is written the way round
/// that makes that true: it asserts the number IS reused, and names in its message what an
/// enforcement would have to make it say instead. An enforcement that persisted the high-water mark
/// turns the final assertion red, which is the signal wanted.
///
/// The two members are asserted DIFFERENT in the same breath, because a reused number is only
/// corruption when two distinct elements land on it.
///
/// rust-internal: drives three commands, no external surface
#[test]
fn the_element_ordinal_a_delete_frees_is_handed_straight_back_to_the_next_element() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    load_on(&engine, OPERATOR_END);

    let held: Vec<Vec<u8>> = (0..3)
        .map(|element| format!("held-{element:02}").into_bytes())
        .collect();
    for (element, member) in held.iter().enumerate() {
        write(
            &engine,
            Command::ZSetAdd {
                key: "eo-reuse".to_string(),
                member: member.clone(),
                score: element as f64,
            },
        );
    }

    let seeded = pages_of(&engine, "zset", "eo-reuse");
    assert_eq!(
        seeded.len(),
        held.len(),
        "DENOMINATOR: {} pages for {} members seeded",
        seeded.len(),
        held.len()
    );
    let ordinal_the_third_member_took = next_element_ordinal_would_be(&engine, "zset", "eo-reuse") - 1;

    // Remove the member holding the HIGHEST ordinal, which is what makes `max` drop.
    let doomed = held.last().expect("three members were seeded").clone();
    write(
        &engine,
        Command::ZSetRemove {
            key: "eo-reuse".to_string(),
            member: doomed.clone(),
        },
    );
    let after_delete = next_element_ordinal_would_be(&engine, "zset", "eo-reuse");

    // A DIFFERENT member, so a shared number is two elements and not one rewritten.
    let arrival = b"arrival-99".to_vec();
    assert_ne!(
        doomed, arrival,
        "the fixture must use two DISTINCT members or a shared ordinal is not corruption"
    );
    write(
        &engine,
        Command::ZSetAdd {
            key: "eo-reuse".to_string(),
            member: arrival.clone(),
            score: 99.0,
        },
    );
    let ordinal_the_arrival_took = next_element_ordinal_would_be(&engine, "zset", "eo-reuse") - 1;

    println!(
        "\n  [reuse, element axis] seeded {} members; the third took ordinal {}; after removing it \
         the next would be {}; the arrival took ordinal {}",
        held.len(),
        ordinal_the_third_member_took,
        after_delete,
        ordinal_the_arrival_took
    );

    assert_eq!(
        ordinal_the_arrival_took, ordinal_the_third_member_took,
        "the arrival took ordinal {ordinal_the_arrival_took} and the removed member had \
         {ordinal_the_third_member_took}. IF THIS IS RED, non-reuse is now enforced somewhere and \
         this module's refutation should be re-read rather than this assertion relaxed. The helper \
         above counts LIVE pages; a red caused by the tombstone being counted is a helper that lost \
         its `deleted` filter, not a reservation."
    );

    // THE FREED NUMBER IS NOW HELD TWICE, AND EXACTLY ONE OF THE TWO IS LIVE.
    //
    // A NEW FACT, not a relaxation of the one above: the removal keeps an entry pointing at the page
    // that records it, and `container_page_ordinal` filters `deleted`, so the tombstone keeps the
    // number it had while the arrival is handed the same number. That is safe for exactly the reason
    // this module has always given -- an ordinal names a POSITION and identity lives in the component
    // -- and it is asserted here rather than left implicit, because "two entries share ordinal 2" is
    // what corruption would also look like.
    let tombstones = tombstone_ordinals_of(&engine, "zset", "eo-reuse");
    println!(
        "  [reuse, element axis] tombstone entries hold {tombstones:?}; the arrival holds \
         Some({ordinal_the_arrival_took})"
    );
    assert_eq!(
        1,
        tombstones.len(),
        "DENOMINATOR: {} tombstone entries for one removal, so the pairing below is not about one \
         removed element",
        tombstones.len()
    );
    assert_eq!(
        Some(ordinal_the_third_member_took as u64),
        tombstones[0],
        "the tombstone does not hold the ordinal its element had, so the number it kept is not the \
         one the arrival was handed back and this pairing is measuring two unrelated things"
    );
}

// =================================================================================================
// 3. THE ONE TOMBSTONE THIS STORE KEEPS CANNOT REFUSE THE WRITE
// =================================================================================================

/// THE TOMBSTONE SURVIVES THE REWRITE, AND A LIVE PAGE THEN READS AS DELETED.
///
/// **THIS TEST WAS WRITTEN TO ASSERT THE OPPOSITE AND THE TREE REFUSED IT.** The first draft claimed
/// the reservation is cleared by the write that would reuse it, reasoning from
/// `deleted_object_index.remove(&object_id)` sitting four statements before the page insert. That
/// call is real, and it is in `sync_bucket_index_object_blocks_with_mode` -- the WHOLE-OBJECT restate
/// path that `FeatureAppend` uses. It is **not** in `upsert_bucket_index_block_inner`, which is the
/// path every container ELEMENT write takes. So on the element path the tombstone only ever grows.
///
/// Measured: after `ZSetAdd`, `ZSetRemove`, `ZSetAdd` of the SAME member the bucket holds a live page
/// AND a tombstone for that page's own object id.
///
/// # SO THE TOMBSTONE IS NOT REFUTED BY BEING CLEARED. IT IS REFUTED THREE OTHER WAYS
///
///   1. **IT IS A MEMBERSHIP SET, NOT A MARK.** It holds `stable_block_object_id(shard, kind,
///      key)` -- a HASH. It can answer "has ordinal N been used?" and it cannot answer "what is
///      the highest ordinal used?", so an assignment built on it probes 0, 1, 2, ... one hash at a
///      time and cannot report a ceiling without 65,535 probes.
///   2. **BOTH WRITE PATHS CLEAR IT, AND THE WHOLE-OBJECT ONE CLEARS EVERY ELEMENT'S.** A restate
///      through `sync_bucket_index_object_blocks_with_mode` removes the id of every address it
///      republishes, so any reservation an element held is dropped by an unrelated whole-object
///      write; and `upsert_bucket_index_block_inner` now clears the id it is filing a live page
///      for, which is what keeps the report honest but is also a clear a reservation cannot allow.
///   3. **A RESERVATION MUST NEVER BE CLEARED, AND THAT INVERTS ITS OWN MEASURED ECONOMICS.** The
///      field's doc chose its shape on a census -- "97.68% of buckets carry no tombstone, and the
///      widest bucket that carries one carries a single id" -- and states where the trade turns over:
///      "the shape wins while fewer than about a quarter of buckets carry one and loses above that,
///      against a measured 2.32%". A tombstone kept for ever puts every bucket that has ever had an
///      element removed on the losing side, carrying one id per removed element rather than one.
///
/// # AND THE DEFECT THAT CAME WITH IT, NOW FIXED
///
/// `object_manager::runtime_report` computes `page.deleted || bucket.deleted() ||
/// bucket.deleted_object_index.contains(&page.object_id())`. While the element write path left the
/// id behind, a surviving tombstone made a LIVE page's object report `deleted` and counted its page
/// as a deleted block ref rather than a hot one -- reaching the public report as
/// `tombstone_object_count=1` on a store holding one live page. Reachable:
/// `TemporalEngine::object_manager_runtime_report` is public and `recovery_sweep_compact` calls it
/// twice.
///
/// The clear now happens on both write paths, and this test holds the fixed arithmetic: the
/// tombstone is gone after the rewrite, the page reads HOT, and the public aggregate reports zero.
/// The four assertions below were written to hold the defect and are inverted, not relaxed -- each
/// one still names the denominator it reads, so a fixture that writes nothing cannot pass it.
///
/// rust-internal: drives a removal and a re-add, no external surface
#[test]
fn the_element_rewrite_clears_the_tombstone_so_a_live_page_reads_as_hot() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    load_on(&engine, OPERATOR_END);

    let member = b"tombstone-member".to_vec();
    let component = zset_name(4.25, &member);
    let object_id = crate::engine::hashing::stable_block_object_id(
        1,
        "zset",
        "eo-tombstone",
    );

    write(
        &engine,
        Command::ZSetAdd {
            key: "eo-tombstone".to_string(),
            member: member.clone(),
            score: 4.25,
        },
    );
    assert_eq!(
        pages_of(&engine, "zset", "eo-tombstone").len(),
        1,
        "DENOMINATOR: the seed did not file a page, so nothing below is being observed"
    );
    let held_before_delete = tombstone_holds(&engine, object_id);

    write(
        &engine,
        Command::ZSetRemove {
            key: "eo-tombstone".to_string(),
            member: member.clone(),
        },
    );
    let held_after_delete = tombstone_holds(&engine, object_id);

    write(
        &engine,
        Command::ZSetAdd {
            key: "eo-tombstone".to_string(),
            member: member.clone(),
            score: 4.25,
        },
    );
    let held_after_rewrite = tombstone_holds(&engine, object_id);
    let pages_after_rewrite = pages_of(&engine, "zset", "eo-tombstone").len();

    println!(
        "\n  [tombstone] object id {object_id}: held before delete={held_before_delete}, after \
         delete={held_after_delete}, after the next write={held_after_rewrite}; pages after that \
         write={pages_after_rewrite}"
    );

    assert!(
        !held_before_delete,
        "the tombstone already held this id before anything was deleted, so the observation below \
         says nothing about the delete"
    );
    assert!(
        held_after_delete,
        "the delete left no tombstone for {object_id}, so there was never a reservation to clear"
    );
    assert!(
        !held_after_rewrite,
        "the tombstone SURVIVED the next write of the same triple. The element write path clears \
         `deleted_object_index` for the id it is filing a live page for; if this is red that \
         `remove` has been lost from `upsert_bucket_index_block_inner` and the stale tombstone is \
         back."
    );
    assert_eq!(
        pages_after_rewrite, 1,
        "the rewrite must have filed its page, or the stale tombstone below is not sitting beside a \
         LIVE page"
    );

    // The consequence. Read TWICE: the per-object row through the internal walk (the same door
    // `address_footprint` reads it through), and the aggregate through the PUBLIC surface a recovery
    // sweep actually calls. The row carries the claim; the aggregate proves it is not confined to an
    // internal structure nobody serves from.
    let (row_deleted, row_block_refs, row_deleted_refs, row_hot) = {
        let shards = engine.shards.read().expect("engine lock poisoned");
        let shard = shards.get(&1).expect("shard 1 is loaded");
        let internal = crate::engine::object_manager::runtime_report(shard);
        let rows: Vec<_> = internal
            .objects
            .iter()
            .filter(|row| row.object_id == object_id)
            .collect();
        assert_eq!(
            rows.len(),
            1,
            "DENOMINATOR: the walk holds {} rows for object id {object_id}, not the one this \
             observation reads",
            rows.len()
        );
        let row = rows[0];
        (
            row.deleted,
            row.block_ref_count,
            row.deleted_block_ref_count,
            row.hot_block_ref_count,
        )
    };

    let public = engine.object_manager_runtime_report(1);
    println!(
        "  [object manager] object {object_id}: block_ref_count={row_block_refs} \
         deleted={row_deleted} deleted_block_ref_count={row_deleted_refs} hot={row_hot}\n  \
         [object manager] public aggregate: object_count={} block_ref_count={} \
         tombstone_object_count={}",
        public.object_count, public.block_ref_count, public.delete_marker_object_count
    );

    assert_eq!(
        row_block_refs, 1,
        "DENOMINATOR: the walk counts {row_block_refs} block refs for a live page, not one"
    );
    assert!(
        !row_deleted,
        "the walk still calls a LIVE page's object deleted, so a stale tombstone is still sitting \
         beside it"
    );
    assert_eq!(
        row_deleted_refs, 0,
        "the walk counts {row_deleted_refs} of this object's block refs as deleted; the page is \
         live and no tombstone should be reclassifying it"
    );
    assert_eq!(
        row_hot, 1,
        "the live page is counted as HOT now that no tombstone reclassifies it; {row_hot} says \
         the arithmetic landed somewhere else"
    );
    assert!(
        public.block_ref_count >= 1,
        "DENOMINATOR: the public report counts {} block refs, so it is not seeing the store at all",
        public.block_ref_count
    );
    assert_eq!(
        public.delete_marker_object_count, 0,
        "the PUBLIC report counts {} delete-marked objects on a store whose one page is LIVE. \
         This is the surface a recovery sweep reads, and the count is what the fix on the element \
         write path exists to make honest",
        public.delete_marker_object_count
    );
}

// =================================================================================================
// 4. WHERE A HIGH-WATER MARK COULD LIVE, AND WHAT THE RELOAD DOES TO IT
// =================================================================================================

/// THE DURABLE MAP SURVIVES A RELOAD. A MARK IN IT DOES NOT SURVIVE THE REBUILD.
///
/// Two separate claims, and the second is the refutation:
///
///   1. `ShardState::zsets` is serialized, so a per-object high-water mark kept beside the elements
///      WOULD come back from a reload. Driven: seven elements written, engine dropped, engine
///      reopened, seven elements still there.
///   2. But the mark's value is the thing at issue, and the reload's door is
///      `reconcile_secondary_views_from_bucket_index`, which derives the same maps FROM the pages and
///      merges. So after a removal the reloaded mark is a `max` over what the pages still hold.
///      Driven: three elements, remove the highest, the in-memory walk still reads 2 live pages and
///      the reload reads 2 -- the number the removed element had is free again, across the restart.
///
/// **ELEMENT BY ELEMENT, NOT BY COUNT**, because a count can balance while two elements swap.
///
/// rust-internal: drives a reload, no external surface
#[test]
fn a_high_water_mark_lives_only_where_the_rebuild_recomputes_it() {
    let dir = tempfile::tempdir().expect("tempdir");

    let survivors: Vec<Vec<u8>> = (0..7)
        .map(|element| format!("survivor-{element:02}").into_bytes())
        .collect();
    let doomed = b"doomed-highest".to_vec();

    {
        let engine = engine_on(dir.path());
        load_on(&engine, OPERATOR_END);
        for (element, member) in survivors.iter().enumerate() {
            write(
                &engine,
                Command::ZSetAdd {
                    key: "eo-reload".to_string(),
                    member: member.clone(),
                    score: element as f64,
                },
            );
        }
        write(
            &engine,
            Command::ZSetAdd {
                key: "eo-reload".to_string(),
                member: doomed.clone(),
                score: 999.0,
            },
        );

        let seeded_pages = pages_of(&engine, "zset", "eo-reload").len();
        let seeded_durable = durable_element_count(&engine, "zset", "eo-reload");
        assert_eq!(
            seeded_pages,
            survivors.len() + 1,
            "DENOMINATOR: {seeded_pages} pages for {} elements seeded",
            survivors.len() + 1
        );
        assert_eq!(
            seeded_durable,
            survivors.len() + 1,
            "DENOMINATOR: the durable map holds {seeded_durable} of {} seeded elements",
            survivors.len() + 1
        );

        write(
            &engine,
            Command::ZSetRemove {
                key: "eo-reload".to_string(),
                member: doomed.clone(),
            },
        );
        let after_delete = next_element_ordinal_would_be(&engine, "zset", "eo-reload");
        println!(
            "\n  [reload] {} elements seeded, highest removed; in memory the next ordinal would be \
             {after_delete}",
            survivors.len() + 1
        );
        assert_eq!(
            after_delete,
            survivors.len(),
            "in memory the walk should already have dropped back to {}",
            survivors.len()
        );

        engine.unload_shard(1);
    }

    let engine = engine_on(dir.path());
    load_on(&engine, OPERATOR_END);

    // CLAIM 1: the durable map came back.
    let reloaded_durable = durable_element_count(&engine, "zset", "eo-reload");
    assert_eq!(
        reloaded_durable,
        survivors.len(),
        "the durable map came back with {reloaded_durable} of {} elements, so it is not a place a \
         mark could live at all",
        survivors.len()
    );

    // ELEMENT BY ELEMENT: every survivor is back, by identity and by score, and the removed one is
    // not. A count alone would pass if two elements had swapped.
    let mut recovered = 0usize;
    for (element, member) in survivors.iter().enumerate() {
        let answered = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::ZSetScore {
                key: "eo-reload".to_string(),
                member: member.clone(),
            },
        });
        let bytes = match answered.response {
            crate::types::CommandResponse::Bytes { value: Some(bytes) } => bytes,
            other => panic!(
                "survivor {element} ({:?}) did not come back after the reload: {other:?}",
                String::from_utf8_lossy(member)
            ),
        };
        let score: f64 = String::from_utf8_lossy(&bytes)
            .parse()
            .expect("a score parses");
        assert!(
            (score - element as f64).abs() < 1e-9,
            "survivor {element} came back at score {score}, not {element}"
        );
        recovered += 1;
    }
    assert_eq!(
        recovered,
        survivors.len(),
        "DENOMINATOR: {recovered} of {} survivors checked",
        survivors.len()
    );
    let doomed_answer = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::ZSetScore {
            key: "eo-reload".to_string(),
            member: doomed.clone(),
        },
    });
    assert!(
        matches!(
            doomed_answer.response,
            crate::types::CommandResponse::Bytes { value: None }
        ),
        "the removed element came back from the reload: {doomed_answer:?}"
    );

    // CLAIM 2: and the mark the rebuild recomputes has dropped back.
    let reloaded_next = next_element_ordinal_would_be(&engine, "zset", "eo-reload");
    println!(
        "  [reload] after the restart the durable map holds {reloaded_durable} elements and the \
         next ordinal the rebuild would derive is {reloaded_next}; the removed element's number \
         was {}",
        survivors.len()
    );
    assert_eq!(
        reloaded_next,
        survivors.len(),
        "the rebuilt walk answers {reloaded_next}, and the removed element's ordinal was {}. IF \
         THIS IS RED a mark now survives the rebuild, and that is the one thing this refutation \
         says cannot be had.",
        survivors.len()
    );
}

// =================================================================================================
// 5. THE ANSWER IS FOUR ANSWERS, ONE PER KIND
// =================================================================================================

/// WHAT AN ORDINAL WOULD MEAN FOR EACH CONTAINER KIND, ASSERTED IN BOTH DIRECTIONS.
///
/// The shape #1982 used: an enumeration is only a finding if the arms that do NOT need the change are
/// asserted not to need it, in the same test as the ones that do.
///
///   * `list` -- the component IS already an ordinal. Asserted by spelling one and matching it.
///   * `hash` -- the component IS the caller's field name. Asserted by writing a field and finding
///     that exact text on the page.
///   * `zset`, `set` -- the component is the member's CONTENT, and the member is already held in a
///     persisted map keyed by itself. Asserted both ways: the name contains the member's hex, and the
///     durable map holds the member.
///
/// rust-internal: reads page entries, no external surface
#[test]
fn what_an_element_ordinal_would_mean_for_each_kind() {
    // IT ASSERTS THAT NO PAGE CARRIES AN ABSENT ELEMENT NAME -- the absence of exactly what this
    // gate creates. Pinned rather than restated because its subject is the per-element ordinal,
    // and the ordinal under the collapsed projection is `ordinal_under_the_gate`'s subject, which
    // drives both arms.
    let _gate_off = super::GateOff::held();
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    load_on(&engine, OPERATOR_END);

    let member = b"kind-member".to_vec();
    let member_hex = hex::encode(&member);

    write(
        &engine,
        Command::ListPush {
            key: "eo-k-list".to_string(),
            member: member.clone(),
            left: false,
        },
    );
    write(
        &engine,
        Command::HashSet {
            key: "eo-k-hash".to_string(),
            field: "a-callers-own-field".to_string(),
            value: member.clone(),
        },
    );
    write(
        &engine,
        Command::SetAdd {
            key: "eo-k-set".to_string(),
            member: member.clone(),
        },
    );
    write(
        &engine,
        Command::ZSetAdd {
            key: "eo-k-zset".to_string(),
            member: member.clone(),
            score: 2.5,
        },
    );

    let one_component = |kind: &str, key: &str| -> String {
        let held = pages_of(&engine, kind, key);
        assert_eq!(
            held.len(),
            1,
            "DENOMINATOR: {kind}/{key} holds {} pages, not the one this arm reads",
            held.len()
        );
        held[0]
            .0
            .clone()
            .unwrap_or_else(|| panic!("{kind}/{key} holds a page with no component"))
    };

    // list: ALREADY an ordinal. Sixteen hexadecimal characters, and the value is the biased
    // sequence the durable map is keyed by.
    let list_component = one_component("list", "eo-k-list");
    assert_eq!(
        list_component.len(),
        16,
        "a list component is {} characters, not the sixteen an ordinal spelling would need",
        list_component.len()
    );
    assert_eq!(
        list_component,
        format!("{:016x}", (0i64 as u64).wrapping_sub(i64::MIN as u64)),
        "a list component is not the biased sequence this arm claims it is"
    );
    assert!(
        !list_component.contains(&member_hex),
        "a list component holds the member's bytes, so it is not a pure ordinal after all"
    );

    // hash: the CALLER'S text. An ordinal would not respell this, it would destroy it.
    let hash_component = one_component("hash", "eo-k-hash");
    assert_eq!(
        hash_component, "a-callers-own-field",
        "a hash component is not the caller's field name; it reads {hash_component}"
    );

    // set and zset: the member's content, twice over -- once in the name, once in a persisted map.
    let set_component = one_component("set", "eo-k-set");
    assert_eq!(
        set_component, member_hex,
        "a set component is not the member's hex"
    );
    let zset_component = one_component("zset", "eo-k-zset");
    assert!(
        zset_component.ends_with(&member_hex),
        "a zset component does not end in the member's hex: {zset_component}"
    );
    assert_eq!(
        zset_component.len(),
        16 + member_hex.len(),
        "a zset component is {} characters, not the sixteen score characters plus {} member ones",
        zset_component.len(),
        member_hex.len()
    );

    assert_eq!(
        durable_element_count(&engine, "set", "eo-k-set"),
        1,
        "the set's durable map does not hold the member the name re-spells"
    );
    assert_eq!(
        durable_element_count(&engine, "zset", "eo-k-zset"),
        1,
        "the zset's durable map does not hold the member the name re-spells"
    );

    println!(
        "\n  list  {list_component}  ALREADY an ordinal ({} chars)\n  hash  {hash_component}  the \
         CALLER's field name -- an ordinal destroys it\n  set   {set_component}  the member's hex, \
         and the durable map holds the member too\n  zset  {zset_component}  score hex + the \
         member's hex, and the durable map holds both",
        list_component.len()
    );
}

// =================================================================================================
// 6. THE READ PATH, COUNTED AT SOURCE LEVEL
// =================================================================================================

/// THE POINT READ PATH IS THREE CALL SITES AND NOT ONE OF THEM IS A CONTAINER.
///
/// #1985 established this and the mandate for this work said to confirm it rather than inherit it, so
/// it is counted here from the source text.
///
/// **THE INPUT SIZE IS ASSERTED BEFORE ANY COUNT IS BELIEVED.** A zero-byte read scores every "there
/// are exactly N" as a pass; this campaign has already had a gate report a clean scan over an empty
/// diff.
///
/// rust-internal: reads the crate's own source, no external surface
#[test]
fn the_point_read_path_is_three_call_sites_and_not_one_of_them_is_a_container() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/engine");
    let execute = std::fs::read_to_string(root.join("execute_on_shard.rs"))
        .expect("execute_on_shard.rs is readable");
    let store =
        std::fs::read_to_string(root.join("bucket_store.rs")).expect("bucket_store.rs is readable");
    assert!(
        execute.len() > 100_000 && store.len() > 5_000,
        "DENOMINATOR: read {} and {} bytes of source; a short read scores every count below as a \
         pass",
        execute.len(),
        store.len()
    );

    // The point lookup itself has ONE caller in the crate, and it is the read path.
    let definition_and_callers = store.matches("bucket_index_block_address(").count();
    assert_eq!(
        definition_and_callers, 2,
        "bucket_index_block_address appears {definition_and_callers} times in bucket_store.rs \
         (its definition plus its callers), not the two this claim rests on"
    );

    // ... and that one is called three times, all from execute_on_shard.
    let read_sites = execute.matches("read_bucket_index_value(").count();
    assert_eq!(
        read_sites, 3,
        "read_bucket_index_value has {read_sites} call sites in execute_on_shard.rs, not three"
    );

    // Each one's kind argument, in source order. A container kind here would move the whole
    // question.
    let mut kinds: Vec<&str> = Vec::new();
    for (offset, _) in execute.match_indices("read_bucket_index_value(") {
        let window = &execute[offset..(offset + 400).min(execute.len())];
        let kind = ["\"string\"", "\"hash\"", "\"set\"", "\"zset\"", "\"list\""]
            .into_iter()
            .find(|kind| window.contains(kind))
            .unwrap_or_else(|| panic!("no kind literal within 400 bytes of a read call site"));
        kinds.push(kind);
    }
    assert_eq!(
        kinds,
        vec!["\"string\"", "\"hash\"", "\"hash\""],
        "the three point-read kinds are {kinds:?}, not string/hash/hash -- a container kind here \
         would mean the point lookup DOES resolve a content-derived name"
    );

    // The component-blind whole-object reader takes no component and its caller has none to give,
    // so an ordinal answers it exactly as a name does. Three callers: hash twice, set once.
    let blind_sites = execute.matches("bucket_index_component_block_addresses(").count();
    assert_eq!(
        blind_sites, 3,
        "bucket_index_component_block_addresses has {blind_sites} callers in execute_on_shard.rs, \
         not three"
    );
    assert!(
        store.contains("pub(super) fn bucket_index_component_block_addresses(\n    shard: &ShardState,\n    model_id: &str,\n    object_key: &str,\n) -> Vec<(Option<Arc<str>>, ElementEntry)>"),
        "bucket_index_component_block_addresses no longer takes exactly (shard, model_id, \
         object_key) -- if it has gained a component argument, the reason it is NOT the obstacle \
         here has changed"
    );

    println!(
        "\n  [read path] bucket_index_block_address: 1 caller (read_bucket_index_value)\n  \
         read_bucket_index_value: {read_sites} call sites, kinds {kinds:?}\n  \
         bucket_index_component_block_addresses: {blind_sites} callers, component-blind by \
         signature\n  source read: {} + {} bytes",
        execute.len(),
        store.len()
    );
}

// =================================================================================================
// 7. THE MIS-PARSE RATE, MEASURED
// =================================================================================================

/// HOW MANY NAMES THIS STORE ALREADY HOLDS ARE ALSO WELL-FORMED ORDINALS MEANING SOMETHING ELSE.
///
/// #1976 assumed 0% for its spelling and measured 1.935%. #1985 measured 100% for lists. Measured
/// here per kind over 20,000 names, against the ordinal spelling an element ordinal would have to
/// use if it were rendered into the same fixed-width hexadecimal field the list already uses.
///
/// A name COUNTS as a mis-parse when it is well-formed under the ordinal spelling AND the value it
/// yields is not the element it actually names.
///
/// **THE LIST ROW IS 100.000% BY CONSTRUCTION, NOT BY LUCK**, and that is the row that decides the
/// stored shape: a list component and a sixteen-character ordinal are the same sixteen hexadecimal
/// characters, so `"0000000000000000"` is sequence `i64::MIN` and ordinal 0 at once. ALL 20,000 and
/// not 19,999 -- this arm was first written expecting one fixed point, and there is none, because a
/// name equals its own ordinal only where `seq - i64::MIN == seq`.
///
/// rust-internal: spells names, no engine
#[test]
fn the_mis_parse_rate_of_an_ordinal_against_the_names_this_store_already_holds() {
    const NAMES: usize = 20_000;

    // A sixteen-character hexadecimal ordinal, the same field width a list component already is.
    fn parses_as_ordinal(name: &str) -> Option<u64> {
        if name.len() != 16 || !name.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            return None;
        }
        u64::from_str_radix(name, 16).ok()
    }

    let mut rows: Vec<(&str, usize, usize)> = Vec::new();

    // list: the component IS sixteen hexadecimal characters.
    let mut list_total = 0usize;
    let mut list_hits = 0usize;
    for element in 0..NAMES {
        let seq = element as i64 - (NAMES as i64 / 2);
        let name = format!("{:016x}", (seq as u64).wrapping_sub(i64::MIN as u64));
        list_total += 1;
        if let Some(ordinal) = parses_as_ordinal(&name) {
            // Well-formed. It is a MIS-parse when the ordinal's value is not the sequence the name
            // actually means -- which is EVERY one of them. A name would equal its own ordinal only
            // where `seq - i64::MIN == seq`, and no value satisfies that, so there is no fixed
            // point. A first draft of this arm expected one and the measurement refused it.
            if ordinal != seq as u64 {
                list_hits += 1;
            }
        }
    }
    rows.push(("list", list_total, list_hits));

    // zset: sixteen score characters followed by the member's hex. Well-formed as an ordinal only
    // when the member is empty, so the length is the discriminator.
    let mut zset_total = 0usize;
    let mut zset_hits = 0usize;
    for element in 0..NAMES {
        let member = if element % 1_000 == 0 {
            Vec::new()
        } else {
            format!("member-{element}").into_bytes()
        };
        let name = zset_name(element as f64, &member);
        zset_total += 1;
        if parses_as_ordinal(&name).is_some() {
            zset_hits += 1;
        }
    }
    rows.push(("zset", zset_total, zset_hits));

    // set: the member's hex and nothing else. Well-formed as an ordinal for every EIGHT-byte
    // member, which is an ordinary member length.
    let mut set_total = 0usize;
    let mut set_hits = 0usize;
    for element in 0..NAMES {
        let member: Vec<u8> = if element % 4 == 0 {
            format!("{element:08}").into_bytes() // exactly eight bytes
        } else {
            format!("member-{element}").into_bytes()
        };
        let name = hex::encode(&member);
        set_total += 1;
        if parses_as_ordinal(&name).is_some() {
            set_hits += 1;
        }
    }
    rows.push(("set", set_total, set_hits));

    println!("\n  [mis-parse of a 16-character hexadecimal ordinal against existing names]");
    for (kind, total, hits) in &rows {
        assert!(
            *total == NAMES,
            "DENOMINATOR: {kind} produced {total} names, not {NAMES}"
        );
        println!(
            "    {kind:<5} {hits:>6} of {total} names are well-formed ordinals of a DIFFERENT \
             meaning  ({:>7.3}%)",
            *hits as f64 * 100.0 / *total as f64
        );
    }

    assert_eq!(
        list_hits, NAMES,
        "EVERY list name should mis-parse. There is no fixed point: a name equals its own ordinal \
         only where `seq - i64::MIN == seq`, which no value satisfies. {list_hits} of {NAMES} did."
    );
    assert!(
        zset_hits > 0,
        "the zset row measured ZERO mis-parses over {NAMES} names, which is the assumption #1976 \
         made and had to retract -- an empty member spells a name that is exactly sixteen \
         characters"
    );
    assert!(
        set_hits > 0,
        "the set row measured ZERO mis-parses over {NAMES} names; an eight-byte member spells \
         sixteen hexadecimal characters and is an ordinary member length"
    );
}

// =================================================================================================
// 8. THE WIDTH AND THE CEILING
// =================================================================================================

/// THE WIDTH AN ELEMENT ORDINAL WOULD TAKE, READ FROM THE FIELDS THEMSELVES, AND THE CEILING.
///
/// Widths come from `field_width` over a real value (#1986's rule: a hand-written table with
/// `size_of` of the wrong type still compiles) and from `size_of` of the field's own declared type.
///
/// THE CEILING IS NOT FREE TO CHOOSE. #1994 narrowed `BlockAddress::block_id` to sixteen bits, so an
/// element ordinal folded into the existing per-object ordinal caps at 65,535 an object -- which is
/// exactly the ceiling of the design that prompted this work, against a measured container shape of
/// p50 100 and one observed case at 2,000.
///
/// AND THE FAILURE AT IT IS NOT LOUD TODAY. `next_block_index_for_object` still ends
/// `unwrap_or(u32::MAX).saturating_add(1)` on this revision, which hands `u32::MAX` out for ever
/// rather than failing. That is #1985's to fix and is asserted here only as the state of the tree, so
/// that this module does not read as though the ceiling were already safe.
///
/// rust-internal: widths and arithmetic, no engine
#[test]
fn the_width_an_element_ordinal_would_take_and_the_ceiling_that_implies() {
    fn field_width<T>(_field: &T) -> usize {
        std::mem::size_of::<T>()
    }

    let name: Option<std::sync::Arc<str>> = Some(std::sync::Arc::from("a-component-name"));
    let component_width = field_width(&name);
    let u16_ordinal = field_width(&Some(0u16));
    let u32_ordinal = field_width(&Some(0u32));

    assert_eq!(
        component_width, 16,
        "the component field reads {component_width} bytes, not the sixteen this whole proposal is \
         about"
    );

    // What the entry would become. The entry is 56 bytes holding 56 of field -- it was 52 before
    // it absorbed the row's two locating fields and shed the flag nothing maintained, so there is
    // NO SLACK left to absorb a new field. The arithmetic is
    // the claim and the rounding is where it lands.
    //
    // 56 IN 56, NOT 60 IN 64. The object id left the address this entry holds inline, taking a
    // whole word out of both numbers. `state.rs` carries the same accounting on `BlockIndex`
    // itself, and the assertion below ties this hand-written sum to the compiler's width so the two
    // cannot drift apart silently -- which is exactly what happened to the 60: it went on
    // describing a structure the engine no longer had, and printed "entry now 56 B holding 60 of
    // field", a field sum LARGER than the type, without anything failing until the width assert
    // below was reached.
    let entry_now = std::mem::size_of::<crate::engine::state::BlockIndex>();
    // THE SUM THE PROSE ABOVE NAMES, and it moved: 52 became 56 when the entry absorbed the
    // row's two locating fields and shed the flag nothing maintained. It was left at 52 while the
    // comment was rewritten to say 56, and NOTHING FAILED -- because 56 - 52 is 4, which is
    // exactly what the slack assertion below was written to expect. A stale sum and a stale slack
    // agreed with each other, so the pair went green while describing a field set the entry no
    // longer has. Neither number is derived, which is why only reading them together caught it.
    let field_bytes_now = 56usize;
    let field_bytes_with_u16 = field_bytes_now - component_width + u16_ordinal;
    let field_bytes_with_u32 = field_bytes_now - component_width + u32_ordinal;

    println!(
        "\n  [width] component Option<Arc<str>> = {component_width} B; Option<u16> = {u16_ordinal} \
         B; Option<u32> = {u32_ordinal} B\n  entry now {entry_now} B holding {field_bytes_now} of \
         field; with a u16 ordinal {field_bytes_with_u16} of field, with a u32 \
         {field_bytes_with_u32}"
    );

    assert_eq!(
        entry_now, 56,
        "the page entry is {entry_now} bytes, not the 56 this arithmetic is written against; \
         re-derive the field sum before trusting the two numbers above"
    );
    // AND THE OPERATOR IS TWO-SIDED NOW, which is the half of this guard that was missing. It
    // read `field_bytes_now <= entry_now`, and it exists because a stale 60 once printed beside a
    // 56-byte entry -- an OVER-count, which `<=` does catch. An UNDER-count it admits: 52 <= 56
    // passes. So ONE stale number failed here and a MATCHED PAIR of them did not, because a field
    // sum of 52 and a slack of 4 are consistent with each other. That is the same shape as any
    // one-sided comparison: it is satisfied on exactly the side the change moves.
    //
    // The entry has no slack left, so the hand-written sum and the compiler's width are the SAME
    // NUMBER, and equality is the whole claim. It cannot be met from one side; it states the zero
    // slack directly rather than as a difference that two stale numbers can agree on; and it
    // removes a subtraction that would have underflowed a `usize` if the sum ever did exceed the
    // type -- the very case the sentence above says this guard is for.
    assert_eq!(
        field_bytes_now, entry_now,
        "the hand-written field sum is {field_bytes_now} B against a {entry_now} B entry. This \
         type has no slack left, so those must be equal -- re-derive the sum rather than adjusting \
         a difference to match it"
    );
    assert!(
        field_bytes_with_u16 < field_bytes_now,
        "a u16 ordinal does not narrow the field sum at all: {field_bytes_with_u16} against \
         {field_bytes_now}"
    );

    // The ceiling, stated. Sixteen bits is the existing ordinal's width after #1994.
    const BLOCK_ID_BITS: u32 = 16;
    let ceiling = (1u64 << BLOCK_ID_BITS) - 1;
    const MEASURED_P50_ELEMENTS: u64 = 100;
    const MEASURED_WIDEST_ELEMENTS: u64 = 2_000;

    println!(
        "  [ceiling] a {BLOCK_ID_BITS}-bit ordinal caps at {ceiling} elements an object; measured \
         container shape p50 {MEASURED_P50_ELEMENTS}, widest observed {MEASURED_WIDEST_ELEMENTS} \
         -- {:.1}x headroom on the widest, not 65,000x",
        ceiling as f64 / MEASURED_WIDEST_ELEMENTS as f64
    );
    assert!(
        ceiling > MEASURED_WIDEST_ELEMENTS,
        "a {BLOCK_ID_BITS}-bit ordinal caps at {ceiling}, below the widest measured object at \
         {MEASURED_WIDEST_ELEMENTS}"
    );

    // And the state of the tree at that ceiling, so this module does not imply it is safe.
    let state = std::fs::read_to_string(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/engine/state.rs"),
    )
    .expect("state.rs is readable");
    assert!(
        state.len() > 100_000,
        "DENOMINATOR: read {} bytes of state.rs",
        state.len()
    );
    assert!(
        state.contains("u32::try_from(highest).unwrap_or(u32::MAX).saturating_add(1)"),
        "next_block_index_for_object no longer saturates at its ceiling. That is #1985's fix \
         landing, and this arm should be retired rather than relaxed -- but until it does, an \
         ordinal at the ceiling is handed out repeatedly rather than refused."
    );
    println!(
        "  [ceiling] and on this revision the assignment still ends \
         `unwrap_or(u32::MAX).saturating_add(1)`, so the failure at the ceiling is silent reuse \
         rather than a refusal (#1985's to fix)"
    );
}

// =================================================================================================
// 9. THE PRIZE, MEASURED ON ONE INSTRUMENT
// =================================================================================================

/// Every page of this shard, as `(kind, key, component length)`.
fn component_census(engine: &TemporalEngine) -> Vec<(String, String, Option<usize>)> {
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard 1 is loaded");
    let mut rows = Vec::new();
    for bucket in shard.bucket_index.bucket_map.values() {
        for (_, page) in bucket.block_index.iter() {
            rows.push((
                page.model_id.as_str().to_string(),
                page.object_key.to_string(),
                page.component.as_deref().map(str::len),
            ));
        }
    }
    rows
}

fn seed_containers(engine: &TemporalEngine, keys: usize, members: usize) {
    let mut commands = Vec::with_capacity(keys * members);
    for k in 0..keys {
        for m in 0..members {
            match k % 3 {
                0 => commands.push(Command::SetAdd {
                    key: format!("c-set-{k}"),
                    member: format!("member-{m}").into_bytes(),
                }),
                1 => commands.push(Command::ZSetAdd {
                    key: format!("c-zset-{k}"),
                    member: format!("member-{m}").into_bytes(),
                    score: m as f64,
                }),
                _ => commands.push(Command::ListPush {
                    key: format!("c-list-{k}"),
                    member: format!("member-{m}").into_bytes(),
                    left: false,
                }),
            }
        }
    }
    for chunk in commands.chunks(1_000) {
        let response = engine.batch_execute(crate::types::BatchExecuteRequest {
            shard_id: 1,
            commands: chunk.to_vec(),
        });
        assert!(response.status.ok, "the seed must ack: {:?}", response.status);
    }
}

fn seed_routed_strings(engine: &TemporalEngine, keys: usize) {
    let commands: Vec<Command> = (0..keys)
        .map(|k| Command::StringSet {
            key: format!("s-{k}"),
            value: vec![b'v'; 32],
        })
        .collect();
    for chunk in commands.chunks(1_000) {
        let response = engine.batch_execute(crate::types::BatchExecuteRequest {
            shard_id: 1,
            commands: chunk.to_vec(),
        });
        assert!(response.status.ok, "the seed must ack: {:?}", response.status);
    }
}

/// WHAT AN ELEMENT ORDINAL WOULD HAVE SAVED A PAGE, AND WHAT THE ASSIGNMENT WOULD HAVE COST.
///
/// Recorded so the refutation carries the number it is declining rather than waving at it.
///
/// ONE INSTRUMENT, BOTH COLUMNS. `ALLOC_BYTES` charges `layout.size()` -- what the caller asked for
/// -- and `ALLOC_CHUNK_BYTES` reads `malloc_usable_size`, what was handed over. The component name is
/// one `Arc<str>` a page, so the span measured is exactly that: every name this store holds,
/// re-allocated as the write path allocated it, and nothing else inside the probe.
///
/// **THE CHUNK RULE IS A FLOOR, NOT AN EQUALITY** (#1969: a 104-byte request read 128), so the chunk
/// column is REPORTED and the request column carries the claim.
///
/// **TWO POPULATIONS, NEVER A MEAN.** #1986 found the mixed histogram holds only 1 and 100 and
/// nothing at two, so a single average over both is a number no object has. The routed population
/// (strings, p50 1 page an object, no component at all) is measured as the control that must save
/// ZERO -- a change to a field that is `None` on every page of it cannot pay there, and a table that
/// showed it paying would be measuring something else.
///
/// Both routing ranges, two corpus sizes, store path length held and asserted.
#[cfg(feature = "alloc-probe")]
#[test]
#[ignore = "counting allocator; run by name under --features alloc-probe"]
fn what_an_element_ordinal_would_have_saved_a_page() {
    use crate::alloc_probe::Probe;

    let component_field_bytes = std::mem::size_of::<Option<std::sync::Arc<str>>>();
    let ordinal_field_bytes = std::mem::size_of::<Option<u16>>();
    let mut path_lengths: Vec<usize> = Vec::new();
    let mut rows_reported = 0usize;

    println!("\n  [the prize an element ordinal declines]");
    for (range_label, end) in [
        ("0..=1023 (shipped)", OPERATOR_END),
        ("0..u32::MAX (legacy)", WHOLE_KEYSPACE_END),
    ] {
        for (size_label, keys) in [("4,000 records", 40usize), ("40,000 records", 400usize)] {
            // --- the container population -------------------------------------------------
            let dir = tempfile::tempdir().expect("tempdir");
            path_lengths.push(dir.path().as_os_str().len());
            let engine = engine_on(dir.path());
            load_on(&engine, end);
            seed_containers(&engine, keys, 100);

            let census = component_census(&engine);
            let named: Vec<usize> = census.iter().filter_map(|row| row.2).collect();
            assert!(
                !named.is_empty(),
                "DENOMINATOR: {range_label} / {size_label} produced no page carrying a component"
            );
            assert_eq!(
                named.len(),
                census.len(),
                "{range_label} / {size_label}: {} of {} container pages carry a component; a page \
                 without one is not part of this claim",
                named.len(),
                census.len()
            );

            // The span is exactly the names: one Arc<str> a page, as the write path makes them.
            let spellings: Vec<String> = named.iter().map(|len| "x".repeat(*len)).collect();
            let probe = Probe::start();
            let held: Vec<std::sync::Arc<str>> = spellings
                .iter()
                .map(|spelling| std::sync::Arc::from(spelling.as_str()))
                .collect();
            let counts = probe.stop();
            std::hint::black_box(&held);

            assert!(
                counts.allocs >= named.len() as u64,
                "the probe counted {} allocations for {} names, so it is not counting the span \
                 described",
                counts.allocs,
                named.len()
            );
            assert!(
                counts.chunk_bytes >= counts.alloc_bytes,
                "the chunk column ({}) read below the request column ({}), which the floor rule \
                 forbids",
                counts.chunk_bytes,
                counts.alloc_bytes
            );

            let pages = named.len() as f64;
            let mean_name = named.iter().sum::<usize>() as f64 / pages;
            println!(
                "    containers  {range_label:<20} {size_label:<14} pages={:<6} name len mean \
                 {mean_name:>5.1}\n                  ALLOC_BYTES {:>8.2} B a name   \
                 ALLOC_CHUNK_BYTES {:>8.2} B a name   in-struct {} -> {} B",
                named.len(),
                counts.alloc_bytes as f64 / pages,
                counts.chunk_bytes as f64 / pages,
                component_field_bytes,
                ordinal_field_bytes
            );
            rows_reported += 1;

            // --- the routed population, as the control that must save nothing ---------------
            let routed_dir = tempfile::tempdir().expect("tempdir");
            path_lengths.push(routed_dir.path().as_os_str().len());
            let routed = engine_on(routed_dir.path());
            load_on(&routed, end);
            seed_routed_strings(&routed, keys * 100);
            let routed_census = component_census(&routed);
            assert!(
                !routed_census.is_empty(),
                "DENOMINATOR: the routed control seeded no page at all"
            );
            let routed_named = routed_census.iter().filter(|row| row.2.is_some()).count();
            println!(
                "    routed      {range_label:<20} {size_label:<14} pages={:<6} pages carrying a \
                 component={routed_named}  -> 0.00 B a page to save",
                routed_census.len()
            );
            assert_eq!(
                routed_named, 0,
                "{routed_named} of {} routed pages carry a component; the control's whole point is \
                 that this population has none, so an ordinal cannot pay on it",
                routed_census.len()
            );
            rows_reported += 1;
        }
    }

    assert_eq!(
        rows_reported, 8,
        "DENOMINATOR: {rows_reported} rows reported, not the eight (2 ranges x 2 sizes x 2 \
         populations) this table claims"
    );
    let first = path_lengths[0];
    assert!(
        path_lengths.iter().all(|length| *length == first),
        "the store path length moved across arms ({path_lengths:?}); allocation BYTES move with it \
         at about six bytes a character, so a byte table across arms of different path lengths is \
         not comparable"
    );
    println!(
        "    store path held at {first} characters across all {} arms",
        path_lengths.len()
    );
}

// =================================================================================================
// 10. THE CEILING'S FAILURE, DRIVEN
// =================================================================================================

/// AT THE CEILING THE ADDRESS ALREADY REFUSES, LOUDLY, AND NAMES THE EXACT CORRUPTION.
///
/// Item three of the question this module answers is "state the width, the ceiling it implies, and
/// fail loudly at it -- never wrap, never saturate". Two thirds of that is already in the tree and
/// the last third is not, and the split is worth driving rather than describing:
///
///   * `BlockAddress::block_id` is STORED as `u16` -- 65,535 elements an object -- while its
///     ACCESSOR hands back `Option<u64>`. A width read off the accessor is four times too generous,
///     which is why #1986's rule is to read it off the field.
///   * `narrow_block_id` is CHECKED, not saturating, and its panic message names the harm:
///     "truncating it would name a different page of the same object". Driven here.
///   * But `next_block_index_for_object` SATURATES before it ever reaches that check, so the value
///     that arrives at the ceiling is `u32::MAX` rather than 65,536 -- still refused, and refused for
///     the wrong reason, by the wrong function, with no mention of the object that overflowed. That
///     half is #1985's.
///
/// rust-internal: drives a panic on an address, no engine
#[test]
fn at_the_ceiling_the_address_refuses_rather_than_truncating() {
    // The stored width is sixteen bits whatever the accessor's type says.
    const CEILING: u64 = u16::MAX as u64;

    let mut fits = crate::block_store::ElementEntry::try_from_parts(1, 0, 16, Some(CEILING), None)
        .expect("an address at the ceiling is constructible");
    assert_eq!(
        fits.block_id(),
        Some(CEILING),
        "an ordinal AT the ceiling must survive the round trip, or the refusal below is refusing \
         the wrong thing"
    );

    // One past it must PANIC, not truncate. A truncating narrow would make ordinal 65,536 and
    // ordinal 0 the same page of the same object -- which is precisely the reuse this module is
    // about, arrived at by arithmetic instead of by a delete.
    let refused = std::panic::catch_unwind(move || {
        fits.set_block_id(Some(CEILING + 1));
        fits.block_id()
    });
    assert!(
        refused.is_err(),
        "an ordinal one past the {CEILING} ceiling was ACCEPTED and came back as {:?}; a silent \
         truncation here is two elements sharing one identity",
        refused.ok()
    );

    println!(
        "\n  [ceiling driven] block_id stored as u16 -> ceiling {CEILING}; accessor type is \
         Option<u64>, which is 4x too generous to read a width from\n  [ceiling driven] \
         set_block_id({}) panicked rather than truncating",
        CEILING + 1
    );
}

// =================================================================================================
// 11. IS THE MEMBER STORED TWICE, AT DOUBLE WIDTH? MEASURED PER KIND AND PER MEMBER WIDTH
// =================================================================================================

/// THE COMPONENT IS A SECOND COPY OF THE MEMBER AT TWO CHARACTERS A BYTE, AND IT IS MEASURED HERE
/// RATHER THAN DERIVED FROM THE FORMAT STRING.
///
/// The arithmetic off the write arms says a member of n bytes costs n in the page payload and 2n again
/// in the component, because both `set` and `zset` spell the member with `hex::encode`. That is a
/// prediction, and predictions about this structure have been wrong four times in this campaign, so
/// every number below comes off a seeded store and the allocator.
///
/// # WHAT IS MEASURED
///
/// For each kind and each member width: the component lengths actually found on the page entries (a
/// HISTOGRAM, never a mean -- a mean over a bimodal population is a number no element has), and the
/// request and chunk bytes for exactly those names, allocated as the write path allocates them.
///
/// # THE CONTROLS, AND WHY THERE ARE TWO
///
///   * `list` -- its component is SIXTEEN characters whatever the member is, so the doubling
///     prediction says it must be FLAT across every width. A kind that moved here would mean the
///     measurement is picking up something other than the member's spelling.
///   * `string` -- no component at all, so it must read ZERO at every width.
///
/// **`ALLOC_CHUNK_BYTES` IS A FLOOR, NOT AN EQUALITY** (#1969: a 104-byte request read 128), so the
/// chunk column is reported and the request column carries the claim.
///
/// Store path length held constant and asserted: allocation BYTES move with it at about six bytes a
/// character.
#[cfg(feature = "alloc-probe")]
#[test]
#[ignore = "counting allocator; run by name under --features alloc-probe"]
fn what_a_component_costs_an_element_at_five_member_widths() {
    use crate::alloc_probe::Probe;

    const WIDTHS: [usize; 5] = [8, 16, 32, 64, 256];
    const MEMBERS: usize = 100;

    let mut path_lengths: Vec<usize> = Vec::new();
    let mut rows = 0usize;
    // (kind, width) -> request bytes a name, so the control's flatness can be asserted afterwards.
    let mut request_per_name: std::collections::BTreeMap<(&str, usize), f64> =
        std::collections::BTreeMap::new();

    println!(
        "\n  [what a component costs an element]  member payload n, component length, and the \
         allocation behind it"
    );
    println!(
        "    {:<7} {:>5} {:>9} {:>11} {:>12} {:>12} {:>8}",
        "kind", "n", "pages", "comp chars", "REQUEST B", "CHUNK B", "chars/n"
    );

    for kind in ["set", "zset", "list", "string"] {
        for width in WIDTHS {
            let dir = tempfile::tempdir().expect("tempdir");
            path_lengths.push(dir.path().as_os_str().len());
            let engine = engine_on(dir.path());
            load_on(&engine, OPERATOR_END);

            let key = format!("w-{kind}-{width}");
            let mut commands = Vec::with_capacity(MEMBERS);
            for element in 0..MEMBERS {
                // A member of EXACTLY `width` bytes, distinct per element.
                let mut member = format!("{element:04}").into_bytes();
                member.resize(width, b'm');
                match kind {
                    "set" => commands.push(Command::SetAdd {
                        key: key.clone(),
                        member,
                    }),
                    "zset" => commands.push(Command::ZSetAdd {
                        key: key.clone(),
                        member,
                        score: element as f64,
                    }),
                    "list" => commands.push(Command::ListPush {
                        key: key.clone(),
                        member,
                        left: false,
                    }),
                    // The second control: one page per key, no component at all.
                    _ => commands.push(Command::StringSet {
                        key: format!("{key}-{element}"),
                        value: member,
                    }),
                }
            }
            for chunk in commands.chunks(1_000) {
                let response = engine.batch_execute(crate::types::BatchExecuteRequest {
                    shard_id: 1,
                    commands: chunk.to_vec(),
                });
                assert!(response.status.ok, "the seed must ack: {:?}", response.status);
            }

            // The component lengths the store ACTUALLY holds, as a histogram.
            let census = component_census(&engine);
            let held: Vec<&(String, String, Option<usize>)> =
                census.iter().filter(|row| row.0 == kind).collect();
            assert_eq!(
                held.len(),
                MEMBERS,
                "DENOMINATOR: {kind}/n={width} filed {} pages of this kind, not the {MEMBERS} \
                 seeded -- the fixture does not reach the population this row claims",
                held.len()
            );
            let mut histogram: std::collections::BTreeMap<Option<usize>, usize> =
                std::collections::BTreeMap::new();
            for row in &held {
                *histogram.entry(row.2).or_default() += 1;
            }

            let lengths: Vec<usize> = held.iter().filter_map(|row| row.2).collect();
            let (request, chunk) = if lengths.is_empty() {
                (0.0, 0.0)
            } else {
                let spellings: Vec<String> =
                    lengths.iter().map(|length| "x".repeat(*length)).collect();
                let probe = Probe::start();
                let names: Vec<std::sync::Arc<str>> = spellings
                    .iter()
                    .map(|spelling| std::sync::Arc::from(spelling.as_str()))
                    .collect();
                let counts = probe.stop();
                std::hint::black_box(&names);
                assert!(
                    counts.chunk_bytes >= counts.alloc_bytes,
                    "{kind}/n={width}: the chunk column read below the request column, which the \
                     floor rule forbids"
                );
                (
                    counts.alloc_bytes as f64 / MEMBERS as f64,
                    counts.chunk_bytes as f64 / MEMBERS as f64,
                )
            };

            let chars: String = histogram
                .keys()
                .map(|length| match length {
                    Some(length) => length.to_string(),
                    None => "none".to_string(),
                })
                .collect::<Vec<_>>()
                .join(",");
            let per_byte = match histogram.keys().next().and_then(|length| *length) {
                Some(length) => format!("{:.2}", length as f64 / width as f64),
                None => "-".to_string(),
            };
            println!(
                "    {kind:<7} {width:>5} {:>9} {chars:>11} {request:>12.2} {chunk:>12.2} \
                 {per_byte:>8}   histogram {:?}",
                held.len(),
                histogram
            );
            request_per_name.insert((kind, width), request);
            rows += 1;
        }
    }

    assert_eq!(
        rows, 20,
        "DENOMINATOR: {rows} rows measured, not the twenty (4 kinds x 5 widths) this table claims"
    );
    let first = path_lengths[0];
    assert!(
        path_lengths.iter().all(|length| *length == first),
        "the store path length moved across arms ({path_lengths:?}); allocation bytes move with it \
         at about six bytes a character"
    );
    println!("    store path held at {first} characters across all {rows} arms");

    // ---- THE CLAIM: set and zset grow with the member, at two characters a byte ----------------
    let set_8 = request_per_name[&("set", 8)];
    let set_256 = request_per_name[&("set", 256)];
    assert!(
        set_256 > set_8 * 8.0,
        "a set component at n=256 costs {set_256:.2} B a name against {set_8:.2} at n=8; if the \
         cost does not grow with the member then the member is NOT being spelled into the name and \
         the whole doubling claim is wrong"
    );
    let zset_8 = request_per_name[&("zset", 8)];
    let zset_256 = request_per_name[&("zset", 256)];
    assert!(
        zset_256 > zset_8 * 4.0,
        "a zset component at n=256 costs {zset_256:.2} B a name against {zset_8:.2} at n=8"
    );

    // ---- CONTROL 1: list is FLAT, because its component is fixed width ------------------------
    let list_8 = request_per_name[&("list", 8)];
    let list_256 = request_per_name[&("list", 256)];
    assert_eq!(
        list_8, list_256,
        "the list control moved from {list_8:.2} to {list_256:.2} B a name across a 32x change in \
         member width; its component is sixteen characters whatever the member is, so a change here \
         means this measurement is picking up something other than the member's spelling"
    );

    // ---- CONTROL 2: string has no component, so zero at every width ---------------------------
    for width in WIDTHS {
        assert_eq!(
            request_per_name[&("string", width)], 0.0,
            "the string control charged {:.2} B a name at n={width}; a kind with no component \
             cannot charge anything for one",
            request_per_name[&("string", width)]
        );
    }

    println!(
        "\n    CLAIM     set  {set_8:.2} -> {set_256:.2} B a name over n=8 -> 256  (grows with the \
         member)\n    CLAIM     zset {zset_8:.2} -> {zset_256:.2} B a name\n    CONTROL   list \
         {list_8:.2} -> {list_256:.2} B a name  (FLAT: fixed-width component)\n    CONTROL   \
         string 0.00 B a name at every width  (no component at all)"
    );
}

// =================================================================================================
// 12. WHO CONSUMES COMPONENT ORDER
// =================================================================================================

/// THE ORDERING PROPERTY IS CLAIMED BY THREE COMMENTS AND CONSUMED BY NOBODY.
///
/// A zset component is score-bits-then-member and a list component is a `i64::MIN`-biased sequence,
/// both so that LEXICAL order is the element's order. `execute_on_shard.rs` says the bias is "what
/// lets recovery and range reads walk the bucket index directly", and `state.rs` says "the index
/// component already encodes score-then-member, so recovery has the order for free". An ordinal that
/// did not preserve that ordering would force those readers to sort -- which would be the refutation,
/// and it is the reason this arm exists.
///
/// **IT IS NOT THE REFUTATION, BECAUSE NO READER TAKES ITS ORDER FROM THE COMPONENT.** Enumerated,
/// with each consumer classified rather than counted:
///
///   * **RECOVERY RE-KEYS BY THE DECODED VALUE.** The three arms of
///     `reconcile_secondary_views_from_bucket_index` decode each component and insert into a
///     `BTreeMap` keyed by the MEMBER BYTES (`set`, `zset`) or by the `i64` SEQUENCE (`list`). The
///     order is re-established by the map; nothing is inherited from the walk. A shuffled walk
///     produces the identical map, and that is asserted below by driving a reload and checking the
///     order came back.
///   * **RANGE READS USE THE DURABLE MAPS.** `zset_members_in_score_range` and `zset_ordered_members`
///     read `shard.zsets`, which is keyed by MEMBER and not by score -- `state.rs` says so directly:
///     "is derived per query -- V1 accepts the per-range sort". `ListRange` reads `shard.lists`, keyed
///     by the `i64` itself. Neither touches a component.
///   * **THE THREE READERS THAT DO SORT BY COMPONENT NEED ONLY A TOTAL ORDER.**
///     `bucket_index_component_block_addresses` sorts before returning;
///     `ObjectBlockRefs::position` binary-searches; `storage_reporting` sorts for a stable report.
///     Any total order satisfies all three, and an ordinal has one.
///   * **AND OF ITS THREE CALLERS, ONE DISCARDS THE COMPONENT AND ONE READS ONLY `len()`.**
///     `SetMembers` takes `|(_, address)|`; `HashLen` takes `.len()`; `HashGetAll` keeps the field
///     name, which is the caller's own text and is not a kind an ordinal touches.
///
/// So the ordering property costs sixteen characters of score prefix on every zset element and buys
/// nothing any reader on this revision consumes. **That is a finding in favour of the ordinal on this
/// axis**, recorded because the refutation is about non-reuse and should not be allowed to collect
/// support it has not earned.
///
/// rust-internal: reads the crate's own source and drives one reload
#[test]
fn the_component_ordering_property_is_consumed_by_no_reader() {
    // THE WALK THIS COMPARES AGAINST IS BUILT BY DECODING EACH ENTRY'S COMPONENT, through an
    // `unwrap_or_default()` that turns an absent name into the EMPTY member -- the same
    // defaulting the production readers were fixed to stop doing. Gated it manufactures a
    // phantom empty member in the expectation while the LISTING answers correctly, so the
    // instrument fails, not the read. The gated listing's order is
    // `gated_listing_folds_the_pages`' subject.
    let _gate_off = super::GateOff::held();
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/engine");
    let internals = std::fs::read_to_string(root.join("storage_bucket_internals.rs"))
        .expect("storage_bucket_internals.rs is readable");
    let execute = std::fs::read_to_string(root.join("execute_on_shard.rs"))
        .expect("execute_on_shard.rs is readable");
    assert!(
        internals.len() > 100_000 && execute.len() > 100_000,
        "DENOMINATOR: read {} and {} bytes; a short read scores every claim below as a pass",
        internals.len(),
        execute.len()
    );

    // The three comments that make the claim are still there, so this arm is answering a live claim
    // and not a remembered one.
    let claims = [
        "Two's-complement bias makes the hex component sort lexically in list order",
        "The persisted component: score bits then member, so lexical order is (score, member) order",
    ];
    for claim in claims {
        assert!(
            execute.contains(claim),
            "the ordering claim {claim:?} is no longer in the tree, so this arm is answering \
             nothing -- re-read the write arms before trusting it"
        );
    }

    // RECOVERY re-keys by the decoded value. The `set` arm inserts the decoded MEMBER; the `list`
    // arm inserts the decoded i64 SEQUENCE. Neither is the component.
    assert!(
        internals.contains(".insert(member, entry.address);"),
        "the set reconcile arm no longer keys by the decoded member; if it has started relying on \
         the walk's order, component order IS load-bearing and this whole arm is wrong"
    );
    assert!(
        internals.contains(".insert(seq, entry.address);"),
        "the list reconcile arm no longer keys by the decoded sequence"
    );
    assert!(
        internals.contains(".insert(member, (score, entry.address));"),
        "the zset reconcile arm no longer keys by the decoded member"
    );

    // RANGE READS use the durable maps, not the component.
    assert!(
        execute.contains("let mut ordered: Vec<(Vec<u8>, u64)> = shard\n        .zsets"),
        "zset_members_in_score_range no longer reads shard.zsets; if it has started walking the \
         bucket index, component order IS load-bearing"
    );

    // AND THE ONE CALLER THAT COULD HAVE CARED DOES NOT ORDER BY THE COMPONENT.
    //
    // RETARGETED, and the reason matters more than the new pattern. This arm used to assert the
    // literal `filter_map(|(_, address)| {` -- that SetMembers THREW THE COMPONENT AWAY -- as a
    // proxy for the property it means, which is that set member order is not component order.
    // The proxy stopped holding when a carried page started needing to say which element it is:
    // `read_block_bytes` now takes the component, so this arm binds it and passes it on. The
    // PROPERTY did not move. Member order is still the order
    // `bucket_index_component_block_addresses` walked in, exactly as before.
    //
    // So the assertion is now on what would actually break it: the component must reach
    // `read_block_bytes` and NOTHING ELSE. If this arm ever sorts, compares or keys by it, member
    // order becomes component order and an ordinal would change it -- which is the thing this
    // whole module is about.
    // THE PROXY IS RETIRED HERE. Three source-text proxies have stood for this property and all
    // three died of the arm changing shape -- the discarded component, then the bare component in
    // the identity slot, then `.into_iter()` with a ban on `sort`/`BTreeSet`. The third died on a
    // COMMENT: the arm stopped sorting altogether, and the sentence explaining why it no longer
    // needs to contains the word the ban matched. A text proxy a comment can fail is measuring the
    // wrong thing, so the property is asserted by DRIVING instead.
    //
    // THE PROPERTY, STATED ONCE: `SetMembers` inherits the order
    // `bucket_index_component_block_addresses` walked in and imposes no ordering of its own. That
    // is what makes an ordinal safe here -- it changes the walk and the listing together, rather
    // than leaving the listing ordered by something the component has stopped being.
    {
        let dir = tempfile::tempdir().expect("tempdir");
        let engine = engine_on(dir.path());
        load_on(&engine, OPERATOR_END);
        // Members whose BYTE order and INSERTION order differ, so inheriting the walk and
        // inheriting the write order are distinguishable answers.
        let members: Vec<Vec<u8>> = [7usize, 1, 9, 3, 5, 0, 8, 2, 6, 4]
            .iter()
            .map(|index| format!("ord-member-{index:02}").into_bytes())
            .collect();
        for member in &members {
            write(
                &engine,
                Command::SetAdd {
                    key: "eo-set-order".to_string(),
                    member: member.clone(),
                },
            );
        }

        // THE WALK'S OWN ORDER, read from the door the listing is built on.
        let walked: Vec<Vec<u8>> = {
            let shards = engine.shards.read().expect("engine lock poisoned");
            let shard = shards.get(&1).expect("shard 1 is loaded");
            crate::engine::bucket_store::bucket_index_component_block_addresses(
                shard,
                "set",
                "eo-set-order",
            )
            .into_iter()
            .filter_map(|(component, _address)| {
                hex::decode(component.as_deref().unwrap_or_default()).ok()
            })
            .collect()
        };
        assert_eq!(
            members.len(),
            walked.len(),
            "DENOMINATOR: the walk yielded {} component(s) for {} members, so comparing orders \
             below would compare different populations",
            walked.len(),
            members.len()
        );

        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::SetMembers {
                key: "eo-set-order".to_string(),
            },
        });
        assert!(response.status.ok, "the listing failed: {response:?}");
        let listed = match response.response {
            crate::types::CommandResponse::Members { members } => members,
            other => panic!("expected Members, got {other:?}"),
        };
        assert_eq!(
            walked, listed,
            "the set listing's order is not the order the whole-object door walked in. It must \
             INHERIT that order and impose none of its own; if it sorts or keys by anything else, \
             respelling the component changes the walk without changing the listing and the two \
             stop agreeing"
        );
        // AND THE WALK IS NOT THE WRITE ORDER, so the assertion above is a real comparison rather
        // than two orders that happen to coincide.
        assert_ne!(
            members, walked,
            "the fixture's write order and the walk's order are the same, so the comparison above \
             cannot tell an inherited order from an insertion order"
        );
    }

    // And driven: a reload puts a list back IN ORDER even though the reconcile re-keys rather than
    // inheriting the walk's order. Element by element, because a length check cannot see a swap.
    let dir = tempfile::tempdir().expect("tempdir");
    let pushed: Vec<Vec<u8>> = (0..9)
        .map(|element| format!("ordered-{element:02}").into_bytes())
        .collect();
    {
        let engine = engine_on(dir.path());
        load_on(&engine, OPERATOR_END);
        for member in &pushed {
            write(
                &engine,
                Command::ListPush {
                    key: "eo-order".to_string(),
                    member: member.clone(),
                    left: false,
                },
            );
        }
        engine.unload_shard(1);
    }
    let engine = engine_on(dir.path());
    load_on(&engine, OPERATOR_END);
    let reloaded = match engine
        .execute(ExecuteRequest {
            shard_id: 1,
            command: Command::ListRange {
                key: "eo-order".to_string(),
                start: 0,
                stop: -1,
            },
        })
        .response
    {
        crate::types::CommandResponse::Members { members } => members,
        other => panic!("a list range answered {other:?}"),
    };
    assert_eq!(
        reloaded.len(),
        pushed.len(),
        "DENOMINATOR: the list came back with {} of {} elements",
        reloaded.len(),
        pushed.len()
    );
    for (element, (want, got)) in pushed.iter().zip(reloaded.iter()).enumerate() {
        assert_eq!(
            want,
            got,
            "position {element} came back as {:?}, not {:?} -- the order is re-established by the \
             BTreeMap the reconcile inserts into, so this is the assertion that would fail if it \
             were inherited from the walk instead",
            String::from_utf8_lossy(got),
            String::from_utf8_lossy(want)
        );
    }

    println!(
        "\n  [component order] 2 write-arm claims still in the tree; 3 reconcile arms re-key by the \
         DECODED value; range reads use shard.zsets / shard.lists; 3 component-sorting readers need \
         only a total order; SetMembers discards the component and HashLen reads len()\n  \
         [component order] {} list elements came back in order across a reload",
        reloaded.len()
    );
}

// =================================================================================================
// 13. THE STOP CONDITION: THE FOLD DELIVERS ELEMENTS THE DURABLE MAP DOES NOT HOLD
// =================================================================================================

/// AN ORDINAL LOSES THE MEMBER ON THE ONE PATH WHERE THE NAME IS THE ONLY COPY, AND THAT PATH IS
/// ORDINARY RECOVERY.
///
/// The case FOR an ordinal rests on the member already being persisted twice: `set_index_serde` holds
/// `(member bytes, address)` and `zset_index_serde` holds `(member bytes, (score, address))`, and the
/// reconcile's own comment calls the name "a second copy of it rendered as text". If the durable map
/// always had the member, an ordinal would have a source for the join and would need no page reads.
///
/// **IT DOES NOT ALWAYS HAVE IT, AND THE EXCEPTION IS NAMED IN THE SAME COMMENT**: the name's score is
/// "the fallback for a member the durable map does not have, which is how an element folded out of the
/// delta log arrives". Established here rather than taken on trust:
///
///   * `apply_key_states` folds THIRTEEN maps -- `features`, `expires_at_ms`, four `control_state_*`
///     and seven `context_*`. **`sets`, `zsets`, `lists` and `hashes` are not among them**, asserted
///     below at source level with the input size checked first.
///   * `fold_delta_block_items` DOES restore the page items, so after a fold the bucket index holds
///     pages for elements whose durable map entry was never written.
///   * So on that path the component is the only copy of the member, which is exactly what #1989's
///     `an_element_the_durable_map_does_not_hold_still_comes_back_from_its_name` exists to protect.
///
/// # THE EXPERIMENT, WHICH IS THAT TEST'S FIXTURE WITH THE NAME MADE ORDINAL-SHAPED
///
/// Two zset members. One is dropped from the durable map only -- the shape a fold produces. Then its
/// page entry's component is rewritten to a SIXTEEN-CHARACTER ordinal spelling, which is what a 2-byte
/// inline page id would leave behind: a name that identifies the page and carries no member. Reload.
///
///   * The untouched member comes back -- the control, so a total loss cannot pass as this finding.
///   * The member whose name became an ordinal **does not**, and cannot: the reconcile's zset arm
///     requires `component.len() > 16` and the durable map has nothing, so the element is skipped and
///     there is no second source. Its bytes are still in the page PAYLOAD, and the reconcile
///     deliberately does not read pages -- that is the whole reason a record carries its outcomes.
///
/// **SO THE ORDINAL IS UNSAFE AND THIS IS THE STOP CONDITION, NOT A HURDLE.** Shipping it would be a
/// lossy migration: an ordinary fold-backed recovery would return fewer members than were written, and
/// nothing would report it. This engine has already shipped a member "served but undeletable" and a
/// store answering above its own shard end; a member lost on reload is worse than either.
///
/// For an ordinal to be safe the FOLD would have to carry the member bytes -- which is a change to the
/// delta record's outcome shape, not to this field, and is #1982's territory rather than this one's.
///
/// rust-internal: mutates the engine's own in-memory index, no external surface
#[test]
fn an_ordinal_loses_the_member_the_fold_delivers_without_a_durable_entry() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let engine_src = std::fs::read_to_string(root.join("engine.rs")).expect("engine.rs is readable");
    assert!(
        engine_src.len() > 100_000,
        "DENOMINATOR: read {} bytes of engine.rs; a short read scores the count below as a pass",
        engine_src.len()
    );

    // The window for one top-level item: from its `fn` line to the next top-level `fn` or `trait`.
    //
    // This bounded `apply_key_states` on `"\n/// Set or clear one key's entry"` -- the doc comment
    // of the item that FOLLOWED it. That is not a property of `apply_key_states`, and when items
    // were later added between the two the window silently grew to span them, so
    // `folded.contains("shard.sets")` started answering about a different function. A window that
    // widens on its own is worse than a red test, so it is bounded on itself now.
    let window = |needle: &str| -> String {
        let start = engine_src
            .find(needle)
            .unwrap_or_else(|| panic!("{needle} is in the tree"));
        let rest = &engine_src[start + needle.len()..];
        let end = ["\nfn ", "\ntrait ", "\npub(super) fn ", "\ntype "]
            .iter()
            .filter_map(|marker| rest.find(marker))
            .min()
            .expect("the item has a top-level successor to bound it");
        rest[..end].to_string()
    };

    // WHAT `apply_key_states` FOLDS: thirteen maps, each PER KEY.
    let folded = window("fn apply_key_states(");
    let folded_maps = folded.matches("apply_key_state_field(").count();
    assert_eq!(
        folded_maps, 13,
        "apply_key_states folds {folded_maps} maps, not the thirteen this finding rests on; \
         re-enumerate them before trusting the exclusion below"
    );
    // STILL EXCLUDED FROM THIS FUNCTION, and now for a different reason than when this was
    // written. The four container maps are restored by `fold_carried_container_elements`, NOT here,
    // because an element may only be restored once the whole fold has finished: a fold replays a
    // suffix of the log and can add an element and then take its page away again, and applying per
    // record would leave it in the durable map for `fill_absent_elements` to serve after a delete.
    for container in ["shard.sets", "shard.zsets", "shard.lists", "shard.hashes"] {
        assert!(
            !folded.contains(container),
            "apply_key_states now writes {container} directly. The carry must be applied AFTER the \
             fold, against the finished page index, or a removed element comes back -- see \
             `fold_carried_container_elements`."
        );
    }

    // AND THE STOP CONDITION IS LIFTED, asserted where the carry actually lives. This block read
    // `assert!(!folded.contains(container))` against `apply_key_states`, and its message said that
    // the fold restoring the durable container maps "is the one change that would make an element
    // ordinal safe". That change has landed: the fold now restores all four from the member bytes
    // the delta record carries, so a folded element's identity no longer arrives by decoding the
    // component its page was filed under. Inverted rather than deleted, because what has to keep
    // holding is that the fold DOES carry them.
    let carried = window("fn fold_carried_container_elements(");
    for container in ["shard.sets", "shard.zsets", "shard.lists", "shard.hashes"] {
        assert!(
            carried.contains(container),
            "the fold no longer restores {container}. The carry that lifted this stop condition has \
             been removed, so an element a delta delivers is once again known only by its component \
             name -- read what changed rather than restoring the old assertion."
        );
    }
    assert!(
        carried.contains("live.contains(") || carried.contains("&live"),
        "the carry no longer filters on whether the fold left a page behind, which is the only \
         thing stopping it from resurrecting a removed element"
    );

    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    load_on(&engine, OPERATOR_END);

    let control = b"fold-control".to_vec();
    let doomed = b"fold-doomed".to_vec();
    write(
        &engine,
        Command::ZSetAdd {
            key: "eo-fold".to_string(),
            member: control.clone(),
            score: 1.0,
        },
    );
    write(
        &engine,
        Command::ZSetAdd {
            key: "eo-fold".to_string(),
            member: doomed.clone(),
            score: 2.0,
        },
    );
    assert_eq!(
        pages_of(&engine, "zset", "eo-fold").len(),
        2,
        "DENOMINATOR: the fixture did not file two pages"
    );

    // The shape a fold produces for ONE element: a page entry with no durable map entry. Then the
    // name is made ordinal-shaped -- sixteen characters, carrying a page number and no member.
    let doomed_component = zset_name(2.0, &doomed);
    {
        let mut shards = engine.shards.write().expect("engine lock poisoned");
        let shard = shards.get_mut(&1).expect("shard 1 is loaded");
        let removed = shard
            .zsets
            .elements_mut_for_test("eo-fold")
            .expect("the zset is present")
            .remove(doomed.as_slice());
        assert!(
            removed.is_some(),
            "the durable map did not hold the doomed member, so this fixture cannot build the state \
             a fold produces"
        );

        // Rewrite that page's component to the ordinal spelling. Remove and re-insert, because the
        // map's handle is DERIVED from the fields -- which is the same property that makes an
        // ordinal a legitimate key at all.
        let CoreIndex {
            bucket_map,
            block_slab_live: live,
            ..
        } = &mut shard.bucket_index;
        let mut rewritten = 0usize;
        for bucket in bucket_map.values_mut() {
            let doomed_handles: Vec<u64> = bucket
                .block_index
                .iter()
                .filter(|(_, page)| page.component.as_deref() == Some(doomed_component.as_str()))
                .map(|(handle, _)| *handle)
                .collect();
            for handle in doomed_handles {
                let Some(mut page) = bucket.block_index.remove(&handle, live) else {
                    continue;
                };
                // A 2-byte inline page id, spelled the width a fixed-width ordinal would be.
                page.component = Some(std::sync::Arc::from("0000000000000001"));
                bucket.block_index.insert(page, live);
                rewritten += 1;
            }
        }
        assert_eq!(
            rewritten, 1,
            "rewrote {rewritten} page components, not the one this experiment needs"
        );
        shard.bucket_index.rebuild_object_block_lookup();
        println!(
            "  [fold] dropped the doomed member from the durable map and made its name a \
             sixteen-character ordinal"
        );
    }

    engine.unload_shard(1);
    load_on(&engine, OPERATOR_END);

    let score_of = |member: &[u8]| -> Option<f64> {
        match engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::ZSetScore {
                    key: "eo-fold".to_string(),
                    member: member.to_vec(),
                },
            })
            .response
        {
            crate::types::CommandResponse::Bytes { value: Some(bytes) } => {
                String::from_utf8_lossy(&bytes).parse::<f64>().ok()
            }
            _ => None,
        }
    };

    let control_score = score_of(&control);
    let doomed_score = score_of(&doomed);
    println!(
        "  [fold] after the reload: control={control_score:?}  doomed={doomed_score:?}\n  [fold] \
         the doomed member's bytes are still in the page payload, and the reconcile does not read \
         pages"
    );

    // THE CONTROL, so a fixture that lost everything cannot pass as this finding.
    assert_eq!(
        control_score,
        Some(1.0),
        "the untouched member did not come back either, so this fixture lost the whole key and says \
         nothing about the ordinal"
    );

    // THE FINDING.
    assert!(
        doomed_score.is_none(),
        "the member came back at {doomed_score:?} from a sixteen-character ordinal name with no \
         durable entry. If this is red, something now recovers an element's identity without the \
         name -- and that is exactly what would LIFT this stop condition, so read what changed \
         rather than relaxing this."
    );
}
