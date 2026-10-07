// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! WHAT A GATED STORE SERVES AFTER IT HAS BEEN WRITTEN, CLOSED AND OPENED AGAIN.
//!
//! # WHY A STORE BOUNDARY AND NOT A ROUND INSIDE ONE ENGINE
//!
//! Every step of this series so far ran with the gate OFF by default and turned it on inside one
//! live engine. That exercises the projection and the consumers it feeds, and it cannot exercise
//! the one thing a default flip makes real: **a store whose index FILE was written by a gated
//! binary, read back by a process that did not write it.**
//!
//! A round trip inside one engine cannot see that. The resident `bucket_index` is already in
//! memory, the model maps are already populated, and a read can be answered from either without
//! the serialized index ever being consulted. So this module goes through the file: write, then
//! `unload_shard` -- which materializes the current in-memory index to `shard-{id}.index.json`
//! before the shard leaves memory -- then DROP THE ENGINE, then open a new one over the same page
//! and index directories.
//!
//! **ITS OWN CACHE DIRECTORY ON THE RELOAD.** Handed the first engine's cache, the second engine
//! answers out of pages the first left warm and the read proves nothing about what the file holds.
//! The reload gets `cache-reloaded`, so every byte it serves came off disk through the index it
//! just decoded.
//!
//! # ALL FOUR CONTAINER KINDS, AND THE ONE THE GATE DOES NOT CLAIM TO TOUCH
//!
//! RESTATED. This said the projection gates ONE arm and that "hash, zset and list emit one entry
//! per element either way", which was true when the set arm was the whole collapse. `list` is
//! collapsed now as well, so THE CONTROL IS TWO KINDS RATHER THAN THREE: `hash` and `zset`, both
//! deliberately held out -- a zset component carries the SCORE, and four hash readers resolve
//! through the index BY COMPONENT with no resident-map fallback. See `index_entry_names_a_page`
//! for the measured statement of each.
//!
//! A kind that is supposed to be unaffected is a CONTROL, and a control that is never read cannot
//! detect the case where the flip moved something nobody expected it to -- so the hash census is
//! now ASSERTED on the gated arm and not merely printed. It was bound and left unread when this
//! module was written, which the compiler reported as an unused variable and nobody acted on; a
//! control nothing asserts is not a control. The four counts are still printed side by side so
//! the difference between "the gate changed this" and "the store is broken" stays visible rather
//! than inferred.
//!
//! # EVERY NUMBER IS PRINTED BEFORE IT IS ASSERTED, AND EVERY FLOOR IS ON REACHING THE PATH
//!
//! `0 == 0` passes as agreement over a path nothing reached. So each arm first establishes that
//! the object CAME BACK AT ALL -- the durable model map holds its elements after the reload -- and
//! only then compares what a read serves against it. A listing that returns nothing because the
//! store is empty and a listing that returns nothing because the index cannot name the elements
//! are the same assertion failure and opposite findings; the floor is what tells them apart.
//!
//! # THE GATE IS HELD BY A GUARD THAT RESTORES IT WHILE UNWINDING
//!
//! The value is process-global and the verdict for this crate is a single-threaded run, so a test
//! that sets this variable and leaves it set contaminates every test after it in an ORDERED,
//! silent way -- which has already happened once in this series. `GateHeld` puts back whatever was
//! there, and `Drop` runs during unwinding, so a FAILING arm here cannot bury its own message
//! under a stranger's.
//!
//! Both directions are set EXPLICITLY, never by removing the variable. Removing it selects the
//! compiled-in default, and the whole subject of this module is a change to that default: an arm
//! that means "off" has to say `0`, or it stops meaning that the moment the default moves.
//!
//! # TWO THINGS THIS MODULE MEASURED THAT ARE NOT OBVIOUS FROM READING THE GATE
//!
//! **THE INDEX IS RE-DERIVED AT LOAD, SO THIS GATE DECIDES WHAT THE READER FILES.** Not what a
//! writer wrote. A store written by an ungated binary, opened by a gated one, comes up with the
//! COLLAPSED entry shape -- the projection is recomputed from the durable model maps on the way
//! in. So there is no grandfathering: moving the default changes how every store already on disk
//! is read, which is why this module had to exist before the default moved rather than after.
//! It is also why no format stamp is spent -- see the cross-version arm below.
//!
//! **THE OPERATOR WARNING IS ANTI-CORRELATED WITH THE DAMAGE.** `reconcile: N component name(s)
//! could not be read and were skipped` fires whenever a gated store is loaded, including in the
//! arms that serve every element correctly, and it stayed SILENT in the arm that served zero
//! before the listing was brought along (a store written ungated and read gated prints nothing,
//! because its entries were readable when the reconcile saw them). It is not a usable alarm for
//! this class of failure and nothing should be built on it.

#![allow(clippy::all)]
use super::*;
use crate::engine::TS_CONTAINER_ONE_ENTRY_A_PAGE;

const ELEMENTS: usize = 40;
const VALUE_WIDTH: usize = 24;

const HASH_KEY: &str = "corpus/hash";
const SET_KEY: &str = "corpus/set";
const ZSET_KEY: &str = "corpus/zset";
const LIST_KEY: &str = "corpus/list";

/// Holds the gate at one value, and puts back whatever was there when it goes out of scope --
/// on a normal drop AND during unwinding.
struct GateHeld {
    restore: Option<String>,
}

impl GateHeld {
    fn at(value: &str) -> Self {
        let restore = std::env::var(TS_CONTAINER_ONE_ENTRY_A_PAGE).ok();
        std::env::set_var(TS_CONTAINER_ONE_ENTRY_A_PAGE, value);
        Self { restore }
    }

    /// Stated as a value and not as an absence. See the module header.
    fn on() -> Self {
        Self::at("1")
    }

    fn off() -> Self {
        Self::at("0")
    }
}

impl Drop for GateHeld {
    fn drop(&mut self) {
        match self.restore.take() {
            Some(previous) => std::env::set_var(TS_CONTAINER_ONE_ENTRY_A_PAGE, previous),
            None => std::env::remove_var(TS_CONTAINER_ONE_ENTRY_A_PAGE),
        }
    }
}

/// What a read served, per container kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct PerKind {
    hash: usize,
    set: usize,
    zset: usize,
    list: usize,
}

impl PerKind {
    fn whole() -> Self {
        Self {
            hash: ELEMENTS,
            set: ELEMENTS,
            zset: ELEMENTS,
            list: ELEMENTS,
        }
    }
}

fn element_bytes(index: usize) -> Vec<u8> {
    let mut bytes = vec![0u8; VALUE_WIDTH];
    let stamp = format!("e-{index:05}");
    for (slot, byte) in stamp.as_bytes().iter().enumerate() {
        if slot < VALUE_WIDTH {
            bytes[slot] = *byte;
        }
    }
    bytes
}

fn field_name(index: usize) -> String {
    format!("f-{index:05}")
}

fn engine_on(cache: &std::path::Path, pages: &std::path::Path, indexes: &std::path::Path) -> TemporalEngine {
    TemporalEngine::with_local_dirs(64 * 1024 * 1024, cache, pages, indexes)
}

fn load_on(engine: &TemporalEngine) {
    let response = engine.load_shard_with(crate::control::LoadShardRequest {
        shard_id: 1,
        table_name: "corpus".to_string(),
        shard_uri: "local://corpus/1".to_string(),
        start_routing_bucket: 0,
        end_routing_bucket: 1023,
        readonly: false,
        load_version: 1,
        local_node_id: Some(1),
    });
    assert!(response.status.ok, "load failed: {:?}", response.status);
}

fn write_to(engine: &TemporalEngine, command: Command) {
    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command,
    });
    assert!(response.status.ok, "write failed: {response:?}");
}

/// `ELEMENTS` elements into each of the four container kinds.
fn write_corpus(engine: &TemporalEngine) {
    for index in 0..ELEMENTS {
        write_to(
            engine,
            Command::HashSet {
                key: HASH_KEY.to_string(),
                field: field_name(index),
                value: element_bytes(index),
            },
        );
        write_to(
            engine,
            Command::SetAdd {
                key: SET_KEY.to_string(),
                member: element_bytes(index),
            },
        );
        write_to(
            engine,
            Command::ZSetAdd {
                key: ZSET_KEY.to_string(),
                member: element_bytes(index),
                score: index as f64,
            },
        );
        write_to(
            engine,
            Command::ListPush {
                key: LIST_KEY.to_string(),
                member: element_bytes(index),
                left: false,
            },
        );
    }
}

fn members_of(engine: &TemporalEngine, command: Command) -> Vec<Vec<u8>> {
    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command,
    });
    assert!(response.status.ok, "read failed: {response:?}");
    match response.response {
        crate::types::CommandResponse::Members { members } => members,
        other => panic!("expected Members, got {other:?}"),
    }
}

/// WHAT A READ SERVES, through the command path every client reaches.
fn served(engine: &TemporalEngine) -> PerKind {
    let hash = {
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::HashGetAll {
                key: HASH_KEY.to_string(),
            },
        });
        assert!(response.status.ok, "read failed: {response:?}");
        match response.response {
            crate::types::CommandResponse::HashEntries { entries } => entries.len(),
            other => panic!("expected HashEntries, got {other:?}"),
        }
    };
    let set = members_of(
        engine,
        Command::SetMembers {
            key: SET_KEY.to_string(),
        },
    )
    .len();
    // `ZSetRange` answers interleaved member/score-string pairs, so the member count is half the
    // items. Division is stated here rather than inside an assertion so an odd length -- which
    // would mean the interleaving broke rather than that a member went missing -- is visible in
    // the printed number.
    let zset_items = members_of(
        engine,
        Command::ZSetRange {
            key: ZSET_KEY.to_string(),
            start: 0,
            stop: -1,
            rev: false,
        },
    )
    .len();
    assert_eq!(
        0,
        zset_items % 2,
        "the zset range answered {zset_items} items, which is not member/score pairs -- the \
         count below would be a reading of a broken interleave rather than of the members"
    );
    let list = members_of(
        engine,
        Command::ListRange {
            key: LIST_KEY.to_string(),
            start: 0,
            stop: -1,
        },
    )
    .len();
    PerKind {
        hash,
        set,
        zset: zset_items / 2,
        list,
    }
}

/// THE AUTHORITY FOR EXISTENCE: what the durable model maps hold, independent of any index entry.
///
/// This is the floor every comparison below rests on. A listing that serves nothing because the
/// store came back empty and a listing that serves nothing because the index cannot name the
/// elements are the same number and opposite findings.
fn durable(engine: &TemporalEngine) -> PerKind {
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard 1 loaded");
    PerKind {
        hash: shard.hashes.get(HASH_KEY).map(|e| e.len()).unwrap_or(0),
        set: shard.sets.get(SET_KEY).map(|e| e.len()).unwrap_or(0),
        zset: shard.zsets.get(ZSET_KEY).map(|e| e.len()).unwrap_or(0),
        list: shard.lists.get(LIST_KEY).map(|e| e.len()).unwrap_or(0),
    }
}

/// Live index entries for one kind and object: how many there are, and how many NAME an element.
fn entries_named(engine: &TemporalEngine, model_id: &str, object_key: &str) -> (usize, usize) {
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard 1 loaded");
    let mut live = 0usize;
    let mut named = 0usize;
    for bucket in shard.bucket_index.bucket_map.values() {
        for page in bucket.block_index.values() {
            if page.deleted || page.model_id.as_str() != model_id || &*page.object_key != object_key
            {
                continue;
            }
            live += 1;
            if page.component.is_some() {
                named += 1;
            }
        }
    }
    (live, named)
}

/// What one pass across the store boundary observed.
struct Boundary {
    /// What a read served after the reload, per kind.
    serves: PerKind,
    /// What the reloaded durable model maps hold -- the authority for existence.
    holds: PerKind,
    /// The set's (live, naming-an-element) entry census IN THE WRITER, immediately before the
    /// unload that materialized the index. This is what the index FILE was written from.
    ///
    /// Without it the cross-version arm is vacuous in the one direction that matters: a reader
    /// that serves everything proves nothing unless the store it opened actually held a
    /// page-named entry to be misread.
    set_entries_written: (usize, usize),
    /// The same census after the reload, which is what the READER derived.
    set_entries_reloaded: (usize, usize),
    hash_entries_reloaded: (usize, usize),
}

/// Write the corpus under `writer`, fold it, materialize the index, DROP the engine, and open a
/// new one under `reader` over the same page and index directories.
fn across_a_store_boundary(writer: &str, reader: &str) -> Boundary {
    let dir = tempfile::tempdir().expect("tempdir");
    let pages = dir.path().join("pages");
    let indexes = dir.path().join("indexes");

    let set_entries_written;
    {
        let _gate = GateHeld::at(writer);
        let engine = engine_on(&dir.path().join("cache"), &pages, &indexes);
        load_on(&engine);
        write_corpus(&engine);
        // Folded, because the shape this gate is about is elements SHARING one page. Without the
        // fold every element is its own page and one entry per page is one entry per element --
        // the gate would be inert and the arm would pass while testing nothing.
        engine
            .compact_shard_blocks(1)
            .expect("the fold round must succeed");
        // READ BEFORE THE UNLOAD, so what the index file was written from is observed rather than
        // inferred from what a later reader derived.
        set_entries_written = entries_named(&engine, "set", SET_KEY);
        // Materializes the in-memory index to the index file, then takes the shard out of memory.
        engine.unload_shard(1);
        drop(engine);
    }

    let _gate = GateHeld::at(reader);
    // ITS OWN CACHE DIRECTORY. Handed the writer's, this engine answers out of pages the writer
    // left warm and the read says nothing about what the index file holds.
    let reloaded = engine_on(&dir.path().join("cache-reloaded"), &pages, &indexes);
    load_on(&reloaded);
    Boundary {
        serves: served(&reloaded),
        holds: durable(&reloaded),
        set_entries_written,
        set_entries_reloaded: entries_named(&reloaded, "set", SET_KEY),
        hash_entries_reloaded: entries_named(&reloaded, "hash", HASH_KEY),
    }
}

fn report(label: &str, seen: &Boundary) {
    println!("  {label}");
    println!(
        "    served : hash {:>3}  set {:>3}  zset {:>3}  list {:>3}   (of {ELEMENTS} written)",
        seen.serves.hash, seen.serves.set, seen.serves.zset, seen.serves.list
    );
    println!(
        "    durable: hash {:>3}  set {:>3}  zset {:>3}  list {:>3}",
        seen.holds.hash, seen.holds.set, seen.holds.zset, seen.holds.list
    );
    println!(
        "    set entries: {} live / {} naming an element AS WRITTEN  ->  {} live / {} naming one \
         AS RELOADED",
        seen.set_entries_written.0,
        seen.set_entries_written.1,
        seen.set_entries_reloaded.0,
        seen.set_entries_reloaded.1
    );
    println!(
        "    hash entries reloaded: {} live / {} naming one",
        seen.hash_entries_reloaded.0, seen.hash_entries_reloaded.1
    );
}

/// A GATED STORE, WRITTEN AND THEN OPENED BY A GATED READER, SERVES EVERY ELEMENT OF ALL FOUR
/// CONTAINER KINDS.
///
/// The question the series has not asked. Nothing before this read a gated store back across the
/// index file, because every step defaulted to gate-off and turned it on inside one engine.
#[test]
fn a_gated_corpus_comes_back_whole_across_a_store_boundary_for_all_four_kinds() {
    println!("\n=== a store written gated, closed, and opened gated ===");
    let seen = across_a_store_boundary("1", "1");
    report("gate on -> gate on", &seen);
    let (serves, holds, set_entries, hash_entries) = (
        seen.serves,
        seen.holds,
        seen.set_entries_reloaded,
        seen.hash_entries_reloaded,
    );
    // THE CONTROL, ASSERTED. Both reader and writer are GATED in this arm, so this is the one
    // place in the module that observes what a gated binary files for a kind held OUT of the
    // collapse. It must still be one NAMED entry per field: if hash collapses here, the set and
    // zset/list figures below stop being attributable to the gate's kind list and the four
    // index-by-component hash readers have to have moved in the same change.
    assert_eq!(
        (ELEMENTS, ELEMENTS),
        hash_entries,
        "a GATED reader filed {} live hash entries of which {} name a field. Hash is held out of          the page-named set on purpose -- see `index_entry_names_a_page` -- so one named entry per          field is what a gated binary must still derive for it",
        hash_entries.0,
        hash_entries.1
    );
    // THE GATED PROJECTION WROTE THIS STORE. Asserted on the WRITER's census, before any reader
    // could have re-derived it, so "a gated store" is established rather than assumed.
    assert!(
        seen.set_entries_written.0 < ELEMENTS && seen.set_entries_written.1 == 0,
        "the writer filed {} live set entries of which {} name an element, so the index this arm \
         materialized is not a gated one",
        seen.set_entries_written.0,
        seen.set_entries_written.1
    );

    // ---- THE FLOOR: THE STORE CAME BACK. ----
    //
    // Before any served count is read. The durable model maps are the authority for existence and
    // they are not the index; if they are short, every number above is about a store that lost the
    // corpus and not about what an index entry can name.
    assert_eq!(
        PerKind::whole(),
        holds,
        "the reload did not bring the corpus back: {holds:?} of {ELEMENTS} per kind. Every served \
         count above is then a reading of an empty store, not of the projection"
    );
    // AND THE GATED PROJECTION ACTUALLY RAN. One entry per page means FEWER entries than elements
    // and none of them naming one; if the set still has forty named entries the writer ran ungated
    // and this arm is the ungated case wearing a gated label.
    assert!(
        set_entries.0 < ELEMENTS && set_entries.1 == 0,
        "the reloaded set has {} live entries of which {} name an element, so the gated projection \
         is not what wrote this index and the comparison below is between two ungated arms",
        set_entries.0,
        set_entries.1
    );

    // ---- AND NOW WHAT A CLIENT GETS. ----
    assert_eq!(
        PerKind::whole(),
        serves,
        "a gated store does not serve its own contents after a reload: {serves:?} of {ELEMENTS} \
         per kind, while the durable maps hold {holds:?}. The elements are IN the store and a read \
         cannot reach them"
    );
}

/// AN UNGATED STORE READ BY A GATED BINARY -- the direction a deployment takes on upgrade.
///
/// Every store in existence today was written ungated, because the gate ships off. Taking a binary
/// whose default is on must not change what any of them serves.
#[test]
fn an_ungated_store_comes_back_whole_under_the_gate() {
    println!("\n=== a store written ungated, closed, and opened gated ===");
    let seen = across_a_store_boundary("0", "1");
    report("gate off -> gate on", &seen);
    let (serves, holds) = (seen.serves, seen.holds);

    // THE STORE WAS WRITTEN THE WAY EVERY STORE IN EXISTENCE WAS: one named entry per element.
    assert_eq!(
        (ELEMENTS, ELEMENTS),
        seen.set_entries_written,
        "the writer filed {} live set entries of which {} name an element, so this is not the \
         ungated store shape every deployment is upgrading FROM",
        seen.set_entries_written.0,
        seen.set_entries_written.1
    );
    // AND THE GATED READER RE-DERIVED IT INTO THE COLLAPSED SHAPE. This is the finding that makes
    // the flip worth reviewing: the index is a DERIVED PROJECTION, re-derived at load, so the gate
    // decides what the READER files and not what the writer wrote. There is no grandfathering --
    // taking this binary changes how every store already on disk is read.
    assert!(
        seen.set_entries_reloaded.0 < ELEMENTS && seen.set_entries_reloaded.1 == 0,
        "the gated reader came up with {} live set entries of which {} name an element, so it did \
         NOT re-derive this ungated index through the gated projection and the arm is not \
         exercising the upgrade path it claims to",
        seen.set_entries_reloaded.0,
        seen.set_entries_reloaded.1
    );
    assert_eq!(
        PerKind::whole(),
        holds,
        "the reload did not bring the ungated corpus back: {holds:?} of {ELEMENTS} per kind"
    );
    assert_eq!(
        PerKind::whole(),
        serves,
        "a gated binary does not serve an ungated store whole: {serves:?} of {ELEMENTS} per kind. \
         This is the direction every existing deployment takes on upgrade"
    );
}

/// A GATED STORE READ BY A BINARY WITHOUT THE GATE, AND WHY NO FORMAT STAMP IS SPENT FOR IT.
///
/// # THIS ARM WAS WRITTEN TO DEMONSTRATE A MISREAD AND MEASURED THE OPPOSITE
///
/// It is kept, restated, because the refutation is the useful half: without it the next person
/// spends an irreversible stamp bump on a hazard that is not there.
///
/// The expectation was that a page-named entry -- `component: None` -- would be read by a binary
/// without the gate as an element whose name is simply absent, indistinguishable from a legitimate
/// `None`. There is a branch for exactly that: the set arm of
/// `reconcile_secondary_views_from_bucket_index` asks
/// `component.and_then(|c| hex::decode(c).ok())` and counts the `None` as an UNREADABLE NAME, the
/// same bucket a corrupt name falls into.
///
/// **It never reaches that branch for the elements, because the index is not a format.** It is a
/// DERIVED PROJECTION of the durable model maps, re-derived at load. The ungated reader opens a
/// store whose file holds ONE unnamed entry and comes up with FORTY NAMED ONES, re-filed from
/// `shard.sets` through the ungated arm of `visit_model_live_blocks`. The page-named entries do
/// not survive into its view at all, so there is nothing left for it to misread, and it serves
/// every element.
///
/// # WHAT THAT MEANS FOR `SHARD_INDEX_FORMAT_VERSION`
///
/// A stamp exists to refuse a store whose durable shape a binary cannot interpret, and
/// `persistence.rs` compares with `<`, so a stamp set too low is accepted SILENTLY. Spending one
/// here would buy nothing: a projection recomputed at load carries no durable shape to disagree
/// about, in either direction. The constant stays where it is, and this test is the reason
/// written down as a measurement rather than as a paragraph.
///
/// If a later step makes the collapsed entry shape SURVIVE a load -- an authoritative index that
/// is persisted rather than re-derived -- this assertion is the one that must go red, and the
/// stamp question reopens at that moment and not before.
#[test]
fn an_ungated_reader_re_derives_a_gated_store_and_so_needs_no_format_stamp() {
    println!("\n=== a store written gated, closed, and opened WITHOUT the gate ===");
    let seen = across_a_store_boundary("1", "0");
    report("gate on -> gate off", &seen);
    let (serves, holds) = (seen.serves, seen.holds);

    // ---- THE FLOOR: A GATED STORE IS WHAT WAS WRITTEN. ----
    //
    // On the WRITER's census, before any reader could have re-derived it. Without this the arm is
    // vacuous in the direction that matters: a reader that serves everything says nothing unless
    // the store it opened really held a page-named entry.
    assert!(
        seen.set_entries_written.0 < ELEMENTS && seen.set_entries_written.1 == 0,
        "the writer filed {} live set entries of which {} name an element, so the index this arm \
         materialized was never a gated one and the reader below had nothing nameless in front \
         of it",
        seen.set_entries_written.0,
        seen.set_entries_written.1
    );
    assert_eq!(
        ELEMENTS, holds.set,
        "the durable set map holds {} of {ELEMENTS} members after the reload, so whatever the \
         listing answers below is about a store that lost them",
        holds.set
    );

    // ---- THE MECHANISM, ASSERTED: THE READER RE-DERIVED THE ENTRIES IT WOULD HAVE MISREAD. ----
    assert_eq!(
        (ELEMENTS, ELEMENTS),
        seen.set_entries_reloaded,
        "the ungated reader came up with {} live set entries of which {} name an element. It must \
         re-file one NAMED entry per element out of the durable map -- that re-derivation is the \
         whole reason the page-named entries are not there to be misread, and if it stops \
         happening the stamp question reopens",
        seen.set_entries_reloaded.0,
        seen.set_entries_reloaded.1
    );

    // ---- AND SO: NO MISREAD. ----
    assert_eq!(
        ELEMENTS, serves.set,
        "an ungated reader served {} of the {} members a gated writer left in this store. The \
         refutation this test records is that it serves ALL of them; a short answer here means a \
         gated store IS misread by a binary without the gate, and the format-stamp question this \
         test closes is reopened",
        serves.set,
        holds.set
    );

    // ---- THE ONE KIND HELD OUT OF THE COLLAPSE IS THE CONTROL. ----
    //
    // `list` was a third of this control and is a collapsed kind now, so what is left is `hash`
    // and `zset`. They still do the control's job here: this arm's READER is ungated, so a short
    // answer means the reload is broken rather than that the gate did anything.
    assert_eq!(
        ELEMENTS, serves.hash,
        "the hash served {} of {ELEMENTS}. The gate does not touch the hash arm, so a short hash \
         means the reload is broken and the set figure above is not attributable to the gate",
        serves.hash
    );
    assert_eq!(
        (ELEMENTS, ELEMENTS),
        seen.hash_entries_reloaded,
        "the hash has {} live entries of which {} name a field. This arm's reader is UNGATED, so \
         every kind must file one named entry per element here -- a short count means the reload \
         is broken, not that the collapse reached a kind it should not have",
        seen.hash_entries_reloaded.0,
        seen.hash_entries_reloaded.1
    );
}
