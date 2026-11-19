// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::*;
use crate::engine::cell_set::{
    decode_value, encode_value, merge_request, Cell, DimSpec, DimType, EntityType, KeySpec, LaneReducer, LaneSpec,
    HEADER_VERSION,
};

const STAMP: u64 = 100;

fn header() -> CellHeader {
    CellHeader {
        version: HEADER_VERSION,
        key: KeySpec {
            namespace: "ns".to_string(),
            entity: EntityType::U64,
            dimensions: vec![DimSpec { name: "channel".to_string(), ty: DimType::U16, required: true }],
        },
        labels: vec!["slot".to_string()],
        lanes: [
            ("sum", LaneReducer::Sum),
            ("max", LaneReducer::Max),
            ("min", LaneReducer::Min),
            ("first", LaneReducer::First),
            ("last", LaneReducer::Last),
            ("count", LaneReducer::Count),
        ]
        .into_iter()
        .map(|(name, reducer)| LaneSpec { name: name.to_string(), reducer })
        .collect(),
    }
}

/// A header that adds one trailing lane: a request under it extends the stored header.
fn wider_header() -> CellHeader {
    let mut h = header();
    h.lanes.push(LaneSpec { name: "extra".to_string(), reducer: LaneReducer::Sum });
    h
}

struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        self.0 >> 33
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n.max(1)
    }
}

fn cells(rng: &mut Rng, h: &CellHeader, universe: u64, n: usize) -> Vec<Cell> {
    (0..n)
        .map(|_| Cell {
            member: Member::U64(rng.below(universe)),
            labels: vec![rng.below(5)],
            lanes: (0..h.lanes.len())
                .map(|_| match rng.below(20) {
                    0 => i64::MAX - rng.below(3) as i64,
                    1 => i64::MIN + rng.below(3) as i64,
                    _ => rng.below(2001) as i64 - 1000,
                })
                .collect(),
        })
        .collect()
}

fn request(rng: &mut Rng, h: &CellHeader, universe: u64, n: usize) -> Vec<u8> {
    encode_value(h, &cells(rng, h, universe, n)).expect("a request of a few cells encodes")
}

/// A STORED sample: the request's cells merged into nothing, so strictly ascending by member as every
/// stored sample is (a request itself may repeat a member or list them in any order).
fn stored(rng: &mut Rng, h: &CellHeader, universe: u64, n: usize) -> Vec<u8> {
    let value = request(rng, h, universe, n);
    merge_request(None, None, &value).expect("a request of a few cells merges into nothing").0
}

/// The state a sample is in under the tail path: its stored base and its tail.
#[derive(Default)]
struct Tailed {
    base: Option<Vec<u8>>,
    tails: SampleTails,
    /// The same tail rebuilt from the log items alone, as a replay builds it.
    replayed: SampleTails,
}

impl Tailed {
    fn current(&self) -> Option<Vec<u8>> {
        let base = self.base.as_ref()?;
        Some(match self.tails.tail(STAMP) {
            Some(tail) => fold_value(base, tail).expect("a tail folds"),
            None => base.clone(),
        })
    }

    /// Writes one request; the receipt counts it landed, or the refusal.
    fn write(&mut self, request: &[u8]) -> Result<(usize, usize), String> {
        let h = header();
        let Some(base) = self.base.clone() else {
            let (value, _, counts) = merge_request(None, None, request)?;
            self.base = Some(value);
            return Ok((counts.created, counts.merged));
        };
        match plan_tail_write(&base, self.tails.tail(STAMP), request)? {
            TailPlan::Tail { write, memo } => {
                let tail = self.tails.tail_or_start(STAMP, write.base_cells)?;
                if let Some(memo) = memo {
                    tail.install_memo(memo);
                }
                tail.push(&write);
                self.replayed.apply_item(&write.item(STAMP))?;
                Ok((write.created, write.merged))
            }
            TailPlan::Whole => {
                let existing = self.current().expect("a base exists");
                let (value, _, counts) = merge_request(Some(&h), Some(&existing), request)?;
                self.base = Some(value);
                self.tails.clear(STAMP);
                self.replayed.clear(STAMP);
                Ok((counts.created, counts.merged))
            }
        }
    }
}

/// The whole-sample path: every request merged into the stored value, as a write has always been.
#[derive(Default)]
struct Whole {
    stored: Option<Vec<u8>>,
}

impl Whole {
    fn write(&mut self, request: &[u8]) -> Result<(usize, usize), String> {
        let h = header();
        let newest = self.stored.is_some().then_some(&h);
        let (value, _, counts) = merge_request(newest, self.stored.as_deref(), request)?;
        self.stored = Some(value);
        Ok((counts.created, counts.merged))
    }
}

fn differential(seed: u64, universe: u64, requests: usize, per_request: u64, require_tail: bool) {
    let h = header();
    let mut rng = Rng(seed);
    let (mut whole, mut tailed) = (Whole::default(), Tailed::default());
    let mut tail_writes = 0usize;
    for step in 0..requests {
        let n = 1 + rng.below(per_request) as usize;
        let request = request(&mut rng, &h, universe, n);
        let expected = whole.write(&request);
        let actual = tailed.write(&request);
        match (&expected, &actual) {
            (Ok(a), Ok(b)) => assert_eq!(a, b, "step {step}: the receipt counts differ"),
            (Err(_), Err(_)) => {}
            _ => panic!("step {step}: whole path {expected:?}, tail path {actual:?}"),
        }
        assert_eq!(tailed.current(), whole.stored, "step {step}: the fold is not the whole-sample value");
        assert_eq!(tailed.tails, tailed.replayed, "step {step}: a replay of the items builds another tail");
        let bytes = tailed.tails.encode();
        assert_eq!(SampleTails::decode(&bytes).expect("stored tails decode"), tailed.tails);
        tail_writes += tailed.tails.tail(STAMP).map_or(0, Tail::write_count);
    }
    assert!(!require_tail || tail_writes > 0, "the run never used a tail, so it proved nothing (seed {seed})");
}

#[test]
fn a_fold_of_base_and_tail_is_the_value_a_whole_sample_write_stores() {
    for (seed, universe, requests, per_request) in
        [(1, 8, 200, 3), (2, 64, 200, 6), (3, 300, 120, 20), (4, 2000, 80, 40), (5, 3, 300, 2)]
    {
        differential(seed, universe, requests, per_request, true);
    }
}

#[test]
fn a_write_that_would_pass_the_cell_limit_is_refused_on_both_paths_at_the_same_request() {
    // 200 cells a request from a universe past the limit: the 21st or so request crosses 4,096.
    differential(11, 60_000, 60, 200, false);
}

#[test]
fn a_tail_is_bounded_in_writes_and_in_cells_and_a_write_past_a_bound_is_stored_whole() {
    let h = header();
    let mut rng = Rng(7);
    let base = stored(&mut rng, &h, 50_000, 64);
    let mut tails = SampleTails::new();
    // Room is max(64, 64 / 8) = 64 cells: 3 requests of 20 fit, the 4th does not.
    let mut kinds = Vec::new();
    for _ in 0..4 {
        let req = request(&mut rng, &h, 50_000, 20);
        match plan_tail_write(&base, tails.tail(STAMP), &req).unwrap() {
            TailPlan::Tail { write, memo } => {
                let tail = tails.tail_or_start(STAMP, write.base_cells).unwrap();
                if let Some(memo) = memo {
                    tail.install_memo(memo);
                }
                tail.push(&write);
                kinds.push("tail");
            }
            TailPlan::Whole => kinds.push("whole"),
        }
    }
    assert_eq!(kinds, ["tail", "tail", "tail", "whole"]);

    // 256 writes of one cell into a base of 4,096 cells fit (room 512); the 257th does not.
    let big = stored(&mut rng, &h, 1_000_000, 1024);
    let mut base = big;
    for _ in 0..2 {
        let more = request(&mut rng, &h, 1_000_000, 1024);
        base = merge_request(Some(&h), Some(&base), &more).unwrap().0;
    }
    let mut tails = SampleTails::new();
    let mut outcome = Vec::new();
    for _ in 0..257 {
        let req = request(&mut rng, &h, 1_000_000, 1);
        match plan_tail_write(&base, tails.tail(STAMP), &req).unwrap() {
            TailPlan::Tail { write, memo } => {
                let tail = tails.tail_or_start(STAMP, write.base_cells).unwrap();
                if let Some(memo) = memo {
                    tail.install_memo(memo);
                }
                tail.push(&write);
                outcome.push(true);
            }
            TailPlan::Whole => outcome.push(false),
        }
    }
    assert_eq!(outcome.iter().filter(|t| **t).count(), MAX_TAIL_WRITES);
    assert!(!outcome[MAX_TAIL_WRITES]);
}

#[test]
fn a_request_that_extends_the_header_is_stored_whole() {
    let h = header();
    let mut rng = Rng(9);
    let base = stored(&mut rng, &h, 100, 10);
    let wide = wider_header();
    let req = encode_value(
        &wide,
        &[Cell { member: Member::U64(1), labels: vec![0], lanes: vec![1, 2, 3, 4, 5, 6, 7] }],
    )
    .unwrap();
    assert_eq!(plan_tail_write(&base, None, &req).unwrap(), TailPlan::Whole);
}

#[test]
fn a_plan_is_the_same_with_the_member_memo_and_without_it() {
    let h = header();
    let mut rng = Rng(13);
    let base = stored(&mut rng, &h, 500, 100);
    let mut with = SampleTails::new();
    let mut without = SampleTails::new();
    for _ in 0..8 {
        let req = request(&mut rng, &h, 500, 5);
        let a = plan_tail_write(&base, with.tail(STAMP), &req).unwrap();
        let b = plan_tail_write(&base, without.tail(STAMP), &req).unwrap();
        let (TailPlan::Tail { write: wa, memo }, TailPlan::Tail { write: wb, .. }) = (a, b) else {
            panic!("both paths take a tail here")
        };
        assert_eq!(wa, wb);
        let t = with.tail_or_start(STAMP, wa.base_cells).unwrap();
        if let Some(memo) = memo {
            t.install_memo(memo);
        }
        t.push(&wa);
        // The second tail never keeps a memo: it plans from the stored sample every time.
        let t = without.tail_or_start(STAMP, wb.base_cells).unwrap();
        t.push(&wb);
    }
    assert!(with.tail(STAMP).unwrap().has_memo());
    assert!(!without.tail(STAMP).unwrap().has_memo());
    assert_eq!(with, without);
}

#[test]
fn stored_tails_and_logged_items_are_canonical_and_refuse_every_other_spelling() {
    let h = header();
    let mut rng = Rng(17);
    let base = stored(&mut rng, &h, 100, 20);
    let mut tails = SampleTails::new();
    for stamp in [5u64, 9, 40] {
        for _ in 0..2 {
            let req = request(&mut rng, &h, 100, 3);
            if let TailPlan::Tail { write, memo } = plan_tail_write(&base, tails.tail(stamp), &req).unwrap() {
                let t = tails.tail_or_start(stamp, write.base_cells).unwrap();
                if let Some(m) = memo {
                    t.install_memo(m);
                }
                t.push(&write);
            }
        }
    }
    let bytes = tails.encode();
    assert_eq!(SampleTails::decode(&bytes).unwrap(), tails);
    for cut in 1..bytes.len() {
        // A cut between two stamps' records is a shorter, valid value; any other cut is refused.
        if let Ok(shorter) = SampleTails::decode(&bytes[..cut]) {
            assert!(shorter.len() < tails.len(), "a value cut at {cut} was accepted whole");
        }
    }
    let mut trailing = bytes.clone();
    trailing.push(0);
    assert!(SampleTails::decode(&trailing).is_err());
    assert!(SampleTails::decode(&[]).unwrap().is_empty());

    let item = TailItem { stamp: 77, base_cells: 300, cells: 2, packed: vec![1, 2, 3] };
    let encoded = item.encode();
    assert_eq!(TailItem::decode(&encoded).unwrap(), item);
    for cut in 0..encoded.len() {
        assert!(TailItem::decode(&encoded[..cut]).is_err(), "an item cut at {cut} was accepted");
    }
    let mut padded = encoded.clone();
    padded.push(0);
    assert!(TailItem::decode(&padded).is_err());
    // A stamp spelled with a padded varint reads back the same number and is still refused.
    let mut loose = Vec::new();
    loose.extend_from_slice(&[0x08, 0xCD, 0x80, 0x00]);
    loose.extend_from_slice(&encoded[2..]);
    assert!(TailItem::decode(&loose).is_err());
}

#[test]
fn at_most_the_stamp_limit_hold_a_tail_and_the_oldest_are_named_to_make_room() {
    let mut tails = SampleTails::new();
    for stamp in [10u64, 20, 30, 40] {
        tails.tail_or_start(stamp, 100).unwrap();
    }
    assert!(tails.tail_or_start(50, 100).is_err());
    assert!(tails.can_hold(20));
    assert!(!tails.can_hold(50));
    assert_eq!(tails.fold_to_make_room(50), vec![10]);
    assert_eq!(tails.fold_to_make_room(20), Vec::<u64>::new());
    assert_eq!(tails.retain_live(|s| s >= 30), 2);
    assert_eq!(tails.stamps().collect::<Vec<_>>(), vec![30, 40]);
    assert!(tails.clear(30));
    assert!(!tails.clear(30));
}

#[test]
fn what_a_tail_write_stores_does_not_grow_with_the_sample_while_a_whole_write_does() {
    let h = header();
    let mut rng = Rng(23);
    let mut tail_item = Vec::new();
    let mut whole_value = Vec::new();
    for size in [64usize, 1024, 4096] {
        let mut base = Vec::new();
        let mut member = 0u64;
        while base.len() < size {
            member += 1;
            base.push(Cell { member: Member::U64(member), labels: vec![0], lanes: vec![1, 2, 3, 4, 5, 6] });
        }
        let base_value = encode_value(&h, &base).unwrap();
        let req = encode_value(
            &h,
            &[Cell { member: Member::U64(member / 2), labels: vec![0], lanes: vec![1, 1, 1, 1, 1, 1] }],
        )
        .unwrap();
        let TailPlan::Tail { write, .. } = plan_tail_write(&base_value, None, &req).unwrap() else {
            panic!("a one cell write rides as a tail")
        };
        tail_item.push(write.item(STAMP).encode().len());
        whole_value.push(merge_request(Some(&h), Some(&base_value), &req).unwrap().0.len());
        let _ = rng.next();
    }
    // The tail item differs only by the width of the base size it names (a varint): at most two
    // bytes across a 64-fold growth, where the whole value grows with the sample.
    assert!(tail_item[2] <= tail_item[0] + 4, "tail item bytes {tail_item:?}");
    assert!(whole_value[2] >= 20 * whole_value[0], "the control must grow: whole value bytes {whole_value:?}");
    assert!(whole_value[2] > tail_item[2] * 20, "tail {tail_item:?} whole {whole_value:?}");
}

#[test]
fn a_decoded_base_and_its_fold_agree_on_the_header() {
    let h = header();
    let mut rng = Rng(29);
    let base = stored(&mut rng, &h, 50, 10);
    let mut tails = SampleTails::new();
    let req = request(&mut rng, &h, 50, 4);
    let TailPlan::Tail { write, memo } = plan_tail_write(&base, None, &req).unwrap() else { panic!("tail") };
    let t = tails.tail_or_start(STAMP, write.base_cells).unwrap();
    t.install_memo(memo.unwrap());
    t.push(&write);
    let folded = fold_value(&base, tails.tail(STAMP).unwrap()).unwrap();
    let (folded_header, _) = decode_value(&folded).unwrap();
    assert_eq!(folded_header, h);
}
