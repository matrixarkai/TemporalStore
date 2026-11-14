// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Tests of the member set and the newest-snapshot anchor of the ranked cell read. They run on the
//! same hand-worked data as the parent module (see `data()` there): per-member `shown` totals over
//! the four snapshots are m1 30, m2 60, m3 200, m4 10 and the bytes member `zz` 5.

use super::*;
use crate::engine::cell_rank_options::{
    last_bounds, validate_options, Anchor, CompiledMembers, MemberFilter, RankOptions, MAX_FILTER_MEMBERS,
};
use crate::engine::cell_set::MAX_MEMBER_BYTES;

fn id(v: u64) -> CellMemberId {
    CellMemberId::U64(v)
}

fn zz() -> CellMemberId {
    CellMemberId::Bytes(b"zz".to_vec())
}

fn members(include: Option<Vec<CellMemberId>>, exclude: Vec<CellMemberId>) -> RankOptions {
    RankOptions { members: MemberFilter { include, exclude }, ..RankOptions::default() }
}

fn anchored() -> RankOptions {
    RankOptions { anchor: Anchor::Newest, ..RankOptions::default() }
}

fn run_with(
    spec: &CellRankSpec,
    options: &RankOptions,
    snaps: &Snapshots,
    now_ms: u64,
) -> Result<RankedCells, String> {
    Plan::compile_with(&header(), spec, options)?.run(now_ms, &views(snaps))
}

fn shown(options: &RankOptions) -> RankedCells {
    run_with(&spec(ALL, lane("shown"), 10), options, &data(), 400).expect("the query is valid")
}

// ---- the member set --------------------------------------------------------------------------

#[test]
fn an_include_and_an_exclude_set_keep_exactly_the_members_they_name() {
    let rows = |ranked: &RankedCells| (order(ranked), scores(ranked), ranked.snapshots);
    // Nothing named: every member, as before.
    assert_eq!(rows(&shown(&RankOptions::default())), (vec![3, 2, 1, 4, 99], vec![200, 60, 30, 10, 5], 4));
    // Include: m1, m3 and the bytes member.
    assert_eq!(
        rows(&shown(&members(Some(vec![id(1), id(3), zz()]), vec![]))),
        (vec![3, 1, 99], vec![200, 30, 5], 4)
    );
    // Exclude alone: everything but m2 and zz.
    assert_eq!(rows(&shown(&members(None, vec![id(2), zz()]))), (vec![3, 1, 4], vec![200, 30, 10], 4));
    // A member in both sets is excluded.
    assert_eq!(rows(&shown(&members(Some(vec![id(2), id(4)]), vec![id(4)]))), (vec![2], vec![60], 4));
    // A member the key does not hold is simply absent: an empty answer, not an error.
    assert_eq!(rows(&shown(&members(Some(vec![id(7)]), vec![]))), (vec![], vec![], 4));
    // A u64 member and a bytes member are different members even when they print alike.
    let bytes_one = CellMemberId::Bytes(vec![1]);
    assert_eq!(rows(&shown(&members(Some(vec![bytes_one]), vec![]))), (vec![], vec![], 4));
}

#[test]
fn the_member_set_is_anded_with_the_predicates_and_cuts_before_the_reduction() {
    // slot = 2 keeps (100,m2,20) (200,m4,8) (300,m1,10) (400,m2,10) (400,m4,2); among m2 and m3 only
    // m2 has such cells, 20 + 10. A set applied after the reduction would answer m2 at 60.
    let mut query = spec(ALL, lane("shown"), 10);
    query.filters = vec![filter("slot", FeatureFilterOp::Equal, 2)];
    let ranked =
        run_with(&query, &members(Some(vec![id(2), id(3)]), vec![]), &data(), 400).unwrap();
    assert_eq!((order(&ranked), scores(&ranked)), (vec![2], vec![30]));
}

#[test]
fn the_order_and_repetition_of_a_set_do_not_change_the_answer() {
    let plain = shown(&members(Some(vec![id(1), id(3), zz()]), vec![id(4), id(9)]));
    let shuffled = shown(&members(Some(vec![zz(), id(3), id(1), id(3), id(1), zz()]), vec![id(9), id(4), id(9)]));
    assert_eq!(plain, shuffled);
    assert_eq!(order(&plain), vec![3, 1, 99]);
    let compiled = |include: Vec<CellMemberId>| {
        CompiledMembers::compile(&MemberFilter { include: Some(include), exclude: vec![] }).unwrap()
    };
    assert_eq!(compiled(vec![id(5), id(1), id(5)]), compiled(vec![id(1), id(5)]));
    assert!(CompiledMembers::default().is_open());
    assert!(!compiled(vec![id(1)]).is_open());
}

#[test]
fn an_include_set_that_names_nothing_is_refused_and_an_empty_exclude_set_is_not() {
    let error = Plan::compile_with(&header(), &spec(ALL, lane("shown"), 1), &members(Some(vec![]), vec![]))
        .err()
        .unwrap();
    assert!(error.contains("names no member"), "{error}");
    assert!(validate_options(&ALL, &members(Some(vec![]), vec![])).is_err());
    assert!(validate_options(&ALL, &members(None, vec![])).is_ok());
    assert!(validate_options(&ALL, &members(Some(vec![id(1)]), vec![])).is_ok());
}

#[test]
fn a_member_set_read_equals_the_same_read_over_cells_filtered_by_an_independent_fold() {
    let mut rng = Rng(0xC0FF_EE12_3456_789B);
    let (mut compared, mut narrowed) = (0usize, 0usize);
    for _ in 0..60 {
        let count = 1 + rng.next() % 8;
        let snapshots: Snapshots = (1..=count)
            .map(|i| {
                let mut cells = Vec::new();
                for member in 0..30u64 {
                    if rng.next() % 2 == 0 {
                        let lanes = [rng.next() % 50, rng.next() % 50, rng.next() % 50, rng.next() % 50].map(|v| v as i64);
                        cells.push(cell(member, rng.next() % 4, lanes));
                    }
                }
                // Bytes members sort after every u64 member.
                for name in [&b"aa"[..], &b"bb"[..]] {
                    if rng.next() % 2 == 0 {
                        cells.push(Cell {
                            member: Member::Bytes(name.to_vec()),
                            labels: vec![rng.next() % 4],
                            lanes: vec![(rng.next() % 50) as i64, 1, 2, 3],
                        });
                    }
                }
                (i * 10, cells)
            })
            .collect();
        let pick = |rng: &mut Rng, p: u64| -> Vec<CellMemberId> {
            let mut set: Vec<CellMemberId> = (0..30u64).filter(|_| rng.next() % 100 < p).map(id).collect();
            if rng.next() % 3 == 0 {
                set.push(CellMemberId::Bytes(b"aa".to_vec()));
            }
            set
        };
        for round in 0..4 {
            let include = (round % 2 == 0).then(|| {
                let mut set = pick(&mut rng, 50);
                if set.is_empty() {
                    set.push(id(0));
                }
                set
            });
            let exclude = if round >= 1 { pick(&mut rng, 25) } else { Vec::new() };
            let options = members(include.clone(), exclude.clone());

            // The fold: a linear scan of the two lists, one cell at a time, with no sorting and no
            // search. The read under test never sees it.
            let wanted = |c: &Cell| {
                let member = match &c.member {
                    Member::U64(v) => CellMemberId::U64(*v),
                    Member::Bytes(b) => CellMemberId::Bytes(b.clone()),
                };
                include.as_ref().map_or(true, |set| set.contains(&member)) && !exclude.contains(&member)
            };
            let folded: Snapshots = snapshots
                .iter()
                .map(|(ts, cells)| (*ts, cells.iter().filter(|c| wanted(c)).cloned().collect()))
                .collect();

            for score in [
                lane("shown"),
                CellScore::Time,
                CellScore::Ratio { numerator: "clicked".to_string(), denominator: "shown".to_string() },
                CellScore::Weighted { lanes: vec!["shown".to_string(), "seen".to_string()], weights: vec![0.5, 2.0] },
            ] {
                for k in [3usize, 1000] {
                    let mut query = spec(ALL, score.clone(), k);
                    query.filters = vec![filter("slot", FeatureFilterOp::NotEqual, 3)];
                    let got = run_with(&query, &options, &snapshots, 0).unwrap();
                    let want = run_on(&query, &folded, 0).unwrap();
                    assert_eq!(got, want, "{score:?} k = {k} include {include:?} exclude {exclude:?}");
                    compared += got.rows.len();
                    if got != run_on(&query, &snapshots, 0).unwrap() {
                        narrowed += 1;
                    }
                }
            }

            // And an oracle that is not the ranked read at all: the `shown` total per member.
            let mut table: std::collections::BTreeMap<Member, i64> = Default::default();
            for (_, cells) in &folded {
                for c in cells.iter().filter(|c| c.labels[0] != 3) {
                    *table.entry(c.member.clone()).or_insert(0) += c.lanes[0];
                }
            }
            let mut expected: Vec<(Member, i64)> = table.into_iter().collect();
            expected.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(&b.0)));
            let mut query = spec(ALL, lane("shown"), 1000);
            query.filters = vec![filter("slot", FeatureFilterOp::NotEqual, 3)];
            let got = run_with(&query, &options, &snapshots, 0).unwrap();
            let got_pairs: Vec<(Member, i64)> = got
                .rows
                .iter()
                .map(|r| {
                    let member = match &r.member {
                        CellMemberId::U64(v) => Member::U64(*v),
                        CellMemberId::Bytes(b) => Member::Bytes(b.clone()),
                    };
                    (member, r.score as i64)
                })
                .collect();
            assert_eq!(got_pairs, expected);
        }
    }
    assert!(compared > 2000, "the comparison covered {compared} rows, too few to mean anything");
    assert!(narrowed > 100, "only {narrowed} reads differ from the unfiltered one: the sets cut too little to test");
}

#[test]
fn a_set_holds_at_most_the_limit_as_sent_and_a_bytes_member_at_most_its_own_limit() {
    assert_eq!(MAX_FILTER_MEMBERS, 1024);
    let ids = |n: usize, from: u64| (0..n as u64).map(|i| id(from + i)).collect::<Vec<_>>();
    let spec_one = spec(ALL, lane("shown"), 10);

    // Include at exactly the limit is accepted AND is a real set: ids 0..1024 hold m1..m4 and not zz.
    let at = members(Some(ids(MAX_FILTER_MEMBERS, 0)), vec![]);
    assert_eq!(order(&shown(&at)), vec![3, 2, 1, 4]);
    let past = members(Some(ids(MAX_FILTER_MEMBERS + 1, 0)), vec![]);
    let error = Plan::compile_with(&header(), &spec_one, &past).err().unwrap();
    assert!(error.contains("include set") && error.contains("over the limit"), "{error}");

    // Exclude at exactly the limit is accepted and cuts nothing here; one past is refused.
    let at = members(None, ids(MAX_FILTER_MEMBERS, 100));
    assert_eq!(order(&shown(&at)), vec![3, 2, 1, 4, 99]);
    let past = members(None, ids(MAX_FILTER_MEMBERS + 1, 100));
    let error = Plan::compile_with(&header(), &spec_one, &past).err().unwrap();
    assert!(error.contains("exclude set") && error.contains("over the limit"), "{error}");

    // The limit is on the request AS SENT: the same member repeated past it is refused, and at it
    // is accepted and still one member.
    let repeated = |n: usize| members(Some(vec![id(3); n]), vec![]);
    assert_eq!(order(&shown(&repeated(MAX_FILTER_MEMBERS))), vec![3]);
    assert!(Plan::compile_with(&header(), &spec_one, &repeated(MAX_FILTER_MEMBERS + 1)).is_err());

    // A bytes member longer than any stored member could be is refused; the longest one is accepted.
    for side in [0, 1] {
        let long = |n: usize| {
            let member = CellMemberId::Bytes(vec![7; n]);
            if side == 0 {
                members(Some(vec![member]), vec![])
            } else {
                members(None, vec![member])
            }
        };
        assert!(Plan::compile_with(&header(), &spec_one, &long(MAX_MEMBER_BYTES)).is_ok(), "side {side}");
        let error = Plan::compile_with(&header(), &spec_one, &long(MAX_MEMBER_BYTES + 1)).err().unwrap();
        assert!(error.contains("member bytes") && error.contains("over the limit"), "side {side}: {error}");
    }
    assert_eq!(MAX_MEMBER_BYTES, 256);
}

// ---- the anchor ------------------------------------------------------------------------------

fn at_stamps(stamps: &[u64]) -> Snapshots {
    stamps.iter().map(|&ts| (ts, vec![cell(1, 0, [1, 0, 0, 0])])).collect()
}

fn count_in(duration_ms: u64, anchor: Anchor, snaps: &Snapshots, now_ms: u64) -> usize {
    let options = RankOptions { anchor, ..RankOptions::default() };
    run_with(&spec(CellWindow::Last { duration_ms }, lane("shown"), 1), &options, snaps, now_ms)
        .unwrap()
        .snapshots
}

#[test]
fn an_anchored_last_window_ends_at_the_newest_snapshot_whatever_the_clock_says() {
    let last = |duration_ms| spec(CellWindow::Last { duration_ms }, lane("shown"), 10);
    let result = |duration_ms, options: &RankOptions, now_ms| {
        let ranked = run_with(&last(duration_ms), options, &data(), now_ms).unwrap();
        (order(&ranked), scores(&ranked), ranked.snapshots)
    };
    // Newest = 400, so the window is [301, 400]: the snapshot at 400 alone. The request clock at
    // 400 would read [300, 400]: the snapshots at 300 and 400.
    let want_anchored = (vec![2, 4], vec![10, 2], 1);
    assert_eq!(result(100, &anchored(), 400), want_anchored);
    assert_eq!(result(100, &RankOptions::default(), 400), (vec![3, 1, 2, 99, 4], vec![100, 10, 10, 5, 2], 2));
    // The clock does not enter an anchored read: a lagged reader, a reader before the writes and a
    // reader at the end of time all get the same answer.
    for now_ms in [0, 399, 400, 10_000_000, u64::MAX] {
        assert_eq!(result(100, &anchored(), now_ms), want_anchored, "now = {now_ms}");
    }
    // A clock far past the writes empties the request-clock read and leaves the anchored one alone.
    assert_eq!(result(100, &RankOptions::default(), 10_000_000).2, 0);
    // Wider: [251, 400] holds 300 and 400; a duration past the newest stamp floors the start at 0.
    assert_eq!(result(150, &anchored(), 0).2, 2);
    assert_eq!(result(10_000, &anchored(), 0).2, 4);
}

#[test]
fn the_anchored_window_spans_exactly_its_duration_and_a_duration_of_zero_holds_nothing() {
    // Newest = 400, start = 401 - duration.
    let snaps = at_stamps(&[300, 301, 400]);
    assert_eq!(count_in(99, Anchor::Newest, &snaps, 0), 1, "[302, 400]");
    assert_eq!(count_in(100, Anchor::Newest, &snaps, 0), 2, "[301, 400]: the start is inclusive");
    assert_eq!(count_in(101, Anchor::Newest, &snaps, 0), 3, "[300, 400]");
    assert_eq!(count_in(1, Anchor::Newest, &snaps, 0), 1, "[400, 400]: one stamp");
    assert_eq!(count_in(0, Anchor::Newest, &snaps, 0), 0, "[401, 401) is empty");
    // The request clock reads one stamp wider and a duration of zero is the instant itself.
    assert_eq!(count_in(99, Anchor::Request, &snaps, 400), 2, "[301, 400]");
    assert_eq!(count_in(0, Anchor::Request, &snaps, 400), 1, "[400, 400]");

    // The start is floored at zero rather than wrapping.
    let early = at_stamps(&[0, 5]);
    assert_eq!(count_in(6, Anchor::Newest, &early, 0), 2, "[0, 5]");
    assert_eq!(count_in(5, Anchor::Newest, &early, 0), 1, "[1, 5]");
    assert_eq!(count_in(u64::MAX, Anchor::Newest, &early, 0), 2);

    // At the largest stamp the exclusive end does not wrap either.
    let top = at_stamps(&[u64::MAX - 1, u64::MAX]);
    assert_eq!(count_in(1, Anchor::Newest, &top, 0), 1);
    assert_eq!(count_in(2, Anchor::Newest, &top, 0), 2);
    assert_eq!(count_in(0, Anchor::Newest, &top, 0), 0);
    assert_eq!(last_bounds(Anchor::Newest, 2, 0, Some(u64::MAX)), Some((u64::MAX - 1, u64::MAX)));
    assert_eq!(last_bounds(Anchor::Newest, 1, 0, Some(5)), Some((5, 5)));
    assert_eq!(last_bounds(Anchor::Newest, 0, 0, Some(5)), None);
    assert_eq!(last_bounds(Anchor::Newest, 7, 0, None), None);
    assert_eq!(last_bounds(Anchor::Request, 7, 20, None), Some((13, 20)));

    // A key with no snapshot has no newest stamp: an empty answer, not an error.
    assert_eq!(count_in(100, Anchor::Newest, &Vec::new(), 0), 0);
}

#[test]
fn an_anchor_on_any_window_but_last_is_refused_and_a_request_anchor_is_accepted_on_all() {
    let windows = [
        CellWindow::Between { start_ms: 1, end_ms: 2 },
        CellWindow::Since { start_ms: 1 },
        CellWindow::LastN { n: 1 },
    ];
    for window in windows {
        let error = validate_options(&window, &anchored()).unwrap_err();
        assert!(error.contains("`last` window"), "{window:?}: {error}");
        assert!(Plan::compile_with(&header(), &spec(window.clone(), lane("shown"), 1), &anchored()).is_err());
        assert!(validate_options(&window, &RankOptions::default()).is_ok(), "{window:?}");
    }
    assert!(validate_options(&CellWindow::Last { duration_ms: 1 }, &anchored()).is_ok());
}

// ---- no option, no difference ----------------------------------------------------------------

#[test]
fn a_read_that_uses_no_option_answers_what_compile_has_always_answered() {
    let mut cases = 0;
    for window in [
        ALL,
        CellWindow::Between { start_ms: 100, end_ms: 300 },
        CellWindow::Last { duration_ms: 150 },
        CellWindow::Since { start_ms: 200 },
        CellWindow::LastN { n: 3 },
    ] {
        for score in [lane("shown"), lane("seen"), CellScore::Time] {
            for filters in [vec![], vec![filter("slot", FeatureFilterOp::NotEqual, 3)]] {
                for now_ms in [0, 400, 9_000] {
                    let mut query = spec(window.clone(), score.clone(), 4);
                    query.filters = filters.clone();
                    let old = run_on(&query, &data(), now_ms);
                    let new = run_with(&query, &RankOptions::default(), &data(), now_ms);
                    assert_eq!(old, new, "{window:?} {score:?} now {now_ms}");
                    cases += 1;
                }
            }
        }
    }
    assert_eq!(cases, 90);
    assert_eq!(RankOptions::default().anchor, Anchor::Request);
}

#[cfg(feature = "alloc-probe")]
mod probe {
    use super::*;
    use crate::alloc_probe::Probe;

    fn dataset(members: u64) -> Snapshots {
        (1..=8u64)
            .map(|ts| {
                let cells = (0..members)
                    .map(|m| Cell {
                        member: Member::U64(m),
                        labels: vec![m % 7],
                        lanes: vec![((m * 31 + ts) % 1000) as i64, 3, 0, 0],
                    })
                    .collect();
                (ts, cells)
            })
            .collect()
    }

    /// Allocations of a ranked read restricted to 64 included and 64 excluded members, over `members`
    /// members. The plan is compiled before the probe starts: compiling costs by the SET, not the data.
    fn allocations(members: u64) -> u64 {
        let snaps = dataset(members);
        let views = views(&snaps);
        let options = members_options();
        let query = CellRankSpec {
            window: CellWindow::LastN { n: 8 },
            filters: Vec::new(),
            score: lane("shown"),
            top_k: 10,
            project: Vec::new(),
        };
        let plan = Plan::compile_with(&header(), &query, &options).unwrap();
        let probe = Probe::start();
        let ranked = plan.run(100, &views).unwrap();
        let spent = probe.stop().allocs;
        assert_eq!(ranked.rows.len(), 10);
        spent
    }

    fn members_options() -> RankOptions {
        members(Some((0..64u64).map(|m| id(m * 3)).collect()), (0..64u64).map(|m| id(m * 6)).collect())
    }

    /// The member set is tested without allocating: a read through a 128-member set allocates the
    /// same number of times over 1 000 members and over 20 000. THE CONTROL: building the same
    /// restriction as a per-cell `Vec` copy allocates in proportion to the cells.
    #[test]
    #[ignore = "measurement: run with --features alloc-probe --ignored"]
    fn a_member_set_read_allocates_the_same_over_a_thousand_members_and_over_twenty_thousand() {
        let small = allocations(1_000);
        let large = allocations(20_000);
        let control = |members: u64| {
            let snaps = dataset(members);
            let probe = Probe::start();
            let kept: usize = snaps
                .iter()
                .map(|(_, cells)| cells.iter().filter(|c| matches!(c.member, Member::U64(m) if m % 3 == 0)).cloned().collect::<Vec<_>>().len())
                .sum();
            let spent = probe.stop().allocs;
            assert!(kept > 0);
            spent
        };
        println!("set read: {small} @1000 members, {large} @20000; copying control: {} @1000, {} @20000", control(1_000), control(20_000));
        assert!(small > 0, "the probe saw no allocation at all, so it is not installed");
        assert_eq!(small, large, "the filtered read's allocations moved with the number of members");
        assert!(control(20_000) > control(1_000) + 10_000, "CONTROL: a per-cell copy did not grow with the cells");
    }
}
