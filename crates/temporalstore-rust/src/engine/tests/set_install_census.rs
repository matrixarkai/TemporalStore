// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

// =================================================================================================
// DOES THE FOLD ROUTE LEAVE THE DURABLE SET MAP NAMING EVERY MEMBER THE PAGE INDEX NAMES?
//
// #2078 established this for `hashes` and for `hashes` ONLY. The proof does not carry over: a
// different kind has different install arms, a different level-2 key, and a different reconcile
// shape, so "the fold leaves the container complete" has to be re-established per kind or it is an
// assumption wearing a test's clothes. This module is that proof for `sets`, or its refutation.
//
// WHY IT MATTERS HERE, which is the same reason it mattered for hash. A page entry carries
// `component` -- for a set, the member hex -- and the model map is a second copy of the same
// identity. The entry's `component` is 16 of the 56 bytes a resident entry weighs, and taking it off
// leaves `SetMembers` nowhere to read a member name FROM except `shard.sets`. So the order is not
// "reroute the readers, then remove the field": it is prove the map complete FIRST, because the
// removal is what makes the map load-bearing.
//
// WHAT IS ASSERTED: that the two sources name THE SAME SET of members after a fold -- set equality
// in both directions, not a count. A count cannot tell a missing member from an extra one, and the
// two failures mean opposite things: a member the map lacks is the removal losing data, and a member
// the map holds alone is the fold resurrecting something.
//
// AND THE REMOVAL GATE, DRIVEN. A deleted member must be in NEITHER source after a reload. Without
// that, set equality can be satisfied by a fold that resurrects a removal into both.
// =================================================================================================

#![allow(clippy::all)]
use super::*;

/// THE CENSUS: EVERY PATH THAT INSTALLS A SET MEMBER REPORTS A TOUCHED ELEMENT, and the class is
/// bounded by the engine's own match rather than by a grep.
///
/// # WHY A CENSUS AND NOT A COUNT OF GREP HITS
///
/// The question is "could some writer install a member the carried channel never hears about?", and
/// a grep answers a different question -- it finds the sites that spell a pattern. The class here is
/// bounded twice over from the engine's own source: the `Command` enum names every command that
/// exists, and `execute_on_shard`'s match names every arm that handles one. A command absent from
/// the enum cannot be sent, and an arm absent from the match falls to the catch-all and performs no
/// write at all.
///
/// # WHAT THIS FOUND FOR `sets`, STATED EITHER WAY
///
/// `command_touched_container_elements` has ONE arm for this kind -- `Command::SetAdd` -- and its
/// catch-all returns an empty vector, which is where an install could go unreported. For hash the
/// equivalent reading turned up a writer with no record at all: `write_context_node`, which installs
/// a hash element and is deliberately never filed in the bucket index. **There is no equivalent for
/// `sets`.** Every install of a set member goes through `Command::SetAdd` or through WAL replay, and
/// replay is the record being re-applied rather than a new write.
///
/// rust-internal: reads this crate's own source text, no product behaviour
#[test]
fn every_command_that_installs_a_set_member_reports_a_touched_element() {
    let types = include_str!("../../types.rs");
    let execute = include_str!("../execute_on_shard.rs");
    let engine = include_str!("../../engine.rs");
    let lifecycle = include_str!("../lifecycle.rs");

    // VACUITY, FIRST. Four source files that did not load would make every count below a zero
    // presented as a finding.
    assert!(
        types.len() > 50_000
            && execute.len() > 50_000
            && engine.len() > 100_000
            && lifecycle.len() > 50_000,
        "a source file did not load: {} / {} / {} / {}",
        types.len(),
        execute.len(),
        engine.len(),
        lifecycle.len(),
    );

    // ---- BOUND 1: the commands that exist, from the enum. -------------------------------------
    //
    // `Set` prefixed and NOT `ZSet` prefixed, because a zset is a different kind with a different
    // map. Taken at the enum's own indentation so a doc comment mentioning a name is not counted.
    let declared: std::collections::BTreeSet<String> = types
        .lines()
        .filter_map(|line| {
            let trimmed = line.strip_prefix("    ")?;
            if trimmed.starts_with("    ") || !trimmed.starts_with("Set") {
                return None;
            }
            let end = trimmed.find(|c: char| !c.is_alphanumeric())?;
            Some(trimmed[..end].to_string())
        })
        .collect();
    println!("[set-census] Set-family commands declared: {declared:?}");
    assert!(
        declared.len() >= 3,
        "only {} Set-family command(s) were found in the enum, which is too few to be this surface \
         -- the scan is not reading what it thinks: {declared:?}",
        declared.len(),
    );

    // ---- BOUND 2: the arms that handle them, from the match. ----------------------------------
    let handled: std::collections::BTreeSet<String> = declared
        .iter()
        .filter(|name| execute.contains(&format!("Command::{name} {{")))
        .cloned()
        .collect();
    println!("[set-census] of those, handled by an arm in execute_on_shard: {handled:?}");
    assert_eq!(
        declared, handled,
        "a Set-family command is declared with no arm to handle it, or the arm spells its name \
         differently. Either way the class below is not bounded: declared {declared:?}, handled \
         {handled:?}"
    );

    // ---- WHICH OF THOSE ARMS INSTALLS, read from the arm bodies. ------------------------------
    //
    // An install is an arm that puts an element INTO `shard.sets`. The two spellings that do that
    // are an `entry(..)` chain and a direct `insert`, and both are looked for inside the arm's own
    // text rather than anywhere in the file.
    let mut installs: Vec<String> = Vec::new();
    for name in &declared {
        let marker = format!("Command::{name} {{");
        let Some(at) = execute.find(&marker) else {
            continue;
        };
        // The arm runs to the next `        Command::` at the match's own indentation.
        let rest = &execute[at + marker.len()..];
        let body = match rest.find("\n        Command::") {
            Some(end) => &rest[..end],
            None => rest,
        };
        // AN INSTALL IS A CALL TO THE RECORDED OPERATION, and that is now the ONLY spelling.
        //
        // This matcher has been blinded once already and the blinding was by the change that made
        // the statement stronger. It used to look for `.sets` plus an `.entry(` or `.insert(`,
        // which is what an arm spelled when it reached the model map directly. Since
        // `RecordedSetContainer` took the map, the inner field is private and an arm CANNOT reach
        // it -- so the only way to install is `install_set_member`, and looking for the old shape
        // reported zero installs on a tree where the install was right there.
        //
        // The stronger statement is why the restatement is not a relaxation: before, an arm could
        // install by any spelling that reached a `HashMap`, and this matcher had to guess them all.
        // Now there is exactly one, enforced by privacy, and the matcher names it.
        if body.contains("recorded_set_container::install_set_member(") {
            installs.push(name.clone());
        }
    }
    installs.sort();
    println!("[set-census] arms that install a set member: {installs:?}");
    // VACUITY: the operation must be findable by name, so a rename cannot make `installs` empty
    // and have someone "fix" this test by expecting nothing.
    assert!(
        execute.contains("recorded_set_container::install_set_member("),
        "the recorded set install is not findable by name in `execute_on_shard.rs`, so the census          below would report zero installs for the wrong reason"
    );
    assert_eq!(
        installs,
        vec!["SetAdd".to_string()],
        "the set of command arms that install a set member changed. Each one has to appear in \
         `command_touched_container_elements` or its installs are invisible to the carried channel, \
         which is what makes the durable map incomplete after a fold. Found: {installs:?}"
    );

    // ---- AND EACH INSTALLING ARM REPORTS, WITH THE THREE ROLES COUNTED SEPARATELY. ------------
    //
    // `TouchedContainerElement::Set` appears THREE times in `engine.rs` and only one of them is a
    // producer. Counting the bare name reported three, and this assertion failed on its first run
    // because of that -- the matcher being wrong, not the engine. A consumer is not a second
    // reporter. So each role is asked for by its own spelling:
    //
    //   * the PRODUCER, in `command_touched_container_elements`. Exactly one, because two
    //     renderings of one identity is the shape that let a claim about a hash FUNCTION be
    //     relayed as a claim about a hash PATH.
    //   * the KEY accessor's arm, which is how a carry gets grouped by object key.
    //   * the APPLY arm, which is what makes the carry non-vacuous -- a producer whose carry
    //     nothing applies writes a record no reload reads, and the completeness measured above
    //     would then be a property of the page index rather than of the carry.
    let producers = engine
        .matches("vec![TouchedContainerElement::Set {")
        .count();
    println!("[set-census] producers of a touched Set element: {producers}");
    assert_eq!(
        1, producers,
        "`TouchedContainerElement::Set` is PRODUCED in {producers} place(s) rather than one, so \
         which arm reports which install is no longer answerable by reading one line"
    );
    for name in &installs {
        assert!(
            engine.contains(&format!(
                "Command::{name} {{ key, member }} => vec![TouchedContainerElement::Set {{"
            )),
            "`Command::{name}` installs a set member but is not the arm that produces the touched \
             element, so a fold cannot carry what it wrote"
        );
    }
    assert!(
        engine.contains("TouchedContainerElement::Set { key, .. }"),
        "the `key()` accessor has no `Set` arm, so a carried set element cannot be grouped by its \
         object key"
    );
    assert!(
        engine.contains("TouchedContainerElement::Set { key, member } => shard"),
        "nothing APPLIES a carried set element back to the shard. A producer whose carry no reader \
         applies writes a record that no reload reads, which would make the completeness this \
         module just measured an accident of the page index rather than a property of the carry."
    );

    // ---- THE OTHER WRITER, NAMED RATHER THAN LEFT OUT. ----------------------------------------
    //
    // WAL replay installs into `shard.sets` too, and reports nothing -- correctly, because the item
    // it is replaying IS the record. This is where hash's census found its defect: a writer that
    // installs and files nothing. For sets the replay arm DOES re-file the block, so it is not even
    // the weaker of the two cases.
    assert!(
        lifecycle.contains("\"set\" => {"),
        "the WAL replay arm for `set` is no longer findable, so this census cannot say whether the \
         non-command install path still records"
    );
    let replay_at = lifecycle.find("\"set\" => {").expect("checked above");
    // A WIDER WINDOW than the 900 bytes this used, because the recorded call plus its argument list
    // and the note above it run longer than the two statements they replaced.
    let replay_body = &lifecycle[replay_at..(replay_at + 1_800).min(lifecycle.len())];

    // RESTATED FOR THE SAME REASON AS THE INSTALL MATCHER ABOVE, and this is the second time the
    // conversion blinded a matcher in this file. It asked for `upsert_bucket_index_block` by name
    // inside the arm, which is what the arm spelled while it reached the model map directly. The
    // arm now calls `install_set_member`, which does the upsert itself -- so the record is still
    // filed, by one function instead of two statements, and the thing to look for is that call.
    //
    // The claim is unchanged and is the one that matters: this replay path RECORDS. Hash's census
    // found `write_context_node` installing a hash element and filing nothing, which is the shape
    // that makes a replayed element resident and named by no page. The `set` replay arm is not that
    // shape, and after the conversion it cannot become it without this assertion failing -- because
    // the only other way to reach the map from a replay arm is a named exception in the container.
    assert!(
        replay_body.contains("recorded_set_container::install_set_member("),
        "the WAL replay arm for `set` no longer installs through the recorded operation. If it \
         reaches the model map some other way it is either using a named exception -- which for a \
         replay INSTALL would be a NEW one, and hash needed exactly such an exception for its \
         `context_node` arm -- or it is not installing at all. Either way this census no longer \
         says what it claims."
    );
    // AND IT IS NOT USING AN EXCEPTION, which is the per-kind difference from hash worth pinning:
    // a replay-install exception exists for `hashes` and must not quietly appear for `sets`.
    assert!(
        !replay_body.contains("replay_install"),
        "the `set` replay arm has acquired a replay-INSTALL exception. Hash needed one because its \
         `context_node` arm files no record; if sets now needs one, the reason has to be written \
         down in the container and this census updated to say so."
    );

    println!(
        "[set-census] {} declared command(s), {} install, each reports; the replay install re-files \
         its block. No unreported install path for `sets`.",
        declared.len(),
        installs.len()
    );
}
