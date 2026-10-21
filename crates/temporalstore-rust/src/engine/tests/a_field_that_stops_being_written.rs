// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A field the encoder stops writing must still decode an image that lacks it.
//!
//! # THE TRAP, AND WHY THE IMAGE CANNOT SHOW IT TO YOU
//!
//! `#[serde(skip_serializing)]` stops a key being written. `#[serde(default)]` makes a missing key
//! decode instead of failing. They are two halves of one decision, and the first without the
//! second is a store that cannot be opened: the first image written without the key makes every
//! subsequent load of it FAIL rather than decode an empty field.
//!
//! Measured rather than reasoned, because the two attributes differ in exactly the way that
//! matters and in no way that is visible:
//!
//! ```text
//! skip             + missing key -> OK   (Default is supplied automatically)
//! skip_serializing + missing key -> ERR  missing field
//! skip             writes: {"kept":0}
//! skip_serializing writes: {"kept":0}
//! ```
//!
//! BOTH WRITE THE SAME BYTES. The stored image gives no way to tell them apart; only the decode
//! side differs. That is why this needs a guard over the source rather than a check over an image.
//!
//! And it is why `skip` is NOT a trigger here: `skip_deserializing` supplies `Default` itself, so
//! `#[serde(skip)]` alone is correct, and flagging it would refuse a shape this tree uses on
//! purpose.
//!
//! # WHY NOW, AND WHICH FIELDS
//!
//! The consolidation is moving series maps off the shape they are stored in, and the way a map
//! stops being written is by gaining `skip_serializing`. So the trap is reachable, repeatedly.
//!
//! Four `ShardState` fields carry NO serde attribute at all. That reads as `#[serde(default)]`
//! while the key is still written -- both always write it -- and is not `default` on decode. Each
//! one therefore needs `default` added in the SAME change that stops writing it, never after.
//! The list is printed by this test rather than written here, so it cannot go stale.
//!
//! # HOW TWO AUDITS OF THIS GOT IT WRONG, WHICH IS WHY THE MATCHER IS SHAPED AS IT IS
//!
//! An attribute is bound to a field by CONTIGUITY, and a doc comment between them breaks the run.
//! An audit that paired attributes to fields by adjacency silently gave a bare field the PREVIOUS
//! field's attribute -- so a table whose shape cannot represent "absent" reported absent as
//! whatever came before it.
//!
//! A second attempt matched the attribute name as TEXT, which counted the dozen prose comments in
//! `state.rs` that explain why some field carries it -- reporting nine violations where there are
//! none.
//!
//! Both are guarded below by controls rather than by care.
//!
//! rust-internal: guards this crate's own stored shape, no product behaviour

/// One field of the struct: its line, its name, and the attributes CONTIGUOUSLY above it.
struct Field {
    line: usize,
    name: String,
    attributes: Vec<String>,
}

impl Field {
    /// Whether anything stops this field being written. `skip` is deliberately not included --
    /// see the module header for the measurement.
    fn stops_being_written(&self) -> bool {
        self.attributes
            .iter()
            .any(|attribute| attribute.contains("skip_serializing"))
    }

    /// Whether a missing key decodes rather than failing.
    fn decodes_a_missing_key(&self) -> bool {
        self.attributes
            .iter()
            .any(|attribute| attribute.contains("default"))
    }
}

/// Every field declared in the named struct's body, with the attributes contiguously above it.
///
/// BOUNDED TO THE STRUCT, not to the file, so this is a property of `ShardState` rather than of
/// whatever else `state.rs` happens to contain.
///
/// CONTIGUOUS, which is the part the adjacency audit got wrong: the walk upward stops at the first
/// line that is not an attribute, so a doc comment, a plain comment or a blank line ends the run
/// and a field with nothing of its own reports NOTHING rather than its neighbour's.
fn fields_of(source: &str, declaration: &str) -> Vec<Field> {
    let lines: Vec<&str> = source.lines().collect();
    let start = lines
        .iter()
        .position(|line| line.starts_with(declaration))
        .unwrap_or_else(|| panic!("{declaration} is in the source"));
    let end = (start + 1..lines.len())
        .find(|&index| lines[index] == "}")
        .expect("the struct body is closed at column zero");

    let mut fields = Vec::new();
    for index in start + 1..end {
        let trimmed = lines[index].trim();
        // A field declaration, not an attribute, comment or nested item.
        let Some((name, rest)) = trimmed.split_once(':') else {
            continue;
        };
        // NOT A PATH SEGMENT. A multi-line field declaration continues with lines like
        // `super::storage_bucket_internals::StoredModelKind,`, whose first colon splits off
        // `super` -- a lowercase word that reads exactly like a field name. One of those was
        // reported as a bare field, and it was visible only because this test PRINTS what it
        // finds rather than only asserting over it. A real field's name is followed by one colon.
        if rest.starts_with(':') {
            continue;
        }
        let name = name.trim_start_matches("pub(super) ").trim_start_matches("pub ").trim();
        if name.is_empty() || !name.chars().all(|c| c.is_ascii_lowercase() || c == '_') {
            continue;
        }
        let mut attributes = Vec::new();
        let mut above = index;
        while above > start + 1 {
            above -= 1;
            let candidate = lines[above].trim();
            if candidate.starts_with("#[") {
                attributes.insert(0, candidate.to_string());
            } else if candidate.starts_with("//") || candidate.is_empty() {
                // A comment is not an item and does not end an attribute's reach. The first
                // version of this walk stopped here, which reported a field as bare when its
                // attribute sat three doc-comment lines above it.
                continue;
            } else {
                // Anything else is the previous item, which does end it.
                break;
            }
        }
        fields.push(Field { line: index + 1, name: name.to_string(), attributes });
    }
    fields
}

/// A FIELD THAT STOPS BEING WRITTEN MUST STILL DECODE AN IMAGE THAT LACKS IT.
#[test]
fn a_field_that_stops_being_written_must_still_decode_an_image_that_lacks_it() {
    let source = include_str!("../state.rs");
    assert!(
        source.len() > 100_000,
        "the source this guard reads came back implausibly short ({} bytes), so a clean result \
         means nothing was scanned",
        source.len()
    );

    let fields = fields_of(source, "pub(super) struct ShardState {");
    // A NON-VACUITY FLOOR, NOT A BUDGET, AND IT HAD TO COME DOWN. This refuses to run
    // unless the matcher finds a plausible number of fields, so a matcher that silently
    // stops matching cannot satisfy the property below over an empty set. It was 40.
    // `ShardState` has 33 fields now -- the map consolidation removed thirteen -- so the
    // floor was failing for the OPPOSITE of the reason it was written: the structure
    // shrank, not the matcher. Thirty still catches a matcher that finds nothing or a
    // handful, and is NOT a target: a floor on a shrinking structure has to track the
    // structure or it becomes a tripwire for progress.
    assert!(
        fields.len() >= 30,
        "only {} field(s) were found in the ShardState body, which has around 33 -- the
         field matcher is not matching, so every assertion below is over an almost empty
         set",
        fields.len()
    );

    // --- THE PROPERTY ---
    let unpaired: Vec<&Field> = fields
        .iter()
        .filter(|field| field.stops_being_written() && !field.decodes_a_missing_key())
        .collect();
    assert!(
        unpaired.is_empty(),
        "these fields stop being written WITHOUT making a missing key decode:\n{}\n\
         \n\
         Each one is a store that cannot be opened. `skip_serializing` means the key is not \
         written into a new image; without `default`, the first image written without it makes \
         every subsequent load of that image FAIL rather than decode an empty field. Add \
         `default` in the same change, never as a follow-up.\n\
         \n\
         `#[serde(skip)]` is NOT this problem and is not reported here: `skip_deserializing` \
         supplies `Default` itself.",
        unpaired
            .iter()
            .map(|field| format!("  state.rs:{} {}", field.line, field.name))
            .collect::<Vec<_>>()
            .join("\n")
    );

    // --- NON-VACUOUS. Without this the test passes while watching nothing, and it would keep
    // --- passing through the whole migration, which is when it matters most.
    let stopping: Vec<&Field> = fields.iter().filter(|f| f.stops_being_written()).collect();
    // THE COUNT FELL, AND HERE IS WHICH ONE WENT. It was two. `context_events_compat` carried
    // `#[serde(rename = "context_events", default, skip_serializing)]` and was retired once no
    // door could deliver its key -- see `the_event_series_on_the_owned_array`. One field is
    // left: the `skip_serializing_if = "Option::is_none"` deadline on the record wire form,
    // which is a CONDITIONAL skip and a different shape from a bare `skip_serializing`, always
    // paired with a default.
    //
    // SO THE GUARD STILL WATCHES SOMETHING, BUT ONLY JUST, and that is worth saying rather
    // than hiding behind a lowered number. At one field this floor has stopped being much of
    // a control. If that last field ever goes, RETIRE this guard rather than lowering the
    // floor to zero -- a control that cannot fail is worse than no control, because it reads
    // as coverage.
    assert!(
        stopping.len() >= 1,
        "no field stops being written: retire this guard rather than lowering the floor"
    );

    // --- THE BARE FIELDS, PRINTED RATHER THAN LISTED IN PROSE, so the warning cannot go stale.
    // --- Each needs `default` in the same change that stops writing it.
    let bare: Vec<&Field> = fields.iter().filter(|f| f.attributes.is_empty()).collect();
    println!(
        "  {} field(s) stop being written, all pairing with `default`",
        stopping.len()
    );
    println!(
        "  {} field(s) carry NO serde attribute, so each needs `default` added in the same \
         change that ever stops writing it:",
        bare.len()
    );
    for field in &bare {
        println!("    state.rs:{} {}", field.line, field.name);
    }

    // --- CONTROL ONE: the matcher finds a violation when there is one.
    let planted = "pub(super) struct Planted {\n    #[serde(skip_serializing)]\n    \
                   bad: u32,\n}\n";
    let caught = fields_of(planted, "pub(super) struct Planted {");
    assert_eq!(caught.len(), 1, "the field matcher missed a planted field");
    assert!(
        caught[0].stops_being_written() && !caught[0].decodes_a_missing_key(),
        "the pairing check accepted a planted field that stops being written with no `default`, \
         so it cannot tell the safe form from the unopenable one"
    );

    // --- CONTROL TWO: a prose mention is not an attribute. This is the audit error that
    // --- reported nine violations where there are none.
    let prose = "pub(super) struct Prose {\n    /// This field is `skip_serializing` on \
                 purpose.\n    fine: u32,\n}\n";
    let read = fields_of(prose, "pub(super) struct Prose {");
    assert_eq!(read.len(), 1, "the field matcher missed the field after a doc comment");
    assert!(
        !read[0].stops_being_written(),
        "a prose mention of the attribute was counted as the attribute"
    );

    // --- CONTROL THREE: BOTH WRONG MODELS AT ONCE, because this test has held each of them.
    //
    // A bare field must not inherit its neighbour's attribute -- the ADJACENCY error, which makes
    // this test lie in the SAFE direction by reporting a bare field as already paired.
    //
    // And a field whose attribute is separated from it by documentation must KEEP that attribute
    // -- the STRICT-CONTIGUITY error, which lies in the other direction by reporting a paired
    // field as bare. The first version of this walk did exactly that, and the control that was
    // meant to catch it asserted the wrong rule, so it confirmed the mistake instead. A control
    // that encodes the author's model tests the model against itself.
    //
    // In Rust a comment is not an item and does not end an attribute's reach. Only another item
    // does. Both halves are asserted here so neither model can come back.
    let fixture = concat!(
        "pub(super) struct Reach {\n",
        "    #[serde(default, skip_serializing)]\n",
        "    /// Documentation between the attribute and the field.\n",
        "    separated: u32,\n",
        "    /// Documentation and nothing else.\n",
        "    bare: u32,\n",
        "}\n",
    );
    let walked = fields_of(fixture, "pub(super) struct Reach {");
    assert_eq!(walked.len(), 2, "expected two fields, got {}", walked.len());
    assert!(
        walked[0].stops_being_written() && walked[0].decodes_a_missing_key(),
        "a field lost an attribute separated from it by documentation: {:?}. A comment is not an \
         item and does not end an attribute's reach, so this reports a paired field as bare",
        walked[0].attributes
    );
    assert!(
        walked[1].attributes.is_empty(),
        "a field with no attribute of its own inherited the previous field's: {:?}. That is how \
         an audit of this struct reported a bare field as already carrying `default`",
        walked[1].attributes
    );

    // --- CONTROL FOUR: A BARE `skip` IS NOT A VIOLATION AND MUST NOT BE FLAGGED.
    //
    // This closes the FALSE-POSITIVE direction, which is the one that would make the guard
    // unusable rather than merely weak. `#[serde(skip)]` is `skip_serializing` plus
    // `skip_deserializing`, and the deserializing half supplies `Default` itself -- measured:
    //
    //     skip             + missing key -> OK   (Default supplied automatically)
    //     skip_serializing + missing key -> ERR  missing field
    //     skip             writes: {"kept":0}
    //     skip_serializing writes: {"kept":0}
    //
    // BOTH WRITE THE SAME BYTES, which is why no check over a stored image can tell them apart
    // and why this guard reads the source. And a guard that flagged `skip` would refuse a shape
    // this tree uses on purpose -- it would fire on the first field of the next consolidation
    // tenant, which is exactly the kind of false positive that gets a guard deleted rather than
    // obeyed.
    let skipped = concat!(
        "pub(super) struct Skipped {\n",
        "    #[serde(skip)]\n",
        "    derived: u32,\n",
        "}\n",
    );
    let read = fields_of(skipped, "pub(super) struct Skipped {");
    assert_eq!(read.len(), 1, "the field matcher missed the skipped field");
    assert!(
        !read[0].attributes.is_empty(),
        "the attribute was not collected at all, so this control proves nothing about whether it \
         would be flagged"
    );
    assert!(
        !read[0].stops_being_written(),
        "a bare `#[serde(skip)]` was reported as stopping a field being written: {:?}. It does \
         stop it being written, and it also stops it being READ, which supplies `Default` and \
         makes a missing key decode. Flagging it would refuse a correct and intentional shape",
        read[0].attributes
    );
}
