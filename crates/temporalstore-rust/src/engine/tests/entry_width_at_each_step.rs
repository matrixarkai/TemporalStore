//! WHAT THE PAGE INDEX ENTRY WEIGHS AT EACH STEP TOWARD TWENTY-FOUR, COMPOSED RATHER THAN LONE.
//!
//! The target shape for this entry is twenty-four bytes carrying NO IDENTITY OF ITS OWN: a flag
//! group, a model spelling, and an address, with the identity supplied by the position the entry
//! is held at. This file measures what each step toward that width actually costs, because every
//! per-field reading of this structure in this campaign has been wrong in the same direction --
//! `size_of` steps at a word, so one to seven bytes of field saving moves it by nothing, and two
//! savings that are each worth nothing can compose to a whole word.
//!
//! # WHY A MIRROR AND NOT THE LIVE TYPE
//!
//! Each step is a STRUCT DECLARED HERE with the field set that step would leave behind. The live
//! entry cannot be measured at a width it does not have yet, and a prose decomposition is the one
//! form of this that nothing can fail. What makes these mirrors evidence rather than arithmetic is
//! the pair of controls below: the step-zero mirror must measure the LIVE entry exactly, and a
//! deliberately wrong mirror must NOT -- so the comparison is shown to discriminate before any
//! step is read off it.
//!
//! # ON THE SHAPE OF THE PINS
//!
//! Each width is pinned with `==` and each alignment with its own `==`. No neighbouring `!=` is
//! written beside a width: `size_of` is always a multiple of `align_of`, so a `== N` entails every
//! neighbour inequality and the neighbour asserts nothing. The alignment pin is NOT dominated by
//! the width pin, because a width admits every alignment that divides it, which is exactly why it
//! is written separately.
//!
//! The `!=` in the NEGATIVE CONTROL is a different claim and not that pattern: it is asserted
//! about a DIFFERENT TYPE, to prove the instrument can refuse, and nothing else here entails it.

use std::mem::{align_of, size_of};
use std::sync::Arc;

use crate::block_store::ElementEntry;
use crate::engine::state::BlockIndex;
use crate::engine::storage_bucket_internals::StoredModelKind;
use crate::index_log::IndexItemKind;

/// STEP ZERO: the field set the live entry has, mirrored field for field and in declaration order.
///
/// Seven fields, forty bytes of field in forty. Held as a mirror so the later steps are the same
/// declaration with terms taken away, which is what makes the differences between them readable as
/// the cost of one step rather than of a rewrite.
#[allow(dead_code)]
struct StepZeroTheLiveEntry {
    kind: IndexItemKind,
    routing_bucket: u32,
    object_key: Arc<str>,
    model_id: StoredModelKind,
    address: ElementEntry,
    dirty: bool,
    deleted: bool,
}

/// THE POSITIVE CONTROL: the mirror measures the live entry, or every number below is about a
/// structure this tree does not contain.
///
/// Both halves are asserted, because a width alone is not an identity -- two different field sets
/// can measure the same -- and because the alignment is the half that decides whether a later
/// narrowing leaves the eight-aligned group or stays inside it.
///
/// AND WHAT THIS CONTROL CANNOT SEE, stated because it was MEASURED and not assumed. Taking one
/// bool off the mirror leaves thirty-nine bytes of field, which rounds back to forty, and this
/// control stays green through it. It discriminates at WORD granularity and no finer: dropping the
/// identity reddens it, dropping a flag does not. So it establishes that the mirror is the live
/// entry to within a word, which is the granularity every number in this file is read at, and it
/// is not evidence that the field LIST matches term for term.
const _: () = assert!(size_of::<StepZeroTheLiveEntry>() == size_of::<BlockIndex>());
const _: () = assert!(align_of::<StepZeroTheLiveEntry>() == align_of::<BlockIndex>());

/// THE NEGATIVE CONTROL: the same mirror with one word added, which must NOT measure the live
/// entry.
///
/// Without this, a positive control that passed could be passing because both sides were being
/// computed from the same stale thing, or because the comparison was satisfied by something other
/// than the field set. This shows the comparison REFUSES when the field set genuinely differs,
/// which is what makes its passing informative.
#[allow(dead_code)]
struct StepZeroWithOneWordTooMany {
    kind: IndexItemKind,
    routing_bucket: u32,
    object_key: Arc<str>,
    model_id: StoredModelKind,
    address: ElementEntry,
    dirty: bool,
    deleted: bool,
    one_word_too_many: u64,
}

const _: () = assert!(size_of::<StepZeroWithOneWordTooMany>() != size_of::<BlockIndex>());

/// STEP ONE: THE IDENTITY LEAVES THE ENTRY, and that is the whole of the width step.
///
/// Six fields, twenty-four bytes of field in twenty-four. The shared pointer to the object key is
/// a sixteen-byte fat pointer, so this is a TWO-WORD step rather than one word and some rounding,
/// and twenty-four is a multiple of the alignment -- so it lands exactly and leaves no slack for
/// the next field to be added into free of charge.
#[allow(dead_code)]
struct StepOneTheIdentityLeaves {
    kind: IndexItemKind,
    routing_bucket: u32,
    model_id: StoredModelKind,
    address: ElementEntry,
    dirty: bool,
    deleted: bool,
}

const _: () = assert!(size_of::<StepOneTheIdentityLeaves>() == 24);
const _: () = assert!(align_of::<StepOneTheIdentityLeaves>() == 8);

/// STEP TWO: THE SAME FIELD SET, PACKED -- AND IT BUYS NOTHING.
///
/// Twenty-four is already a multiple of eight and step one already holds twenty-four bytes of
/// field, so there is no slack for packing to reclaim: alignment one moves the ALIGNMENT and
/// leaves the WIDTH exactly where it was. The width pin below is the argument against spending
/// `repr(packed)` here at all, since packing a field of this entry would cost every borrow of it.
#[allow(dead_code)]
#[repr(C, packed)]
struct StepTwoTheSameFieldSetPacked {
    kind: IndexItemKind,
    routing_bucket: u32,
    model_id: StoredModelKind,
    address: ElementEntry,
    dirty: bool,
    deleted: bool,
}

const _: () = assert!(size_of::<StepTwoTheSameFieldSetPacked>() == 24);
const _: () = assert!(align_of::<StepTwoTheSameFieldSetPacked>() == 1);

/// STEP TWO-A: THE SAME FIELD SET UNDER A DECLARED ORDER AND NOT PACKED -- FORTY, NOT TWENTY-FOUR.
///
/// This is the row that decides whether a stable field order is affordable, and it was found by
/// MUTATION rather than by reasoning: taking `packed` off step two was expected to move only the
/// alignment, and it moved the width too. A declared order cannot reorder the fields to fill its
/// own padding, so the one-byte terms sit either side of an eight-aligned address and each buys a
/// hole -- sixteen bytes of them, which is the entire step that removing the identity won.
///
/// So the two reprs are NOT a choice between the same width at two alignments. Step one at
/// alignment eight and step two at alignment one both measure twenty-four, and a declared order
/// without packing measures forty. If a stable field order is ever wanted here it has to be
/// `repr(C, packed)` and pay the borrow restrictions; `repr(C)` alone gives up the width step.
#[allow(dead_code)]
#[repr(C)]
struct StepTwoAUnderADeclaredOrderUnpacked {
    kind: IndexItemKind,
    routing_bucket: u32,
    model_id: StoredModelKind,
    address: ElementEntry,
    dirty: bool,
    deleted: bool,
}

const _: () = assert!(size_of::<StepTwoAUnderADeclaredOrderUnpacked>() == 40);
const _: () = assert!(align_of::<StepTwoAUnderADeclaredOrderUnpacked>() == 8);

/// STEP THREE: THE THREE ONE-BYTE TERMS BECOME ONE FLAG GROUP -- AND IT ALSO BUYS NOTHING.
///
/// The kind, the dirty bit and the deleted bit are three separate bytes in step one. Folded into
/// one byte this is twenty-two bytes of field, and twenty-two rounds back to twenty-four. This is
/// the measured form of the rule that a saving smaller than a word is a saving of nothing: the
/// step is real, the width does not move, and a per-field reading would have reported two bytes.
#[allow(dead_code)]
struct StepThreeTheFlagsFoldIntoOneByte {
    flags: u8,
    routing_bucket: u32,
    model_id: StoredModelKind,
    address: ElementEntry,
}

const _: () = assert!(size_of::<StepThreeTheFlagsFoldIntoOneByte>() == 24);
const _: () = assert!(align_of::<StepThreeTheFlagsFoldIntoOneByte>() == 8);

/// STEP FOUR: THE ADDRESS BECOMES ONE WORD, ON TOP OF STEP ONE AND NOT INSTEAD OF IT.
///
/// Sixteen bytes of field in sixteen. Composed with step one this is a second whole word, which is
/// why it is measured here rather than inferred: on the forty-byte entry the same narrowing was
/// refused on measurement, because the three terms that travel with the address followed it into
/// the tail and rounded the structure straight back. With the identity already gone there is no
/// tail for them to round into, and the step lands.
#[allow(dead_code)]
struct StepFourTheAddressBecomesOneWord {
    kind: IndexItemKind,
    routing_bucket: u32,
    model_id: StoredModelKind,
    address: u64,
    dirty: bool,
    deleted: bool,
}

const _: () = assert!(size_of::<StepFourTheAddressBecomesOneWord>() == 16);
const _: () = assert!(align_of::<StepFourTheAddressBecomesOneWord>() == 8);

/// STEPS THREE AND FOUR COMPOSED: fourteen bytes of field, and still sixteen.
///
/// The flag fold is worth nothing on its own and nothing here either -- the pair measures exactly
/// what step four measures alone. Recorded because the mirror case is the trap: two steps each
/// worth nothing have composed to a whole word elsewhere in this structure, and the only way to
/// know which way a pair goes is to declare the pair.
#[allow(dead_code)]
struct StepsThreeAndFourComposed {
    flags: u8,
    routing_bucket: u32,
    model_id: StoredModelKind,
    address: u64,
}

const _: () = assert!(size_of::<StepsThreeAndFourComposed>() == 16);
const _: () = assert!(align_of::<StepsThreeAndFourComposed>() == 8);

/// WHAT THE HOLDER PAYS, which is the number a footprint claim has to be made in.
///
/// The entry is not held alone. The list arm of `BlockIndexMap` holds `(u64, BlockIndex)` per
/// page, so the handle travels with every entry and the element is what a page actually costs
/// resident. Stated here beside the entry widths so that a width step is never reported as a
/// footprint step without the element it lands in.
const _: () = assert!(size_of::<(u64, BlockIndex)>() == 48);
const _: () = assert!(size_of::<(u64, StepOneTheIdentityLeaves)>() == 32);

/// THE PART OF THE IDENTITY THAT DOES NOT LEAVE THE PROCESS WHEN IT LEAVES THE ENTRY.
///
/// A shared pointer to the object key is sixteen bytes wherever it is held. Taken off the entry it
/// has to be held SOMEWHERE the position of the entry can be resolved from, and if that somewhere
/// is one slot per page then the sixteen bytes have moved rather than gone. This is the arithmetic
/// a re-keying has to clear, and it is pinned here so the two numbers are read side by side.
const _: () = assert!(size_of::<Arc<str>>() == 16);

#[test]
fn the_width_of_the_entry_at_each_step_toward_the_target_shape() {
    let rows: Vec<(&str, usize, usize, usize)> = vec![
        (
            "step 0  the live entry, seven fields",
            40,
            size_of::<StepZeroTheLiveEntry>(),
            align_of::<StepZeroTheLiveEntry>(),
        ),
        (
            "step 1  the identity leaves",
            24,
            size_of::<StepOneTheIdentityLeaves>(),
            align_of::<StepOneTheIdentityLeaves>(),
        ),
        (
            "step 2  step 1, packed",
            24,
            size_of::<StepTwoTheSameFieldSetPacked>(),
            align_of::<StepTwoTheSameFieldSetPacked>(),
        ),
        (
            "step 2a step 1, declared order, unpacked",
            24,
            size_of::<StepTwoAUnderADeclaredOrderUnpacked>(),
            align_of::<StepTwoAUnderADeclaredOrderUnpacked>(),
        ),
        (
            "step 3  step 1, flags folded",
            22,
            size_of::<StepThreeTheFlagsFoldIntoOneByte>(),
            align_of::<StepThreeTheFlagsFoldIntoOneByte>(),
        ),
        (
            "step 4  step 1, address one word",
            16,
            size_of::<StepFourTheAddressBecomesOneWord>(),
            align_of::<StepFourTheAddressBecomesOneWord>(),
        ),
        (
            "steps 3+4 composed",
            14,
            size_of::<StepsThreeAndFourComposed>(),
            align_of::<StepsThreeAndFourComposed>(),
        ),
    ];

    for (name, field_bytes, width, alignment) in &rows {
        println!(
            "{name:36}  field {field_bytes:>3} B   width {width:>3} B   align {alignment} B   slack {} B",
            width - field_bytes
        );
    }

    // THE IDENTITY OF THE SUBJECT BEFORE ANY CONTENT CHECK: the mirror is the live entry, or the
    // rows above describe a structure this tree does not contain. Asserted here as well as in the
    // const position so that a reader of the printed table has the control in the same output.
    assert_eq!(
        size_of::<StepZeroTheLiveEntry>(),
        size_of::<BlockIndex>(),
        "the step-zero mirror must measure the live entry"
    );
    assert_eq!(
        align_of::<StepZeroTheLiveEntry>(),
        align_of::<BlockIndex>(),
        "the step-zero mirror must align like the live entry"
    );

    // EVERY STEP IS COMPOSED ON STEP ONE, so each row is the cost of that step ON TOP of the
    // identity leaving, never its cost against the live entry alone.
    assert_eq!(size_of::<StepOneTheIdentityLeaves>(), 24);
    assert_eq!(size_of::<StepTwoTheSameFieldSetPacked>(), 24);
    assert_eq!(size_of::<StepTwoAUnderADeclaredOrderUnpacked>(), 40);
    assert_eq!(size_of::<StepThreeTheFlagsFoldIntoOneByte>(), 24);
    assert_eq!(size_of::<StepFourTheAddressBecomesOneWord>(), 16);
    assert_eq!(size_of::<StepsThreeAndFourComposed>(), 16);
}
