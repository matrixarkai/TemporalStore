// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! WHAT THE PAGE INDEX ENTRY WEIGHS AT EACH STEP TOWARD TWENTY-FOUR, COMPOSED RATHER THAN LONE.
//!
//! The target shape for this entry is twenty-four bytes carrying NO IDENTITY OF ITS OWN: a flag
//! group, a model spelling, and an address, with the identity supplied by the position the entry
//! is held at. This file measures what each step toward that width actually costs, because every
//! per-field reading of this structure in this campaign has been wrong in the same direction --
//! `size_of` steps at a word, so one to seven bytes of field saving moves it by nothing, and two
//! savings that are each worth nothing can compose to a whole word.
//!
//! # EVERY MIRROR HERE DECLARED TWO FIELDS THE LIVE ENTRY NO LONGER HAS
//!
//! They carried `kind: IndexItemKind` and `routing_bucket: u32` -- five bytes between them -- and
//! the commit that removed both from `BlockIndex` did not remove them from here. THE CONTROL WENT
//! ON PASSING, which is the whole reason this is written down rather than quietly fixed: the live
//! entry went from forty bytes of field to thirty-five, forty and thirty-five BOTH ROUND TO FORTY,
//! and the step-zero mirror kept measuring the live entry while describing a field set the live
//! entry had lost. The control's own doc says it discriminates at word granularity and no finer;
//! this is that limit being reached in practice rather than in a note.
//!
//! WHAT THAT HID WAS NOT ONE NUMBER BUT THREE CONCLUSIONS, each now restated from a measurement:
//!
//!   * step one had "twenty-four bytes of field in twenty-four ... no slack for packing to
//!     reclaim". It is NINETEEN bytes of field in twenty-four, with FIVE bytes of slack;
//!   * step two said packing "buys nothing". Packed, the same field set measures NINETEEN -- the
//!     step is worth five bytes, not zero;
//!   * step two-A said a declared order "gives up the width step" at forty. It measures
//!     THIRTY-TWO: the penalty is one word, not two.
//!
//! The widths of steps zero, one, three and four did not move at all, which is why the staleness
//! was invisible: the five bytes the retired fields contributed were absorbed by the rounding in
//! every one of those rows.
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
//! AND THE STEP-ZERO CONTROL NOW READS OFFSETS AND NOT ONLY WIDTHS, for the reason the staleness
//! above gives: a width is a sum, a sum hides its terms, and two different field lists round to
//! the same width. An offset names one term and where it sits, so a mirror carrying a field the
//! live entry does not have cannot place the rest where the live entry places them. The field sum
//! is pinned beside the widths as well, but it is a different claim -- it moves when a field's own
//! WIDTH moves, not when the list does.
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

/// THE FIELD SUM OF THE LIVE ENTRY, built from the types its fields are declared with.
///
/// Spelled as types rather than as a literal so that a field whose own width moves elsewhere fails
/// here instead of leaving this file describing the old one. Every row below is this sum with
/// terms taken away, which is what makes the differences readable as the cost of one step.
const LIVE_FIELD_SUM: usize = size_of::<Arc<str>>()        // object_key
    + size_of::<StoredModelKind>()                         // model_id
    + size_of::<ElementEntry>()                            // address
    + size_of::<bool>()                                    // dirty
    + size_of::<bool>(); // deleted
const _: () = assert!(LIVE_FIELD_SUM == 35);

/// STEP ZERO: the field set the live entry has, mirrored field for field and in declaration order.
///
/// FIVE fields, thirty-five bytes of field in forty, and so five bytes of slack. It read "Seven
/// fields, forty bytes of field in forty" while two of those seven had already left the live
/// entry -- see the module docs for what that hid.
#[allow(dead_code)]
struct StepZeroTheLiveEntry {
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
/// AND NEITHER OF THEM CAN SEE A FIELD LIST, which is the whole of what went wrong here. Taking
/// one bool off the mirror leaves thirty-four bytes of field, which rounds back to forty, and both
/// assertions stay green; ADDING the five bytes of `kind` and `routing_bucket` back also leaves
/// forty, and both stay green through that too. They discriminate at WORD granularity and no
/// finer, and that is not a theory -- five bytes of field left this mirror's subject and these two
/// assertions did not notice for a whole commit.
const _: () = assert!(size_of::<StepZeroTheLiveEntry>() == size_of::<BlockIndex>());
const _: () = assert!(align_of::<StepZeroTheLiveEntry>() == align_of::<BlockIndex>());
const _: () = assert!(size_of::<StepZeroTheLiveEntry>() == 40);

/// THE CONTROL THAT CAN SEE THE FIELD LIST: every field at the same offset in the mirror as in the
/// live entry.
///
/// A width is a sum and a sum hides its terms. An OFFSET names one term and where it sits, so a
/// mirror carrying a field the live entry does not have cannot place the remaining fields where
/// the live entry places them -- the extra term pushes at least one of them. That is what the
/// width pins above could not do: with `kind` and `routing_bucket` still declared here, this
/// mirror put the model spelling at thirty-seven while the live entry puts it at thirty-two, and
/// an offset pin would have refused the commit that retired them.
///
/// EXHAUSTIVE OVER THE LIVE ENTRY'S FIELDS, five of five. A field ADDED to `BlockIndex` does not
/// fail here -- nothing in a list of offsets is forced to mention it -- so this is not a
/// substitute for the destructuring guards elsewhere; it is the check that the terms named here
/// are where the live type puts them.
const _: () = assert!(
    std::mem::offset_of!(StepZeroTheLiveEntry, object_key)
        == std::mem::offset_of!(BlockIndex, object_key)
);
const _: () = assert!(
    std::mem::offset_of!(StepZeroTheLiveEntry, model_id)
        == std::mem::offset_of!(BlockIndex, model_id)
);
const _: () = assert!(
    std::mem::offset_of!(StepZeroTheLiveEntry, address) == std::mem::offset_of!(BlockIndex, address)
);
const _: () = assert!(
    std::mem::offset_of!(StepZeroTheLiveEntry, dirty) == std::mem::offset_of!(BlockIndex, dirty)
);
const _: () = assert!(
    std::mem::offset_of!(StepZeroTheLiveEntry, deleted) == std::mem::offset_of!(BlockIndex, deleted)
);

/// AND THE FIELD SUM, which is the half that moves when a field's own WIDTH moves rather than when
/// the list does. Built from the live entry's field types, so a term that widens elsewhere fails
/// here instead of leaving every decomposition below describing the old one.
const _: () = assert!(LIVE_FIELD_SUM == 35);
const _: () = assert!(size_of::<StepZeroTheLiveEntry>() - LIVE_FIELD_SUM == 5);

/// THE NEGATIVE CONTROL: the same mirror with one word added, which must NOT measure the live
/// entry.
///
/// Without this, a positive control that passed could be passing because both sides were being
/// computed from the same stale thing, or because the comparison was satisfied by something other
/// than the field set. This shows the comparison REFUSES when the field set genuinely differs,
/// which is what makes its passing informative.
///
/// AND IT REFUSES BY A WHOLE WORD, which is the limit of what it proves. Forty-three bytes of
/// field measures forty-eight, so this control is decisive against a mirror that is a WORD wrong
/// and silent against one that is a BYTE wrong -- which is exactly the error that stood here.
#[allow(dead_code)]
struct StepZeroWithOneWordTooMany {
    object_key: Arc<str>,
    model_id: StoredModelKind,
    address: ElementEntry,
    dirty: bool,
    deleted: bool,
    one_word_too_many: u64,
}

const _: () = assert!(size_of::<StepZeroWithOneWordTooMany>() != size_of::<BlockIndex>());
const _: () = assert!(size_of::<StepZeroWithOneWordTooMany>() == 48);

/// STEP ONE: THE IDENTITY LEAVES THE ENTRY, and that is the whole of the width step.
///
/// Four fields, NINETEEN bytes of field in twenty-four. The shared pointer to the object key is a
/// sixteen-byte fat pointer, so this is a TWO-WORD step rather than one word and some rounding.
///
/// AND IT LEAVES FIVE BYTES OF SLACK, WHICH INVERTS THE RULE THIS ENTRY IS GOVERNED BY. This read
/// "twenty-four bytes of field in twenty-four ... so it lands exactly and leaves no slack for the
/// next field to be added into free of charge" -- true of a mirror carrying five bytes the live
/// entry had already lost, and false of the live field set. Nineteen is not a multiple of eight,
/// so an eight-aligned struct holding it has five bytes standing empty BY CONSTRUCTION. At forty
/// the entry has five bytes of slack as well; what changes at twenty-four is only that a field up
/// to five bytes wide is free in both, and that has to be re-priced rather than inherited from the
/// decomposition that was written when the entry was believed to be exactly full.
#[allow(dead_code)]
struct StepOneTheIdentityLeaves {
    model_id: StoredModelKind,
    address: ElementEntry,
    dirty: bool,
    deleted: bool,
}

const STEP_ONE_FIELD_SUM: usize = LIVE_FIELD_SUM - size_of::<Arc<str>>();
const _: () = assert!(STEP_ONE_FIELD_SUM == 19);
const _: () = assert!(size_of::<StepOneTheIdentityLeaves>() == 24);
const _: () = assert!(align_of::<StepOneTheIdentityLeaves>() == 8);
const _: () = assert!(size_of::<StepOneTheIdentityLeaves>() - STEP_ONE_FIELD_SUM == 5);

/// STEP TWO: THE SAME FIELD SET, PACKED -- AND IT BUYS FIVE BYTES, NOT NOTHING.
///
/// NINETEEN, MEASURED. This said "Twenty-four is already a multiple of eight and step one already
/// holds twenty-four bytes of field, so there is no slack for packing to reclaim: alignment one
/// moves the ALIGNMENT and leaves the WIDTH exactly where it was." Both premises belonged to a
/// mirror with five extra bytes of field. Step one holds nineteen, so there IS slack, and packing
/// takes the whole of it: twenty-four to nineteen at alignment one.
///
/// THAT DOES NOT MAKE IT WORTH SPENDING. Packing a field of this entry costs every borrow of it --
/// a packed field cannot be referenced -- and the entry is borrowed at every read site in the
/// crate. What the number changes is the ARGUMENT: the case against packing here is the borrow
/// restriction, and it can no longer be waved away as buying nothing.
#[allow(dead_code)]
#[repr(C, packed)]
struct StepTwoTheSameFieldSetPacked {
    model_id: StoredModelKind,
    address: ElementEntry,
    dirty: bool,
    deleted: bool,
}

const _: () = assert!(size_of::<StepTwoTheSameFieldSetPacked>() == 19);
const _: () = assert!(align_of::<StepTwoTheSameFieldSetPacked>() == 1);
const _: () = assert!(size_of::<StepTwoTheSameFieldSetPacked>() == STEP_ONE_FIELD_SUM);

/// STEP TWO-A: THE SAME FIELD SET UNDER A DECLARED ORDER AND NOT PACKED -- THIRTY-TWO.
///
/// This is the row that decides whether a stable field order is affordable, and it was found by
/// MUTATION rather than by reasoning: taking `packed` off step two was expected to move only the
/// alignment, and it moved the width too. A declared order cannot reorder the fields to fill its
/// own padding, so the one-byte model spelling sits in front of an eight-aligned address and buys
/// seven bytes of hole, and the two flags buy six more at the tail.
///
/// THE PENALTY IS ONE WORD AND NOT TWO. This said forty, which was measured on a mirror carrying
/// `kind` and `routing_bucket`: with those gone it is thirty-two. So a declared order costs eight
/// bytes over step one rather than the entire step the identity's removal won, and the conclusion
/// it was written to support -- that `repr(C)` alone gives the width step up -- is wrong as
/// stated. `repr(C, packed)` is still the only form that keeps the width at or below step one.
#[allow(dead_code)]
#[repr(C)]
struct StepTwoAUnderADeclaredOrderUnpacked {
    model_id: StoredModelKind,
    address: ElementEntry,
    dirty: bool,
    deleted: bool,
}

const _: () = assert!(size_of::<StepTwoAUnderADeclaredOrderUnpacked>() == 32);
const _: () = assert!(align_of::<StepTwoAUnderADeclaredOrderUnpacked>() == 8);
const _: () =
    assert!(size_of::<StepTwoAUnderADeclaredOrderUnpacked>() - size_of::<StepOneTheIdentityLeaves>() == 8);

/// STEP THREE: THE TWO FLAGS BECOME ONE FLAG GROUP -- AND IT BUYS NOTHING.
///
/// The dirty bit and the deleted bit are two separate bytes in step one. Folded into one byte this
/// is EIGHTEEN bytes of field, and eighteen rounds back to twenty-four. This is the measured form
/// of the rule that a saving smaller than the slack is a saving of nothing: the step is real, the
/// width does not move, and a per-field reading would have reported one byte.
///
/// THE FOLD IS NOW TWO TERMS AND NOT THREE. It folded `kind`, `dirty` and `deleted`, and `kind`
/// has left the entry -- so the group this describes is the two flags that remain, and the field
/// sum it reaches is eighteen rather than twenty-two.
#[allow(dead_code)]
struct StepThreeTheFlagsFoldIntoOneByte {
    flags: u8,
    model_id: StoredModelKind,
    address: ElementEntry,
}

const STEP_THREE_FIELD_SUM: usize =
    size_of::<u8>() + size_of::<StoredModelKind>() + size_of::<ElementEntry>();
const _: () = assert!(STEP_THREE_FIELD_SUM == 18);
const _: () = assert!(size_of::<StepThreeTheFlagsFoldIntoOneByte>() == 24);
const _: () = assert!(align_of::<StepThreeTheFlagsFoldIntoOneByte>() == 8);

/// STEP FOUR: THE ADDRESS BECOMES ONE WORD, ON TOP OF STEP ONE AND NOT INSTEAD OF IT.
///
/// ELEVEN bytes of field in sixteen. Composed with step one this is a second whole word, which is
/// why it is measured here rather than inferred: on the forty-byte entry the same narrowing was
/// refused on measurement, because the three terms that travel with the address followed it into
/// the tail and rounded the structure straight back. With the identity already gone there is no
/// tail for them to round into, and the step lands.
///
/// AND IT LANDS AT SIXTEEN WITH FIVE BYTES OF SLACK, not at a full sixteen of field. That matters
/// for the same reason step one's slack does: the end of this leg is not an entry packed exactly,
/// it is an entry with room in it.
#[allow(dead_code)]
struct StepFourTheAddressBecomesOneWord {
    model_id: StoredModelKind,
    address: u64,
    dirty: bool,
    deleted: bool,
}

const STEP_FOUR_FIELD_SUM: usize = STEP_ONE_FIELD_SUM - size_of::<ElementEntry>() + size_of::<u64>();
const _: () = assert!(STEP_FOUR_FIELD_SUM == 11);
const _: () = assert!(size_of::<StepFourTheAddressBecomesOneWord>() == 16);
const _: () = assert!(align_of::<StepFourTheAddressBecomesOneWord>() == 8);
const _: () = assert!(size_of::<StepFourTheAddressBecomesOneWord>() - STEP_FOUR_FIELD_SUM == 5);

/// STEPS THREE AND FOUR COMPOSED: ten bytes of field, and still sixteen.
///
/// The flag fold is worth nothing on its own and nothing here either -- the pair measures exactly
/// what step four measures alone. Recorded because the mirror case is the trap: two steps each
/// worth nothing have composed to a whole word elsewhere in this structure, and the only way to
/// know which way a pair goes is to declare the pair.
#[allow(dead_code)]
struct StepsThreeAndFourComposed {
    flags: u8,
    model_id: StoredModelKind,
    address: u64,
}

const _: () = assert!(size_of::<StepsThreeAndFourComposed>() == 16);
const _: () = assert!(align_of::<StepsThreeAndFourComposed>() == 8);
const _: () =
    assert!(size_of::<StepsThreeAndFourComposed>() == size_of::<StepFourTheAddressBecomesOneWord>());

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
            "step 0  the live entry, five fields",
            LIVE_FIELD_SUM,
            size_of::<StepZeroTheLiveEntry>(),
            align_of::<StepZeroTheLiveEntry>(),
        ),
        (
            "step 1  the identity leaves",
            STEP_ONE_FIELD_SUM,
            size_of::<StepOneTheIdentityLeaves>(),
            align_of::<StepOneTheIdentityLeaves>(),
        ),
        (
            "step 2  step 1, packed",
            STEP_ONE_FIELD_SUM,
            size_of::<StepTwoTheSameFieldSetPacked>(),
            align_of::<StepTwoTheSameFieldSetPacked>(),
        ),
        (
            "step 2a step 1, declared order, unpacked",
            STEP_ONE_FIELD_SUM,
            size_of::<StepTwoAUnderADeclaredOrderUnpacked>(),
            align_of::<StepTwoAUnderADeclaredOrderUnpacked>(),
        ),
        (
            "step 3  step 1, flags folded",
            STEP_THREE_FIELD_SUM,
            size_of::<StepThreeTheFlagsFoldIntoOneByte>(),
            align_of::<StepThreeTheFlagsFoldIntoOneByte>(),
        ),
        (
            "step 4  step 1, address one word",
            STEP_FOUR_FIELD_SUM,
            size_of::<StepFourTheAddressBecomesOneWord>(),
            align_of::<StepFourTheAddressBecomesOneWord>(),
        ),
        (
            "steps 3+4 composed",
            10,
            size_of::<StepsThreeAndFourComposed>(),
            align_of::<StepsThreeAndFourComposed>(),
        ),
    ];

    for (name, field_bytes, width, alignment) in &rows {
        println!(
            "{name:40}  field {field_bytes:>3} B   width {width:>3} B   align {alignment} B   slack {} B",
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

    // AND THE SUM, which is the half that caught two retired fields still being declared here.
    // Read off the live entry's own field TYPES, so a mirror that stops describing that field set
    // fails even when it rounds to the same width.
    assert_eq!(
        35, LIVE_FIELD_SUM,
        "the live entry's field sum moved; the rows above are decompositions of the old one"
    );
    assert_eq!(
        5,
        size_of::<BlockIndex>() - LIVE_FIELD_SUM,
        "the live entry's slack moved, so what the next field added here costs has moved with it"
    );

    // EVERY STEP IS COMPOSED ON STEP ONE, so each row is the cost of that step ON TOP of the
    // identity leaving, never its cost against the live entry alone.
    assert_eq!(size_of::<StepOneTheIdentityLeaves>(), 24);
    assert_eq!(size_of::<StepTwoTheSameFieldSetPacked>(), 19);
    assert_eq!(size_of::<StepTwoAUnderADeclaredOrderUnpacked>(), 32);
    assert_eq!(size_of::<StepThreeTheFlagsFoldIntoOneByte>(), 24);
    assert_eq!(size_of::<StepFourTheAddressBecomesOneWord>(), 16);
    assert_eq!(size_of::<StepsThreeAndFourComposed>(), 16);
}
