// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! The slab manifest, binary shape.
//!
//! The manifest is the record of which slabs a block store holds and what state each is in. It
//! was a JSON document; this is the protobuf form of the same content, and this module is its
//! codec: [`load_slab_manifest_at`](super::slab_manifest::load_slab_manifest_at) reads either
//! form, chosen by [`is_wire_manifest`], so a store written by an older build still opens.
//!
//! # WHY A CONTAINER BYTE IN FRONT OF THE FRAME
//!
//! A framed record starts with `varint32(payload_len)`, and a JSON document starts with `{`, which
//! is `0x7B` -- a valid length of 123. Deciding the shape from the first byte of the FRAME would
//! read a manifest whose payload is exactly 123 bytes as JSON and reject it as corrupt, and
//! nothing about it would look different from any other manifest until the day a store reached
//! that size. So one byte in front of the frame decides the shape, the same convention the
//! snapshot manifest uses (`0x01`: not a brace, not whitespace, not valid JSON anywhere), rather
//! than a second spelling for the same idea. `a_frame_of_123_bytes_starts_with_an_open_brace`
//! builds that exact manifest.
//!
//! The frame behind the byte carries a length and a crc32c, which the JSON form never had: a
//! flipped bit in a raw protobuf body can decode to a different valid value, where JSON mostly
//! failed to parse.
//!
//! # WHAT AN OLDER BUILD DOES WITH THIS
//!
//! `BlockStore::open` treats any manifest that fails to load as "rebuild it from the slabs", so a
//! build that cannot read this form pays one cold-start rebuild and rewrites the file in its own
//! shape; it does not fail to open. The cost of a rollback is therefore time, not availability.
//!
//! # THE WRITER
//!
//! `persist_slab_manifest` encodes with [`encode_slab_manifest`] and writes the result in a single
//! `write_all`. It landed after the reader, so no build that writes this form ever existed without
//! one that reads it. The JSON arm of the reader stays for as long as any build that wrote a JSON
//! manifest could have left one in a live store, which this tree cannot rule out.
//!
//! Field numbers are fixed by what is stored on disk and must not be renumbered or reused.

use prost::Message;

use super::{BlockStoreSlabDescriptor, BlockStoreSlabManifest, BlockStoreSlabState};
use crate::record_framing::{decode_framed_at, encode_framed};

/// The first byte of a binary manifest. A JSON document opens with `{` or JSON whitespace, and
/// `0x01` is neither, so one byte decides the shape.
pub(super) const SLAB_MANIFEST_CONTAINER_PROTOBUF: u8 = 0x01;

/// Slab lifecycle state as stored. `Unspecified` is "a build that did not say" and is refused by
/// name rather than defaulted: a manifest written by a newer build with a state this one does not
/// know must not be read as the first state it happens to number as zero.
#[derive(Clone, Copy, Debug, PartialEq, Eq, ::prost::Enumeration)]
#[repr(i32)]
pub(super) enum WireSlabState {
    Unspecified = 0,
    Active = 1,
    Sealed = 2,
    DelayedDestroy = 3,
    Purged = 4,
}

/// One slab's stored descriptor. Optional fields keep their presence on the wire, because
/// "unknown" and "zero" are different answers for a timestamp or a block id.
#[derive(Clone, PartialEq, Message)]
pub(super) struct WireSlabDescriptor {
    #[prost(uint64, tag = "1")]
    pub(super) stored_slab_id: u64,
    #[prost(uint64, tag = "2")]
    pub(super) block_slab_id: u64,
    #[prost(enumeration = "WireSlabState", tag = "3")]
    pub(super) state: i32,
    #[prost(uint64, tag = "4")]
    pub(super) physical_bytes: u64,
    #[prost(uint64, tag = "5")]
    pub(super) logical_bytes: u64,
    #[prost(uint64, optional, tag = "6")]
    pub(super) created_unix_ms: Option<u64>,
    #[prost(uint64, optional, tag = "7")]
    pub(super) updated_unix_ms: Option<u64>,
    #[prost(uint64, optional, tag = "8")]
    pub(super) first_block_id: Option<u64>,
    #[prost(uint64, optional, tag = "9")]
    pub(super) last_block_id: Option<u64>,
    #[prost(uint64, tag = "10")]
    pub(super) readable_prefix_physical_bytes: u64,
    #[prost(uint64, optional, tag = "11")]
    pub(super) verified_source_mtime_unix_ms: Option<u64>,
    #[prost(bool, tag = "12")]
    pub(super) has_corruption: bool,
    #[prost(uint64, optional, tag = "13")]
    pub(super) first_error_offset: Option<u64>,
    #[prost(string, optional, tag = "14")]
    pub(super) first_error: Option<String>,
}

/// The whole manifest: a version and every slab descriptor.
#[derive(Clone, PartialEq, Message)]
pub(super) struct WireSlabManifest {
    #[prost(uint32, tag = "1")]
    pub(super) version: u32,
    #[prost(message, repeated, tag = "2")]
    pub(super) slabs: Vec<WireSlabDescriptor>,
}

/// Whether these bytes are a binary manifest rather than a JSON one.
pub(super) fn is_wire_manifest(bytes: &[u8]) -> bool {
    bytes.first() == Some(&SLAB_MANIFEST_CONTAINER_PROTOBUF)
}

/// Decode a binary manifest. Every failure is an `Err` naming what was wrong: a manifest that
/// does not decode is corruption of a committed file, never an empty store.
pub(super) fn decode_slab_manifest(bytes: &[u8]) -> Result<BlockStoreSlabManifest, String> {
    if !is_wire_manifest(bytes) {
        return Err("missing the binary manifest container byte".to_string());
    }
    let (wire, next) = decode_framed_at::<WireSlabManifest>(bytes, 1)
        .map_err(|error| error.to_string())?;
    if next != bytes.len() {
        return Err(format!(
            "{} bytes follow the manifest record",
            bytes.len() - next
        ));
    }
    from_wire(wire)
}

fn from_wire(wire: WireSlabManifest) -> Result<BlockStoreSlabManifest, String> {
    let WireSlabManifest { version, slabs } = wire;
    let slabs = slabs
        .into_iter()
        .map(descriptor_from_wire)
        .collect::<Result<Vec<_>, _>>()?;
    Ok(BlockStoreSlabManifest { version, slabs })
}

// EVERY FIELD IS NAMED ON BOTH SIDES AND NEITHER PATTERN HAS A `..`, so a field added to
// `BlockStoreSlabDescriptor` is a compile error here rather than a value that quietly stops
// reaching the stored file.
fn descriptor_from_wire(wire: WireSlabDescriptor) -> Result<BlockStoreSlabDescriptor, String> {
    let WireSlabDescriptor {
        stored_slab_id,
        block_slab_id,
        state,
        physical_bytes,
        logical_bytes,
        created_unix_ms,
        updated_unix_ms,
        first_block_id,
        last_block_id,
        readable_prefix_physical_bytes,
        verified_source_mtime_unix_ms,
        has_corruption,
        first_error_offset,
        first_error,
    } = wire;
    let state = match WireSlabState::try_from(state) {
        Ok(WireSlabState::Active) => BlockStoreSlabState::Active,
        Ok(WireSlabState::Sealed) => BlockStoreSlabState::Sealed,
        Ok(WireSlabState::DelayedDestroy) => BlockStoreSlabState::DelayedDestroy,
        Ok(WireSlabState::Purged) => BlockStoreSlabState::Purged,
        Ok(WireSlabState::Unspecified) => {
            return Err(format!(
                "slab {block_slab_id} carries no lifecycle state (unspecified)"
            ))
        }
        Err(_) => {
            return Err(format!(
                "slab {block_slab_id} carries lifecycle state {state}, which this build does not know"
            ))
        }
    };
    Ok(BlockStoreSlabDescriptor {
        stored_slab_id,
        block_slab_id,
        state,
        physical_bytes,
        logical_bytes,
        created_unix_ms,
        updated_unix_ms,
        first_block_id,
        last_block_id,
        readable_prefix_physical_bytes,
        verified_source_mtime_unix_ms,
        has_corruption,
        first_error_offset,
        first_error,
    })
}

fn descriptor_to_wire(descriptor: &BlockStoreSlabDescriptor) -> WireSlabDescriptor {
    let BlockStoreSlabDescriptor {
        stored_slab_id,
        block_slab_id,
        state,
        physical_bytes,
        logical_bytes,
        created_unix_ms,
        updated_unix_ms,
        first_block_id,
        last_block_id,
        readable_prefix_physical_bytes,
        verified_source_mtime_unix_ms,
        has_corruption,
        first_error_offset,
        first_error,
    } = descriptor.clone();
    let state = match state {
        BlockStoreSlabState::Active => WireSlabState::Active,
        BlockStoreSlabState::Sealed => WireSlabState::Sealed,
        BlockStoreSlabState::DelayedDestroy => WireSlabState::DelayedDestroy,
        BlockStoreSlabState::Purged => WireSlabState::Purged,
    };
    WireSlabDescriptor {
        stored_slab_id,
        block_slab_id,
        state: state as i32,
        physical_bytes,
        logical_bytes,
        created_unix_ms,
        updated_unix_ms,
        first_block_id,
        last_block_id,
        readable_prefix_physical_bytes,
        verified_source_mtime_unix_ms,
        has_corruption,
        first_error_offset,
        first_error,
    }
}

/// The whole manifest in its stored shape, built straight from the descriptors.
///
/// NOT FROM A `BlockStoreSlabManifest`: the writer would have to clone every descriptor into one
/// just to read it back out, and the frame already has to hold the encoded bytes (its length and
/// checksum come before the payload), so the alternative to taking the descriptors by reference is
/// holding all of them twice.
fn wire_from_slabs<'a>(
    version: u32,
    slabs: impl IntoIterator<Item = &'a BlockStoreSlabDescriptor>,
) -> WireSlabManifest {
    WireSlabManifest {
        version,
        slabs: slabs.into_iter().map(descriptor_to_wire).collect(),
    }
}

/// The bytes of a binary manifest: the container byte, then one checksummed frame.
pub(super) fn encode_slab_manifest<'a>(
    version: u32,
    slabs: impl IntoIterator<Item = &'a BlockStoreSlabDescriptor>,
) -> Vec<u8> {
    let mut out = vec![SLAB_MANIFEST_CONTAINER_PROTOBUF];
    out.extend_from_slice(&encode_framed(&wire_from_slabs(version, slabs)));
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::block_store::paths::slab_manifest_path;
    use crate::block_store::slab_backend::BlockStoreReadTally;
    use crate::block_store::slab_manifest::load_slab_manifest_at;

    /// A descriptor whose every field carries a DIFFERENT value, so a field copied to the wrong
    /// place changes the answer instead of swapping two equal numbers.
    fn full_descriptor(slab: u64, state: BlockStoreSlabState) -> BlockStoreSlabDescriptor {
        BlockStoreSlabDescriptor {
            stored_slab_id: slab + 1_000,
            block_slab_id: slab,
            state,
            physical_bytes: 11_111 + slab,
            logical_bytes: 22_222 + slab,
            created_unix_ms: Some(1_234_567_890_123),
            updated_unix_ms: Some(1_234_567_990_456),
            first_block_id: Some(33_333 + slab),
            last_block_id: Some(44_444 + slab),
            readable_prefix_physical_bytes: 55_555 + slab,
            verified_source_mtime_unix_ms: Some(1_234_567_777_789),
            has_corruption: true,
            first_error_offset: Some(66_666 + slab),
            first_error: Some(format!("sentinel-{slab}")),
        }
    }

    fn bare_descriptor(slab: u64) -> BlockStoreSlabDescriptor {
        BlockStoreSlabDescriptor {
            stored_slab_id: slab,
            block_slab_id: slab,
            state: BlockStoreSlabState::Sealed,
            physical_bytes: 0,
            logical_bytes: 0,
            created_unix_ms: None,
            updated_unix_ms: None,
            first_block_id: None,
            last_block_id: None,
            readable_prefix_physical_bytes: 0,
            verified_source_mtime_unix_ms: None,
            has_corruption: false,
            first_error_offset: None,
            first_error: None,
        }
    }

    fn manifest_of(slabs: Vec<BlockStoreSlabDescriptor>) -> BlockStoreSlabManifest {
        BlockStoreSlabManifest { version: 1, slabs }
    }

    fn encode_manifest(manifest: &BlockStoreSlabManifest) -> Vec<u8> {
        encode_slab_manifest(manifest.version, &manifest.slabs)
    }

    fn manifest_to_wire(manifest: &BlockStoreSlabManifest) -> WireSlabManifest {
        wire_from_slabs(manifest.version, &manifest.slabs)
    }

    fn write_manifest_file(root: &std::path::Path, bytes: &[u8]) {
        let path = slab_manifest_path(root);
        std::fs::create_dir_all(path.parent().expect("a manifest path has a parent"))
            .expect("create the manifest directory");
        std::fs::write(&path, bytes).expect("write the manifest");
    }

    fn load(
        root: &std::path::Path,
    ) -> Result<std::collections::BTreeMap<u64, BlockStoreSlabDescriptor>, String> {
        let mut tally = BlockStoreReadTally::start();
        load_slab_manifest_at(root, &mut tally).map_err(|error| error.to_string())
    }

    #[test]
    fn every_descriptor_field_survives_the_wire_round_trip() {
        let states = [
            BlockStoreSlabState::Active,
            BlockStoreSlabState::Sealed,
            BlockStoreSlabState::DelayedDestroy,
            BlockStoreSlabState::Purged,
        ];
        let mut slabs: Vec<_> = states
            .iter()
            .enumerate()
            .map(|(index, state)| full_descriptor(index as u64 + 1, *state))
            .collect();
        slabs.push(bare_descriptor(9));
        let manifest = manifest_of(slabs);
        // DENOMINATOR: every state and both shapes of optional are in the input.
        assert_eq!(manifest.slabs.len(), 5);

        let decoded = decode_slab_manifest(&encode_manifest(&manifest))
            .expect("what the encoder wrote must decode");
        assert_eq!(decoded, manifest);
    }

    #[test]
    fn a_wire_manifest_loads_with_values_only_those_bytes_can_carry() {
        let dir = tempfile::tempdir().unwrap();
        let manifest = manifest_of(vec![
            full_descriptor(3, BlockStoreSlabState::Sealed),
            full_descriptor(4, BlockStoreSlabState::Active),
        ]);
        write_manifest_file(dir.path(), &encode_manifest(&manifest));

        let loaded = load(dir.path()).expect("a binary manifest must load");
        // THE SENTINELS ARE THE POINT. This reader has no rebuild-from-slabs fallback to recover
        // the same answer, but a timestamp of 1_234_567_890_123 and a "sentinel-3" error string
        // are values no inspection of a slab file could produce.
        assert_eq!(loaded.len(), 2);
        let three = loaded.get(&3).expect("slab 3");
        assert_eq!(three.created_unix_ms, Some(1_234_567_890_123));
        assert_eq!(three.first_error.as_deref(), Some("sentinel-3"));
        assert_eq!(three.state, BlockStoreSlabState::Sealed);
        assert_eq!(loaded.get(&4).expect("slab 4").state, BlockStoreSlabState::Active);
    }

    #[test]
    fn a_json_manifest_still_loads_through_the_same_reader() {
        let dir = tempfile::tempdir().unwrap();
        let manifest = manifest_of(vec![
            full_descriptor(5, BlockStoreSlabState::Sealed),
            bare_descriptor(6),
        ]);
        let json = serde_json::to_vec(&manifest).expect("serialise");
        assert_eq!(json.first(), Some(&b'{'), "this must be the JSON spelling");
        write_manifest_file(dir.path(), &json);

        let loaded = load(dir.path()).expect("a JSON manifest must still load");
        assert_eq!(loaded.len(), 2);
        assert_eq!(loaded.get(&5).expect("slab 5").first_error.as_deref(), Some("sentinel-5"));
    }

    #[test]
    fn the_two_spellings_load_to_the_same_descriptors() {
        let manifest = manifest_of(vec![
            full_descriptor(7, BlockStoreSlabState::DelayedDestroy),
            bare_descriptor(8),
        ]);
        let as_json = tempfile::tempdir().unwrap();
        let as_wire = tempfile::tempdir().unwrap();
        write_manifest_file(as_json.path(), &serde_json::to_vec(&manifest).unwrap());
        write_manifest_file(as_wire.path(), &encode_manifest(&manifest));
        let from_json = load(as_json.path()).expect("json");
        let from_wire = load(as_wire.path()).expect("wire");
        assert_eq!(from_json.len(), 2);
        assert_eq!(from_json, from_wire);
    }

    #[test]
    fn a_frame_of_123_bytes_starts_with_an_open_brace() {
        // Find the padding that makes the PAYLOAD exactly 123 bytes -- the length whose varint
        // is the byte JSON opens with.
        let mut found = None;
        // The payload is 12 bytes plus the padding, so the answer is near 111. The range is wider
        // than that so the search finds it rather than this comment being trusted.
        for pad in 0..=120usize {
            let mut descriptor = bare_descriptor(1);
            descriptor.first_error = Some("x".repeat(pad));
            let wire = manifest_to_wire(&manifest_of(vec![descriptor.clone()]));
            if wire.encoded_len() == 123 {
                found = Some((pad, descriptor));
                break;
            }
        }
        let (pad, descriptor) = found.expect("some padding must reach a 123-byte payload");
        let manifest = manifest_of(vec![descriptor]);

        // THE HAZARD, STATED AS BYTES: the bare frame opens with `{`, so a first-byte sniff
        // would hand this manifest to the JSON parser.
        let bare_frame = encode_framed(&manifest_to_wire(&manifest));
        assert_eq!(bare_frame[0], b'{', "pad {pad}: a 123-byte payload frames as an open brace");
        assert!(!is_wire_manifest(&bare_frame));

        // THE FIX, STATED AS BYTES: with the container byte in front the same manifest is recognised
        // and decodes.
        let bytes = encode_manifest(&manifest);
        assert!(is_wire_manifest(&bytes));
        assert_ne!(bytes[0], b'{');
        assert_eq!(decode_slab_manifest(&bytes).expect("decodes"), manifest);

        let dir = tempfile::tempdir().unwrap();
        write_manifest_file(dir.path(), &bytes);
        assert_eq!(load(dir.path()).expect("loads from disk").len(), 1);
    }

    #[test]
    fn no_json_document_opens_with_the_container_byte() {
        // Every byte a JSON document may open with: a brace, or the four JSON whitespace bytes
        // (pretty-printed output really does start with one in some writers).
        for opening in [b'{', b' ', b'\t', b'\n', b'\r'] {
            assert_ne!(opening, SLAB_MANIFEST_CONTAINER_PROTOBUF);
            assert!(!is_wire_manifest(&[opening, b'"']));
        }
        assert!(!is_wire_manifest(b"{\"version\":1,\"slabs\":[]}"));
        assert!(!is_wire_manifest(b""));
        // And the positive control: the byte itself IS recognised, so the loop above is not
        // vacuously true of a predicate that never fires.
        assert!(is_wire_manifest(&[SLAB_MANIFEST_CONTAINER_PROTOBUF]));
    }

    #[test]
    fn a_damaged_wire_manifest_is_an_error_and_never_an_empty_store() {
        let manifest = manifest_of(vec![full_descriptor(2, BlockStoreSlabState::Sealed)]);
        let good = encode_manifest(&manifest);
        // DENOMINATOR: the undamaged bytes load, so every failure below is the damage.
        let dir = tempfile::tempdir().unwrap();
        write_manifest_file(dir.path(), &good);
        assert_eq!(load(dir.path()).expect("undamaged").len(), 1);

        let mut flipped = good.clone();
        let last = flipped.len() - 1;
        flipped[last] ^= 0x40;
        let truncated = good[..good.len() - 3].to_vec();
        let mut trailing = good.clone();
        trailing.extend_from_slice(b"junk");

        for (name, bytes) in [("flipped", flipped), ("truncated", truncated), ("trailing", trailing)] {
            let case = tempfile::tempdir().unwrap();
            write_manifest_file(case.path(), &bytes);
            let error = load(case.path()).expect_err(name);
            assert!(error.contains("corrupt slab manifest"), "{name}: {error}");
        }
    }

    #[test]
    fn an_unspecified_or_unknown_state_is_refused_by_name() {
        let mut wire = manifest_to_wire(&manifest_of(vec![full_descriptor(4, BlockStoreSlabState::Sealed)]));
        // DENOMINATOR: as built, it decodes.
        assert!(from_wire(wire.clone()).is_ok());

        wire.slabs[0].state = WireSlabState::Unspecified as i32;
        let unspecified = from_wire(wire.clone()).expect_err("unspecified is refused");
        assert!(unspecified.contains("unspecified"), "{unspecified}");

        wire.slabs[0].state = 99;
        let unknown = from_wire(wire).expect_err("an unknown state is refused");
        assert!(unknown.contains("does not know"), "{unknown}");
    }

    #[test]
    fn the_stored_field_numbers_do_not_move() {
        // EVERY FIELD A DIFFERENT NON-ZERO VALUE. Proto3 leaves a zero scalar off the wire, so a
        // zero here would say nothing about its field number, and distinct values mean a swapped
        // pair of numbers changes the bytes instead of exchanging two equal ones.
        //
        // WHAT THIS REPLACES: the manifest was JSON, where serde writes one NAME per field and a
        // renamed Rust field silently changed the file; a guard had to assert the key spellings.
        // A protobuf file has no names to drift. What reaches disk is a field NUMBER and a wire
        // type, and those are what a stored manifest outlives the process on.
        let descriptor = BlockStoreSlabDescriptor {
            stored_slab_id: 1,
            block_slab_id: 2,
            state: BlockStoreSlabState::Sealed,
            physical_bytes: 3,
            logical_bytes: 4,
            created_unix_ms: Some(5),
            updated_unix_ms: Some(6),
            first_block_id: Some(7),
            last_block_id: Some(8),
            readable_prefix_physical_bytes: 9,
            verified_source_mtime_unix_ms: Some(10),
            has_corruption: true,
            first_error_offset: Some(11),
            first_error: Some("e".to_string()),
        };
        let inner: Vec<u8> = vec![
            0x08, 0x01, // 1  stored_slab_id
            0x10, 0x02, // 2  block_slab_id
            0x18, 0x02, // 3  state = Sealed
            0x20, 0x03, // 4  physical_bytes
            0x28, 0x04, // 5  logical_bytes
            0x30, 0x05, // 6  created_unix_ms
            0x38, 0x06, // 7  updated_unix_ms
            0x40, 0x07, // 8  first_block_id
            0x48, 0x08, // 9  last_block_id
            0x50, 0x09, // 10 readable_prefix_physical_bytes
            0x58, 0x0a, // 11 verified_source_mtime_unix_ms
            0x60, 0x01, // 12 has_corruption
            0x68, 0x0b, // 13 first_error_offset
            0x72, 0x01, b'e', // 14 first_error
        ];
        // DENOMINATOR: fourteen fields were meant to be in there, so a field dropped from the
        // message cannot leave this fixture silently shorter than the thing it describes.
        assert_eq!(inner.len(), 13 * 2 + 3);
        let mut expected: Vec<u8> = vec![0x08, 0x01, 0x12, inner.len() as u8];
        expected.extend_from_slice(&inner);

        let wire = wire_from_slabs(1, [&descriptor]);
        assert_eq!(
            wire.encode_to_vec(),
            expected,
            "the stored field numbers or wire types moved; a manifest on disk would read as something else"
        );
    }
}
