// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Merge-on-write cell sets for the feature model: the key, the per-key header and the packed
//! snapshot value. Pure functions over bytes and values; no engine state.
//!
//! # THE SHAPE
//!
//! A feature key holds one snapshot per timestamp, as every feature key does. A snapshot is a set
//! of CELLS. A cell is a member (a `u64` or non-empty bytes), up to [`MAX_LABELS_PER_CELL`] named
//! `u64` labels, and between 1 and [`MAX_LANES_PER_CELL`] named `i64` lanes. Writing a cell whose
//! member is already in the snapshot merges lane by lane under the reducer the header declares for
//! that lane ([`LaneReducer`]); labels are replaced by the latest write.
//!
//! # THE KEY
//!
//! The stored feature key is `cells`, then each part after a `0x1f` separator: the namespace, the
//! entity, and one part per declared dimension. Every variable-length part carries its byte length
//! as a fixed-width hex prefix and every number is fixed-width big-endian hex, so a text value
//! that contains the separator, or the digits of a length, cannot make two different keys spell
//! the same string, and lexical order of the numeric parts is numeric order.
//!
//! # THE HEADER
//!
//! The key spec, label names and lane names/reducers are written once, as a header, with the first
//! write to a key family, and every later write is checked against it by [`check_header`]. A later
//! write may ADD trailing label or lane names when the stored header's version allows it
//! ([`ADDITIVE_FROM_VERSION`]); old snapshots then read the new fields as zero. It may never
//! reorder, rename, retype or drop an existing field, and the refusal names the differing field.
//!
//! # BYTES
//!
//! Header and snapshot are protobuf-shaped (varints, length-delimited fields) and are written with
//! the field helpers the protocol types already own. There is no JSON and no second reader: a
//! value this module cannot read is refused, never guessed.
//!
//! In a snapshot, labels and lanes are packed varints inside their cell (lanes zigzag), and when
//! the `u64` members ascend strictly each is written as the distance from the previous one. A
//! malformed or truncated header, snapshot or key is an `Err`, never a panic.
//!
//! The limits are constants in this one place so raising one is a one-line change.

use crate::types::{
    decode_varint, encode_bytes_field, encode_varint, encode_varint_field,
};
use std::fmt::Write as _;

/// Dimensions a key spec may declare.
pub(crate) const MAX_KEY_DIMENSIONS: usize = 8;
/// Labels a cell may carry.
pub(crate) const MAX_LABELS_PER_CELL: usize = 4;
/// Lanes a cell may carry.
pub(crate) const MAX_LANES_PER_CELL: usize = 16;
/// Bytes in a bytes member or a bytes entity.
pub(crate) const MAX_MEMBER_BYTES: usize = 256;
/// Bytes in a text dimension, a text entity, or the namespace.
pub(crate) const MAX_TEXT_DIMENSION_BYTES: usize = 256;
/// Cells in one write request.
pub(crate) const MAX_CELLS_PER_REQUEST: usize = 1024;
/// Cells in one stored snapshot.
pub(crate) const MAX_CELLS_PER_SNAPSHOT: usize = 4096;
/// Largest `k` a ranked read may ask for.
pub(crate) const MAX_TOP_K: usize = 1000;
/// Bytes in a field name (dimension, label or lane).
pub(crate) const MAX_NAME_BYTES: usize = 64;

/// The version this code writes.
pub(crate) const HEADER_VERSION: u32 = 1;
/// The first header version whose later writes may add trailing labels or lanes.
pub(crate) const ADDITIVE_FROM_VERSION: u32 = 1;

/// The one place a bound is compared: `got` over `limit` is refused, `got == limit` is accepted.
pub(crate) fn within(what: &str, got: usize, limit: usize) -> Result<(), String> {
    if got > limit {
        return Err(format!("{what} {got} is over the limit of {limit}"));
    }
    Ok(())
}

/// `1..=limit`.
fn within_nonzero(what: &str, got: usize, limit: usize) -> Result<(), String> {
    if got == 0 {
        return Err(format!("{what} must be at least 1"));
    }
    within(what, got, limit)
}

pub(crate) fn validate_cells_in_request(count: usize) -> Result<(), String> {
    within("cells per request", count, MAX_CELLS_PER_REQUEST)
}

pub(crate) fn validate_cells_in_snapshot(count: usize) -> Result<(), String> {
    within("cells per snapshot", count, MAX_CELLS_PER_SNAPSHOT)
}

pub(crate) fn validate_top_k(k: usize) -> Result<(), String> {
    within_nonzero("top-k", k, MAX_TOP_K)
}

fn validate_name(what: &'static str, name: &str) -> Result<(), String> {
    within_nonzero(what, name.len(), MAX_NAME_BYTES)
}

// ------------------------------------------------------------------------------------------
// Key
// ------------------------------------------------------------------------------------------

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum EntityType {
    U64,
    Text,
    Bytes,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum DimType {
    U16,
    U32,
    U64,
    Text,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct DimSpec {
    pub(crate) name: String,
    pub(crate) ty: DimType,
    pub(crate) required: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct KeySpec {
    pub(crate) namespace: String,
    pub(crate) entity: EntityType,
    pub(crate) dimensions: Vec<DimSpec>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum EntityValue {
    U64(u64),
    Text(String),
    Bytes(Vec<u8>),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum DimValue {
    U16(u16),
    U32(u32),
    U64(u64),
    Text(String),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct CellKey {
    pub(crate) entity: EntityValue,
    /// One slot per declared dimension, in spec order; `None` for an absent optional dimension.
    pub(crate) dims: Vec<Option<DimValue>>,
}

const VIEW_TAG: &str = "cells";
const SEP: char = '\u{1f}';

impl KeySpec {
    pub(crate) fn validate(&self) -> Result<(), String> {
        within_nonzero("namespace bytes", self.namespace.len(), MAX_TEXT_DIMENSION_BYTES)?;
        within("key dimensions", self.dimensions.len(), MAX_KEY_DIMENSIONS)?;
        for (i, dim) in self.dimensions.iter().enumerate() {
            validate_name("dimension name bytes", &dim.name)?;
            if self.dimensions[..i].iter().any(|earlier| earlier.name == dim.name) {
                return Err(format!("dimension name `{}` is declared twice", dim.name));
            }
        }
        Ok(())
    }
}

fn text_part(out: &mut String, text: &str) -> Result<(), String> {
    within("text bytes", text.len(), MAX_TEXT_DIMENSION_BYTES)?;
    let _ = write!(out, "t{:04x}:{text}", text.len());
    Ok(())
}

/// An upper bound on the bytes of the stored key, so the key is built in one allocation whatever
/// its size.
fn key_capacity(spec: &KeySpec, key: &CellKey) -> usize {
    let entity = match &key.entity {
        EntityValue::U64(_) => 17,
        EntityValue::Text(t) => 6 + t.len(),
        EntityValue::Bytes(b) => 6 + 2 * b.len(),
    };
    let dims: usize = key
        .dims
        .iter()
        .map(|d| match d {
            Some(DimValue::Text(t)) => 7 + t.len(),
            _ => 18,
        })
        .sum();
    VIEW_TAG.len() + 8 + spec.namespace.len() + entity + dims
}

/// The stored feature key for `key` under `spec`. Refuses a key that does not fit the spec.
///
/// Built in ONE allocation: the cost of a key does not depend on how long its text parts are.
pub(crate) fn encode_key(spec: &KeySpec, key: &CellKey) -> Result<String, String> {
    spec.validate()?;
    if key.dims.len() != spec.dimensions.len() {
        return Err(format!(
            "key has {} dimension values, the spec declares {}",
            key.dims.len(),
            spec.dimensions.len()
        ));
    }
    let mut out = String::with_capacity(key_capacity(spec, key));
    out.push_str(VIEW_TAG);
    out.push(SEP);
    text_part(&mut out, &spec.namespace)?;
    out.push(SEP);
    match (spec.entity, &key.entity) {
        (EntityType::U64, EntityValue::U64(v)) => {
            let _ = write!(out, "u{v:016x}");
        }
        (EntityType::Text, EntityValue::Text(t)) => text_part(&mut out, t)?,
        (EntityType::Bytes, EntityValue::Bytes(b)) => {
            within_nonzero("entity bytes", b.len(), MAX_MEMBER_BYTES)?;
            let _ = write!(out, "b{:04x}:", b.len());
            for byte in b {
                let _ = write!(out, "{byte:02x}");
            }
        }
        _ => return Err("entity value does not match the declared entity type".to_string()),
    }
    for (dim, value) in spec.dimensions.iter().zip(&key.dims) {
        out.push(SEP);
        match (dim.ty, value) {
            (_, None) if !dim.required => out.push('-'),
            (_, None) => return Err(format!("dimension `{}` is required", dim.name)),
            (DimType::U16, Some(DimValue::U16(v))) => {
                let _ = write!(out, "h{v:04x}");
            }
            (DimType::U32, Some(DimValue::U32(v))) => {
                let _ = write!(out, "w{v:08x}");
            }
            (DimType::U64, Some(DimValue::U64(v))) => {
                let _ = write!(out, "u{v:016x}");
            }
            (DimType::Text, Some(DimValue::Text(t))) => text_part(&mut out, t)?,
            _ => return Err(format!("dimension `{}` has a value of the wrong type", dim.name)),
        }
    }
    Ok(out)
}

struct Scan<'a> {
    rest: &'a str,
}

impl<'a> Scan<'a> {
    fn take(&mut self, n: usize) -> Result<&'a str, String> {
        let part = self.rest.get(..n).ok_or("key is truncated")?;
        self.rest = &self.rest[n..];
        Ok(part)
    }
    fn sep(&mut self) -> Result<(), String> {
        if self.rest.starts_with(SEP) {
            self.rest = &self.rest[SEP.len_utf8()..];
            Ok(())
        } else {
            Err("key part is not followed by a separator".to_string())
        }
    }
    fn hex_num(&mut self, digits: usize) -> Result<u64, String> {
        u64::from_str_radix(self.take(digits)?, 16).map_err(|_| "bad hex number in key".to_string())
    }
    /// A part whose body is `width` characters for each byte the prefix counts.
    fn len_prefixed(&mut self, width: usize) -> Result<&'a str, String> {
        let len = self.hex_num(4)? as usize;
        if self.take(1)? != ":" {
            return Err("key length prefix is not followed by ':'".to_string());
        }
        self.take(len * width)
    }
}

/// Reads a stored feature key back into the values it was built from.
pub(crate) fn decode_key(spec: &KeySpec, stored: &str) -> Result<CellKey, String> {
    let mut scan = Scan { rest: stored };
    if scan.take(VIEW_TAG.len())? != VIEW_TAG {
        return Err("not a cell-set key".to_string());
    }
    scan.sep()?;
    if scan.take(1)? != "t" {
        return Err("namespace part is not text".to_string());
    }
    let namespace = scan.len_prefixed(1)?;
    if namespace != spec.namespace {
        return Err(format!("key namespace `{namespace}` is not `{}`", spec.namespace));
    }
    scan.sep()?;
    let tag = scan.take(1)?;
    let entity = match (spec.entity, tag) {
        (EntityType::U64, "u") => EntityValue::U64(scan.hex_num(16)?),
        (EntityType::Text, "t") => EntityValue::Text(scan.len_prefixed(1)?.to_string()),
        (EntityType::Bytes, "b") => {
            let digits = scan.len_prefixed(2)?;
            let bytes = (0..digits.len() / 2)
                .map(|i| u8::from_str_radix(digits.get(2 * i..2 * i + 2).unwrap_or("zz"), 16))
                .collect::<Result<Vec<u8>, _>>()
                .map_err(|_| "bad hex in entity bytes".to_string())?;
            EntityValue::Bytes(bytes)
        }
        _ => return Err("entity part does not match the declared entity type".to_string()),
    };
    let mut dims = Vec::with_capacity(spec.dimensions.len());
    for dim in &spec.dimensions {
        scan.sep()?;
        let tag = scan.take(1)?;
        dims.push(match (dim.ty, tag) {
            (_, "-") if !dim.required => None,
            (DimType::U16, "h") => Some(DimValue::U16(scan.hex_num(4)? as u16)),
            (DimType::U32, "w") => Some(DimValue::U32(scan.hex_num(8)? as u32)),
            (DimType::U64, "u") => Some(DimValue::U64(scan.hex_num(16)?)),
            (DimType::Text, "t") => Some(DimValue::Text(scan.len_prefixed(1)?.to_string())),
            _ => return Err(format!("dimension `{}` part has the wrong type", dim.name)),
        });
    }
    if !scan.rest.is_empty() {
        return Err("key has trailing bytes".to_string());
    }
    let key = CellKey { entity, dims };
    // Hex parsing is lenient (a sign, upper case); the stored form is not. A key that is not
    // exactly what `encode_key` writes for the values it holds is not a key this module made.
    if encode_key(spec, &key)? != stored {
        return Err("key is not in canonical form".to_string());
    }
    Ok(key)
}

// ------------------------------------------------------------------------------------------
// Lanes
// ------------------------------------------------------------------------------------------

/// How two writes to the same member combine on one lane.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum LaneReducer {
    Sum,
    Max,
    Min,
    /// Keep the value the member first had.
    First,
    /// Keep the value of the latest write.
    Last,
    /// The lane counts writes; the value a write carries is ignored.
    Count,
}

impl LaneReducer {
    fn tag(self) -> u64 {
        match self {
            LaneReducer::Sum => 0,
            LaneReducer::Max => 1,
            LaneReducer::Min => 2,
            LaneReducer::First => 3,
            LaneReducer::Last => 4,
            LaneReducer::Count => 5,
        }
    }
    fn from_tag(tag: u64) -> Option<Self> {
        Some(match tag {
            0 => LaneReducer::Sum,
            1 => LaneReducer::Max,
            2 => LaneReducer::Min,
            3 => LaneReducer::First,
            4 => LaneReducer::Last,
            5 => LaneReducer::Count,
            _ => return None,
        })
    }
    pub(crate) fn name(self) -> &'static str {
        match self {
            LaneReducer::Sum => "sum",
            LaneReducer::Max => "max",
            LaneReducer::Min => "min",
            LaneReducer::First => "first",
            LaneReducer::Last => "last",
            LaneReducer::Count => "count",
        }
    }

    /// The lane value of a member's first write.
    pub(crate) fn initial(self, incoming: i64) -> i64 {
        match self {
            LaneReducer::Count => 1,
            _ => incoming,
        }
    }

    /// The lane value after merging one more write into `existing`. Sums saturate.
    pub(crate) fn combine(self, existing: i64, incoming: i64) -> i64 {
        match self {
            LaneReducer::Sum => existing.saturating_add(incoming),
            LaneReducer::Max => existing.max(incoming),
            LaneReducer::Min => existing.min(incoming),
            LaneReducer::First => existing,
            LaneReducer::Last => incoming,
            LaneReducer::Count => existing.saturating_add(1),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct LaneSpec {
    pub(crate) name: String,
    pub(crate) reducer: LaneReducer,
}

// ------------------------------------------------------------------------------------------
// Header
// ------------------------------------------------------------------------------------------

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct CellHeader {
    pub(crate) version: u32,
    pub(crate) key: KeySpec,
    pub(crate) labels: Vec<String>,
    pub(crate) lanes: Vec<LaneSpec>,
}

impl CellHeader {
    pub(crate) fn validate(&self) -> Result<(), String> {
        self.key.validate()?;
        within("labels per cell", self.labels.len(), MAX_LABELS_PER_CELL)?;
        within_nonzero("lanes per cell", self.lanes.len(), MAX_LANES_PER_CELL)?;
        for (i, label) in self.labels.iter().enumerate() {
            validate_name("label name bytes", label)?;
            if self.labels[..i].contains(label) {
                return Err(format!("label name `{label}` is declared twice"));
            }
        }
        for (i, lane) in self.lanes.iter().enumerate() {
            validate_name("lane name bytes", &lane.name)?;
            if self.lanes[..i].iter().any(|earlier| earlier.name == lane.name) {
                return Err(format!("lane name `{}` is declared twice", lane.name));
            }
        }
        Ok(())
    }

    pub(crate) fn encode(&self) -> Result<Vec<u8>, String> {
        self.validate()?;
        let mut out = Vec::new();
        encode_varint_field(&mut out, 1, self.version as u64);
        encode_bytes_field(&mut out, 2, self.key.namespace.as_bytes());
        encode_varint_field(&mut out, 3, entity_tag(self.key.entity));
        let mut record = Vec::new();
        for dim in &self.key.dimensions {
            record.clear();
            encode_bytes_field(&mut record, 1, dim.name.as_bytes());
            encode_varint_field(&mut record, 2, dim_tag(dim.ty));
            encode_varint_field(&mut record, 3, dim.required as u64);
            encode_bytes_field(&mut out, 4, &record);
        }
        for label in &self.labels {
            encode_bytes_field(&mut out, 5, label.as_bytes());
        }
        for lane in &self.lanes {
            record.clear();
            encode_bytes_field(&mut record, 1, lane.name.as_bytes());
            encode_varint_field(&mut record, 2, lane.reducer.tag());
            encode_bytes_field(&mut out, 6, &record);
        }
        Ok(out)
    }

    pub(crate) fn decode(bytes: &[u8]) -> Result<CellHeader, String> {
        let mut cursor = 0;
        let mut version = None;
        let mut namespace = None;
        let mut entity = None;
        let mut dimensions = Vec::new();
        let mut labels = Vec::new();
        let mut lanes = Vec::new();
        while cursor < bytes.len() {
            let (field, wire) = read_tag(bytes, &mut cursor)?;
            match (field, wire) {
                (1, 0) => version = Some(read_u32(bytes, &mut cursor)?),
                (2, 2) => namespace = Some(read_string(bytes, &mut cursor)?),
                (3, 0) => {
                    entity = Some(
                        entity_from_tag(read_varint(bytes, &mut cursor)?)
                            .ok_or("unknown entity type in header")?,
                    )
                }
                (4, 2) => {
                    within("key dimensions", dimensions.len() + 1, MAX_KEY_DIMENSIONS)?;
                    let record = read_bytes(bytes, &mut cursor)?;
                    dimensions.push(decode_dim(record)?);
                }
                (5, 2) => {
                    within("labels per cell", labels.len() + 1, MAX_LABELS_PER_CELL)?;
                    labels.push(read_string(bytes, &mut cursor)?);
                }
                (6, 2) => {
                    within("lanes per cell", lanes.len() + 1, MAX_LANES_PER_CELL)?;
                    let record = read_bytes(bytes, &mut cursor)?;
                    lanes.push(decode_lane(record)?);
                }
                _ => return Err(format!("unknown header field {field}/{wire}")),
            }
        }
        let version = version.ok_or("header has no version")?;
        if version > HEADER_VERSION {
            return Err(format!(
                "header version {version} is newer than the {HEADER_VERSION} this build reads"
            ));
        }
        let header = CellHeader {
            version,
            key: KeySpec {
                namespace: namespace.ok_or("header has no namespace")?,
                entity: entity.ok_or("header has no entity type")?,
                dimensions,
            },
            labels,
            lanes,
        };
        header.validate()?;
        Ok(header)
    }
}

fn entity_tag(entity: EntityType) -> u64 {
    match entity {
        EntityType::U64 => 0,
        EntityType::Text => 1,
        EntityType::Bytes => 2,
    }
}

fn entity_from_tag(tag: u64) -> Option<EntityType> {
    Some(match tag {
        0 => EntityType::U64,
        1 => EntityType::Text,
        2 => EntityType::Bytes,
        _ => return None,
    })
}

fn dim_tag(ty: DimType) -> u64 {
    match ty {
        DimType::U16 => 0,
        DimType::U32 => 1,
        DimType::U64 => 2,
        DimType::Text => 3,
    }
}

fn dim_from_tag(tag: u64) -> Option<DimType> {
    Some(match tag {
        0 => DimType::U16,
        1 => DimType::U32,
        2 => DimType::U64,
        3 => DimType::Text,
        _ => return None,
    })
}

fn read_varint(bytes: &[u8], cursor: &mut usize) -> Result<u64, String> {
    decode_varint(bytes, cursor).ok_or_else(|| "truncated or overlong varint".to_string())
}

fn read_u32(bytes: &[u8], cursor: &mut usize) -> Result<u32, String> {
    u32::try_from(read_varint(bytes, cursor)?).map_err(|_| "value does not fit 32 bits".to_string())
}

fn read_tag(bytes: &[u8], cursor: &mut usize) -> Result<(u64, u64), String> {
    let tag = read_varint(bytes, cursor)?;
    Ok((tag >> 3, tag & 7))
}

fn read_bytes<'a>(bytes: &'a [u8], cursor: &mut usize) -> Result<&'a [u8], String> {
    let len = usize::try_from(read_varint(bytes, cursor)?).map_err(|_| "length overflows".to_string())?;
    let end = cursor.checked_add(len).ok_or("length overflows")?;
    let slice = bytes.get(*cursor..end).ok_or("field runs past the end of the value")?;
    *cursor = end;
    Ok(slice)
}

fn read_string(bytes: &[u8], cursor: &mut usize) -> Result<String, String> {
    String::from_utf8(read_bytes(bytes, cursor)?.to_vec()).map_err(|_| "text field is not UTF-8".to_string())
}

fn decode_dim(record: &[u8]) -> Result<DimSpec, String> {
    let mut cursor = 0;
    let (mut name, mut ty, mut required) = (None, None, None);
    while cursor < record.len() {
        let (field, wire) = read_tag(record, &mut cursor)?;
        match (field, wire) {
            (1, 2) => name = Some(read_string(record, &mut cursor)?),
            (2, 0) => ty = Some(dim_from_tag(read_varint(record, &mut cursor)?).ok_or("unknown dimension type")?),
            (3, 0) => required = Some(read_varint(record, &mut cursor)? != 0),
            _ => return Err("unknown dimension field".to_string()),
        }
    }
    Ok(DimSpec {
        name: name.ok_or("dimension has no name")?,
        ty: ty.ok_or("dimension has no type")?,
        required: required.ok_or("dimension has no required flag")?,
    })
}

fn decode_lane(record: &[u8]) -> Result<LaneSpec, String> {
    let mut cursor = 0;
    let (mut name, mut reducer) = (None, None);
    while cursor < record.len() {
        let (field, wire) = read_tag(record, &mut cursor)?;
        match (field, wire) {
            (1, 2) => name = Some(read_string(record, &mut cursor)?),
            (2, 0) => reducer = Some(LaneReducer::from_tag(read_varint(record, &mut cursor)?).ok_or("unknown lane reducer")?),
            _ => return Err("unknown lane field".to_string()),
        }
    }
    Ok(LaneSpec { name: name.ok_or("lane has no name")?, reducer: reducer.ok_or("lane has no reducer")? })
}

/// Compares the header a write declares with the one stored. `Ok(None)`: identical.
/// `Ok(Some(h))`: the write only appends trailing labels or lanes, `h` is the header to store.
/// `Err`: anything else, naming the differing field.
pub(crate) fn check_header(stored: &CellHeader, requested: &CellHeader) -> Result<Option<CellHeader>, String> {
    requested.validate()?;
    let (s, r) = (&stored.key, &requested.key);
    if s.namespace != r.namespace {
        return Err(format!("key namespace differs: stored `{}`, request `{}`", s.namespace, r.namespace));
    }
    if s.entity != r.entity {
        return Err(format!("key entity type differs: stored {:?}, request {:?}", s.entity, r.entity));
    }
    if s.dimensions.len() != r.dimensions.len() {
        return Err(format!(
            "key dimensions differ: stored {}, request {}",
            s.dimensions.len(),
            r.dimensions.len()
        ));
    }
    for (i, (a, b)) in s.dimensions.iter().zip(&r.dimensions).enumerate() {
        if a.name != b.name {
            return Err(format!("dimension {i} name differs: stored `{}`, request `{}`", a.name, b.name));
        }
        if a.ty != b.ty {
            return Err(format!("dimension `{}` type differs: stored {:?}, request {:?}", a.name, a.ty, b.ty));
        }
        if a.required != b.required {
            return Err(format!("dimension `{}` required flag differs", a.name));
        }
    }

    let mut extended = stored.clone();
    for (i, stored_label) in stored.labels.iter().enumerate() {
        match requested.labels.get(i) {
            None => {
                return Err(format!(
                    "request declares {} labels, stored header has {}: label `{stored_label}` is missing",
                    requested.labels.len(),
                    stored.labels.len()
                ))
            }
            Some(name) if name != stored_label => {
                return Err(format!("label {i} name differs: stored `{stored_label}`, request `{name}`"))
            }
            Some(_) => {}
        }
    }
    for (i, stored_lane) in stored.lanes.iter().enumerate() {
        match requested.lanes.get(i) {
            None => {
                return Err(format!(
                    "request declares {} lanes, stored header has {}: lane `{}` is missing",
                    requested.lanes.len(),
                    stored.lanes.len(),
                    stored_lane.name
                ))
            }
            Some(lane) if lane.name != stored_lane.name => {
                return Err(format!("lane {i} name differs: stored `{}`, request `{}`", stored_lane.name, lane.name))
            }
            Some(lane) if lane.reducer != stored_lane.reducer => {
                return Err(format!(
                    "lane `{}` reducer differs: stored {}, request {}",
                    lane.name,
                    stored_lane.reducer.name(),
                    lane.reducer.name()
                ))
            }
            Some(_) => {}
        }
    }
    let adds_labels = requested.labels.len() > stored.labels.len();
    let adds_lanes = requested.lanes.len() > stored.lanes.len();
    if !adds_labels && !adds_lanes {
        return Ok(None);
    }
    if stored.version < ADDITIVE_FROM_VERSION {
        return Err(format!(
            "stored header version {} does not allow adding labels or lanes",
            stored.version
        ));
    }
    extended.labels = requested.labels.clone();
    extended.lanes = requested.lanes.clone();
    extended.validate()?;
    Ok(Some(extended))
}

// ------------------------------------------------------------------------------------------
// Snapshot
// ------------------------------------------------------------------------------------------

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum Member {
    U64(u64),
    Bytes(Vec<u8>),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Cell {
    pub(crate) member: Member,
    pub(crate) labels: Vec<u64>,
    pub(crate) lanes: Vec<i64>,
}

fn zigzag(value: i64) -> u64 {
    ((value << 1) ^ (value >> 63)) as u64
}

fn unzigzag(value: u64) -> i64 {
    ((value >> 1) as i64) ^ -((value & 1) as i64)
}

fn validate_cell(header: &CellHeader, cell: &Cell) -> Result<(), String> {
    if let Member::Bytes(bytes) = &cell.member {
        within_nonzero("member bytes", bytes.len(), MAX_MEMBER_BYTES)?;
    }
    within("labels per cell", cell.labels.len(), MAX_LABELS_PER_CELL)?;
    within_nonzero("lanes per cell", cell.lanes.len(), MAX_LANES_PER_CELL)?;
    if cell.labels.len() != header.labels.len() {
        return Err(format!("cell has {} labels, the header declares {}", cell.labels.len(), header.labels.len()));
    }
    if cell.lanes.len() != header.lanes.len() {
        return Err(format!("cell has {} lanes, the header declares {}", cell.lanes.len(), header.lanes.len()));
    }
    Ok(())
}

/// Whether the `u64` members, in the order given, are strictly ascending, so each can be stored
/// as the distance from the one before. At least one `u64` member must be present.
fn delta_codable(cells: &[Cell]) -> bool {
    let mut previous: Option<u64> = None;
    for cell in cells {
        if let Member::U64(v) = cell.member {
            if previous.is_some_and(|p| v <= p) {
                return false;
            }
            previous = Some(v);
        }
    }
    previous.is_some()
}

/// The snapshot field that says `u64` members are stored as distances.
const SNAPSHOT_DELTA_FIELD: u64 = 2;

/// Packs `cells` for one timestamp. Refuses past [`MAX_CELLS_PER_SNAPSHOT`].
///
/// When the `u64` members ascend strictly (always true of a snapshot [`merge_into_snapshot`]
/// kept), each is written as the distance from the previous one, starting from zero: ids that
/// are close together cost one byte instead of up to ten. Any other order is written as it is.
pub(crate) fn encode_snapshot(header: &CellHeader, cells: &[Cell]) -> Result<Vec<u8>, String> {
    validate_cells_in_snapshot(cells.len())?;
    let delta = delta_codable(cells);
    let mut out = Vec::new();
    if delta {
        encode_varint_field(&mut out, SNAPSHOT_DELTA_FIELD, 1);
    }
    let mut previous = 0u64;
    for cell in cells {
        validate_cell(header, cell)?;
        let mut record = Vec::new();
        match &cell.member {
            Member::U64(v) if delta => {
                encode_varint_field(&mut record, 1, v - previous);
                previous = *v;
            }
            Member::U64(v) => encode_varint_field(&mut record, 1, *v),
            Member::Bytes(b) => encode_bytes_field(&mut record, 2, b),
        }
        let mut labels = Vec::new();
        cell.labels.iter().for_each(|v| encode_varint(&mut labels, *v));
        encode_bytes_field(&mut record, 3, &labels);
        let mut lanes = Vec::new();
        cell.lanes.iter().for_each(|v| encode_varint(&mut lanes, zigzag(*v)));
        encode_bytes_field(&mut record, 4, &lanes);
        encode_bytes_field(&mut out, 1, &record);
    }
    Ok(out)
}

fn unpack_varints(bytes: &[u8], limit: usize, what: &str) -> Result<Vec<u64>, String> {
    let mut cursor = 0;
    let mut out = Vec::new();
    while cursor < bytes.len() {
        within(what, out.len() + 1, limit)?;
        out.push(read_varint(bytes, &mut cursor)?);
    }
    Ok(out)
}

/// Reads a snapshot written under this header OR an earlier one that declared fewer labels or
/// lanes: the missing trailing values read as zero.
pub(crate) fn decode_snapshot(header: &CellHeader, bytes: &[u8]) -> Result<Vec<Cell>, String> {
    let mut cursor = 0;
    let mut cells: Vec<Cell> = Vec::new();
    let mut delta = false;
    let mut previous = 0u64;
    while cursor < bytes.len() {
        let (field, wire) = read_tag(bytes, &mut cursor)?;
        if (field, wire) == (SNAPSHOT_DELTA_FIELD, 0) {
            if !cells.is_empty() || delta {
                return Err("snapshot delta flag is repeated or follows a cell".to_string());
            }
            if read_varint(bytes, &mut cursor)? != 1 {
                return Err("snapshot delta flag is not 1".to_string());
            }
            delta = true;
            continue;
        }
        if (field, wire) != (1, 2) {
            return Err(format!("unknown snapshot field {field}/{wire}"));
        }
        validate_cells_in_snapshot(cells.len() + 1)?;
        let record = read_bytes(bytes, &mut cursor)?;
        let mut at = 0;
        let (mut member, mut labels, mut lanes) = (None, Vec::new(), Vec::new());
        while at < record.len() {
            let (f, w) = read_tag(record, &mut at)?;
            match (f, w) {
                (1, 0) => {
                    let raw = read_varint(record, &mut at)?;
                    let value = if delta {
                        previous.checked_add(raw).ok_or("member distance overflows u64")?
                    } else {
                        raw
                    };
                    previous = value;
                    member = Some(Member::U64(value));
                }
                (2, 2) => member = Some(Member::Bytes(read_bytes(record, &mut at)?.to_vec())),
                (3, 2) => labels = unpack_varints(read_bytes(record, &mut at)?, header.labels.len(), "labels per cell")?,
                (4, 2) => {
                    lanes = unpack_varints(read_bytes(record, &mut at)?, header.lanes.len(), "lanes per cell")?
                        .into_iter()
                        .map(unzigzag)
                        .collect()
                }
                _ => return Err(format!("unknown cell field {f}/{w}")),
            }
        }
        let member = member.ok_or("cell has no member")?;
        if let Member::Bytes(b) = &member {
            within_nonzero("member bytes", b.len(), MAX_MEMBER_BYTES)?;
        }
        labels.resize(header.labels.len(), 0);
        lanes.resize(header.lanes.len(), 0);
        cells.push(Cell { member, labels, lanes });
    }
    Ok(cells)
}

/// Whether `cells` are strictly ascending by member: sorted and without a repeat. A stored
/// snapshot is always this; a request need not be.
pub(crate) fn is_strictly_ascending(cells: &[Cell]) -> bool {
    cells.windows(2).all(|pair| pair[0].member < pair[1].member)
}

/// What a merge did, for the receipt of the write that asked for it.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct MergeCounts {
    /// Incoming cells whose member was not in the snapshot: a cell was added.
    pub(crate) created: usize,
    /// Incoming cells whose member was already there: lanes were combined into it.
    pub(crate) merged: usize,
}

/// Merges `incoming` into `snapshot` (kept sorted by member) under the header's lane reducers.
/// Nothing is changed when the result would hold more than [`MAX_CELLS_PER_SNAPSHOT`] cells or any
/// incoming cell is invalid. A member that appears twice in `incoming` merges twice.
pub(crate) fn merge_into_snapshot(header: &CellHeader, snapshot: &mut Vec<Cell>, incoming: &[Cell]) -> Result<MergeCounts, String> {
    validate_cells_in_request(incoming.len())?;
    if !is_strictly_ascending(snapshot) {
        return Err("stored snapshot is not strictly ascending by member".to_string());
    }
    for cell in incoming {
        validate_cell(header, cell)?;
    }
    let mut next = snapshot.clone();
    let mut counts = MergeCounts::default();
    for cell in incoming {
        match next.binary_search_by(|have| have.member.cmp(&cell.member)) {
            Ok(at) => {
                let have = &mut next[at];
                for (i, lane) in header.lanes.iter().enumerate() {
                    have.lanes[i] = lane.reducer.combine(have.lanes[i], cell.lanes[i]);
                }
                have.labels.clone_from(&cell.labels);
                counts.merged += 1;
            }
            Err(at) => {
                validate_cells_in_snapshot(next.len() + 1)?;
                let lanes = header.lanes.iter().zip(&cell.lanes).map(|(l, v)| l.reducer.initial(*v)).collect();
                next.insert(at, Cell { member: cell.member.clone(), labels: cell.labels.clone(), lanes });
                counts.created += 1;
            }
        }
    }
    *snapshot = next;
    Ok(counts)
}

// ------------------------------------------------------------------------------------------
// The stored and the requested value
// ------------------------------------------------------------------------------------------

/// A value is the header followed by the snapshot, so every stored snapshot describes itself: a
/// snapshot written before a label or lane was added still decodes under the header it was
/// written with, and a trim of old snapshots can never take the header with it.
pub(crate) fn encode_value(header: &CellHeader, cells: &[Cell]) -> Result<Vec<u8>, String> {
    let mut out = Vec::new();
    encode_bytes_field(&mut out, 1, &header.encode()?);
    encode_bytes_field(&mut out, 2, &encode_snapshot(header, cells)?);
    Ok(out)
}

/// The header and the still-packed snapshot bytes of a value, the header still undecoded.
pub(crate) fn split_value_raw(bytes: &[u8]) -> Result<(&[u8], &[u8]), String> {
    let mut cursor = 0;
    let (mut header, mut snapshot) = (None, None);
    while cursor < bytes.len() {
        let (field, wire) = read_tag(bytes, &mut cursor)?;
        match (field, wire) {
            (1, 2) if header.is_none() => header = Some(read_bytes(bytes, &mut cursor)?),
            (2, 2) if snapshot.is_none() => snapshot = Some(read_bytes(bytes, &mut cursor)?),
            _ => return Err(format!("unexpected or repeated value field {field}/{wire}")),
        }
    }
    Ok((header.ok_or("value has no header")?, snapshot.unwrap_or(&[])))
}

/// The header and the still-packed snapshot bytes of a value.
pub(crate) fn split_value(bytes: &[u8]) -> Result<(CellHeader, &[u8]), String> {
    let (header, snapshot) = split_value_raw(bytes)?;
    Ok((CellHeader::decode(header)?, snapshot))
}

pub(crate) fn decode_value(bytes: &[u8]) -> Result<(CellHeader, Vec<Cell>), String> {
    let (header, snapshot) = split_value(bytes)?;
    let cells = decode_snapshot(&header, snapshot)?;
    Ok((header, cells))
}

/// The value to store for one timestamp: the request's cells merged into what is stored there.
///
/// * `newest` is the header of the newest stored snapshot of the key, `None` for a key with no
///   snapshots; the request's header is checked against it ([`check_header`]) and may extend it.
/// * `existing` is the value stored at the request's timestamp, if any. It is read under the
///   effective header, so a snapshot that predates an added label or lane gains it as zero before
///   the request is merged in.
///
/// Returns the new value, the effective header and the [`MergeCounts`] of the merge. Refuses
/// without a result when the header differs, a bound is passed, or `existing` cannot be read.
/// This is the one entry point a write policy calls: it owns header checking, the upgrade of an
/// older stored snapshot, the merge and the encode.
pub(crate) fn merge_request(
    newest: Option<&CellHeader>,
    existing: Option<&[u8]>,
    request: &[u8],
) -> Result<(Vec<u8>, CellHeader, MergeCounts), String> {
    let newest = newest.map(|header| std::rc::Rc::new(header.clone()));
    let own;
    let existing = match existing {
        None => Existing::Nothing,
        Some(bytes) => {
            let (header, snapshot) = split_value(bytes)?;
            own = header;
            Existing::Stored { header: &own, snapshot }
        }
    };
    let (effective, cells, counts) = merge_point(newest.as_ref(), existing, request)?;
    Ok((cells.encode_value(&effective)?, (*effective).clone(), counts))
}

// ------------------------------------------------------------------------------------------
// The flat form
// ------------------------------------------------------------------------------------------
//
// A merge that holds each cell as a `Cell` pays several allocations per cell per step: the labels
// `Vec`, the lanes `Vec`, a `Vec` for a bytes member, a temporary record and two packed-field
// temporaries while it is written, and a clone of the whole snapshot to keep the merge atomic. The
// flat form holds the same cells in four vectors for the WHOLE snapshot -- members, one arena for
// bytes members, labels and lanes at a fixed stride -- so a merge costs a fixed number of
// allocations at any cell count. It reads and writes exactly the bytes `decode_snapshot` and
// `encode_snapshot` do; the tests pin that against those two, which stay as the plain statement of
// the format.

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum FlatMember {
    U64(u64),
    /// `len` bytes of the arena from `start`.
    Bytes { start: usize, len: usize },
}

/// Orders members as [`Member`] does: every `u64` before every bytes member, bytes lexically.
fn cmp_members(a: &FlatMember, a_arena: &[u8], b: &FlatMember, b_arena: &[u8]) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    match (a, b) {
        (FlatMember::U64(x), FlatMember::U64(y)) => x.cmp(y),
        (FlatMember::U64(_), FlatMember::Bytes { .. }) => Ordering::Less,
        (FlatMember::Bytes { .. }, FlatMember::U64(_)) => Ordering::Greater,
        (FlatMember::Bytes { start: s1, len: l1 }, FlatMember::Bytes { start: s2, len: l2 }) => {
            a_arena[*s1..*s1 + *l1].cmp(&b_arena[*s2..*s2 + *l2])
        }
    }
}

/// The cells of one snapshot, sorted by member once merged, in four vectors.
#[derive(Debug)]
pub(crate) struct FlatCells {
    labels_per_cell: usize,
    lanes_per_cell: usize,
    members: Vec<FlatMember>,
    arena: Vec<u8>,
    labels: Vec<u64>,
    lanes: Vec<i64>,
}

/// Bytes a varint of `value` takes.
fn varint_len(value: u64) -> usize {
    (64 - (value | 1).leading_zeros() as usize).div_ceil(7)
}

/// Bytes a length-delimited field of `len` content bytes takes, with a one-byte tag. An empty
/// field is not written (see `encode_bytes_field`), so it takes none.
fn field_len(len: usize) -> usize {
    if len == 0 {
        0
    } else {
        1 + varint_len(len as u64) + len
    }
}

/// How many cells the packed snapshot holds, read off its top-level fields only. A damaged image
/// stops the count early; the decode that follows reports the damage.
fn count_cells(bytes: &[u8]) -> usize {
    let (mut cursor, mut count) = (0, 0);
    while cursor < bytes.len() {
        let Ok((field, wire)) = read_tag(bytes, &mut cursor) else { break };
        let step = match (field, wire) {
            (1, 2) => read_bytes(bytes, &mut cursor).map(|_| count += 1),
            (SNAPSHOT_DELTA_FIELD, 0) => read_varint(bytes, &mut cursor).map(|_| ()),
            _ => break,
        };
        if step.is_err() {
            break;
        }
    }
    count
}

/// Appends the packed varints of `bytes` to `out`, refusing more than `limit` of them.
fn unpack_into<T>(
    bytes: &[u8],
    limit: usize,
    what: &str,
    out: &mut Vec<T>,
    convert: impl Fn(u64) -> T,
) -> Result<(), String> {
    let (mut cursor, mut count) = (0, 0);
    while cursor < bytes.len() {
        within(what, count + 1, limit)?;
        out.push(convert(read_varint(bytes, &mut cursor)?));
        count += 1;
    }
    Ok(())
}

impl FlatCells {
    /// No cells, with room for `room` of them.
    pub(crate) fn empty(header: &CellHeader, room: usize) -> FlatCells {
        let room = room.min(MAX_CELLS_PER_SNAPSHOT + 1);
        let (labels_per_cell, lanes_per_cell) = (header.labels.len(), header.lanes.len());
        FlatCells {
            labels_per_cell,
            lanes_per_cell,
            members: Vec::with_capacity(room),
            arena: Vec::new(),
            labels: Vec::with_capacity(room * labels_per_cell),
            lanes: Vec::with_capacity(room * lanes_per_cell),
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.members.len()
    }

    /// Reads what [`decode_snapshot`] reads, under the same limits, with room for `room` more
    /// cells than `bytes` holds so a merge into it does not have to grow it.
    pub(crate) fn decode(header: &CellHeader, bytes: &[u8], room: usize) -> Result<FlatCells, String> {
        let mut out = FlatCells::empty(header, count_cells(bytes).saturating_add(room));
        let (labels_per_cell, lanes_per_cell) = (out.labels_per_cell, out.lanes_per_cell);
        let mut cursor = 0;
        let mut delta = false;
        let mut previous = 0u64;
        while cursor < bytes.len() {
            let (field, wire) = read_tag(bytes, &mut cursor)?;
            if (field, wire) == (SNAPSHOT_DELTA_FIELD, 0) {
                if !out.members.is_empty() || delta {
                    return Err("snapshot delta flag is repeated or follows a cell".to_string());
                }
                if read_varint(bytes, &mut cursor)? != 1 {
                    return Err("snapshot delta flag is not 1".to_string());
                }
                delta = true;
                continue;
            }
            if (field, wire) != (1, 2) {
                return Err(format!("unknown snapshot field {field}/{wire}"));
            }
            validate_cells_in_snapshot(out.members.len() + 1)?;
            let record = read_bytes(bytes, &mut cursor)?;
            let (labels_at, lanes_at, arena_at) = (out.labels.len(), out.lanes.len(), out.arena.len());
            let mut at = 0;
            let mut member = None;
            while at < record.len() {
                let (f, w) = read_tag(record, &mut at)?;
                match (f, w) {
                    (1, 0) => {
                        let raw = read_varint(record, &mut at)?;
                        let value = if delta {
                            previous.checked_add(raw).ok_or("member distance overflows u64")?
                        } else {
                            raw
                        };
                        previous = value;
                        member = Some(FlatMember::U64(value));
                    }
                    (2, 2) => {
                        let body = read_bytes(record, &mut at)?;
                        // A repeated member field keeps the last, as it does when decoded to a `Cell`.
                        out.arena.truncate(arena_at);
                        if out.arena.capacity() == 0 {
                            // Every bytes member is inside `bytes`, so this is one allocation, once.
                            out.arena.reserve_exact(bytes.len());
                        }
                        out.arena.extend_from_slice(body);
                        member = Some(FlatMember::Bytes { start: arena_at, len: body.len() });
                    }
                    (3, 2) => {
                        out.labels.truncate(labels_at);
                        let packed = read_bytes(record, &mut at)?;
                        unpack_into(packed, labels_per_cell, "labels per cell", &mut out.labels, |v| v)?;
                    }
                    (4, 2) => {
                        out.lanes.truncate(lanes_at);
                        let packed = read_bytes(record, &mut at)?;
                        unpack_into(packed, lanes_per_cell, "lanes per cell", &mut out.lanes, unzigzag)?;
                    }
                    _ => return Err(format!("unknown cell field {f}/{w}")),
                }
            }
            let member = member.ok_or("cell has no member")?;
            if let FlatMember::Bytes { len, .. } = member {
                within_nonzero("member bytes", len, MAX_MEMBER_BYTES)?;
            }
            // Values the cell did not carry (a snapshot written under an earlier header) read as zero.
            out.labels.resize(labels_at + labels_per_cell, 0);
            out.lanes.resize(lanes_at + lanes_per_cell, 0);
            out.members.push(member);
        }
        Ok(out)
    }

    /// The same cells under a header that declares more labels or lanes: the new ones read as zero.
    fn widen(&mut self, header: &CellHeader) {
        let (labels_per_cell, lanes_per_cell) = (header.labels.len(), header.lanes.len());
        if (labels_per_cell, lanes_per_cell) == (self.labels_per_cell, self.lanes_per_cell) {
            return;
        }
        fn spread<T: Copy + Default>(old: &[T], from: usize, to: usize, cells: usize) -> Vec<T> {
            let mut out = vec![T::default(); cells * to];
            for i in 0..cells {
                out[i * to..i * to + from].copy_from_slice(&old[i * from..(i + 1) * from]);
            }
            out
        }
        let cells = self.members.len();
        self.labels = spread(&self.labels, self.labels_per_cell, labels_per_cell, cells);
        self.lanes = spread(&self.lanes, self.lanes_per_cell, lanes_per_cell, cells);
        self.labels_per_cell = labels_per_cell;
        self.lanes_per_cell = lanes_per_cell;
    }

    fn is_strictly_ascending(&self) -> bool {
        self.members
            .windows(2)
            .all(|pair| cmp_members(&pair[0], &self.arena, &pair[1], &self.arena).is_lt())
    }

    /// Merges `incoming` into these cells under the header's lane reducers, as
    /// [`merge_into_snapshot`] does: a member already here has its lanes combined and its labels
    /// replaced; a member that is not is inserted in order. A member that appears twice in
    /// `incoming` merges twice. On `Err` the cells are left half merged, so a caller must not use
    /// them.
    pub(crate) fn merge(&mut self, header: &CellHeader, incoming: &FlatCells) -> Result<MergeCounts, String> {
        validate_cells_in_request(incoming.len())?;
        if !self.is_strictly_ascending() {
            return Err("stored snapshot is not strictly ascending by member".to_string());
        }
        if !incoming.members.is_empty() {
            if incoming.labels_per_cell != header.labels.len() {
                return Err(format!(
                    "cell has {} labels, the header declares {}",
                    incoming.labels_per_cell,
                    header.labels.len()
                ));
            }
            if incoming.lanes_per_cell != header.lanes.len() {
                return Err(format!(
                    "cell has {} lanes, the header declares {}",
                    incoming.lanes_per_cell,
                    header.lanes.len()
                ));
            }
        }
        debug_assert_eq!((self.labels_per_cell, self.lanes_per_cell), (header.labels.len(), header.lanes.len()));
        let (nl, nn) = (self.labels_per_cell, self.lanes_per_cell);
        self.members.reserve(incoming.len());
        self.labels.reserve(incoming.len() * nl);
        self.lanes.reserve(incoming.len() * nn);
        self.arena.reserve(incoming.arena.len());
        let mut counts = MergeCounts::default();
        for (i, member) in incoming.members.iter().enumerate() {
            let found = self
                .members
                .binary_search_by(|have| cmp_members(have, &self.arena, member, &incoming.arena));
            let in_labels = &incoming.labels[i * nl..(i + 1) * nl];
            let in_lanes = &incoming.lanes[i * nn..(i + 1) * nn];
            match found {
                Ok(at) => {
                    let have_lanes = &mut self.lanes[at * nn..(at + 1) * nn];
                    for ((lane, have), add) in header.lanes.iter().zip(have_lanes).zip(in_lanes) {
                        *have = lane.reducer.combine(*have, *add);
                    }
                    self.labels[at * nl..(at + 1) * nl].copy_from_slice(in_labels);
                    counts.merged += 1;
                }
                Err(at) => {
                    validate_cells_in_snapshot(self.members.len() + 1)?;
                    let stored = match *member {
                        FlatMember::U64(v) => FlatMember::U64(v),
                        FlatMember::Bytes { start, len } => {
                            let here = self.arena.len();
                            self.arena.extend_from_slice(&incoming.arena[start..start + len]);
                            FlatMember::Bytes { start: here, len }
                        }
                    };
                    self.members.insert(at, stored);
                    self.labels.splice(at * nl..at * nl, in_labels.iter().copied());
                    self.lanes.splice(
                        at * nn..at * nn,
                        header.lanes.iter().zip(in_lanes).map(|(lane, v)| lane.reducer.initial(*v)),
                    );
                    counts.created += 1;
                }
            }
        }
        Ok(counts)
    }

    /// Whether the `u64` members ascend strictly, so each is written as a distance
    /// (see [`delta_codable`]).
    fn delta_codable(&self) -> bool {
        let mut previous: Option<u64> = None;
        for member in &self.members {
            if let FlatMember::U64(v) = *member {
                if previous.is_some_and(|p| v <= p) {
                    return false;
                }
                previous = Some(v);
            }
        }
        previous.is_some()
    }

    fn labels_of(&self, i: usize) -> &[u64] {
        &self.labels[i * self.labels_per_cell..(i + 1) * self.labels_per_cell]
    }

    fn lanes_of(&self, i: usize) -> &[i64] {
        &self.lanes[i * self.lanes_per_cell..(i + 1) * self.lanes_per_cell]
    }

    /// The bytes of cell `i`'s record, where `previous` is the `u64` member before it (the
    /// distance base when `delta`).
    fn record_len(&self, i: usize, delta: bool, previous: u64) -> usize {
        let member = match self.members[i] {
            FlatMember::U64(v) if delta => 1 + varint_len(v - previous),
            FlatMember::U64(v) => 1 + varint_len(v),
            FlatMember::Bytes { len, .. } => 1 + varint_len(len as u64) + len,
        };
        let labels: usize = self.labels_of(i).iter().map(|v| varint_len(*v)).sum();
        let lanes: usize = self.lanes_of(i).iter().map(|v| varint_len(zigzag(*v))).sum();
        member + field_len(labels) + field_len(lanes)
    }

    /// The bytes of the packed snapshot.
    fn snapshot_len(&self, delta: bool) -> usize {
        let mut total = if delta { 2 } else { 0 };
        let mut previous = 0u64;
        for (i, member) in self.members.iter().enumerate() {
            let record = self.record_len(i, delta, previous);
            if let FlatMember::U64(v) = member {
                previous = *v;
            }
            total += 1 + varint_len(record as u64) + record;
        }
        total
    }

    /// The value to store: the header, then the packed snapshot, in one allocation of exactly the
    /// size written (a first pass measures it). Byte for byte what [`encode_value`] writes for the
    /// same cells.
    pub(crate) fn encode_value(&self, header: &CellHeader) -> Result<Vec<u8>, String> {
        validate_cells_in_snapshot(self.members.len())?;
        for member in &self.members {
            if let FlatMember::Bytes { len, .. } = member {
                within_nonzero("member bytes", *len, MAX_MEMBER_BYTES)?;
            }
        }
        if (self.labels_per_cell, self.lanes_per_cell) != (header.labels.len(), header.lanes.len()) {
            return Err(format!(
                "cells hold {} labels and {} lanes, the header declares {} and {}",
                self.labels_per_cell,
                self.lanes_per_cell,
                header.labels.len(),
                header.lanes.len()
            ));
        }
        let delta = self.delta_codable();
        let head = header.encode()?;
        let snapshot_len = self.snapshot_len(delta);
        let mut out = Vec::with_capacity(field_len(head.len()) + field_len(snapshot_len));
        encode_bytes_field(&mut out, 1, &head);
        if snapshot_len > 0 {
            encode_varint(&mut out, (2 << 3) | 2);
            encode_varint(&mut out, snapshot_len as u64);
        }
        if delta {
            encode_varint_field(&mut out, SNAPSHOT_DELTA_FIELD, 1);
        }
        let mut previous = 0u64;
        for (i, member) in self.members.iter().enumerate() {
            encode_varint(&mut out, (1 << 3) | 2);
            encode_varint(&mut out, self.record_len(i, delta, previous) as u64);
            match *member {
                FlatMember::U64(v) if delta => {
                    encode_varint_field(&mut out, 1, v - previous);
                    previous = v;
                }
                FlatMember::U64(v) => encode_varint_field(&mut out, 1, v),
                FlatMember::Bytes { start, len } => encode_bytes_field(&mut out, 2, &self.arena[start..start + len]),
            }
            let labels = self.labels_of(i);
            let packed: usize = labels.iter().map(|v| varint_len(*v)).sum();
            if packed > 0 {
                encode_varint(&mut out, (3 << 3) | 2);
                encode_varint(&mut out, packed as u64);
                labels.iter().for_each(|v| encode_varint(&mut out, *v));
            }
            let lanes = self.lanes_of(i);
            let packed: usize = lanes.iter().map(|v| varint_len(zigzag(*v))).sum();
            if packed > 0 {
                encode_varint(&mut out, (4 << 3) | 2);
                encode_varint(&mut out, packed as u64);
                lanes.iter().for_each(|v| encode_varint(&mut out, zigzag(*v)));
            }
        }
        debug_assert_eq!(out.len(), out.capacity(), "the size pass and the write pass disagree");
        Ok(out)
    }
}

/// The header and cells of a request value.
pub(crate) fn decode_request(bytes: &[u8]) -> Result<(CellHeader, FlatCells), String> {
    let (header, packed) = split_value(bytes)?;
    let cells = FlatCells::decode(&header, packed, 0)?;
    Ok((header, cells))
}

/// What a timestamp holds when one more request point is merged into it.
pub(crate) enum Existing<'a> {
    /// Nothing is stored or planned there.
    Nothing,
    /// A stored value: its header, and its still-packed snapshot.
    Stored { header: &'a CellHeader, snapshot: &'a [u8] },
    /// Cells an earlier point of the same request already merged there, with the header they were
    /// merged under.
    Planned { header: std::rc::Rc<CellHeader>, cells: FlatCells },
}

/// One request point merged into what its timestamp holds. The one place that owns the header
/// check against the newest stored header, the upgrade of an older snapshot, and the merge.
/// [`FlatCells::encode_value`] is the only step left to the caller, so a caller that only needs to
/// know whether the request is accepted can skip it. Returns the effective header (the newest
/// stored one itself when the request declares exactly that), the merged cells and the counts.
pub(crate) fn merge_point(
    newest: Option<&std::rc::Rc<CellHeader>>,
    existing: Existing<'_>,
    request: &[u8],
) -> Result<(std::rc::Rc<CellHeader>, FlatCells, MergeCounts), String> {
    let (requested, incoming) = decode_request(request)?;
    validate_cells_in_request(incoming.len())?;
    let effective = match newest {
        None => std::rc::Rc::new(requested),
        Some(stored) => match check_header(stored, &requested)? {
            Some(extended) => std::rc::Rc::new(extended),
            None => std::rc::Rc::clone(stored),
        },
    };
    let mut cells = match existing {
        Existing::Nothing => FlatCells::empty(&effective, incoming.len()),
        Existing::Stored { header, snapshot } => {
            if *header != *effective {
                // The stored snapshot may only be an earlier, shorter form of the effective header.
                check_header(header, &effective)?;
            }
            FlatCells::decode(&effective, snapshot, incoming.len())?
        }
        Existing::Planned { header, mut cells } => {
            if *header != *effective {
                check_header(&header, &effective)?;
                cells.widen(&effective);
            }
            cells
        }
    };
    let counts = cells.merge(&effective, &incoming)?;
    Ok((effective, cells, counts))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spec() -> KeySpec {
        KeySpec {
            namespace: "ns".to_string(),
            entity: EntityType::U64,
            dimensions: vec![
                DimSpec { name: "channel".to_string(), ty: DimType::U16, required: true },
                DimSpec { name: "kind".to_string(), ty: DimType::Text, required: false },
            ],
        }
    }

    fn header() -> CellHeader {
        CellHeader {
            version: HEADER_VERSION,
            key: spec(),
            labels: vec!["slot".to_string()],
            lanes: vec![
                LaneSpec { name: "watch".to_string(), reducer: LaneReducer::Sum },
                LaneSpec { name: "peak".to_string(), reducer: LaneReducer::Max },
            ],
        }
    }

    fn key(entity: u64, channel: u16, kind: Option<&str>) -> CellKey {
        CellKey {
            entity: EntityValue::U64(entity),
            dims: vec![Some(DimValue::U16(channel)), kind.map(|k| DimValue::Text(k.to_string()))],
        }
    }

    #[test]
    fn a_key_round_trips_and_numeric_order_is_lexical_order() {
        let s = spec();
        for k in [key(7, 3, Some("like")), key(u64::MAX, u16::MAX, None), key(0, 0, Some(""))] {
            let stored = encode_key(&s, &k).unwrap();
            assert_eq!(decode_key(&s, &stored).unwrap(), k, "{stored:?}");
        }
        let low = encode_key(&s, &key(9, 1, None)).unwrap();
        let high = encode_key(&s, &key(10, 1, None)).unwrap();
        assert!(low < high, "fixed-width numbers must sort numerically: {low} {high}");
        assert!(low.starts_with("cells\u{1f}t0002:ns\u{1f}u0000000000000009"), "{low:?}");
    }

    #[test]
    fn a_text_value_holding_the_separator_or_a_length_prefix_cannot_collide() {
        let s = KeySpec {
            namespace: "ns".to_string(),
            entity: EntityType::Text,
            dimensions: vec![
                DimSpec { name: "a".to_string(), ty: DimType::Text, required: true },
                DimSpec { name: "b".to_string(), ty: DimType::Text, required: true },
            ],
        };
        let mk = |e: &str, a: &str, b: &str| CellKey {
            entity: EntityValue::Text(e.to_string()),
            dims: vec![Some(DimValue::Text(a.to_string())), Some(DimValue::Text(b.to_string()))],
        };
        // A naive join of ("x", "y\u{1f}z", "w") and ("x\u{1f}y", "z", "w") is the same string.
        let first = mk("x", "y\u{1f}z", "w");
        let second = mk("x\u{1f}y", "z", "w");
        let a = encode_key(&s, &first).unwrap();
        let b = encode_key(&s, &second).unwrap();
        assert_ne!(a, b);
        assert_eq!(decode_key(&s, &a).unwrap(), first);
        assert_eq!(decode_key(&s, &b).unwrap(), second);
        // A value that spells a whole length-prefixed part inside itself.
        let tricky = mk("x", "0001:y\u{1f}t0001:z", "");
        let plain = mk("x", "0001:y", "z");
        assert_ne!(encode_key(&s, &tricky).unwrap(), encode_key(&s, &plain).unwrap());
        assert_eq!(decode_key(&s, &encode_key(&s, &tricky).unwrap()).unwrap(), tricky);
        // Exhaustive over a tiny alphabet that includes the separator: every distinct key spells a
        // distinct string.
        let alphabet = ["", "a", "\u{1f}", "a\u{1f}", "\u{1f}a", "t0001:a"];
        let mut seen = std::collections::HashMap::new();
        for e in alphabet {
            for x in alphabet {
                for y in alphabet {
                    let k = mk(e, x, y);
                    let stored = encode_key(&s, &k).unwrap();
                    assert_eq!(seen.insert(stored, (e, x, y)), None, "collision at {e:?} {x:?} {y:?}");
                }
            }
        }
        assert_eq!(seen.len(), alphabet.len().pow(3));
    }

    #[test]
    fn a_bytes_entity_round_trips_and_is_bounded() {
        let s = KeySpec { namespace: "ns".into(), entity: EntityType::Bytes, dimensions: vec![] };
        for bytes in [vec![0u8], vec![0, 255, 16, 31], vec![0x1f; MAX_MEMBER_BYTES]] {
            let k = CellKey { entity: EntityValue::Bytes(bytes), dims: vec![] };
            let stored = encode_key(&s, &k).unwrap();
            assert_eq!(decode_key(&s, &stored).unwrap(), k);
        }
        let empty = CellKey { entity: EntityValue::Bytes(vec![]), dims: vec![] };
        assert!(encode_key(&s, &empty).is_err());
        let over = CellKey { entity: EntityValue::Bytes(vec![1; MAX_MEMBER_BYTES + 1]), dims: vec![] };
        assert!(encode_key(&s, &over).unwrap_err().contains("over the limit"));
    }

    #[test]
    fn keys_that_do_not_fit_the_spec_are_refused() {
        let s = spec();
        let missing = CellKey { entity: EntityValue::U64(1), dims: vec![None, None] };
        assert!(encode_key(&s, &missing).unwrap_err().contains("channel"));
        let wrong = CellKey { entity: EntityValue::U64(1), dims: vec![Some(DimValue::U32(1)), None] };
        assert!(encode_key(&s, &wrong).unwrap_err().contains("wrong type"));
        let short = CellKey { entity: EntityValue::U64(1), dims: vec![Some(DimValue::U16(1))] };
        assert!(encode_key(&s, &short).is_err());
        let entity = CellKey { entity: EntityValue::Text("x".into()), dims: vec![Some(DimValue::U16(1)), None] };
        assert!(encode_key(&s, &entity).unwrap_err().contains("entity"));
        assert!(decode_key(&s, "cells").is_err());
        let good = encode_key(&s, &key(1, 1, None)).unwrap();
        assert!(decode_key(&s, &format!("{good}x")).is_err());
        assert!(decode_key(&s, &good[..good.len() - 1]).is_err());
    }

    #[test]
    fn a_header_round_trips_and_a_snapshot_round_trips_with_values_only_this_path_makes() {
        let h = header();
        assert_eq!(CellHeader::decode(&h.encode().unwrap()).unwrap(), h);
        let cells = vec![
            Cell { member: Member::U64(u64::MAX), labels: vec![u64::MAX], lanes: vec![i64::MIN, -1] },
            Cell { member: Member::Bytes(vec![0, 255, 7]), labels: vec![0], lanes: vec![63, -64] },
            Cell { member: Member::U64(0), labels: vec![300], lanes: vec![i64::MAX, 0] },
        ];
        let bytes = encode_snapshot(&h, &cells).unwrap();
        assert_eq!(decode_snapshot(&h, &bytes).unwrap(), cells);
        // The wire form is the packed one: zigzag makes 63 the single byte 0x7e and -64 the byte 0x7f, so the second cell's lane pack is exactly [0x7e, 0x7f], and the member is field 2.
        let second = encode_snapshot(&h, &cells[1..2]).unwrap();
        assert_eq!(second, vec![0x0a, 0x0c, 0x12, 0x03, 0, 255, 7, 0x1a, 0x01, 0x00, 0x22, 0x02, 0x7e, 0x7f]);
        // Trailing garbage and an unknown field are refused, never skipped.
        assert!(decode_snapshot(&h, &[0x08, 0x01]).is_err());
        assert!(decode_snapshot(&h, &[0x0a, 0x05, 0x08]).is_err());
        assert!(CellHeader::decode(&[0x38, 0x01]).is_err());
    }

    #[test]
    fn an_old_snapshot_reads_a_new_trailing_lane_and_label_as_zero() {
        let old = header();
        let cells = vec![Cell { member: Member::U64(5), labels: vec![9], lanes: vec![4, 6] }];
        let bytes = encode_snapshot(&old, &cells).unwrap();
        let mut grown = old.clone();
        grown.labels.push("bucket".to_string());
        grown.lanes.push(LaneSpec { name: "likes".to_string(), reducer: LaneReducer::Count });
        let extended = check_header(&old, &grown).unwrap().expect("an additive change");
        assert_eq!(extended, grown);
        let read = decode_snapshot(&extended, &bytes).unwrap();
        assert_eq!(read[0].labels, vec![9, 0]);
        assert_eq!(read[0].lanes, vec![4, 6, 0]);
        // The same bytes under a header with FEWER lanes than the cell holds are refused.
        let mut shrunk = old.clone();
        shrunk.lanes.pop();
        assert!(decode_snapshot(&shrunk, &bytes).unwrap_err().contains("lanes per cell"));
    }

    #[test]
    fn a_header_check_accepts_the_same_header_and_additions_and_refuses_the_rest_naming_the_field() {
        let stored = header();
        assert_eq!(check_header(&stored, &stored).unwrap(), None);
        let refused = |change: &dyn Fn(&mut CellHeader), expect: &str| {
            let mut r = stored.clone();
            change(&mut r);
            let err = check_header(&stored, &r).expect_err(expect);
            assert!(err.contains(expect), "`{err}` does not name `{expect}`");
        };
        refused(&|r| r.key.namespace = "other".into(), "namespace");
        refused(&|r| r.key.entity = EntityType::Text, "entity");
        refused(&|r| { r.key.dimensions.pop(); }, "dimensions");
        refused(&|r| r.key.dimensions[0].name = "chan".into(), "dimension 0 name");
        refused(&|r| r.key.dimensions[0].ty = DimType::U32, "`channel` type");
        refused(&|r| r.key.dimensions[1].required = true, "`kind` required");
        refused(&|r| r.labels[0] = "bucket".into(), "label 0 name");
        refused(&|r| r.labels.clear(), "label `slot` is missing");
        refused(&|r| r.lanes.swap(0, 1), "lane 0 name");
        refused(&|r| r.lanes[1].name = "top".into(), "lane 1 name");
        refused(&|r| r.lanes[0].reducer = LaneReducer::Max, "`watch` reducer");
        refused(&|r| { r.lanes.pop(); }, "lane `peak` is missing");
        // A new field past the limit is refused by the limit, and an old header refuses growth.
        refused(&|r| { for i in 0..MAX_LANES_PER_CELL { r.lanes.push(LaneSpec { name: format!("x{i}"), reducer: LaneReducer::Sum }); } }, "lanes per cell");
        let mut old = stored.clone();
        old.version = 0;
        let mut grown = old.clone();
        grown.labels.push("bucket".into());
        assert!(check_header(&old, &grown).unwrap_err().contains("version"));
        assert_eq!(check_header(&old, &old).unwrap(), None);
    }

    #[test]
    fn each_lane_reducer_combines_as_documented() {
        use LaneReducer::*;
        let table: [(LaneReducer, i64, i64, i64, i64); 6] = [
            // reducer, first write, second write -> initial, combined
            (Sum, 5, 7, 5, 12),
            (Max, 5, 7, 5, 7),
            (Min, 5, 7, 5, 5),
            (First, 5, 7, 5, 5),
            (Last, 5, 7, 5, 7),
            (Count, 5, 7, 1, 2),
        ];
        for (reducer, a, b, initial, combined) in table {
            assert_eq!(reducer.initial(a), initial, "{}", reducer.name());
            assert_eq!(reducer.combine(reducer.initial(a), b), combined, "{}", reducer.name());
        }
        assert_eq!(Sum.combine(i64::MAX, 1), i64::MAX);
        assert_eq!(Sum.combine(i64::MIN, -1), i64::MIN);
        assert_eq!(Count.combine(i64::MAX, 0), i64::MAX);
    }

    #[test]
    fn merging_applies_each_lane_reducer_per_member_and_replaces_labels() {
        let h = header();
        let mut snapshot = Vec::new();
        let cell = |m: u64, slot: u64, watch: i64, peak: i64| Cell { member: Member::U64(m), labels: vec![slot], lanes: vec![watch, peak] };
        merge_into_snapshot(&h, &mut snapshot, &[cell(2, 1, 10, 3), cell(1, 1, 1, 1)]).unwrap();
        merge_into_snapshot(&h, &mut snapshot, &[cell(2, 9, 5, 8), cell(2, 9, 5, 2)]).unwrap();
        assert_eq!(snapshot, vec![cell(1, 1, 1, 1), cell(2, 9, 20, 8)]);
        assert!(snapshot.windows(2).all(|w| w[0].member < w[1].member), "sorted by member");
    }

    /// One row per bound: (name, refusal at limit + 1, acceptance at exactly the limit).
    #[test]
    fn every_bound_refuses_one_past_the_limit_and_accepts_the_limit() {
        fn spec_with_dims(n: usize) -> Result<(), String> {
            KeySpec {
                namespace: "ns".into(),
                entity: EntityType::U64,
                dimensions: (0..n).map(|i| DimSpec { name: format!("d{i}"), ty: DimType::U16, required: false }).collect(),
            }
            .validate()
        }
        fn header_with(labels: usize, lanes: usize) -> Result<(), String> {
            CellHeader {
                version: HEADER_VERSION,
                key: spec(),
                labels: (0..labels).map(|i| format!("l{i}")).collect(),
                lanes: (0..lanes).map(|i| LaneSpec { name: format!("n{i}"), reducer: LaneReducer::Sum }).collect(),
            }
            .validate()
        }
        fn cell_with_member(bytes: usize) -> Result<Vec<u8>, String> {
            let h = CellHeader { labels: vec![], lanes: vec![LaneSpec { name: "n".into(), reducer: LaneReducer::Sum }], ..header() };
            encode_snapshot(&h, &[Cell { member: Member::Bytes(vec![1; bytes]), labels: vec![], lanes: vec![1] }])
        }
        fn text_dim(bytes: usize) -> Result<String, String> {
            let s = KeySpec {
                namespace: "ns".into(),
                entity: EntityType::Text,
                dimensions: vec![DimSpec { name: "t".into(), ty: DimType::Text, required: true }],
            };
            encode_key(&s, &CellKey { entity: EntityValue::Text("e".into()), dims: vec![Some(DimValue::Text("x".repeat(bytes)))] })
        }
        fn cells_in_snapshot(n: usize) -> Result<Vec<u8>, String> {
            let h = CellHeader { labels: vec![], lanes: vec![LaneSpec { name: "n".into(), reducer: LaneReducer::Sum }], ..header() };
            let cells: Vec<Cell> = (0..n as u64).map(|m| Cell { member: Member::U64(m), labels: vec![], lanes: vec![1] }).collect();
            encode_snapshot(&h, &cells)
        }
        fn cells_in_request(n: usize) -> Result<(), String> {
            let h = CellHeader { labels: vec![], lanes: vec![LaneSpec { name: "n".into(), reducer: LaneReducer::Sum }], ..header() };
            let cells: Vec<Cell> = (0..n as u64).map(|m| Cell { member: Member::U64(m), labels: vec![], lanes: vec![1] }).collect();
            merge_into_snapshot(&h, &mut Vec::new(), &cells).map(|_| ())
        }
        fn namespace_bytes(n: usize) -> Result<(), String> {
            KeySpec { namespace: "n".repeat(n), entity: EntityType::U64, dimensions: vec![] }.validate()
        }
        fn entity_bytes(n: usize) -> Result<(), String> {
            let s = KeySpec { namespace: "ns".into(), entity: EntityType::Bytes, dimensions: vec![] };
            encode_key(&s, &CellKey { entity: EntityValue::Bytes(vec![1; n]), dims: vec![] }).map(|_| ())
        }
        fn name_bytes(n: usize) -> Result<(), String> {
            CellHeader {
                version: HEADER_VERSION,
                key: spec(),
                labels: vec!["x".repeat(n)],
                lanes: vec![LaneSpec { name: "n".into(), reducer: LaneReducer::Sum }],
            }
            .validate()
        }
        let rows: Vec<(&str, usize, Box<dyn Fn(usize) -> Result<(), String>>)> = vec![
            ("dimensions per key", MAX_KEY_DIMENSIONS, Box::new(spec_with_dims)),
            ("labels per cell", MAX_LABELS_PER_CELL, Box::new(|n| header_with(n, 1))),
            ("lanes per cell", MAX_LANES_PER_CELL, Box::new(|n| header_with(0, n))),
            ("member bytes", MAX_MEMBER_BYTES, Box::new(|n| cell_with_member(n).map(|_| ()))),
            ("text dimension bytes", MAX_TEXT_DIMENSION_BYTES, Box::new(|n| text_dim(n).map(|_| ()))),
            ("cells per request", MAX_CELLS_PER_REQUEST, Box::new(cells_in_request)),
            ("cells per snapshot", MAX_CELLS_PER_SNAPSHOT, Box::new(|n| cells_in_snapshot(n).map(|_| ()))),
            ("top-k", MAX_TOP_K, Box::new(validate_top_k)),
            ("namespace bytes", MAX_TEXT_DIMENSION_BYTES, Box::new(namespace_bytes)),
            ("entity bytes", MAX_MEMBER_BYTES, Box::new(entity_bytes)),
            ("field name bytes", MAX_NAME_BYTES, Box::new(name_bytes)),
        ];
        for (name, limit, run) in &rows {
            assert!(run(*limit).is_ok(), "{name}: exactly the limit ({limit}) must be accepted: {:?}", run(*limit));
            let err = run(limit + 1).expect_err(name);
            assert!(err.contains("over the limit"), "{name}: `{err}`");
        }
        assert_eq!(rows.len(), 11);
        // The limits are the ones the design states.
        assert_eq!((MAX_KEY_DIMENSIONS, MAX_LABELS_PER_CELL, MAX_LANES_PER_CELL), (8, 4, 16));
        assert_eq!((MAX_MEMBER_BYTES, MAX_TEXT_DIMENSION_BYTES, MAX_NAME_BYTES), (256, 256, 64));
        assert_eq!((MAX_CELLS_PER_REQUEST, MAX_CELLS_PER_SNAPSHOT, MAX_TOP_K), (1024, 4096, 1000));
        // Zero is refused where at least one is required.
        assert!(validate_top_k(0).is_err());
        assert!(header_with(0, 0).is_err());
        assert!(cell_with_member(0).is_err());
    }

    #[test]
    fn a_decoded_snapshot_is_held_to_the_same_bounds() {
        let h = CellHeader { labels: vec![], lanes: vec![LaneSpec { name: "n".into(), reducer: LaneReducer::Sum }], ..header() };
        let record = |m: u64| {
            let mut r = Vec::new();
            encode_varint_field(&mut r, 1, m);
            encode_bytes_field(&mut r, 4, &[2]);
            let mut out = Vec::new();
            encode_bytes_field(&mut out, 1, &r);
            out
        };
        let at_limit: Vec<u8> = (0..MAX_CELLS_PER_SNAPSHOT as u64).flat_map(record).collect();
        assert_eq!(decode_snapshot(&h, &at_limit).unwrap().len(), MAX_CELLS_PER_SNAPSHOT);
        let over: Vec<u8> = (0..=MAX_CELLS_PER_SNAPSHOT as u64).flat_map(record).collect();
        assert!(decode_snapshot(&h, &over).unwrap_err().contains("over the limit"));
    }

    #[test]
    fn a_merge_that_would_overflow_the_snapshot_changes_nothing() {
        let h = CellHeader { labels: vec![], lanes: vec![LaneSpec { name: "n".into(), reducer: LaneReducer::Sum }], ..header() };
        let mut snapshot: Vec<Cell> = (0..MAX_CELLS_PER_SNAPSHOT as u64).map(|m| Cell { member: Member::U64(m), labels: vec![], lanes: vec![1] }).collect();
        let before = snapshot.clone();
        let fresh = Cell { member: Member::U64(u64::MAX), labels: vec![], lanes: vec![1] };
        assert!(merge_into_snapshot(&h, &mut snapshot, &[fresh]).is_err());
        assert_eq!(snapshot, before);
        // A merge into an existing member at the limit is fine.
        let existing = Cell { member: Member::U64(0), labels: vec![], lanes: vec![4] };
        merge_into_snapshot(&h, &mut snapshot, &[existing]).unwrap();
        assert_eq!(snapshot[0].lanes, vec![5]);
    }

    fn one_lane_header() -> CellHeader {
        CellHeader {
            labels: vec![],
            lanes: vec![LaneSpec { name: "n".into(), reducer: LaneReducer::Sum }],
            ..header()
        }
    }

    fn request(h: &CellHeader, cells: &[(u64, i64)]) -> Vec<u8> {
        let cells: Vec<Cell> = cells
            .iter()
            .map(|(m, v)| Cell { member: Member::U64(*m), labels: vec![], lanes: vec![*v] })
            .collect();
        encode_value(h, &cells).unwrap()
    }

    #[test]
    fn a_value_carries_its_header_and_round_trips() {
        let h = header();
        let cells = vec![Cell { member: Member::U64(1), labels: vec![2], lanes: vec![3, -4] }];
        let value = encode_value(&h, &cells).unwrap();
        assert_eq!(decode_value(&value).unwrap(), (h.clone(), cells));
        assert_eq!(value[0], 0x0a, "the header is field 1");
        assert!(split_value(&[0x0a, 0x00]).is_err(), "an empty header is refused");
        assert!(decode_value(&[]).is_err(), "a value with no header is refused");
        let mut twice = value.clone();
        twice.extend(&value);
        assert!(split_value(&twice).is_err(), "a repeated field is refused");
    }

    #[test]
    fn a_request_merges_into_what_is_stored_and_the_first_write_creates_the_key() {
        let h = one_lane_header();
        let (first, effective, counts) = merge_request(None, None, &request(&h, &[(1, 10), (2, 20)])).unwrap();
        assert_eq!(effective, h);
        assert_eq!(counts, MergeCounts { created: 2, merged: 0 });
        let (second, _, counts) =
            merge_request(Some(&h), Some(&first), &request(&h, &[(2, 5), (3, 7)])).unwrap();
        assert_eq!(counts, MergeCounts { created: 1, merged: 1 });
        let (_, cells) = decode_value(&second).unwrap();
        let sums: Vec<(Member, i64)> = cells.into_iter().map(|c| (c.member, c.lanes[0])).collect();
        assert_eq!(sums, vec![(Member::U64(1), 10), (Member::U64(2), 25), (Member::U64(3), 7)]);
    }

    #[test]
    fn a_request_with_another_header_is_refused_and_one_that_adds_a_lane_upgrades_the_stored_snapshot() {
        let h = one_lane_header();
        let (stored, _, _) = merge_request(None, None, &request(&h, &[(1, 10)])).unwrap();
        let mut renamed = h.clone();
        renamed.lanes[0].name = "m".into();
        let err = merge_request(Some(&h), Some(&stored), &request(&renamed, &[(1, 1)])).unwrap_err();
        assert!(err.contains("lane 0 name"), "{err}");
        // Additive: a second lane. The stored member reads the new lane as zero, then merges.
        let mut grown = h.clone();
        grown.lanes.push(LaneSpec { name: "extra".into(), reducer: LaneReducer::Sum });
        let add = encode_value(
            &grown,
            &[Cell { member: Member::U64(1), labels: vec![], lanes: vec![1, 9] }],
        )
        .unwrap();
        let (upgraded, effective, _) = merge_request(Some(&h), Some(&stored), &add).unwrap();
        assert_eq!(effective, grown);
        let (own, cells) = decode_value(&upgraded).unwrap();
        assert_eq!(own, grown);
        assert_eq!(cells[0].lanes, vec![11, 9]);
        // Going back to the shorter header is refused: nothing is dropped.
        assert!(merge_request(Some(&grown), Some(&upgraded), &request(&h, &[(1, 1)])).is_err());
    }

    #[test]
    fn a_request_over_the_cell_limit_is_refused_and_at_the_limit_is_accepted() {
        let h = one_lane_header();
        let at: Vec<(u64, i64)> = (0..MAX_CELLS_PER_REQUEST as u64).map(|m| (m, 1)).collect();
        assert!(merge_request(None, None, &request(&h, &at)).is_ok());
        let over: Vec<(u64, i64)> = (0..=MAX_CELLS_PER_REQUEST as u64).map(|m| (m, 1)).collect();
        assert!(merge_request(None, None, &request(&h, &over))
            .unwrap_err()
            .contains("cells per request"));
    }

    #[test]
    fn the_version_gate_accepts_at_exactly_the_first_additive_version_and_refuses_below() {
        let mut at = header();
        at.version = ADDITIVE_FROM_VERSION;
        let mut grown = at.clone();
        grown.lanes.push(LaneSpec { name: "extra".into(), reducer: LaneReducer::Sum });
        assert_eq!(check_header(&at, &grown).unwrap().expect("additive").lanes.len(), 3);
        // A header older than the first additive version, spelled as a literal so no arithmetic
        // on the constant can overflow when a plant moves it.
        let mut below = at.clone();
        below.version = 0;
        assert!(ADDITIVE_FROM_VERSION > 0);
        assert!(check_header(&below, &grown).unwrap_err().contains("version"));
        // A header from a newer build than this one is refused at decode, never guessed at.
        let mut future = header().encode().unwrap();
        future[1] = (HEADER_VERSION + 1) as u8;
        assert!(CellHeader::decode(&future).unwrap_err().contains("newer"));
    }

    #[test]
    fn sorted_u64_members_are_stored_as_distances_and_cost_one_byte_each() {
        let h = one_lane_header();
        let cell = |m: u64| Cell { member: Member::U64(m), labels: vec![], lanes: vec![1] };
        let cells = [cell(100), cell(101), cell(105)];
        let bytes = encode_snapshot(&h, &cells).unwrap();
        // flag, then member 100 from zero, then +1, then +4; lane 1 zigzags to 2.
        assert_eq!(
            bytes,
            vec![
                0x10, 0x01, 0x0a, 0x05, 0x08, 100, 0x22, 0x01, 0x02, 0x0a, 0x05, 0x08, 1, 0x22,
                0x01, 0x02, 0x0a, 0x05, 0x08, 4, 0x22, 0x01, 0x02
            ]
        );
        assert_eq!(decode_snapshot(&h, &bytes).unwrap(), cells.to_vec());
        // Far-apart ids keep their full width and still round trip, through the largest id.
        let wide = [cell(0), cell(1 << 40), cell(u64::MAX)];
        let packed = encode_snapshot(&h, &wide).unwrap();
        assert_eq!(decode_snapshot(&h, &packed).unwrap(), wide.to_vec());
        // Ids a unit apart cost one byte each as distances; the same cells written out of order
        // (the raw form) carry every id at full width.
        let dense: Vec<Cell> = (0..1000u64).map(|i| cell(u64::MAX / 2 + i)).collect();
        let packed = encode_snapshot(&h, &dense).unwrap();
        let mut shuffled = dense.clone();
        shuffled.reverse();
        let raw = encode_snapshot(&h, &shuffled).unwrap();
        assert!(packed.len() < 1000 * 7 + 20, "{} bytes", packed.len());
        assert!(raw.len() > packed.len() + 1000 * 5, "raw {} vs packed {}", raw.len(), packed.len());
        assert_eq!(decode_snapshot(&h, &packed).unwrap(), dense);
    }

    #[test]
    fn members_out_of_order_or_repeated_are_written_as_they_are_and_read_back() {
        let h = one_lane_header();
        let cell = |m: u64| Cell { member: Member::U64(m), labels: vec![], lanes: vec![1] };
        for cells in [vec![cell(5), cell(3)], vec![cell(4), cell(4)], vec![cell(9), Cell { member: Member::Bytes(vec![1]), labels: vec![], lanes: vec![1] }, cell(2)]] {
            let bytes = encode_snapshot(&h, &cells).unwrap();
            assert_ne!(bytes[0], 0x10, "not delta coded: {cells:?}");
            assert_eq!(decode_snapshot(&h, &bytes).unwrap(), cells);
        }
        // A stored snapshot must ascend: merging into one that does not is refused, unchanged.
        let mut unsorted = vec![cell(5), cell(3)];
        let before = unsorted.clone();
        assert!(merge_into_snapshot(&h, &mut unsorted, &[cell(1)]).unwrap_err().contains("ascending"));
        assert_eq!(unsorted, before);
        assert!(is_strictly_ascending(&[cell(1), cell(2)]));
        assert!(!is_strictly_ascending(&[cell(2), cell(2)]));
    }

    #[test]
    fn a_distance_that_overflows_or_a_misplaced_flag_is_refused() {
        let h = one_lane_header();
        let record = |d: u64| {
            let mut r = Vec::new();
            encode_varint_field(&mut r, 1, d);
            encode_bytes_field(&mut r, 4, &[2]);
            let mut out = Vec::new();
            encode_bytes_field(&mut out, 1, &r);
            out
        };
        let mut flagged = vec![0x10, 0x01];
        flagged.extend(record(u64::MAX));
        flagged.extend(record(1));
        assert!(decode_snapshot(&h, &flagged).unwrap_err().contains("overflows"));
        let mut late = record(1);
        late.extend([0x10, 0x01]);
        assert!(decode_snapshot(&h, &late).unwrap_err().contains("flag"));
        assert!(decode_snapshot(&h, &[0x10, 0x01, 0x10, 0x01]).unwrap_err().contains("flag"));
        assert!(decode_snapshot(&h, &[0x10, 0x02]).unwrap_err().contains("flag"));
    }

    #[test]
    fn a_key_that_is_not_the_canonical_spelling_is_refused() {
        let s = spec();
        let good = encode_key(&s, &key(255, 10, None)).unwrap();
        assert_eq!(decode_key(&s, &good).unwrap(), key(255, 10, None));
        // Upper-case hex and a leading sign parse as numbers but are not what encode_key writes.
        assert!(decode_key(&s, &good.replace("u00000000000000ff", "u00000000000000FF")).unwrap_err().contains("canonical"));
        assert!(decode_key(&s, &good.replace("h000a", "h+00a")).is_err());
    }

    #[test]
    fn a_header_that_declares_too_many_fields_is_refused_while_it_is_read() {
        let mut h = header();
        h.labels = (0..MAX_LABELS_PER_CELL).map(|i| format!("l{i}")).collect();
        assert!(CellHeader::decode(&h.encode().unwrap()).is_ok());
        let mut bytes = h.encode().unwrap();
        encode_bytes_field(&mut bytes, 5, b"one-more");
        assert!(CellHeader::decode(&bytes).unwrap_err().contains("labels per cell"));
        let mut dims = header().encode().unwrap();
        for i in 0..MAX_KEY_DIMENSIONS {
            let mut record = Vec::new();
            encode_bytes_field(&mut record, 1, format!("d{i}").as_bytes());
            encode_varint_field(&mut record, 2, 0);
            encode_varint_field(&mut record, 3, 0);
            encode_bytes_field(&mut dims, 4, &record);
        }
        assert!(CellHeader::decode(&dims).unwrap_err().contains("key dimensions"));
    }

    /// A deterministic stream of bytes: no clock, no randomness a failure could not reproduce.
    fn xorshift(state: &mut u64) -> u64 {
        *state ^= *state << 13;
        *state ^= *state >> 7;
        *state ^= *state << 17;
        *state
    }

    /// Every reader is fed damaged input and must return, `Ok` or `Err`, without panicking. A panic
    /// fails the test, so the assertions below are about the PATH having been exercised: a damaged
    /// image must be refused in the great majority of cases, or the table is not damaging anything.
    #[test]
    fn a_malformed_or_truncated_image_is_an_error_and_never_a_panic() {
        let h = header();
        let cells = vec![
            Cell { member: Member::U64(3), labels: vec![7], lanes: vec![-5, 9] },
            Cell { member: Member::U64(900), labels: vec![0], lanes: vec![i64::MIN, i64::MAX] },
            Cell { member: Member::Bytes(vec![1, 2, 3]), labels: vec![300], lanes: vec![1, 2] },
        ];
        let value = encode_value(&h, &cells).unwrap();
        let header_bytes = h.encode().unwrap();
        let (_, packed) = split_value(&value).unwrap();
        let packed = packed.to_vec();
        let s = spec();
        let stored_key = encode_key(&s, &key(77, 5, Some("like"))).unwrap();
        let mut attempts = 0usize;
        let mut refused = 0usize;
        let mut run = |bytes: &[u8]| {
            attempts += 1;
            let results = [
                decode_value(bytes).map(|_| ()),
                CellHeader::decode(bytes).map(|_| ()),
                decode_snapshot(&h, bytes).map(|_| ()),
                merge_request(None, Some(bytes), &value).map(|_| ()),
                merge_request(Some(&h), None, bytes).map(|_| ()),
                decode_key(&s, &String::from_utf8_lossy(bytes)).map(|_| ()),
            ];
            refused += results.iter().filter(|r| r.is_err()).count();
        };
        // 1. Every strict prefix of each image, which is what a torn write leaves.
        for image in [&value[..], &header_bytes[..], &packed[..], stored_key.as_bytes()] {
            for cut in 0..image.len() {
                run(&image[..cut]);
            }
        }
        // 2. Every single-byte change of each image, at every position, to all 256 values.
        for image in [&value[..], &packed[..], stored_key.as_bytes()] {
            for at in 0..image.len() {
                for byte in 0..=255u8 {
                    let mut damaged = image.to_vec();
                    damaged[at] = byte;
                    run(&damaged);
                }
            }
        }
        // 3. Declared lengths far past the end, and varints that never end.
        run(&[0x0a, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x7f]);
        run(&[0x0a, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01]);
        run(&[0x80; 64]);
        run(&[0xff; 64]);
        // 4. Pseudo-random garbage of every small length.
        let mut state = 0x9e37_79b9_7f4a_7c15u64;
        for len in 0..200usize {
            for _ in 0..40 {
                let junk: Vec<u8> = (0..len).map(|_| xorshift(&mut state) as u8).collect();
                run(&junk);
            }
        }
        assert!(attempts > 20_000, "the table ran only {attempts} images");
        // Damage must mostly be noticed: six readers per image, and the great majority refuse.
        assert!(refused * 2 > attempts * 6 / 2, "only {refused} of {} reads refused", attempts * 6);
        // The undamaged images still read, so the refusals above are about the damage.
        assert!(decode_value(&value).is_ok());
        assert!(CellHeader::decode(&header_bytes).is_ok());
        assert!(decode_snapshot(&h, &packed).is_ok());
        assert!(decode_key(&s, &stored_key).is_ok());
        assert!(CellHeader::decode(&header_bytes[..header_bytes.len() - 1]).is_err());
        assert!(decode_snapshot(&h, &packed[..packed.len() - 1]).is_err());
        assert!(decode_key(&s, &stored_key[..stored_key.len() - 1]).is_err());
    }

    // --------------------------------------------------------------------------------------
    // The flat form reads and writes the bytes the plain form does
    // --------------------------------------------------------------------------------------

    fn flat_to_cells(flat: &FlatCells) -> Vec<Cell> {
        (0..flat.len())
            .map(|i| Cell {
                member: match flat.members[i] {
                    FlatMember::U64(v) => Member::U64(v),
                    FlatMember::Bytes { start, len } => Member::Bytes(flat.arena[start..start + len].to_vec()),
                },
                labels: flat.labels_of(i).to_vec(),
                lanes: flat.lanes_of(i).to_vec(),
            })
            .collect()
    }

    /// The merge as it was written over `Cell`s: the oracle the flat form is held to.
    fn plain_merge_request(
        newest: Option<&CellHeader>,
        existing: Option<&[u8]>,
        request: &[u8],
    ) -> Result<(Vec<u8>, CellHeader, MergeCounts), String> {
        let (requested, cells) = decode_value(request)?;
        validate_cells_in_request(cells.len())?;
        let effective = match newest {
            None => requested,
            Some(stored) => check_header(stored, &requested)?.unwrap_or_else(|| stored.clone()),
        };
        let mut snapshot = match existing {
            None => Vec::new(),
            Some(bytes) => {
                let (own, packed) = split_value(bytes)?;
                if own != effective {
                    check_header(&own, &effective)?;
                }
                decode_snapshot(&effective, packed)?
            }
        };
        let counts = merge_into_snapshot(&effective, &mut snapshot, &cells)?;
        Ok((encode_value(&effective, &snapshot)?, effective, counts))
    }

    fn random_header(state: &mut u64, labels: usize, lanes: usize) -> CellHeader {
        let reducers = [LaneReducer::Sum, LaneReducer::Max, LaneReducer::Min, LaneReducer::Last, LaneReducer::Count];
        CellHeader {
            version: HEADER_VERSION,
            key: spec(),
            labels: (0..labels).map(|i| format!("l{i}")).collect(),
            lanes: (0..lanes)
                .map(|i| LaneSpec { name: format!("n{i}"), reducer: reducers[xorshift(state) as usize % reducers.len()] })
                .collect(),
        }
    }

    /// A value spread over the sizes a varint changes length at, and the extremes.
    fn random_value(state: &mut u64) -> u64 {
        let width = xorshift(state) % 6;
        match width {
            0 => xorshift(state) % 4,
            1 => xorshift(state) % 200,
            2 => xorshift(state) % 20_000,
            3 => xorshift(state) % (1 << 40),
            4 => [0, u64::MAX, u64::MAX - 1, 1 << 63][xorshift(state) as usize % 4],
            _ => xorshift(state),
        }
    }

    fn random_cells(state: &mut u64, h: &CellHeader, count: usize, bytes_members: bool) -> Vec<Cell> {
        (0..count)
            .map(|_| {
                let member = if bytes_members && xorshift(state) % 3 == 0 {
                    let len = 1 + xorshift(state) as usize % 6;
                    Member::Bytes((0..len).map(|_| xorshift(state) as u8 % 4).collect())
                } else {
                    // A small range, so a request repeats members and merges into its own cells.
                    Member::U64(xorshift(state) % 40 + if xorshift(state) % 5 == 0 { 1 << 33 } else { 0 })
                };
                Cell {
                    member,
                    labels: (0..h.labels.len()).map(|_| random_value(state)).collect(),
                    lanes: (0..h.lanes.len()).map(|_| random_value(state) as i64).collect(),
                }
            })
            .collect()
    }

    #[test]
    fn the_flat_form_reads_and_writes_the_bytes_the_plain_form_does() {
        let mut state = 0x9e37_79b9_7f4a_7c15u64;
        let mut checked = 0;
        for round in 0..400 {
            let h = random_header(&mut state, round % 5 % 4, 1 + round % 3);
            let mut cells = random_cells(&mut state, &h, round % 17, round % 2 == 0);
            // A stored snapshot is sorted and without repeats, so build one the plain way.
            let mut sorted = Vec::new();
            merge_into_snapshot(&h, &mut sorted, &cells).unwrap();
            cells = sorted;
            let plain = encode_value(&h, &cells).unwrap();
            let (decoded_header, packed) = split_value(&plain).unwrap();
            assert_eq!(decoded_header, h);
            let flat = FlatCells::decode(&h, packed, 0).unwrap();
            assert_eq!(flat_to_cells(&flat), decode_snapshot(&h, packed).unwrap(), "round {round}: decoded cells");
            assert_eq!(flat.encode_value(&h).unwrap(), plain, "round {round}: encoded bytes");
            checked += 1;
        }
        assert_eq!(checked, 400);
    }

    #[test]
    fn a_flat_merge_stores_the_bytes_and_counts_the_plain_merge_does() {
        let mut state = 0x2545_f491_4f6c_dd1du64;
        let (mut merged_any, mut created_any, mut bytes_seen) = (0u64, 0u64, 0u64);
        for round in 0..400 {
            let h = random_header(&mut state, round % 3, 1 + round % 4);
            let mut stored = Vec::new();
            merge_into_snapshot(&h, &mut stored, &random_cells(&mut state, &h, round % 23, round % 2 == 0)).unwrap();
            let incoming = random_cells(&mut state, &h, 1 + round % 29, round % 3 == 0);
            let stored_value = encode_value(&h, &stored).unwrap();
            let request = encode_value(&h, &incoming).unwrap();
            let plain = plain_merge_request(Some(&h), Some(&stored_value), &request);
            let flat = merge_request(Some(&h), Some(&stored_value), &request);
            assert_eq!(flat, plain, "round {round}");
            let (_, _, counts) = flat.unwrap();
            merged_any += counts.merged as u64;
            created_any += counts.created as u64;
            bytes_seen += incoming.iter().filter(|c| matches!(c.member, Member::Bytes(_))).count() as u64;
            // The first write of a key, and a key with a header and no snapshot at the timestamp.
            assert_eq!(
                merge_request(None, None, &request),
                plain_merge_request(None, None, &request),
                "round {round}: first write"
            );
        }
        // The rounds must have exercised both arms of the merge and the bytes arena, or the
        // comparison above compared nothing.
        assert!(merged_any > 500 && created_any > 500 && bytes_seen > 100, "{merged_any} {created_any} {bytes_seen}");
    }

    #[test]
    fn a_flat_merge_refuses_what_the_plain_merge_refuses() {
        let h = one_lane_header();
        // Over the snapshot limit at the first created cell, and exactly at the limit.
        let full: Vec<(u64, i64)> = (0..MAX_CELLS_PER_REQUEST as u64).map(|m| (m, 1)).collect();
        let mut stored = request(&h, &full);
        for round in 1..5u64 {
            let more: Vec<(u64, i64)> = (0..MAX_CELLS_PER_REQUEST as u64).map(|m| (round * 10_000 + m, 1)).collect();
            let next = request(&h, &more);
            let plain = plain_merge_request(Some(&h), Some(&stored), &next);
            assert_eq!(merge_request(Some(&h), Some(&stored), &next), plain, "round {round}");
            match plain {
                Ok((value, _, _)) => stored = value,
                Err(err) => {
                    assert!(err.contains("cells per snapshot"), "{err}");
                    assert_eq!(round, 4, "the limit is {MAX_CELLS_PER_SNAPSHOT}, not before it");
                }
            }
        }
        // Wrong header, damaged stored value, damaged request, over-long request.
        let renamed = CellHeader { lanes: vec![LaneSpec { name: "m".into(), reducer: LaneReducer::Sum }], ..h.clone() };
        let one = request(&h, &[(1, 1)]);
        for (newest, existing, req) in [
            (Some(&h), Some(one.as_slice()), request(&renamed, &[(1, 1)])),
            (Some(&h), Some(&one[..one.len() - 1]), request(&h, &[(1, 1)])),
            (Some(&h), None, one[..one.len() - 1].to_vec()),
            (None, None, request(&h, &(0..=MAX_CELLS_PER_REQUEST as u64).map(|m| (m, 1)).collect::<Vec<_>>())),
        ] {
            let plain = plain_merge_request(newest, existing, &req);
            assert!(plain.is_err());
            assert!(merge_request(newest, existing, &req).is_err());
        }
    }

    #[test]
    fn points_planned_at_one_timestamp_merge_as_two_requests_one_after_the_other_do() {
        let mut state = 0x1234_5678_9abc_def1u64;
        let base = random_header(&mut state, 1, 2);
        // The second request adds a label and a lane, so the cells planned by the first are widened.
        let wider = CellHeader {
            labels: vec!["l0".into(), "extra".into()],
            lanes: vec![base.lanes[0].clone(), base.lanes[1].clone(), LaneSpec { name: "added".into(), reducer: LaneReducer::Sum }],
            ..base.clone()
        };
        for (second, widened) in [(&base, false), (&wider, true)] {
            let first_request = encode_value(&base, &random_cells(&mut state, &base, 12, true)).unwrap();
            let second_request = encode_value(second, &random_cells(&mut state, second, 12, true)).unwrap();
            // As two requests applied one after the other, each stored in between.
            let (stored_value, effective, first_counts) = plain_merge_request(None, None, &first_request).unwrap();
            let (expected, expected_header, second_counts) =
                plain_merge_request(Some(&effective), Some(&stored_value), &second_request).unwrap();
            // As two points planned at one timestamp, the cells carried in between.
            let first = std::rc::Rc::new(effective.clone());
            let (header, cells, counts) = merge_point(None, Existing::Nothing, &first_request).unwrap();
            assert_eq!(counts, first_counts);
            let (header, cells, counts) =
                merge_point(Some(&header), Existing::Planned { header: header.clone(), cells }, &second_request).unwrap();
            assert_eq!(counts, second_counts);
            assert_eq!(*header, expected_header);
            assert_eq!(widened, *header != *first, "the second request {} the header", if widened { "extends" } else { "keeps" });
            assert_eq!(cells.encode_value(&header).unwrap(), expected);
        }
    }

    #[test]
    fn a_flat_value_is_written_in_the_one_allocation_the_size_pass_measured() {
        let mut state = 7u64;
        let h = random_header(&mut state, 2, 3);
        let mut cells = Vec::new();
        merge_into_snapshot(&h, &mut cells, &random_cells(&mut state, &h, 30, true)).unwrap();
        let (_, packed) = split_value(&encode_value(&h, &cells).unwrap()).map(|(a, b)| (a, b.to_vec())).unwrap();
        let flat = FlatCells::decode(&h, &packed, 0).unwrap();
        let value = flat.encode_value(&h).unwrap();
        assert_eq!(value.len(), value.capacity(), "the value was grown instead of sized");
        assert_eq!(value, encode_value(&h, &cells).unwrap());
    }

    /// The counting allocator is installed only under `alloc-probe`, so these are `#[ignore]`d and
    /// pinned separately in CI. They show that the allocations a cell or a key costs do not depend
    /// on how big its key or member is: only what the bytes cost may grow.
    #[cfg(feature = "alloc-probe")]
    mod probe {
        use super::*;
        use crate::alloc_probe::Probe;

        fn counted<T>(work: impl FnOnce() -> T) -> (u64, u64) {
            let probe = Probe::start();
            std::hint::black_box(work());
            let counts = probe.stop();
            (counts.allocs, counts.alloc_bytes)
        }

        fn text_key(spec: &KeySpec, len: usize) -> CellKey {
            let _ = spec;
            CellKey {
                entity: EntityValue::Text("e".repeat(len)),
                dims: vec![Some(DimValue::Text("d".repeat(len)))],
            }
        }

        #[test]
        #[ignore = "measurement: run with --features alloc-probe --ignored"]
        fn a_cell_set_key_costs_one_allocation_whatever_its_size() {
            let spec = KeySpec {
                namespace: "ns".into(),
                entity: EntityType::Text,
                dimensions: vec![DimSpec { name: "t".into(), ty: DimType::Text, required: true }],
            };
            let (small_key, large_key) = (text_key(&spec, 8), text_key(&spec, MAX_TEXT_DIMENSION_BYTES));
            let (small_allocs, small_bytes) = counted(|| encode_key(&spec, &small_key).unwrap());
            let (large_allocs, large_bytes) = counted(|| encode_key(&spec, &large_key).unwrap());
            println!("key: {small_allocs} allocs {small_bytes} B at 8; {large_allocs} allocs {large_bytes} B at 256");
            // The control: the probe sees SIZE, so equal counts are not a blind instrument.
            assert!(large_bytes > small_bytes + 400, "the probe cannot see bytes: {small_bytes} vs {large_bytes}");
            assert_eq!(small_allocs, 1, "a key is built in one allocation");
            assert_eq!(large_allocs, small_allocs, "allocations depend on key size");
        }

        /// A merge request costs the same allocations at 50 cells as at 1,000: the cells are held
        /// in a few vectors for the whole snapshot. Measured equal; the bound is a handful.
        #[test]
        #[ignore = "measurement: run with --features alloc-probe --ignored"]
        fn a_merge_request_costs_the_same_allocations_at_any_cell_count() {
            let h = CellHeader {
                labels: vec!["slot".into()],
                lanes: vec![LaneSpec { name: "n".into(), reducer: LaneReducer::Sum }],
                ..header()
            };
            let value_of = |count: u64| -> Vec<u8> {
                let cells: Vec<Cell> = (0..count)
                    .map(|m| Cell { member: Member::U64(m * 3), labels: vec![1], lanes: vec![1] })
                    .collect();
                encode_value(&h, &cells).unwrap()
            };
            let allocs = |count: u64| -> (u64, u64) {
                let (stored, request) = (value_of(count), value_of(count));
                counted(|| merge_request(Some(&h), Some(&stored), &request).unwrap())
            };
            let (few, few_bytes) = allocs(50);
            let (many, many_bytes) = allocs(1000);
            println!("merge_request: {few} allocs {few_bytes} B at 50 cells; {many} allocs {many_bytes} B at 1000");
            assert!(many_bytes > few_bytes * 4, "the probe cannot see the cell count");
            assert!(few > 5, "the probe is blind: {few}");
            assert!(many <= few + 4, "allocations grew with the cell count: {few} -> {many}");
        }

        #[test]
        #[ignore = "measurement: run with --features alloc-probe --ignored"]
        fn a_cell_costs_the_same_allocations_at_any_member_size() {
            let h = CellHeader {
                labels: vec!["slot".into()],
                lanes: vec![LaneSpec { name: "n".into(), reducer: LaneReducer::Sum }],
                ..header()
            };
            let cells_of = |count: usize, width: usize| -> Vec<Cell> {
                (0..count)
                    .map(|i| {
                        let mut member = vec![b'm'; width];
                        member[..8].copy_from_slice(&(i as u64).to_be_bytes());
                        Cell { member: Member::Bytes(member), labels: vec![1], lanes: vec![1] }
                    })
                    .collect()
            };
            // Allocations per cell as the SLOPE between two counts, so the fixed cost divides out.
            let slope = |width: usize| -> (f64, f64) {
                let (mut allocs, mut bytes) = ([0u64; 2], [0u64; 2]);
                for (i, count) in [10usize, 110].into_iter().enumerate() {
                    let cells = cells_of(count, width);
                    let (a, b) = counted(|| {
                        let mut snapshot = Vec::new();
                        merge_into_snapshot(&h, &mut snapshot, &cells).unwrap();
                        let packed = encode_snapshot(&h, &snapshot).unwrap();
                        decode_snapshot(&h, &packed).unwrap()
                    });
                    allocs[i] = a;
                    bytes[i] = b;
                }
                ((allocs[1] - allocs[0]) as f64 / 100.0, (bytes[1] - bytes[0]) as f64 / 100.0)
            };
            let (small_allocs, small_bytes) = slope(16);
            let (large_allocs, large_bytes) = slope(MAX_MEMBER_BYTES);
            println!("per cell: {small_allocs:.2} allocs {small_bytes:.0} B at 16; {large_allocs:.2} allocs {large_bytes:.0} B at 256");
            assert!(large_bytes > small_bytes + 400.0, "the probe cannot see bytes");
            assert!(small_allocs > 1.0, "the probe sees no per-cell allocation at all");
            assert!(
                (large_allocs - small_allocs).abs() < 0.01,
                "per-cell allocations grew with the member: {small_allocs:.2} vs {large_allocs:.2}"
            );
        }
    }
}
