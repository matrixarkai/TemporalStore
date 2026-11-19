// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! The kind an outcome item names, held without an allocation when the kind is a known one.
//!
//! Every staged outcome carried its kind as a `String`, built with `to_string()` from a string
//! literal on every write: a malloc and a copy of "control_counter" or "hash" per staged item, for
//! a label that is one of a few dozen fixed names. The kinds common enough to have a wire code
//! (`wal_proto::KIND_CODES`) are held as borrows of that roster; any other kind is held as an owned
//! string, so an unknown name still works exactly as it did.
//!
//! WHAT DOES NOT MOVE: the serialized form. The type serializes as the plain string and reads back
//! from the plain string, so a record written before this change reads the same and a record
//! written after it is byte-identical.
//!
//! rust-internal: an allocation shape, no product behaviour

use std::borrow::{Borrow, Cow};
use std::fmt;

use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// Kinds an outcome names that have no wire code: they travel as their name, and are held as a
/// borrow all the same. The list only decides what allocates; a kind missing from it is correct.
pub(crate) const UNCODED_KINDS: &[&str] = &["record", "page", "cell_retry"];

/// The roster entry for `kind`, if it is a known one.
fn known(kind: &str) -> Option<&'static str> {
    crate::wal_proto::KIND_CODES
        .iter()
        .chain(UNCODED_KINDS)
        .find(|known| **known == kind)
        .copied()
}

#[derive(Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct OutcomeKind(Cow<'static, str>);

impl OutcomeKind {
    /// The kind named `kind`: a borrow of the roster entry when it is a known kind, an owned copy
    /// otherwise.
    pub fn of(kind: &str) -> Self {
        match known(kind) {
            Some(known) => Self(Cow::Borrowed(known)),
            None => Self(Cow::Owned(kind.to_owned())),
        }
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Whether this kind is held without an allocation. For the guard that pins the shape.
    pub fn is_borrowed(&self) -> bool {
        matches!(self.0, Cow::Borrowed(_))
    }
}

impl From<&str> for OutcomeKind {
    fn from(kind: &str) -> Self {
        Self::of(kind)
    }
}

impl From<&String> for OutcomeKind {
    fn from(kind: &String) -> Self {
        Self::of(kind)
    }
}

impl From<String> for OutcomeKind {
    fn from(kind: String) -> Self {
        match known(&kind) {
            Some(known) => Self(Cow::Borrowed(known)),
            None => Self(Cow::Owned(kind)),
        }
    }
}

impl From<OutcomeKind> for String {
    fn from(kind: OutcomeKind) -> Self {
        kind.0.into_owned()
    }
}

impl std::ops::Deref for OutcomeKind {
    type Target = str;
    fn deref(&self) -> &str {
        &self.0
    }
}

impl Borrow<str> for OutcomeKind {
    fn borrow(&self) -> &str {
        &self.0
    }
}

impl AsRef<str> for OutcomeKind {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for OutcomeKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&*self.0, f)
    }
}

impl fmt::Display for OutcomeKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&*self.0, f)
    }
}

impl PartialEq<str> for OutcomeKind {
    fn eq(&self, other: &str) -> bool {
        &*self.0 == other
    }
}

impl PartialEq<&str> for OutcomeKind {
    fn eq(&self, other: &&str) -> bool {
        &*self.0 == *other
    }
}

impl PartialEq<String> for OutcomeKind {
    fn eq(&self, other: &String) -> bool {
        &*self.0 == other.as_str()
    }
}

impl Serialize for OutcomeKind {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for OutcomeKind {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        String::deserialize(deserializer).map(Self::from)
    }
}
