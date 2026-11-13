# Tombstone Identity Lookups

Fresh order members often have no retained tombstone. Large histories use a compact
negative-membership filter to avoid searching the row list for those absent identities.
Possible matches still compare model kind, object key and element name exactly; a filter
result alone cannot identify a row, remove a row or update a block address.

The filter uses the Rust `bloomfilter` crate. It starts at 64 rows, grows with the history,
and is rebuilt or released after substantial shrinkage. It is derived from existing rows
on decode and rebuild, and never serialized. Row order and first-match behavior are unchanged.
Block-address updates cannot change the identity; removals may leave extra positive bits,
which only cause an exact scan. No retention rule, WAL fence, deadline or retry changes.

Empty buckets still hold a one-word optional pointer. A nonempty row allocation gains one
optional filter pointer; filter allocation starts only at the threshold. The filter bitmap
budget is tested separately from full process RSS and does not duplicate names or addresses.
Construction failure falls back to exact scans.

Tests compare scalar and filtered lookups, growth, shrinkage, duplicate identities, forced
positive results for absent names, clone/decode and byte-identical serialized row sequences.
Bucket-level work counters cover both missing lookup and missing re-add cleanup paths.
Reducing those visits is not by itself an end-to-end or large-corpus acceptance result.
