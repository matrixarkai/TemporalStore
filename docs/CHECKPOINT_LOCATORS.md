# Checkpoint Locator Encoding

New served indexes use binary codec 3. The reader also accepts codec 2 at the
supported index struct version. Both codecs use named MessagePack fields inside
the existing compressed container; the struct version and logical index model
are unchanged.

Codec 3 stores `wal_resident_blocks` as three integer arrays: key deltas, log-ID
deltas, and sequence deltas. Entries are sorted by log ID, sequence, and key.
Key and sequence arithmetic is modular over 64 bits, so decreasing values and
the full integer domain round-trip without truncation. Log IDs are monotonic;
the decoder rejects overflow, unequal column lengths, extra columns, and
duplicate reconstructed keys. Multiple keys may intentionally name the same
log record.

For a resident single-block string, codec 3 stores its object record beside the
block entry under `r`. The block supplies the address and length; the record
retains its other fields, including flags, model ID, and logical-size class.
The decoder rebuilds the object row before filing ownership. A row is omitted
from the separate object table only when the existing ownership index names that
exact object and its sole live block matches the row's address. Released,
ambiguous, multi-block, or unmatched rows keep their separate representation.
Duplicate embedded records and conflicting addresses are rejected. A bare block
or bucket decoder refuses embedded object records instead of dropping them.

The runtime table remains keyed by the complete locator identity. No recovery
locator is discarded by this encoding. Codec-2 maps remain readable without
replaying a covered log, including when that log has already been reclaimed.
Older binaries reject codec 3 as unsupported rather than interpreting its arrays
as an older map.

Upgrade every replay reader before resuming writes or maintenance. Downgrading
requires a backup written in the older supported format. An encoding-size
measurement does not establish recovery correctness, runtime memory savings,
or end-to-end maintenance throughput; those require separate validation.
