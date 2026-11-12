# Indexed compressed WAL reads

Large binary WAL records can carry many independently addressed blocks. Reading
one of those blocks no longer requires inflating every other block in the batch.
The writer compresses the record header separately and groups carried blocks into
independent Zstd frames, targeting 4 KiB of uncompressed bytes per group. A block
larger than that target remains whole in its own frame.

## Selection and compatibility

The default indexed path applies when binary framing and WAL compression are
enabled, the record carries at least eight blocks, and the measured protobuf size
is between 64 KiB and 16 MiB. Other records retain the existing encoding path.
An oversized directory also falls back to a single compressed frame.

The directory occupies a standard Zstd skippable frame. Decompressing the complete
stream yields exactly the same protobuf bytes as the existing borrowing encoder:
the record header followed by the staged-block fields. The compressed payload
marker and outer length/checksum envelope are unchanged. The existing streaming
replay decoder accepts the concatenated frames without interpreting the directory.
Legacy JSON, escaped protobuf, raw protobuf, and single-frame compressed records
continue through their existing readers.

Set `TS_WAL_INDEXED_BLOCKS=0` to disable indexing for new records. Reading an
indexed record does not consult this setting. The switch does not disable WAL
compression, change acknowledgement timing, or remove a durability barrier.

## Point-read integrity

The directory stores compressed-frame lengths, decoded lengths, checksums, and
block positions. A cold point read binds the directory to the outer record CRC32C
using checksum combination, validates its ranges and record identity, then reads
and checksums the selected compressed frame. It decodes the selected block's
protobuf and checks its object and element identity before returning bytes.

A point read checks the directory and the selected frame, not the actual contents
of unrelated frames it does not read. Full replay still reads and checks the
complete outer record. CRC32C detects accidental corruption; it is not an
authentication mechanism. Unknown directory versions fall back to full replay
decoding rather than making the underlying Zstd stream unreadable.

## Memory and amplification

Each WAL store retains at most 4 MiB of directory-cache allocation, accounting for
descriptor vector capacities, offsets, shared ownership, and queue capacity. This
is additional metadata outside the configured payload-cache budget. It does not
cache decoded block groups. In-flight buffers, allocator overhead, and the
existing legacy decoded-record cache remain separate memory costs.

Small compression groups and the directory can increase WAL bytes compared with
whole-record compression. Measure accepted writes, storage-miss reads, and log
bytes together when selecting a deployment policy. Fewer requested file bytes or
decoded bytes do not by themselves prove lower device I/O or physical write
amplification. The WAL read counter records requested bytes, not device traffic.
