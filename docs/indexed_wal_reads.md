# Indexed compressed WAL reads

Large binary WAL records can carry many independently addressed blocks. Reading
one of those blocks no longer requires inflating every other block in the batch.
Log-ID resolution uses ordered sealed-segment names to skip earlier segments
without opening their headers; it still validates the candidate's actual header.
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

Checksum composition uses compile-time Castagnoli shift operators rather than
rebuilding matrices for every compressed frame. The immutable table occupies
8 KiB per process on 64-bit hosts (4 KiB on 32-bit hosts), with no lazy
initialization, locks, or heap allocation. The combined CRC32C is unchanged for
every byte length; this does not skip frame checks or introduce a new encoding.

A point read checks the directory and the selected frame, not the actual contents
of unrelated frames it does not read. Full replay still reads and checks the
complete outer record. CRC32C detects accidental corruption; it is not an
authentication mechanism. Unknown directory versions fall back to full replay
decoding rather than making the underlying Zstd stream unreadable.

Large records can also use the WAL's packed transport across block-footer slots.
A seekable view validates that transport's header and every footer, then maps
logical record ranges to physical chunks. The selected read includes those small
transport checks without reading or allocating the unrelated payload prefix.

## Memory and amplification

Each WAL store retains at most 4 MiB of directory-cache allocation, accounting for
descriptor vector capacities, offsets, shared ownership, and queue capacity. This
is additional metadata outside the configured payload-cache budget. It does not
cache decoded block groups. In-flight buffers, allocator overhead, and the
existing legacy decoded-record cache remain separate memory costs.

### Checked reader reuse

On systems with stable Unix file identities, carried-block reads also reuse
sealed-segment descriptors. Every reuse checks the current path's device, inode,
length, modification time, and change time. Replaced or deleted paths are not
served through old descriptors. The base header is read from the descriptor that
will serve the bytes, not from a separately opened path.

Descriptor lookup metadata is bounded to 256 KiB and 128 descriptors per WAL
store. A separate ordered segment-list cache retains at most eight shard lists
and 1 MiB. These budgets are additional to the 4 MiB compressed-record directory
cache and the configured payload cache. No decoded payload is retained by the
reader cache. Directory changes invalidate lists; a failed lookup refreshes once
even if filesystem timestamps did not distinguish a concurrent roll.

Active files are not retained because they can grow and be replaced. The sum of
file lengths represented by retained sealed descriptors is limited to 64 MiB.
Owned reclaim closes shard descriptors before unlinking or replacing files, so
they do not keep its reclaimed data alive. An external process can unlink a file
between operations; its descriptor is retired on the next lookup, eviction, or
store shutdown, with the represented-file-size limit bounding delayed release.
Opening or listing under descriptor pressure can retire an owned cached reader
and retry; the cache does not require all 128 descriptor slots to be available.

Unix point reads use positional I/O instead of changing a shared file cursor.
Every point read still validates the transport header, every footer, and the
selected compressed data. Other platforms retain uncached descriptor lookup and
the seek/read implementation when stable identity or positional I/O is not
available. Write encoding, acknowledgement timing, and durability barriers do not
change.

The legacy decoded-record cache uses weak log identities. Its bounded decoded
values can outlive a caller, but they do not retain a WAL owner, its descriptors,
or its owned scratch directory after the real store handles are dropped.

Small compression groups and the directory can increase WAL bytes compared with
whole-record compression. Measure accepted writes, storage-miss reads, and log
bytes together when selecting a deployment policy. Fewer requested file bytes or
decoded bytes do not by themselves prove lower device I/O or physical write
amplification. The WAL read counter records requested bytes, not device traffic.

Synchronous shared hash-row batches can leave small packed payloads out of the
payload cache until a read requests them. The WAL append registers their locations
before releasing the mutation guard, and acknowledgement uses the same configured
durability barrier. This avoids write-side payload copies and cache admission;
the first read may instead fetch and decompress a WAL compression group. Measure
that tradeoff with both active writers and readers, not write throughput alone.

An overwrite refreshes the payload cache when an overwritten resident frame is
already cached. A single probe of the first resident address in the shared
projection makes this decision before retiring the old frame. Cold writes do not
displace the read-demanded working set, while hot packed rows can remain cached
across updates. This is an admission policy, not a guarantee that every partial
projection or capacity-constrained write remains cached.

This policy requires a unique shared projection, deferred materialization, and
cache-independent timestamp comparisons for conditional rows. Async writes, bulk
ingestion, memory-limited configurations, replay, consensus apply, generic command
batches, missing timestamp columns, and stored timestamp values over 64 bytes keep
eager caching. Set `TS_HASH_WRITE_AROUND=0` before startup to retain eager caching
on every path. No WAL encoding, cache capacity, or synchronization policy changes.
