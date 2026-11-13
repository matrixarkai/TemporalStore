# Container Ordinal Allocation

Container writes retain an existing element's ordinal. A new element uses the highest
live ordinal plus one, or the existing zero fallback past the addressable ceiling.
Deleting the highest element still frees that position; ordinals do not become identities.

Each recorded map now caches summaries for at most 64 hot objects. A summary tracks the
highest two live ordinals and their multiplicities, not one row per element. Ordinary
installs and removals maintain it at the private mutation boundary. Replacing the highest
member can reuse the known runner-up without rescanning the whole container. If repeated
deletions leave the next maximum unknown, the summary is discarded and recomputed.

Replay removal, durable folding, reconciliation and compaction address rewrites invalidate
affected summaries. Decode and clone start cold. Key adoption retains the shared canonical
key handle. The cache is transient, bounded per map and omitted from serialization;
no WAL, snapshot or address format changes, and no deadline or retry changes are needed.

The write-path work test drives hash, set, sorted-set and list containers at 64, 128 and
256 elements. Each build examines one sibling after its initial empty lookup, instead of
repeating the growing sibling scan on every append. Other write-path work remains, so
this count is not an end-to-end latency or large-corpus acceptance result.
