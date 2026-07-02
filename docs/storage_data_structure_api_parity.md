# Storage Data Structure And API Parity

This shared C++/Rust case verifies the storage surfaces that callers and
readiness gates rely on.

Required Rust fields:

- slot/object/page authority from the first-class slot index
- SlotStore layout states
- ObjectManager runtime state
- block-address metadata: segment, offset, length, block id, object id, routing slot, extent id, checksum
- block-store segment index entries
- stream-backed extent lifecycle and legacy zone aliases
- StorageManager phase order, pressure signals, and merged dump/load surface

C++ should emit comparable `Index -> SlotMap -> SlotNode -> PageIndex/Object`,
Stream/Zone, PageStore/BlockStore, and StorageManager fields so the shared
runner can compare outputs family by family.
