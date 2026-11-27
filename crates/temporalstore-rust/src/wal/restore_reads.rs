// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Sequence-qualified restore reads; only setup and cold ownership run under the writer.

use super::*;

struct Span {
    path: PathBuf,
    base: u64,
    header: u64,
    end: u64,
    metadata: fs::Metadata,
    sealed: bool,
}

struct Session {
    counts: Arc<point_reads::Counts>,
    spans: Vec<Span>,
    active: Option<File>,
    requests: Vec<(u64, u64)>,
    // Drop handles before their retention/ownership and release admission last.
    _lease: WriteAheadLogReadLease,
    _writer: Arc<ShardWriter>,
    _permit: point_reads::Permit,
}

fn prepare(
    store: &LocalWriteAheadLogStore,
    shard: ShardId,
    requests: &[(u64, u64)],
    writable_intent: bool,
) -> Result<Option<Session>, WriteAheadLogError> {
    if !writable_intent || requests.is_empty() || requests.len() > 4096 || requests.iter().any(|(_, seq)| *seq == 0) {
        return Ok(None);
    }
    let writer = store.shard_writer(shard);
    let (state, counts) = {
        let inner = writer.lock_inner_raw();
        (inner.point_reads.clone(), inner.point_read_counts.clone())
    };
    let permit = state.enter();
    let lease = {
        let mut retention = writer.coordinator.read_retention.lock().expect("read retention lock poisoned");
        // Unlike already-protected point reads, restore admission must precede the GC plan.
        if retention.collecting {
            return Ok(None);
        }
        let sequence = requests.iter().map(|(_, sequence)| *sequence).min().expect("nonempty above");
        store.register_read_lease(&writer.coordinator, &mut retention, sequence)
    };
    let mut spans = Vec::new();
    let mut active = None;
    {
        let mut inner = writer.lock_inner_raw();
        // Do not borrow an observer's cached sequence as ownership. Cold reconciliation
        // remains one serialized, fallible repair under the lifetime append flock.
        WalAppendLock::acquire(&mut inner, shard)?;
        flush_pending_locked(&mut inner, shard)?;
        let (last, active_end) = resolve_last_sequence_for_append(&mut inner, shard, store.flat_append())?;
        inner.last_sequence_by_shard.insert(shard, last);
        let active_path = active_wal_path(&mut inner, shard);
        for piece in wal_segment_pieces(&inner.root, shard) {
            let mut file = File::open(&piece.path)?;
            let (base, header) = read_wal_base_from_file(&mut file)?;
            let metadata = file.metadata()?;
            let sealed = piece.path.as_path() != active_path.as_path();
            // Sealed extents cannot grow under this flock. Active reservations can:
            // restrict them to the owning writer's checked logical end instead.
            let end = if sealed { metadata.len() } else { active_end };
            if end > metadata.len() || end < header {
                return Err(WalPieceChanged::io_error());
            }
            if !sealed {
                active = Some(file);
            }
            if end > header {
                spans.push(Span { path: piece.path, base, header, end, metadata, sealed });
            }
        }
    }
    Ok(Some(Session {
        _writer: writer, _permit: permit, _lease: lease, counts, spans, active,
        requests: requests.to_vec(),
    }))
}

impl Session {
    fn load(&mut self) -> Result<Vec<Option<Vec<u8>>>, WriteAheadLogError> {
        let mut open: Option<(usize, File)> = None;
        let mut frames = Vec::with_capacity(self.requests.len());
        for &(log_id, sequence) in &self.requests {
            let after = self.spans.partition_point(|span| span.base <= log_id);
            let hit = after.checked_sub(1).filter(|&i| log_id - self.spans[i].base < self.spans[i].end - self.spans[i].header);
            let Some(index) = hit else { frames.push(None); continue; };
            let span = &self.spans[index];
            let file = if span.sealed {
                if open.as_ref().map(|(i, _)| *i) != Some(index) {
                    drop(open.take());
                    let file = File::open(&span.path)?;
                    if point_readers::stamp(&file.metadata()?) != point_readers::stamp(&span.metadata) {
                        return Err(WalPieceChanged::io_error());
                    }
                    open = Some((index, file));
                }
                &mut open.as_mut().expect("opened above").1
            } else {
                self.active.as_mut().expect("active descriptor captured with span")
            };
            let current = file.metadata()?;
            if !same_observed_wal_metadata(Some(&span.metadata), Some(&current)) || current.len() < span.end {
                return Err(WalPieceChanged::io_error());
            }
            let physical = span.header + (log_id - span.base);
            file.seek(SeekFrom::Start(physical))?;
            let raw = {
                let bounded = (&mut *file).take(span.end - physical);
                let mut reader = BufReader::with_capacity(1024, bounded);
                read_raw_record(&mut reader)?
            };
            if let Some(record) = raw.as_ref() {
                // Completed framed-record I/O only; header probes and cold scans are excluded.
                self.counts.complete(record.physical_bytes);
                if decode_wal_line(&record.bytes)?.sequence != sequence {
                    return Err(WriteAheadLogError::Corruption("restore record sequence changed".into()));
                }
            }
            if span.sealed && point_readers::stamp(&file.metadata()?) != point_readers::stamp(&span.metadata) {
                return Err(WalPieceChanged::io_error());
            }
            frames.push(raw.map(|record| record.bytes));
        }
        Ok(frames)
    }
}

pub(super) fn read(
    store: &LocalWriteAheadLogStore,
    shard: ShardId,
    requests: &[(u64, u64)],
    writable_intent: bool,
) -> Result<Option<Vec<Option<Vec<u8>>>>, WriteAheadLogError> {
    let Some(mut session) = prepare(store, shard, requests, writable_intent)? else { return Ok(None); };
    #[cfg(test)]
    notify_test(shard, true, requests);
    let frames = session.load()?;
    #[cfg(test)]
    {
        let present = requests.iter().zip(&frames).filter_map(|(request, frame)|
            frame.as_ref().map(|_| *request)).collect::<Vec<_>>();
        notify_test(shard, false, &present);
    }
    Ok(Some(frames))
}

#[cfg(test)]
type TestHook = Box<dyn FnMut(ShardId, bool, &[(u64, u64)])>;

#[cfg(test)]
thread_local! {
    static TEST_HOOK: std::cell::RefCell<Option<TestHook>> = const { std::cell::RefCell::new(None) };
}

#[cfg(test)]
fn notify_test(shard: ShardId, prepared: bool, requests: &[(u64, u64)]) {
    TEST_HOOK.with(|slot| {
        if let Some(hook) = slot.borrow_mut().as_mut() { hook(shard, prepared, requests); }
    });
}

#[cfg(test)]
pub(super) fn with_hook_for_test<R>(hook: TestHook, run: impl FnOnce() -> R) -> R {
    struct Restore(Option<TestHook>);
    impl Drop for Restore {
        fn drop(&mut self) {
            TEST_HOOK.with(|slot| { *slot.borrow_mut() = self.0.take(); });
        }
    }
    let _restore = Restore(TEST_HOOK.with(|slot| slot.replace(Some(hook))));
    run()
}

#[cfg(test)]
#[path = "restore_batch_tests.rs"]
mod tests;
