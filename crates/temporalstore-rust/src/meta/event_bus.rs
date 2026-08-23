// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Subscribing to metadata change, rather than polling for it.
//!
//! Every metadata change already funnels through `record_topology_event`, which
//! stamps a version and appends to a bounded ring. Reading it meant polling that
//! ring and diffing, so anything that wanted to *react* to a change -- a proxy
//! refreshing a route, an operator tool watching a drain -- had to ask
//! repeatedly and work out for itself what was new.
//!
//! This turns that ring into something you can subscribe to. It deliberately
//! does not publish from inside `record_topology_event`: that runs while the
//! metadata write lock is held, and fanning out to subscribers there would put
//! their cost, and any one of them misbehaving, inside the lock. A pump reads
//! the ring on its own and fans out afterwards, so publishing stays exactly as
//! cheap as it was.

use super::*;
use std::collections::BTreeSet;
use std::sync::mpsc::{sync_channel, Receiver, SyncSender, TrySendError};
use std::sync::Mutex;

/// How many undelivered events one subscriber may bank up before the oldest are
/// dropped. A subscriber that stops reading must not be able to grow the
/// metaserver's memory, and must not be able to slow anyone else down.
pub const SUBSCRIBER_QUEUE_DEPTH: usize = 128;

/// One metadata change, plus what the subscriber missed before it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopologyNotice {
    pub event: TopologyChangeEvent,
    /// Events this subscriber did not receive between the previous notice and
    /// this one, either because it was not reading fast enough or because the
    /// ring recycled them before the pump got there.
    ///
    /// Reported rather than hidden: a subscriber that silently misses a drain
    /// or a freeze is worse than one that knows it has fallen behind.
    pub missed: u64,
}

/// A handle on the stream. Dropping it unsubscribes on the next fan-out.
#[derive(Debug)]
pub struct TopologySubscription {
    pub id: u64,
    receiver: Receiver<TopologyNotice>,
}

impl TopologySubscription {
    /// Take the next notice, or `None` if nothing is waiting.
    pub fn try_next(&self) -> Option<TopologyNotice> {
        self.receiver.try_recv().ok()
    }

    /// Wait up to `timeout` for the next notice.
    pub fn next_before(&self, timeout: Duration) -> Option<TopologyNotice> {
        self.receiver.recv_timeout(timeout).ok()
    }

    /// Everything currently waiting, oldest first.
    pub fn drain(&self) -> Vec<TopologyNotice> {
        let mut out = Vec::new();
        while let Ok(notice) = self.receiver.try_recv() {
            out.push(notice);
        }
        out
    }
}

struct Subscriber {
    id: u64,
    /// Kinds this subscriber cares about. Empty means every kind.
    kinds: BTreeSet<String>,
    sender: SyncSender<TopologyNotice>,
    missed: u64,
}

#[derive(Default)]
struct BusState {
    next_id: u64,
    subscribers: Vec<Subscriber>,
    /// Highest `topology_version` already fanned out. Versions increase by
    /// exactly one per event, which is what makes a dropped event detectable
    /// rather than merely suspected.
    cursor: u64,
}

/// Fan-out for metadata change, held outside the metadata lock.
#[derive(Default)]
pub(crate) struct MetaEventBus {
    state: Mutex<BusState>,
}

impl std::fmt::Debug for MetaEventBus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MetaEventBus").finish_non_exhaustive()
    }
}

impl MetaEventBus {
    fn subscribe(&self, kinds: BTreeSet<String>) -> TopologySubscription {
        let (sender, receiver) = sync_channel(SUBSCRIBER_QUEUE_DEPTH);
        let mut state = self.state.lock().expect("event bus lock poisoned");
        state.next_id += 1;
        let id = state.next_id;
        state.subscribers.push(Subscriber {
            id,
            kinds,
            sender,
            missed: 0,
        });
        TopologySubscription { id, receiver }
    }

    fn unsubscribe(&self, id: u64) {
        let mut state = self.state.lock().expect("event bus lock poisoned");
        state.subscribers.retain(|subscriber| subscriber.id != id);
    }

    fn subscriber_count(&self) -> usize {
        self.state
            .lock()
            .expect("event bus lock poisoned")
            .subscribers
            .len()
    }

    /// Fan `events` out, and account for anything the ring recycled first.
    ///
    /// `oldest_retained` is the lowest version still in the ring; anything
    /// between the cursor and it was dropped before the pump saw it.
    fn deliver(&self, events: Vec<TopologyChangeEvent>, oldest_retained: Option<u64>, newest: u64) {
        let mut state = self.state.lock().expect("event bus lock poisoned");
        let lost = match oldest_retained {
            Some(oldest) if state.cursor > 0 && oldest > state.cursor + 1 => {
                oldest - state.cursor - 1
            }
            _ => 0,
        };
        if newest > state.cursor {
            state.cursor = newest;
        }
        if state.subscribers.is_empty() {
            // Nothing listening: the cursor still moves, so a subscriber
            // arriving later starts from now rather than replaying history.
            return;
        }
        if lost > 0 {
            for subscriber in state.subscribers.iter_mut() {
                subscriber.missed = subscriber.missed.saturating_add(lost);
            }
        }
        let mut dropped = Vec::new();
        for event in events {
            for subscriber in state.subscribers.iter_mut() {
                if !subscriber.kinds.is_empty() && !subscriber.kinds.contains(&event.kind) {
                    continue;
                }
                let notice = TopologyNotice {
                    event: event.clone(),
                    missed: subscriber.missed,
                };
                match subscriber.sender.try_send(notice) {
                    Ok(()) => subscriber.missed = 0,
                    Err(TrySendError::Full(_)) => {
                        // Not reading fast enough. Drop this one and say so on
                        // the next notice that does get through, rather than
                        // blocking the pump on the slowest subscriber.
                        subscriber.missed = subscriber.missed.saturating_add(1);
                    }
                    Err(TrySendError::Disconnected(_)) => dropped.push(subscriber.id),
                }
            }
        }
        if !dropped.is_empty() {
            state
                .subscribers
                .retain(|subscriber| !dropped.contains(&subscriber.id));
        }
    }
}

impl SingleNodeMeta {
    /// Subscribe to every metadata change.
    pub fn subscribe_topology(&self) -> TopologySubscription {
        self.events.subscribe(BTreeSet::new())
    }

    /// Subscribe to the given event kinds only -- `"server_state"`,
    /// `"shard_state"`, `"add_table"` and so on, as recorded on the event.
    pub fn subscribe_topology_kinds<I, S>(&self, kinds: I) -> TopologySubscription
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.events
            .subscribe(kinds.into_iter().map(Into::into).collect())
    }

    pub fn unsubscribe_topology(&self, id: u64) {
        self.events.unsubscribe(id);
    }

    pub fn topology_subscriber_count(&self) -> usize {
        self.events.subscriber_count()
    }

    /// Fan out everything recorded since the last call. Returns how many events
    /// were delivered to at least one subscriber.
    pub fn pump_topology_events(&self) -> usize {
        let cursor = {
            let state = self.events.state.lock().expect("event bus lock poisoned");
            state.cursor
        };
        let (events, oldest_retained, newest) = {
            let state = self.inner.read().expect("meta lock poisoned");
            let oldest = state
                .topology_events
                .front()
                .map(|event| event.topology_version);
            let newest = state
                .topology_events
                .back()
                .map(|event| event.topology_version)
                .unwrap_or(cursor);
            let fresh = state
                .topology_events
                .iter()
                .filter(|event| event.topology_version > cursor)
                .cloned()
                .collect::<Vec<_>>();
            (fresh, oldest, newest)
        };
        let count = events.len();
        self.events.deliver(events, oldest_retained, newest);
        count
    }

    /// Background loop pumping on an interval.
    pub fn start_topology_event_pump(&self, interval_ms: u64) -> thread::JoinHandle<()> {
        let meta = self.clone();
        let interval = Duration::from_millis(interval_ms.max(1));
        thread::spawn(move || loop {
            meta.pump_topology_events();
            thread::sleep(interval);
        })
    }
}
