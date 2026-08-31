// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Per-shard read and write rate limits.
//!
//! One token bucket per direction. A bucket is defined by a rate (tokens per second) and a burst
//! (how many tokens may accumulate while idle), and a rate of zero means the direction is not
//! limited at all -- which is the default, so a deployment that sets nothing behaves exactly as it
//! did before this existed.

use std::collections::HashMap;
use std::time::Instant;

use crate::types::ShardId;

/// Which direction a command consumes from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum QuotaKind {
    Read,
    Write,
}

/// A token bucket that never waits: it either has the tokens or it does not.
///
/// Tokens are not counted. What is stored is a point in time that consumption has been charged up
/// to, so the tokens available at any moment are however much time has passed since it, capped at
/// the burst. Charging N tokens moves that point forward by N token-times. Nothing accumulates
/// while idle beyond the cap, and no timer has to run to refill anything.
#[derive(Debug)]
pub(crate) struct TokenBucket {
    rate: u64,
    burst: u64,
    per_token_ns: i128,
    per_burst_ns: i128,
    /// Consumption is charged up to here, as nanoseconds since `origin`. Starts one full burst
    /// BEHIND the origin, which is what makes a new bucket start full: the tokens available are
    /// however much time separates this point from now, and at the origin that is a whole burst.
    /// Signed for that reason -- an unsigned zero would mean a new bucket had nothing in it, and
    /// the first request against it would be refused.
    charged_ns: i128,
    origin: Instant,
}

impl TokenBucket {
    pub(crate) fn new(rate: u64, burst: u64) -> Self {
        // A burst of zero would mean nothing may ever accumulate, so a caller sending at exactly
        // the rate would still be refused on any jitter. One second of credit is the useful
        // default and matches what a rate alone implies.
        let burst = if burst == 0 { rate } else { burst };
        let per_token_ns: i128 = if rate == 0 {
            0
        } else {
            1_000_000_000i128 / rate as i128
        };
        let per_burst_ns = per_token_ns * burst as i128;
        Self {
            rate,
            burst,
            per_token_ns,
            per_burst_ns,
            charged_ns: -per_burst_ns,
            origin: Instant::now(),
        }
    }

    pub(crate) fn rate(&self) -> u64 {
        self.rate
    }

    pub(crate) fn burst(&self) -> u64 {
        self.burst
    }

    fn now_ns(&self) -> i128 {
        self.origin.elapsed().as_nanos() as i128
    }

    /// Take `tokens` if they are there. Returns false without waiting if they are not.
    pub(crate) fn try_consume(&mut self, tokens: u64) -> bool {
        self.try_consume_at(tokens, self.now_ns())
    }

    fn try_consume_at(&mut self, tokens: u64, now_ns: i128) -> bool {
        if self.rate == 0 {
            return true;
        }
        // Credit older than one burst is gone -- that is what the cap means.
        let floor = now_ns - self.per_burst_ns;
        let from = self.charged_ns.max(floor);
        let charged_to = from + tokens as i128 * self.per_token_ns;
        if charged_to > now_ns {
            return false;
        }
        self.charged_ns = charged_to;
        true
    }

    /// How many tokens are available right now. Diagnostics only.
    pub(crate) fn available(&self) -> u64 {
        if self.rate == 0 || self.per_token_ns == 0 {
            return u64::MAX;
        }
        let now_ns = self.now_ns();
        if now_ns <= self.charged_ns {
            return 0;
        }
        let idle = (now_ns - self.charged_ns).min(self.per_burst_ns);
        (idle / self.per_token_ns) as u64
    }
}

/// What a shard's limits are. Zero qps means that direction is not limited.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ShardQuotaConfig {
    pub read_qps: u64,
    pub read_burst: u64,
    pub write_qps: u64,
    pub write_burst: u64,
}

impl ShardQuotaConfig {
    /// The default limits, from the environment. Absent or zero means unlimited.
    pub fn from_env() -> Self {
        let read = |name: &str| -> u64 {
            std::env::var(name)
                .ok()
                .and_then(|value| value.trim().parse::<u64>().ok())
                .unwrap_or(0)
        };
        Self {
            read_qps: read("TS_SHARD_READ_QPS"),
            read_burst: read("TS_SHARD_READ_BURST"),
            write_qps: read("TS_SHARD_WRITE_QPS"),
            write_burst: read("TS_SHARD_WRITE_BURST"),
        }
    }

    pub fn is_unlimited(&self) -> bool {
        self.read_qps == 0 && self.write_qps == 0
    }
}

/// What a shard's limit has allowed and refused.
///
/// Both halves, not just refusals: a refusal count with no denominator says nothing about whether
/// the limit is close or miles away. Only shards that HAVE a limit carry these -- an unlimited
/// shard has nothing to count, and keeping it that way is what lets the fast path stay on a read
/// lock.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct QuotaCounters {
    pub read_allowed: u64,
    pub read_refused: u64,
    pub write_allowed: u64,
    pub write_refused: u64,
}

/// The buckets for one shard.
#[derive(Debug)]
pub(crate) struct ShardQuota {
    read: Option<TokenBucket>,
    write: Option<TokenBucket>,
    counters: QuotaCounters,
}

impl ShardQuota {
    pub(crate) fn new(config: ShardQuotaConfig) -> Self {
        Self {
            read: (config.read_qps > 0)
                .then(|| TokenBucket::new(config.read_qps, config.read_burst)),
            write: (config.write_qps > 0)
                .then(|| TokenBucket::new(config.write_qps, config.write_burst)),
            counters: QuotaCounters::default(),
        }
    }

    pub(crate) fn counters(&self) -> QuotaCounters {
        self.counters
    }

    /// Take one token for `kind`. An unlimited direction always succeeds.
    pub(crate) fn try_consume(&mut self, kind: QuotaKind) -> bool {
        let bucket = match kind {
            QuotaKind::Read => self.read.as_mut(),
            QuotaKind::Write => self.write.as_mut(),
        };
        let allowed = match bucket {
            Some(bucket) => bucket.try_consume(1),
            None => true,
        };
        match (kind, allowed) {
            (QuotaKind::Read, true) => self.counters.read_allowed += 1,
            (QuotaKind::Read, false) => self.counters.read_refused += 1,
            (QuotaKind::Write, true) => self.counters.write_allowed += 1,
            (QuotaKind::Write, false) => self.counters.write_refused += 1,
        }
        allowed
    }

    pub(crate) fn limit_of(&self, kind: QuotaKind) -> Option<(u64, u64)> {
        let bucket = match kind {
            QuotaKind::Read => self.read.as_ref(),
            QuotaKind::Write => self.write.as_ref(),
        };
        bucket.map(|bucket| (bucket.rate(), bucket.burst()))
    }
}

/// Every shard's limits, so they can be changed on a running service.
#[derive(Debug, Default)]
pub(crate) struct QuotaTable {
    by_shard: HashMap<ShardId, ShardQuota>,
    configured: HashMap<ShardId, ShardQuotaConfig>,
}

impl QuotaTable {
    /// Replace a shard's limits. Rebuilding the bucket resets its credit, which is the honest
    /// behaviour for a new limit: credit accumulated under the old rate means nothing under the
    /// new one.
    pub(crate) fn set(&mut self, shard_id: ShardId, config: ShardQuotaConfig) {
        self.configured.insert(shard_id, config);
        // The bucket is rebuilt, but the counters are carried across: they record what this shard
        // has done, not what this bucket has done, and resetting them on every reconfiguration
        // would erase the history right when someone is watching it to decide the new number.
        let carried = self
            .by_shard
            .get(&shard_id)
            .map(|quota| quota.counters())
            .unwrap_or_default();
        let mut quota = ShardQuota::new(config);
        quota.counters = carried;
        self.by_shard.insert(shard_id, quota);
    }

    pub(crate) fn config_of(&self, shard_id: ShardId) -> Option<ShardQuotaConfig> {
        self.configured.get(&shard_id).copied()
    }

    /// Take one token for `kind` on `shard_id`, falling back to the environment default the first
    /// time a shard is seen.
    pub(crate) fn try_consume(
        &mut self,
        shard_id: ShardId,
        kind: QuotaKind,
        default: ShardQuotaConfig,
    ) -> bool {
        if !self.by_shard.contains_key(&shard_id) {
            if default.is_unlimited() {
                return true;
            }
            self.by_shard.insert(shard_id, ShardQuota::new(default));
            self.configured.entry(shard_id).or_insert(default);
        }
        self.by_shard
            .get_mut(&shard_id)
            .map(|quota| quota.try_consume(kind))
            .unwrap_or(true)
    }

    /// Whether this shard has any limit at all. Answered under a read lock, so an unlimited
    /// shard never blocks another thread.
    pub(crate) fn limits(&self, shard_id: ShardId) -> bool {
        self.by_shard.contains_key(&shard_id)
    }

    /// What this shard's limit has allowed and refused, if it has one.
    pub(crate) fn counters_of(&self, shard_id: ShardId) -> Option<QuotaCounters> {
        self.by_shard.get(&shard_id).map(|quota| quota.counters())
    }

    /// Every shard that carries a limit, for reporting.
    pub(crate) fn limited_shards(&self) -> Vec<ShardId> {
        let mut shards: Vec<ShardId> = self.by_shard.keys().copied().collect();
        shards.sort_unstable();
        shards
    }

    pub(crate) fn limit_of(&self, shard_id: ShardId, kind: QuotaKind) -> Option<(u64, u64)> {
        self.by_shard
            .get(&shard_id)
            .and_then(|quota| quota.limit_of(kind))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn what_the_limit_allowed_and_refused_is_counted() {
        let mut quota = ShardQuota::new(ShardQuotaConfig {
            write_qps: 10,
            write_burst: 3,
            ..Default::default()
        });
        let mut allowed = 0;
        for _ in 0..20 {
            if quota.try_consume(QuotaKind::Write) {
                allowed += 1;
            }
        }
        let counters = quota.counters();
        assert_eq!(counters.write_allowed, allowed);
        assert_eq!(counters.write_refused, 20 - allowed);
        assert!(counters.write_refused > 0, "the limit should have refused some");
        assert_eq!(counters.read_allowed, 0, "reads were never asked for");
    }

    #[test]
    fn an_unlimited_direction_still_counts_what_it_allowed() {
        // The denominator matters: without it a refusal count says nothing about how close the
        // limit is.
        let mut quota = ShardQuota::new(ShardQuotaConfig {
            write_qps: 1,
            write_burst: 1,
            ..Default::default()
        });
        for _ in 0..5 {
            quota.try_consume(QuotaKind::Read);
        }
        assert_eq!(quota.counters().read_allowed, 5);
        assert_eq!(quota.counters().read_refused, 0);
    }

    #[test]
    fn changing_a_limit_keeps_what_the_shard_has_already_done() {
        let mut table = QuotaTable::default();
        table.set(
            1,
            ShardQuotaConfig {
                write_qps: 1,
                write_burst: 1,
                ..Default::default()
            },
        );
        for _ in 0..10 {
            table.try_consume(1, QuotaKind::Write, ShardQuotaConfig::default());
        }
        let before = table.counters_of(1).unwrap();
        assert!(before.write_refused > 0);

        table.set(
            1,
            ShardQuotaConfig {
                write_qps: 1_000,
                write_burst: 1_000,
                ..Default::default()
            },
        );
        let after = table.counters_of(1).unwrap();
        assert_eq!(
            after.write_refused, before.write_refused,
            "reconfiguring erased the history someone would be watching to pick the new number"
        );
    }

    #[test]
    fn a_rate_of_zero_never_refuses() {
        let mut bucket = TokenBucket::new(0, 0);
        for _ in 0..10_000 {
            assert!(bucket.try_consume(1));
        }
    }

    #[test]
    fn a_bucket_starts_full_and_then_refuses() {
        // 100 per second, 10 of credit. The first ten go through on the starting credit; the
        // eleventh has to wait for time that has not passed.
        let mut bucket = TokenBucket::new(100, 10);
        let mut allowed = 0;
        for _ in 0..50 {
            if bucket.try_consume_at(1, 0) {
                allowed += 1;
            }
        }
        assert_eq!(
            allowed, 10,
            "the burst is what may be spent before any time passes"
        );
    }

    #[test]
    fn credit_returns_at_the_rate_and_stops_at_the_burst() {
        let mut bucket = TokenBucket::new(1_000, 10);
        // Spend the burst at t=0.
        for _ in 0..10 {
            assert!(bucket.try_consume_at(1, 0));
        }
        assert!(!bucket.try_consume_at(1, 0));

        // At 1000/s a token is worth a millisecond. Five milliseconds buys five.
        let five_ms = 5_000_000i128;
        let mut allowed = 0;
        for _ in 0..20 {
            if bucket.try_consume_at(1, five_ms) {
                allowed += 1;
            }
        }
        assert_eq!(allowed, 5, "five milliseconds is worth five tokens");

        // Idling for a very long time does not accumulate more than the burst.
        let an_hour = 3_600_000_000_000i128;
        let mut allowed = 0;
        for _ in 0..100 {
            if bucket.try_consume_at(1, an_hour) {
                allowed += 1;
            }
        }
        assert_eq!(allowed, 10, "credit stops accumulating at the burst");
    }

    #[test]
    fn the_two_directions_do_not_share_credit() {
        let mut quota = ShardQuota::new(ShardQuotaConfig {
            read_qps: 1,
            read_burst: 1,
            write_qps: 1,
            write_burst: 1,
        });
        assert!(quota.try_consume(QuotaKind::Read));
        // Spending the read credit must leave the write credit alone.
        assert!(quota.try_consume(QuotaKind::Write));
    }

    #[test]
    fn a_direction_left_at_zero_stays_unlimited() {
        let mut quota = ShardQuota::new(ShardQuotaConfig {
            write_qps: 1,
            write_burst: 1,
            ..Default::default()
        });
        for _ in 0..1_000 {
            assert!(quota.try_consume(QuotaKind::Read), "reads were not limited");
        }
    }
}
