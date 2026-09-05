// SPDX-License-Identifier: MIT OR Apache-2.0
// Copyright (c) 2026 Konstantin Osipov.

//! The quota contract, and a reference implementation for tests.
//!
//! The real quota is not in this crate. It has to survive leader changes (keep a ledger of
//! outstanding grants that outlives a failover, or a new leader lends budget that is already
//! out), use a clock for lease expiry, and decide when to move grants between nodes. All of that
//! belongs to the server that embeds this crate. The crate defines only the contract: the
//! [`Quota`] trait. A [`BCounter`](crate::BCounter) is written and tested against it.

use std::collections::BTreeMap;

/// The contract a quota must satisfy to feed [`BCounter`](crate::BCounter)s safely.
///
/// A `Quota` holds a global budget and lends slices of it (*grants*) to nodes. It must keep one
/// rule: the total it has lent is never more than its ceiling.
///
/// ```text
///   Σ outstanding grants ≤ limit + Δ
/// ```
///
/// If it keeps that rule, the counters it feeds can never exceed `limit + Δ`. `Δ = 0` means no
/// overshoot. `Δ > 0` lets the total go up to `Δ` over the limit, in exchange for fewer false
/// denials. Persistence, lease expiry, and rebalancing are the implementor's job. The
/// implementor is the server, not this crate.
pub trait Quota<Id> {
    /// Lend up to `want` more rights to `who`. Returns the amount lent -- at most `want`, or `0`
    /// when the quota is empty. Must not let the total lent exceed the ceiling.
    fn grant(&mut self, who: &Id, want: u64) -> u64;

    /// Take `amount` unused rights back from `who`. Pairs with
    /// [`BCounter::reclaim`](crate::BCounter::reclaim), which computes how much is safe to
    /// return.
    fn reclaim(&mut self, who: &Id, amount: u64);

    /// Rights free to lend: the ceiling minus the total lent.
    fn available(&self) -> u64;
}

/// A small in-process [`Quota`] for tests and examples. **Not for production**: it is not
/// durable, it is not safe across a leader change, and it has no lease expiry, so a crashed
/// node's grant stays lent until something calls [`reclaim`](Quota::reclaim). The real quota is
/// the server's.
#[derive(Clone, Debug, Default)]
pub struct LocalQuota<Id: Ord + Clone> {
    ceiling: u64,
    outstanding: BTreeMap<Id, u64>,
}

impl<Id: Ord + Clone> LocalQuota<Id> {
    /// A quota that lends at most `ceiling` in total. Pass `limit + Δ` to allow an overshoot of
    /// `Δ`.
    #[must_use]
    pub fn new(ceiling: u64) -> Self {
        Self {
            ceiling,
            outstanding: BTreeMap::new(),
        }
    }

    /// Total rights lent out across all nodes.
    #[must_use]
    pub fn outstanding(&self) -> u64 {
        self.outstanding.values().copied().sum()
    }
}

impl<Id: Ord + Clone> Quota<Id> for LocalQuota<Id> {
    fn grant(&mut self, who: &Id, want: u64) -> u64 {
        let give = want.min(self.available());
        if give > 0 {
            *self.outstanding.entry(who.clone()).or_default() += give;
        }
        give
    }

    fn reclaim(&mut self, who: &Id, amount: u64) {
        if let Some(held) = self.outstanding.get_mut(who) {
            *held -= amount.min(*held);
        }
    }

    fn available(&self) -> u64 {
        self.ceiling.saturating_sub(self.outstanding())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_quota_never_lends_past_its_ceiling() {
        let mut quota = LocalQuota::new(100);
        assert_eq!(quota.grant(&1, 70), 70);
        assert_eq!(quota.available(), 30);
        // Node 2 asks for 50 but only 30 remains.
        assert_eq!(quota.grant(&2, 50), 30);
        assert_eq!(quota.available(), 0);
        assert_eq!(quota.grant(&1, 1), 0); // empty
        assert_eq!(quota.outstanding(), 100);
    }

    #[test]
    fn reclaim_returns_rights_to_the_quota() {
        let mut quota = LocalQuota::new(100);
        quota.grant(&1, 80);
        quota.reclaim(&1, 30);
        assert_eq!(quota.available(), 50);
        // Reclaiming more than held is clamped, never underflows.
        quota.reclaim(&1, 1000);
        assert_eq!(quota.available(), 100);
    }
}
