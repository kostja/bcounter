// SPDX-License-Identifier: MIT OR Apache-2.0
// Copyright (c) 2026 Konstantin Osipov.

//! The allocator contract, and a reference in-process implementation.
//!
//! The real allocator lives in the server embedding this crate: it needs durability across
//! leader changes (an outstanding-grant ledger that survives failover, or a new leader
//! re-hands-out budget already lent → overshoot), a clock for lease expiry and fencing, and a
//! rebalancing policy that decides when to move grants between nodes. None of that belongs in a
//! pure crate. What belongs here is the *contract* that allocator must meet -- the [`Pool`]
//! trait -- so an [`Escrow`](crate::Escrow) can be written and tested against it.

use std::collections::BTreeMap;

/// The contract a quota pool must satisfy to feed [`Escrow`](crate::Escrow) counters safely.
///
/// A pool owns a global budget and lends slices of it (*grants*) to nodes. Its **one invariant**
/// is that the rights it has lent never exceed its ceiling:
///
/// ```text
///   Σ outstanding grants ≤ limit + Δ
/// ```
///
/// Honor that and the escrow counters it feeds can never let the cluster exceed `limit + Δ`
/// (`Δ = 0` for strict, overshoot-free enforcement; `Δ > 0` trades a bounded overshoot for fewer
/// false denials). Everything else -- persistence, lease TTLs, when and how to rebalance -- is
/// the implementor's, and the implementor is expected to be the server, not this crate.
pub trait Pool<Id> {
    /// Lend up to `want` additional rights to `who`. Returns the amount actually granted
    /// (`≤ want`), which is `0` when the pool is exhausted. **Must not** let total outstanding
    /// grants exceed the ceiling.
    fn grant(&mut self, who: &Id, want: u64) -> u64;

    /// Take `amount` previously-granted, now-unused rights back from `who`. Pairs with
    /// [`Escrow::reclaim`](crate::Escrow::reclaim), which computes how much is safe to return.
    fn release(&mut self, who: &Id, amount: u64);

    /// Rights currently free to be lent -- `ceiling − Σ outstanding grants`.
    fn available(&self) -> u64;
}

/// A minimal in-process [`Pool`] for tests and examples. **Not for production**: it is neither
/// durable across restarts nor safe across a leader change, and it has no lease expiry -- a
/// crashed node's grant is stranded until something calls [`release`](Pool::release). The real
/// pool is the server's.
#[derive(Clone, Debug, Default)]
pub struct LocalPool<Id: Ord + Clone> {
    ceiling: u64,
    outstanding: BTreeMap<Id, u64>,
}

impl<Id: Ord + Clone> LocalPool<Id> {
    /// A pool that will lend out at most `ceiling` in total. Pass `limit + Δ` for a pool that
    /// tolerates an overshoot of `Δ`.
    #[must_use]
    pub fn new(ceiling: u64) -> Self {
        Self {
            ceiling,
            outstanding: BTreeMap::new(),
        }
    }

    /// Total rights currently lent out across all nodes.
    #[must_use]
    pub fn outstanding(&self) -> u64 {
        self.outstanding.values().copied().sum()
    }
}

impl<Id: Ord + Clone> Pool<Id> for LocalPool<Id> {
    fn grant(&mut self, who: &Id, want: u64) -> u64 {
        let give = want.min(self.available());
        if give > 0 {
            *self.outstanding.entry(who.clone()).or_default() += give;
        }
        give
    }

    fn release(&mut self, who: &Id, amount: u64) {
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
    fn a_pool_never_lends_past_its_ceiling() {
        let mut pool = LocalPool::new(100);
        assert_eq!(pool.grant(&1, 70), 70);
        assert_eq!(pool.available(), 30);
        // Node 2 asks for 50 but only 30 remains.
        assert_eq!(pool.grant(&2, 50), 30);
        assert_eq!(pool.available(), 0);
        assert_eq!(pool.grant(&1, 1), 0); // exhausted
        assert_eq!(pool.outstanding(), 100);
    }

    #[test]
    fn release_returns_rights_to_the_pool() {
        let mut pool = LocalPool::new(100);
        pool.grant(&1, 80);
        pool.release(&1, 30);
        assert_eq!(pool.available(), 50);
        // Releasing more than held is clamped, never underflows.
        pool.release(&1, 1000);
        assert_eq!(pool.available(), 100);
    }
}
