// SPDX-License-Identifier: MIT OR Apache-2.0
// Copyright (c) 2026 Konstantin Osipov.

//! A map of named [`Escrow`]s -- one quota per scope, spent as a path.
//!
//! A single write is often subject to several limits at once: an object counts against its
//! bucket's capacity, its tenant's, and the cluster's. Those scopes form a path, and a write
//! must be admitted by **every** level or by none. [`EscrowMap`] holds one [`Escrow`] per scope
//! and spends across a whole path **all-or-none**.
//!
//! Spending here is purely local: it checks the rights already granted to each scope, no pool
//! and no view of other nodes. When a scope is short, [`spend`](EscrowMap::spend) names it and
//! charges nothing; the shell then tops that scope up from its pool (via
//! [`grant`](EscrowMap::grant)) and retries. Keeping the pool out of the fan-out is what lets
//! the map stay pure -- each scope has its own limit and therefore its own pool, and that
//! bookkeeping belongs to the shell.
//!
//! Sparse: a scope appears only once it is granted rights or spends; an untouched scope reads
//! zero and costs nothing.

use std::collections::BTreeMap;

use crate::{Denied, Escrow};

/// A map of per-scope escrow counters, keyed by scope identity `K`.
///
/// `K` names a limited scope (a bucket id, a tenant id, a `(kind, id)` pair); `Id` is the node
/// identity of the underlying [`Escrow`]s (defaults to `u32`). See the module docs.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EscrowMap<K: Ord + Clone, Id: Ord + Clone = u32> {
    /// This node's slot, stamped into every escrow it creates.
    me: Id,
    /// One escrow per scope with rights or usage. Sparse.
    escrows: BTreeMap<K, Escrow<Id>>,
}

impl<K: Ord + Clone, Id: Ord + Clone> EscrowMap<K, Id> {
    /// A fresh, empty map for the node identified by `me`.
    #[must_use]
    pub fn new(me: Id) -> Self {
        Self {
            me,
            escrows: BTreeMap::new(),
        }
    }

    /// True until the first scope gains rights or usage.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.escrows.is_empty()
    }

    /// Rights this node can still spend on `scope` without asking its pool. Zero for an
    /// untouched scope.
    #[must_use]
    pub fn local_available(&self, scope: &K) -> u64 {
        self.escrows.get(scope).map_or(0, Escrow::local_available)
    }

    /// Global net usage recorded against `scope`, from this node's merged view.
    #[must_use]
    pub fn global_used(&self, scope: &K) -> u64 {
        self.escrows.get(scope).map_or(0, Escrow::global_used)
    }

    /// Grant `amount` rights to one `scope`, creating its escrow if new. This is how the shell
    /// tops a scope up after draining its pool.
    pub fn grant(&mut self, scope: &K, amount: u64) {
        let me = self.me.clone();
        self.escrows
            .entry(scope.clone())
            .or_insert_with(|| Escrow::new(me, 0))
            .grant(amount);
    }

    /// Spend `amount` against every scope on `path`, all-or-none, from rights already granted.
    ///
    /// Every scope is checked before any is charged, so on a shortfall nothing is spent and no
    /// escrow is created. Scopes on a path are expected distinct (a bucket, its tenant, the
    /// root -- never the same scope twice).
    ///
    /// # Errors
    ///
    /// The first scope (in `path` order) whose local rights are short, with its [`Denied`].
    /// Nothing is spent; the caller tops that scope up (see [`grant`](EscrowMap::grant)) and
    /// retries.
    pub fn spend(&mut self, path: &[K], amount: u64) -> Result<(), (K, Denied)> {
        for scope in path {
            let available = self.local_available(scope);
            if available < amount {
                return Err((scope.clone(), Denied { available }));
            }
        }
        for scope in path {
            // Guaranteed to succeed: every scope was just checked to have room.
            let _ = self
                .escrows
                .get_mut(scope)
                .expect("a scope with room exists")
                .spend(amount);
        }
        Ok(())
    }

    /// Return `amount` rights on every scope on `path` -- a free, a delete. Always succeeds.
    pub fn refund(&mut self, path: &[K], amount: u64) {
        let me = self.me.clone();
        for scope in path {
            self.escrows
                .entry(scope.clone())
                .or_insert_with(|| Escrow::new(me.clone(), 0))
                .refund(amount);
        }
    }

    /// Return up to `amount` unused rights on one `scope` toward its pool. Returns how much was
    /// reclaimed (see [`Escrow::reclaim`]).
    #[must_use]
    pub fn reclaim(&mut self, scope: &K, amount: u64) -> u64 {
        self.escrows.get_mut(scope).map_or(0, |e| e.reclaim(amount))
    }

    /// Every scope with usage, and its global net usage. For metrics and inspection.
    pub fn iter(&self) -> impl Iterator<Item = (&K, u64)> {
        self.escrows
            .iter()
            .map(|(scope, e)| (scope, e.global_used()))
    }

    /// Fold another replica's map in: merge each scope's escrow view, scope by scope. The join
    /// that makes the observation a CRDT -- idempotent, commutative, associative, so replicas
    /// converge under gossip in any order. Local grants are untouched.
    pub fn merge(&mut self, other: &Self) {
        let me = self.me.clone();
        for (scope, their) in &other.escrows {
            self.escrows
                .entry(scope.clone())
                .or_insert_with(|| Escrow::new(me.clone(), 0))
                .merge(their);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    /// Grant the three scopes of a path their per-scope rights on one node.
    fn granted(bucket: u64, tenant: u64, root: u64) -> EscrowMap<&'static str> {
        let mut m: EscrowMap<&str> = EscrowMap::new(1);
        m.grant(&"bucket", bucket);
        m.grant(&"tenant", tenant);
        m.grant(&"root", root);
        m
    }

    #[test]
    fn a_fresh_map_is_empty_and_reads_zero() {
        let m: EscrowMap<&str> = EscrowMap::new(1);
        assert!(m.is_empty());
        assert_eq!(m.local_available(&"bucket"), 0);
        assert_eq!(m.global_used(&"bucket"), 0);
    }

    #[test]
    fn a_spend_within_every_grant_lands_on_all_levels() {
        let mut m = granted(100, 500, 9999);
        assert_eq!(m.spend(&["bucket", "tenant", "root"], 40), Ok(()));
        assert_eq!(m.global_used(&"bucket"), 40);
        assert_eq!(m.global_used(&"tenant"), 40);
        assert_eq!(m.global_used(&"root"), 40);
    }

    #[test]
    fn a_spend_short_at_one_level_charges_nothing() {
        let mut m = granted(100, 30, 9999);
        // Fits the bucket (100) but not the tenant grant (30).
        let denied = m.spend(&["bucket", "tenant", "root"], 40);
        assert_eq!(denied, Err(("tenant", Denied { available: 30 })));
        // All-or-none: not even the level that had room is charged.
        assert_eq!(m.global_used(&"bucket"), 0);
        assert_eq!(m.global_used(&"tenant"), 0);
        assert_eq!(m.global_used(&"root"), 0);
    }

    #[test]
    fn the_short_scope_is_the_first_one_in_path_order() {
        let mut m = granted(10, 20, 9999);
        let denied = m.spend(&["bucket", "tenant", "root"], 40);
        assert_eq!(denied, Err(("bucket", Denied { available: 10 })));
    }

    #[test]
    fn a_top_up_reopens_a_short_scope() {
        let mut m = granted(100, 30, 9999);
        assert!(m.spend(&["bucket", "tenant", "root"], 40).is_err());
        m.grant(&"tenant", 20); // shell tops the tenant up from its pool
        assert_eq!(m.spend(&["bucket", "tenant", "root"], 40), Ok(()));
    }

    #[test]
    fn a_refund_lowers_every_level() {
        let mut m = granted(100, 500, 9999);
        m.spend(&["bucket", "tenant", "root"], 80).unwrap();
        m.refund(&["bucket", "tenant", "root"], 30);
        assert_eq!(m.global_used(&"bucket"), 50);
        assert_eq!(m.global_used(&"tenant"), 50);
    }

    #[test]
    fn merge_carries_each_scope_from_both_replicas() {
        let mut a: EscrowMap<&str> = EscrowMap::new(1);
        a.grant(&"bucket", 1000);
        a.spend(&["bucket"], 300).unwrap();
        let mut b: EscrowMap<&str> = EscrowMap::new(2);
        b.grant(&"tenant", 1000);
        b.spend(&["tenant"], 400).unwrap();
        a.merge(&b);
        assert_eq!(a.global_used(&"bucket"), 300); // kept
        assert_eq!(a.global_used(&"tenant"), 400); // adopted
    }

    // ---- CRDT laws over the map's view -----------------------------------

    fn single_map(me: u32, scope: u8, spent: u64, freed: u64) -> EscrowMap<u8> {
        let mut m: EscrowMap<u8> = EscrowMap::new(me);
        m.grant(&scope, u64::MAX);
        m.spend(&[scope], spent).unwrap();
        m.refund(&[scope], freed);
        m
    }

    fn map_state() -> impl Strategy<Value = EscrowMap<u8>> {
        (0u32..4, 0u8..4, 0u64..100_000, 0u64..100_000)
            .prop_map(|(me, scope, spent, freed)| single_map(me, scope, spent, freed))
    }

    fn merged(base: &EscrowMap<u8>, parts: &[EscrowMap<u8>]) -> EscrowMap<u8> {
        let mut acc = base.clone();
        for p in parts {
            acc.merge(p);
        }
        acc
    }

    proptest! {
        #[test]
        fn map_merge_is_idempotent(a in map_state()) {
            let mut once = a.clone();
            once.merge(&a);
            prop_assert_eq!(once, a);
        }

        #[test]
        fn map_merge_is_commutative(base in map_state(), a in map_state(), b in map_state()) {
            prop_assert_eq!(merged(&base, &[a.clone(), b.clone()]), merged(&base, &[b, a]));
        }

        #[test]
        fn map_all_merge_orders_converge(parts in prop::collection::vec(map_state(), 0..8)) {
            let base: EscrowMap<u8> = EscrowMap::new(0);
            let forward = merged(&base, &parts);
            let mut reversed = parts.clone();
            reversed.reverse();
            prop_assert_eq!(forward, merged(&base, &reversed));
        }
    }
}
