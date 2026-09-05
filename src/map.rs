// SPDX-License-Identifier: MIT OR Apache-2.0
// Copyright (c) 2026 Konstantin Osipov.

//! A map of named [`BCounter`]s, one quota per scope, charged over a path.
//!
//! A single write can be subject to several limits at once. An object counts against its bucket,
//! its tenant, and the whole cluster. These scopes form a path, and a write must be admitted at
//! every level or none. [`BCounterMap`] holds one [`BCounter`] per scope. [`acquire`] charges a
//! whole path all-or-none: it checks the rights already granted to each scope, and charges
//! nothing unless all of them have room.
//!
//! Charging is local. It uses no quota and no view of other nodes. When a scope is short,
//! [`acquire`] names it and charges nothing. The server then tops that scope up from its own
//! quota and retries. Each scope has its own limit and its own quota, so that bookkeeping stays
//! in the server.
//!
//! The map is sparse: a scope appears only once it is granted rights or used.
//!
//! [`acquire`]: BCounterMap::acquire

use std::collections::BTreeMap;

use crate::{BCounter, Denied};

/// A map of per-scope counters, keyed by scope identity `K`.
///
/// `K` names a limited scope (a bucket id, a tenant id, a `(kind, id)` pair). `Id` is the node
/// identity of the underlying [`BCounter`]s (defaults to `u32`). See the module docs.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BCounterMap<K: Ord + Clone, Id: Ord + Clone = u32> {
    /// This node's slot, stamped into every counter it creates.
    me: Id,
    /// One counter per scope with rights or usage. Sparse.
    counters: BTreeMap<K, BCounter<Id>>,
}

impl<K: Ord + Clone, Id: Ord + Clone> BCounterMap<K, Id> {
    /// A fresh, empty map for the node identified by `me`.
    #[must_use]
    pub fn new(me: Id) -> Self {
        Self {
            me,
            counters: BTreeMap::new(),
        }
    }

    /// True until the first scope gains rights or usage.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.counters.is_empty()
    }

    /// Rights this node can still acquire on `scope` without asking its quota. Zero for a scope
    /// it has not touched.
    #[must_use]
    pub fn local_available(&self, scope: &K) -> u64 {
        self.counters
            .get(scope)
            .map_or(0, BCounter::local_available)
    }

    /// Cluster-wide net usage on `scope`, from this node's merged view.
    #[must_use]
    pub fn global_used(&self, scope: &K) -> u64 {
        self.counters.get(scope).map_or(0, BCounter::global_used)
    }

    /// Grant `amount` rights to one `scope`, creating its counter if new. This is how the server
    /// tops a scope up after drawing from its quota.
    pub fn grant(&mut self, scope: &K, amount: u64) {
        let me = self.me.clone();
        self.counters
            .entry(scope.clone())
            .or_insert_with(|| BCounter::new(me, 0))
            .grant(amount);
    }

    /// Acquire `amount` against every scope on `path`, all-or-none, from rights already granted.
    ///
    /// Every scope is checked before any is charged. On a shortfall nothing is charged and no
    /// counter is created. Scopes on a path are expected to be distinct: a bucket, its tenant,
    /// the root, never the same scope twice.
    ///
    /// # Errors
    ///
    /// The first scope, in `path` order, whose local rights are short, with its [`Denied`].
    /// Nothing is charged. The caller tops that scope up (see [`grant`](BCounterMap::grant)) and
    /// retries.
    pub fn acquire(&mut self, path: &[K], amount: u64) -> Result<(), (K, Denied)> {
        for scope in path {
            let available = self.local_available(scope);
            if available < amount {
                return Err((scope.clone(), Denied { available }));
            }
        }
        for scope in path {
            // Guaranteed to succeed: every scope was just checked to have room.
            let _ = self
                .counters
                .get_mut(scope)
                .expect("a scope with room exists")
                .acquire(amount);
        }
        Ok(())
    }

    /// Release `amount` rights on every scope on `path`: a free or a delete. Always succeeds.
    pub fn release(&mut self, path: &[K], amount: u64) {
        let me = self.me.clone();
        for scope in path {
            self.counters
                .entry(scope.clone())
                .or_insert_with(|| BCounter::new(me.clone(), 0))
                .release(amount);
        }
    }

    /// Reclaim up to `amount` unused rights on one `scope`. Returns how much was reclaimed (see
    /// [`BCounter::reclaim`]).
    #[must_use]
    pub fn reclaim(&mut self, scope: &K, amount: u64) -> u64 {
        self.counters
            .get_mut(scope)
            .map_or(0, |e| e.reclaim(amount))
    }

    /// Every scope with usage, and its cluster-wide net usage. For metrics and inspection.
    pub fn iter(&self) -> impl Iterator<Item = (&K, u64)> {
        self.counters
            .iter()
            .map(|(scope, e)| (scope, e.global_used()))
    }

    /// Merge another replica's map in, scope by scope. Each scope's view merges as a CRDT
    /// (idempotent, commutative, associative), so replicas converge under gossip in any order.
    /// Local grants are not changed.
    pub fn merge(&mut self, other: &Self) {
        let me = self.me.clone();
        for (scope, their) in &other.counters {
            self.counters
                .entry(scope.clone())
                .or_insert_with(|| BCounter::new(me.clone(), 0))
                .merge(their);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    /// Grant the three scopes of a path their per-scope rights on one node.
    fn granted(bucket: u64, tenant: u64, root: u64) -> BCounterMap<&'static str> {
        let mut m: BCounterMap<&str> = BCounterMap::new(1);
        m.grant(&"bucket", bucket);
        m.grant(&"tenant", tenant);
        m.grant(&"root", root);
        m
    }

    #[test]
    fn a_fresh_map_is_empty_and_reads_zero() {
        let m: BCounterMap<&str> = BCounterMap::new(1);
        assert!(m.is_empty());
        assert_eq!(m.local_available(&"bucket"), 0);
        assert_eq!(m.global_used(&"bucket"), 0);
    }

    #[test]
    fn an_acquire_within_every_grant_lands_on_all_levels() {
        let mut m = granted(100, 500, 9999);
        assert_eq!(m.acquire(&["bucket", "tenant", "root"], 40), Ok(()));
        assert_eq!(m.global_used(&"bucket"), 40);
        assert_eq!(m.global_used(&"tenant"), 40);
        assert_eq!(m.global_used(&"root"), 40);
    }

    #[test]
    fn an_acquire_short_at_one_level_charges_nothing() {
        let mut m = granted(100, 30, 9999);
        // Fits the bucket (100) but not the tenant grant (30).
        let denied = m.acquire(&["bucket", "tenant", "root"], 40);
        assert_eq!(denied, Err(("tenant", Denied { available: 30 })));
        // All-or-none: not even the level that had room is charged.
        assert_eq!(m.global_used(&"bucket"), 0);
        assert_eq!(m.global_used(&"tenant"), 0);
        assert_eq!(m.global_used(&"root"), 0);
    }

    #[test]
    fn the_short_scope_is_the_first_one_in_path_order() {
        let mut m = granted(10, 20, 9999);
        let denied = m.acquire(&["bucket", "tenant", "root"], 40);
        assert_eq!(denied, Err(("bucket", Denied { available: 10 })));
    }

    #[test]
    fn a_top_up_reopens_a_short_scope() {
        let mut m = granted(100, 30, 9999);
        assert!(m.acquire(&["bucket", "tenant", "root"], 40).is_err());
        m.grant(&"tenant", 20); // the server tops the tenant up from its quota
        assert_eq!(m.acquire(&["bucket", "tenant", "root"], 40), Ok(()));
    }

    #[test]
    fn a_release_lowers_every_level() {
        let mut m = granted(100, 500, 9999);
        m.acquire(&["bucket", "tenant", "root"], 80).unwrap();
        m.release(&["bucket", "tenant", "root"], 30);
        assert_eq!(m.global_used(&"bucket"), 50);
        assert_eq!(m.global_used(&"tenant"), 50);
    }

    #[test]
    fn merge_carries_each_scope_from_both_replicas() {
        let mut a: BCounterMap<&str> = BCounterMap::new(1);
        a.grant(&"bucket", 1000);
        a.acquire(&["bucket"], 300).unwrap();
        let mut b: BCounterMap<&str> = BCounterMap::new(2);
        b.grant(&"tenant", 1000);
        b.acquire(&["tenant"], 400).unwrap();
        a.merge(&b);
        assert_eq!(a.global_used(&"bucket"), 300); // kept
        assert_eq!(a.global_used(&"tenant"), 400); // adopted
    }

    // ---- CRDT laws over the map's view -----------------------------------

    fn single_map(me: u32, scope: u8, acquired: u64, released: u64) -> BCounterMap<u8> {
        let mut m: BCounterMap<u8> = BCounterMap::new(me);
        m.grant(&scope, u64::MAX);
        m.acquire(&[scope], acquired).unwrap();
        m.release(&[scope], released);
        m
    }

    fn map_state() -> impl Strategy<Value = BCounterMap<u8>> {
        (0u32..4, 0u8..4, 0u64..100_000, 0u64..100_000)
            .prop_map(|(me, scope, acquired, released)| single_map(me, scope, acquired, released))
    }

    fn merged(base: &BCounterMap<u8>, parts: &[BCounterMap<u8>]) -> BCounterMap<u8> {
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
            let base: BCounterMap<u8> = BCounterMap::new(0);
            let forward = merged(&base, &parts);
            let mut reversed = parts.clone();
            reversed.reverse();
            prop_assert_eq!(forward, merged(&base, &reversed));
        }
    }
}
