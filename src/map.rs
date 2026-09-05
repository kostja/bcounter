// SPDX-License-Identifier: MIT OR Apache-2.0
// Copyright (c) 2026 Konstantin Osipov.

//! A map of named [`BCounter`]s -- one quota per scope, charged as a path.
//!
//! The naming follows Akka's `PNCounterMap` (a map of named counters); this is the same shape
//! with [`BCounter`] values. It exists because a single write is often subject to **several**
//! limits at once: an object counts against its bucket's capacity, its tenant's, and the
//! cluster's; a request counts against a user's quota and a global one. Those scopes form a
//! path, and a write must be admitted by **every** level or by none -- charging the inner scope
//! but not the outer would leave the outer understated the moment the write is refused higher
//! up.
//!
//! # All-or-none fan-out
//!
//! [`inc`](BCounterMap::inc) takes the whole path -- each scope with its own limit -- and one
//! amount. It checks every scope first and records nothing unless all have room, so a refused
//! write leaves no partial charge behind and the caller gets back the scope that ran out. No
//! rollback: the check precedes the first mutation.
//!
//! # Limits live at the call site, not in the merged state
//!
//! A scope's limit is passed in on every call, applied afresh. The merged state is pure usage
//! -- the per-node slots of each [`BCounter`] -- so reconfiguring a limit takes effect on the
//! next call with no state change to gossip, and two nodes that briefly disagree on a limit
//! still converge on usage. This is why [`available`](BCounterMap::available) takes the limit
//! as an argument rather than reading a stored one.
//!
//! # Sparse
//!
//! A scope appears only once it has usage: an untouched scope reads zero and costs nothing, and
//! a *denied* charge creates no entry. A map over a million idle scopes holds a million
//! nothings.

use std::collections::BTreeMap;

use crate::{BCounter, Denied};

/// A map of per-scope capacity counters, keyed by scope identity `K`.
///
/// `K` is whatever names a limited scope -- a bucket id, a tenant id, a `(kind, id)` pair.
/// `Id` is the node identity of the underlying [`BCounter`]s (defaults to `u32`). See the
/// module docs for the fan-out and limit model.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BCounterMap<K: Ord + Clone, Id: Ord + Clone = u32> {
    /// This node's slot, stamped into every counter it creates.
    me: Id,
    /// One counter per scope that has usage. Sparse.
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

    /// True until the first scope gains usage.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.counters.is_empty()
    }

    /// Net usage recorded against `scope`, from this node's view. Zero for an untouched scope.
    #[must_use]
    pub fn read(&self, scope: &K) -> u64 {
        self.counters.get(scope).map_or(0, BCounter::read)
    }

    /// Room left under `limit` for `scope`, from this node's view. The limit is supplied here,
    /// not stored -- see the module docs.
    #[must_use]
    pub fn available(&self, scope: &K, limit: u64) -> u64 {
        limit.saturating_sub(self.read(scope))
    }

    /// Charge `amount` against every scope on `path`, all-or-none.
    ///
    /// Each entry pairs a scope with its current limit. Every scope is checked before any is
    /// charged, so on refusal nothing is recorded and no entry is created; on success the
    /// amount lands on this node's slot in each scope. Scopes on a path are expected distinct
    /// (a bucket, its tenant, the root -- never the same scope twice).
    ///
    /// # Errors
    ///
    /// The first scope (in `path` order) that lacks room, with its [`Denied`]. Nothing is
    /// charged in that case.
    pub fn inc(&mut self, path: &[(K, u64)], amount: u64) -> Result<(), (K, Denied)> {
        // Check every level first; a partial charge is never allowed to exist.
        for (scope, limit) in path {
            let used = self.read(scope);
            if u128::from(used) + u128::from(amount) > u128::from(*limit) {
                return Err((
                    scope.clone(),
                    Denied {
                        available: limit.saturating_sub(used),
                    },
                ));
            }
        }
        // All had room: record on each. The limit is refreshed into the counter so its own
        // guard agrees with what we just checked, then the charge -- known to fit -- lands.
        let me = self.me.clone();
        for (scope, limit) in path {
            let counter = self
                .counters
                .entry(scope.clone())
                .or_insert_with(|| BCounter::new(me.clone(), *limit));
            counter.set_limit(*limit);
            let _ = counter.inc(amount);
        }
        Ok(())
    }

    /// Free `amount` from every scope on `path`. Always succeeds; a free only lowers usage.
    pub fn dec(&mut self, path: &[K], amount: u64) {
        let me = self.me.clone();
        for scope in path {
            self.counters
                .entry(scope.clone())
                .or_insert_with(|| BCounter::new(me.clone(), u64::MAX))
                .dec(amount);
        }
    }

    /// Every scope with usage, and its net usage. For metrics and inspection.
    pub fn iter(&self) -> impl Iterator<Item = (&K, u64)> {
        self.counters.iter().map(|(scope, c)| (scope, c.read()))
    }

    /// Fold another replica's map in: merge each scope's counter, scope by scope. The join that
    /// makes the map a CRDT -- idempotent, commutative, associative, so replicas converge under
    /// gossip in any order. A scope only this replica knows is kept; one only the other knows
    /// is adopted.
    pub fn merge(&mut self, other: &Self) {
        let me = self.me.clone();
        for (scope, their) in &other.counters {
            self.counters
                .entry(scope.clone())
                .or_insert_with(|| BCounter::new(me.clone(), their.limit()))
                .merge(their);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    /// Three scopes on a path, tightest in the middle, at the given per-scope limits.
    fn path(bucket: u64, tenant: u64, root: u64) -> [(&'static str, u64); 3] {
        [("bucket", bucket), ("tenant", tenant), ("root", root)]
    }

    #[test]
    fn a_fresh_map_is_empty_and_reads_zero() {
        let m: BCounterMap<&str> = BCounterMap::new(1);
        assert!(m.is_empty());
        assert_eq!(m.read(&"bucket"), 0);
        assert_eq!(m.available(&"bucket", 100), 100);
    }

    #[test]
    fn a_charge_within_every_limit_lands_on_all_levels() {
        let mut m: BCounterMap<&str> = BCounterMap::new(1);
        assert_eq!(m.inc(&path(100, 500, 9999), 40), Ok(()));
        assert_eq!(m.read(&"bucket"), 40);
        assert_eq!(m.read(&"tenant"), 40);
        assert_eq!(m.read(&"root"), 40);
    }

    #[test]
    fn a_charge_over_one_level_is_refused_and_nothing_is_recorded() {
        let mut m: BCounterMap<&str> = BCounterMap::new(1);
        // Fits the bucket (100) but not the tenant (30).
        let denied = m.inc(&path(100, 30, 9999), 40);
        assert_eq!(denied, Err(("tenant", Denied { available: 30 })));
        // All-or-none: not even the level that had room is charged, and nothing is created.
        assert!(m.is_empty());
        assert_eq!(m.read(&"bucket"), 0);
        assert_eq!(m.read(&"root"), 0);
    }

    #[test]
    fn the_denied_scope_is_the_first_one_short_in_path_order() {
        let mut m: BCounterMap<&str> = BCounterMap::new(1);
        // Both bucket and tenant are too small; the bucket comes first on the path.
        let denied = m.inc(&path(10, 20, 9999), 40);
        assert_eq!(denied, Err(("bucket", Denied { available: 10 })));
    }

    #[test]
    fn a_free_lowers_every_level() {
        let mut m: BCounterMap<&str> = BCounterMap::new(1);
        m.inc(&path(100, 500, 9999), 80).unwrap();
        m.dec(&["bucket", "tenant", "root"], 30);
        assert_eq!(m.read(&"bucket"), 50);
        assert_eq!(m.read(&"tenant"), 50);
        assert_eq!(m.read(&"root"), 50);
    }

    #[test]
    fn a_limit_is_applied_at_the_call_site_not_from_stored_state() {
        let mut m: BCounterMap<&str> = BCounterMap::new(1);
        m.inc(&[("bucket", 100)], 100).unwrap();
        // Same usage, a lower limit this call -> refused.
        assert_eq!(
            m.inc(&[("bucket", 100)], 1),
            Err(("bucket", Denied { available: 0 }))
        );
        // Raise the limit this call -> the very same write is admitted. The limit was never
        // baked into the merged state; it rides on the call.
        assert_eq!(m.inc(&[("bucket", 200)], 50), Ok(()));
        assert_eq!(m.read(&"bucket"), 150);
        assert_eq!(m.available(&"bucket", 200), 50);
    }

    #[test]
    fn merge_carries_each_scope_from_both_replicas() {
        let mut a: BCounterMap<&str> = BCounterMap::new(1);
        let mut b: BCounterMap<&str> = BCounterMap::new(2);
        a.inc(&[("bucket", 1000)], 300).unwrap(); // only a knows "bucket"
        b.inc(&[("tenant", 1000)], 400).unwrap(); // only b knows "tenant"
        a.merge(&b);
        assert_eq!(a.read(&"bucket"), 300); // kept
        assert_eq!(a.read(&"tenant"), 400); // adopted
    }

    #[test]
    fn merge_sums_a_shared_scope_across_nodes() {
        let mut a: BCounterMap<&str> = BCounterMap::new(1);
        let mut b: BCounterMap<&str> = BCounterMap::new(2);
        a.inc(&[("root", 1000)], 300).unwrap();
        b.inc(&[("root", 1000)], 400).unwrap();
        a.merge(&b);
        assert_eq!(a.read(&"root"), 700);
    }

    // ---- CRDT laws over the map ------------------------------------------

    /// A map holding usage for a single scope on one node's slot.
    fn single_map(me: u32, scope: u8, consumed: u64, freed: u64) -> BCounterMap<u8> {
        let mut m: BCounterMap<u8> = BCounterMap::new(me);
        m.inc(&[(scope, u64::MAX)], consumed).unwrap();
        m.dec(&[scope], freed);
        m
    }

    fn map_state() -> impl Strategy<Value = BCounterMap<u8>> {
        (0u32..4, 0u8..4, 0u64..100_000, 0u64..100_000)
            .prop_map(|(me, scope, consumed, freed)| single_map(me, scope, consumed, freed))
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
            prop_assert_eq!(
                merged(&base, &[a.clone(), b.clone()]),
                merged(&base, &[b, a])
            );
        }

        #[test]
        fn map_merge_is_associative(a in map_state(), b in map_state(), c in map_state()) {
            let mut left = a.clone();
            left.merge(&b);
            left.merge(&c);
            let mut bc = b;
            bc.merge(&c);
            let mut right = a;
            right.merge(&bc);
            prop_assert_eq!(left, right);
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
