// SPDX-License-Identifier: MIT OR Apache-2.0
// Copyright (c) 2026 Konstantin Osipov.

//! A bounded counter (BCounter) CRDT for distributed capacity quotas.
//!
//! The problem this solves: enforce "no more than `limit` in total" -- bytes stored, objects
//! held, connections open -- across a cluster where every node accepts writes, **without a
//! round trip on the write path**. The literature's answer is the bounded counter (Almeida &
//! Baquero; Balegas et al.), a CRDT built for exactly numeric invariants. [`BCounter`] here is
//! a capacity counter; a rate/bandwidth variant (a tick-refilled token bucket) is out of scope
//! -- per-node lease refill (`rate / nodes`) rounds toward nothing for small quotas on large
//! clusters and needs its own model.
//!
//! # The shape: a PN-counter over per-node slots, checked against a limit
//!
//! Each node owns one **slot**, identified by a stable node id (`Id` -- a Raft node id, a
//! member uuid, whatever names a replica), and writes **only its own slot**. That single-writer
//! discipline is what makes a slot a register whose state merge (elementwise `max`) loses
//! nothing. A slot records two grow-only totals: `consumed` (a write, [`inc`](BCounter::inc),
//! charges it) and `freed` (a free -- a delete, [`dec`](BCounter::dec) -- credits it). The
//! global net usage is `Σ consumed − Σ freed`, and the invariant is `net ≤ limit`.
//!
//! Grow-only-plus-max is the whole of the CRDT: idempotent (merging a state twice changes
//! nothing -- so gossip redelivery is safe), commutative and associative (so any delivery
//! order converges). This is "merge by join, not arithmetic add", the property a counter
//! reconstructed from re-delivered gossip must have.
//!
//! # Bounded overshoot, not exactness -- deliberately
//!
//! A node admits a write when **its own view** of the net usage leaves room. That view lags the
//! true total by whatever other nodes have consumed and not yet gossiped, so the true total can
//! briefly exceed `limit` -- by **at most the sum of other nodes' un-gossiped consumption**,
//! never unboundedly. The alternative (a strict per-node budget with transfers, the classical
//! exact BCounter) never overshoots but **falsely denies** a write whose quota is stranded on
//! another node -- a worse answer for a capacity quota, where a user under their limit being
//! refused reads as a bug. This crate chooses the honest, bounded overshoot.
//!
//! # Sans-network, sans-time
//!
//! Nothing here reads a clock or a socket. `inc`/`dec`/`merge` are pure state transitions you
//! drive directly; your own layer owns gossip and persistence. That is what makes the invariant
//! testable in microseconds rather than on a cluster.
//!
//! # Example
//!
//! ```
//! use bcounter::{BCounter, Denied};
//!
//! // Two nodes, each with its own view of a 100-unit limit.
//! let mut a: BCounter = BCounter::new(1, 100); // node 1
//! let mut b: BCounter = BCounter::new(2, 100); // node 2
//!
//! assert_eq!(a.inc(60), Ok(()));               // node 1 spends 60 of the budget
//! assert_eq!(b.inc(60), Ok(()));               // node 2, unaware, spends 60 too
//!
//! a.merge(&b);                                 // gossip: node 1 learns of node 2's usage
//! assert_eq!(a.read(), 120);                   // bounded overshoot, now visible
//! assert_eq!(a.inc(1), Err(Denied { available: 0 })); // and further writes are refused
//! ```

#![forbid(unsafe_code)]
#![warn(missing_docs)]

mod map;
pub use map::BCounterMap;

use std::collections::BTreeMap;

/// What one node has contributed to a counter. Both fields are **grow-only**: a write only
/// adds to `consumed`, a free only adds to `freed`, so a slot merges by taking the larger of
/// each and never loses an update.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct SlotState {
    /// Total charged against this slot -- writes. Grow-only.
    consumed: u64,
    /// Total credited back to this slot -- frees, deletes. Grow-only.
    freed: u64,
}

/// A refused write, carrying what the counter believes is still available.
///
/// No `retry_after`: for a capacity quota there is nothing that makes room but a free, and when
/// that happens is not the counter's to predict. The caller answers with a quota error and the
/// `available` figure.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Denied {
    /// Room left under the limit, from this node's (possibly stale) view. Zero, or small.
    pub available: u64,
}

/// A distributed capacity counter enforcing `net usage ≤ limit`.
///
/// `Id` is the node identity -- the type that names a replica in your cluster. It defaults to
/// `u32` (a Raft node id, say); use `u64`, a uuid, or any `Ord + Clone` type instead. One
/// counter per limited resource per node. See the crate docs for the model.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BCounter<Id: Ord + Clone = u32> {
    /// This node's slot -- the only one it writes.
    me: Id,
    /// The ceiling. Config, replicated to every node, not part of the merged state: a
    /// reconfiguration governs the usage already recorded, immediately, because the check reads
    /// the limit afresh every `inc`.
    limit: u64,
    /// Every node's contribution this replica has seen, merged. Sparse: a node appears only
    /// once it has charged or freed.
    slots: BTreeMap<Id, SlotState>,
}

impl<Id: Ord + Clone> BCounter<Id> {
    /// A fresh counter for the node identified by `me`, with `limit` as the global ceiling.
    #[must_use]
    pub fn new(me: Id, limit: u64) -> Self {
        Self {
            me,
            limit,
            slots: BTreeMap::new(),
        }
    }

    /// Replace the ceiling. Governs usage already recorded, at once -- lowering it can put the
    /// counter over, which the next `inc` then refuses until frees bring it back.
    pub fn set_limit(&mut self, limit: u64) {
        self.limit = limit;
    }

    /// The ceiling in force.
    #[must_use]
    pub fn limit(&self) -> u64 {
        self.limit
    }

    /// Net usage across the cluster, from this node's view: `Σ consumed − Σ freed`.
    ///
    /// Saturating at zero: in a stale view a node may have seen more frees than the writes they
    /// undo (a delete gossiped before the write it undoes), which is transient and means
    /// "nothing used", not a negative quantity.
    #[must_use]
    pub fn read(&self) -> u64 {
        let consumed: u128 = self.slots.values().map(|s| u128::from(s.consumed)).sum();
        let freed: u128 = self.slots.values().map(|s| u128::from(s.freed)).sum();
        u64::try_from(consumed.saturating_sub(freed)).unwrap_or(u64::MAX)
    }

    /// Room left under the limit, from this node's view. Zero once at or over the limit.
    #[must_use]
    pub fn available(&self) -> u64 {
        self.limit.saturating_sub(self.read())
    }

    /// Record a write of `amount` -- the guarded operation. Admitted while this node's view of
    /// the usage leaves room under the limit; refused otherwise, with the room it believes is
    /// left. This is the upper-bounded counter's increment: usage rises, capped by `limit`.
    ///
    /// **Optimistic**: the check is against the *seen* usage, so two nodes writing concurrently
    /// against the same near-full limit can together exceed it -- by at most their mutual
    /// un-gossiped consumption (see the crate docs). The write lands on this node's own slot,
    /// which no other node writes.
    ///
    /// # Errors
    ///
    /// [`Denied`] when the write would breach the limit in this node's view.
    pub fn inc(&mut self, amount: u64) -> Result<(), Denied> {
        let used = u128::from(self.read());
        if used + u128::from(amount) <= u128::from(self.limit) {
            self.slots.entry(self.me.clone()).or_default().consumed += amount;
            Ok(())
        } else {
            Err(Denied {
                available: self.available(),
            })
        }
    }

    /// Record a free of `amount` -- a delete. The counter's decrement: always succeeds, it can
    /// only lower usage.
    ///
    /// Landed on this node's slot. A node may free what another node wrote (deleting someone
    /// else's object), so a single slot's `freed` may exceed its own `consumed`; only the
    /// cluster-wide sums are constrained, and globally frees never exceed writes.
    pub fn dec(&mut self, amount: u64) {
        self.slots.entry(self.me.clone()).or_default().freed += amount;
    }

    /// Fold another replica's state into this one: the join that makes this a CRDT.
    ///
    /// Per slot, the larger `consumed` and the larger `freed` win -- each is grow-only and
    /// single-writer, so the larger is the newer and nothing is lost. Idempotent (merging the
    /// same state twice is a no-op), commutative and associative, so gossip may arrive in any
    /// order, more than once, and every replica still converges. `me` and `limit` are local
    /// context and are left untouched.
    pub fn merge(&mut self, other: &Self) {
        for (slot, their) in &other.slots {
            let mine = self.slots.entry(slot.clone()).or_default();
            mine.consumed = mine.consumed.max(their.consumed);
            mine.freed = mine.freed.max(their.freed);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    /// A counter holding exactly `consumed`/`freed` on one slot, for building merge inputs.
    /// `limit` is `MAX` so the write always lands; merge laws are independent of the limit.
    fn single(me: u32, consumed: u64, freed: u64) -> BCounter {
        let mut c = BCounter::new(me, u64::MAX);
        c.inc(consumed).expect("MAX limit admits any write");
        c.dec(freed);
        c
    }

    /// Merge a list of node states into a base, in the given order.
    fn merge_all(base: &BCounter, parts: &[BCounter]) -> BCounter {
        let mut acc = base.clone();
        for p in parts {
            acc.merge(p);
        }
        acc
    }

    // ---- single-node behaviour -------------------------------------------

    #[test]
    fn a_fresh_counter_is_empty_and_all_available() {
        let c: BCounter = BCounter::new(1, 100);
        assert_eq!(c.read(), 0);
        assert_eq!(c.available(), 100);
        assert_eq!(c.limit(), 100);
    }

    #[test]
    fn inc_records_usage_and_dec_frees_it() {
        let mut c: BCounter = BCounter::new(1, 100);
        assert_eq!(c.inc(30), Ok(()));
        assert_eq!(c.read(), 30);
        assert_eq!(c.available(), 70);
        c.dec(10);
        assert_eq!(c.read(), 20);
        assert_eq!(c.available(), 80);
    }

    #[test]
    fn inc_is_refused_at_the_limit_and_leaves_state_unchanged() {
        let mut c: BCounter = BCounter::new(1, 100);
        c.inc(90).unwrap();
        assert_eq!(c.inc(10), Ok(())); // the exact fill is admitted
        assert_eq!(c.read(), 100);
        assert_eq!(c.available(), 0);
        assert_eq!(c.inc(1), Err(Denied { available: 0 })); // one over is refused, nothing moves
        assert_eq!(c.read(), 100);
    }

    #[test]
    fn denial_reports_the_room_that_is_left() {
        let mut c: BCounter = BCounter::new(1, 100);
        c.inc(70).unwrap();
        assert_eq!(c.inc(50), Err(Denied { available: 30 }));
    }

    #[test]
    fn a_free_reopens_room_for_a_write() {
        let mut c: BCounter = BCounter::new(1, 100);
        c.inc(100).unwrap();
        assert_eq!(c.inc(20), Err(Denied { available: 0 }));
        c.dec(20);
        assert_eq!(c.inc(20), Ok(()));
        assert_eq!(c.read(), 100);
    }

    #[test]
    fn lowering_the_limit_governs_usage_already_recorded() {
        let mut c: BCounter = BCounter::new(1, 100);
        c.inc(80).unwrap();
        c.set_limit(50);
        assert_eq!(c.available(), 0); // already over: no room
        assert_eq!(c.inc(1), Err(Denied { available: 0 }));
        c.dec(40);
        assert_eq!(c.available(), 10);
        assert_eq!(c.inc(10), Ok(()));
    }

    // ---- merge: distribution and convergence -----------------------------

    #[test]
    fn merge_sums_what_each_node_wrote_on_its_own_slot() {
        let mut a: BCounter = BCounter::new(1, 1000);
        let mut b: BCounter = BCounter::new(2, 1000);
        a.inc(300).unwrap();
        b.inc(400).unwrap();
        a.merge(&b);
        assert_eq!(a.read(), 700);
        assert_eq!(a.available(), 300);
    }

    #[test]
    fn a_node_may_free_what_another_node_wrote() {
        let mut a: BCounter = BCounter::new(1, 1000);
        a.inc(500).unwrap();
        let mut b: BCounter = BCounter::new(2, 1000);
        b.merge(&a); // b learns of a's 500
        b.dec(200); // b frees 200 of a's units; the free lands on b's own slot
        assert_eq!(b.read(), 300);
        a.merge(&b);
        assert_eq!(a.read(), 300);
    }

    #[test]
    fn a_stale_view_of_more_frees_than_writes_reads_as_zero_not_underflow() {
        let mut a: BCounter = BCounter::new(1, 1000);
        a.merge(&single(2, 0, 40)); // a free gossiped ahead of the write it undoes
        assert_eq!(a.read(), 0);
        assert_eq!(a.available(), 1000);
    }

    // ---- the deliberate bound: overshoot, never unbounded -----------------

    #[test]
    fn concurrent_writes_may_overshoot_by_the_un_gossiped_remainder() {
        let mut a: BCounter = BCounter::new(1, 100);
        let mut b: BCounter = BCounter::new(2, 100);
        assert_eq!(a.inc(100), Ok(())); // admitted: a's view is empty
        assert_eq!(b.inc(100), Ok(())); // admitted: b's view is empty too
        a.merge(&b);
        assert_eq!(a.read(), 200); // over the limit -- by exactly b's un-gossiped 100
        assert_eq!(a.available(), 0);
        assert_eq!(a.inc(1), Err(Denied { available: 0 })); // now visible, further writes refused
    }

    // ---- CRDT laws, over arbitrary states --------------------------------

    fn node_state() -> impl Strategy<Value = BCounter> {
        (0u32..6, 0u64..100_000, 0u64..100_000)
            .prop_map(|(me, consumed, freed)| single(me, consumed, freed))
    }

    proptest! {
        #[test]
        fn merge_is_idempotent(a in node_state()) {
            let mut once = a.clone();
            once.merge(&a);
            prop_assert_eq!(once, a);
        }

        #[test]
        fn redelivering_a_state_changes_nothing(a in node_state(), b in node_state()) {
            let mut base = a;
            base.merge(&b);
            let mut again = base.clone();
            again.merge(&b); // the same gossip a second time
            prop_assert_eq!(again, base);
        }

        #[test]
        fn merge_is_commutative(base in node_state(), a in node_state(), b in node_state()) {
            let ab = merge_all(&base, &[a.clone(), b.clone()]);
            let ba = merge_all(&base, &[b, a]);
            prop_assert_eq!(ab, ba);
        }

        #[test]
        fn merge_is_associative(a in node_state(), b in node_state(), c in node_state()) {
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
        fn merge_never_lowers_a_slot(a in node_state(), b in node_state()) {
            let before = a.clone();
            let mut after = a;
            after.merge(&b);
            for (slot, s0) in &before.slots {
                let s1 = after.slots.get(slot).expect("a merged slot never vanishes");
                prop_assert!(s1.consumed >= s0.consumed);
                prop_assert!(s1.freed >= s0.freed);
            }
        }

        /// Strong eventual consistency: the same set of node states, merged in any order,
        /// converges to one state.
        #[test]
        fn all_merge_orders_converge(parts in prop::collection::vec(node_state(), 0..8)) {
            let base: BCounter = BCounter::new(0, 1000);
            let forward = merge_all(&base, &parts);
            let mut reversed = parts.clone();
            reversed.reverse();
            let backward = merge_all(&base, &reversed);
            prop_assert_eq!(forward, backward);
        }

        /// The guard holds locally: a single counter never carries its own view over the
        /// limit, and a refused write leaves the counter untouched.
        #[test]
        fn a_single_node_never_charges_itself_over_the_limit(
            limit in 1u64..10_000,
            amounts in prop::collection::vec(0u64..3_000, 0..50),
        ) {
            let mut c: BCounter = BCounter::new(1, limit);
            for amt in amounts {
                let before = c.read();
                match c.inc(amt) {
                    Ok(()) => prop_assert!(c.read() <= limit),
                    Err(Denied { available }) => {
                        prop_assert_eq!(c.read(), before);
                        prop_assert_eq!(available, limit - before);
                    }
                }
            }
        }
    }
}
