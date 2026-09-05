// SPDX-License-Identifier: MIT OR Apache-2.0
// Copyright (c) 2026 Konstantin Osipov.

//! An escrow bounded counter for distributed capacity quotas.
//!
//! Enforce *"no more than `limit` in total"* -- bytes stored, objects held, connections open --
//! across a cluster where every node accepts writes, without a round trip on the write path.
//! This is the escrow (reservation) design of the bounded counter of Balegas et al. ("Extending
//! Eventually Consistent Cloud Databases for Enforcing Numeric Invariants", 2015,
//! arXiv:1503.09052), which descends from O'Neil's escrow transactional method.
//!
//! # The model
//!
//! A [`Quota`] holds the global budget. It lends slices of it, called **grants**, to nodes. A
//! node acquires only against the rights it holds. This check is local; it needs no view of
//! other nodes:
//!
//! ```text
//!   acquire admitted  ⟺  net used on this node + amount ≤ granted to this node
//! ```
//!
//! The quota keeps one rule: `Σ grants ≤ limit`. Each node's usage is capped by its grant, so
//! `Σ used ≤ Σ grants ≤ limit`. The limit is never exceeded. There is no overshoot.
//!
//! The cost is the *false denial*. A node that has used up its grant must refuse a write, even
//! when another node holds unused quota, until the quota moves a grant across.
//!
//! To trade a small overshoot for fewer false denials, let the quota lend `limit + Δ` in total.
//! Then `Σ used ≤ limit + Δ`. `Δ = 0` is strict escrow. A large `Δ` behaves like an optimistic
//! counter. This choice belongs to the quota, not the counter.
//!
//! # What is in this crate, and what is not
//!
//! [`BCounter`] is a pure, single-node data structure. It holds the rights a node has, what it
//! has used, and a gossiped view of every node's usage. The view is a grow-only CRDT, merged by
//! taking the larger value in each slot, so the cluster-wide usage can be read for metrics and
//! rebalancing. It reads no clock and no socket.
//!
//! The quota is deliberately not in this crate. It has to survive leader changes, use a clock
//! for lease expiry, and run a rebalancing policy. That work belongs to the server that embeds
//! this crate. The crate defines only the contract the quota must meet: the [`Quota`] trait. A
//! minimal [`LocalQuota`] is provided for tests and examples, not for production.
//!
//! # Example
//!
//! ```
//! use bcounter::{BCounter, LocalQuota, Quota};
//!
//! // A quota with a global limit of 100, and a node drawing from it.
//! let mut quota = LocalQuota::new(100);
//! let mut a: BCounter = BCounter::new(1, 0); // node 1, no rights yet
//!
//! // `draw` acquires, asking the quota for more when the local grant is short.
//! assert_eq!(a.draw(&mut quota, 60), Ok(())); // the quota grants 60 to node 1
//! assert_eq!(a.granted(), 60);
//! assert_eq!(a.local_available(), 0);
//!
//! // A delete releases rights locally.
//! a.release(25);
//! assert_eq!(a.local_available(), 25);
//! ```

#![forbid(unsafe_code)]
#![warn(missing_docs)]

mod map;
mod quota;

pub use map::BCounterMap;
pub use quota::{LocalQuota, Quota};

use std::collections::BTreeMap;

/// What one node has done with its rights, as seen in the gossiped view. Both fields are
/// grow-only: an acquire only adds to `acquired`, a release only adds to `released`. A slot
/// merges by taking the larger of each, so no update is lost.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct SlotState {
    /// Total acquired against this slot -- writes. Grow-only.
    acquired: u64,
    /// Total released by this slot -- frees, deletes. Grow-only.
    released: u64,
}

impl SlotState {
    /// Net usage: `acquired − released`, saturating at zero for a stale view.
    fn net(self) -> u64 {
        self.acquired.saturating_sub(self.released)
    }
}

/// A refused acquire. It carries the rights this node still holds.
///
/// A denial means this node's grant is used up. It does not mean the cluster is at its limit. To
/// recover, ask the [`Quota`] for more (see [`BCounter::draw`]). The quota grants more if the
/// cluster still has room. Only an empty quota makes the denial final.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Denied {
    /// Rights left on this node -- `granted − net used`. Zero when the grant is used up.
    pub available: u64,
}

/// One node's escrow bounded counter for a single limited resource.
///
/// It holds the rights granted to this node, what it has used, and a merged view of every node's
/// usage. `Id` is the node identity (defaults to `u32` -- a Raft node id, a member uuid, any
/// `Ord + Clone`). See the crate docs for the model.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BCounter<Id: Ord + Clone = u32> {
    /// This node's slot -- the only one it writes.
    me: Id,
    /// Rights this node currently holds, from the quota. Local; not part of the merged view.
    granted: u64,
    /// Every node's usage this replica has seen, merged. Sparse: a node appears only once it has
    /// acquired or released. This node's own slot lives here too.
    slots: BTreeMap<Id, SlotState>,
}

impl<Id: Ord + Clone> BCounter<Id> {
    /// A fresh counter for `me`, holding `granted` rights to start (often `0`, then topped up
    /// from a [`Quota`]).
    #[must_use]
    pub fn new(me: Id, granted: u64) -> Self {
        Self {
            me,
            granted,
            slots: BTreeMap::new(),
        }
    }

    /// This node's own net usage: `acquired − released` on its slot.
    fn net_used(&self) -> u64 {
        self.slots.get(&self.me).copied().unwrap_or_default().net()
    }

    /// Rights granted to this node.
    #[must_use]
    pub fn granted(&self) -> u64 {
        self.granted
    }

    /// Rights this node can still acquire without asking the quota: `granted − net used`.
    #[must_use]
    pub fn local_available(&self) -> u64 {
        self.granted.saturating_sub(self.net_used())
    }

    /// Cluster-wide net usage, from this node's merged view: `Σ (acquired − released)`. For
    /// metrics, and for a quota deciding whether to rebalance.
    #[must_use]
    pub fn global_used(&self) -> u64 {
        let acquired: u128 = self.slots.values().map(|s| u128::from(s.acquired)).sum();
        let released: u128 = self.slots.values().map(|s| u128::from(s.released)).sum();
        u64::try_from(acquired.saturating_sub(released)).unwrap_or(u64::MAX)
    }

    /// Acquire `amount` against the local grant. This is the guarded operation. It is local and
    /// consults no other node. It succeeds while this node's rights leave room.
    ///
    /// # Errors
    ///
    /// [`Denied`] when the grant is used up. On its own this is not a client-facing denial: ask
    /// the [`Quota`] for more (see [`draw`](BCounter::draw)). Only an empty quota makes it final.
    pub fn acquire(&mut self, amount: u64) -> Result<(), Denied> {
        if self.local_available() >= amount {
            self.slots.entry(self.me.clone()).or_default().acquired += amount;
            Ok(())
        } else {
            Err(Denied {
                available: self.local_available(),
            })
        }
    }

    /// Release `amount` rights: a free or a delete. Always succeeds. It lowers usage and returns
    /// rights to this node.
    ///
    /// A node may release what another node acquired -- deleting someone else's object -- so a
    /// slot's `released` may exceed its own `acquired`. Only the cluster-wide sums are
    /// constrained.
    pub fn release(&mut self, amount: u64) {
        self.slots.entry(self.me.clone()).or_default().released += amount;
    }

    /// Accept `amount` more rights from the quota.
    pub fn grant(&mut self, amount: u64) {
        self.granted = self.granted.saturating_add(amount);
    }

    /// Reclaim up to `amount` unused rights from this node, lowering its grant. Returns how much
    /// was actually reclaimed. This is bounded by [`local_available`], because rights already in
    /// use cannot be taken back. Hand the returned figure to [`Quota::reclaim`].
    ///
    /// [`local_available`]: BCounter::local_available
    #[must_use]
    pub fn reclaim(&mut self, amount: u64) -> u64 {
        let take = amount.min(self.local_available());
        self.granted -= take;
        take
    }

    /// Acquire `amount`. If the local grant is short, ask `quota` for the rest first. This is the
    /// common entry point: it turns a local shortfall into a quota request.
    ///
    /// # Errors
    ///
    /// [`Denied`] only when the quota cannot cover the shortfall either -- the cluster is at its
    /// limit. `available` then reflects the rights on hand after the top-up.
    pub fn draw(&mut self, quota: &mut impl Quota<Id>, amount: u64) -> Result<(), Denied> {
        let have = self.local_available();
        if have < amount {
            let got = quota.grant(&self.me, amount - have);
            self.grant(got);
        }
        self.acquire(amount)
    }

    /// Merge another replica's view into this one. For each slot, take the larger `acquired` and
    /// the larger `released`. Each field is grow-only and written by one node, so the larger
    /// value is the newer one and nothing is lost. The merge is idempotent, commutative, and
    /// associative: gossip may arrive in any order, or more than once, and every replica's
    /// `global_used` still converges. `me` and `granted` are local and are not changed.
    pub fn merge(&mut self, other: &Self) {
        for (slot, their) in &other.slots {
            let mine = self.slots.entry(slot.clone()).or_default();
            mine.acquired = mine.acquired.max(their.acquired);
            mine.released = mine.released.max(their.released);
        }
    }

    /// Export the slots as plain tuples `(node, acquired, released)`, for a gossip layer to send.
    /// A first version returns every slot; a later one can return only those changed since the
    /// last call. This mentions no bytes and no transport: the caller encodes the tuples in its
    /// own wire format, ships them, and the peer feeds them to [`apply`](BCounter::apply). It is
    /// the only thing a counter has to expose to be gossiped -- `merge` cannot reach across the
    /// wire because a peer's `BCounter` cannot be rebuilt from nothing.
    #[must_use]
    pub fn delta(&self) -> Vec<(Id, u64, u64)> {
        self.slots
            .iter()
            .map(|(id, s)| (id.clone(), s.acquired, s.released))
            .collect()
    }

    /// Merge the slots a peer exported with [`delta`](BCounter::delta). Per slot, the larger
    /// `acquired` and the larger `released` win -- the same rule as [`merge`](BCounter::merge).
    /// Idempotent, so a delta may arrive more than once. `me` and `granted` are local and are not
    /// changed.
    pub fn apply(&mut self, delta: &[(Id, u64, u64)]) {
        for (id, acquired, released) in delta {
            let mine = self.slots.entry(id.clone()).or_default();
            mine.acquired = mine.acquired.max(*acquired);
            mine.released = mine.released.max(*released);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    /// A counter whose own slot holds exactly `acquired`/`released`, with a wide grant. For
    /// building merge inputs; the grant is set high so `acquire` always lands.
    fn node(me: u32, acquired: u64, released: u64) -> BCounter {
        let mut e = BCounter::new(me, u64::MAX);
        e.acquire(acquired).expect("MAX grant admits any acquire");
        e.release(released);
        e
    }

    fn merge_all(base: &BCounter, parts: &[BCounter]) -> BCounter {
        let mut acc = base.clone();
        for p in parts {
            acc.merge(p);
        }
        acc
    }

    // ---- local behaviour -------------------------------------------------

    #[test]
    fn a_fresh_counter_holds_only_what_it_was_granted() {
        let e: BCounter = BCounter::new(1, 30);
        assert_eq!(e.granted(), 30);
        assert_eq!(e.local_available(), 30);
        assert_eq!(e.global_used(), 0);
    }

    #[test]
    fn acquire_is_capped_by_the_grant_not_by_a_global_limit() {
        let mut e: BCounter = BCounter::new(1, 50);
        assert_eq!(e.acquire(50), Ok(()));
        assert_eq!(e.local_available(), 0);
        assert_eq!(e.acquire(1), Err(Denied { available: 0 }));
        assert_eq!(e.global_used(), 50);
    }

    #[test]
    fn a_release_hands_rights_back_locally() {
        let mut e: BCounter = BCounter::new(1, 50);
        e.acquire(50).unwrap();
        e.release(20);
        assert_eq!(e.local_available(), 20);
        assert_eq!(e.acquire(20), Ok(()));
    }

    #[test]
    fn grant_and_reclaim_move_rights_in_and_out() {
        let mut e: BCounter = BCounter::new(1, 10);
        e.grant(40);
        assert_eq!(e.granted(), 50);
        e.acquire(30).unwrap();
        // Only the unused 20 can be reclaimed, not the 30 in use.
        assert_eq!(e.reclaim(100), 20);
        assert_eq!(e.granted(), 30);
        assert_eq!(e.local_available(), 0);
    }

    #[test]
    fn draw_tops_up_from_the_quota_then_acquires() {
        let mut quota = LocalQuota::new(100);
        let mut e: BCounter = BCounter::new(1, 0);
        assert_eq!(e.draw(&mut quota, 70), Ok(()));
        assert_eq!(e.granted(), 70);
        assert_eq!(quota.available(), 30);
    }

    #[test]
    fn draw_is_denied_only_when_the_quota_is_empty_too() {
        let mut quota = LocalQuota::new(50);
        let mut e: BCounter = BCounter::new(1, 0);
        // 40 fits (the quota grants 40).
        assert_eq!(e.draw(&mut quota, 40), Ok(()));
        // 20 more: local has 0, the quota has only 10 -> topped to 10, still short -> denied.
        assert_eq!(e.draw(&mut quota, 20), Err(Denied { available: 10 }));
        assert_eq!(quota.available(), 0);
    }

    // ---- the merged view converges (CRDT laws) ---------------------------

    #[test]
    fn merge_observes_every_node_s_usage() {
        let mut a: BCounter = BCounter::new(1, 1000);
        let mut b: BCounter = BCounter::new(2, 1000);
        a.acquire(300).unwrap();
        b.acquire(400).unwrap();
        a.merge(&b);
        assert_eq!(a.global_used(), 700);
        // Merging does not touch local rights.
        assert_eq!(a.granted(), 1000);
        assert_eq!(a.local_available(), 700);
    }

    #[test]
    fn a_stale_view_of_more_releases_than_acquires_reads_as_zero() {
        let mut a: BCounter = BCounter::new(1, 1000);
        a.merge(&node(2, 0, 40));
        assert_eq!(a.global_used(), 0);
    }

    fn view() -> impl Strategy<Value = BCounter> {
        (0u32..6, 0u64..100_000, 0u64..100_000)
            .prop_map(|(me, acquired, released)| node(me, acquired, released))
    }

    proptest! {
        #[test]
        fn merge_is_idempotent(a in view()) {
            let mut once = a.clone();
            once.merge(&a);
            prop_assert_eq!(once, a);
        }

        #[test]
        fn merge_is_commutative(base in view(), a in view(), b in view()) {
            prop_assert_eq!(merge_all(&base, &[a.clone(), b.clone()]), merge_all(&base, &[b, a]));
        }

        #[test]
        fn merge_is_associative(a in view(), b in view(), c in view()) {
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
        fn all_merge_orders_converge(parts in prop::collection::vec(view(), 0..8)) {
            let base: BCounter = BCounter::new(0, 0);
            let forward = merge_all(&base, &parts);
            let mut reversed = parts.clone();
            reversed.reverse();
            prop_assert_eq!(forward, merge_all(&base, &reversed));
        }

        /// Gossiping a delta gives the same result as merging the whole counter -- so the wire
        /// path (`delta` on one node, `apply` on another) converges like `merge` does.
        #[test]
        fn apply_of_a_delta_equals_merge(base in view(), other in view()) {
            let mut merged = base.clone();
            merged.merge(&other);
            let mut applied = base;
            applied.apply(&other.delta());
            prop_assert_eq!(applied, merged);
        }

        /// Applying the same delta twice changes nothing after the first.
        #[test]
        fn apply_is_idempotent(base in view(), other in view()) {
            let d = other.delta();
            let mut once = base.clone();
            once.apply(&d);
            let mut twice = once.clone();
            twice.apply(&d);
            prop_assert_eq!(once, twice);
        }

        /// The safety property: with total grants capped, cluster usage never exceeds the cap,
        /// no matter how acquires and merges interleave.
        #[test]
        fn total_usage_never_exceeds_total_grants(
            ceiling in 0u64..5000,
            ops in prop::collection::vec((0usize..5, 0u64..1000), 0..40),
        ) {
            let mut quota = LocalQuota::new(ceiling);
            let mut nodes: Vec<BCounter> = (0..5u32).map(|id| BCounter::new(id, 0)).collect();
            for (who, amount) in ops {
                let _ = nodes[who].draw(&mut quota, amount);
            }
            let mut observer = nodes[0].clone();
            for n in &nodes[1..] {
                observer.merge(n);
            }
            prop_assert!(observer.global_used() <= ceiling);
        }
    }
}
