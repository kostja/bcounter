// SPDX-License-Identifier: MIT OR Apache-2.0
// Copyright (c) 2026 Konstantin Osipov.

//! An escrow bounded counter for distributed capacity quotas.
//!
//! Enforce *"no more than `limit` in total"* -- bytes stored, objects held, connections open --
//! across a cluster where every node accepts writes, **without a round trip on the write path**.
//! This is the escrow (reservation) design of the bounded counter of Balegas et al. ("Extending
//! Eventually Consistent Cloud Databases for Enforcing Numeric Invariants", 2015,
//! arXiv:1503.09052), which itself descends from O'Neil's escrow transactional method.
//!
//! # The model
//!
//! A **pool** owns the global budget and hands out **grants** -- slices of it -- to nodes. A
//! node spends only against the rights it holds, purely locally, no view of anyone else
//! required:
//!
//! ```text
//!   spend admitted  ⟺  net spent on this node + amount ≤ granted to this node
//! ```
//!
//! The safety property follows directly from the pool's one invariant, `Σ grants ≤ limit`:
//! since each node's spending is capped by its grant, `Σ spent ≤ Σ grants ≤ limit`. **The limit
//! is never exceeded** -- no overshoot, ever -- as long as the pool honors its ceiling. That is
//! the whole point of escrow, and the trade for it is the *false denial*: a node whose grant is
//! spent must refuse a write even while unspent quota sits on another node, until the pool moves
//! a grant across (see [`Pool`]).
//!
//! An operator willing to tolerate a bounded overshoot in exchange for fewer false denials
//! simply lets the pool hand out `limit + Δ` in grants; then `Σ spent ≤ limit + Δ`. `Δ = 0` is
//! strict escrow; large `Δ` approaches an optimistic counter. The knob lives in the pool, not
//! here.
//!
//! # What is in this crate, and what is not
//!
//! [`Escrow`] is a **pure, single-node** data structure: the rights a node holds, what it has
//! spent, and a gossiped view of every node's spending (a grow-only CRDT, merged by elementwise
//! `max`, so the global usage can be observed for metrics and rebalancing). It reads no clock
//! and no socket.
//!
//! The **pool / allocator is deliberately out of this crate** -- it needs durability across
//! leader changes, a clock for lease expiry, and a rebalancing policy, all of which belong to
//! the shell embedding this. What the crate defines is the [`Pool`] **trait**: the contract that
//! allocator must satisfy for the escrow counters it feeds to stay safe. A minimal in-process
//! [`LocalPool`] is provided for tests and examples, not for production.
//!
//! # Example
//!
//! ```
//! use bcounter::{Escrow, LocalPool, Pool};
//!
//! // A pool owning a global limit of 100, and a node drawing from it.
//! let mut pool = LocalPool::new(100);
//! let mut a: Escrow = Escrow::new(1, 0); // node 1, no rights yet
//!
//! // `draw` spends, topping up from the pool when the local grant is short.
//! assert_eq!(a.draw(&mut pool, 60), Ok(())); // pool grants 60 to node 1
//! assert_eq!(a.granted(), 60);
//! assert_eq!(a.local_available(), 0);
//!
//! // A delete returns rights locally.
//! a.refund(25);
//! assert_eq!(a.local_available(), 25);
//! ```

#![forbid(unsafe_code)]
#![warn(missing_docs)]

mod map;
mod pool;

pub use map::EscrowMap;
pub use pool::{LocalPool, Pool};

use std::collections::BTreeMap;

/// What one node has done with its rights, as seen in the gossiped view. Both fields are
/// **grow-only**: a spend only adds to `spent`, a refund only adds to `freed`, so a slot merges
/// by taking the larger of each and never loses an update.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct SlotState {
    /// Total spent against this slot -- writes. Grow-only.
    spent: u64,
    /// Total returned by this slot -- frees, deletes. Grow-only.
    freed: u64,
}

impl SlotState {
    /// Net spending: `spent − freed`, saturating at zero for a stale view.
    fn net(self) -> u64 {
        self.spent.saturating_sub(self.freed)
    }
}

/// A refused spend, carrying the rights this node still holds locally.
///
/// A denial here means *this node's grant is exhausted*, not that the cluster is at its limit.
/// The caller's recourse is to ask the [`Pool`] for more grant (which [`Escrow::draw`] does) --
/// the pool grants it if global quota remains, and only a pool that is itself empty turns this
/// into a true denial to the client.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Denied {
    /// Rights left on this node -- `granted − net spent`. Zero when the grant is spent.
    pub available: u64,
}

/// One node's escrow for a single limited resource.
///
/// Holds the rights granted to this node, what it has spent, and a merged view of every node's
/// spending. `Id` is the node identity (defaults to `u32` -- a Raft node id, a member uuid, any
/// `Ord + Clone`). See the crate docs for the model.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Escrow<Id: Ord + Clone = u32> {
    /// This node's slot -- the only one it writes.
    me: Id,
    /// Rights this node currently holds, from the pool. Local; not part of the merged view.
    granted: u64,
    /// Every node's spending this replica has seen, merged. Sparse: a node appears only once it
    /// has spent or freed. This node's own slot lives here too.
    slots: BTreeMap<Id, SlotState>,
}

impl<Id: Ord + Clone> Escrow<Id> {
    /// A fresh escrow for `me`, holding `granted` rights to start (often `0`, then topped up
    /// from a [`Pool`]).
    #[must_use]
    pub fn new(me: Id, granted: u64) -> Self {
        Self {
            me,
            granted,
            slots: BTreeMap::new(),
        }
    }

    /// This node's own net spending: `spent − freed` on its slot.
    fn net_spent(&self) -> u64 {
        self.slots.get(&self.me).copied().unwrap_or_default().net()
    }

    /// Rights granted to this node.
    #[must_use]
    pub fn granted(&self) -> u64 {
        self.granted
    }

    /// Rights this node can still spend without asking the pool: `granted − net spent`.
    #[must_use]
    pub fn local_available(&self) -> u64 {
        self.granted.saturating_sub(self.net_spent())
    }

    /// Global net usage across the cluster, from this node's merged view: `Σ (spent − freed)`.
    /// For metrics and for a pool deciding whether to rebalance.
    #[must_use]
    pub fn global_used(&self) -> u64 {
        let spent: u128 = self.slots.values().map(|s| u128::from(s.spent)).sum();
        let freed: u128 = self.slots.values().map(|s| u128::from(s.freed)).sum();
        u64::try_from(spent.saturating_sub(freed)).unwrap_or(u64::MAX)
    }

    /// Spend `amount` against the local grant -- the guarded operation, and purely local: it
    /// consults no view of other nodes. Admitted while this node's rights leave room.
    ///
    /// # Errors
    ///
    /// [`Denied`] when the grant is exhausted. This is not a client-facing denial on its own:
    /// the caller should ask the [`Pool`] for more (see [`draw`](Escrow::draw)); only an empty
    /// pool makes it final.
    pub fn spend(&mut self, amount: u64) -> Result<(), Denied> {
        if self.local_available() >= amount {
            self.slots.entry(self.me.clone()).or_default().spent += amount;
            Ok(())
        } else {
            Err(Denied {
                available: self.local_available(),
            })
        }
    }

    /// Return `amount` rights -- a free, a delete. Always succeeds; it can only lower usage and
    /// hand rights back to this node.
    ///
    /// A node may free what another node spent (deleting someone else's object), so a slot's
    /// `freed` may exceed its own `spent`; only the cluster-wide sums are constrained.
    pub fn refund(&mut self, amount: u64) {
        self.slots.entry(self.me.clone()).or_default().freed += amount;
    }

    /// Accept `amount` additional rights from the pool.
    pub fn grant(&mut self, amount: u64) {
        self.granted = self.granted.saturating_add(amount);
    }

    /// Return up to `amount` **unused** rights toward the pool, lowering this node's grant.
    /// Returns how much was actually reclaimed -- bounded by [`local_available`], since spent
    /// rights cannot be taken back. The caller hands the returned figure to [`Pool::release`].
    ///
    /// [`local_available`]: Escrow::local_available
    #[must_use]
    pub fn reclaim(&mut self, amount: u64) -> u64 {
        let take = amount.min(self.local_available());
        self.granted -= take;
        take
    }

    /// Spend `amount`, topping up from `pool` first if the local grant is short. The common
    /// entry point: it turns a local shortfall into a pool request automatically.
    ///
    /// # Errors
    ///
    /// [`Denied`] only when the pool cannot cover the shortfall either -- i.e. the cluster is
    /// genuinely at its limit. `available` then reflects the rights on hand after the top-up.
    pub fn draw(&mut self, pool: &mut impl Pool<Id>, amount: u64) -> Result<(), Denied> {
        let have = self.local_available();
        if have < amount {
            let got = pool.grant(&self.me, amount - have);
            self.grant(got);
        }
        self.spend(amount)
    }

    /// Fold another replica's view into this one: the join that makes the observation a CRDT.
    ///
    /// Per slot, the larger `spent` and the larger `freed` win -- each is grow-only and
    /// single-writer, so the larger is newer and nothing is lost. Idempotent, commutative and
    /// associative, so gossip may arrive in any order, more than once, and every replica's
    /// `global_used` still converges. `me` and `granted` are local and are left untouched.
    pub fn merge(&mut self, other: &Self) {
        for (slot, their) in &other.slots {
            let mine = self.slots.entry(slot.clone()).or_default();
            mine.spent = mine.spent.max(their.spent);
            mine.freed = mine.freed.max(their.freed);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    /// An escrow whose own slot holds exactly `spent`/`freed`, with a wide grant. For building
    /// merge inputs; the grant is set generously so `spend` lands.
    fn node(me: u32, spent: u64, freed: u64) -> Escrow {
        let mut e = Escrow::new(me, u64::MAX);
        e.spend(spent).expect("MAX grant admits any spend");
        e.refund(freed);
        e
    }

    fn merge_all(base: &Escrow, parts: &[Escrow]) -> Escrow {
        let mut acc = base.clone();
        for p in parts {
            acc.merge(p);
        }
        acc
    }

    // ---- local escrow behaviour ------------------------------------------

    #[test]
    fn a_fresh_escrow_holds_only_what_it_was_granted() {
        let e: Escrow = Escrow::new(1, 30);
        assert_eq!(e.granted(), 30);
        assert_eq!(e.local_available(), 30);
        assert_eq!(e.global_used(), 0);
    }

    #[test]
    fn spend_is_capped_by_the_grant_not_by_a_global_limit() {
        let mut e: Escrow = Escrow::new(1, 50);
        assert_eq!(e.spend(50), Ok(()));
        assert_eq!(e.local_available(), 0);
        assert_eq!(e.spend(1), Err(Denied { available: 0 }));
        assert_eq!(e.global_used(), 50);
    }

    #[test]
    fn a_refund_hands_rights_back_locally() {
        let mut e: Escrow = Escrow::new(1, 50);
        e.spend(50).unwrap();
        e.refund(20);
        assert_eq!(e.local_available(), 20);
        assert_eq!(e.spend(20), Ok(()));
    }

    #[test]
    fn grant_and_reclaim_move_rights_in_and_out() {
        let mut e: Escrow = Escrow::new(1, 10);
        e.grant(40);
        assert_eq!(e.granted(), 50);
        e.spend(30).unwrap();
        // Only the unused 20 can be reclaimed, not the spent 30.
        assert_eq!(e.reclaim(100), 20);
        assert_eq!(e.granted(), 30);
        assert_eq!(e.local_available(), 0);
    }

    #[test]
    fn draw_tops_up_from_the_pool_then_spends() {
        let mut pool = LocalPool::new(100);
        let mut e: Escrow = Escrow::new(1, 0);
        assert_eq!(e.draw(&mut pool, 70), Ok(()));
        assert_eq!(e.granted(), 70);
        assert_eq!(pool.available(), 30);
    }

    #[test]
    fn draw_is_denied_only_when_the_pool_is_empty_too() {
        let mut pool = LocalPool::new(50);
        let mut e: Escrow = Escrow::new(1, 0);
        // 40 fits (pool grants 40).
        assert_eq!(e.draw(&mut pool, 40), Ok(()));
        // 20 more: local has 0, pool has only 10 -> topped to 10, still short -> denied.
        assert_eq!(e.draw(&mut pool, 20), Err(Denied { available: 10 }));
        assert_eq!(pool.available(), 0);
    }

    // ---- the merged view converges (CRDT laws) ---------------------------

    #[test]
    fn merge_observes_every_node_s_spending() {
        let mut a: Escrow = Escrow::new(1, 1000);
        let mut b: Escrow = Escrow::new(2, 1000);
        a.spend(300).unwrap();
        b.spend(400).unwrap();
        a.merge(&b);
        assert_eq!(a.global_used(), 700);
        // Merging does not touch local rights.
        assert_eq!(a.granted(), 1000);
        assert_eq!(a.local_available(), 700);
    }

    #[test]
    fn a_stale_view_of_more_frees_than_spends_reads_as_zero() {
        let mut a: Escrow = Escrow::new(1, 1000);
        a.merge(&node(2, 0, 40));
        assert_eq!(a.global_used(), 0);
    }

    fn view() -> impl Strategy<Value = Escrow> {
        (0u32..6, 0u64..100_000, 0u64..100_000)
            .prop_map(|(me, spent, freed)| node(me, spent, freed))
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
            let base: Escrow = Escrow::new(0, 0);
            let forward = merge_all(&base, &parts);
            let mut reversed = parts.clone();
            reversed.reverse();
            prop_assert_eq!(forward, merge_all(&base, &reversed));
        }

        /// The safety property: with total grants capped, cluster spending never exceeds the
        /// cap -- no matter how spends and merges interleave.
        #[test]
        fn total_spending_never_exceeds_total_grants(
            ceiling in 0u64..5000,
            spends in prop::collection::vec((0usize..5, 0u64..1000), 0..40),
        ) {
            let mut pool = LocalPool::new(ceiling);
            let mut nodes: Vec<Escrow> = (0..5u32).map(|id| Escrow::new(id, 0)).collect();
            for (who, amount) in spends {
                let _ = nodes[who].draw(&mut pool, amount);
            }
            let mut observer = nodes[0].clone();
            for n in &nodes[1..] {
                observer.merge(n);
            }
            prop_assert!(observer.global_used() <= ceiling);
        }
    }
}
