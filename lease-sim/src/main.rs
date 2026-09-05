// SPDX-License-Identifier: MIT OR Apache-2.0
// Copyright (c) 2026 Konstantin Osipov.

//! A simulation of the tree-leased quota: the exact algorithm the server will run.
//!
//! The plumtree spanning tree roots at the governor. Leases flow down it: the governor grants
//! to its children, each child sub-grants to its own out of what it holds. Two lease kinds run
//! side by side -- **capacity** (a stock: bytes) and **rate** (a flow: a share of the refill
//! rate). Usage flows up: every node keeps its own usage in a [`BCounter`] slot, merges its
//! children's reports into the same counter, and reports the merged map to its parent. The
//! governor's `global_used()` is the cluster total. Because the map merges by node id, a branch
//! that moves in the tree is never counted twice.
//!
//! The protocol on one tree edge:
//!
//! - `Request`: the child wants more. A parent that cannot fill it asks its own parent for the
//!   shortfall, so demand reaches the governor through the tree.
//! - `Grant`: confirms the lease for the governor's epoch. The child dates its lease from the
//!   tick it *sent* the request, so a parent that lapses it (dated from arrival, later) never
//!   re-lends room the child still considers its own. The answer to a `Renew` also carries
//!   `hold`, all the parent books for the child; less than the child holds is a cut.
//! - `Renew`: keep-alive and report -- the child's whole grant, its usage map, and the members
//!   of its subtree. Sent every `TTL/2`, and at once when the child's bookings or epoch
//!   changed. A parent books what the child reports. A child not renewed for a TTL lapses.
//! - `Release`: a node that moved to a new parent hands its lease back to the old one -- only
//!   once the new parent has confirmed the adoption. Until then both book it, and nobody
//!   re-lends it.
//! - `Shrink`: a parent that cut a child's lease and cannot give the difference back itself
//!   passes the cut on to its own children.
//!
//! Rules the simulation showed to be necessary:
//!
//! - A node keeps its parent while that parent delivered a heartbeat within the last two
//!   heartbeat periods, and never takes its own child as parent. Without this, every tree
//!   repair after one lost message moves a lease.
//! - A node drops a lease the moment it lapses: the parent has re-lent that room. Under
//!   `allow` it writes on anyway, self-granting what nobody booked; the next report carries
//!   that up as an over-commit for the tree to absorb.
//! - Under `deny`, a parent cuts a child only after being over-committed for longer than a
//!   round trip: a moving lease is booked twice for exactly that long, on purpose.
//! - A new governor grants nothing and cuts nobody until its reports cover every live member
//!   (the tree has re-oriented and every old lease is booked again) or one TTL has passed.
//! - Lazy plumtree sets stay small and random. A node lazy-linked to everyone grafts onto the
//!   source itself after every lost message, and the tree flattens onto the governor.
//!
//! The governor's heartbeat, broadcast over plumtree, orients the tree: a node's parent is the
//! peer that delivered it.
//!
//! Two policies. `allow`: a node without a good lease (lapsed, or not yet confirmed in the
//! current epoch) writes anyway, and a parent adopts a re-parented lease past its own room.
//! `deny`: it refuses until confirmed, and an adoption past room is cut. What remains is
//! measured:
//!
//!   (a) a governor change: true overshoot, over-commit, the false denials it caused, how far
//!       the governor's total lags the truth, and the re-lease window,
//!   (b) a mid-tree node down and back: the flow to its subtree,
//!   (c) joins: the over-commit lease adoption causes,
//!
//! each with the lease messages per tick it cost, over cluster sizes and TTLs, and over
//! several seeds: the bounds are the worst seed, the costs the mean.
//!
//! Deterministic (seeded), no external dependencies. Run: `cargo run -p lease-sim --release`.

use std::collections::{BTreeMap, BTreeSet};

use bcounter::BCounter;
use plumtree_fsm::{Action, Config, Message, Plumtree};

type Id = u32;

// ---------------------------------------------------------------- rng

struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
    fn unit(&mut self) -> f64 {
        (self.next() >> 11) as f64 / (1u64 << 53) as f64
    }
    fn below(&mut self, n: usize) -> usize {
        (self.next() % n as u64) as usize
    }
}

// ---------------------------------------------------------------- protocol

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Kind {
    Cap = 0,
    Rate = 1,
}

/// The messages on a tree edge.
#[derive(Clone, Debug)]
enum Lease {
    /// child -> parent: lend me up to `want` more. `sent` is the child's clock; the reply
    /// echoes it, and the child dates its lease from it.
    Request { kind: Kind, want: u64, sent: u64 },
    /// parent -> child: `amount` more is yours, confirmed for `epoch`. As the answer to a
    /// `Renew` (`renewal`), `hold` is all the parent books for you -- less than you hold means
    /// a cut -- and it confirms an adoption. The answer to a `Request` says nothing about the
    /// booking as a whole: the report may not have arrived yet.
    Grant {
        kind: Kind,
        amount: u64,
        epoch: u64,
        sent: u64,
        renewal: bool,
        hold: u64,
    },
    /// child -> parent: keep-alive and report. The child's whole grant (a new parent adopts
    /// it), the usage map of its subtree (capacity only), and the subtree's members.
    Renew {
        kind: Kind,
        granted: u64,
        usage: Vec<(Id, u64, u64)>,
        members: Vec<Id>,
        sent: u64,
    },
    /// child -> parent: I no longer hold `amount` of what you lent me.
    Release { kind: Kind, amount: u64 },
    /// parent -> child: your grant is now `to`. Unsolicited; the next ack carries the same
    /// figure as `hold`, so a lost `Shrink` heals.
    Shrink { kind: Kind, to: u64 },
}

#[derive(Clone, Debug)]
enum Wire {
    Plum(Message<Id>),
    Lease(Lease),
}

struct Pending {
    due: u64,
    dst: Id,
    from: Id,
    msg: Wire,
}

/// What a parent remembers about one child's lease of one kind.
#[derive(Clone)]
struct Lent {
    granted: u64,
    /// The child's subtree usage from its last report (capacity only): the floor for a cut.
    used: u64,
    /// The child's subtree members from its last report (capacity only). In a real system
    /// this is a bitmap of raft ids: a few bytes per hundred nodes.
    members: BTreeSet<Id>,
    expires: u64,
    /// When we last gave this child more: a report sent before that is stale by the gift.
    given_at: u64,
}

impl Lent {
    fn new(expires: u64) -> Self {
        Lent {
            granted: 0,
            used: 0,
            members: BTreeSet::new(),
            expires,
            given_at: 0,
        }
    }
}

/// One node's side of one lease kind, as the holder.
#[derive(Clone, Copy)]
struct Held {
    /// The epoch this lease was last confirmed in.
    epoch: u64,
    /// Valid while `now <= valid_until`; refreshed by any `Grant`.
    valid_until: u64,
    last_renew: u64,
    last_request: u64,
}

impl Held {
    fn none() -> Self {
        Held {
            epoch: 0,
            valid_until: 0,
            last_renew: 0,
            last_request: 0,
        }
    }
}

// ---------------------------------------------------------------- node

struct Node {
    id: Id,
    tree: Plumtree<Id>,
    member: bool,
    up: bool,
    parent: Option<Id>,
    /// The last tick the parent delivered a heartbeat.
    parent_seen: u64,
    /// After a parent change: (old parent, capacity, share) to release once the new parent
    /// has confirmed the adoption. Until then both parents book it, and nobody re-lends it.
    pending_release: Option<(Id, u64, u64)>,
    /// Since when this node has been over-committed, per kind. A moving lease is booked by
    /// both parents for a round trip; a cut waits longer than that.
    over_since: [Option<u64>; 2],
    epoch: u64,
    /// Bookings changed since the last report: report at the end of this tick.
    dirty: bool,

    // Capacity, as holder: the library's counter does the accounting. Its own slot is this
    // node's usage; the other slots are its children's reports, merged.
    cap: BCounter<Id>,
    cap_held: Held,
    /// Own true usage (what the durable per-node table would hold).
    cap_used: u64,
    /// Sum lent to children (the lender side; may exceed room after an adoption).
    cap_lent: u64,
    cap_children: BTreeMap<Id, Lent>,

    // Rate, as holder: a share of the refill rate, and the local token bucket.
    share: u64,
    rate_held: Held,
    tokens: f64,
    rate_lent: u64,
    rate_children: BTreeMap<Id, Lent>,

    // per-tick flow accounting (rate)
    offered: u64,
    admitted: u64,
}

impl Node {
    fn active(&self) -> bool {
        self.member && self.up
    }
    /// Capacity room this node may still lend or spend.
    fn cap_room(&self) -> i64 {
        self.cap.local_available() as i64 - self.cap_lent as i64
    }
    /// What this node has promised of `kind` beyond its own lease -- an adoption, or a lost
    /// `Release`.
    fn overcommit(&self, kind: Kind) -> u64 {
        match kind {
            Kind::Cap => (-self.cap_room()).max(0) as u64,
            Kind::Rate => self.rate_lent.saturating_sub(self.share),
        }
    }
    /// What this node may still lend of `kind`.
    fn room(&self, kind: Kind) -> u64 {
        match kind {
            Kind::Cap => self.cap_room().max(0) as u64,
            Kind::Rate => self.share.saturating_sub(self.rate_lent),
        }
    }
    /// This node's view of its subtree's usage: the merged counter.
    fn subtree_used(&self) -> u64 {
        self.cap.global_used()
    }
    /// This node's view of its subtree's members, itself included.
    fn subtree_members(&self) -> BTreeSet<Id> {
        let mut out = BTreeSet::new();
        out.insert(self.id);
        for (c, l) in &self.cap_children {
            out.insert(*c);
            out.extend(l.members.iter().copied());
        }
        out
    }
    fn refill(&self) -> f64 {
        self.share.saturating_sub(self.rate_lent) as f64
    }
    fn held(&mut self, kind: Kind) -> &mut Held {
        match kind {
            Kind::Cap => &mut self.cap_held,
            Kind::Rate => &mut self.rate_held,
        }
    }
    fn books(&mut self, kind: Kind) -> (&mut BTreeMap<Id, Lent>, &mut u64) {
        match kind {
            Kind::Cap => (&mut self.cap_children, &mut self.cap_lent),
            Kind::Rate => (&mut self.rate_children, &mut self.rate_lent),
        }
    }
}

// ---------------------------------------------------------------- params & events

#[derive(Clone, Copy, Debug)]
enum Event {
    Governor,
    Join,
    Down(Id),
    Up(Id),
    /// Choose a mid-tree node (one with descendants) and start recording its subtree's flow.
    PickMidTree,
    DownMidTree,
    UpMidTree,
}

#[derive(Clone)]
struct Params {
    nodes: u32,
    /// Eager peers per node at start, and lazy peers. Lazy sets must stay small and random:
    /// a node lazy-linked to everyone grafts onto the source itself after every lost message,
    /// and the tree flattens onto the governor.
    fanout: usize,
    lazy: usize,
    loss: f64,
    latency: u64,
    /// Governor heartbeat period.
    heartbeat: u64,
    /// Lease TTL; renew every TTL/2.
    ttl: u64,
    /// A node asks again for the same kind at most every this many ticks.
    req_every: u64,
    /// Under `deny`, a node cuts a child only after being over-committed this long.
    cut_after: u64,
    /// Capacity: the limit, the per-request amount, the chunk a node asks for.
    limit: u64,
    load: u64,
    chunk: u64,
    /// Rate: the global refill (tokens/tick) and what each node offers per tick.
    rate: u64,
    offered: u64,
    /// `allow`: a node without a good lease (lapsed, or not yet confirmed in the current
    /// epoch) writes anyway, and a parent adopts a re-parented lease past its own room.
    /// `deny` (false): it refuses until confirmed, and an adoption past room is cut.
    allow: bool,
    rounds: u64,
    events: Vec<(u64, Event)>,
    seed: u64,
}

// ---------------------------------------------------------------- world

struct World {
    nodes: Vec<Node>,
    governor: Id,
    epoch: u64,
    /// The tick of the last governor change.
    governor_since: u64,
    net: Vec<Pending>,
    rng: Rng,
    now: u64,
    true_total: u64,
    mid: Option<Id>,
    subtree: Vec<Id>,
    p: Params,
    // measurements
    lease_msgs: u64,
    peak_overshoot: u64,
    peak_double_booked: u64,
    release_done_at: Option<u64>,
    /// false denials (a write refused while room existed cluster-wide), per tick
    fd_by_tick: Vec<u64>,
    fd_this_tick: u64,
    /// the same, for the recorded subtree only
    sfd_by_tick: Vec<u64>,
    sfd_this_tick: u64,
    /// how far the governor's total trailed the truth, per tick
    lag_by_tick: Vec<u64>,
    /// per-tick subtree flow ratio (admitted/offered) from `PickMidTree` on; `NaN` if nothing
    /// was offered that tick
    subtree_flow: Vec<f64>,
}

impl World {
    fn new(p: Params) -> Self {
        let mut rng = Rng(p.seed);
        let ids: Vec<Id> = (0..p.nodes).collect();
        let nodes = ids
            .iter()
            .map(|&id| Self::fresh(id, &ids, &p, &mut rng))
            .collect();
        let mut w = World {
            nodes,
            governor: 0,
            epoch: 1,
            governor_since: 0,
            net: Vec::new(),
            rng,
            now: 0,
            true_total: 0,
            mid: None,
            subtree: Vec::new(),
            p,
            lease_msgs: 0,
            peak_overshoot: 0,
            peak_double_booked: 0,
            release_done_at: None,
            fd_by_tick: Vec::new(),
            fd_this_tick: 0,
            sfd_by_tick: Vec::new(),
            sfd_this_tick: 0,
            lag_by_tick: Vec::new(),
            subtree_flow: Vec::new(),
        };
        w.crown(0);
        w
    }

    fn fresh(id: Id, others: &[Id], p: &Params, rng: &mut Rng) -> Node {
        let mut peers: Vec<Id> = others.iter().copied().filter(|&x| x != id).collect();
        for i in (1..peers.len()).rev() {
            peers.swap(i, rng.below(i + 1));
        }
        let cut = p.fanout.min(peers.len());
        let lazy_end = (cut + p.lazy).min(peers.len());
        // The GRAFT timeout must exceed the tree's delivery time (depth x latency), or lazy
        // peers graft straight onto the source and the tree flattens.
        let cfg = Config {
            graft_timeout: 8,
            cache_cap: 4096,
        };
        Node {
            id,
            tree: Plumtree::new(
                id,
                peers[..cut].to_vec(),
                peers[cut..lazy_end].to_vec(),
                cfg,
            ),
            member: true,
            up: true,
            parent: None,
            parent_seen: 0,
            pending_release: None,
            over_since: [None, None],
            epoch: 0,
            dirty: false,
            cap: BCounter::new(id, 0),
            cap_held: Held::none(),
            cap_used: 0,
            cap_lent: 0,
            cap_children: BTreeMap::new(),
            share: 0,
            rate_held: Held::none(),
            tokens: 0.0,
            rate_lent: 0,
            rate_children: BTreeMap::new(),
            offered: 0,
            admitted: 0,
        }
    }

    fn idx(&self, id: Id) -> Option<usize> {
        self.nodes.iter().position(|n| n.id == id)
    }

    /// Make `id` the governor: its lease is the limit itself. A demoted governor first drops
    /// its root grant to what it has already promised, so it does not carry the whole limit
    /// into a child role. The new governor hands back what it held from its old parent.
    fn crown(&mut self, id: Id) {
        if let Some(old) = self.idx(self.governor) {
            if self.nodes[old].id != id {
                let n = &mut self.nodes[old];
                let keep = n.cap_used + n.cap_lent;
                let excess = n.cap.granted().saturating_sub(keep);
                let _ = n.cap.reclaim(excess);
                n.share = n.rate_lent;
                n.cap_held = Held::none();
                n.rate_held = Held::none();
            }
        }
        self.governor = id;
        self.governor_since = self.now;
        if let Some(i) = self.idx(id) {
            let n = &self.nodes[i];
            let (old_parent, g, sh) = (n.parent, n.cap.granted(), n.share);
            if let Some(op) = old_parent {
                for (kind, amount) in [(Kind::Cap, g), (Kind::Rate, sh)] {
                    if amount > 0 {
                        self.send(id, op, Wire::Lease(Lease::Release { kind, amount }));
                    }
                }
            }
        }
        let (limit, rate, epoch) = (self.p.limit, self.p.rate, self.epoch);
        if let Some(i) = self.idx(id) {
            let n = &mut self.nodes[i];
            n.parent = None;
            n.epoch = epoch;
            let more = limit.saturating_sub(n.cap.granted());
            n.cap.grant(more);
            n.cap_held = Held {
                epoch,
                valid_until: u64::MAX,
                ..Held::none()
            };
            n.share = rate;
            n.rate_held = Held {
                epoch,
                valid_until: u64::MAX,
                ..Held::none()
            };
        }
    }

    fn send(&mut self, from: Id, dst: Id, msg: Wire) {
        if matches!(msg, Wire::Lease(_)) {
            self.lease_msgs += 1;
        }
        if self.rng.unit() >= self.p.loss {
            self.net.push(Pending {
                due: self.now + self.p.latency,
                dst,
                from,
                msg,
            });
        }
    }

    /// Run a node's plumtree actions.
    fn pump(&mut self, i: usize, from: Option<Id>) {
        let id = self.nodes[i].id;
        for a in self.nodes[i].tree.ready() {
            match a {
                Action::Send(peer, m) => self.send(id, peer, Wire::Plum(m)),
                Action::Deliver(payload) => self.on_heartbeat(i, from, &payload),
            }
        }
    }

    /// A heartbeat from the governor reached this node: adopt its epoch and, unless we are the
    /// governor, take the peer that delivered it as our parent -- with hysteresis: keep the
    /// current parent while it delivered within the last two heartbeat periods, so a tree
    /// repair after one lost message does not move a lease; and never take our own child.
    /// A parent change carries our leases over and releases them from the old parent. A lease
    /// that has lapsed is dropped first: a returning node asks afresh instead of reporting a
    /// stale grant.
    fn on_heartbeat(&mut self, i: usize, from: Option<Id>, payload: &[u8]) {
        let epoch = u64::from_be_bytes(payload[0..8].try_into().unwrap());
        let gov = u32::from_be_bytes(payload[8..12].try_into().unwrap());
        let me = self.nodes[i].id;
        if epoch < self.nodes[i].epoch {
            return;
        }
        if epoch > self.nodes[i].epoch {
            // A new epoch: report now, so the ack confirms the lease in it without waiting
            // for the periodic renewal.
            self.nodes[i].dirty = true;
        }
        self.nodes[i].epoch = epoch;
        if me == gov {
            return;
        }
        let Some(f) = from else { return };
        let now = self.now;
        let old = self.nodes[i].parent;
        if old == Some(f) {
            self.nodes[i].parent_seen = now;
            return;
        }
        let n = &self.nodes[i];
        let parent_alive = old.is_some() && now < n.parent_seen + 2 * self.p.heartbeat;
        let is_child = n.cap_children.contains_key(&f) || n.rate_children.contains_key(&f);
        if parent_alive || is_child {
            return;
        }
        self.lapse(i);
        let (g, sh) = (self.nodes[i].cap.granted(), self.nodes[i].share);
        self.drop_lapsed(i);
        // A release still owed to an even older parent goes out now; this one waits for the
        // new parent's confirmation.
        if let Some((op, g0, s0)) = self.nodes[i].pending_release.take() {
            for (kind, amount) in [(Kind::Cap, g0), (Kind::Rate, s0)] {
                if amount > 0 {
                    self.send(me, op, Wire::Lease(Lease::Release { kind, amount }));
                }
            }
        }
        if let Some(op) = old {
            if g > 0 || sh > 0 {
                self.nodes[i].pending_release = Some((op, g, sh));
            }
        }
        self.nodes[i].parent = Some(f);
        self.nodes[i].parent_seen = now;
        self.renew(i);
    }

    /// A lapsed lease has been, or may have been, re-lent by the parent: drop what is not
    /// spent or lent, so the next report does not claim it and the next ack cannot revive it.
    fn drop_lapsed(&mut self, i: usize) {
        let now = self.now;
        let n = &mut self.nodes[i];
        if now > n.cap_held.valid_until {
            let room = n.cap_room().max(0) as u64;
            if n.cap.reclaim(room) > 0 {
                n.dirty = true;
            }
        }
        if now > n.rate_held.valid_until && n.share > n.rate_lent {
            n.share = n.rate_lent;
            n.dirty = true;
        }
    }

    /// Over-committed to our children: pass a cut down. Bookings stay until each child
    /// reports; a lost `Shrink` is repeated as `hold` in the next ack.
    fn cascade(&mut self, i: usize, kind: Kind) {
        let me = self.nodes[i].id;
        let mut over = self.nodes[i].overcommit(kind);
        if over == 0 {
            return;
        }
        let asks: Vec<(Id, u64)> = {
            let n = &self.nodes[i];
            let map = match kind {
                Kind::Cap => &n.cap_children,
                Kind::Rate => &n.rate_children,
            };
            map.iter()
                .filter_map(|(&c, l)| {
                    let cut = over.min(l.granted.saturating_sub(l.used));
                    over -= cut;
                    (cut > 0).then_some((c, l.granted - cut))
                })
                .collect()
        };
        for (c, to) in asks {
            self.send(me, c, Wire::Lease(Lease::Shrink { kind, to }));
        }
    }

    /// The parent books less than we hold: give the difference back, what is unspent of it.
    fn cut_to(&mut self, i: usize, kind: Kind, hold: u64) {
        let n = &mut self.nodes[i];
        let cut = match kind {
            Kind::Cap => {
                let cut = n.cap.granted().saturating_sub(hold);
                n.cap.reclaim(cut)
            }
            Kind::Rate => {
                let cut = n.share.saturating_sub(hold);
                n.share -= cut;
                cut
            }
        };
        if cut > 0 {
            n.dirty = true;
            self.cascade(i, kind);
        }
    }

    /// Report to the parent: both renewals, and capacity we are clearly not going to use.
    fn renew(&mut self, i: usize) {
        let Some(parent) = self.nodes[i].parent else {
            return;
        };
        let id = self.nodes[i].id;
        let now = self.now;
        self.nodes[i].dirty = false;
        // Hand back room beyond two chunks, so idle nodes do not sit on the quota.
        let spare = self.nodes[i].cap_room().max(0) as u64;
        let keep = 2 * self.p.chunk;
        if spare > keep {
            let back = self.nodes[i].cap.reclaim(spare - keep);
            if back > 0 {
                self.send(
                    id,
                    parent,
                    Wire::Lease(Lease::Release {
                        kind: Kind::Cap,
                        amount: back,
                    }),
                );
            }
        }
        // The same for rate: keep our own demand plus what we lent, hand back the rest.
        let keep = self.p.offered + self.nodes[i].rate_lent;
        if self.nodes[i].share > keep {
            let back = self.nodes[i].share - keep;
            self.nodes[i].share = keep;
            self.send(
                id,
                parent,
                Wire::Lease(Lease::Release {
                    kind: Kind::Rate,
                    amount: back,
                }),
            );
        }
        let n = &self.nodes[i];
        let cap = Lease::Renew {
            kind: Kind::Cap,
            granted: n.cap.granted(),
            usage: n.cap.delta(),
            members: n.subtree_members().into_iter().collect(),
            sent: now,
        };
        // Always sent, even with nothing held: after a `Shrink` to zero the parent must hear
        // the cut, or its stale booking keeps it over-committed.
        let rate = Lease::Renew {
            kind: Kind::Rate,
            granted: n.share,
            usage: Vec::new(),
            members: Vec::new(),
            sent: now,
        };
        self.nodes[i].cap_held.last_renew = now;
        self.nodes[i].rate_held.last_renew = now;
        self.send(id, parent, Wire::Lease(cap));
        self.send(id, parent, Wire::Lease(rate));
    }

    /// Ask the parent for `want` more of `kind`, at most once per `req_every` ticks.
    fn request(&mut self, i: usize, kind: Kind, want: u64) {
        let Some(parent) = self.nodes[i].parent else {
            return;
        };
        let now = self.now;
        let id = self.nodes[i].id;
        let every = self.p.req_every;
        let held = self.nodes[i].held(kind);
        if want == 0 || now < held.last_request + every {
            return;
        }
        held.last_request = now;
        self.send(
            id,
            parent,
            Wire::Lease(Lease::Request {
                kind,
                want,
                sent: now,
            }),
        );
    }

    fn on_lease(&mut self, i: usize, from: Id, msg: Lease) {
        let me = self.nodes[i].id;
        let ttl = self.p.ttl;
        let now = self.now;
        let epoch = self.nodes[i].epoch;
        match msg {
            Lease::Request { kind, want, sent } => {
                // The governor rule: nothing new until the reports cover every live member,
                // or one TTL has passed since the change.
                if me == self.governor && !self.governor_may_grant() {
                    return;
                }
                let give = want.min(self.nodes[i].room(kind));
                let hold = u64::MAX;
                if give > 0 {
                    let (map, lent) = self.nodes[i].books(kind);
                    let e = map.entry(from).or_insert_with(|| Lent::new(now + ttl));
                    e.granted += give;
                    e.expires = now + ttl;
                    e.given_at = now;
                    *lent += give;
                }
                // Always answer: a 0 still confirms the lease in this epoch.
                self.send(
                    me,
                    from,
                    Wire::Lease(Lease::Grant {
                        kind,
                        amount: give,
                        epoch,
                        sent,
                        renewal: false,
                        hold,
                    }),
                );
                // What we could not give, ask for.
                self.request(i, kind, want - give);
            }
            Lease::Grant {
                kind,
                amount,
                epoch: e,
                sent,
                renewal,
                hold,
            } => {
                {
                    let n = &mut self.nodes[i];
                    match kind {
                        Kind::Cap => n.cap.grant(amount),
                        Kind::Rate => n.share += amount,
                    }
                    let held = n.held(kind);
                    held.epoch = held.epoch.max(e);
                    held.valid_until = held.valid_until.max(sent + ttl);
                }
                // Only the current parent's word counts: a late ack from an old parent knows
                // nothing of what the new one gave.
                if self.nodes[i].parent != Some(from) || !renewal {
                    return;
                }
                self.cut_to(i, kind, hold);
                // The parent has booked us: the old parent may let go.
                {
                    if let Some((op, g0, s0)) = self.nodes[i].pending_release.take() {
                        for (kind, amount) in [(Kind::Cap, g0), (Kind::Rate, s0)] {
                            if amount > 0 {
                                self.send(me, op, Wire::Lease(Lease::Release { kind, amount }));
                            }
                        }
                    }
                }
            }
            Lease::Renew {
                kind,
                granted,
                usage,
                members,
                sent,
            } => {
                // The child's report is the truth of what it can spend: book exactly that.
                // A new child (re-parented, or joined) is adopted this way; an old one is
                // reconciled, which also heals a lost `Release` or `Shrink`. A report sent
                // before our last gift reached the child does not include it: keep the booking.
                let allow = self.p.allow;
                let used: u64 = usage.iter().map(|(_, a, r)| a.saturating_sub(*r)).sum();
                let booked = {
                    let n = &mut self.nodes[i];
                    if kind == Kind::Cap {
                        n.cap.apply(&usage);
                    }
                    let (map, lent) = n.books(kind);
                    let e = map.entry(from).or_insert_with(|| Lent::new(0));
                    let members: BTreeSet<Id> = members.into_iter().collect();
                    let new_granted = if sent > e.given_at {
                        granted
                    } else {
                        e.granted.max(granted)
                    };
                    let changed = e.granted != new_granted || e.members != members;
                    *lent = *lent - e.granted + new_granted;
                    e.granted = new_granted;
                    e.used = used;
                    e.members = members;
                    e.expires = now + ttl;
                    if changed {
                        n.dirty = true;
                    }
                    new_granted
                };
                // Under `deny`, what we booked beyond our own lease has to come back: the ack
                // tells the child to keep less, down to what its subtree has used. The booking
                // stays until it reports the cut. A new governor waits for its window first:
                // its bookings are still arriving.
                let settling = me == self.governor && !self.governor_may_grant();
                let long_enough = self.nodes[i].over_since[kind as usize]
                    .is_some_and(|since| now >= since + self.p.cut_after);
                let hold = if allow || settling || !long_enough {
                    booked
                } else {
                    let over = self.nodes[i].overcommit(kind);
                    booked - over.min(booked.saturating_sub(used))
                };
                self.send(
                    me,
                    from,
                    Wire::Lease(Lease::Grant {
                        kind,
                        amount: 0,
                        epoch,
                        sent,
                        renewal: true,
                        hold,
                    }),
                );
            }
            Lease::Release { kind, amount } => {
                let n = &mut self.nodes[i];
                let (map, lent) = n.books(kind);
                if let Some(e) = map.get_mut(&from) {
                    let back = amount.min(e.granted);
                    e.granted -= back;
                    *lent -= back;
                    if e.granted == 0 {
                        map.remove(&from);
                    }
                    n.dirty = true;
                }
            }
            Lease::Shrink { kind, to } => {
                if self.nodes[i].parent == Some(from) {
                    self.cut_to(i, kind, to);
                }
            }
        }
    }

    /// The governor rule: nothing new until the reports cover every live member (the tree has
    /// re-oriented and every old lease is booked again), or one TTL has passed.
    fn governor_may_grant(&self) -> bool {
        if self.now >= self.governor_since + self.p.ttl {
            return true;
        }
        let Some(g) = self.idx(self.governor) else {
            return false;
        };
        let covered = self.nodes[g].subtree_members();
        self.nodes
            .iter()
            .filter(|n| n.active())
            .all(|n| covered.contains(&n.id))
    }

    /// Lapse children that stopped renewing.
    fn lapse(&mut self, i: usize) {
        let now = self.now;
        for kind in [Kind::Cap, Kind::Rate] {
            let (map, lent) = self.nodes[i].books(kind);
            let gone: Vec<(Id, u64)> = map
                .iter()
                .filter(|(_, l)| l.expires < now)
                .map(|(&c, l)| (c, l.granted))
                .collect();
            let any = !gone.is_empty();
            for (c, g) in gone {
                map.remove(&c);
                *lent -= g;
            }
            if any {
                self.nodes[i].dirty = true;
            }
        }
    }

    /// Whether the node's own lease of `kind` is good right now: confirmed in the current
    /// epoch and not lapsed. A confirmed grant of zero is a good lease: the quota said no.
    fn lease_ok(&self, i: usize, kind: Kind) -> bool {
        let n = &self.nodes[i];
        let held = match kind {
            Kind::Cap => &n.cap_held,
            Kind::Rate => &n.rate_held,
        };
        held.epoch == n.epoch && self.now <= held.valid_until
    }

    fn offer_load(&mut self, i: usize) {
        let id = self.nodes[i].id;
        let in_subtree = self.mid.is_some() && self.subtree.contains(&id);

        // Capacity: one write of `load`. With a good lease, spend room. Without one, `allow`
        // writes anyway -- self-granting what no parent has booked, which the next report
        // carries up as an over-commit -- and `deny` refuses.
        let load = self.p.load;
        let good = self.lease_ok(i, Kind::Cap);
        let room = self.nodes[i].cap_room() >= load as i64;
        if (good && room) || (!good && self.p.allow) {
            if !(good && room) {
                self.nodes[i].cap.grant(load);
            }
            self.nodes[i].cap.acquire(load).ok();
            self.nodes[i].cap_used += load;
            self.true_total += load;
        } else {
            if self.true_total + load <= self.p.limit {
                self.fd_this_tick += 1;
                if in_subtree {
                    self.sfd_this_tick += 1;
                }
            }
            let chunk = self.p.chunk;
            self.request(i, Kind::Cap, chunk);
        }

        // Rate: `offered` requests, one token each. Without a good lease, `allow` admits
        // them all and `deny` none.
        let offered = self.p.offered;
        let good = self.lease_ok(i, Kind::Rate);
        let allow = self.p.allow;
        let refill = self.nodes[i].refill();
        let n = &mut self.nodes[i];
        n.tokens = (n.tokens + refill).min(refill.max(1.0) * 2.0);
        n.offered += offered;
        let mut admitted = 0;
        for _ in 0..offered {
            if good && n.tokens >= 1.0 {
                n.tokens -= 1.0;
                admitted += 1;
            } else if !good && allow {
                admitted += 1;
            }
        }
        n.admitted += admitted;
        if admitted < offered {
            let short = (offered + n.rate_lent).saturating_sub(n.share).max(1);
            self.request(i, Kind::Rate, short);
        }
    }

    fn step(&mut self) {
        self.now += 1;
        self.apply_events();

        // Deliver.
        let (due, keep): (Vec<Pending>, Vec<Pending>) =
            self.net.drain(..).partition(|m| m.due <= self.now);
        self.net = keep;
        for m in due {
            let Some(i) = self.idx(m.dst) else { continue };
            if !self.nodes[i].active() {
                continue;
            }
            match m.msg {
                Wire::Plum(pm) => {
                    self.nodes[i].tree.on_message(m.from, pm);
                    self.pump(i, Some(m.from));
                }
                Wire::Lease(l) => self.on_lease(i, m.from, l),
            }
        }

        // Governor heartbeat.
        if self.now.is_multiple_of(self.p.heartbeat) {
            if let Some(g) = self.idx(self.governor) {
                let mut payload = self.epoch.to_be_bytes().to_vec();
                payload.extend_from_slice(&self.governor.to_be_bytes());
                self.nodes[g].tree.broadcast(payload);
                self.pump(g, None);
            }
        }

        // Every active node: lapse children, report, load, heal, tick the tree.
        for i in 0..self.nodes.len() {
            if !self.nodes[i].active() {
                continue;
            }
            self.lapse(i);
            self.drop_lapsed(i);
            let n = &self.nodes[i];
            let due = self.now >= n.cap_held.last_renew + self.p.ttl / 2;
            if n.parent.is_some() && (n.dirty || due) {
                self.renew(i);
            }
            self.offer_load(i);
            // Over-committed after an adoption: ask the parent to cover it, and note since when.
            for kind in [Kind::Cap, Kind::Rate] {
                let over = self.nodes[i].overcommit(kind);
                let since = &mut self.nodes[i].over_since[kind as usize];
                *since = if over > 0 {
                    since.or(Some(self.now))
                } else {
                    None
                };
                self.request(i, kind, over);
            }
            self.nodes[i].tree.tick(1);
            self.pump(i, None);
        }

        self.measure();
    }

    fn measure(&mut self) {
        self.peak_overshoot = self
            .peak_overshoot
            .max(self.true_total.saturating_sub(self.p.limit));
        let booked = self
            .nodes
            .iter()
            .filter(|n| n.active())
            .map(|n| n.overcommit(Kind::Cap))
            .max()
            .unwrap_or(0);
        self.peak_double_booked = self.peak_double_booked.max(booked);
        let lag = self.idx(self.governor).map_or(0, |g| {
            self.true_total.abs_diff(self.nodes[g].subtree_used())
        });
        self.lag_by_tick.push(lag);
        if self.release_done_at.is_none()
            && self.now > self.governor_since
            && self.governor_may_grant()
        {
            self.release_done_at = Some(self.now);
        }
        self.fd_by_tick.push(self.fd_this_tick);
        self.fd_this_tick = 0;
        self.sfd_by_tick.push(self.sfd_this_tick);
        self.sfd_this_tick = 0;
        if self.mid.is_some() {
            let (o, a) = self
                .nodes
                .iter()
                .filter(|n| self.subtree.contains(&n.id))
                .fold((0u64, 0u64), |(o, a), n| (o + n.offered, a + n.admitted));
            self.subtree_flow.push(if o == 0 {
                f64::NAN
            } else {
                a as f64 / o as f64
            });
        }
        for n in &mut self.nodes {
            n.offered = 0;
            n.admitted = 0;
        }
    }

    fn window_slice(v: &[u64], from: u64, to: u64) -> &[u64] {
        let (a, b) = (from as usize, (to as usize).min(v.len()));
        &v[a.min(b)..b]
    }

    /// False denials in the tick window `[from, to)`.
    fn false_denials_in(&self, from: u64, to: u64) -> u64 {
        Self::window_slice(&self.fd_by_tick, from, to).iter().sum()
    }

    fn subtree_false_denials_in(&self, from: u64, to: u64) -> u64 {
        Self::window_slice(&self.sfd_by_tick, from, to).iter().sum()
    }

    /// The most the governor's total trailed the truth in `[from, to)`.
    fn peak_lag_in(&self, from: u64, to: u64) -> u64 {
        Self::window_slice(&self.lag_by_tick, from, to)
            .iter()
            .copied()
            .max()
            .unwrap_or(0)
    }

    /// Ticks after `from` until the governor's total stayed within 5% of the truth for five
    /// ticks.
    fn catch_up_after(&self, from: u64) -> Option<u64> {
        let close = self.p.limit / 20;
        let mut streak = 0u64;
        for (k, &lag) in self.lag_by_tick.iter().enumerate().skip(from as usize) {
            streak = if lag <= close { streak + 1 } else { 0 };
            if streak >= 5 {
                return Some(k as u64 - from - 4);
            }
        }
        None
    }

    fn descendants(&self, root: Id) -> Vec<Id> {
        let mut out = Vec::new();
        let mut frontier = vec![root];
        while let Some(x) = frontier.pop() {
            for n in &self.nodes {
                if n.parent == Some(x) && n.id != x && !out.contains(&n.id) {
                    out.push(n.id);
                    frontier.push(n.id);
                }
            }
        }
        out
    }

    fn apply_events(&mut self) {
        let due: Vec<Event> = self
            .p
            .events
            .iter()
            .filter(|(at, _)| *at == self.now)
            .map(|(_, e)| *e)
            .collect();
        for e in due {
            match e {
                Event::Governor => {
                    let next = self
                        .nodes
                        .iter()
                        .filter(|n| n.active() && n.id != self.governor)
                        .map(|n| n.id)
                        .next()
                        .unwrap_or(self.governor);
                    self.epoch += 1;
                    self.release_done_at = None;
                    self.crown(next);
                }
                Event::Down(id) | Event::Up(id) => self.membership_event(e, id),
                Event::PickMidTree => {
                    // The node with the most descendants, other than the governor.
                    let pick = self
                        .nodes
                        .iter()
                        .filter(|n| n.active() && n.id != self.governor)
                        .map(|n| (self.descendants(n.id).len(), n.id))
                        .filter(|(d, _)| *d > 0)
                        .max();
                    if let Some((_, id)) = pick {
                        self.subtree = self.descendants(id);
                        self.mid = Some(id);
                    }
                }
                Event::DownMidTree => {
                    if let Some(id) = self.mid {
                        self.membership_event(Event::Down(id), id);
                    }
                }
                Event::UpMidTree => {
                    if let Some(id) = self.mid {
                        self.membership_event(Event::Up(id), id);
                    }
                }
                Event::Join => {
                    let new_id = self.nodes.iter().map(|n| n.id).max().unwrap_or(0) + 1;
                    let peers: Vec<Id> = self
                        .nodes
                        .iter()
                        .filter(|n| n.member)
                        .map(|n| n.id)
                        .collect();
                    let p = self.p.clone();
                    let node = Self::fresh(new_id, &peers, &p, &mut self.rng);
                    self.nodes.push(node);
                    for n in &mut self.nodes {
                        if n.active() && n.id != new_id {
                            n.tree.membership(&[new_id], &[]);
                        }
                    }
                }
            }
        }
    }

    fn membership_event(&mut self, e: Event, id: Id) {
        match e {
            Event::Down(_) => {
                if let Some(i) = self.idx(id) {
                    self.nodes[i].up = false;
                }
                for n in &mut self.nodes {
                    if n.active() && n.id != id {
                        n.tree.down(&[id]);
                    }
                }
            }
            Event::Up(_) => {
                if let Some(i) = self.idx(id) {
                    self.nodes[i].up = true;
                    self.nodes[i].parent = None; // re-parent on the next heartbeat
                }
                for n in &mut self.nodes {
                    if n.active() && n.id != id {
                        n.tree.up(&[id]);
                    }
                }
            }
            _ => {}
        }
        if self.governor == id && !self.nodes.iter().any(|n| n.id == id && n.active()) {
            let next = self
                .nodes
                .iter()
                .filter(|n| n.active())
                .map(|n| n.id)
                .next()
                .unwrap_or(id);
            self.epoch += 1;
            self.crown(next);
        }
    }

    fn run_to_end(&mut self) {
        while self.now < self.p.rounds {
            self.step();
        }
    }

    /// Lease messages per tick, per node.
    fn msgs_per_node_tick(&self) -> f64 {
        self.lease_msgs as f64 / self.p.rounds as f64 / f64::from(self.p.nodes)
    }

    fn pct(&self, v: u64) -> f64 {
        v as f64 / self.p.limit as f64 * 100.0
    }
}

// ---------------------------------------------------------------- scenarios

const EVENT_AT: u64 = 300;

fn base(nodes: u32, ttl: u64, allow: bool) -> Params {
    Params {
        nodes,
        fanout: 3,
        lazy: 6,
        loss: 0.05,
        latency: 1,
        heartbeat: 5,
        ttl,
        req_every: 3,
        cut_after: 3,
        limit: 2_000_000,
        load: 20,
        chunk: 2_000_000 / u64::from(nodes) / 4,
        // Refill above demand, so (b) measures the outage and not contention for shares.
        rate: 3 * u64::from(nodes),
        offered: 2,
        allow,
        rounds: 600,
        events: Vec::new(),
        seed: 0x1EA5E,
    }
}

/// (a) A governor change, after the tree has settled.
fn governor_change(nodes: u32, ttl: u64, allow: bool) -> Params {
    Params {
        events: vec![(EVENT_AT, Event::Governor)],
        ..base(nodes, ttl, allow)
    }
}

/// (b) Record a subtree 20 ticks early; its root goes down at `EVENT_AT` and returns 4 TTLs
/// later.
fn mid_tree_outage(nodes: u32, ttl: u64, allow: bool) -> Params {
    Params {
        events: vec![
            (EVENT_AT - 20, Event::PickMidTree),
            (EVENT_AT, Event::DownMidTree),
            (EVENT_AT + 4 * ttl, Event::UpMidTree),
        ],
        ..base(nodes, ttl, allow)
    }
}

/// (c) Five joins in quick succession.
fn joins(nodes: u32, ttl: u64, allow: bool) -> Params {
    Params {
        events: (0..5).map(|k| (EVENT_AT + k * 3, Event::Join)).collect(),
        ..base(nodes, ttl, allow)
    }
}

/// The window an event's effects are attributed to.
fn window(ttl: u64, heartbeat: u64) -> (u64, u64) {
    (EVENT_AT, EVENT_AT + 2 * ttl + heartbeat + 15)
}

struct Flow {
    baseline: f64,
    floor: f64,
    /// Ticks from the outage until the flow first fell below 95% of baseline.
    dip_after: Option<u64>,
    /// How long it then stayed down: ticks until back at 95% of baseline for five ticks.
    dip_len: Option<u64>,
}

/// Flow summary for (b). `None` when the tree was flat and no subtree could be recorded.
fn flow_summary(w: &World, pick_at: u64, down_at: u64) -> Option<Flow> {
    let f = &w.subtree_flow; // index 0 == tick pick_at
    if w.mid.is_none() || f.is_empty() {
        return None;
    }
    let avg = |s: &[f64]| {
        let v: Vec<f64> = s.iter().copied().filter(|x| !x.is_nan()).collect();
        if v.is_empty() {
            f64::NAN
        } else {
            v.iter().sum::<f64>() / v.len() as f64
        }
    };
    let down_idx = ((down_at - pick_at) as usize).min(f.len());
    let baseline = avg(&f[..down_idx]);
    let floor = f[down_idx..]
        .iter()
        .copied()
        .filter(|x| !x.is_nan())
        .fold(f64::INFINITY, f64::min);
    let ok = |x: f64| !x.is_nan() && x >= 0.95 * baseline;
    let start = f
        .iter()
        .enumerate()
        .skip(down_idx)
        .find(|(_, &x)| !ok(x))
        .map(|(k, _)| k);
    let mut dip_len = None;
    if let Some(st) = start {
        let mut streak = 0u64;
        for (k, &x) in f.iter().enumerate().skip(st) {
            streak = if ok(x) { streak + 1 } else { 0 };
            if streak >= 5 {
                dip_len = Some((k - st) as u64 - 4);
                break;
            }
        }
    }
    Some(Flow {
        baseline,
        floor,
        dip_after: start.map(|st| (st - down_idx) as u64),
        dip_len,
    })
}

fn policy(allow: bool) -> &'static str {
    if allow {
        "allow"
    } else {
        "deny"
    }
}

fn opt(v: Option<f64>) -> String {
    v.map_or("-".to_string(), |t| format!("{t:.0}"))
}

const SEEDS: [u64; 5] = [0x1EA5E, 1, 2, 3, 4];

fn mean(v: &[f64]) -> f64 {
    if v.is_empty() {
        f64::NAN
    } else {
        v.iter().sum::<f64>() / v.len() as f64
    }
}

fn mean_opt(v: &[Option<u64>]) -> Option<f64> {
    let present: Vec<f64> = v.iter().flatten().map(|&x| x as f64).collect();
    (present.len() == v.len() && !v.is_empty()).then(|| mean(&present))
}

fn fmax(v: &[f64]) -> f64 {
    v.iter().copied().fold(f64::NEG_INFINITY, f64::max)
}

/// One cell of (a) or (c): the worst overshoot and over-commit over the seeds (the bounds),
/// the mean of the costs.
fn event_cell(mk: fn(u32, u64, bool) -> Params, n: u32, ttl: u64, allow: bool) -> [String; 7] {
    let (mut over, mut booked, mut fd, mut lag, mut catch, mut win, mut msgs) =
        (vec![], vec![], vec![], vec![], vec![], vec![], vec![]);
    for &seed in &SEEDS {
        let mut w = World::new(Params {
            seed,
            ..mk(n, ttl, allow)
        });
        w.run_to_end();
        let mut c = World::new(Params {
            seed,
            ..base(n, ttl, allow)
        });
        c.run_to_end();
        let (a, b) = window(ttl, w.p.heartbeat);
        over.push(w.pct(w.peak_overshoot));
        booked.push(w.pct(w.peak_double_booked));
        fd.push(
            w.false_denials_in(a, b)
                .saturating_sub(c.false_denials_in(a, b)) as f64,
        );
        lag.push(c.pct(c.peak_lag_in(a, b)));
        catch.push(w.catch_up_after(EVENT_AT));
        win.push(w.release_done_at.map(|t| t - EVENT_AT));
        msgs.push(w.msgs_per_node_tick());
    }
    [
        format!("{:.3}%", fmax(&over)),
        format!("{:.1}%", fmax(&booked)),
        format!("{:.0}", mean(&fd)),
        format!("{:.1}%", fmax(&lag)),
        opt(mean_opt(&catch)),
        opt(mean_opt(&win)),
        format!("{:.2}", mean(&msgs)),
    ]
}

fn main() {
    println!(
        "tree-leased quotas over a lossy surrogate network (5% loss, fanout 3, heartbeat 5), \
         {} seeds per row: bounds are the worst seed, costs the mean",
        SEEDS.len()
    );
    println!("+false-den is the event window's count beyond a no-event control's\n");

    println!("(a) governor change at tick {EVENT_AT}");
    println!(
        "  {:>4} {:>4} {:>6} {:>10} {:>11} {:>10} {:>7} {:>8} {:>7} {:>11}",
        "N",
        "TTL",
        "policy",
        "overshoot",
        "overbooked",
        "+false-den",
        "lag ctl",
        "catch-up",
        "window",
        "msgs/node/t"
    );
    for &n in &[10u32, 50, 200] {
        for &ttl in &[10u64, 40] {
            for &allow in &[true, false] {
                let c = event_cell(governor_change, n, ttl, allow);
                println!(
                    "  {:>4} {:>4} {:>6} {:>10} {:>11} {:>10} {:>7} {:>8} {:>7} {:>11}",
                    n,
                    ttl,
                    policy(allow),
                    c[0],
                    c[1],
                    c[2],
                    c[3],
                    c[4],
                    c[5],
                    c[6]
                );
            }
        }
    }

    println!(
        "\n(b) a mid-tree node down at {EVENT_AT}, back 4 TTLs later: its subtree's rate flow"
    );
    println!(
        "  {:>4} {:>4} {:>6} {:>5} {:>8} {:>6} {:>9} {:>8} {:>14} {:>11}",
        "N",
        "TTL",
        "policy",
        "trees",
        "baseline",
        "floor",
        "dip after",
        "dip len",
        "+cap false-den",
        "msgs/node/t"
    );
    for &n in &[10u32, 50, 200] {
        for &ttl in &[10u64, 40] {
            for &allow in &[true, false] {
                let (mut base_, mut floor, mut after, mut len, mut fd, mut msgs) =
                    (vec![], vec![], vec![], vec![], vec![], vec![]);
                let mut dipped = 0usize;
                for &seed in &SEEDS {
                    let mut w = World::new(Params {
                        seed,
                        ..mid_tree_outage(n, ttl, allow)
                    });
                    w.run_to_end();
                    let Some(f) = flow_summary(&w, EVENT_AT - 20, EVENT_AT) else {
                        continue;
                    };
                    let (a, b) = (EVENT_AT, EVENT_AT + 6 * ttl);
                    base_.push(f.baseline);
                    floor.push(f.floor);
                    if let Some(x) = f.dip_after {
                        dipped += 1;
                        after.push(x as f64);
                        len.push(f.dip_len.map_or(f64::NAN, |l| l as f64));
                    }
                    fd.push(w.subtree_false_denials_in(a, b) as f64);
                    msgs.push(w.msgs_per_node_tick());
                }
                let dip = |v: &[f64]| {
                    if dipped == 0 {
                        "-".to_string()
                    } else {
                        format!("{:.0} ({dipped}x)", mean(v))
                    }
                };
                println!(
                    "  {:>4} {:>4} {:>6} {:>5} {:>8.2} {:>6.2} {:>9} {:>8} {:>14.0} {:>11.2}",
                    n,
                    ttl,
                    policy(allow),
                    base_.len(),
                    mean(&base_),
                    floor.iter().copied().fold(f64::INFINITY, f64::min),
                    dip(&after),
                    dip(&len),
                    mean(&fd),
                    mean(&msgs)
                );
            }
        }
    }

    println!("\n(c) five joins from tick {EVENT_AT}: over-commit from lease adoption");
    println!(
        "  {:>4} {:>4} {:>6} {:>11} {:>10} {:>10} {:>11}",
        "N", "TTL", "policy", "overbooked", "overshoot", "+false-den", "msgs/node/t"
    );
    for &n in &[10u32, 50, 200] {
        for &ttl in &[10u64, 40] {
            for &allow in &[true, false] {
                let c = event_cell(joins, n, ttl, allow);
                println!(
                    "  {:>4} {:>4} {:>6} {:>11} {:>10} {:>10} {:>11}",
                    n,
                    ttl,
                    policy(allow),
                    c[1],
                    c[0],
                    c[2],
                    c[6]
                );
            }
        }
    }
    println!(
        "\n  overshoot  = true usage over the limit, peak, % of limit\n  \
         overbooked = the most any node had booked beyond its own lease, peak; a moving lease is\n  \
                      booked by both parents until the new one confirms, on purpose\n  \
         +false-den = writes refused while room existed, in the event window, beyond a control run\n  \
         lag ctl    = how far the governor's total trails the truth with no event, peak\n  \
         catch-up   = ticks after the change until the new governor's total is within 5% of the truth\n  \
         window     = ticks after the change until the new governor may grant again\n  \
         trees      = seeds whose tree had a mid-tree node to take down\n  \
         dip after  = ticks from the outage until the subtree's flow fell below 95% of baseline\n  \
         dip len    = ticks it then stayed down (and in how many seeds it dipped at all)"
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn steady_state_stays_within_the_limit_and_leases_everyone() {
        let mut w = World::new(base(20, 20, true));
        w.run_to_end();
        assert_eq!(w.peak_overshoot, 0, "no event, so no overshoot");
        for n in w.nodes.iter().filter(|n| n.active() && n.id != w.governor) {
            assert!(n.parent.is_some(), "node {} never got a parent", n.id);
            assert!(n.cap.granted() > 0, "node {} never got a lease", n.id);
        }
        assert!(w.true_total > 0);
    }

    #[test]
    fn the_governor_total_never_double_counts() {
        // The usage map merges by node id, so the governor's total is at most the truth,
        // however the tree moves around.
        let mut w = World::new(governor_change(60, 10, true));
        while w.now < w.p.rounds {
            w.step();
            let g = w.idx(w.governor).unwrap();
            assert!(
                w.nodes[g].subtree_used() <= w.true_total,
                "tick {}: governor sees {} > truth {}",
                w.now,
                w.nodes[g].subtree_used(),
                w.true_total
            );
        }
    }

    #[test]
    fn deny_never_overshoots_on_a_governor_change() {
        for &(n, ttl) in &[(20u32, 20u64), (100, 20), (200, 40)] {
            let mut w = World::new(governor_change(n, ttl, false));
            w.run_to_end();
            assert_eq!(
                w.peak_overshoot, 0,
                "N={n} TTL={ttl}: deny must never overshoot"
            );
        }
    }

    #[test]
    fn deny_overshoot_is_bounded_by_the_overcommit_when_the_ttl_is_too_short() {
        // TTL 10 at N=200 is shorter than the tree takes to re-orient: late reports arrive
        // after the governor resumed lending. Even then the overshoot is bounded by what the
        // Shrink cascade is still collecting.
        let mut w = World::new(governor_change(200, 10, false));
        w.run_to_end();
        assert!(
            w.peak_overshoot <= w.peak_double_booked,
            "overshoot {} exceeds the over-commit bound {}",
            w.peak_overshoot,
            w.peak_double_booked
        );
    }

    #[test]
    fn allow_overshoot_on_a_governor_change_is_within_the_window_spend() {
        // At most what every node can spend during the re-lease window.
        let (n, ttl) = (20u32, 20u64);
        let mut w = World::new(governor_change(n, ttl, true));
        w.run_to_end();
        let window_spend = u64::from(n) * w.p.load * (ttl + w.p.heartbeat);
        assert!(
            w.peak_overshoot <= window_spend,
            "overshoot {} exceeds the window bound {window_spend}",
            w.peak_overshoot
        );
    }

    #[test]
    fn a_subtree_dips_under_deny_and_recovers() {
        let ttl = 20;
        let mut w = World::new(mid_tree_outage(30, ttl, false));
        w.run_to_end();
        let f = flow_summary(&w, EVENT_AT - 20, EVENT_AT).expect("a mid-tree node at N=30");
        assert!(
            f.baseline > 0.0,
            "subtree offered nothing before the outage"
        );
        assert!(
            f.floor < f.baseline,
            "flow never dipped: floor {} baseline {}",
            f.floor,
            f.baseline
        );
        assert!(f.dip_after.is_some(), "flow never fell");
        assert!(f.dip_len.is_some(), "subtree flow never recovered");
    }

    #[test]
    fn joins_never_overshoot_under_deny() {
        for &(n, ttl) in &[(30u32, 20u64), (200, 40)] {
            let mut w = World::new(joins(n, ttl, false));
            w.run_to_end();
            assert_eq!(w.peak_overshoot, 0, "N={n} TTL={ttl}");
        }
        let mut w = World::new(joins(200, 10, false));
        w.run_to_end();
        assert!(w.peak_overshoot <= w.peak_double_booked);
    }
}
