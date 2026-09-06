// SPDX-License-Identifier: MIT OR Apache-2.0
// Copyright (c) 2026 Konstantin Osipov.

//! The tree-leased quota, driven the way a server drives it.
//!
//! Every node runs a [`Lease`] (the protocol) and a [`Plumtree`] (the overlay). This file is
//! only the driver: a surrogate network with latency and loss, Raft's cluster view arriving
//! with a delay, a failure detector with a delay, a load, and the measurements. The node logic
//! lives in the two crates; nothing here decides anything about leases.
//!
//! Measured, over cluster sizes, TTLs and seeds:
//!
//!   (a) a leader change: overshoot, over-booking, the false denials it caused, how long the
//!       new leader's total takes to catch up, how deep the tree got and how fast it
//!       rebalanced, and the message cost,
//!   (b) a mid-tree node down and back: the flow to its subtree,
//!   (c) joins: the over-commit that lease adoption causes,
//!   (d) two data centres: how much of the traffic crosses between them.
//!
//! Deterministic (seeded). Run: `cargo run -p lease-sim --release`.

use std::collections::{BTreeMap, BTreeSet};

use leasetree::{Action as LAction, Config as LConfig, Lease, Limit, Message as LMessage};
use plumtree_fsm::{Action as PAction, Config as PConfig, Message as PMessage, Plumtree};

type Id = u32;
type Key = &'static str;
const BYTES: Key = "bytes";
const RPS: Key = "rps";

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

// ---------------------------------------------------------------- the world

enum Wire {
    Plum(PMessage<Id>),
    Lease(LMessage<Id, Key>),
}

struct Pending {
    due: u64,
    dst: Id,
    from: Id,
    msg: Wire,
}

struct Node {
    id: Id,
    dc: u8,
    tree: Plumtree<Id>,
    lease: Lease<Id, Key>,
    member: bool,
    up: bool,
    /// True own usage of the stock: what the durable per-node table would hold.
    used: u64,
    /// The hop count the leader's latest message arrived at.
    depth: Option<u16>,
    offered: u64,
    admitted: u64,
}

impl Node {
    fn active(&self) -> bool {
        self.member && self.up
    }
}

#[derive(Clone, Copy, Debug)]
enum Event {
    Leader,
    Join,
    Down(Id),
    Up(Id),
    PickMidTree,
    DownMidTree,
    UpMidTree,
}

#[derive(Clone)]
struct Params {
    nodes: u32,
    /// Data centres; node `i` is in `i % dcs`.
    dcs: u8,
    /// Nodes per data centre that list a peer in each other data centre. The others know
    /// only their own; that bounds the cross-DC edges of the tree by construction.
    gateways: usize,
    fanout: usize,
    /// Extra random lazy peers per node beyond the eager ones.
    lazy: usize,
    loss: f64,
    latency: u64,
    cross_latency: u64,
    ttl: u64,
    limit: u64,
    load: u64,
    chunk: u64,
    rate: u64,
    offered: u64,
    /// How often the leader sends something over the overlay.
    leader_period: u64,
    /// How long Raft's view takes to reach a node, and the failure detector its verdict.
    view_delay: u64,
    fd_delay: u64,
    rounds: u64,
    events: Vec<(u64, Event)>,
    seed: u64,
}

struct World {
    nodes: Vec<Node>,
    leader: Id,
    term: u64,
    net: Vec<Pending>,
    /// Cluster views on their way to nodes: (due, node).
    views: Vec<(u64, Id)>,
    /// Failure-detector verdicts on their way: (due, event).
    verdicts: Vec<(u64, Event)>,
    rng: Rng,
    now: u64,
    true_total: u64,
    mid: Option<Id>,
    subtree: Vec<Id>,
    p: Params,
    // measurements
    lease_msgs: u64,
    cross_lease_msgs: u64,
    plum_msgs: u64,
    cross_plum_msgs: u64,
    /// Plumtree link swaps (bare grafts) so far.
    swaps: u64,
    /// How cross-DC links re-entered eager sets: by the message that did it.
    cross_adds: BTreeMap<&'static str, u64>,
    peak_overshoot: u64,
    peak_overbooked: u64,
    fd_by_tick: Vec<u64>,
    fd_this_tick: u64,
    sfd_by_tick: Vec<u64>,
    sfd_this_tick: u64,
    lag_by_tick: Vec<u64>,
    depth_by_tick: Vec<u16>,
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
            leader: 0,
            term: 1,
            net: Vec::new(),
            views: Vec::new(),
            verdicts: Vec::new(),
            rng,
            now: 0,
            true_total: 0,
            mid: None,
            subtree: Vec::new(),
            p,
            lease_msgs: 0,
            cross_lease_msgs: 0,
            plum_msgs: 0,
            cross_plum_msgs: 0,
            swaps: 0,
            cross_adds: BTreeMap::new(),
            peak_overshoot: 0,
            peak_overbooked: 0,
            fd_by_tick: Vec::new(),
            fd_this_tick: 0,
            sfd_by_tick: Vec::new(),
            sfd_this_tick: 0,
            lag_by_tick: Vec::new(),
            depth_by_tick: Vec::new(),
            subtree_flow: Vec::new(),
        };
        // Everyone starts with the view: Raft has settled before we begin.
        let members: Vec<Id> = w.nodes.iter().map(|n| n.id).collect();
        for n in &mut w.nodes {
            n.lease.set_cluster_view(Some(&members), 0, 1);
        }
        w
    }

    fn dc_of(id: Id, p: &Params) -> u8 {
        (id % u32::from(p.dcs)) as u8
    }

    fn fresh(id: Id, others: &[Id], p: &Params, rng: &mut Rng) -> Node {
        let dc = Self::dc_of(id, p);
        let mut peers: Vec<Id> = others.iter().copied().filter(|&x| x != id).collect();
        for i in (1..peers.len()).rev() {
            peers.swap(i, rng.below(i + 1));
        }
        // The overlay knows a few peers, not everyone: `fanout` plus `lazy` in the same
        // domain, and, for a gateway, one in each other domain at the cost of the latency
        // ratio. Plumtree picks eager and lazy among them.
        let gateway = (id / u32::from(p.dcs)) < p.gateways as u32;
        let cost_of = |q: Id| -> u8 {
            if Self::dc_of(q, p) == dc {
                0
            } else {
                p.cross_latency as u8
            }
        };
        let mut chosen: Vec<(Id, u8)> = Vec::new();
        let mut same = 0usize;
        let mut entered = BTreeSet::new();
        for &q in &peers {
            let cost = cost_of(q);
            let take = if cost == 0 {
                same += 1;
                same <= p.fanout + p.lazy
            } else {
                gateway && entered.insert(cost)
            };
            if take {
                chosen.push((q, cost));
            }
        }
        let cfg = PConfig {
            graft_timeout: 8,
            cache_cap: 4096,
            fanout: p.fanout,
            ..PConfig::default()
        };
        let mut lease = Lease::new(id, LConfig { ttl: p.ttl });
        lease.set_limit(
            BYTES,
            Limit::Stock {
                limit: p.limit,
                chunk: p.chunk,
                acquired: 0,
                released: 0,
            },
        );
        lease.set_limit(
            RPS,
            Limit::Rate {
                limit: p.rate,
                chunk: p.offered,
            },
        );
        Node {
            id,
            dc,
            tree: Plumtree::new(id, chosen, cfg),
            lease,
            member: true,
            up: true,
            used: 0,
            depth: None,
            offered: 0,
            admitted: 0,
        }
    }

    fn idx(&self, id: Id) -> Option<usize> {
        self.nodes.iter().position(|n| n.id == id)
    }

    fn members(&self) -> Vec<Id> {
        self.nodes
            .iter()
            .filter(|n| n.member)
            .map(|n| n.id)
            .collect()
    }

    fn send(&mut self, from: Id, dst: Id, msg: Wire) {
        let cross = self.nodes[self.idx(from).unwrap()].dc != self.nodes[self.idx(dst).unwrap()].dc;
        match msg {
            Wire::Lease(_) => {
                self.lease_msgs += 1;
                self.cross_lease_msgs += u64::from(cross);
            }
            Wire::Plum(ref pm) => {
                self.plum_msgs += 1;
                self.cross_plum_msgs += u64::from(cross);
                if matches!(pm, PMessage::Graft(None)) {
                    self.swaps += 1;
                }
            }
        }
        if self.rng.unit() >= self.p.loss {
            let latency = if cross {
                self.p.cross_latency
            } else {
                self.p.latency
            };
            self.net.push(Pending {
                due: self.now + latency,
                dst,
                from,
                msg,
            });
        }
    }

    /// Run a node's queued actions: plumtree's and the lease's.
    fn pump(&mut self, i: usize, from: Option<Id>) {
        let id = self.nodes[i].id;
        for a in self.nodes[i].tree.ready() {
            match a {
                PAction::Send(peer, m) => self.send(id, peer, Wire::Plum(m)),
                PAction::Deliver(payload) => {
                    // The leader's traffic: its term and id. Raft's word arrives separately
                    // and slower; the message is only a hint, and the deliverer is upstream.
                    let term = u64::from_be_bytes(payload[0..8].try_into().unwrap());
                    let leader = u32::from_be_bytes(payload[8..12].try_into().unwrap());
                    let n = &mut self.nodes[i];
                    if term > n.lease.term() {
                        n.lease.set_cluster_view(None, leader, term);
                    }
                    if let Some(f) = from {
                        n.lease.set_upstream(f);
                    }
                }
            }
        }
        self.pump_lease(i);
    }

    fn pump_lease(&mut self, i: usize) {
        let id = self.nodes[i].id;
        let up = self.nodes[i].active();
        for LAction::Send(peer, m) in self.nodes[i].lease.ready() {
            if up {
                self.send(id, peer, Wire::Lease(m));
            }
        }
    }

    fn offer_load(&mut self, i: usize) {
        let id = self.nodes[i].id;
        let in_subtree = self.mid.is_some() && self.subtree.contains(&id);
        let load = self.p.load;
        match self.nodes[i].lease.acquire(&[(BYTES, load)]) {
            Ok(()) => {
                self.nodes[i].used += load;
                self.true_total += load;
            }
            Err(_) => {
                if self.true_total + load <= self.p.limit {
                    self.fd_this_tick += 1;
                    if in_subtree {
                        self.sfd_this_tick += 1;
                    }
                }
            }
        }
        let offered = self.p.offered;
        let n = &mut self.nodes[i];
        n.offered += offered;
        for _ in 0..offered {
            if n.lease.acquire(&[(RPS, 1)]).is_ok() {
                n.admitted += 1;
            }
        }
    }

    fn step(&mut self) {
        self.now += 1;
        self.apply_events();

        // Raft's view and the failure detector's verdicts arrive.
        let (due, keep): (Vec<_>, Vec<_>) = self.views.drain(..).partition(|(t, _)| *t <= self.now);
        self.views = keep;
        let members = self.members();
        for (_, id) in due {
            if let Some(i) = self.idx(id) {
                self.nodes[i]
                    .lease
                    .set_cluster_view(Some(&members), self.leader, self.term);
                self.pump_lease(i);
            }
        }
        let (due, keep): (Vec<_>, Vec<_>) =
            self.verdicts.drain(..).partition(|(t, _)| *t <= self.now);
        self.verdicts = keep;
        for (_, e) in due {
            self.verdict(e);
        }

        // The network delivers.
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
                    if let PMessage::Gossip { id, round, .. } = &pm {
                        if id.0 == self.leader {
                            self.nodes[i].depth = Some(*round);
                        }
                    }
                    let kind = match &pm {
                        PMessage::Gossip { .. } => "gossip",
                        PMessage::Ihave(_) => "ihave",
                        PMessage::Graft(Some(_)) => "graft",
                        PMessage::Graft(None) => "swap-in",
                        PMessage::Prune => "prune",
                    };
                    let from_dc = self.nodes[self.idx(m.from).unwrap()].dc;
                    let cross = from_dc != self.nodes[i].dc;
                    let had = self.nodes[i].tree.eager().any(|&e| e == m.from);
                    self.nodes[i].tree.on_message(m.from, pm);
                    let has = self.nodes[i].tree.eager().any(|&e| e == m.from);
                    if cross && !had && has {
                        *self.cross_adds.entry(kind).or_insert(0) += 1;
                    }
                    self.pump(i, Some(m.from));
                }
                Wire::Lease(lm) => {
                    self.nodes[i].lease.on_message(m.from, lm);
                    self.pump_lease(i);
                }
            }
        }

        // The leader says something over the overlay now and then.
        if self.now.is_multiple_of(self.p.leader_period) {
            if let Some(l) = self.idx(self.leader) {
                let mut payload = self.term.to_be_bytes().to_vec();
                payload.extend_from_slice(&self.leader.to_be_bytes());
                self.nodes[l].tree.broadcast(payload);
                self.nodes[l].depth = Some(0);
                self.pump(l, None);
            }
        }

        // Every node: load, then the clocks.
        for i in 0..self.nodes.len() {
            if !self.nodes[i].member {
                continue;
            }
            if self.nodes[i].up {
                self.offer_load(i);
                self.nodes[i].tree.tick(1);
            }
            // A partitioned node's clock still runs; its messages go nowhere.
            self.nodes[i].lease.tick(1);
            self.pump(i, None);
        }
        self.measure();
    }

    fn verdict(&mut self, e: Event) {
        match e {
            Event::Down(id) => {
                for n in &mut self.nodes {
                    if n.active() && n.id != id {
                        n.tree.down(&[id]);
                        n.lease.down(&[id]);
                    }
                }
            }
            Event::Up(id) => {
                for n in &mut self.nodes {
                    if n.active() && n.id != id {
                        n.tree.up(&[id]);
                        n.lease.up(&[id]);
                    }
                }
            }
            _ => {}
        }
    }

    fn measure(&mut self) {
        self.peak_overshoot = self
            .peak_overshoot
            .max(self.true_total.saturating_sub(self.p.limit));
        let booked = self
            .nodes
            .iter()
            .filter(|n| n.active())
            .filter_map(|n| n.lease.stats(&BYTES))
            .map(|s| s.overcommit)
            .max()
            .unwrap_or(0);
        self.peak_overbooked = self.peak_overbooked.max(booked);
        let lag = self.idx(self.leader).map_or(0, |l| {
            self.true_total.abs_diff(self.nodes[l].lease.usage(&BYTES))
        });
        self.lag_by_tick.push(lag);
        let depth = self
            .nodes
            .iter()
            .filter(|n| n.active())
            .filter_map(|n| n.depth)
            .max()
            .unwrap_or(0);
        self.depth_by_tick.push(depth);
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

    fn window_slice<T>(v: &[T], from: u64, to: u64) -> &[T] {
        let (a, b) = (from as usize, (to as usize).min(v.len()));
        &v[a.min(b)..b]
    }

    fn false_denials_in(&self, from: u64, to: u64) -> u64 {
        Self::window_slice(&self.fd_by_tick, from, to).iter().sum()
    }

    fn subtree_false_denials_in(&self, from: u64, to: u64) -> u64 {
        Self::window_slice(&self.sfd_by_tick, from, to).iter().sum()
    }

    /// Ticks after `from` until the leader's total stayed within 5% of the truth for five
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

    /// The tree's depth before `at`, its peak after, and ticks until back within one hop of
    /// the old depth.
    fn depth_recovery(&self, at: u64) -> (u16, u16, Option<u64>) {
        let before = Self::window_slice(&self.depth_by_tick, at - 20, at)
            .iter()
            .copied()
            .max()
            .unwrap_or(0);
        let after = &self.depth_by_tick[at as usize..];
        let peak = after.iter().copied().max().unwrap_or(0);
        let mut streak = 0u64;
        let mut back = None;
        for (k, &d) in after.iter().enumerate() {
            streak = if d <= before + 1 { streak + 1 } else { 0 };
            if streak >= 10 {
                back = Some(k as u64 - 9);
                break;
            }
        }
        (before, peak, back)
    }

    /// Tree edges whose ends are in different data centres, right now.
    fn cross_edges(&self) -> usize {
        self.nodes
            .iter()
            .filter(|n| n.active())
            .filter(|n| {
                n.lease
                    .parent()
                    .and_then(|p| self.idx(*p))
                    .is_some_and(|pi| self.nodes[pi].dc != n.dc)
            })
            .count()
    }

    fn descendants(&self, root: Id) -> Vec<Id> {
        let mut out = Vec::new();
        let mut frontier = vec![root];
        while let Some(x) = frontier.pop() {
            for n in &self.nodes {
                if n.lease.parent() == Some(&x) && n.id != x && !out.contains(&n.id) {
                    out.push(n.id);
                    frontier.push(n.id);
                }
            }
        }
        out
    }

    fn schedule_views(&mut self) {
        let delay = self.p.view_delay;
        let ids: Vec<Id> = self
            .nodes
            .iter()
            .filter(|n| n.member)
            .map(|n| n.id)
            .collect();
        for id in ids {
            let jitter = self.rng.below(2) as u64;
            let due = if id == self.leader {
                self.now
            } else {
                self.now + delay + jitter
            };
            self.views.push((due, id));
        }
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
                Event::Leader => {
                    let next = self
                        .nodes
                        .iter()
                        .filter(|n| n.active() && n.id != self.leader)
                        .map(|n| n.id)
                        .next()
                        .unwrap_or(self.leader);
                    self.term += 1;
                    self.leader = next;
                    self.schedule_views();
                }
                Event::Down(id) | Event::Up(id) => self.liveness(e, id),
                Event::PickMidTree => {
                    let pick = self
                        .nodes
                        .iter()
                        .filter(|n| n.active() && n.id != self.leader)
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
                        self.liveness(Event::Down(id), id);
                    }
                }
                Event::UpMidTree => {
                    if let Some(id) = self.mid {
                        self.liveness(Event::Up(id), id);
                    }
                }
                Event::Join => {
                    let new_id = self.nodes.iter().map(|n| n.id).max().unwrap_or(0) + 1;
                    let peers = self.members();
                    let p = self.p.clone();
                    let node = Self::fresh(new_id, &peers, &p, &mut self.rng);
                    self.nodes.push(node);
                    let new_dc = Self::dc_of(new_id, &self.p);
                    for n in &mut self.nodes {
                        if n.active() && n.id != new_id {
                            let cost = if n.dc == new_dc {
                                0
                            } else {
                                p.cross_latency as u8
                            };
                            n.tree.membership(&[(new_id, cost)], &[]);
                        }
                    }
                    self.schedule_views();
                }
            }
        }
    }

    fn liveness(&mut self, e: Event, id: Id) {
        let Some(i) = self.idx(id) else { return };
        match e {
            Event::Down(_) => self.nodes[i].up = false,
            Event::Up(_) => self.nodes[i].up = true,
            _ => {}
        }
        let due = self.now + self.p.fd_delay;
        self.verdicts.push((due, e));
        if self.leader == id && !self.nodes[i].active() {
            let next = self
                .nodes
                .iter()
                .filter(|n| n.active())
                .map(|n| n.id)
                .next()
                .unwrap_or(id);
            self.term += 1;
            self.leader = next;
            self.schedule_views();
        }
    }

    fn run_to_end(&mut self) {
        while self.now < self.p.rounds {
            self.step();
        }
    }

    fn msgs_per_node_tick(&self) -> f64 {
        self.lease_msgs as f64 / self.p.rounds as f64 / f64::from(self.p.nodes)
    }

    fn pct(&self, v: u64) -> f64 {
        v as f64 / self.p.limit as f64 * 100.0
    }
}

// ---------------------------------------------------------------- scenarios

const EVENT_AT: u64 = 300;

fn base(nodes: u32, ttl: u64) -> Params {
    // Eager fanout about log2 N + 1, as the paper suggests; a fanout of 3 at N = 200 gives a
    // tree a dozen deep, and then a cross-domain shortcut looks worth ten hops.
    let fanout = (f64::from(nodes).log2().ceil() as usize + 1).max(3);
    Params {
        nodes,
        dcs: 1,
        gateways: 2,
        fanout,
        lazy: 6,
        loss: 0.05,
        latency: 1,
        cross_latency: 10,
        ttl,
        limit: 2_000_000,
        load: 20,
        chunk: 2_000_000 / u64::from(nodes) / 4,
        rate: 3 * u64::from(nodes),
        offered: 2,
        leader_period: 5,
        view_delay: 3,
        fd_delay: 5,
        rounds: 600,
        events: Vec::new(),
        seed: 0x1EA5E,
    }
}

fn leader_change(nodes: u32, ttl: u64) -> Params {
    Params {
        events: vec![(EVENT_AT, Event::Leader)],
        ..base(nodes, ttl)
    }
}

fn mid_tree_outage(nodes: u32, ttl: u64) -> Params {
    Params {
        events: vec![
            (EVENT_AT - 20, Event::PickMidTree),
            (EVENT_AT, Event::DownMidTree),
            (EVENT_AT + 4 * ttl, Event::UpMidTree),
        ],
        ..base(nodes, ttl)
    }
}

fn joins(nodes: u32, ttl: u64) -> Params {
    Params {
        events: (0..5).map(|k| (EVENT_AT + k * 3, Event::Join)).collect(),
        ..base(nodes, ttl)
    }
}

fn two_dcs(nodes: u32, ttl: u64) -> Params {
    Params {
        dcs: 2,
        events: vec![(EVENT_AT, Event::Leader)],
        ..base(nodes, ttl)
    }
}

fn window(ttl: u64) -> (u64, u64) {
    (EVENT_AT, EVENT_AT + 2 * ttl + 20)
}

struct Flow {
    baseline: f64,
    floor: f64,
    dip_after: Option<u64>,
    dip_len: Option<u64>,
}

fn flow_summary(w: &World, pick_at: u64, down_at: u64) -> Option<Flow> {
    let f = &w.subtree_flow;
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

/// One cell of (a), (c) or (d): the worst overshoot and over-booking over the seeds (the
/// bounds), the mean of the costs.
struct Cell {
    overshoot: f64,
    overbooked: f64,
    false_den: f64,
    catch_up: Option<f64>,
    depth_before: f64,
    depth_peak: f64,
    depth_back: Option<f64>,
    msgs: f64,
    cross_lease: f64,
    cross_plum: f64,
    cross_edges: f64,
}

fn event_cell(mk: fn(u32, u64) -> Params, n: u32, ttl: u64) -> Cell {
    let mut over = vec![];
    let mut booked = vec![];
    let mut fd = vec![];
    let mut catch = vec![];
    let mut d_before = vec![];
    let mut d_peak = vec![];
    let mut d_back = vec![];
    let mut msgs = vec![];
    let mut xl = vec![];
    let mut xp = vec![];
    let mut xe = vec![];
    for &seed in &SEEDS {
        let mut w = World::new(Params { seed, ..mk(n, ttl) });
        w.run_to_end();
        let mut c = World::new(Params {
            seed,
            dcs: w.p.dcs,
            ..base(n, ttl)
        });
        c.run_to_end();
        let (a, b) = window(ttl);
        over.push(w.pct(w.peak_overshoot));
        booked.push(w.pct(w.peak_overbooked));
        fd.push(
            w.false_denials_in(a, b)
                .saturating_sub(c.false_denials_in(a, b)) as f64,
        );
        catch.push(w.catch_up_after(EVENT_AT));
        let (before, peak, back) = w.depth_recovery(EVENT_AT);
        d_before.push(f64::from(before));
        d_peak.push(f64::from(peak));
        d_back.push(back);
        msgs.push(w.msgs_per_node_tick());
        xl.push(w.cross_lease_msgs as f64 / w.lease_msgs.max(1) as f64 * 100.0);
        xp.push(w.cross_plum_msgs as f64 / w.plum_msgs.max(1) as f64 * 100.0);
        xe.push(w.cross_edges() as f64);
    }
    Cell {
        overshoot: fmax(&over),
        overbooked: fmax(&booked),
        false_den: mean(&fd),
        catch_up: mean_opt(&catch),
        depth_before: mean(&d_before),
        depth_peak: fmax(&d_peak),
        depth_back: mean_opt(&d_back),
        msgs: mean(&msgs),
        cross_lease: mean(&xl),
        cross_plum: mean(&xp),
        cross_edges: fmax(&xe),
    }
}

fn main() {
    println!(
        "tree-leased quotas: leasetree over plumtree-fsm, 5% loss, fanout log2 N + 1, {} seeds per row: \
         bounds are the worst seed, costs the mean",
        SEEDS.len()
    );
    println!("+false-den is the event window's count beyond a no-event control's\n");

    println!("(a) leader change at tick {EVENT_AT}");
    println!(
        "  {:>4} {:>4} {:>10} {:>11} {:>10} {:>8} {:>12} {:>11}",
        "N",
        "TTL",
        "overshoot",
        "overbooked",
        "+false-den",
        "catch-up",
        "depth b/p/back",
        "msgs/node/t"
    );
    for &n in &[10u32, 50, 200] {
        for &ttl in &[10u64, 40] {
            {
                let c = event_cell(leader_change, n, ttl);
                println!(
                    "  {:>4} {:>4} {:>9.3}% {:>10.1}% {:>10.0} {:>8} {:>5.0}/{:>2.0}/{:>3} {:>11.2}",
                    n,
                    ttl,
                    c.overshoot,
                    c.overbooked,
                    c.false_den,
                    opt(c.catch_up),
                    c.depth_before,
                    c.depth_peak,
                    opt(c.depth_back),
                    c.msgs
                );
            }
        }
    }

    println!(
        "\n(b) a mid-tree node down at {EVENT_AT}, back 4 TTLs later: its subtree's rate flow"
    );
    println!(
        "  {:>4} {:>4} {:>5} {:>8} {:>6} {:>9} {:>8} {:>14} {:>11}",
        "N",
        "TTL",
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
            {
                let (mut base_, mut floor, mut after, mut len, mut fd, mut msgs) =
                    (vec![], vec![], vec![], vec![], vec![], vec![]);
                let mut dipped = 0usize;
                for &seed in &SEEDS {
                    let mut w = World::new(Params {
                        seed,
                        ..mid_tree_outage(n, ttl)
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
                    "  {:>4} {:>4} {:>5} {:>8.2} {:>6.2} {:>9} {:>8} {:>14.0} {:>11.2}",
                    n,
                    ttl,
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
        "  {:>4} {:>4} {:>11} {:>10} {:>10} {:>11}",
        "N", "TTL", "overbooked", "overshoot", "+false-den", "msgs/node/t"
    );
    for &n in &[10u32, 50, 200] {
        for &ttl in &[10u64, 40] {
            {
                let c = event_cell(joins, n, ttl);
                println!(
                    "  {:>4} {:>4} {:>10.1}% {:>9.3}% {:>10.0} {:>11.2}",
                    n, ttl, c.overbooked, c.overshoot, c.false_den, c.msgs
                );
            }
        }
    }

    println!(
        "\n(d) two data centres (latency 1 inside, 10 across), with a leader change at {EVENT_AT}"
    );
    println!(
        "  {:>4} {:>4} {:>10} {:>12} {:>12} {:>11} {:>14} {:>11}",
        "N",
        "TTL",
        "overshoot",
        "cross lease",
        "cross plum",
        "cross edges",
        "depth b/p/back",
        "msgs/node/t"
    );
    for &n in &[50u32, 200] {
        for &ttl in &[40u64] {
            {
                let c = event_cell(two_dcs, n, ttl);
                println!(
                    "  {:>4} {:>4} {:>9.3}% {:>11.1}% {:>11.1}% {:>11.0} {:>7.0}/{:>2.0}/{:>3} {:>11.2}",
                    n,
                    ttl,
                    c.overshoot,
                    c.cross_lease,
                    c.cross_plum,
                    c.cross_edges,
                    c.depth_before,
                    c.depth_peak,
                    opt(c.depth_back),
                    c.msgs
                );
            }
        }
    }
    println!(
        "\n  overshoot   = true usage over the limit, peak, % of limit\n  \
         overbooked  = the most any node had booked beyond its own lease, peak; a moving lease is\n  \
                       booked by both parents until the new one confirms, on purpose\n  \
         +false-den  = writes refused while room existed, in the event window, beyond a control run\n  \
         catch-up    = ticks after the change until the new leader's total is within 5% of the truth\n  \
         depth       = tree depth before the change / peak after / ticks until back within one hop\n  \
         cross lease = share of lease messages that crossed between data centres (cross plum: the overlay's)\n  \
         cross edges = tree edges between the data centres at the end, worst seed\n  \
         trees       = seeds whose tree had a mid-tree node to take down\n  \
         dip after   = ticks from the outage until the subtree's flow fell below 95% of baseline\n  \
         dip len     = ticks it then stayed down (and in how many seeds it dipped at all)"
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn steady_state_stays_within_the_limit_and_leases_everyone() {
        let mut w = World::new(base(20, 20));
        w.run_to_end();
        assert_eq!(w.peak_overshoot, 0);
        for n in w.nodes.iter().filter(|n| n.active() && n.id != w.leader) {
            assert!(
                n.lease.parent().is_some(),
                "node {} never got a parent",
                n.id
            );
            assert!(
                n.lease.stats(&BYTES).unwrap().granted > 0,
                "node {} never got a lease",
                n.id
            );
        }
        assert!(w.true_total > 0);
    }

    #[test]
    fn the_leader_total_never_double_counts() {
        let mut w = World::new(leader_change(60, 10));
        while w.now < w.p.rounds {
            w.step();
            let l = w.idx(w.leader).unwrap();
            assert!(
                w.nodes[l].lease.usage(&BYTES) <= w.true_total,
                "tick {}: leader sees {} > truth {}",
                w.now,
                w.nodes[l].lease.usage(&BYTES),
                w.true_total
            );
        }
    }

    #[test]
    fn a_stock_never_overshoots_on_a_leader_change() {
        for &(n, ttl) in &[(20u32, 20u64), (100, 20), (200, 10), (200, 40)] {
            let mut w = World::new(leader_change(n, ttl));
            w.run_to_end();
            assert_eq!(w.peak_overshoot, 0, "N={n} TTL={ttl}");
        }
    }

    #[test]
    fn a_subtree_keeps_flowing_through_its_parent_s_outage() {
        // A rate admits without a lease, so the subtree's flow never stalls, and it is back
        // at baseline once re-leased.
        let mut w = World::new(mid_tree_outage(30, 20));
        w.run_to_end();
        let f = flow_summary(&w, EVENT_AT - 20, EVENT_AT).expect("a mid-tree node at N=30");
        assert!(f.baseline > 0.8, "baseline {}", f.baseline);
        assert!(f.floor > 0.0, "the subtree stalled");
        if f.dip_after.is_some() {
            assert!(f.dip_len.is_some(), "never recovered");
        }
    }

    #[test]
    fn joins_never_overshoot() {
        for &(n, ttl) in &[(30u32, 20u64), (200, 10), (200, 40)] {
            let mut w = World::new(joins(n, ttl));
            w.run_to_end();
            assert_eq!(w.peak_overshoot, 0, "N={n} TTL={ttl}");
        }
    }

    #[test]
    fn the_tree_rebalances_after_a_leader_change() {
        let mut w = World::new(leader_change(200, 40));
        w.run_to_end();
        let (before, _peak, back) = w.depth_recovery(EVENT_AT);
        assert!(back.is_some(), "the tree never came back to depth {before}");
        let last = *w.depth_by_tick.last().unwrap();
        assert!(last <= before + 1, "ended at depth {last}, was {before}");
    }

    #[test]
    fn two_data_centres_are_joined_by_few_lease_edges() {
        let mut w = World::new(two_dcs(50, 40));
        w.run_to_end();
        let edges = w.cross_edges();
        assert!(
            edges <= 4,
            "{edges} tree edges cross between the data centres"
        );
        assert_eq!(w.peak_overshoot, 0);
    }
}
