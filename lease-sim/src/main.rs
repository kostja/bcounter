// SPDX-License-Identifier: MIT OR Apache-2.0
// Copyright (c) 2026 Konstantin Osipov.

//! The tree-leased rate limit, driven the way a server drives it.
//!
//! Every node runs a [`Lease`] (the protocol) and a [`Plumtree`] (the overlay). This file is
//! only the driver: a surrogate network with latency and loss, Raft's cluster view arriving
//! with a delay, a failure detector with a delay, a load, and the measurements. The node logic
//! lives in the two crates; nothing here decides anything about shares.
//!
//! Measured, over cluster sizes, TTLs and seeds:
//!
//!   (a) a leader change: how far the cluster admits over the rate, how many requests were
//!       throttled while the rate had room, how deep the tree got and how fast it rebalanced,
//!       and the message cost,
//!   (b) a mid-tree node down and back: the flow to its subtree,
//!   (c) joins: the same figures,
//!   (d) two data centres: how much of the traffic crosses between them, and whether the
//!       lease tree follows the overlay's.
//!
//! Deterministic (seeded). Run: `cargo run -p lease-sim --release`.

use std::collections::BTreeSet;

use leasetree::{Action as LAction, Config as LConfig, Lease, LeaseRequest, LeaseResponse, Limit};
use plumtree_fsm::{Action as PAction, Config as PConfig, Message as PMessage, Plumtree};

type Id = u32;
type Key = &'static str;
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
    Call(LeaseRequest<Id, Key>),
    Reply(LeaseResponse<Id, Key>),
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
    /// The hop count the leader's latest message arrived at, and the peers the last two
    /// came through.
    depth: Option<u16>,
    deliverers: [Option<Id>; 2],
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
    /// Nodes per data centre that list a peer in each other data centre.
    gateways: usize,
    fanout: usize,
    /// Extra random lazy peers per node beyond the eager ones.
    lazy: usize,
    loss: f64,
    latency: u64,
    cross_latency: u64,
    ttl: u64,
    /// The cluster-wide rate, what each node offers per tick, and the chunk a node asks for.
    rate: u64,
    offered: u64,
    chunk: u64,
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
    views: Vec<(u64, Id)>,
    verdicts: Vec<(u64, Event)>,
    rng: Rng,
    now: u64,
    mid: Option<Id>,
    subtree: Vec<Id>,
    p: Params,
    // measurements
    lease_msgs: u64,
    cross_lease_msgs: u64,
    plum_msgs: u64,
    cross_plum_msgs: u64,
    /// Requests admitted per tick, cluster-wide, and throttled while the rate had room.
    admitted_by_tick: Vec<u64>,
    throttled_by_tick: Vec<u64>,
    admitted_this_tick: u64,
    throttled_this_tick: u64,
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
            mid: None,
            subtree: Vec::new(),
            p,
            lease_msgs: 0,
            cross_lease_msgs: 0,
            plum_msgs: 0,
            cross_plum_msgs: 0,
            admitted_by_tick: Vec::new(),
            throttled_by_tick: Vec::new(),
            admitted_this_tick: 0,
            throttled_this_tick: 0,
            depth_by_tick: Vec::new(),
            subtree_flow: Vec::new(),
        };
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
            RPS,
            Limit {
                limit: p.rate,
                chunk: p.chunk,
            },
        );
        Node {
            id,
            dc,
            tree: Plumtree::new(id, chosen, cfg),
            lease,
            member: true,
            up: true,
            depth: None,
            deliverers: [None, None],
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
            Wire::Call(_) | Wire::Reply(_) => {
                self.lease_msgs += 1;
                self.cross_lease_msgs += u64::from(cross);
            }
            Wire::Plum(_) => {
                self.plum_msgs += 1;
                self.cross_plum_msgs += u64::from(cross);
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
        for LAction::Call(peer, req) in self.nodes[i].lease.ready() {
            if up {
                self.send(id, peer, Wire::Call(req));
            }
        }
    }

    fn offer_load(&mut self, i: usize) {
        let offered = self.p.offered;
        let n = &mut self.nodes[i];
        n.offered += offered;
        let mut admitted = 0;
        for _ in 0..offered {
            if n.lease.acquire(&[(RPS, 1)]).is_ok() {
                admitted += 1;
            }
        }
        n.admitted += admitted;
        self.admitted_this_tick += admitted;
        self.throttled_this_tick += offered - admitted;
    }

    fn step(&mut self) {
        self.now += 1;
        self.apply_events();

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
                            let n = &mut self.nodes[i];
                            n.depth = Some(*round);
                            n.deliverers = [Some(m.from), n.deliverers[0]];
                        }
                    }
                    self.nodes[i].tree.on_message(m.from, pm);
                    self.pump(i, Some(m.from));
                }
                Wire::Call(req) => {
                    let resp = self.nodes[i].lease.on_request(m.from, req);
                    self.send(m.dst, m.from, Wire::Reply(resp));
                    self.pump_lease(i);
                }
                Wire::Reply(resp) => {
                    self.nodes[i].lease.on_response(m.from, resp);
                    self.pump_lease(i);
                }
            }
        }

        if self.now.is_multiple_of(self.p.leader_period) {
            if let Some(l) = self.idx(self.leader) {
                let mut payload = self.term.to_be_bytes().to_vec();
                payload.extend_from_slice(&self.leader.to_be_bytes());
                self.nodes[l].tree.broadcast(payload);
                self.nodes[l].depth = Some(0);
                self.pump(l, None);
            }
        }

        for i in 0..self.nodes.len() {
            if !self.nodes[i].member {
                continue;
            }
            if self.nodes[i].up {
                self.offer_load(i);
                self.nodes[i].tree.tick(1);
            }
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
        self.admitted_by_tick.push(self.admitted_this_tick);
        self.throttled_by_tick.push(self.throttled_this_tick);
        self.admitted_this_tick = 0;
        self.throttled_this_tick = 0;
        let depth = self
            .nodes
            .iter()
            .filter(|n| n.active())
            .filter_map(|n| n.depth)
            .max()
            .unwrap_or(0);
        self.depth_by_tick.push(depth);
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

    /// How far the cluster admitted over the rate across `[from, to)`, as a share of what the
    /// rate allowed. A token bucket bursts for a tick; a rate is an average.
    fn overshoot_in(&self, from: u64, to: u64) -> f64 {
        (self.utilization_in(from, to) - 100.0).max(0.0)
    }

    /// Admitted over `[from, to)` as a share of what the rate allowed.
    fn utilization_in(&self, from: u64, to: u64) -> f64 {
        let s = Self::window_slice(&self.admitted_by_tick, from, to);
        if s.is_empty() {
            return f64::NAN;
        }
        s.iter().sum::<u64>() as f64 / (self.p.rate * s.len() as u64) as f64 * 100.0
    }

    /// Requests throttled in `[from, to)` in ticks where the cluster admitted less than the
    /// rate: the rate had room and a node refused anyway.
    fn false_throttles_in(&self, from: u64, to: u64) -> u64 {
        let a = Self::window_slice(&self.admitted_by_tick, from, to);
        let t = Self::window_slice(&self.throttled_by_tick, from, to);
        a.iter()
            .zip(t)
            .map(|(&adm, &thr)| thr.min(self.p.rate.saturating_sub(adm)))
            .sum()
    }

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

    /// Non-leader nodes whose lease parent delivered one of the last two of the leader's
    /// messages: the lease tree's agreement with the overlay's.
    fn agreement(&self) -> (usize, usize) {
        let nodes: Vec<&Node> = self
            .nodes
            .iter()
            .filter(|n| n.active() && n.id != self.leader)
            .collect();
        let agree = nodes
            .iter()
            .filter(|n| {
                let p = n.lease.parent();
                p.is_some() && n.deliverers.iter().any(|d| d.as_ref() == p)
            })
            .count();
        (agree, nodes.len())
    }

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
}

// ---------------------------------------------------------------- scenarios

const EVENT_AT: u64 = 300;

fn base(nodes: u32, ttl: u64) -> Params {
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
        // Demand is 2 per node per tick; the rate allows three quarters of it, so the limit
        // binds and shares must move to where the load is.
        rate: 3 * u64::from(nodes) / 2,
        offered: 2,
        chunk: 2,
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

struct Cell {
    overshoot: f64,
    utilization: f64,
    false_throttles: f64,
    depth_before: f64,
    depth_peak: f64,
    depth_back: Option<f64>,
    msgs: f64,
    cross_lease: f64,
    cross_plum: f64,
    cross_edges: f64,
    agreement: f64,
}

fn event_cell(mk: fn(u32, u64) -> Params, n: u32, ttl: u64) -> Cell {
    let (mut over, mut util, mut ft, mut d_before, mut d_peak, mut d_back, mut msgs) =
        (vec![], vec![], vec![], vec![], vec![], vec![], vec![]);
    let (mut xl, mut xp, mut xe, mut ag) = (vec![], vec![], vec![], vec![]);
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
        over.push(w.overshoot_in(a, b));
        util.push(w.utilization_in(a, b));
        ft.push(
            w.false_throttles_in(a, b)
                .saturating_sub(c.false_throttles_in(a, b)) as f64,
        );
        let (before, peak, back) = w.depth_recovery(EVENT_AT);
        d_before.push(f64::from(before));
        d_peak.push(f64::from(peak));
        d_back.push(back);
        msgs.push(w.msgs_per_node_tick());
        xl.push(w.cross_lease_msgs as f64 / w.lease_msgs.max(1) as f64 * 100.0);
        xp.push(w.cross_plum_msgs as f64 / w.plum_msgs.max(1) as f64 * 100.0);
        xe.push(w.cross_edges() as f64);
        let (agree, all) = w.agreement();
        ag.push(agree as f64 / all.max(1) as f64 * 100.0);
    }
    Cell {
        overshoot: fmax(&over),
        utilization: mean(&util),
        false_throttles: mean(&ft),
        depth_before: mean(&d_before),
        depth_peak: fmax(&d_peak),
        depth_back: mean_opt(&d_back),
        msgs: mean(&msgs),
        cross_lease: mean(&xl),
        cross_plum: mean(&xp),
        cross_edges: fmax(&xe),
        agreement: mean(&ag),
    }
}

fn main() {
    println!(
        "tree-leased rate limits: leasetree over plumtree-fsm, 5% loss, fanout log2 N + 1, demand \
         at 4/3 of the rate, {} seeds per row: bounds are the worst seed, costs the mean",
        SEEDS.len()
    );
    println!("+throttled is the event window's count beyond a no-event control's\n");

    for (title, mk) in [
        (
            "(a) leader change at tick 300",
            leader_change as fn(u32, u64) -> Params,
        ),
        ("(c) five joins from tick 300", joins),
    ] {
        println!("{title}");
        println!(
            "  {:>4} {:>4} {:>10} {:>8} {:>11} {:>14} {:>11}",
            "N", "TTL", "overshoot", "used", "+throttled", "depth b/p/back", "msgs/node/t"
        );
        for &n in &[10u32, 50, 200] {
            for &ttl in &[10u64, 40] {
                let c = event_cell(mk, n, ttl);
                println!(
                    "  {:>4} {:>4} {:>9.1}% {:>7.0}% {:>11.0} {:>7.0}/{:>2.0}/{:>3} {:>11.2}",
                    n,
                    ttl,
                    c.overshoot,
                    c.utilization,
                    c.false_throttles,
                    c.depth_before,
                    c.depth_peak,
                    opt(c.depth_back),
                    c.msgs
                );
            }
        }
        println!();
    }

    println!("(b) a mid-tree node down at {EVENT_AT}, back 4 TTLs later: its subtree's flow");
    println!(
        "  {:>4} {:>4} {:>5} {:>8} {:>6} {:>9} {:>8} {:>11}",
        "N", "TTL", "trees", "baseline", "floor", "dip after", "dip len", "msgs/node/t"
    );
    for &n in &[10u32, 50, 200] {
        for &ttl in &[10u64, 40] {
            let (mut base_, mut floor, mut after, mut len, mut msgs) =
                (vec![], vec![], vec![], vec![], vec![]);
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
                base_.push(f.baseline);
                floor.push(f.floor);
                if let Some(x) = f.dip_after {
                    dipped += 1;
                    after.push(x as f64);
                    len.push(f.dip_len.map_or(f64::NAN, |l| l as f64));
                }
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
                "  {:>4} {:>4} {:>5} {:>8.2} {:>6.2} {:>9} {:>8} {:>11.2}",
                n,
                ttl,
                base_.len(),
                mean(&base_),
                floor.iter().copied().fold(f64::INFINITY, f64::min),
                dip(&after),
                dip(&len),
                mean(&msgs)
            );
        }
    }

    println!(
        "\n(d) two data centres (latency 1 inside, 10 across), with a leader change at {EVENT_AT}"
    );
    println!(
        "  {:>4} {:>4} {:>10} {:>12} {:>12} {:>11} {:>7} {:>14} {:>11}",
        "N",
        "TTL",
        "overshoot",
        "cross lease",
        "cross plum",
        "cross edges",
        "follows",
        "depth b/p/back",
        "msgs/node/t"
    );
    for &n in &[50u32, 200] {
        let c = event_cell(two_dcs, n, 40);
        println!(
            "  {:>4} {:>4} {:>9.1}% {:>11.1}% {:>11.1}% {:>11.0} {:>6.0}% {:>7.0}/{:>2.0}/{:>3} {:>11.2}",
            n,
            40,
            c.overshoot,
            c.cross_lease,
            c.cross_plum,
            c.cross_edges,
            c.agreement,
            c.depth_before,
            c.depth_peak,
            opt(c.depth_back),
            c.msgs
        );
    }
    println!(
        "\n  overshoot   = admitted over the rate across the window, as a share of the rate, worst seed\n  \
         used        = admitted in the window, as a share of what the rate allowed\n  \
         +throttled  = requests refused in ticks where the rate had room, beyond a control run\n  \
         depth       = tree depth before the change / peak after / ticks until back within one hop\n  \
         cross lease = share of lease messages that crossed between data centres (cross plum: the overlay's)\n  \
         cross edges = tree edges between the data centres at the end, worst seed\n  \
         follows     = nodes whose lease parent delivered one of the last two of the leader's messages\n  \
         trees       = seeds whose tree had a mid-tree node to take down\n  \
         dip after   = ticks from the outage until the subtree's flow fell below 95% of baseline\n  \
         dip len     = ticks it then stayed down (and in how many seeds it dipped at all)"
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn steady_state_leases_everyone_when_the_rate_covers_the_demand() {
        let mut w = World::new(Params {
            rate: 3 * 20,
            ..base(20, 20)
        });
        w.run_to_end();
        let mut shareless = 0;
        for n in w.nodes.iter().filter(|n| n.active() && n.id != w.leader) {
            assert!(
                n.lease.parent().is_some(),
                "node {} never got a parent",
                n.id
            );
            if n.lease.stats(&RPS).unwrap().granted == 0 {
                shareless += 1; // a call in flight, at most
            }
        }
        assert!(shareless <= 1, "{shareless} nodes without a share");
    }

    #[test]
    fn the_rate_is_used_under_contention() {
        let mut w = World::new(base(20, 20));
        w.run_to_end();
        let used = w.utilization_in(200, 600);
        assert!(used > 90.0, "the rate is used: {used:.0}%");
    }

    #[test]
    fn the_rate_holds_in_steady_state() {
        for &(n, ttl) in &[(20u32, 20u64), (200, 40)] {
            let mut w = World::new(base(n, ttl));
            w.run_to_end();
            assert_eq!(w.overshoot_in(200, 600), 0.0, "N={n} TTL={ttl}");
        }
    }

    #[test]
    fn a_leader_change_overshoots_by_at_most_the_unleased_round_trips() {
        // A node without a share admits everything; the bound is every node's demand for the
        // time it takes to be re-leased, a few ticks.
        for &(n, ttl) in &[(20u32, 20u64), (200, 40)] {
            let mut w = World::new(leader_change(n, ttl));
            w.run_to_end();
            let (a, b) = window(ttl);
            let over = w.overshoot_in(a, b);
            assert!(over <= 50.0, "N={n} TTL={ttl}: {over}% over the rate");
        }
    }

    #[test]
    fn a_subtree_keeps_flowing_through_its_parent_s_outage() {
        let mut w = World::new(mid_tree_outage(30, 20));
        w.run_to_end();
        let f = flow_summary(&w, EVENT_AT - 20, EVENT_AT).expect("a mid-tree node at N=30");
        assert!(f.baseline > 0.5, "baseline {}", f.baseline);
        assert!(f.floor > 0.0, "the subtree stalled");
        if f.dip_after.is_some() {
            assert!(f.dip_len.is_some(), "never recovered");
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
    fn the_lease_tree_settles_on_the_overlay_s_tree() {
        for (mk, loss) in [
            (leader_change as fn(u32, u64) -> Params, 0.0),
            (leader_change, 0.05),
            (two_dcs, 0.0),
            (two_dcs, 0.05),
        ] {
            let mut w = World::new(Params {
                loss,
                ..mk(200, 40)
            });
            w.run_to_end();
            let (agree, all) = w.agreement();
            let floor = if loss == 0.0 { 100 } else { 80 };
            assert!(
                agree * 100 >= all * floor,
                "loss {loss}: {agree} of {all} nodes have an overlay deliverer as lease parent"
            );
        }
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
    }
}
