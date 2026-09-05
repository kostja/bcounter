// SPDX-License-Identifier: MIT OR Apache-2.0
// Copyright (c) 2026 Konstantin Osipov.

//! A surrogate-network simulation of `bcounter` and `plumtree` working together for a
//! distributed quota.
//!
//! Every node runs a `plumtree::Plumtree` and a `bcounter::BCounter`. A node acquires quota
//! locally against a lease it drew from the governor (a `bcounter::LocalQuota`). It gossips its
//! usage as a `bcounter` delta over `plumtree`; peers `apply` it. The network drops a
//! configurable fraction of messages, and a timeline of events -- governor changes, nodes
//! joining and leaving, nodes going down and coming back up -- plays out during the run.
//!
//! It checks two things:
//!   * **enforcement** — the true total usage never exceeds the quota's ceiling, and
//!   * **convergence** — after the load stops, every reachable member's view of the global usage
//!     agrees with the truth, through message loss and the whole event timeline.
//!
//! Deterministic (seeded), no external dependencies. Run: `cargo run -p gossip-sim`.

use bcounter::{BCounter, LocalQuota, Quota};
use plumtree_fsm::{Action, Config, Message, Plumtree};

// ---------------------------------------------------------------- rng

/// SplitMix64 — a tiny deterministic PRNG.
struct Rng {
    state: u64,
}
impl Rng {
    fn new(seed: u64) -> Self {
        Self { state: seed }
    }
    fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
    fn unit(&mut self) -> f64 {
        (self.next_u64() >> 11) as f64 / (1u64 << 53) as f64
    }
    fn below(&mut self, n: usize) -> usize {
        (self.next_u64() % n as u64) as usize
    }
}

// ---------------------------------------------------------------- wire format

/// Encode a `BCounter<u32>` delta as bytes: 20 per slot, big-endian. The sim's stand-in for
/// Picodata's msgpack; the point is the bytes are opaque to `plumtree`.
fn encode(delta: &[(u32, u64, u64)]) -> Vec<u8> {
    let mut out = Vec::with_capacity(delta.len() * 20);
    for (id, a, r) in delta {
        out.extend_from_slice(&id.to_be_bytes());
        out.extend_from_slice(&a.to_be_bytes());
        out.extend_from_slice(&r.to_be_bytes());
    }
    out
}
fn decode(bytes: &[u8]) -> Vec<(u32, u64, u64)> {
    bytes
        .chunks_exact(20)
        .map(|c| {
            let id = u32::from_be_bytes(c[0..4].try_into().unwrap());
            let a = u64::from_be_bytes(c[4..12].try_into().unwrap());
            let r = u64::from_be_bytes(c[12..20].try_into().unwrap());
            (id, a, r)
        })
        .collect()
}

// ---------------------------------------------------------------- world

/// A scheduled change to the cluster.
#[derive(Clone, Copy, Debug)]
enum Event {
    /// The governor (lease root) moves to another member -- a Raft leader change.
    Governor,
    /// A brand-new node joins the cluster.
    Join,
    /// A node leaves for good (membership shrinks).
    Leave(u32),
    /// A node becomes unreachable but stays a member (a transient failure).
    Down(u32),
    /// A downed node becomes reachable again and catches up.
    Up(u32),
}

struct Node {
    id: u32,
    tree: Plumtree<u32>,
    usage: BCounter<u32>,
    /// Still part of the cluster (a `Leave` clears this).
    member: bool,
    /// Reachable and processing (a `Down` clears it, an `Up` restores it).
    up: bool,
}

impl Node {
    /// Live for the run's purposes: a member that is reachable.
    fn active(&self) -> bool {
        self.member && self.up
    }
}

struct Pending {
    due: u64,
    dst: u32,
    from: u32,
    msg: Message<u32>,
}

struct Params {
    nodes: u32,
    limit: u64,
    delta: u64,
    chunk: u64,
    fanout: usize,
    loss: f64,
    latency: u64,
    gossip_every: u64,
    load_amount: u64,
    load_until: u64,
    rounds: u64,
    events: Vec<(u64, Event)>,
    seed: u64,
}

struct World {
    nodes: Vec<Node>,
    quota: LocalQuota<u32>,
    governor: u32,
    net: Vec<Pending>,
    rng: Rng,
    now: u64,
    true_total: u64,
    p: Params,
}

impl World {
    fn new(p: Params) -> Self {
        let mut rng = Rng::new(p.seed);
        let ids: Vec<u32> = (0..p.nodes).collect();
        let nodes = ids
            .iter()
            .map(|&id| Node {
                id,
                tree: fresh_tree(id, &ids, p.fanout, &mut rng),
                usage: BCounter::new(id, 0),
                member: true,
                up: true,
            })
            .collect();
        World {
            nodes,
            quota: LocalQuota::new(p.limit + p.delta),
            governor: 0,
            net: Vec::new(),
            rng,
            now: 0,
            true_total: 0,
            p,
        }
    }

    fn idx(&self, id: u32) -> Option<usize> {
        self.nodes.iter().position(|n| n.id == id)
    }

    /// Run the actions a node produced: enqueue sends (dropping some), apply delivered payloads.
    fn run(&mut self, node_id: u32, actions: Vec<Action<u32>>) {
        let Some(i) = self.idx(node_id) else { return };
        for a in actions {
            match a {
                Action::Send(dst, msg) => {
                    if self.rng.unit() >= self.p.loss {
                        self.net.push(Pending {
                            due: self.now + self.p.latency,
                            dst,
                            from: node_id,
                            msg,
                        });
                    }
                }
                Action::Deliver(payload) => {
                    self.nodes[i].usage.apply(&decode(&payload));
                }
            }
        }
    }

    /// Drain a node's plumtree outbound queue and run it.
    fn pump(&mut self, i: usize) {
        let id = self.nodes[i].id;
        let actions = self.nodes[i].tree.take_outbound();
        self.run(id, actions);
    }

    /// A node draws a lease chunk from the governor if it is short, then acquires `amount`.
    fn offer_load(&mut self, i: usize) {
        let amount = self.p.load_amount;
        if self.nodes[i].usage.local_available() < amount {
            let id = self.nodes[i].id;
            let got = self.quota.grant(&id, self.p.chunk);
            self.nodes[i].usage.grant(got);
        }
        if self.nodes[i].usage.acquire(amount).is_ok() {
            self.true_total += amount;
        }
    }

    fn step(&mut self) {
        self.now += 1;
        self.apply_events();

        // Deliver due messages to reachable members.
        let mut due = Vec::new();
        let mut keep = Vec::new();
        for m in self.net.drain(..) {
            if m.due <= self.now {
                due.push(m);
            } else {
                keep.push(m);
            }
        }
        self.net = keep;
        for m in due {
            if let Some(i) = self.idx(m.dst) {
                if self.nodes[i].active() {
                    self.nodes[i].tree.on_message(self.now, m.from, m.msg);
                    self.pump(i);
                }
            }
        }

        // Load and gossip on every active node.
        for i in 0..self.nodes.len() {
            if !self.nodes[i].active() {
                continue;
            }
            if self.now <= self.p.load_until {
                self.offer_load(i);
            }
            if self.now.is_multiple_of(self.p.gossip_every) {
                let bytes = encode(&self.nodes[i].usage.delta());
                self.nodes[i].tree.broadcast(self.now, bytes);
                self.pump(i);
            }
        }

        // Tick every active node's tree.
        for i in 0..self.nodes.len() {
            if self.nodes[i].active() {
                self.nodes[i].tree.tick(self.now);
                self.pump(i);
            }
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
                Event::Governor => self.governor = self.next_active_after(self.governor),
                Event::Down(id) => {
                    if let Some(i) = self.idx(id) {
                        self.nodes[i].up = false;
                    }
                    // Peers set the down node aside so their trees route around it.
                    for n in &mut self.nodes {
                        if n.active() && n.id != id {
                            n.tree.down(&[id]);
                        }
                    }
                    if self.governor == id {
                        self.governor = self.next_active_after(id);
                    }
                }
                Event::Up(id) => {
                    if let Some(i) = self.idx(id) {
                        self.nodes[i].up = true;
                    }
                    for n in &mut self.nodes {
                        if n.active() && n.id != id {
                            n.tree.up(&[id]);
                        }
                    }
                }
                Event::Leave(id) => {
                    if let Some(i) = self.idx(id) {
                        self.nodes[i].member = false;
                    }
                    for n in &mut self.nodes {
                        if n.active() {
                            n.tree.membership(&[], &[id]);
                        }
                    }
                    if self.governor == id {
                        self.governor = self.next_active_after(id);
                    }
                }
                Event::Join => {
                    let new_id = self.nodes.iter().map(|n| n.id).max().unwrap_or(0) + 1;
                    let peers: Vec<u32> = self
                        .nodes
                        .iter()
                        .filter(|n| n.member)
                        .map(|n| n.id)
                        .collect();
                    let tree = fresh_tree(new_id, &peers, self.p.fanout, &mut self.rng);
                    self.nodes.push(Node {
                        id: new_id,
                        tree,
                        usage: BCounter::new(new_id, 0),
                        member: true,
                        up: true,
                    });
                    for n in &mut self.nodes {
                        if n.active() && n.id != new_id {
                            n.tree.membership(&[new_id], &[]);
                        }
                    }
                }
            }
        }
    }

    fn next_active_after(&self, id: u32) -> u32 {
        self.nodes
            .iter()
            .filter(|n| n.active() && n.id != id)
            .map(|n| n.id)
            .next()
            .unwrap_or(id)
    }

    /// True usage never exceeds the ceiling.
    fn enforcement_holds(&self) -> bool {
        self.true_total <= self.p.limit + self.p.delta
    }

    /// Every reachable member's view of the global usage equals the truth.
    fn converged(&self) -> bool {
        self.nodes
            .iter()
            .filter(|n| n.active())
            .all(|n| n.usage.global_used() == self.true_total)
    }

    /// Every reachable member agrees with every other -- strong eventual consistency. Weaker
    /// than `converged`: it does not require the agreed value to equal the truth, only that the
    /// live replicas hold the same value.
    fn agreed(&self) -> bool {
        let mut vals = self
            .nodes
            .iter()
            .filter(|n| n.active())
            .map(|n| n.usage.global_used());
        match vals.next() {
            Some(first) => vals.all(|v| v == first),
            None => true,
        }
    }

    fn run_to_end(&mut self) {
        while self.now < self.p.rounds {
            self.step();
        }
    }
}

/// A tree for `me`: `fanout` random eager peers from `others`, the rest lazy.
fn fresh_tree(me: u32, others: &[u32], fanout: usize, rng: &mut Rng) -> Plumtree<u32> {
    let mut peers: Vec<u32> = others.iter().copied().filter(|&p| p != me).collect();
    for i in (1..peers.len()).rev() {
        peers.swap(i, rng.below(i + 1));
    }
    let cut = fanout.min(peers.len());
    let eager = peers[..cut].to_vec();
    let lazy = peers[cut..].to_vec();
    // The sim's clock ticks once per round, so the GRAFT timeout is in rounds, not the 500ms
    // default -- otherwise lazy repair never fires within a run.
    let cfg = Config {
        graft_timeout: 3,
        cache_cap: 4096,
    };
    Plumtree::new(me, eager, lazy, cfg)
}

fn base(loss: f64) -> Params {
    Params {
        nodes: 16,
        limit: 1_000_000,
        delta: 0,
        chunk: 1_000_000 / 16,
        fanout: 3,
        loss,
        latency: 1,
        gossip_every: 4,
        load_amount: 50,
        load_until: 200,
        rounds: 400,
        events: Vec::new(),
        seed: 0xB0A710,
    }
}

/// A full lifecycle: a node goes down and comes back, the governor changes twice, a node joins,
/// another leaves. Load runs 1..200; everything is timed so each departed or downed node's usage
/// has spread before it goes (a node dying mid-load would leave an un-gossiped tail that only
/// durable per-node usage recovers -- which this sim does not model).
fn lifecycle(loss: f64) -> Params {
    Params {
        rounds: 500,
        events: vec![
            (50, Event::Down(5)),    // node 5 unreachable during load
            (90, Event::Governor),   // leader change while 5 is down
            (130, Event::Up(5)),     // node 5 returns, catches up, resumes load
            (160, Event::Join),      // a new node joins mid-load
            (250, Event::Down(8)),   // a transient failure in the quiet phase
            (300, Event::Governor),  // another leader change
            (350, Event::Up(8)),     // node 8 returns
            (400, Event::Leave(12)), // node 12 leaves for good, its usage long since spread
        ],
        ..base(loss)
    }
}

/// A long, random timeline of events: nodes flap down and back up, some leave, some join, and
/// the leader changes -- all at random rounds, with a quiet tail so the run settles. The events
/// are timed to stay convergent: every downed node returns before the quiet tail (its own slot
/// keeps its full usage and re-gossips it), and leaves happen in the quiet phase after the
/// leaver's final usage has spread. `flappers` and `leavers` are disjoint sets of nodes.
fn random_params(seed: u64) -> Params {
    let mut rng = Rng::new(seed ^ 0x5EED_1234);
    let n = 16u32;
    let load_until = 200u64;
    let rounds = 800u64;
    let settle_from = rounds - 200; // no events after this; everything is up

    let mut ids: Vec<u32> = (0..n).collect();
    for i in (1..ids.len()).rev() {
        ids.swap(i, rng.below(i + 1));
    }
    let flappers: Vec<u32> = ids[0..3 + rng.below(3)].to_vec(); // 3..5 nodes flap
    let leavers: Vec<u32> = ids[8..8 + rng.below(3)].to_vec(); // 0..2 nodes leave (disjoint)

    let mut events: Vec<(u64, Event)> = Vec::new();
    for f in flappers {
        let down_at = 40 + rng.below((settle_from - 120) as usize) as u64;
        let up_at = (down_at + 20 + rng.below(60) as u64).min(settle_from - 10);
        events.push((down_at, Event::Down(f)));
        events.push((up_at, Event::Up(f)));
    }
    for l in leavers {
        let at = load_until + 40 + rng.below((settle_from - load_until - 60) as usize) as u64;
        events.push((at, Event::Leave(l)));
    }
    for _ in 0..1 + rng.below(4) {
        events.push((
            30 + rng.below((settle_from - 60) as usize) as u64,
            Event::Governor,
        ));
    }
    for _ in 0..rng.below(3) {
        events.push((
            30 + rng.below((load_until + 100) as usize) as u64,
            Event::Join,
        ));
    }

    Params {
        nodes: n,
        load_until,
        rounds,
        events,
        ..base(0.1)
    }
}

fn main() {
    println!("bcounter + plumtree over a surrogate network (16 nodes, quota Y=1_000_000)");
    println!("  load runs to round 200, then quiet rounds; gossip carries full state\n");
    println!(
        "  {:>10}  {:>12}  {:>12}  {:>11}  {:>16}",
        "loss", "enforced?", "converged?", "usage %Y", "scenario"
    );

    type Mk = fn(f64) -> Params;
    let scenarios: &[(&str, Mk)] = &[
        ("steady", base),
        ("governor change", |l| Params {
            events: vec![(120, Event::Governor)],
            ..base(l)
        }),
        ("node leaves", |l| Params {
            events: vec![(250, Event::Leave(7))],
            ..base(l)
        }),
        ("node joins", |l| Params {
            events: vec![(120, Event::Join)],
            ..base(l)
        }),
        ("full lifecycle", lifecycle),
    ];

    for &(name, mk) in scenarios {
        for &loss in &[0.0, 0.1, 0.3] {
            let mut w = World::new(mk(loss));
            w.run_to_end();
            println!(
                "  {:>9.0}%  {:>12}  {:>12}  {:>10.1}%  {:>16}",
                loss * 100.0,
                yesno(w.enforcement_holds()),
                yesno(w.converged()),
                w.true_total as f64 / w.p.limit as f64 * 100.0,
                name,
            );
        }
    }
    println!(
        "\n  random lifecycles (each a different history of down/up, join, leave, leader change):"
    );
    for seed in 0..6u64 {
        let mut w = World::new(random_params(seed));
        w.run_to_end();
        println!(
            "  {:>8}  enforced={:<4}  agreed={:<4}  converged={:<4}",
            format!("seed {seed}"),
            yesno(w.enforcement_holds()),
            yesno(w.agreed()),
            yesno(w.converged()),
        );
    }

    println!(
        "\n  Read: usage stays within the quota (enforced), and every reachable member's view\n  \
         converges to the true total -- through 30% message loss and a full lifecycle of leader\n  \
         changes, a node down and back up, a join, and a leave. Convergence rests on\n  \
         re-broadcasting full state, not on any message arriving."
    );
}

fn yesno(b: bool) -> &'static str {
    if b {
        "yes"
    } else {
        "NO"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn run(p: Params) -> World {
        let mut w = World::new(p);
        w.run_to_end();
        w
    }

    #[test]
    fn converges_without_loss() {
        let w = run(base(0.0));
        assert!(w.converged(), "views did not converge without loss");
        assert!(w.enforcement_holds());
        assert!(w.true_total > 0, "no load was applied");
    }

    #[test]
    fn converges_despite_heavy_loss() {
        // A third of messages dropped; plumtree GRAFT plus full-state re-broadcast still heal it.
        let w = run(base(0.3));
        assert!(w.converged(), "views did not converge under 30% loss");
        assert!(w.enforcement_holds());
    }

    #[test]
    fn enforcement_holds_when_load_exceeds_the_limit() {
        // Offer far more than the quota; the escrow ceiling must still cap the true total.
        let p = Params {
            load_amount: 2000,
            load_until: 400,
            rounds: 500,
            ..base(0.1)
        };
        let w = run(p);
        assert!(w.enforcement_holds(), "usage exceeded the ceiling");
        // Filled to the limit bar per-node remainders too small for one more acquire.
        let slack = w.p.nodes as u64 * w.p.load_amount;
        assert!(
            w.true_total > w.p.limit - slack && w.true_total <= w.p.limit,
            "true_total {} not near the limit {}",
            w.true_total,
            w.p.limit
        );
    }

    #[test]
    fn full_lifecycle_stays_enforced_and_converges() {
        // Leader changes, a node down and back up, a join and a leave -- all in one run, under
        // loss.
        for &loss in &[0.0, 0.1, 0.3] {
            let w = run(lifecycle(loss));
            assert!(
                w.enforcement_holds(),
                "lifecycle exceeded the ceiling at {loss} loss"
            );
            assert!(
                w.converged(),
                "lifecycle did not converge at {loss} loss: true_total={}",
                w.true_total
            );
        }
    }

    #[test]
    fn random_lifecycles_hold_invariants_every_tick() {
        // Many different random histories of down/up, join, leave, and leader change, under
        // loss. The invariants are checked on *every* tick, not just at the end.
        use std::collections::BTreeMap;
        for seed in 0..40u64 {
            let mut w = World::new(random_params(seed));
            let mut last: BTreeMap<u32, u64> = BTreeMap::new();
            while w.now < w.p.rounds {
                w.step();
                // Safety: usage never exceeds the ceiling.
                assert!(
                    w.true_total <= w.p.limit + w.p.delta,
                    "seed {seed} tick {}: overshoot",
                    w.now
                );
                for n in w.nodes.iter().filter(|n| n.active()) {
                    let g = n.usage.global_used();
                    // No phantom: a node never sees more usage than truly happened.
                    assert!(
                        g <= w.true_total,
                        "seed {seed} tick {}: node {} sees {g} > true {}",
                        w.now,
                        n.id,
                        w.true_total
                    );
                    // Monotone: a node's view never regresses (grow-only; no releases here).
                    if let Some(&prev) = last.get(&n.id) {
                        assert!(
                            g >= prev,
                            "seed {seed} tick {}: node {} regressed {prev} -> {g}",
                            w.now,
                            n.id
                        );
                    }
                    last.insert(n.id, g);
                }
            }
            // Liveness: once quiet, the live replicas agree, on the truth.
            assert!(
                w.converged(),
                "seed {seed}: did not converge, true={}",
                w.true_total
            );
        }
    }

    #[test]
    fn news_reaches_every_node_within_log_m_rounds() {
        // No loss, no churn: a fresh broadcast reaches every node in O(log M) rounds -- the
        // eager tree is ~log M deep, with lazy GRAFT to catch stragglers.
        let m = 16u32;
        let mut p = base(0.0);
        p.nodes = m;
        p.events.clear();
        let mut w = World::new(p);
        for _ in 0..300 {
            w.step();
        }
        assert!(w.converged(), "warm-up did not converge");

        // Node 0 makes one fresh change and broadcasts it right away.
        let i0 = w.idx(0).unwrap();
        w.nodes[i0].usage.grant(1000);
        w.nodes[i0].usage.acquire(777).unwrap();
        w.true_total += 777;
        let bytes = encode(&w.nodes[i0].usage.delta());
        w.nodes[i0].tree.broadcast(w.now, bytes);
        w.pump(i0);

        // Count rounds until every node has it.
        let mut rounds = 0u64;
        while rounds < 4 * u64::from(m) {
            w.step();
            rounds += 1;
            if w.converged() {
                break;
            }
        }
        assert!(w.converged(), "news did not reach every node");
        let log_m = u64::from(32 - (m - 1).leading_zeros()); // ceil(log2 M)
        let bound = 6 * log_m;
        assert!(
            rounds <= bound,
            "news took {rounds} rounds for M={m}, over the O(log M) bound {bound}"
        );
    }
}
