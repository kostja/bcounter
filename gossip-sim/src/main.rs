// SPDX-License-Identifier: MIT OR Apache-2.0
// Copyright (c) 2026 Konstantin Osipov.

//! A surrogate-network simulation of `bcounter` and `plumtree` working together for a
//! distributed quota.
//!
//! Every node runs a `plumtree::Plumtree` and a `bcounter::BCounter`. A node acquires quota
//! locally against a lease it drew from the governor (a `bcounter::LocalQuota`). It gossips its
//! usage as a `bcounter` delta over `plumtree`; peers `apply` it. The network drops a
//! configurable fraction of messages, and the run can change the governor and add or remove a
//! node partway through.
//!
//! It checks two things:
//!   * **enforcement** — the true total usage never exceeds the quota's ceiling, and
//!   * **convergence** — after the load stops, every live node's view of the global usage agrees
//!     with the truth, even under message loss, a governor change, and membership churn.
//!
//! Deterministic (seeded), no external dependencies. Run: `cargo run -p gossip-sim`.

use bcounter::{BCounter, LocalQuota, Quota};
use plumtree::{Action, Config, Message, Plumtree};

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

/// Encode a `BCounter<u32>` delta as bytes: 20 per slot (u32 node + u64 acquired + u64 released),
/// big-endian. This is the sim's stand-in for Picodata's msgpack; the point is that the bytes
/// are opaque to `plumtree`.
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

struct Node {
    id: u32,
    tree: Plumtree<u32>,
    usage: BCounter<u32>,
    alive: bool,
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
    /// Move the governor to a new node at this round (`None` to leave it).
    change_governor_at: Option<u64>,
    /// Kill a node at this round (in these runs, during the quiet phase, after its usage has
    /// spread -- a node dying mid-load would leave its un-gossiped tail unaccounted, which real
    /// durable per-node usage would recover but this sim does not model).
    kill_at: Option<(u64, u32)>,
    /// Add a fresh node at this round.
    join_at: Option<u64>,
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
                alive: true,
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
        self.apply_scenario_events();

        // Deliver due messages.
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
                if self.nodes[i].alive {
                    let actions = self.nodes[i].tree.on_message(self.now, m.from, m.msg);
                    self.run(m.dst, actions);
                }
            }
        }

        // Load and gossip.
        for i in 0..self.nodes.len() {
            if !self.nodes[i].alive {
                continue;
            }
            if self.now <= self.p.load_until {
                self.offer_load(i);
            }
            if self.now.is_multiple_of(self.p.gossip_every) {
                let bytes = encode(&self.nodes[i].usage.delta());
                let actions = self.nodes[i].tree.broadcast(self.now, bytes);
                let id = self.nodes[i].id;
                self.run(id, actions);
            }
        }

        // Tick every live node's tree.
        for i in 0..self.nodes.len() {
            if self.nodes[i].alive {
                let actions = self.nodes[i].tree.tick(self.now);
                let id = self.nodes[i].id;
                self.run(id, actions);
            }
        }
    }

    fn apply_scenario_events(&mut self) {
        if self.p.change_governor_at == Some(self.now) {
            // The ledger moves with the governor -- modelling raft inheritance. `LocalQuota`
            // already holds the outstanding grants, so nothing is re-issued.
            self.governor = self.next_live_after(self.governor);
        }
        if let Some((at, victim)) = self.p.kill_at {
            if at == self.now {
                if let Some(i) = self.idx(victim) {
                    self.nodes[i].alive = false;
                }
                for n in &mut self.nodes {
                    if n.alive {
                        n.tree.membership(&[], &[victim]);
                    }
                }
                if self.governor == victim {
                    self.governor = self.next_live_after(victim);
                }
            }
        }
        if self.p.join_at == Some(self.now) {
            let new_id = self.nodes.iter().map(|n| n.id).max().unwrap_or(0) + 1;
            let live: Vec<u32> = self
                .nodes
                .iter()
                .filter(|n| n.alive)
                .map(|n| n.id)
                .collect();
            let tree = fresh_tree(new_id, &live, self.p.fanout, &mut self.rng);
            self.nodes.push(Node {
                id: new_id,
                tree,
                usage: BCounter::new(new_id, 0),
                alive: true,
            });
            for n in &mut self.nodes {
                if n.alive && n.id != new_id {
                    n.tree.membership(&[new_id], &[]);
                }
            }
        }
    }

    fn next_live_after(&self, id: u32) -> u32 {
        self.nodes
            .iter()
            .filter(|n| n.alive && n.id != id)
            .map(|n| n.id)
            .next()
            .unwrap_or(id)
    }

    /// True usage never exceeds the ceiling.
    fn enforcement_holds(&self) -> bool {
        self.true_total <= self.p.limit + self.p.delta
    }

    /// Every live node's view of the global usage equals the truth.
    fn converged(&self) -> bool {
        self.nodes
            .iter()
            .filter(|n| n.alive)
            .all(|n| n.usage.global_used() == self.true_total)
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
    // Shuffle (Fisher-Yates) for a random eager subset.
    for i in (1..peers.len()).rev() {
        peers.swap(i, rng.below(i + 1));
    }
    let cut = fanout.min(peers.len());
    let eager = peers[..cut].to_vec();
    let lazy = peers[cut..].to_vec();
    // The sim's clock ticks once per round, so the GRAFT timeout is in rounds, not the
    // 500ms default -- otherwise lazy repair never fires within a run.
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
        change_governor_at: None,
        kill_at: None,
        join_at: None,
        seed: 0xB0A710,
    }
}

fn main() {
    println!("bcounter + plumtree over a surrogate network (16 nodes, quota Y=1_000_000)");
    println!("  load runs to round 200, then 200 quiet rounds; gossip carries full state\n");
    println!(
        "  {:>10}  {:>12}  {:>12}  {:>11}  {:>10}",
        "loss", "enforced?", "converged?", "usage %Y", "scenario"
    );

    type Mk = fn(f64) -> Params;
    let scenarios: &[(&str, Mk)] = &[
        ("steady", base),
        ("governor change", |l| Params {
            change_governor_at: Some(120),
            ..base(l)
        }),
        ("node leaves", |l| Params {
            kill_at: Some((250, 7)),
            ..base(l)
        }),
        ("node joins", |l| Params {
            join_at: Some(120),
            ..base(l)
        }),
    ];

    for &(name, mk) in scenarios {
        for &loss in &[0.0, 0.1, 0.3] {
            let mut w = World::new(mk(loss));
            w.run_to_end();
            println!(
                "  {:>9.0}%  {:>12}  {:>12}  {:>10.1}%  {:>10}",
                loss * 100.0,
                yesno(w.enforcement_holds()),
                yesno(w.converged()),
                w.true_total as f64 / w.p.limit as f64 * 100.0,
                name,
            );
        }
    }
    println!(
        "\n  Read: usage stays within the quota (enforced), and every live node's view converges\n  \
         to the true total -- through 30% message loss, a governor change, and a node joining or\n  \
         leaving. Convergence rests on re-broadcasting full state, not on any message arriving."
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
    fn survives_a_governor_change() {
        let p = Params {
            change_governor_at: Some(120),
            ..base(0.1)
        };
        let w = run(p);
        assert!(w.converged());
        assert!(w.enforcement_holds());
    }

    #[test]
    fn survives_a_node_leaving() {
        let p = Params {
            kill_at: Some((250, 7)),
            ..base(0.1)
        };
        let w = run(p);
        assert!(w.converged(), "did not converge after a node left");
        assert!(w.enforcement_holds());
    }

    #[test]
    fn survives_a_node_joining() {
        let p = Params {
            join_at: Some(120),
            ..base(0.1)
        };
        let w = run(p);
        assert!(w.converged(), "did not converge after a node joined");
        assert!(w.enforcement_holds());
    }
}
