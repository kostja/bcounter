# bcounter

An escrow bounded counter for distributed capacity quotas, plus a map of them for hierarchical
(path) quotas and the quota trait that feeds them. Pure, `#![forbid(unsafe_code)]`, no
network, no clock. This is the escrow (reservation) design of the [bounded counter][balegas] of
Balegas et al., which descends from O'Neil's escrow transactional method.

Enforce *"no more than `limit` in total"* — bytes stored, objects held, connections open —
across a cluster where every node accepts writes, without a round trip on the write path.

> **What this crate is:** the data structures — [`BCounter`], [`BCounterMap`] — and the [`Quota`]
> trait the server's quota must satisfy. That quota is not here. It needs durability, a
> clock for lease expiry, and a rebalancing policy, so it belongs to the server that embeds this
> crate. A minimal [`LocalQuota`] is included for tests and examples. The lease, Plumtree, and
> adaptive-gossip parts of [The model](#the-model) marked *(planned)* are where that server is
> headed.

## Data structure

A `Quota` holds the global budget. It lends slices of it, called grants, to nodes. A node
acquires only against the rights it holds. The check is local; it needs no view of other nodes:

```text
  acquire admitted  ⟺  net used on this node + amount ≤ granted to this node
```

The quota keeps one rule: `Σ grants ≤ limit`. Each node's usage is capped by its grant, so
`Σ used ≤ Σ grants ≤ limit`. The limit is never exceeded. There is no overshoot.

The cost is the false denial. A node that has used up its grant must refuse a write, even when
another node holds unused quota, until the quota moves a grant across.

Each `BCounter` also keeps a gossiped view of every node's usage. The view is a grow-only CRDT,
merged by taking the larger value in each slot (idempotent, commutative, associative; laws
proven by `proptest`). It lets any node read the cluster-wide usage, for metrics and for the
quota's rebalancing decisions.

```rust
use bcounter::{BCounter, LocalQuota, Quota};

let mut quota = LocalQuota::new(100);      // a quota with a limit of 100
let mut a: BCounter = BCounter::new(1, 0); // node 1, no rights yet

// `draw` acquires, asking the quota for more when the local grant is short.
assert_eq!(a.draw(&mut quota, 60), Ok(())); // the quota grants 60 to node 1
assert_eq!(a.granted(), 60);
assert_eq!(a.local_available(), 0);

a.release(25);                             // a delete returns rights locally
assert_eq!(a.local_available(), 25);
```

The node id is generic (`BCounter<Id>`, default `u32` — a Raft node id, a member uuid, any
`Ord + Clone`).

### The quota contract

The quota is not in this crate, but its contract is. The server's quota implements [`Quota`]:

```rust
pub trait Quota<Id> {
    fn grant(&mut self, who: &Id, want: u64) -> u64; // lend up to `want`; Σ grants ≤ ceiling
    fn reclaim(&mut self, who: &Id, amount: u64);    // take unused rights back
    fn available(&self) -> u64;                      // rights free to lend
}
```

Keep `Σ grants ≤ limit + Δ`, and the counters it feeds can never exceed `limit + Δ`. `Δ = 0`
gives strict, overshoot-free enforcement. `Δ > 0` allows an overshoot of at most `Δ` in exchange
for fewer false denials. Durability across leader changes, lease expiry, and rebalancing are the
implementor's job, and the implementor is the server.

### Hierarchical quotas

`BCounterMap` holds one `BCounter` per scope. A single write is subject to several limits at once
— bucket, tenant, root — so it charges the whole path all-or-none: every level is checked before
any is charged. Charging is local, against each scope's granted rights. When a scope is short,
the map names it and charges nothing; the server tops it up from that scope's quota and retries.

```rust
use bcounter::BCounterMap;

let mut m: BCounterMap<&str> = BCounterMap::new(1);
m.grant(&"bucket", 100);            // rights the server drew from each scope's quota
m.grant(&"tenant", 500);
m.grant(&"root", 9999);
assert_eq!(m.acquire(&["bucket", "tenant", "root"], 40), Ok(()));
assert_eq!(m.global_used(&"tenant"), 40);
```

### Gossiping with plumtree

`bcounter` holds the state; it does not move it between nodes. To gossip, export a counter's
slots with `delta()` and merge a peer's with `apply()`:

- `delta() -> Vec<(node, acquired, released)>` — the slots to send, as plain tuples (you encode
  them in your own wire format)
- `apply(&[(node, acquired, released)])` — merge a peer's slots, per-slot max, idempotent

Pair it with [`plumtree-fsm`](https://github.com/kostja/plumtree) (an epidemic-broadcast layer) and a
worker fiber:

1. `counter.delta()` → encode → `plumtree.broadcast(now, bytes)`
2. run plumtree's `Send` actions over your connection pool
3. on receipt, hand the message to `plumtree.on_message`; for a `Deliver`, decode and
   `counter.apply(...)`

Neither library knows about the other. The `gossip-sim` crate in this repository runs the two
over a lossy surrogate network: it shows a quota staying enforced and every node's view of the
usage converging through 30% message loss, a governor change, and nodes joining and leaving.

---

## The model

To enforce a global limit while every node admits writes locally, you have to give something up.
The question is what, and how much. This section is the analysis behind the design.

### Two errors, and you have to pick

- **Overshoot:** admitting past the limit `Y`. This is an optimistic counter's error. It spends
  against a stale global view, corrects itself after the next gossip, but can go up to `(N−1)·Y`
  before it does.
- **False denial:** refusing a write while global quota still exists. This is the escrow
  counter's error, and this crate's. A node's grant runs dry while quota sits on another node.

You cannot make both zero without coordinating on every write, which is the round trip we avoid.
Escrow makes overshoot zero and pays in false denials. Two things reduce the false denials: a
larger `Δ` (overshoot allowance) and more frequent gossip.

### Escrow as an inventory problem

A node's grant is like inventory. Consumption is demand. A top-up from the quota is a resupply.
Running out is a stockout, which here is a false denial. This is the standard (s,S) inventory
problem.

Model node *i*'s consumption as a compound process: events at rate `λᵢ`, sizes with second
moment `E[J²]`. Over a top-up interval `τ = 1/f`, consumption has mean `λᵢ·E[J]·τ` and variance
`λᵢ·E[J²]·τ`. To keep this node's false-denial probability under `p`, its grant must cover:

```
gᵢ(τ)  ≥  λᵢ·E[J]·τ   +   z_p · √( λᵢ·E[J²]·τ )        z_p = Φ⁻¹(1 − p)
         └─── drift ───┘   └──── safety stock ────┘
```

subject to `Σ gᵢ ≤ Y`.

### The √N cost of decentralizing

Add up the safety stock over `N` nodes. Decentralized escrow needs total slack
`z_p·Σ√(λᵢ E[J²] τ)`. A central counter that coordinates on every write needs only
`z_p·√(Σ λᵢ E[J²] τ)`. For equal nodes the ratio is √N. Splitting the quota across `N` nodes
multiplies the required safety margin by √N. This is the cost of avoiding the round trip, and it
is why small quotas on large clusters are hard.

### The frequency law

Aggregate consumption over one interval has standard deviation `σ_C = √(Λ·E[J²]·τ)`, with
`Λ = Σλᵢ`. To keep the fluctuation within a fraction `ε` of `Y` at confidence `z`, the minimum
top-up frequency is:

```
        z²      Λ · E[J²]
f  ≥  ─────  ·  ─────────           for ε = 0.1, z = 1.28:  f ≈ 164 · Λ·E[J²] / Y²
       ε²          Y²
```

- `f` grows as `1/Y²`. Halve the quota and you need four times the coordination. This is the
  small-quota problem in one line.
- `f` needs the rate `Λ` and the dispersion `E[J²]`, not just the average. Heavy-tailed object
  sizes (common in S3) raise `E[J²]` and need more frequent gossip than the average rate
  suggests. A Gaussian assumption underestimates the false denials; for the tail, use a ruin
  bound (`P(dry) ≤ e^{−R·b}`).
- Units: `f` is `1/time`, but none of `{N, Y, ε, p}` has time in it. So you must supply a rate
  `Λ`, or a fill horizon `T = Y/Λ`. An admin usually knows one of these ("about 1 TB per day").
  Make it an input.

### Feasibility check and the smallest enforceable quota *(planned)*

Topping up faster than some `f_max` is not practical (about 10 Hz per hot counter is a reasonable
default). Solve the frequency law for `Y` at `f_max`:

```
Y_min  =  (z / ε) · √( Λ · E[J²] / f_max )
```

If a quota is below `Y_min` for its workload, refuse it when it is configured, rather than
enforce it badly. Report the three options: raise `Y`, loosen the target (show the tightest band
reachable at `f_max`), or accept a soft, best-effort limit. `f_max` is per hot counter. The map
is sparse, so only a few counters are near full and busy at once, and their deltas fit in one
gossip message.

### Enforcement architecture *(planned)*

`BCounter` and `Quota` are the mechanism. The server adds the policy:

- **Hierarchical leases over a broadcast tree.** A [Plumtree][plumtree] tree, built from the Raft
  membership and repaired by lazy-push, carries the grants. The root owns the quota; each parent
  sub-lends to its subtree. The tree cuts the √N cost to per-level fan-out, and puts the
  high-frequency work on the busiest link, the root.
- **Fenced, expiring leases.** A lease holder gives up its grant a little before the grantor
  takes it back, so a take-back never double-lends (which would overshoot). A lease TTL longer
  than the tree-repair time reclaims a dead subtree's grant on its own. The ledger of outstanding
  grants is Raft-durable, so a leader change never re-lends budget.
- **A fixed operating point with event-triggered correction.** Pick a conservative point offline
  (for example, a lease of about the fair share, assuming about 50% use by a cooperative user).
  Top up at the cadence the frequency law gives. When a node's lease runs low ahead of schedule,
  it triggers an early top-up. There is no continuous control loop; feedback happens only on that
  exception.

### Threat model

These quotas are for capacity planning with cooperative users. They are not a security boundary.
The `Δ` slack, and any soft-limit fallback, can be abused by a user who spreads writes across
nodes on purpose. A quota that must be a hard billing or abuse limit uses the tight path instead:
coordinate per write on the counter that is near full. That is a separate case, and worth naming.

## Scope

Implemented: `BCounter`, `BCounterMap`, the `Quota` trait, and the reference `LocalQuota`.
Planned (in the server): the durable, lease-based, expiring quota; the Plumtree broadcast tree;
the feasibility check (`Y_min`) and adaptive gossip; a rate/bandwidth variant; and delta-encoded
gossip. The `sim/` crate is a discrete-event simulator that drives the real `BCounter` and
measures overshoot and false denial against the formulas above. It confirms overshoot is exactly
zero at `Δ = 0` and at most `Δ` otherwise, and that false denials fall with a finer lease or a
larger `Δ`.

## References

- **Bounded counter:** Valter Balegas, Diogo Serra, Sérgio Duarte, Carla Ferreira, Rodrigo
  Rodrigues, Nuno Preguiça, Marc Shapiro, Mahsa Najafzadeh. *Extending Eventually Consistent
  Cloud Databases for Enforcing Numeric Invariants.* IEEE SRDS 2015. [arXiv:1503.09052][balegas]
- **CRDTs:** Marc Shapiro, Nuno Preguiça, Carlos Baquero, Marek Zawirski. *Conflict-free
  Replicated Data Types.* SSS 2011.
- **Escrow method:** Patrick E. O'Neil. *The Escrow Transactional Method.* ACM TODS 11(4), 1986.
- **Demarcation protocol:** Daniel Barbará-Millá, Hector Garcia-Molina. *The Demarcation
  Protocol.* VLDB Journal 3(3), 1994.
- **Broadcast tree:** João Leitão, José Pereira, Luís Rodrigues. *Epidemic Broadcast Trees*
  (Plumtree). IEEE SRDS 2007.
- **Inventory / safety stock:** Paul H. Zipkin. *Foundations of Inventory Management.* 2000.
- **Ruin theory (heavy tails):** Søren Asmussen, Hansjörg Albrecher. *Ruin Probabilities.* 2nd
  ed., 2010.

## License

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE) or
[MIT license](LICENSE-MIT) at your option. Unless you explicitly state otherwise, any
contribution intentionally submitted for inclusion in this crate by you, as defined in the
Apache-2.0 license, shall be dual licensed as above, without any additional terms or conditions.

[balegas]: https://arxiv.org/abs/1503.09052 "Valter Balegas et al., Extending Eventually Consistent Cloud Databases for Enforcing Numeric Invariants, 2015 (arXiv:1503.09052)"
[plumtree]: https://asc.di.fct.unl.pt/~jleitao/pdf/srds07-leitao.pdf "João Leitão, José Pereira, Luís Rodrigues, Epidemic Broadcast Trees, SRDS 2007"
