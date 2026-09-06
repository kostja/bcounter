# bcounter

An escrow bounded counter for distributed capacity quotas, plus a map of them for hierarchical
(path) quotas and the quota trait that feeds them. Pure, `#![forbid(unsafe_code)]`, no
network, no clock. This is the escrow (reservation) design of the [bounded counter][balegas] of
Balegas et al., which descends from O'Neil's escrow transactional method.

Enforce *"no more than `limit` in total"* — bytes stored, objects held, connections open —
across a cluster where every node accepts writes, without a round trip on the write path.

It is the first of three small crates that together do that:

- `bcounter` does the accounting: a node's grant, and a usage map that merges by node id.
- [`plumtree-fsm`](https://github.com/kostja/plumtree) is the overlay: it tells a node which
  peer is upstream, towards the leader.
- [`leasetree`](https://github.com/kostja/leasetree) is the protocol: leases handed down a tree
  rooted at the leader, usage reported up, everything TTL'd and fenced by the leader's term.

> **What this crate is:** the data structures — [`BCounter`], [`BCounterMap`] — and the [`Quota`]
> trait a lender must satisfy. The lender is not here: `leasetree` is one, the tree of leases;
> a minimal [`LocalQuota`] is included for tests and examples.

## How it works

A `Quota` holds the global budget. It lends slices of it, called grants, to nodes. A node
acquires only against the rights it holds. The check is local; it needs no view of other nodes:

```text
  acquire admitted  ⟺  net used on this node + amount ≤ granted to this node
```

The quota keeps one rule: `Σ grants ≤ limit`. Each node's usage is capped by its grant, so
`Σ used ≤ Σ grants ≤ limit`. The limit is never exceeded. There is no overshoot.

The cost is the false denial. A node that has used up its grant must refuse a write, even when
another node holds unused quota, until the quota moves a grant across.

Each `BCounter` also keeps a map of every node's usage it has heard of: its own slot, and the
slots reported to it. The map is a grow-only CRDT, merged by taking the larger value in each
slot (idempotent, commutative, associative; laws proven by `proptest`). Merged up a tree, it
gives the root the cluster-wide usage, exact, with no node counted twice however the tree
moves.

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

### Reporting usage

`bcounter` holds the state; it does not move it. Two plain methods carry a report, with no wire
format of their own:

- `delta() -> Vec<(node, acquired, released)>` — this node's slots, as plain tuples (encode them
  however you like)
- `apply(&[(node, acquired, released)])` — merge a report: per-slot max, idempotent

This is what `leasetree` sends up the tree: a child reports its `delta()`, the parent
`apply`s it, and reports its own merged `delta()` to its parent in turn. The leader's
`global_used()` is the cluster's total, exact up to report lag. Enforcement never reads it: a
node admits a write against its own lease. The total is for metrics, for the audit, and for
the leader's decisions on where quota should go.

Usage is not gossiped node-to-node. The `gossip-sim` crate in this repository does flood
`delta`/`apply` state over `plumtree-fsm`, but as a transport test of that library --
convergence under message loss and churn -- not as the quota design.

## In numbers: when is it accurate enough?

Two different questions hide behind "accuracy". They have different answers.

### The limit itself: exact, always

A node never spends past its lease, and the leader never lends past the limit. So the cluster
never exceeds the limit -- at any cluster size, message rate, or load. Overshoot is zero by
construction, and every simulator in this repository measures 0.00 at every setting. This part
needs no gossip: a node admits a write against its local lease, and asks for more when short.

### False denials: the price of no overshoot

A write is refused only when this node's lease is dry *and* the lender has nothing left -- the
quota is truly exhausted, or rights are stranded in idle nodes' leases. Away from the limit this
does not happen: a dry node simply refills. Near the limit, in the `sim` crate, with each node
holding a fair-share chunk (Y/N) and no rebalancing, about 10% of attempts were refused while
quota sat elsewhere. A finer chunk trims that; an overshoot allowance Δ of 5% cuts it to about
2%, and 25% to under 1%. `leasetree` runs at Δ = 0 and gets its false denials down another
way: idle nodes hand back what they hold beyond two chunks, and demand is forwarded up the
tree at once.

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
larger `Δ` (overshoot allowance) and more frequent lease top-ups.

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

- `f` grows as `1/Y²`. Halve the quota and you need four times the coordination.
- `f` needs the rate `Λ` and the dispersion `E[J²]`, not just the average. Heavy-tailed object
  sizes (common in S3) raise `E[J²]` and need more frequent top-ups than the average rate
  suggests. A Gaussian assumption underestimates the false denials; for the tail, use a ruin
  bound (`P(dry) ≤ e^{−R·b}`).
- Units: `f` is `1/time`, but none of `{N, Y, ε, p}` has time in it. So you must supply a rate
  `Λ`, or a fill horizon `T = Y/Λ`. An admin usually knows one of these ("about 1 TB per day").
  Make it an input.

### Feasibility check and the smallest enforceable quota

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

### Enforcement architecture

`BCounter` and `Quota` are the mechanism. `leasetree` adds the policy, and it is what the
model above led to:

- **Leases down a tree, usage up it.** The tree follows the overlay `plumtree-fsm` maintains
  and roots at the Raft leader. The leader holds the whole limit and lends chunks to its
  children; each child lends out of what it holds. The tree cuts the √N cost to per-level
  fan-out, and puts the high-frequency work on the busiest link, the root. Usage comes back up
  as the merged `delta()` map, so the leader's total is exact.
- **Leases with a TTL, fenced by the term.** A lease is dated from the tick its request was
  sent and is good for `ttl`; a child renews at `ttl / 2`, and a lease not renewed lapses at
  the parent and is dropped by the child at once. A grant carries the leader's Raft term, and a
  lease is spendable only once confirmed in the current term, so a deposed leader's grants die
  with it. Nothing about leases is persisted: only each node's own usage is durable, and the
  leader's bookings are rebuilt from reports after a change.
- **On demand, with a floor.** A node asks for a chunk when it runs short and hands back what
  it holds beyond two chunks when idle; there is no control loop. A parent that cannot fill a
  request forwards it and pushes room down as soon as it arrives.

### Threat model

These quotas are for capacity planning with cooperative users. They are not a security boundary.
The `Δ` slack, and any soft-limit fallback, can be abused by a user who spreads writes across
nodes on purpose. A quota that must be a hard billing or abuse limit uses the tight path instead:
coordinate per write on the counter that is near full. That is a separate case, and worth naming.

## Scope

This crate is the data structures: `BCounter`, `BCounterMap`, the `Quota` trait, and the
reference `LocalQuota`. The lease protocol is `leasetree`, the overlay is `plumtree-fsm`, and
the failure detector, the durable usage table and the transport belong to the server that
embeds them. Three simulators live in this repository, none published:

- `sim` drives a `BCounter` per node against a `LocalQuota` and measures overshoot and false
  denial against the formulas above: overshoot is exactly zero at `Δ = 0` and at most `Δ`
  otherwise, and false denials fall with a finer chunk or a larger `Δ`.
- `gossip-sim` floods `delta`/`apply` state over `plumtree-fsm` under loss and churn, as a
  transport test of that library.
- `lease-sim` runs `leasetree` over `plumtree-fsm` the way a server would, with a surrogate
  network, Raft's view arriving late, a failure detector, a load and two data centres, and
  measures a leader change, a mid-tree outage, joins, and cross-DC traffic. Its driver is the
  reference for embedding the three crates.

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
