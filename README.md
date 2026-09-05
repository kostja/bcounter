# bcounter

An **escrow bounded counter** for distributed capacity quotas, plus a map of them for
hierarchical (path) quotas and the allocator trait that feeds them. Pure,
`#![forbid(unsafe_code)]`, no network, no clock. This is the escrow (reservation) design of the
[bounded counter][balegas] of Balegas et al., descended from O'Neil's escrow transactional
method.

Enforce *"no more than `limit` in total"* — bytes stored, objects held, connections open —
across a cluster where every node accepts writes, **without a round trip on the write path**.

> **What this crate is:** the pure data structures — [`Escrow`], [`EscrowMap`] — and the
> [`Pool`] **trait** the allocator must satisfy. The allocator/pool *implementation* lives in
> the shell embedding this (it needs durability, a clock for lease TTLs, and a rebalancing
> policy); a minimal in-process [`LocalPool`] is provided for tests and examples. The lease,
> Plumtree, and adaptive-gossip machinery in [The model](#the-model) marked *(planned)* describe
> where that shell is headed.

## Data structure

A **pool** owns the global budget and hands out **grants** — slices of it — to nodes. A node
spends only against the rights it holds, purely locally, consulting no one:

```text
  spend admitted  ⟺  net spent on this node + amount ≤ granted to this node
```

The safety property follows from the pool's one invariant, `Σ grants ≤ limit`: since each node's
spending is capped by its grant, `Σ spent ≤ Σ grants ≤ limit`. **The limit is never exceeded —
no overshoot, ever** — as long as the pool honors its ceiling. The trade for that guarantee is
the *false denial*: a node whose grant is spent must refuse a write even while unspent quota
sits on another node, until the pool moves a grant across.

Each `Escrow` also carries a gossiped view of every node's spending — a grow-only CRDT merged by
elementwise `max` (idempotent, commutative, associative; laws proven by `proptest`) — so global
usage can be observed for metrics and for the pool's rebalancing decisions.

```rust
use bcounter::{Escrow, LocalPool, Pool};

let mut pool = LocalPool::new(100);    // a pool owning a limit of 100
let mut a: Escrow = Escrow::new(1, 0); // node 1, no rights yet

// `draw` spends, topping up from the pool when the local grant is short.
assert_eq!(a.draw(&mut pool, 60), Ok(())); // pool grants 60 to node 1
assert_eq!(a.granted(), 60);
assert_eq!(a.local_available(), 0);

a.refund(25);                          // a delete returns rights locally
assert_eq!(a.local_available(), 25);
```

The node id is generic (`Escrow<Id>`, default `u32` — a Raft node id, a member uuid, any
`Ord + Clone`).

### The allocator contract

The pool is out of this crate, but its contract is not. Any allocator implements [`Pool`]:

```rust
pub trait Pool<Id> {
    fn grant(&mut self, who: &Id, want: u64) -> u64; // lend up to `want`; Σ grants ≤ ceiling
    fn release(&mut self, who: &Id, amount: u64);    // take unused rights back
    fn available(&self) -> u64;                      // rights free to lend
}
```

Honor `Σ outstanding grants ≤ limit + Δ` and the escrow counters it feeds can never exceed
`limit + Δ` (`Δ = 0` for strict, overshoot-free enforcement; `Δ > 0` trades a bounded overshoot
for fewer false denials). Durability across leader changes, lease TTLs, and rebalancing policy
are the implementor's — expected to be the shell.

### Hierarchical quotas

[`EscrowMap`] holds one `Escrow` per scope. A single write is subject to several limits at once
— bucket, tenant, root — so it spends across the whole path **all-or-none**: every level is
checked before any is charged. Spending is purely local against each scope's granted rights; a
short scope is named back to the caller, which tops it up from that scope's pool and retries.

```rust
use bcounter::EscrowMap;

let mut m: EscrowMap<&str> = EscrowMap::new(1);
m.grant(&"bucket", 100);            // rights the shell drew from each scope's pool
m.grant(&"tenant", 500);
m.grant(&"root", 9999);
assert_eq!(m.spend(&["bucket", "tenant", "root"], 40), Ok(()));
assert_eq!(m.global_used(&"tenant"), 40);
```

---

## The model

Enforcing a global limit while every node admits writes locally means giving up *something* —
the only question is what, and how much. This section is the analysis behind the design.

### The two errors are dual

- **Overshoot** — admitting past the limit `Y`. An *optimistic* counter's error (spend against a
  stale global view; self-correcting but unbounded up to `(N−1)·Y`).
- **False denial** — refusing a write while global quota still exists. The *escrow* counter's
  error, and this crate's: a node's grant runs dry while quota sits elsewhere.

You cannot drive both to zero without coordinating on every write (the round trip we refuse to
pay). Escrow fixes overshoot at zero and pays in false denials; the `Δ` allowance and gossip
frequency slide along the single knob between the two.

### Escrow as an inventory problem

Treat a node's grant as **inventory**, consumption as **demand**, a pool top-up as
**replenishment**. Running dry = stockout = false denial. This is the (s,S) replenishment
problem and its ruin-theory cousin.

Model node *i*'s consumption as a compound process: events at rate `λᵢ`, sizes with second
moment `E[J²]`. Over a replenishment interval `τ = 1/f`, consumption has mean `λᵢ·E[J]·τ` and
variance `λᵢ·E[J²]·τ`. To hold this node's false-denial probability under `p`, its grant must
cover

```
gᵢ(τ)  ≥  λᵢ·E[J]·τ   +   z_p · √( λᵢ·E[J²]·τ )        z_p = Φ⁻¹(1 − p)
         └─── drift ───┘   └──── safety stock ────┘
```

subject to `Σ gᵢ ≤ Y`.

### The √N risk-pooling penalty

Summing safety stock over `N` nodes, decentralized escrow needs total slack
`z_p·Σ√(λᵢ E[J²] τ)`; a central counter coordinating per write needs only
`z_p·√(Σ λᵢ E[J²] τ)`. For homogeneous nodes the ratio is **√N** — distributing the quota
multiplies the required margin by √N. That is the price of no round trip, and it is why small
quotas on large clusters are hard.

### The frequency law

Aggregate consumption over one interval has std `σ_C = √(Λ·E[J²]·τ)`, `Λ = Σλᵢ`. Requiring the
fluctuation within a fraction `ε` of `Y` at confidence `z` gives the minimum replenishment
frequency:

```
        z²      Λ · E[J²]
f  ≥  ─────  ·  ─────────           for ε = 0.1, z = 1.28:  f ≈ 164 · Λ·E[J²] / Y²
       ε²          Y²
```

- `f ∝ 1/Y²` — halve the quota, quadruple the coordination. The sharp form of the small-quota
  wall.
- `f` needs the **rate** `Λ` *and* the **dispersion** `E[J²]`, not the mean alone. Heavy-tailed
  object sizes (S3's reality) inflate `E[J²]`; Gaussian assumptions understate false denials, so
  use a Lundberg / ruin bound (`P(dry) ≤ e^{−R·b}`) for the tail.
- **Dimensions:** `f` is `1/time`, and none of `{N, Y, ε, p}` carries time — so you must supply a
  rate `Λ` (or a fill horizon `T = Y/Λ`). Admins usually know it ("~1 TB/day"); make it an input.

### Feasibility gate and the minimum enforceable quota *(planned)*

Replenishing faster than some `f_max` (~10 Hz per hot counter is a sane default) is infeasible.
Inverting the law at `f_max` gives a smallest enforceable quota:

```
Y_min  =  (z / ε) · √( Λ · E[J²] / f_max )
```

A quota below `Y_min` for its workload is **refused at configuration time** — diagnostically,
offering three levers: raise `Y`, loosen the target (report the tightest band achievable at
`f_max`), or accept it as a soft/best-effort limit. `f_max` is per *hot* counter; the sparse map
keeps only a handful near-full-and-active, and their deltas batch into one gossip message.

### Enforcement architecture *(planned)*

`Escrow` + `Pool` are the mechanism; the shell adds the policy:

- **Hierarchical leases over a broadcast tree.** A [Plumtree][plumtree] tree, seeded from Raft
  membership and repaired by lazy-push, carries grants: the root owns the pool, each parent
  sub-leases to its subtree. Hierarchy cuts the √N penalty to per-level fan-out and localizes
  the high-frequency need to the busiest link (the root).
- **Fenced, TTL'd leases.** A holder self-expires *before* the grantor reclaims (a clock-skew
  margin), so expiry never double-allocates. TTL ≥ tree-repair time reclaims a dead subtree's
  grant; the outstanding-grant ledger is Raft-durable so a leader change never re-lends budget.
- **Static operating point + event-triggered correction.** Pick a conservative point offline
  (e.g. lease ≈ the fair share, assume ~50% utilization for a cooperative user), replenish at
  the cadence the frequency law prescribes, and let a node whose lease depletes ahead of schedule
  trigger an early top-up. Feedback only on the exception path — not a continuous loop.

### Threat model

These quotas are **capacity planning for cooperative users**, not a security boundary. The `Δ`
slack (and any soft-limit fallback) is exploitable by a user who deliberately fans writes across
nodes. A quota that must be a hard billing/abuse limit routes to the tight path (per-write
coordination on the near-full counter) — a separate class, named explicitly.

## Scope

**Implemented:** the escrow `Escrow` and `EscrowMap`, the `Pool` trait, and a reference
`LocalPool`. **Planned (in the shell):** the durable, lease-based, TTL-fenced pool; the Plumtree
broadcast tree; the adaptive-gossip / feasibility (`Y_min`) layer; a rate/bandwidth variant; and
delta-encoded gossip. The `sim/` crate is a discrete-event simulator that drives the real
`Escrow` and measures overshoot and false-denial against the formulas above — so the constants
ship measured, not asserted. It confirms overshoot is exactly zero at `Δ = 0` and never exceeds
`Δ`, and that false denials fall with a finer lease or a larger `Δ`.

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
