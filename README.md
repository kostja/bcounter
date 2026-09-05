# bcounter

An **optimistic bounded-counter** CRDT for distributed capacity quotas, plus a map of them for
hierarchical (path) quotas. Pure, `#![forbid(unsafe_code)]`, no network, no clock. A simpler
relative of the escrow-based [bounded counter][balegas] of Balegas et al.: it never falsely
denies, and in exchange it can overshoot the limit by a bounded amount.

Enforce *"no more than `limit` in total"* — bytes stored, objects held, connections open —
across a cluster where every node accepts writes, **without a round trip on the write path**.

> **What this crate implements today (v0.1):** the optimistic counter and the path map below.
> The escrow / lease / adaptive-gossip machinery in [The model](#the-model) is the design it is
> growing into; the sections marked *(planned)* describe the target, not the current code.

## Data structure

Each node owns one **slot** (identified by a node id) and writes only its own slot, so a state
merge is elementwise `max` and loses nothing. A slot keeps two grow-only totals — `consumed`
(a write) and `freed` (a delete) — and the invariant is `Σ consumed − Σ freed ≤ limit`.

Grow-only-plus-max is the whole CRDT: **idempotent** (gossip redelivery is safe),
**commutative** and **associative** (any delivery order converges). The laws are proven by
`proptest` in the test suite.

```rust
use bcounter::{BCounter, Denied};

let mut a: BCounter = BCounter::new(1, 100); // node 1, limit 100
let mut b: BCounter = BCounter::new(2, 100); // node 2

assert_eq!(a.inc(60), Ok(()));   // node 1 spends 60
assert_eq!(b.inc(60), Ok(()));   // node 2, unaware, spends 60 too

a.merge(&b);                     // gossip
assert_eq!(a.read(), 120);       // bounded overshoot, now visible
assert_eq!(a.inc(1), Err(Denied { available: 0 }));
```

The node id is generic (`BCounter<Id>`, default `u32` — a Raft node id, a member uuid, any
`Ord + Clone`). Your layer owns gossip and persistence; `inc` / `dec` / `merge` are pure state
transitions.

### Hierarchical quotas

[`BCounterMap`] is a map of named counters (after Akka's `PNCounterMap`). A single write is
charged against every scope on a path — object → bucket → tenant → root — **all-or-none**:
every level is checked before any is charged, and limits ride on each call rather than living
in the merged state.

```rust
use bcounter::BCounterMap;

let mut m: BCounterMap<&str> = BCounterMap::new(1);
// Charge 40 against the bucket (limit 100), tenant (500) and root (9999) at once.
m.inc(&[("bucket", 100), ("tenant", 500), ("root", 9999)], 40).unwrap();
assert_eq!(m.read(&"tenant"), 40);
```

---

## The model

Enforcing a global limit while every node admits writes locally means giving up *something* —
the only question is what, and how much. This section is the analysis behind the design: the
two errors, the queueing model that quantifies them, the frequency law that follows, and the
enforcement architecture that model implies.

### The two errors are dual

- **Overshoot** — admitting past the limit `Y`. The *optimistic* counter's error.
- **False denial** — refusing a write while global quota still exists. The *escrow* model's error.

You cannot drive both to zero without coordinating on every write (the round trip we refuse to
pay). So there is one knob, not two independent dials, and **gossip frequency sets where on the
knob you sit.** The workload decides how good a deal you get.

### Escrow as an inventory problem

Treat a node's local budget `bᵢ` as **inventory**, consumption as **demand**, and a gossip sync
as **replenishment**. Running dry = stockout = false denial. This is the classic (s,S)
replenishment problem and its ruin-theory cousin.

Model node *i*'s consumption as a compound process: events at rate `λᵢ`, each of size drawn from
a distribution with second moment `E[J²]`. Over a sync interval `τ = 1/f`, the amount consumed
has mean `λᵢ·E[J]·τ` and variance `λᵢ·E[J²]·τ`. To hold this node's false-denial probability
under `p`, it must carry

```
bᵢ(τ)  ≥  λᵢ·E[J]·τ   +   z_p · √( λᵢ·E[J²]·τ )        z_p = Φ⁻¹(1 − p)
         └─── drift ───┘   └──── safety stock ────┘
```

Escrow's hard constraint is `Σ bᵢ ≤ Y` — you cannot hand out more budget than the limit.

### The √N risk-pooling penalty

Summing the safety stock over `N` nodes, a decentralized quota needs total slack
`z_p·Σ√(λᵢ E[J²] τ)`. A **central** counter coordinating per write would need only
`z_p·√(Σ λᵢ E[J²] τ)`. For homogeneous nodes the ratio is **√N**: distributing the quota
multiplies the required safety margin by √N. That is the price of no round trip, and it is a
theorem, not an implementation detail. It is why small quotas on large clusters are hard.

### The frequency law

Aggregate consumption over one sync interval has standard deviation `σ_C = √(Λ·E[J²]·τ)`, where
`Λ = Σλᵢ` is the cluster event rate. Requiring the fluctuation to stay within a fraction `ε` of
`Y` at confidence `z` (e.g. `ε = 0.1`, one-sided 90% → `z ≈ 1.28`) gives the minimum gossip
frequency:

```
        z²      Λ · E[J²]                                    z²   Λ · E[J²]
f  ≥  ─────  ·  ─────────           equivalently   f  ≈  ──────── · ─────────
       ε²          Y²                                      ε²          Y²
```

For `ε = 0.1`, `z = 1.28`: **`f ≈ 164 · Λ·E[J²] / Y²`** gossips per second.

Two things fall out of the shape:

- `f ∝ 1/Y²` — halve the quota, quadruple the gossip. This is the sharp form of the small-quota
  wall.
- `f` needs both the **rate** `Λ` and the **dispersion** `E[J²]`, not the mean alone.
  Heavy-tailed object sizes (S3's reality) inflate `E[J²]` and demand more frequent gossip than
  the average rate suggests. Assuming Gaussian jumps *understates* false denials; for a sharper
  tail use a Lundberg / ruin bound (`P(dry) ≤ e^{−R·b}`) instead of the normal quantile.

**Dimensional note.** `f` has units of `1/time`. None of `{N, Y, ε, p}` carries time, so a
frequency *cannot* be computed from those alone — you must supply a rate `Λ` (or a fill horizon
`T = Y/Λ`). Admins usually know it ("this bucket grows ~1 TB/day"); surface it as an input.

### Feasibility gate and the minimum enforceable quota *(planned)*

Sync faster than some `f_max` (network/CPU budget; ~10 Hz per hot counter is a sane default) is
infeasible. Inverting the frequency law at `f_max` yields a **smallest enforceable quota**:

```
Y_min  =  (z / ε) · √( Λ · E[J²] / f_max )
```

A quota below `Y_min` for its workload cannot be held to `(ε, p)` at any affordable cadence, so
it is **refused at configuration time** rather than silently degraded — diagnostically, offering
the three levers: raise `Y` to ≥ `Y_min`, loosen the target (report the tightest band achievable
at `f_max`), or accept it as a soft/best-effort limit. `f_max` is per *hot* counter; because the
map is sparse, only a handful of counters are ever near-full-and-active, and their deltas batch
into one physical gossip message, so the *message* rate stays bounded.

### Enforcement architecture *(planned)*

The current counter is the `Δ → ∞` corner of the knob (never false-denies, overshoot up to
`(N−1)Y`). The full design adds the escrow end and an operating point between them:

- **Hierarchical leases over a broadcast tree.** A [Plumtree][plumtree] spanning tree, seeded
  from Raft membership (rooted at the leader, repaired by lazy-push), carries lease grants. The
  root owns the pool; each parent sub-leases to its subtree. Hierarchy cuts the √N penalty to
  per-level fan-out, and localizes the high-frequency need to the busiest link (the root).
- **Fenced, TTL'd leases.** A lease holder self-expires *before* the grantor reclaims (a clock-
  skew margin), so expiry never double-allocates (→ overshoot). TTL ≥ tree-repair time reclaims
  a dead subtree's budget automatically; the outstanding-lease ledger is Raft-durable so a
  leader change never re-hands-out budget.
- **Static operating point + event-triggered correction.** Rather than an online controller,
  pick a conservative point offline (e.g. over-provision leases to ~2× the fair share `Y/N` and
  assume ~50% utilization for a well-intentioned user), gossip at the fixed cadence the
  frequency law prescribes, and let a node whose lease depletes *ahead of schedule* trigger an
  early sync. Feedback lives only on the exception path — bang-bang control, not a continuous
  loop.

### Threat model

These quotas are **capacity planning for cooperative users**, not a security boundary. The
overshoot/soft-limit slack is exploitable by a user who deliberately fans writes across all `N`
nodes. A quota that must be a hard billing/abuse limit routes to the tight path (per-write
coordination on the near-full counter) — a separate class, and one worth naming explicitly.

## Scope

**Implemented (v0.1):** the optimistic `BCounter` and `BCounterMap` above. **Planned:** escrow /
lease allocation, the Plumtree broadcast tree, the adaptive-gossip / feasibility layer, a
rate/bandwidth variant (a tick-refilled token bucket), and delta-encoded gossip. The `sim/`
crate in this repository is a discrete-event simulator that measures actual overshoot and false
denial against the formulas above, so the constants ship measured rather than asserted.

## References

- **Bounded counter:** Valter Balegas, Diogo Serra, Sérgio Duarte, Carla Ferreira, Rodrigo
  Rodrigues, Nuno Preguiça, Marc Shapiro, Mahsa Najafzadeh. *Extending Eventually Consistent
  Cloud Databases for Enforcing Numeric Invariants.* IEEE SRDS 2015. [arXiv:1503.09052][balegas]
- **CRDTs:** Marc Shapiro, Nuno Preguiça, Carlos Baquero, Marek Zawirski. *Conflict-free
  Replicated Data Types.* SSS 2011.
- **Escrow method:** Patrick E. O'Neil. *The Escrow Transactional Method.* ACM TODS 11(4), 1986.
- **Demarcation protocol:** Daniel Barbará-Millá, Hector Garcia-Molina. *The Demarcation
  Protocol: A Technique for Maintaining Constraints in Distributed Database Systems.* VLDB
  Journal 3(3), 1994.
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
