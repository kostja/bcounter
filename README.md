# bcounter

A **bounded counter** ([BCounter][paper]) CRDT for distributed capacity quotas, plus a map of
them for hierarchical (path) quotas. Pure, `#![forbid(unsafe_code)]`, no network, no clock.

Enforce *"no more than `limit` in total"* — bytes stored, objects held, connections open —
across a cluster where every node accepts writes, **without a round trip on the write path**.

## The model

Each node owns one **slot** (identified by a node id) and writes only its own slot, so a state
merge is elementwise `max` and loses nothing. A slot keeps two grow-only totals — `consumed`
(a write) and `freed` (a delete) — and the invariant is `Σ consumed − Σ freed ≤ limit`.

Grow-only-plus-max is the whole CRDT: **idempotent** (gossip redelivery is safe),
**commutative** and **associative** (any delivery order converges). The laws are proven by
`proptest` in the test suite.

### Bounded overshoot, deliberately

A node admits a write when *its own* (possibly stale) view leaves room, so the true total can
briefly exceed the limit — by **at most other nodes' un-gossiped consumption**, never
unboundedly. The exact alternative (a strict per-node budget with transfers) never overshoots
but *falsely denies* a write whose quota is stranded on another node — a worse answer for a
capacity quota. This crate chooses the honest, bounded overshoot.

## Usage

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

## Scope

This is a **capacity** counter. A rate/bandwidth variant (a tick-refilled token bucket),
delta-encoded gossip, and quota transfers are not included: per-node lease refill rounds toward
nothing for small quotas on large clusters, and deltas/GC need causal-stability tracking. Those
belong to a later release with their own model.

## License

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE) or
[MIT license](LICENSE-MIT) at your option. Unless you explicitly state otherwise, any
contribution intentionally submitted for inclusion in this crate by you, as defined in the
Apache-2.0 license, shall be dual licensed as above, without any additional terms or conditions.

[paper]: https://arxiv.org/abs/1503.09052
