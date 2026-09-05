# lease-sim

A simulation of tree-leased quotas: [`bcounter`](..) for the accounting,
[`plumtree-fsm`](https://github.com/kostja/plumtree) for the spanning tree, and the lease
protocol the server will run on top of them. It is not published; it is the test bench for
the design.

```
cargo run -p lease-sim --release   # the three tables
cargo test -p lease-sim --release  # the invariants
```

## What it models

The tree roots at the governor. Leases flow down: capacity (bytes, a stock) and rate (a share
of a refill, a flow). Usage flows up as a per-node map (`BCounter::delta`), merged at every
level, so a moving branch is never counted twice. Every lease is TTL'd and epoch-fenced. The
network loses 5% of messages. The rules the protocol needs are listed at the top of
`src/main.rs`; each one was found by a run that went wrong without it.

## What it measures

- **(a) a governor change**: overshoot, peak over-booking, false denials beyond a no-event
  control, the new governor's window and catch-up time, and messages per node per tick.
- **(b) a mid-tree node down and back**: when its subtree's flow drops, how far, for how long.
- **(c) five joins**: the over-commit from lease adoption.

Each cell runs over five seeds. Bounds (overshoot, over-booking) are the worst seed; costs
are the mean.

## Reading the numbers

With a well-shaped tree, neither policy overshoots on a governor change or a join: the new
governor lends nothing until every old lease is booked again, and a moving lease is booked by
both parents until the new one confirms. Over-booking peaks at 10–33% of the limit during a
re-orientation and is transient.

The policies differ in false denials: `deny` refuses two to three times as many writes as
`allow` after a governor change, and refuses a subtree's writes for tens of ticks after its
parent dies. `allow` covers the unavailability gap but still throttles a node briefly once it
is re-leased at zero.

Message cost is 0.3–1.7 lease messages per node per tick. The TTL sets it: `TTL=10` costs
about three times `TTL=40`. The TTL also bounds every recovery time: a subtree's flow returns
within a few TTLs; a lost message is covered by the next renewal at `TTL/2`.
