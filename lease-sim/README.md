# lease-sim

The tree-leased quota, driven the way a server drives it. Every node runs a
[`leasetree`](https://github.com/kostja/leasetree) `Lease` (the protocol) and a
[`plumtree-fsm`](https://github.com/kostja/plumtree) `Plumtree` (the overlay). This crate is only
the driver: a surrogate network with latency and loss, Raft's cluster view arriving with a delay,
a failure detector with a delay, a load, and the measurements. Nothing here decides anything
about leases; that is what makes it a preview of embedding the quotas in a server.

```
cargo run -p lease-sim --release   # the four tables
cargo test -p lease-sim --release  # the invariants
```

## What it measures

Over cluster sizes 10/50/200, TTLs 10/40, both policies and five seeds. Bounds (overshoot,
over-booking) are the worst seed; costs are the mean.

- **(a) a leader change**: overshoot, peak over-booking, false denials beyond a no-event
  control, how long the new leader's total takes to catch up, how deep the tree got and how
  fast it rebalanced, and lease messages per node per tick.
- **(b) a mid-tree node down and back**: when its subtree's rate flow drops, how far, how long.
- **(c) five joins**: the over-commit from lease adoption.
- **(d) two data centres** at latency 1 inside and 10 across, with a leader change: the share
  of lease and overlay traffic that crosses, and the number of tree edges that do.

## Reading the numbers

Neither policy overshoots in any scenario. Over-booking peaks during a re-orientation, because
a moving lease is booked by both parents until the new one confirms; it is transient and not a
spending risk. The policies differ in false denials: `deny` refuses two to three times as many
writes as `allow`.

The tree's depth is `log2 N`-ish with an eager fanout of `log2 N + 1`, and returns there within
a few of the leader's messages after a change. Only `gateways` nodes per data centre list a
peer in another; that bounds the cross-DC tree edges by construction (2 per DC here), and the
cost rules choose well among what remains: 3% of lease traffic and 0.2% of overlay traffic
cross at N=200.

The driver in `src/main.rs` is the reference for the caller's side: what to feed the two state
machines, from where, and when.
