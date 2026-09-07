# lease-sim

The tree-leased rate limit, driven the way a server drives it. Every node runs a
[`leasetree`](https://github.com/kostja/leasetree) `Lease` (the protocol) and a
[`plumtree-fsm`](https://github.com/kostja/plumtree) `Plumtree` (the overlay). This crate is only
the driver: a surrogate network with latency and loss, Raft's cluster view arriving with a delay,
a failure detector with a delay, a load, and the measurements. Nothing here decides anything
about shares; that is what makes it a preview of embedding the rate limits in a server.

```
cargo run -p lease-sim --release   # the four tables
cargo test -p lease-sim --release  # the invariants
```

## What it measures

Every node offers two requests per tick against a cluster-wide rate of one and a half per node,
so the limit binds and shares must move to where the load is. Over cluster sizes 10/50/200,
TTLs 10/40 and five seeds; bounds are the worst seed, costs the mean.

- **(a) a leader change** and **(c) five joins**: how far the cluster admitted over the rate
  across the event window, how much of the rate it used, requests throttled while the rate
  had room beyond a no-event control, how deep the tree got and how fast it rebalanced, and
  lease messages per node per tick.
- **(b) a mid-tree node down and back**: when its subtree's flow drops, how far, how long.
- **(d) two data centres** at latency 1 inside and 10 across, with a leader change: the share
  of lease and overlay traffic that crosses, the number of tree edges that do, and how many
  nodes' lease parent is the peer the overlay delivers through.

## Reading the numbers

A rate is an average, and a token bucket bursts for a tick, so overshoot is measured across
the window. In steady state the cluster admits at or under the rate. Around a leader change
or a join it admits over it, by what unleased nodes admit while they wait for a share: a
round trip or two, the trade a rate limit makes for availability.

Under contention a few nodes hold no share at all: shares are handed out first come, and
nothing rebalances them among busy nodes. That is a fairness question, open.

The lease tree has no shape of its own: at zero loss it is the overlay's tree exactly, and
under loss most nodes are on it, the rest inside the two-delivery lag of a link swap in
progress. Only `gateways` nodes per data centre list a peer in another; that bounds the
cross-DC tree edges by construction.

The driver in `src/main.rs` is the reference for the caller's side: what to feed the two state
machines, from where, and when.
