# plumtree

A pure [Plumtree][paper] (epidemic broadcast tree) state machine. No IO, no clock. You feed it
events; it returns actions. A caller — a fiber with a connection pool — runs the actions and
supplies the clock. The same split [`bcounter`](https://crates.io/crates/bcounter) uses.

Plumtree spreads a message to every node over a spanning tree, and repairs the tree with lazy
gossip when a message is lost. Tree-cost delivery, gossip-level resilience.

## How it works

Each node keeps two peer sets. **Eager** peers get the full message. **Lazy** peers get only its
id (an `IHAVE`). A node missing a message asks for it with a `GRAFT`, which pulls that peer into
its eager set. A node that gets the same message twice prunes the duplicate link with a `PRUNE`.
The eager links settle into a tree in a round or two; the lazy links stand by to repair it.

## Use

```rust
use plumtree::{Plumtree, Message, Action, Config};

let mut n: Plumtree<u32> = Plumtree::new(1, [2, 3], [4], Config::default());

// Start a broadcast: full push to eager peers now, IHAVE to lazy peers on the next tick.
let actions = n.broadcast(0, b"hello".to_vec());

// The caller runs each action:
for a in actions {
    match a {
        Action::Send(peer, msg) => { /* serialize msg, send to peer */ }
        Action::Deliver(payload) => { /* hand payload to the application */ }
    }
}
```

The event methods are `broadcast`, `on_message`, `tick` (the clock), and `membership`. Each
returns the actions to run.

## Pairing with bcounter for gossip

`plumtree` carries opaque bytes; `bcounter` produces them. A worker fiber joins the two:

1. `bcounter` emits a delta — `counter.delta()`.
2. the fiber encodes it and calls `plumtree.broadcast(now, bytes)`.
3. the fiber sends each resulting `Send` action over its connection pool.
4. on the receiver, the fiber hands the message to `plumtree.on_message`; for a `Deliver` it
   decodes the bytes and calls `counter.apply(...)`.

Neither library knows about the other. Carry CRDT state, not one-off events: re-broadcast the
current state on a timer and merge on receipt, so a lost message is covered by the next
broadcast and a duplicate is harmless.

## License

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE) or
[MIT license](LICENSE-MIT) at your option.

[paper]: https://asc.di.fct.unl.pt/~jleitao/pdf/srds07-leitao.pdf "João Leitão, José Pereira, Luís Rodrigues, Epidemic Broadcast Trees, SRDS 2007"
