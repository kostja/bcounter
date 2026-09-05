// SPDX-License-Identifier: MIT OR Apache-2.0
// Copyright (c) 2026 Konstantin Osipov.

//! Discrete-event simulator for the `bcounter` escrow model.
//!
//! It drives the real [`bcounter::BCounter`] across `N` nodes drawing from one central
//! [`bcounter::LocalQuota`] with ceiling `Y + Δ`. Each node consumes quota as a compound-Poisson
//! stream (events at rate `lambda`, sizes heavy-tailed lognormal), drawing a lease `chunk` of
//! rights when its local grant runs short. It measures:
//!
//!   * **overshoot** — bounded by `Δ` and exactly zero at `Δ = 0` (the escrow safety guarantee,
//!     the opposite of the optimistic counter's), and
//!   * **false denials** — writes refused while the *true* total was still under `Y`, caused by
//!     rights stranded in idle nodes' leases. Their rate falls two ways: a finer lease `chunk`
//!     (less stranded per lease) or an overshoot allowance `Δ`.
//!
//! The two headline sweeps vary the chunk (at `Δ = 0`) and `Δ` (at a fixed chunk). A periodic
//! rebalance -- nodes returning unused rights to the quota -- is also wired (`rebalance_hz`), but
//! returning *all* unused rights simply churns the central quota without cutting false denials,
//! so it is left out of the headline; a smarter idle→busy transfer is future work.
//!
//! Deterministic (seeded), no external dependencies, no wall clock. Run: `cargo run -p
//! bcounter-sim`.

use bcounter::{BCounter, LocalQuota, Quota};

/// SplitMix64 — a tiny, deterministic PRNG. Enough for a simulator; not for cryptography.
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

    /// Uniform in `[0, 1)`.
    fn unit(&mut self) -> f64 {
        (self.next_u64() >> 11) as f64 / (1u64 << 53) as f64
    }

    /// Exponential inter-arrival time for a Poisson process of the given rate.
    fn exp(&mut self, rate: f64) -> f64 {
        -(1.0 - self.unit()).ln() / rate
    }

    /// A standard normal deviate (Box–Muller).
    fn normal(&mut self) -> f64 {
        let u1 = 1.0 - self.unit();
        let u2 = self.unit();
        (-2.0 * u1.ln()).sqrt() * (std::f64::consts::TAU * u2).cos()
    }

    /// A lognormal draw with arithmetic `mean` and coefficient of variation `cv` (std/mean).
    fn lognormal(&mut self, mean: f64, cv: f64) -> f64 {
        let s2 = (1.0 + cv * cv).ln();
        let mu = mean.ln() - s2 / 2.0;
        (mu + s2.sqrt() * self.normal()).exp()
    }
}

/// One simulation's inputs.
#[derive(Clone, Copy)]
struct Params {
    /// Number of cluster nodes.
    nodes: u32,
    /// The global limit `Y`.
    limit: u64,
    /// Overshoot allowance: the quota's ceiling is `limit + delta`.
    delta: u64,
    /// Rights a node draws from the quota in one top-up (its lease chunk).
    chunk: u64,
    /// Per-node write rate (writes/sec).
    lambda: f64,
    /// Mean write size.
    size_mean: f64,
    /// Coefficient of variation of write size.
    size_cv: f64,
    /// Rebalance frequency (Hz): how often each node returns unused rights to the quota. `0` =
    /// never.
    rebalance_hz: f64,
    /// How long to run, in seconds.
    duration: f64,
    /// PRNG seed.
    seed: u64,
}

/// One simulation's measurements. `true_total` is asserted only by tests, not read by the
/// binary; that is intentional.
#[allow(dead_code)]
struct Outcome {
    /// True cluster total (ground truth: everything admitted). Never exceeds `limit + delta`.
    true_total: u64,
    /// `true_total − limit`, saturating. Bounded by `delta`; zero when `delta == 0`.
    overshoot: u64,
    admitted: u64,
    denied: u64,
    /// Denials of writes that would have fit under the true limit — the stranding cost.
    false_denials: u64,
}

/// Run one simulation, driving the real `BCounter` on every node against a shared `LocalQuota`.
fn run(p: &Params) -> Outcome {
    let mut rng = Rng::new(p.seed);
    let mut quota = LocalQuota::new(p.limit + p.delta);
    let mut nodes: Vec<BCounter> = (0..p.nodes).map(|id| BCounter::new(id, 0)).collect();

    let mut next_write: Vec<f64> = (0..p.nodes as usize).map(|_| rng.exp(p.lambda)).collect();
    let rebalance_tau = if p.rebalance_hz > 0.0 {
        1.0 / p.rebalance_hz
    } else {
        f64::INFINITY
    };
    let mut next_rebalance = rebalance_tau;

    let mut true_total: u64 = 0;
    let mut admitted = 0u64;
    let mut denied = 0u64;
    let mut false_denials = 0u64;

    loop {
        let (soonest_node, soonest_write) = next_write
            .iter()
            .enumerate()
            .min_by(|a, b| a.1.partial_cmp(b.1).unwrap())
            .map(|(i, &t)| (i, t))
            .unwrap();

        let now = soonest_write.min(next_rebalance);
        if now > p.duration {
            break;
        }

        if next_rebalance <= soonest_write {
            // Rebalance: every node returns all its unused rights to the quota, so quota
            // stranded in idle leases can be re-lent to busy nodes.
            for (id, node) in nodes.iter_mut().enumerate() {
                let unused = node.local_available();
                let returned = node.reclaim(unused);
                quota.reclaim(&(id as u32), returned);
            }
            next_rebalance += rebalance_tau;
        } else {
            let amount = rng.lognormal(p.size_mean, p.size_cv).round() as u64;
            let amount = amount.max(1);
            let node = &mut nodes[soonest_node];
            // Draw a lease chunk when short (never fewer than the shortfall).
            if node.local_available() < amount {
                let want = p.chunk.max(amount - node.local_available());
                let got = quota.grant(&(soonest_node as u32), want);
                node.grant(got);
            }
            match node.acquire(amount) {
                Ok(()) => {
                    admitted += 1;
                    true_total += amount;
                }
                Err(_) => {
                    denied += 1;
                    if true_total + amount <= p.limit {
                        false_denials += 1;
                    }
                }
            }
            next_write[soonest_node] = now + rng.exp(p.lambda);
        }
    }

    Outcome {
        true_total,
        overshoot: true_total.saturating_sub(p.limit),
        admitted,
        denied,
        false_denials,
    }
}

/// Summary of `reps` replicates at one operating point.
struct Summary {
    /// Mean overshoot as a percentage of the limit.
    overshoot_pct: f64,
    /// Mean false-denial rate as a percentage of all write attempts.
    false_denial_pct: f64,
    /// Largest overshoot seen, as a percentage of the limit (should never exceed `Δ`).
    max_overshoot_pct: f64,
}

/// Average `reps` replicates of `base`, varying only the seed.
fn measure(base: &Params, reps: u64) -> Summary {
    let mut overshoot_sum = 0.0;
    let mut max_overshoot = 0.0f64;
    let mut fd_sum = 0.0;
    for r in 0..reps {
        let p = Params {
            seed: base.seed.wrapping_add(r.wrapping_mul(0x1000_0001)),
            ..*base
        };
        let o = run(&p);
        let os = o.overshoot as f64 / base.limit as f64 * 100.0;
        overshoot_sum += os;
        max_overshoot = max_overshoot.max(os);
        let attempts = (o.admitted + o.denied).max(1);
        fd_sum += o.false_denials as f64 / attempts as f64 * 100.0;
    }
    Summary {
        overshoot_pct: overshoot_sum / reps as f64,
        false_denial_pct: fd_sum / reps as f64,
        max_overshoot_pct: max_overshoot,
    }
}

fn main() {
    let base = Params {
        nodes: 16,
        limit: 1_000_000,
        delta: 0,
        chunk: 1_000_000 / 16, // fair share Y/N
        lambda: 62.5,          // cluster rate ≈ 100_000 units/sec
        size_mean: 100.0,
        size_cv: 1.5,
        rebalance_hz: 0.0,
        duration: 20.0, // ~2 fill times: quota is reached and contended
        seed: 0xC0FFEE,
    };
    let reps = 200u64;
    let fair = base.limit / u64::from(base.nodes); // Y/N

    println!("bcounter escrow simulation (real BCounter + central LocalQuota)");
    println!(
        "  N={} nodes, Y={} units, fair share Y/N={}, cluster rate≈{:.0}/s, size cv={}, {} reps\n",
        base.nodes,
        base.limit,
        fair,
        f64::from(base.nodes) * base.lambda * base.size_mean,
        base.size_cv,
        reps
    );

    println!(
        "  (1) strict escrow (Δ=0): overshoot is always exactly zero. A smaller lease chunk\n  \
         strands less quota, so false denials fall."
    );
    println!(
        "  {:>14}  {:>12}  {:>12}  {:>14}",
        "chunk", "overshoot %Y", "max o/s %Y", "false-denial %"
    );
    for &div in &[1u64, 2, 4, 8, 16, 32] {
        let p = Params {
            chunk: (fair / div).max(1),
            delta: 0,
            ..base
        };
        let s = measure(&p, reps);
        println!(
            "  {:>14}  {:>12.2}  {:>12.2}  {:>14.2}",
            format!("Y/N÷{div}"),
            s.overshoot_pct,
            s.max_overshoot_pct,
            s.false_denial_pct
        );
    }

    println!("\n  (2) an overshoot allowance Δ buys down false denials at a fixed chunk (Y/N)");
    println!(
        "  {:>14}  {:>12}  {:>12}  {:>14}",
        "Δ (%Y)", "overshoot %Y", "max o/s %Y", "false-denial %"
    );
    for &d_pct in &[0u64, 5, 10, 25, 50] {
        let p = Params {
            chunk: fair,
            delta: base.limit * d_pct / 100,
            ..base
        };
        let s = measure(&p, reps);
        println!(
            "  {:>14}  {:>12.2}  {:>12.2}  {:>14.2}",
            d_pct, s.overshoot_pct, s.max_overshoot_pct, s.false_denial_pct
        );
    }

    println!(
        "\n  Read: at Δ=0 the overshoot is exactly 0 at every chunk size. This is escrow's\n  \
         guarantee. False denials fall in two ways: a finer lease chunk, or a larger Δ. With Δ,\n  \
         the overshoot is at most Δ."
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base() -> Params {
        Params {
            nodes: 8,
            limit: 100_000,
            delta: 0,
            chunk: 100_000 / 8,
            lambda: 50.0,
            size_mean: 100.0,
            size_cv: 1.0,
            rebalance_hz: 0.0,
            duration: 20.0,
            seed: 1,
        }
    }

    #[test]
    fn same_seed_is_deterministic() {
        let p = Params {
            rebalance_hz: 5.0,
            ..base()
        };
        let a = run(&p);
        let b = run(&p);
        assert_eq!(a.true_total, b.true_total);
        assert_eq!(a.false_denials, b.false_denials);
    }

    #[test]
    fn strict_escrow_never_overshoots() {
        // The headline guarantee: Δ=0 means the true total never crosses the limit, whatever
        // the lease chunk.
        let fair = base().limit / u64::from(base().nodes);
        for &div in &[1u64, 4, 16] {
            let p = Params {
                chunk: (fair / div).max(1),
                delta: 0,
                ..base()
            };
            let s = measure(&p, 50);
            assert_eq!(
                s.max_overshoot_pct, 0.0,
                "overshoot at chunk Y/N÷{div} must be zero"
            );
        }
    }

    #[test]
    fn overshoot_never_exceeds_delta() {
        // With headroom Δ, the total may cross the limit but never by more than Δ.
        let fair = base().limit / u64::from(base().nodes);
        for &d_pct in &[10u64, 25, 50] {
            let p = Params {
                chunk: fair,
                delta: base().limit * d_pct / 100,
                ..base()
            };
            let s = measure(&p, 50);
            assert!(
                s.max_overshoot_pct <= d_pct as f64 + 0.001,
                "max overshoot {:.2}% exceeded Δ={}%",
                s.max_overshoot_pct,
                d_pct
            );
        }
    }

    #[test]
    fn smaller_chunks_reduce_false_denials() {
        // A finer lease strands less quota, so fewer writes are refused with room to spare.
        let fair = base().limit / u64::from(base().nodes);
        let coarse = measure(
            &Params {
                chunk: fair,
                delta: 0,
                ..base()
            },
            100,
        )
        .false_denial_pct;
        let fine = measure(
            &Params {
                chunk: fair / 8,
                delta: 0,
                ..base()
            },
            100,
        )
        .false_denial_pct;
        assert!(
            fine < coarse,
            "a finer chunk should lower false denials: Y/N÷8 {fine:.2}% vs Y/N {coarse:.2}%"
        );
    }

    #[test]
    fn headroom_reduces_false_denials() {
        let fair = base().limit / u64::from(base().nodes);
        let strict = measure(
            &Params {
                chunk: fair,
                delta: 0,
                ..base()
            },
            100,
        )
        .false_denial_pct;
        let slack = measure(
            &Params {
                chunk: fair,
                delta: base().limit / 2,
                ..base()
            },
            100,
        )
        .false_denial_pct;
        assert!(
            slack < strict,
            "an overshoot allowance should lower false denials: Δ=50% {slack:.2}% vs Δ=0 {strict:.2}%"
        );
    }
}
