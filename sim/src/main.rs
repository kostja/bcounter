// SPDX-License-Identifier: MIT OR Apache-2.0
// Copyright (c) 2026 Konstantin Osipov.

//! Discrete-event simulator for the `bcounter` model.
//!
//! It drives the real [`bcounter::BCounter`] across `N` nodes, each consuming quota as a
//! compound-Poisson stream (events at rate `lambda`, sizes drawn heavy-tailed from a
//! lognormal), and gossiping — a full-mesh state merge — every `1/f` seconds. It measures the
//! *actual* overshoot of the limit and compares it to the frequency law in the README:
//!
//!   overshoot ~ Λ_vol / f      (Λ_vol = cluster consumption rate in units/sec)
//!
//! and confirms the boundary: with no gossip the overshoot climbs to `(N-1)·Y`.
//!
//! Deterministic (seeded), no external dependencies, no wall clock. Run: `cargo run -p
//! bcounter-sim`.

use bcounter::BCounter;

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

    /// Uniform in the half-open interval `[0, 1)`.
    fn unit(&mut self) -> f64 {
        // 53 bits of mantissa precision.
        (self.next_u64() >> 11) as f64 / (1u64 << 53) as f64
    }

    /// Exponential inter-arrival time for a Poisson process of the given rate.
    fn exp(&mut self, rate: f64) -> f64 {
        -(1.0 - self.unit()).ln() / rate
    }

    /// A standard normal deviate (Box–Muller).
    fn normal(&mut self) -> f64 {
        let u1 = 1.0 - self.unit(); // in (0, 1], keeps ln finite
        let u2 = self.unit();
        (-2.0 * u1.ln()).sqrt() * (std::f64::consts::TAU * u2).cos()
    }

    /// A lognormal draw with the given arithmetic `mean` and coefficient of variation `cv`
    /// (std/mean). Heavy-tailed for `cv > 1`, which is the realistic case for object sizes.
    fn lognormal(&mut self, mean: f64, cv: f64) -> f64 {
        let s2 = (1.0 + cv * cv).ln();
        let mu = mean.ln() - s2 / 2.0;
        (mu + s2.sqrt() * self.normal()).exp()
    }
}

/// One simulation's inputs.
#[derive(Clone, Copy)]
struct Params {
    /// Number of cluster nodes, each an independent writer.
    nodes: u32,
    /// The global limit being enforced.
    limit: u64,
    /// Per-node event rate (writes/sec).
    lambda: f64,
    /// Mean write size, in the same units as `limit`.
    size_mean: f64,
    /// Coefficient of variation of write size (dispersion). `> 1` is heavy-tailed.
    size_cv: f64,
    /// Gossip frequency (Hz). `0.0` means never gossip after the start.
    gossip_hz: f64,
    /// How long to run, in seconds.
    duration: f64,
    /// PRNG seed.
    seed: u64,
}

/// One simulation's measurements. Some fields are asserted only by the tests, not read by the
/// binary, which is intentional.
#[allow(dead_code)]
struct Outcome {
    /// True cluster total at the end (ground truth: the sum of everything admitted).
    true_total: u64,
    /// `true_total − limit`, saturating at zero. The overshoot.
    overshoot: u64,
    /// Writes admitted; writes refused.
    admitted: u64,
    denied: u64,
    /// Denials issued while the *true* total was still under the limit — the false denials.
    /// For the optimistic counter this must be zero: a node's view never exceeds the truth.
    false_denials: u64,
}

/// Cluster consumption rate in units/sec: `N · lambda · mean_size`.
fn consumption_rate(p: &Params) -> f64 {
    f64::from(p.nodes) * p.lambda * p.size_mean
}

/// Run one simulation, driving the real `BCounter` on every node.
fn run(p: &Params) -> Outcome {
    let mut rng = Rng::new(p.seed);
    let mut nodes: Vec<BCounter> = (0..p.nodes).map(|id| BCounter::new(id, p.limit)).collect();

    // Event-driven: each node's next write time, plus the next gossip tick.
    let mut next_write: Vec<f64> = (0..p.nodes as usize).map(|_| rng.exp(p.lambda)).collect();
    let gossip_tau = if p.gossip_hz > 0.0 {
        1.0 / p.gossip_hz
    } else {
        f64::INFINITY
    };
    let mut next_gossip = gossip_tau;

    let mut true_total: u64 = 0;
    let mut admitted = 0u64;
    let mut denied = 0u64;
    let mut false_denials = 0u64;

    loop {
        // Whichever happens first: the earliest write, or the next gossip.
        let (soonest_node, soonest_write) = next_write
            .iter()
            .enumerate()
            .min_by(|a, b| a.1.partial_cmp(b.1).unwrap())
            .map(|(i, &t)| (i, t))
            .unwrap();

        let now = soonest_write.min(next_gossip);
        if now > p.duration {
            break;
        }

        if next_gossip <= soonest_write {
            // Full-mesh gossip: fold every node's state, then hand the union back to each.
            let mut merged = nodes[0].clone();
            for n in &nodes[1..] {
                merged.merge(n);
            }
            for n in &mut nodes {
                n.merge(&merged);
            }
            next_gossip += gossip_tau;
        } else {
            // A write at `soonest_node`.
            let draw = rng.lognormal(p.size_mean, p.size_cv).round() as u64;
            let amount = draw.max(1);
            match nodes[soonest_node].inc(amount) {
                Ok(()) => {
                    admitted += 1;
                    true_total += amount;
                }
                Err(_) => {
                    denied += 1;
                    // A *false* denial refuses a write that would have fit in the true
                    // remaining room -- not merely one issued while under the limit. The
                    // optimistic counter never does this: it denies only when its own view
                    // (which never exceeds the truth) plus the write already breaches Y, so
                    // the truth plus the write breaches it too.
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

/// Percentile of a slice (nearest-rank), `q` in `[0, 1]`.
fn percentile(sorted: &[f64], q: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let idx = ((sorted.len() as f64 - 1.0) * q).round() as usize;
    sorted[idx]
}

/// Run `reps` replicates at one gossip frequency and summarize the overshoot, as a percentage
/// of the limit.
struct Summary {
    mean_pct: f64,
    p90_pct: f64,
    max_pct: f64,
    false_denials: u64,
}

fn sweep_point(base: &Params, gossip_hz: f64, reps: u64) -> Summary {
    let mut overshoots: Vec<f64> = Vec::with_capacity(reps as usize);
    let mut false_denials = 0u64;
    for r in 0..reps {
        let p = Params {
            gossip_hz,
            seed: base.seed.wrapping_add(r.wrapping_mul(0x1000_0001)),
            ..*base
        };
        let o = run(&p);
        overshoots.push(o.overshoot as f64 / base.limit as f64 * 100.0);
        false_denials += o.false_denials;
    }
    overshoots.sort_by(|a, b| a.partial_cmp(b).unwrap());
    Summary {
        mean_pct: overshoots.iter().sum::<f64>() / overshoots.len() as f64,
        p90_pct: percentile(&overshoots, 0.90),
        max_pct: percentile(&overshoots, 1.0),
        false_denials,
    }
}

fn main() {
    let base = Params {
        nodes: 16,
        limit: 1_000_000,
        lambda: 62.5, // per node; cluster rate = 16 * 62.5 * 100 = 100_000 units/sec
        size_mean: 100.0,
        size_cv: 1.5,   // heavy-tailed object sizes
        gossip_hz: 0.0, // overwritten per sweep point
        duration: 120.0,
        seed: 0xC0FFEE,
    };
    let reps = 400u64;
    let rate = consumption_rate(&base); // units/sec
    let fill_time = base.limit as f64 / rate;

    println!("bcounter overshoot simulation (optimistic counter, full-mesh gossip)");
    println!(
        "  N={} nodes, Y={} units, cluster rate Λ={:.0} units/s, mean size={} (cv={}), \
         fill time≈{:.1}s, {} reps/point",
        base.nodes, base.limit, rate, base.size_mean, base.size_cv, fill_time, reps
    );
    println!(
        "  frequency law predicts overshoot ~ Λ/f; the '(Λ/f)/Y' column is that reference line\n"
    );
    println!(
        "  {:>8}  {:>9}  {:>10}  {:>9}  {:>9}  {:>11}  {:>6}",
        "f (Hz)", "τ (ms)", "mean %Y", "p90 %Y", "max %Y", "(Λ/f)/Y %", "false"
    );

    // f = 0 first (never gossip) -> the (N-1)*Y boundary; then a rising sweep.
    let ceiling_pct = f64::from(base.nodes - 1) * 100.0;
    let s0 = sweep_point(&base, 0.0, reps);
    println!(
        "  {:>8}  {:>9}  {:>10.1}  {:>9.1}  {:>9.1}  {:>11}  {:>6}   (ceiling (N-1)·Y = {:.0}%)",
        "0", "∞", s0.mean_pct, s0.p90_pct, s0.max_pct, "-", s0.false_denials, ceiling_pct
    );

    for &f in &[1.0, 2.0, 5.0, 10.0, 20.0, 50.0, 100.0] {
        let s = sweep_point(&base, f, reps);
        let ref_pct = rate / f / base.limit as f64 * 100.0;
        println!(
            "  {:>8.0}  {:>9.1}  {:>10.2}  {:>9.2}  {:>9.2}  {:>11.2}  {:>6}",
            f,
            1000.0 / f,
            s.mean_pct,
            s.p90_pct,
            s.max_pct,
            ref_pct,
            s.false_denials
        );
    }
    println!(
        "\n  Read: overshoot falls ~1/f (matching the reference line to a small constant), \
         the optimistic\n  counter never false-denies, and with no gossip it saturates at the \
         (N-1)·Y ceiling."
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base() -> Params {
        Params {
            nodes: 8,
            limit: 100_000,
            lambda: 50.0,
            size_mean: 100.0,
            size_cv: 1.0,
            gossip_hz: 0.0,
            duration: 60.0,
            seed: 1,
        }
    }

    #[test]
    fn same_seed_is_deterministic() {
        let p = Params {
            gossip_hz: 5.0,
            ..base()
        };
        let a = run(&p);
        let b = run(&p);
        assert_eq!(a.true_total, b.true_total);
        assert_eq!(a.admitted, b.admitted);
        assert_eq!(a.denied, b.denied);
    }

    #[test]
    fn optimistic_counter_never_false_denies() {
        // Across a spread of gossip rates, a denial only ever happens once the truth is at the
        // limit -- the defining property of the optimistic branch.
        for &f in &[0.0, 1.0, 10.0, 100.0] {
            let s = sweep_point(&base(), f, 50);
            assert_eq!(s.false_denials, 0, "false denials at f={f}");
        }
    }

    #[test]
    fn no_gossip_saturates_near_the_n_minus_one_ceiling() {
        // With no exchange each node fills the whole limit on its own -> total ≈ N·Y.
        let s = sweep_point(&base(), 0.0, 100);
        let ceiling = f64::from(base().nodes - 1) * 100.0; // (N-1)*Y as %Y
        assert!(
            s.mean_pct > 0.8 * ceiling,
            "mean overshoot {:.0}% should approach the {:.0}% ceiling",
            s.mean_pct,
            ceiling
        );
    }

    #[test]
    fn overshoot_shrinks_as_gossip_quickens() {
        let slow = sweep_point(&base(), 2.0, 200).mean_pct;
        let fast = sweep_point(&base(), 50.0, 200).mean_pct;
        assert!(
            fast < slow,
            "faster gossip should overshoot less: f=50 gave {fast:.2}% vs f=2 {slow:.2}%"
        );
    }

    #[test]
    fn overshoot_tracks_the_one_over_f_reference() {
        // Measured overshoot should sit within a small constant factor of the Λ/f line in the
        // regime where gossip is frequent relative to the fill.
        let b = base();
        let rate = consumption_rate(&b);
        for &f in &[10.0, 20.0, 50.0] {
            let measured = sweep_point(&b, f, 300).mean_pct;
            let reference = rate / f / b.limit as f64 * 100.0;
            let ratio = measured / reference;
            assert!(
                (0.2..5.0).contains(&ratio),
                "at f={f} measured {measured:.2}% vs Λ/f {reference:.2}% (ratio {ratio:.2})"
            );
        }
    }
}
