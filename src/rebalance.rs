//! RTP thread load rebalancing. Once a second the main thread looks at every RTP thread:
//! - **Hot:** a thread is hot when either:
//!   - its published load is at or over `--rebalance-load-pct` (70%); or
//!   - it started at least `--rebalance-late-cycles` (5) cycles late in the last second,
//!     a sign that its core is crowded even if its own work is light.
//! - **What moves:** one whole call or conference per hot thread per round, so its legs
//!   end up together on one thread. Among the groups with legs on the hot thread, the one
//!   with the most legs there moves.
//! - **Where to:** the coolest thread that isn't hot, by the same rule as binding (load
//!   step, then legs). When the source is hot by load, the target must be at least 10
//!   points cooler.
//! - **Gently:**
//!   - each thread gives or takes at most one group per round;
//!   - a group that moved stays put for 10 s;
//!   - a move must not make the target hot. The moving legs are estimated at the source's
//!     load ÷ its legs each, and the target's load plus that must stay under the threshold.
//!
//! [`plan`] decides; the engine carries the moves out (`Engine::rebalance`), and each leg is
//! handed over whole by its RTP thread (see `leg::rtp_thread`).

use std::cmp::Reverse;
use std::collections::HashSet;
use std::time::Duration;

use crate::cycle::LOAD_STEP;
use crate::leg::ThreadLoad;

/// How often the main thread rebalances.
pub const EVERY: Duration = Duration::from_secs(1);
/// A group that moved stays on its thread at least this long.
pub const MIN_STAY: Duration = Duration::from_secs(10);

/// When a thread counts as hot. A zero turns that trigger off.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RebalanceConfig {
    /// Load, in percent.
    pub load_pct: u32,
    /// Late cycles in the last second.
    pub late_cycles: u64,
}

impl Default for RebalanceConfig {
    fn default() -> Self {
        RebalanceConfig { load_pct: 70, late_cycles: 5 }
    }
}

impl RebalanceConfig {
    pub fn off() -> RebalanceConfig {
        RebalanceConfig { load_pct: 0, late_cycles: 0 }
    }

    pub fn enabled(&self) -> bool {
        self.load_pct > 0 || self.late_cycles > 0
    }

    /// Why a thread is hot, if it is.
    fn hot(&self, t: &ThreadLoad) -> Option<Reason> {
        if self.load_pct > 0 && t.load >= self.load_pct * 10 {
            Some(Reason::Load(t.load))
        } else if self.late_cycles > 0 && t.late >= self.late_cycles {
            Some(Reason::Late(t.late))
        } else {
            None
        }
    }
}

/// A call or conference, as the planner sees it.
#[derive(Debug, Clone)]
pub struct Group {
    /// The thread each of its legs is on.
    pub legs: Vec<usize>,
    /// It hasn't moved in the last `MIN_STAY`.
    pub movable: bool,
}

/// Why a thread gives up a group.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Reason {
    /// Its load, in permille.
    Load(u32),
    /// Its late cycles in the last second.
    Late(u64),
}

/// One group to move: every leg of `groups[group]` goes to thread `to`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Move {
    pub group: usize,
    pub from: usize,
    pub to: usize,
    pub reason: Reason,
}

/// Decides this round's moves. `threads[i]` is thread `i`.
pub fn plan(cfg: &RebalanceConfig, threads: &[ThreadLoad], groups: &[Group]) -> Vec<Move> {
    debug_assert!(threads.iter().enumerate().all(|(i, t)| t.index == i));
    let mut legs: Vec<usize> = threads.iter().map(|t| t.legs).collect();
    let mut hot: Vec<(usize, Reason)> = threads.iter().filter_map(|t| cfg.hot(t).map(|r| (t.index, r))).collect();
    // The hottest first: by load, then by late cycles.
    hot.sort_by_key(|&(_, r)| match r {
        Reason::Load(l) => (Reverse(1), Reverse(l as u64)),
        Reason::Late(n) => (Reverse(0), Reverse(n)),
    });

    let mut used: HashSet<usize> = HashSet::new();
    let mut moved: HashSet<usize> = HashSet::new();
    let mut moves = Vec::new();
    for (from, reason) in hot {
        if used.contains(&from) {
            continue;
        }
        let target = threads
            .iter()
            .filter(|t| t.index != from && !used.contains(&t.index) && cfg.hot(t).is_none())
            .filter(|t| match reason {
                Reason::Load(load) => t.load + 2 * LOAD_STEP <= load,
                Reason::Late(_) => true,
            })
            .min_by_key(|t| (t.load / LOAD_STEP, legs[t.index], t.index));
        let Some(to) = target.map(|t| t.index) else { continue };

        // The group with the most legs on the hot thread that fits on the target without
        // making it hot.
        let per_leg = threads[from].load / legs[from].max(1) as u32;
        let fits = |incoming: usize| cfg.load_pct == 0 || threads[to].load + per_leg * (incoming as u32) < cfg.load_pct * 10;
        let best = groups
            .iter()
            .enumerate()
            .filter(|(g, group)| group.movable && !moved.contains(g))
            .filter_map(|(g, group)| {
                let here = group.legs.iter().filter(|&&t| t == from).count();
                let incoming = group.legs.iter().filter(|&&t| t != to).count();
                (here > 0 && fits(incoming)).then_some((g, here, group.legs.len()))
            })
            .max_by_key(|&(g, here, size)| (here, Reverse(size), Reverse(g)));
        let Some((group, _, _)) = best else { continue };

        for &t in &groups[group].legs {
            if t != to {
                legs[t] -= 1;
                legs[to] += 1;
            }
        }
        used.extend([from, to]);
        moved.insert(group);
        moves.push(Move { group, from, to, reason });
    }
    moves
}

#[cfg(test)]
mod tests {
    use super::*;

    fn thread(index: usize, load_pct: u32, late: u64, legs: usize) -> ThreadLoad {
        ThreadLoad { index, name: format!("rtp-{index}"), load: load_pct * 10, late, legs }
    }

    fn group(legs: &[usize]) -> Group {
        Group { legs: legs.to_vec(), movable: true }
    }

    #[test]
    fn nothing_moves_when_no_thread_is_hot() {
        let threads = [thread(0, 60, 2, 6), thread(1, 0, 0, 0)];
        assert!(plan(&RebalanceConfig::default(), &threads, &[group(&[0, 0])]).is_empty());
    }

    #[test]
    fn a_hot_thread_gives_its_biggest_group_to_the_coolest_thread() {
        let threads = [thread(0, 85, 0, 6), thread(1, 40, 0, 2), thread(2, 10, 0, 2)];
        let groups = [group(&[0, 0]), group(&[0, 0, 0, 1]), group(&[1, 2])];
        let moves = plan(&RebalanceConfig::default(), &threads, &groups);
        assert_eq!(moves, [Move { group: 1, from: 0, to: 2, reason: Reason::Load(850) }], "the conference with 3 legs there");
    }

    #[test]
    fn late_cycles_make_a_thread_hot_even_when_its_load_is_low() {
        let threads = [thread(0, 5, 20, 2), thread(1, 5, 0, 0)];
        let moves = plan(&RebalanceConfig::default(), &threads, &[group(&[0, 0])]);
        assert_eq!(moves, [Move { group: 0, from: 0, to: 1, reason: Reason::Late(20) }]);
    }

    #[test]
    fn gentle_limits() {
        let cfg = RebalanceConfig::default();
        // A group that moved recently stays.
        let threads = [thread(0, 90, 0, 2), thread(1, 0, 0, 0)];
        assert!(plan(&cfg, &threads, &[Group { legs: vec![0, 0], movable: false }]).is_empty());
        // The target must be 10 points cooler than a thread hot by load.
        let threads = [thread(0, 75, 0, 2), thread(1, 69, 0, 0)];
        assert!(plan(&cfg, &threads, &[group(&[0, 0])]).is_empty());
        // A move must not make the target hot: two legs worth 45% each onto a thread at 60%.
        let threads = [thread(0, 90, 0, 2), thread(1, 60, 0, 0)];
        assert!(plan(&cfg, &threads, &[group(&[0, 0])]).is_empty());
        // Legs are weighed by load, not counted: a busy thread with few heavy legs gives
        // them to a thread with many light ones.
        let threads = [thread(0, 75, 0, 4), thread(1, 20, 0, 6)];
        assert_eq!(plan(&cfg, &threads, &[group(&[0, 0])]).len(), 1);
        // Each thread gives or takes one group per round: two hot threads, one cool one.
        let threads = [thread(0, 90, 0, 4), thread(1, 80, 0, 4), thread(2, 0, 0, 0)];
        let moves = plan(&cfg, &threads, &[group(&[0, 0]), group(&[1, 1])]);
        assert_eq!(moves.len(), 1);
        assert_eq!((moves[0].from, moves[0].to), (0, 2), "the hottest goes first");
        // Off means off.
        assert!(plan(&RebalanceConfig::off(), &[thread(0, 100, 50, 2), thread(1, 0, 0, 0)], &[group(&[0, 0])]).is_empty());
    }

    #[test]
    fn a_split_call_is_brought_together() {
        // A call split over two threads; the other legs on the hot thread just moved there.
        let threads = [thread(0, 5, 10, 3), thread(1, 0, 0, 1)];
        let groups = [group(&[0, 1]), Group { legs: vec![0, 0], movable: false }];
        let moves = plan(&RebalanceConfig::default(), &threads, &groups);
        assert_eq!(moves, [Move { group: 0, from: 0, to: 1, reason: Reason::Late(10) }], "only one leg has to move");
    }
}
