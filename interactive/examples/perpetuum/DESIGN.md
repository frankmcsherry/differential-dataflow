# Perpetuum — a game about reaching fixed point

*Investigation + working prototype, 2026-08-24. Prompt: a "perpetual motion
machine" with fixed parts (input data, expressions) that must be repaired so it
reaches a perfect fixed point and fully resolves — constrained so the trivial
0 → 0 solution is out of reach.*

## The one idea everything hangs on

In differential dataflow, "done" is not a judged condition — it is a **measured**
one. A computation at fixed point produces no updates, does no work, and may
exit; until then it cannot. So the game needs no win-checker comparing your
state to a stored answer: **you win when the runtime goes silent.** The victory
screen is a flatlined profiler.

The complementary mechanic falls out of the same physics: **your edits are
diffs, and their cost is the actual DD work they cause.** The score (`energy`)
counts every input update the machine processes — the machine's own churn and
your edits alike. A surgical fix whose blast radius dies out quickly beats a
rebuild-the-world fix, which is precisely the incremental-computation cost
model. The game teaches "think in deltas" without ever saying so.

Throughline shared with the earlier belt/factory demos: *DD work = disturbance
propagation; equilibrium ⇒ no work.* The game makes equilibrium the goal.

## The prototype (playable now)

- `perpetuum.rs` — driver, in the style of `belt.rs`: parse → lower → render
  `belt.ddp` (rule-184/TASEP, **unchanged and sealed** — the "fixed
  expressions"), feed its `result` moves back into occupancy, REPL on stdin.
- `levels/*.lvl` — data-only levels: topology (`cycle`/`chain`/`edge`), items,
  tool budgets, par, render rows.
- `solutions/*.txt` — pipeable scripts; each is also a regression test
  (all five currently RESOLVE, at par).

```
cargo run --release --example perpetuum -- examples/perpetuum/levels/01-two-rings.lvl
# mv A B | cut A B | link A B | t [n] | r | help | q     (PERPETUUM_IDS=1 for cell ids)
cargo run --release --example perpetuum -- examples/perpetuum/levels/01-two-rings.lvl \
  < examples/perpetuum/solutions/01.txt        # scripted solve
```

A level opens in perpetual motion — a bubble circling a ring forever, the
energy meter counting DD's cost for it. Edits (`mv`/`cut`/`link`) do not tick
the machine; `t`/`r` do. The machine is STILL when its move set is empty; it is
RESOLVED when it is still **and whole** (see below).

## Why the trivial solution is unreachable

Three fences, each doing a distinct job:

1. **Conservation** — `mv` teleports an item; nothing creates or destroys one,
   and levels have no drains. The empty fixed point 0 → 0 simply isn't in the
   reachable state space.
2. **The loose-ends rule** — `cut` creates a dead end that items pile against:
   silence by strangulation. So RESOLVED additionally requires every cell to
   keep an out-edge. A jam is a fixed point of a *broken* machine, and the game
   says so ("loose ends at [5, 16]"). Cuts are for re-plumbing, not plugging.
3. **Out-degree ≤ 1** — enforced by the level loader, because the sealed
   physics would duplicate an item that has two empty successors. (Merges —
   in-degree 2 — are fine; the program's `min` arbitrates, and level 4 uses one.)

## The puzzle theory

For rule-184 with at most one successor per cell, a configuration is at fixed
point iff the occupied set is **successor-closed** (no item faces an empty
cell). Consequences, which are the level design space:

- **Rings rest full or empty.** A ring with a gap churns forever — the gap
  itself circulates. This is the canonical perpetual motion machine.
- **Stillness is arithmetic.** With item count fixed, resolving means writing
  N as a sum of cycle sizes (subset-sum). Level 2 is exactly this.
- **Local calm can be globally wrong.** A full ring is at rest, but if the
  arithmetic demands its items elsewhere you must disturb an equilibrium and
  pay for the blast (level 3).
- **Tails give flexible capacity.** A feeder chain into a ring rests at
  ring-size + any contiguous suffix of the chain pressed against it. You fix
  the counts; the machine settles itself, and the settling cost is measured —
  *which* item you remove changes the bill (level 4: 18 vs 26 energy).
- **Sometimes no resting state exists.** Rings 6 and 7 with 10 items cannot
  rest; the fix is surgery — cut and re-link belts into cycles whose sizes can
  absorb N exactly (level 5: split the 7 into 4+3, fill 6 and 4). Cheaper
  decompositions exist than the obvious one; par rewards finding them.

Levels shipped: `01-two-rings` (rings rest full/empty), `02-subset-sum`,
`03-false-calm` (break a local equilibrium), `04-feeder` (merge junction +
self-settling), `05-surgery` (re-plumb the topology). Par values are measured
from the solution scripts.

## Where this can go next

- **Live mode.** The machine free-runs at a few ticks/sec while you edit;
  urgency comes from the energy meter, not a timer. (The current turn-based
  REPL was chosen to make solutions scriptable and verifiable.)
- **Blast-radius overlay.** Render cells whose occupancy changed this tick
  (the `render_peek.py` / `^^` overlay idea) so the disturbance cone is
  visible as it propagates and dies.
- **The resolver pack (inner fixed points).** A second physics where the
  machine runs a corrective `var` loop (chase-style: violations → repairs →
  new violations) that converges per tick by construction (monotone per key,
  finite domain — a divergent inner loop would hang `tick`, which is bad UX,
  so divergence lives across host epochs only). The player tunes designated
  input rows until the repair stream dries up. Same win condition, logical
  rather than spatial puzzles.
- **Server version.** Run the machine on `ddir_server`: the level is an
  installed program, instruments (utilization, is-balanced analyzers) are
  *other* programs importing its exported traces, and winning lets the server
  literally `drop` the dataflow — "the machine, at rest, may exit." Also the
  natural home for shared-trace multiplayer curiosities.
- **Welds.** Already supported by loader and physics (a welded cell holds an
  immovable item and loses its out-edge — exempt from the loose-ends rule):
  parking-lot/buffer components with flexible capacity. No shipped level uses
  them yet.

## Design dead-ends worth remembering

- **"Anchored but machine-movable" items don't exist**: rule-184 occupancy has
  no item identity, so a constraint tied to *an item* is untrackable once the
  CA moves it. Constraints must attach to cells (welds) or to component-level
  counts (conservation per component: the CA never crosses components, only
  `mv` does).
- **Inner-scope divergence is not a usable failure state**: a non-converging
  `var` loop hangs the epoch. All "unresolved" states must churn across host
  ticks, never inside one.
- **Throughput goals contradict the win condition**: in this physics, motion
  *is* work; a "keep the belt running" objective can never quiesce. The
  resolution is thematic: you are not asked to run the machine, you are asked
  to balance it.
