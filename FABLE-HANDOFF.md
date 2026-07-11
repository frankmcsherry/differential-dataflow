# FABLE hand-off — amortizing DDIR interpretation toward compiled performance

This is a **context** hand-off, not a plan. It gives you the codebases, the abstractions that exist,
the harnesses to measure with, the current numbers, guardrails, and a factual log of what's been
tried. It deliberately does **not** hand you our conclusions about where the gap "is" or which
approach to take — reach your own, from the measurements. Where prior notes contain analysis, treat
it as a hypothesis to verify, not a result to inherit.

## Remit (broad)

Close the gap between **interpreted DDIR** and **compiled (native) differential dataflow**, by
amortizing per-tuple interpretation overhead through columnar batch execution (corgi is the current
vehicle). Reuse the existing abstractions where they serve; you have permission to **replace pieces
(e.g. write new tactics)** where they don't. Constraint on the *shape* of a solution, not the
approach: the win must come from **amortizing** interpretation across batches — pushing corgi full of
bespoke per-workload Rust (hand-compiling the interpreter) is a loss, not a win. Prefer reusing
existing parts over building new ones.

## The three codebases — BRANCH BEFORE YOU TOUCH

All three are live and shared with other agents/humans. **Create your own working branches off the
start points below; do not modify or force-push the existing branches.**

1. **DD + DDIR** — one repo: `/Users/mcsherry/Projects/dd-corgi-mn`
   - It's the `differential-dataflow` workspace; **DDIR is the `interactive/` member crate**.
   - Start point: branch **`corgi-on-mn`** @ `7b2a6ff8` (tag `fable-start-2026-07-11`). This is the
     current backend (columnar arrangement + `int_proxy` reduce/join over corgi columns).
   - Other branches worth reading (don't build on blindly — see the log): `corgi-columnar-bridge`
     (a columnar `int_proxy` bridge variant), `corgi-survey-merge` (a reverted merge experiment).
   - DD proper (spine, batcher, `Chunk` trait, `int_proxy`, native cursor tactics) lives here too —
     you may change DD, but branch it (it's the same repo).
2. **corgi** (the columnar engine): `/Users/mcsherry/Projects/wip`, remote `frankmcsherry/wip`
   - Start point: branch **`dd-arrange-api`** @ `f556868` (what `interactive/Cargo.toml` pins).
   - **Guardrail: consume corgi's kernels; do not stuff corgi with bespoke Rust.** An active corgi
     agent owns this repo — coordinate, branch, don't clobber.
3. **int-proxy tactic (columnar variant)** — DD-repo branch **`fm/int-proxy-columnar`** (fetch it):
   a columnar reshaping of the `int_proxy` reduce/join seam, by a tactic-focused agent.

## Abstractions that exist (described, not judged)

- **`Chunk` / `CorgiChunk`** (`interactive/src/corgi_chunk.rs`): a cursor-less columnar arrangement —
  key/val are corgi `Value` columns, times are SoA (`ColTimes`, `col_times.rs`), diffs a `Vec`. It
  implements DD's `Chunk` trait (merge / advance / settle / extract) but not `NavigableChunk`.
- **Tactics**: `JoinTactic` (`corgi_join.rs`, the #790 iterator-of-containers join) and
  **`int_proxy`** (`differential-dataflow/src/operators/int_proxy/`, PR #781): a backend-agnostic
  reduce/join seam over integer proxies `(key_hash, value_id)`, so the framework never needs the
  key/val Rust types. `interactive/src/corgi_reduce_backend.rs` implements the reduce side;
  `backend/corgi.rs` wires it.
- **Native DD tactics** (`reduce_abelian`, `join_core`, cursor-based) are also right here and are
  what the **vec** backend (`interactive/src/backend/vec.rs`) uses.

Permission, restated: `int_proxy` is *one* way to bridge dynamic-shape columns to DD's static-typed
operators. If it introduces overhead you don't need, new tactics are fair game — raw DD tactics are
available to compose. Reuse first.

## How to measure (all from `interactive/`, `~/.cargo/bin/cargo`)

- **Correctness gate** (must stay green): `cargo run --release --example corgi_progs` → "all programs
  match vec backend" (6 programs, each asserted == the vec backend).
- **Three-way, at scale**: `N=50000,100000,150000,250000 cargo run --release --example corgi_scc_big`
  → native DD / DDIR-vec / DDIR-corgi, e=2n.
- **Per-operator**: `cargo run --release --example corgi_scorecard` → corgi-vs-vec ratio per operator.
- **Profiling** (samply → pollard or the Firefox profiler): `N=100000 ITERS=6 samply record
  --save-only -o /tmp/p.json.gz -- target/release/examples/corgi_scc_prof` (and `vec_scc_prof` for
  the vec twin — built the same way, apples-to-apples). `corgi_scc_prof` loops corgi-only at a size.

## Current measured state (numbers only)

SCC, e=2n, this machine (arm64), `corgi-on-mn @ 7b2a6ff8`:

| n | native | vec-DDIR (×nat) | corgi-DDIR (×nat, ×vec) |
|---|---|---|---|
| 50k | 2.02s | 1.34s (0.7×) | 2.63s (1.3×, 1.96×) |
| 100k | 4.19s | 3.04s (0.7×) | 5.96s (1.4×, 1.96×) |
| 250k | 12.96s | 9.87s (0.8×) | ~linear (~2×) |

Scorecard (corgi ÷ vec, lower=faster): map ~0.33×, filter ~0.75×, arrange ~0.9×, join ~0.66×,
reduce_distinct/count ~1.07×, reach ~1.2×. (These are *inputs* to your own analysis, not a verdict.)

## Guardrails (hard)

1. **Correctness gate green on every change** — `corgi_progs` == vec, all 6 programs.
2. **Scaling test PAST the chunk boundary** — always measure at **n ≥ 150k** (e=2n), not just small n.
   The arrangement's chunk `TARGET` is `1<<18 = 262144`; below it everything is a single chunk and
   multi-chunk bugs are invisible. A single-chunk speedup can be a mirage (see log).
3. **Profile before concluding.** Several plausible-sounding optimizations this project turned out
   net-zero or negative; the profiler caught each. Don't reason about where time goes — measure it.

## Log of what's been tried (facts + measured outcome; no strategy attached)

- **Columnar times** (`ColTimes`, SoA over `<T as Columnar>::Container`): removed the per-row
  `PointStamp` allocation from the arrangement. SCC ~2.15× → ~1.95× vec. Kept.
- **value-as-id**: for primitive keys/vals the value is used as the id (skip hashing). reduce
  microbenches ~1.18× → ~1.07×; SCC unchanged. Kept.
- **Hash unification**: compound ids now use native `corgi::hash` (not a branch-local `hash_rows`).
  Perf-neutral, correctness-preserving. Kept.
- **Columnar `int_proxy` bridge** (`corgi-columnar-bridge`, from `fm/int-proxy-columnar`): SCC flat,
  shallow-time reduce microbenches regressed ~27%. Not merged; branch preserved.
- **survey/group_bounds merge** (`corgi-survey-merge`, reverted @ `7b2a6ff8`): replaced the
  two-pointer merge's per-pair compare with a galloped `survey`. Single-chunk SCC 1.94× → 1.62× vec,
  **but** past the chunk boundary it under-consolidated the arrangement and went super-linear (n=150k:
  19.5× vec vs 2.08× with the two-pointer). Reverted the merge; kept `group_bounds` in `advance`
  (perf-neutral). Open corgi ask: a group-range `Both` for `survey` so it composes with out-of-band
  times. **This is the concrete instance of guardrail #2.**
- `find_ranges` seek for the reduce present, and a couple of other micro-changes: net-negative or
  neutral, reverted. (Details in git log / the memory notes.)

## Prior notes (hypotheses, NOT conclusions to inherit)

- `corgi-kernel-findings-scc.md` (repo root): our per-kernel profile analysis of corgi's cost on SCC,
  written for the corgi agent. It contains *our* hypotheses (and one self-correction) — verify against
  your own profiles.
- Git history on `corgi-on-mn` is heavily commented; each commit message states what changed and the
  measured delta.

## Coordinates summary

| thing | where | ref |
|---|---|---|
| DD + DDIR (start) | `/Users/mcsherry/Projects/dd-corgi-mn` | branch `corgi-on-mn` @ `7b2a6ff8` / tag `fable-start-2026-07-11` |
| corgi (start) | `/Users/mcsherry/Projects/wip` (`frankmcsherry/wip`) | branch `dd-arrange-api` @ `f556868` |
| tactic columnar variant | DD repo | `fm/int-proxy-columnar` |
| bridge experiment | DD repo | branch `corgi-columnar-bridge` |
| reverted survey merge | DD repo | branch `corgi-survey-merge` |
