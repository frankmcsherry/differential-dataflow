# Recorded runs of the original completed spike

These runs used the corrected, incrementally packed selection implementation
at the original `file-chunk-spike` checkpoint (`3fecff64`). They predate the
master-next port. Validation of that port is summarized in `../README.md`.

- `summary.csv` collects source size, peak RSS, timings, I/O, completed rounds,
  and any watchdog stop. It is the starting point for comparing workloads.
- Each `<case>.log` records source construction and then one line per query
  round, including file reads/writes and RSS. A final `verified` line means
  the computation checked all requested rounds successfully.
- Each `<case>.json` records the command, exit status, kernel peak RSS, and
  watchdog outcome for that process. Absolute command paths identify the
  original local run; use the current runner to reproduce the workload.
- `cursor_medium` and `giant_key_bypass` were deliberately stopped at the memory
  guard. They document limits, not successful completed computations.
- `large_profile` adds post-selection RSS diagnostics to the `large` workload.
  The moving-key cases shift their selection after each insertion/retraction
  pair; their later-round median mixes hits and changed-set reads.

The case definitions live in `../run.py`; the measurement interpretation and
limitations are in `../README.md`. Earlier implementation-debugging runs have
been removed from the PR.
