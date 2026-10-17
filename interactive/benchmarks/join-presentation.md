# Join presentation allocation control

The join backend used two heap-allocated run-description vectors per matching
key, plus a merge-head vector per side when compound values spanned chunks.
Run descriptions now use four inline slots (spilling when needed), and each
side reuses its merge-head capacity across keys. Payload columns, matching,
time handling and consolidation are unchanged.

```sh
cargo test -p interactive --lib --test corgi_backend --test corgi_chunk_merge
cargo run --release -p interactive --example join_present_bench -- 50000 21
```

To compare against the parent, copy the new example into a separate checkout
of the parent commit and run the same command there. Build both binaries
before alternating timing runs. Allocation counting uses a separate pass.

Apple M4, Rust 1.96.0, System allocator, 50,000 keys, two values per key;
median of three alternating runs, each with 21 timing samples:

| Values / chunks | Before ns/key | After ns/key | Before allocation calls | After |
| --- | ---: | ---: | ---: | ---: |
| Integer / 1 | 69.8 | 55.7 | 100,112 | 112 |
| Integer / 4 | 167.9 | 163.3 | 100,126 | 126 |
| Pair / 1 | 63.7 | 49.9 | 100,140 | 140 |
| Pair / 4 | 337.8 | 309.5 | 200,238 | 252 |

[Raw measurements](join-presentation.json) retain revisions and binary hashes.
These are presentation times, including bridge equality and difference checks,
over prebuilt columns. They exclude ingestion, sorting, proxy matching,
projection, scheduling and graph iteration. Scalar `u64` timestamps are cheaper
than DDIR's dynamic iteration coordinates. The server uses mimalloc rather than
System. No whole-query speedup is established by this control.

The regression test uses seven chunks, compound values, cancellation and
successive keys to exercise both inline-capacity spilling and scratch reuse.
