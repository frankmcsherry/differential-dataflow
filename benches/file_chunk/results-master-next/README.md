# Verification of the master-next port

Base: `TimelyDataflow/differential-dataflow:master-next` at `75fba2b9`.
These are small correctness checks of the port, separate from the original
checkpoint's larger performance measurements. No large-data rerun was made.

```sh
cp benches/file_chunk/Cargo.lock Cargo.lock
cargo test --release -p differential-dataflow --lib --example file_chunk_spike --example key_cache_bench --example key_cache --test key_cache --locked
cargo build --release -p differential-dataflow --example file_chunk_spike --example key_cache --example key_cache_bench --locked
python3 benches/file_chunk/run.py --case small --memory-mib 128 --seconds 30 --output benches/file_chunk/results-master-next
target/release/examples/file_chunk_spike 4096 8 6 40 361 --moving --clustered --nocache
target/release/examples/key_cache
target/release/examples/key_cache_bench 4096 8 4096 hot64
```

- 42 library tests passed, one existing test ignored; 10 cache tests and six
  file-wrapper tests passed. Both additional example test targets compile.
- The 1,048,576-update file-backed join completed 12 checked rounds below
  7 MiB peak RSS. All rounds after the initial selection had zero file I/O.
- The moving clustered-key check produced correct counts and retractions,
  reading one chunk for each changed selection and none for repeated selections.
- The cache example reported four initial reads, zero on repetition, two for
  a new suffix batch, and six after eviction.
- The earlier cache benchmark's checksums agreed across resident/paged and
  cached/uncached modes in its small hot-key run.

See `../tests-master-next.log` and the raw files in this directory.
