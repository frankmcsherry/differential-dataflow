# Reduction order and allocation control

Presentation assigns value proxies in structural order within each real key;
the proxy harness sorts and consolidates each bracket by proxy. Min now gathers
only the first nonzero value from each bracket. Collect expands positive
multiplicities in the order already established. Neither needs a second
candidate gather and structural sort.

Reconciliation appends old output negations directly to the current key's tail
of the shared corrections vector, combines desired output there, and compacts
zero differences in place. This removes a temporary vector per key. Scalar
value comparisons still read borrowed columns.

```sh
cargo test -p interactive --lib --test ordered_reduce --test corgi_backend --test reduce_term
cargo run --release -p interactive --example reduce_values_bench -- 50000 8 11
```

The new integration test compares Vec and Corgi across four epochs of signed,
ragged values, negative multiplicities, list prefixes, retractions and restoration.
Existing backend tests cover hashed-key collisions. Min includes nonzero negative
values; Collect repeats positive copies and can emit an empty list for an
all-negative group. A fully retracted group emits nothing.

## Controlled measurements

Apple M4, System allocator, scalar `u64` timestamps, 50,000 keys, eight values
per key, three interleaved runs with 11 timings each. **Both sides use the
same allocation-free Corgi comparator**, proposed separately as a refresh of
[closed, unmerged PR #20](https://github.com/frankmcsherry/WIP/pull/20).
The DDIR dependency pin is unchanged in this branch.

| Reducer / values / existing output | Before ns/key | After ns/key | Before allocation calls | After |
| --- | ---: | ---: | ---: | ---: |
| Count / integer / yes | 23.63 | 16.54 | 50,019 | 20 |
| Min / integer / yes | 63.99 | 19.38 | 50,071 | 13 |
| Min / pair / yes | 87.56 | 24.89 | 50,097 | 24 |
| Collect / integer / yes | 113.84 | 52.48 | 50,098 | 33 |
| Collect / pair / yes | 162.24 | 95.12 | 50,121 | 44 |

[Raw controls](reduce-order.json) also preserve a rejected gathered-equality
prototype. That prototype took 131.84 ns/key for the final case above: copying
comparison operands outweighed its dispatch savings after scalar comparisons
became cheap. It is absent from this branch.

The timer covers value reduction and reconciliation only. Native columns and
proxy presentation are prepared beforehand; emitted columns are checked outside
the timer, and allocation counting is a separate pass. To reproduce the table,
copy the example into the parent checkout and use the identical Corgi override
in both checkouts. The raw record's local paths describe the historical
experiment; no local override is needed to build or test this branch. Whole-query
improvement from this DDIR change alone remains unestablished.
