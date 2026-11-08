
## Ledgers

DDIR maintains continually evolving collections called LEDGERs.
A ledger is a collection of timestamped updates, each written `(data, time, diff)`, and two frontiers, `read` and `write`.
The `write` frontier is an antichain that is a lower bound on any future `time` that might be part of an added update.
The `read` frontier is a lower bound on times that are certain to accumulate correctly, a mechanism to compact the ledgers.

Each ledger is identified by some unique id, perhaps a UUID.
These are expected not to conflict, and naming a ledger by its id is meant to be unambiguous.
The identifiers are not meant to expose any further structure, at this point at least.

A ledger is represented by a collection of "committed SPANs", each represents durable information about a span of time in the ledger.
Each span is a tuple `(id, lower, upper, since, batch)`, where:
* `id` is the identifier of the ledger the span belongs to.
* `lower` and `upper` indicate a span of time `[lower, upper)` whose updates the span exactly covers.
* `since` indicates that all updates are at times that compare exactly to any time at least `since`, allowing the data to be compacted.
* `batch` is an enum that can represent e.g. EMPTY | INLINE(bytes) | BLOB(hash), indicating how to retrieve the updates themselves (e.g. a blob by its hash, from somewhere).
Each span is a self-describing statement about part of a ledger.

Any spans with matching `(id, lower, upper)` shoud not disagree, and up to `since` can be determined from the ledger.
Similarly, a ledger can be reconstructed from a set of abutting spans that cover the relevant history of the ledger.

A set of spans, and for each ledger a "initial time" from which it starts, along with the `hash` data backing any blob variants, suffices to represent a set of evolving ledgers.
We might write single-dimensionally timestamped ledgers as:
```
-- Variants to represent data; `List(int)` is a byte array.
type Payload = Empty | Inline List(int) | Blob List(int);

-- spans   : ((id), (lower, upper, since, payload))
-- initial : ((id), (initial))
let spans   = input 0 : ((int), (int, int, int, Payload));
let initial = input 1 : ((int), (int));
```

<details>
<summary><b>Multitemporal ledgers</b>: We can generalize time beyond single integers (details).</summary>
<p>

We could generalize the `int` frontiers to antichains over pointstamps with the type `List(List(int))`.
We *should* do this, respect multitemporal times, but additionally the antichain logic must be inlined repeatedly.
Their logic would then be

```
-- time_le(a, b)     : (a, b) zip map ((x, y) -> (x, y) le) fold_all
-- below(u, t)       : time_le(u, t) and not time_le(t, u)
-- frontier_le(A, B) : every b in B has some a in A with time_le(a, b)
-- minimize(L)       : the elements of L that no element of L is strictly below
-- join(A, B)        : minimize of the coordinatewise maxima of all pairs
-- meet(A, B)        : minimize of A append B

    -- A le B
    (a_f, b_f) cap_list map ((a_f, b) ->
        (b, a_f) cap_list map ((b, a) -> (a, b) zip map ((x, y) -> (x, y) le) fold_all) fold_any) fold_all,
    -- join(A, B)
    let nested = (a_f, b_f) cap_list map ((a_f, b) ->
        (b, a_f) cap_list map ((b, a) -> (a, b) zip map ((x, y) -> (x, y) max))) flatten in
    let cross = nested.1 in
    let dominated = (cross, cross) cap_list map ((l, t) ->
        (t, l) cap_list map ((t, u) ->
            ((u, t) zip map ((x, y) -> (x, y) le) fold_all, ((t, u) zip map ((x, y) -> (x, y) le) fold_all, 0) eq) and) fold_any) in
    (dominated map (d -> (d, 0) eq), cross) zip filter,
    -- meet(A, B)
    let both = (a_f, b_f) append in
    let dominated = (both, both) cap_list map ((l, t) -> ...same as above... ) in
    (dominated map (d -> (d, 0) eq), both) zip filter
```

</details>
<p>

### Properties of Ledgers

Ledgers as above are the source of truth for the data they contain, but there are derived properties of interest.
We write those properties as computations over the ledger data, rather than independent sources of truth to keep in sync.
Updates to the ledger also update the derived quantities, in sync.

There is some pre-work to do to set first restrict our attention to "valid" spans.

```
-- WELL-FORMEDNESS: a span must have `lower` strictly before `upper`. A span
-- without this could bridge a gap in a walk, so the walk below ignores such
-- spans, and the invariant says there are none.
let malformed = spans | filter((_, (lo, hi, _, _)) -> (lo, hi) ge);
let formed = spans | filter((_, (lo, hi, _, _)) -> (lo, hi) lt);

-- Spans keyed by where they start, clamped to the initial time so that a span
-- beginning before the ledger counts from the initial time. Spans ending at or
-- before the initial time say nothing about the ledger and are dropped.
let by_lower = formed
             | join(initial, ((id), (lo, hi, _, _), (init)) -> ((id), (hi, (lo, init) max, init)))
             | filter((_, (hi, _, init)) -> (init, hi) lt)
             | map(((id), (hi, lo, _)) -> ((id, lo), (hi)));
```

#### Write frontiers

Each ledger has a "write frontier" that is the time (antichain) at which the earliest new write can occur.
It is the largest recorded `upper`, as by invariants all times not at least this belong to a span.

```
-- WRITE FRONTIER: the greatest `upper` among a ledger's spans, or its initial
-- time if it has none. `min` over negated candidates is `max`.
let write = formed
          | map(((id), (_, hi, _, _)) -> ((id), (hi)))
          + initial
          | map(((id), (t)) -> ((id), (t neg)))
          | min
          | map(((id), (w)) -> ((id), (w neg)));

-- Spans keyed by the time they end at, with where they start and their since.
let by_upper = spans | map(((id), (lo, hi, s, _)) -> ((id, hi), (lo, s)));
```

#### Readability

A ledger is "readable" in an interval `[lower, upper)` when among the spans with `since` no more than `lower` there is a path from the ledger's `initial` to `upper`.
A ledger is "live readable" if it is readable for an `upper` equal to the ledger's write frontier, for some `lower`.
A ledger's "read frontier" is the least `lower` for which it is live readable.

The readable intervals can be restricted to "maximal" intervals, of those pairs `(lower, upper)` that do not dominate each other.
We can determine these pairs by walking forward from `initial`, joining with spans that match `upper`, taxing the max of `lower` and the span's `since`, and then discarding dominated pairs.
```
-- READABILITY: walk forward from `initial`, joining each `(lower, upper)` pair
-- with the spans that start at `upper`, taking the greater of `lower` and the
-- span's `since`, and keeping the least `lower` for each `upper` reached.
-- A row `((id, upper), (lower))` says the ledger is readable in `[lower, upper)`.
let readable = rd::reached;
rd: {
    let step = reached | join(by_lower, ((id, _), (lo), (hi, s)) -> ((id, hi), ((lo, s) max)));
    set reached = initial | map(((id), (t)) -> ((id, t), (t))) + step | min;
}

-- MAXIMAL: readable intervals contained in no other readable interval.
let pairs = readable | map(((id, u), (lo)) -> ((id), (lo, u)));
let dominated = pairs
              | join(pairs, ((id), (lo, u), (lo2, u2)) -> ((id), (lo, u, lo2, u2)))
              | filter((_, (lo, u, lo2, u2)) -> (((lo2, lo) le, (u, u2) le) and, ((lo2, u2), (lo, u)) ne) and)
              | map(((id), (lo, u, _, _)) -> ((id, u), (lo)))
              | distinct;
let maximal = readable - dominated;

-- READ FRONTIER: the least `lower` readable up to the write frontier. A ledger
-- with one is "live readable"; the invariant says `stranded` is empty.
let read = readable
         | map(((id, u), (lo)) -> ((id), (u, lo)))
         | join(write, ((id), (u, lo), (w)) -> ((id), (u, lo, w)))
         | filter((_, (u, _, w)) -> (u, w) eq)
         | map(((id), (_, lo, _)) -> ((id), (lo)));
let live = read | map(((id), _) -> ((id), ()));
```

### Ledger evolution

Ledgers go through a lifecycle from creation, continual advance of write frontiers, compaction of existing spans, and eventually sealing and dropping.

A ledger is created through its introduction into a `registry`, which identifies the shape and initial time of the ledger.
No spans are required as part of this creation.

Ledgers advance their write frontier through the continual introduction of new spans.
Write frontier advancing spans have a `lower` matching the ledger's write frontier, and a new `upper` strictly greater than it.
The introduction advances the ledgers write frontier, and defines the updates that inhabit the space between the old and new write frontiers.
If a ledger was live readable before the introduction it remains so afterward, as the new `lower` matches the old write frontier.

Ledgers evolve their readability through "compaction", a process that replaces contiguous spans (one or more) with a new span whose `since` may strictly advance.
The new span should have the least `lower` and greatest `upper` of the spans it replaces, and a `since` at least those of the replaced spans.
The `batch` of the new span must accumulated identically to the replaced spans, for times at least the new span's `since`.
By choosing a large `since`, the replacement span may be able to consolidate updates, and even discard those that accumulate to zero.
The removal of spans is what weakens readability, and has the potential to permanently impair the ledger; it should have guards, which we discuss further in the compaction section.

The acts of creating compacted spans and dropping their inputs can be decoupled, the former being a valuable roll-up step and the latter being resource reclamation.

Ledgers are "sealed" by advancing their write frontier to the empty frontier: `[]`.
Ledgers are "dropped" by removing all of their spans.

---

### Operations and cohorts

Ledgers will be organized into "cohorts", and actions on ledgers will take place in the context of a cohort.
We will use two sets to represent the contents of a cohort, that being the state of multiple ledgers:
```
manifest : (id, lower, upper, since, payload: Empty | Inline(bytes) | Blob(hash))
registry : (id, shape, initial)
```

The verbs we use for ledgers, applied to a cohort, include:

1.  `create(shape, start) -> id`: from a specification of the column shapes, i.e. both data columns and the time width, and an initial time `start`, create a new collection and provide an identifier for it.
    The specific transition is that `registry` gains an `(id, shape, start)` tuple, exactly when `id` is not already present.
2.  `append(id, lower, upper, batch) -> RESULT<(), WRITE>`: for a ledger `id`, attempt to commit the span `[lower, upper)` with `since = start` and `batch` identifying the updates.
    All updates in `batch` must lie in the interval `[lower, upper)`.
    When the write frontier of `id` equals `lower` this operation should succeed, commiting the span durably and advancing the ledger's write frontier.
    When the write frontier of `id` is not `lower` this operation should fail and return the actual write frontier.
    The specific transition is that `manifest` gains a `(id, lower, upper, start, batch)` tuple, exactly when `id` is present in `registry` and `lower` equals the largest span upper of `id` already in `manifest`, or `start` if there is none.

All operations must:
1. Apply atomically in a serial order.
2. Respect real-time completion ordering: if one operation completes before another starts, the first must be applied before the second.
3. Operations should reflect the durability properties of the ledger when they return, or return a durability future that can be inspected.

## Manifests

A collection of spans is data that can be recorded in its own ledger, which we call a MANIFEST.
A manifest largely contains metadata, and can describe a substantially larger volume of changing data than it must explicitly record.

A manifest is likely associated with a transaction resolving mechanism, for example the DDP that resolves read and write sets.
Spans added to a manifest can cover many contained ledgers, and the mechanism would confirm the data maintain any stated invariants (e.g. only one new batch at each (id, upper)).
Once the span is determined, another mechanism should attempt to commit it using the parent mechanism, e.g. a root or a manifest containing it (or other mechanisms, should they exist).

At the same time, the manifest needs to record other metadata about each ledger, for example its shape.
This is a second collection, that we'll think of as paired to form a manifest.
The registry is likely write-once for each `id`, though this could be relaxed.

The `lower`, `upper`, and `since` fields are all antichains over times that are products of integers.
Structurally, each is `LIST LIST INT`, or `[[INT]]`.
The withs of the three fields must agree for a ledger.

### Register backing

A cohort can be backed by one durable register and a durable immutable blob store.
The cohort is implicit in calls to `create` and `append`.
The backing uses these operations:

```
put(bytes) -> hash
get(hash) -> bytes
read_root() -> (version, hash?)
cas_root(expected_version, hash) -> bool
```

`put` returns only once the bytes are durable, and `get` recovers the bytes by their hash.
Root reads and compare-and-swap operations are linearizable.
`cas_root` succeeds only if the current version equals `expected_version`, replacing the hash and advancing the version atomically and durably.
Versions are never reused.
An unwritten root has version zero and no hash.

For a first implementation, a state blob contains the cohort's registry and spans.
An unwritten root represents an empty registry and no spans.
The state named by the root is committed; other state blobs are proposals.
Every blob referenced by a proposed state must be durable before the root can name that state.
The write frontier of each ledger is derived from its initial time and spans as above.

#### Creation

To implement `create(shape, start)`:

1. Read the root's version and recover its state.
2. Choose a fresh ledger identifier `id`, absent from the registry.
3. Form a new state by adding `(id, shape, start)` to the registry, preserving all existing registry entries and spans.
4. Store the new state with `put`, then attempt `cas_root` with the version read in step 1.
5. On success, return `id`.
   On failure, repeat from step 1.

The successful root CAS commits the creation.
The new ledger has no spans, and its write frontier is `start`.

#### Append

To implement `append(id, lower, upper, batch)`, the ledger must exist, `lower` must be strictly before `upper`, and the batch must contain raw updates of the ledger's shape in `[lower, upper)`.

1. Read the root's version and recover its state.
2. Determine the ledger's write frontier `write`.
   If it differs from `lower`, return `Err(write)`.
3. Make the batch's payload durable if it is backed by a blob.
   An inline payload will be stored with the new state; an empty payload needs no blob.
4. Form a new state by adding `(id, lower, upper, start, batch)` to the spans, preserving all existing registry entries and spans.
5. Store the new state with `put`, then attempt `cas_root` with the version read in step 1.
6. On success, return `Ok(())`.
   On failure, repeat from step 1, rechecking the write frontier against the newly committed state.

The successful root CAS commits the append.
A failed CAS alone does not imply `Err(write)`: another operation may have changed a different ledger in the cohort.
An `Err(write)` is decided at the root read that supplied that frontier.
Payload blobs can be reused across attempts.

#### Ordering and recovery

Successful operations are ordered by their root CAS operations, with rejected appends ordered at their deciding root reads.
This order respects completion before invocation and gives the cohort strict serializability.
Separate cohorts use separate roots and can commit independently.

Recovery reads the root and recovers the state and payloads reachable from it.
A crash before publication leaves the previously committed state intact.
A crash after publication preserves the operation, even if its caller has not received the result.
Blobs written by unsuccessful attempts can remain unreferenced; their reclamation is deferred.
Storage failures and an unknown CAS outcome are distinct from a rejected append's `Err(write)`.

## Compaction

Compaction is the act of commiting new spans that cover other spans, and which advance the `since` field.
For any set of spans `Si`,
    from some `lower` contiguously to an `upper`,
    for any `since` at least all the `since` of the spans,
    one can form a new span of `lower`, `upper`, `since`, with `batch` describing the accumulated updates, with any update times advanced not past `since`.

Compaction is then the act of replacing an existing set of spans with a new span that covers them.
This action requires guards to maintain viability: one could compact [0, 1), [1, 2), and [2, 3) to the spans [0, 2) and [1, 3) but this would lose viability.
To ensure viability, one could monitor a viability view, or one could have a readset that includes spans on a viable path; it is not enough just to avoid collisions among read and write sets.

Compaction is constrained by "read holds": requirements that the collection can still be read at certain times.
Read holds can be explicit, where an interactive user wants to retain historical detail (e.g. to support external communication that can fail).
Read holds can be implicit, defined by named users who need to read as far back as any uncommitted output of their own, e.g. dataflows that produce outputs.
The `since` that compaction chooses should never exceed any read hold.

## Timelines

The times of ledgers have a type, but not yet a *meaning*.
A timeline is a set of ledgers whose times are meant to be comparable.
It is not certain what technical guarantees come with this;
such ledgers could be assumed to be self-pacing, in that once one reaches an `upper` others should be expected to reach it "soon".

## Computation

Programs define ways of producing output ledgers from input ledgers.
Classic IVM takes external inputs and produces new outputs.
The transaction resolver takes candidate transactions, which it filters into rollback/commit, and which are then removed from the input.
"Continual tasks" are dataflows that similarly accept inputs, produce outputs, and may manipulate both of them.
    Their intent is that any computation can be modeled as (input, state) -> (state', output),
    generalized perhaps to (input, state) -> (input', state'), as output seems the same as state.

## Proximal target

A client submits a transaction containing changes grouped by ledger identifier within one cohort.
Each group is a creation, an append, or a combined creation and append.
The combined form takes `shape`, `start`, `upper`, and `batch`, with `lower = since = start` implicit.
Groups must have strictly increasing identifiers, making repeated changes to one ledger structurally invalid.
The ordering of identifiers and strictness of append bounds can be checked without consulting cohort state.
The transaction commits only if every group succeeds; otherwise it leaves the cohort unchanged.
Transactions resolve in an order that respects real-time completion, and each observes the committed effects of preceding transactions.
The resolver produces conclusions that the backing publishes before reporting committed results.

The first checkpoint uses the memory backing.
Ledgers can be created and appended independently of any installed computation.
A DDIR computation can consume their committed updates and frontier progress.
The Rust implementation and the `.ddp` rules are checked against the same operation histories, comparing transaction outcomes and resulting registry and manifest state.
Those histories include disjoint transactions, competing appends, chained appends across transactions, combined creation and append, structural rejection, and rollback of tentative effects when a group fails.

The target then replaces the memory backing with a durable implementation of the same blob and root operations.
After restart, the committed cohort is recovered from its root and its referenced blobs.
The same histories and DDIR consumption checks must continue to pass with that backing.
An interrupted publication may leave its caller uncertain, but recovery must distinguish committed state from uncommitted proposals.

A subsequent implementation can use the `.ddp` resolver to commit cohort changes through a ledger containing its registry and manifest updates.
Compaction, reclamation, and deployment metadata remain later extensions of this foundation.
