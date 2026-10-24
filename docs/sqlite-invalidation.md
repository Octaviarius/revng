# Indexed SQLite cache invalidation

The local provider previously joined objects with changed dependencies on
configuration alone and then filtered by object-ID and savepoint ranges. SQLite
could scan the objects table and repeatedly inspect most dependencies with the
same configuration. Dependency deletion could similarly use only the first
column of its primary key, repeating a large scan for each invalidated owner.

The replacement uses temporary metadata tables; it does not copy object payloads
or change the persistent schema:

1. Copy object row IDs, keys and savepoints into an indexed temporary table.
2. Materialize the distinct changed dependencies, including explicit invalidations.
3. Drive indexed object-range searches from dependencies with `CROSS JOIN`.
   Handle root ancestors separately, avoiding the original `OR` join predicate.
4. Deduplicate invalidated row IDs using an integer primary key.
5. Build a temporary composite owner key and scan dependencies once, probing that
   key to remove dependencies of invalidated objects.
6. Delete objects with the existing `DELETE ... RETURNING` query and discard the
   temporary tables inside the existing provider transaction.

The original matching rules are preserved, including cross-container matching
during object selection and container matching during dependency removal. This
patch does not introduce concurrent writers or alter pipeline/cache identifiers.
The dependency cleanup still costs a scan of the dependency table; this is a
tradeoff against repeated scans and should be measured for tiny invalidations too.

## Validation

`tests/pypeline/python/test_sqlite_invalidation.py` compares exact removed objects
and remaining tables with a frozen copy of the original SQL over 100 seeded
fixtures. Cases include empty inputs, duplicate model paths, explicit object
invalidations, roots, children, configurations, containers, inclusive savepoint
boundaries and binary object IDs. An additional targeted test checks the
cross-container, root and boundary behavior. Temporary-table cleanup is checked.

Run without native bindings:

```sh
python tests/pypeline/python/test_sqlite_invalidation.py
python tests/pypeline/python/test_sqlite_invalidation.py --benchmark
```

The synthetic benchmark has 77,820 objects (12,970 functions at six savepoints)
and 12,970 dependencies. On this machine, SQLite 3.53.2 under WSL completed the
new route in 0.658 seconds; the original route was interrupted at its five-second
budget. These numbers do not measure a full game database or end-to-end analysis.
Windows SQLite 3.50.4 completed the new route in 0.282 seconds with the same
five-second interruption for the original route.

The existing `test_all.py` suite plus these tests passed under the bundled WSL
Python runtime: 25 tests, 100 subtests. The source tree needs CMake's generated
copy of `support/buffered_reader.py` at `pypeline/utils/buffered_reader.py`; a
separate staged Python tree was used for this test, without changing the installed
runtime. CTest registers the standalone SQL regression test as well.

The running game analysis, installed rev.ng files, model and cache were not
modified by this fork's changes or tests. Candidate-model checkpointing and the
15-minute SQL deadline in the game-specific recovery wrapper are separate from
this provider optimization; they have not been added as generic fork APIs.
