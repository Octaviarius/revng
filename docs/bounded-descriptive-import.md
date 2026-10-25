# Bounded descriptive imports

The native descriptive-function-info pipe accumulated model-read dependencies
for every requested function before converting the entire result to Python.
On a 12,970-function project this exhausted 32 GiB of RAM and 16 GiB of swap.
The kernel OOM report, rather than a native assertion, identified this failure.

Only this pipe opts into serial batches of 64 functions. Each call modifies
the same container and returns bounded native dependency data. The operations
and order within each function are unchanged. Other pipes keep their existing
execution policy, including parallel C emission.

Within a batch, model-path strings are interned in a local pool. Paths shared
by every function are compressed once. Each function keeps a reference to
that common block and a compressed set of its additional paths. The original
per-function read sets are preserved exactly; unrelated model edits do not
invalidate those functions.

Storage materializes a self-contained versioned blob for each object using
the existing custom_dependencies table. No schema migration or cache deletion
is needed. On invalidation, the common block is matched once per unique block
and individual paths are checked only when necessary. The same runtime or a
newer compatible runtime must interpret these records; reverting to a runtime
without this invalidation handler is unsupported.

Saving remains atomic with the existing SQLite transaction. A failure before
the next savepoint leaves the preceding complete checkpoint usable. The
in-memory module container itself is not made smaller by this change.
