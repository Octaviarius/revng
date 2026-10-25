# Clift verification and recovery of completed batches

The class recursion check in `ModuleVerifier` revisited shared containment
subgraphs for every path. A live project remained in the first Clifter result
archive for more than an hour; sampled stacks contained 63–65 nested classes.
Cache successful checks by `(Type, RootType)` for the lifetime of one verifier.
Keep the original recursion diagnostics and checks for distinct definitions.

`union-with-shared-value-tree.mlir` contains 35 union levels with two shared
value fields per level. With the pinned installed native runtime, the original
verifier exceeded 12 seconds; the memoized verifier completed in 0.785 seconds
including process startup. The first real function (`0x401000`, 525849-byte
bytecode) exceeded 20 seconds originally; patched loading and explicit
verification took 0.275 seconds, excluding startup. Thirteen existing type
fixtures produced identical output, exit status and diagnostics with both
verifiers. These are targeted measurements, not a whole-program speedup.

For pipes with whole-model invalidation, the parallel runner no longer parses
the unused `dependencies.yml`. The observed first archive contained about
1.28 GB of dependency data. Custom invalidation entries remain checked; other
pipes retain their existing dependency handling.

`REVNG_PARALLEL_RESUME_DIR` optionally names a trusted saved runner directory
containing numbered pipe and batch directories. Completed Clifter batches can
be reused only when the worker log contains the completion marker and all
model, configuration, input and auxiliary file hashes match. Normal output
ownership and verification checks still run. Reuse is explicit because the
operator must also ensure compatibility of the native Clifter implementation.
Keep the saved results and new run on the same filesystem for hard links.
Per-batch loading and merge messages expose progress of this serial phase.

The deployed native compatibility wrapper is built from the exact installed
revision, not from incompatible generated headers. It replaces the dialect's
attribute verifier with the memoized implementation and retains all other
checks. It is loaded at process startup; running native code is not patched.
