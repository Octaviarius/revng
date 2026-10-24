# Pipeline parallelism audit (2026-10-24)

Scope: the saved king pipeline and all 48 pipes registered by native runtime
`ba8cdb0278084da07b2655f19d1e5f1ce2ebbbdf`, plus Python `import-files`.
Signatures were obtained programmatically from the installed bindings; source
review used fork `25d280e31`. This is a candidate audit, not a claim that untested
pipes are equivalent or faster. Source and installed native revisions differ.
No running analysis, installed runtime or deployed Python snapshot was changed.

## Main decompilation path

`lift -> collect-cfg -> isolate -> segregate-stack-accesses -> clift-functions
-> tagless-single-file` contains 45 pipe occurrences, 34 distinct pipe names.
Two occurrences are already enabled for processes; two more (`isolate`, `clifter`)
have separate per-function outputs; 29 occurrences modify per-function containers
in place. These counts describe pipeline structure, not fractions of wall time.

| Stage / pipes | Assessment | Work required |
| --- | --- | --- |
| collect-cfg | Enabled; 2.57x on measured 1024-function sample with 8 workers | Preserve conservative model invalidation and memory cap |
| isolate | Highest-priority separate-output candidate | Full root per worker, full model, preserve inline-function bundles; compare LLVM semantics, output ownership and dependencies |
| attach-debug-info; inline-always-inline-functions | Per-function candidates | ReadWrite worker protocol and safe module replacement |
| enforce-abi; promote-csvs | Per-function candidates | Same protocol; audit model reads and CFG access |
| pure-llvm-passes-pipe (repeated configurations) | Strong candidate for expensive configurations | Slice per-function modules; validate each pass list, not just the pipe name |
| remove-lifting-artifacts; promote-init-csv-to-undef; inject-stack-size-probes-at-callsites; promote-stack-pointer | Per-function candidates | ReadWrite protocol; keep fast transforms in process or fuse them |
| simplify-switch; segregate-stack-accesses; emit-function-pointers | Per-function candidates | Share binaries/model, partition modules, check dependency coverage |
| switch-to-statements; fix-pointer-size | Per-function candidates | Same protocol; benchmark startup versus useful work |
| clifter | Straightforward separate-output candidate | Slice Input; retain complete model; compare normalized MLIR and invalidation |
| import-function-data-model; pure-mlir-passes-pipe; verify-function-against-model; import-descriptive-function-info | Per-function candidates | ReadWrite Clift protocol; MLIR context per process; tiny passes may lose time |
| emit-c | Enabled; measured separately | Existing sliced input and strict merge |
| emit-c-as-single-file; headers; directory/archive export | Global output / reduction | Keep ordered final aggregation in parent; only split expensive preparation if profiling justifies it |

### isolate needs more than a flag

`IsolateFunctions.h` declares read-only CFG/root and write-only function modules,
so the current runner can represent its argument shape. However its constructor
serializes/clones the root, can dispose the input, and creates dynamic declarations.
`runOnFunction` outlines functions and AlwaysInline callees into one cloned module,
sharing a function map and PendingUses. The destructor runs global DCE and splits
all requested outputs into independent modules. Independent processes can contain
this state, but different batch boundaries may affect naming, metadata, helper
retention and model-read dependency ownership. Start with complete CFG broadcast;
its bundles already include inline callees, but slicing must be audited before
opting in. Exact bitcode equality is useful if available; otherwise compare
normalized IR, verify modules and examine each difference. Compilation alone is
insufficient. Test cache hits and edits, including unrelated model fields.

### Required ReadWrite runner support

The current `_output_index` deliberately rejects any writable argument whose
access is not exactly WRITE. Merely adding metadata to the other pipes fails.
Extend the protocol to send owned input modules to a worker and return their
transformed replacements. Keep unrelated modules in the parent. Require all owned
IDs exactly once, reject foreign outputs, preserve valid container state, and
replace per-output dependencies/custom invalidation records consistently. On any
failure publish neither partial modules nor partial cache state. Keep a full
model in each process; share unsliced global inputs only as serialized data.
Constructor caches and tracking make dependency checks necessary even where the
model itself is read-only. Pure LLVM/MLIR pipes declare no model reads but their
configured passes still need independent serial/parallel comparisons.

Do not run an external process for every tiny transform of every function.
Group audited transformations between savepoints into batch-local pipelines,
or use persistent workers after correctness is established. Keep the savepoints,
model changes and SQLite writes owned by the coordinator. Persistent workers
must clear module/context state so memory does not grow indefinitely. Choose
thresholds from measured time and input bytes as well as function count; the
collect-cfg 256-function trial showed startup can erase the entire gain.

## Other branches and analyses

- process-assembly, yield-assembly, yield-cfg and yield-call-graph-slice have
  per-function outputs and are additional candidates. They do not accelerate the
  requested C export unless those artifacts are also requested.
- emit-string-constants is a per-function ReadWrite candidate on cleanup-ir.
- emit-single-type-definition can partition by type ID, not function ID, after
  generalizing and validating the object-kind contract.
- lift / detect-abi use global reachability and interprocedural information;
  they need algorithm-specific decomposition, not the current function runner.
- detect-stack-size, detect-c-strings and convert-functions-to-cabi could have
  parallel local work followed by validated model-diff merging. Current runner
  keeps analyses serial. Conflicting type IDs or edits cannot simply be merged.
- DLA can potentially parallelize local constraint collection, followed by global
  graph solving/type merging. This is a separate native change; independent
  per-function outputs cannot replace its interprocedural result.
- Shared type imports, root LLVM passes, module merging, whole call-graph creation,
  compilation/linking and final single-file/header exports remain global at the
  current API boundary. Some admit map/reduce internally; they are not inherently
  impossible to parallelize, but lack a proven independent partition today.
- SQLite publication stays transactional in the parent. Optimize SQL and batch
  writes; multiple competing writers to one cache are not the next speedup.

## Recommended order

1. Audit and benchmark isolate on a saved root/CFG/model, without restarting the
   current analysis. Use 2/4/8 workers and capture memory as with collect-cfg.
2. Implement ReadWrite module replacement and test ownership, unchanged objects,
   failure rollback, dependencies and cache edits.
3. Enable expensive pure LLVM configurations, enforce-abi and stack transforms
   individually; then benchmark grouped stages between savepoints.
4. Enable clifter and expensive Clift passes. Retain sequential thresholds for
   debug/metadata transforms and final aggregation.
5. Only profile-driven work on DLA/global analyses and other artifacts afterward.

Whole-pipeline acceleration cannot be derived from counts or from collect-cfg's
2.57x result. Measure time per stage/configuration, serialization, CPU and tree
RSS, with the same input and serial baseline, before promising a total speedup.

## Complete native inventory

The table is a contract-based classification; each candidate still needs the
validation described above. Full arguments, access modes, container kinds and
pipeline references are in [the machine-readable inventory](benchmarks/pipeline-parallelism-inventory.json).

| Pipe | Classification | Pipeline occurrences |
| --- | --- | ---: |
| `attach-debug-info` | ReadWrite function candidate | 2 |
| `clifter` | separate-output function candidate | 1 |
| `collect-cfg` | enabled | 1 |
| `compile-root-module` | global/aggregate; requires algorithm-specific work | 2 |
| `emit-c` | enabled | 2 |
| `emit-c-as-directory` | global/aggregate; requires algorithm-specific work | 1 |
| `emit-c-as-single-file` | global/aggregate; requires algorithm-specific work | 2 |
| `emit-function-pointers` | ReadWrite function candidate | 1 |
| `emit-helper-header` | global/aggregate; requires algorithm-specific work | 2 |
| `emit-segment-references` | global/aggregate; requires algorithm-specific work | 1 |
| `emit-single-type-definition` | per-type candidate | 1 |
| `emit-string-constants` | ReadWrite function candidate | 1 |
| `emit-type-and-global-header` | global/aggregate; requires algorithm-specific work | 2 |
| `enforce-abi` | ReadWrite function candidate | 1 |
| `fix-pointer-size` | ReadWrite function candidate | 1 |
| `hex-dump` | global/aggregate; requires algorithm-specific work | 1 |
| `import-descriptive-function-info` | ReadWrite function candidate | 1 |
| `import-descriptive-info` | global/aggregate; requires algorithm-specific work | 3 |
| `import-function-data-model` | ReadWrite function candidate | 1 |
| `import-function-declarations` | global/aggregate; requires algorithm-specific work | 2 |
| `import-segment-declarations` | global/aggregate; requires algorithm-specific work | 2 |
| `import-types` | global/aggregate; requires algorithm-specific work | 2 |
| `inject-stack-size-probes-at-callsites` | ReadWrite function candidate | 1 |
| `inline-always-inline-functions` | ReadWrite function candidate | 1 |
| `invoke-isolated-functions` | global/aggregate; requires algorithm-specific work | 1 |
| `isolate` | separate-output function candidate | 1 |
| `lift` | global/aggregate; requires algorithm-specific work | 1 |
| `link-for-translation` | global/aggregate; requires algorithm-specific work | 2 |
| `link-support` | global/aggregate; requires algorithm-specific work | 2 |
| `merge-llvm-modules` | global/aggregate; requires algorithm-specific work | 1 |
| `process-assembly` | separate-output function candidate | 1 |
| `process-call-graph` | global/aggregate; requires algorithm-specific work | 1 |
| `promote-csvs` | ReadWrite function candidate | 2 |
| `promote-init-csv-to-undef` | ReadWrite function candidate | 1 |
| `promote-stack-pointer` | ReadWrite function candidate | 1 |
| `pure-llvm-passes-pipe` | ReadWrite function candidate | 10 |
| `pure-llvm-passes-root-pipe` | global/aggregate; requires algorithm-specific work | 4 |
| `pure-mlir-passes-pipe` | ReadWrite function candidate | 1 |
| `remove-lifting-artifacts` | ReadWrite function candidate | 1 |
| `segregate-stack-accesses` | ReadWrite function candidate | 1 |
| `simplify-switch` | ReadWrite function candidate | 1 |
| `switch-to-statements` | ReadWrite function candidate | 1 |
| `verify-against-model` | global/aggregate; requires algorithm-specific work | 0 |
| `verify-function-against-model` | ReadWrite function candidate | 1 |
| `yield-assembly` | separate-output function candidate | 1 |
| `yield-call-graph` | global/aggregate; requires algorithm-specific work | 1 |
| `yield-call-graph-slice` | separate-output function candidate | 1 |
| `yield-cfg` | separate-output function candidate | 1 |

Python `import-files` is coordinator file ingestion; keep it sequential.
