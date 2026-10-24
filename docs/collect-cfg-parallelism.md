# Parallel collect-cfg

`collect-cfg` opts into the existing isolated-process runner. Each worker loads
an independent copy of the complete LLVM root module and complete model; only
the requested output function IDs are partitioned. The native CFG algorithm is
unchanged. Worker outputs are merged only after all batches succeed. Global
analyses remain sequential.

## Cache correctness

The serial native implementation mutates LLVM helper state despite declaring a
read-only root input. Comparisons therefore create fresh input containers and
models for every case. The native function-summary oracle also caches model
reads: their fine-grained dependency ownership changes with batch boundaries.
Exact CFG equality alone is insufficient to establish cache correctness.

For parallel CFG outputs, the coordinator stores a custom invalidation marker
for every function. Any nonempty model diff invalidates all marked CFG outputs.
This is deliberately conservative: model edits can require more recomputation
than serial fine-grained invalidation. An unchanged model still reuses cached
outputs. Integration tests include a model edit unrelated to recorded native
paths and verify recomputation of all marked functions.

SQLite migration v4 fixes the primary key of `custom_dependencies` to include
argument index and object ID. The old key allowed only one custom record per
pipe/configuration. The migration retains existing objects, ordinary dependencies
and all surviving custom records; it cannot recover records previously lost.
An isolated Python overlay discovers its own migrations without relying on the
installed wheel's file listing. Restore the saved database and model together
when rolling back to a runtime without these invalidation semantics.

## Measurement method

The benchmark uses a read-only backup of the game project after completed DLA
and C ABI analyses. Function IDs are sampled with seed 0 from the full model.
Every execution uses the same selected functions, root module and model.
No native files or live project database are modified by benchmarks.

Elapsed time includes worker preparation, serialization, process startup, native
work and merging. It excludes benchmark-only setup and verification. Memory is
the peak sampled sum of parent and descendant RSS at 100 ms intervals, in GiB;
shared mappings can be counted more than once. Existing background analysis is
outside this process tree. These are single trials, not confidence intervals.
The production workload and uneven function costs can change speedups.
`--batch-size 0` in the benchmark selects one balanced batch per worker. Ordinary
CLI batch size remains 256; use a larger batch to amortize full-root reloads.

The small 256-function sample showed essentially no speedup (1.02x / 1.04x /
0.99x for 2 / 4 / 8 processes). Full-root startup dominates small requests; the
CLI retains thresholds and a sequential fallback.

## Validation

50 Python tests and 100 SQL subtests passed, covering worker failures, timeouts,
output ownership, serial fallback, shared root inputs, cache hits, conservative
model invalidation and non-destructive SQLite migration. Native benchmark
results below compare serialized CFG bytes exactly. Fine-grained native path
dependencies are reported separately and need not match; effective invalidation
is checked through the whole-model callback.

## Native result: 1024 functions

| Processes | Seconds | Speedup | Peak tree RSS, GiB | Exact CFG match |
| --- | ---: | ---: | ---: | --- |
| Serial | 294.201 | 1.000x | 2.94 | Yes |
| 2 | 182.180 | 1.615x | 7.17 | Yes |
| 4 | 132.781 | 2.216x | 11.07 | Yes |
| 8 | 114.449 | 2.571x | 19.10 | Yes |

All four output hashes are
`d612ee0d508315fd4c49da946a6a229ec27b6ae061b42b5c4b4c497235a30697`.
Eight processes improved wall time by 2.57x while consuming substantially more
CPU and memory. Four processes achieved 2.22x at about 11 GiB. These measurements
cover collect-cfg only, not full decompilation or equivalence with game behavior.

Raw reports: [1024 functions](benchmarks/collect-cfg-1024.json),
[256 functions](benchmarks/collect-cfg-256.json). The larger run used committed
runner `f8a5208e5` and native revision
`ba8cdb0278084da07b2655f19d1e5f1ce2ebbbdf`; the smaller run records its earlier
base commit and an explicit note identifying the then-uncommitted feature.

For the large game request, use `--cores 8 --parallel-batch-size 1536
--parallel-timeout 1800` to amortize full-module startup across roughly eight
batches. This batch size is an operational choice for approximately 12245
remaining functions, not another measured benchmark configuration.
