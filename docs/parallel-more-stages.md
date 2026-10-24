# More per-function process stages

The isolated runner now supports one audited ReadWrite function container as
well as a separate write-only output. It slices owned modules into each worker,
requires exactly the requested outputs, preserves all unrequested parent objects,
and only replaces the parent container after all worker output and dependency
checks pass. Worker crashes, missing/foreign objects and timeouts publish no
partial changes. Source input modules remain unchanged in the coordinator.

Native opt-in is centralized in `python/revng/pypeline/native_parallel.py`:

- isolate: complete CFG/root/model per process, disjoint function outputs;
  at least two workers and a minimum batch size of 1024 in ordinary operation.
- clifter: sliced LLVM input modules, complete model; at least four workers
  and a minimum batch size of 1024. Two workers lost time in the measurement.
- collect-cfg and emit-c retain their previous behavior.
- pure-llvm-passes-pipe supports ReadWrite execution for one exact audited
  optimization list through `configure_native_parallel_pipes(...,
  experimental_llvm=True)`. It is not enabled in the ordinary CLI: measured
  subprocess overhead exceeded its useful work. Other lists stay sequential.

Minimum batch sizes can increase the requested batch size. Worker limits still
respect --cores and available CPUs; per-pipe minimum workers select sequential
execution if the effective worker count is too small. Existing global count
thresholds and small-work fallback continue to apply. These are measured heuristics,
not guarantees for every program. No model analyses or SQLite writes run in workers.

## Cache behavior

isolate and clifter use whole-model custom invalidation, as collect-cfg does.
Native model-read ownership can vary with constructor/cache reuse across batches;
parallel outputs invalidate on every nonempty model edit. Unchanged models retain
cache hits. This sacrifices selective invalidation to avoid stale results.
ReadWrite fixture integration verifies that after a model edit the original
pre-transform savepoint is restored and the transformation is applied once.

## Native comparison

All experiments read saved cache/model inputs. The currently running game analysis,
installed native libraries and its deployed Python snapshot remain unchanged.
The new code takes effect only in a future deployment/restart.

Clifter outputs are compared byte-for-byte. LLVM bitcode is not byte-stable across
independent contexts: identified struct suffixes, declaration positions and
metadata numbering can differ. Comparison verifies the unmodified modules with
the native LLVM verifier, then compares LLVM-printed text of comparison-only
copies. A small adapter uses LLVM's own symbol-list sort; LLVM strip-nondebug
removes local symbol naming differences. The heavy optimization trial additionally
runs standard instcombine/reassociate/gvn before comparison, because independent
optimizations can schedule equivalent expressions differently. The normalization
is never applied to published worker outputs. Attributes, types, metadata and
instructions remain in the comparison except what the stated LLVM passes change.

Negative controls verify that changed constants, arithmetic operations and the
order of side-effecting calls are detected. These comparisons establish agreement
with serial rev.ng at this IR boundary, not behavioral equivalence with king.exe.

The comparator uses the installed LLVM 16 C API for parsing, passes and printing;
the tiny C++ adapter only sorts module symbol lists. It compiles once into a
source-hashed temporary directory with the matching installed clang++/headers.
Tests also compare the C API result with the stock LLVM tools.

Elapsed timings include runner preparation, worker startup, computation and
merging, but exclude benchmark-only preparation of earlier pipeline stages and
comparison. Memory is sampled aggregate process-tree RSS (shared mappings may
be counted multiple times), not PSS. Trials ran alongside the existing analysis
and some independent validation jobs, so treat speedups as single-trial estimates.
A previously timed serial isolate reference was reused after comparison was
strengthened; its stored modules, model hash, selected IDs and cache savepoints
are checked by --resume. No repeat of the expensive serial isolation was needed.

## Results

| Stage | Functions | Serial, s | 2 workers, s | 4 workers, s | Best measured speedup |
| --- | ---: | ---: | ---: | ---: | ---: |
| isolate | 512 | 143.807 | 128.425 | 154.172 | 1.12x (2 workers) |
| clifter | 4096 | 227.615 | 249.318 | 197.821 | 1.15x (4 workers) |
| LLVM optimization list | 1024 | 3.657 | 8.271 | 6.448 | serial is faster |

| Stage | Serial RSS, GiB | 2 workers RSS, GiB | 4 workers RSS, GiB |
| --- | ---: | ---: | ---: |

| isolate-512 | 3.34 | 7.85 | 13.78 |
| clifter-4096 | 1.96 | 3.13 | 4.25 |
| llvm-optimization-1024 | 0.70 | 1.53 | 2.25 |

The isolate experiment deliberately forced the existing thresholds off to expose
startup losses. Production requires larger batches; the 512-function experiment
is not a benchmark of the entire 12970-function project or of its tuned batch size.
Do not extrapolate these speedups to all stages. Full-root loading/cloning and
coordinator serialization/merging constrain scaling; more processes are not
uniformly faster. A future optimization can reduce repeated parsing during merge.

Clifter matched serialized output exactly in every mode. Isolate matched after
symbol-list ordering and strip-nondebug normalization, with no optimization needed.
The LLVM optimization list matched after the stated additional LLVM passes; it
remains disabled automatically due to the slowdown. All model-aware parallel
outputs passed whole-model invalidation coverage checks; native fine-grained path
sets can differ and are reported honestly in the JSON.

Raw reports: [isolate](benchmarks/isolate-512.json),
[clifter](benchmarks/clifter-4096.json),
[LLVM optimization](benchmarks/llvm-optimization-1024.json).
Runner commit fields identify the then-current checkout; these were working-tree
measurements during implementation. The report notes distinguish this from a
clean-tree benchmark claim. The core runner was committed as a8925156a before
final threshold/comparison refinements.

## Validation and deployment

The broad suite passed 64 tests and 100 SQL subtests. After the final worker/batch
profiles and LLVM comparator refinements, all 37 focused runner/comparator tests
passed again. They include actual payload transformation, unrequested-object
preservation, mutation followed by worker failure, missing/foreign outputs,
cache-hit reuse, original-input restoration after a model edit, measured-profile
fallback, configuration allowlists and LLVM comparison negative controls.
Native binding smoke checks resolved isolate output index 2 and clifter index 1;
automatic pure LLVM opt-in remained disabled. CMake packages native_parallel.py.
No production restart or cache migration was performed for this change.
