# Parallel functions within one project: implementation study

Status: process workers support `emit-c` and `collect-cfg`. Global analyses and
other pipes retain serial execution. See [collect-cfg measurements and cache
semantics](collect-cfg-parallelism.md). The original implementation study follows
below (source review baseline `352c75e3c`).

## Implemented interface

```sh
revng project artifact emit-c --cores 4 --parallel-batch-size 256 --debug parallel-log -o functions.tar --tar
```

Without `--cores`, the local coordinator uses all CPUs available to its process,
respecting the OS affinity mask. `--cores N` caps the number of function workers;
it does not change machine-wide affinity or the running analysis. `--cores 1`
keeps the original sequential runner. `--parallel-workers` is an alias.

Small operations remain in process. The default minimum request is 1024
functions, with at least 256 requested functions per active worker. Active
processes are further capped by the number of batches. These count-based
heuristics avoid obvious startup losses; they do not guarantee that every
eligible workload is faster. The knobs are `--parallel-min-functions` and
`--parallel-min-per-worker`; benchmarking can force them to 0 and 1. Batch size
defaults to 256. `--parallel-timeout` limits each subprocess, including startup,
to 600 seconds by default. The caller must still choose a memory-appropriate
`--cores` limit for large models.

For Python pipeboxes, `Pipe.parallel_output` declares the single function-kind
write-only argument. All readable inputs are broadcast unless their names are
explicitly listed in `parallel_inputs`. The audited `emit-c` implementation
accesses only `Input.getModule(ObjectID(Function.Entry()))`, so its `Modules`
argument is sliced by batch. The full model is always shared unchanged. Global
file inputs and unsliced containers are written once and hard-linked to worker
directories. Worker settings do not change semantic configuration IDs. A
non-destructive SQLite migration v4 corrects the custom-dependency primary key
so each output object retains its invalidation record.

Native subprocesses directly execute `pypeline-run-pipe` with the pipebox's
libraries; spawning a Python CLI for each native batch had substantial startup
cost and was removed. The coordinator alone merges output containers and returns
dependencies to the normal savepoint path. Worker crashes/timeouts cancel pending
work and kill/reap running worker process groups on Linux. Output IDs, duplicate
archive entries and dependency ownership are checked before publishing output.
Use `--debug DIR` to retain per-batch reproducer scripts, logs and inputs; otherwise
temporary worker directories are cleaned up at command exit.

The existing serial context handles global analyses, unsupported pipes and small
requests. Remote daemon computation retains its existing behavior; explicit
client `--cores >1` is rejected there because client CPU availability does not
describe server resources.

## Measured result

Native `emit-c` was checked on 1260 cached game functions from an existing,
completed test project. Its complete source model was used unchanged. The source
cache was opened read-only; workers operated on separate temporary files without
opening a project database. The active whole-game project was not used.

Three repetitions, batch size 256, four available CPUs, `nice -n 15` under WSL:

| Execution | Median seconds | Speedup against in-process serial |
| --- | ---: | ---: |
| In-process serial | 4.066 | 1.00x |
| One external worker (control) | 8.708 | 0.47x |
| Two workers | 5.371 | 0.76x |
| Four workers | 3.549 | 1.15x |

All emitted C bytes, normalized model dependencies and custom invalidation
records matched the sequential runner. Input serialization hashes stayed the
same. The emitted-container SHA-256 was
`64e8dc998649d094326b4ea758703cd22633d0e10930e1e0ca9a9102ae5a9da8`
in every run. Empty outer custom-invalidation lists are normalized to empty lists
per argument; their absence and emptiness have identical meaning.

Raw elapsed/CPU/RSS measurements are in [native-emit-c.json](benchmarks/native-emit-c.json).
Peak sampled aggregate RSS with four workers was approximately 0.77 GB. This
includes the benchmark parent and descendants; it is sampled RSS, not PSS or a
precise memory allocation peak. Four-worker times ranged from 3.470 to 3.627 s.
This is a modest emission-stage speedup, not a whole-program or DLA speedup.
Two workers lost time in this batch configuration, illustrating why thresholds
and a sequential mode are necessary.

The experiment loaded this branch's Python runner with the already installed
native revision `ba8cdb0278084da07b2655f19d1e5f1ce2ebbbdf` in a separate process;
no installed files were replaced. A full native rebuild of develop was not
performed. The changed native-facing API is Python-side pipe metadata and command
construction; the native algorithm itself is unchanged.

`benchmark_parallel_runner.py` supports native cached Clift inputs and a clearly
labelled synthetic CPU fixture. It records elapsed time, CPU time, sampled RSS,
input preservation and exact output/dependency equality, and accepts `--repeat`.
It deliberately forces thresholds off to expose both speedups and regressions.
The current validation suite passed 50 tests and 100 SQL subtests, including
whole-model invalidation after unrelated model edits and non-destructive SQLite
migration. The test suite exercises subprocess failures, aborts, timeout reaping, serial
fallback, thresholds/core caps, sliced and shared inputs, CMake registration,
CLI wiring, cache-hit reruns and invalidation after model edits. These checks
establish equivalence to serial emission, not behavioral equivalence of generated
C with the original game.

## Existing execution path

| Location | Observed behavior | Consequence |
| --- | --- | --- |
| `python/revng/pypeline/pipeline.py`, `Pipeline.schedule` | Builds tasks from the predecessor chain; explicitly anticipates a future parallel subclass | Scheduling more pipeline nodes alone does not parallelize a large per-function node |
| `python/revng/pypeline/schedule/schedule.py`, `Schedule.run` | Picks one ready task and runs it synchronously on shared working containers | Keep this ordering for the first implementation |
| `python/revng/pypeline/schedule/scheduled_task.py`, `PipeScheduledTask._run_impl` | Calls `RunnerContext.run_pipe`, then maps returned dependencies to containers/savepoints | Natural boundary to return one merged result from several workers |
| `python/revng/pypeline/runner_context.py` | Already serializes models, inputs and dependencies for external `run-pipe` execution | Reuse this protocol, with a separate worker directory per batch |
| `include/revng/PipeboxCommon/Helpers/PipeRunPipes/FunctionPipe.h` | Constructs one pipe-run instance and iterates requested functions sequentially | Per-function work exists but independence is not an explicit execution contract |
| `include/revng/PipeboxCommon/Helpers/PipeRunPipes/Helpers.h`, `ObjectDependenciesHelper` | Clears/resumes model tracking and pushes/pops a tracking stack for each object | Read-only model arguments still involve mutable tracking state |
| `include/revng/PipeboxCommon/LLVMContainer.h`, `LLVMFunctionContainer` | Modules share one `LLVMContext` and a mutable module map | A parallel loop over the existing instance is unsafe without further work |
| `include/revng/PipeboxCommon/CliftContainers.h` | Per-function modules share an `MLIRContext` and a mutable module map | Parallel MLIR support alone does not establish thread safety of the surrounding containers |
| `lib/DataLayoutAnalysis/DLA.cpp`, `AnalyzeDataLayout::run` | Builds one layout graph from requested modules, runs graph transformations, writes shared model types | Exclude this interprocedural analysis from per-function execution |

`FunctionPipe` also shares the pipe-run instance itself, which may own caches,
pass managers and references to mutable containers. Replacing its loop with
`parallelFor` would require auditing those states, model tracking, progress and
logging, not just enabling LLVM threading. Python threads around in-process
native calls provide no isolation guarantee either.

## Recommended first implementation

Add an opt-in `ParallelRunnerContext` at the `RunnerContext.run_pipe` boundary.
Use independent executable worker processes, coordinated by one parent project.
Keep a single input binary, complete model snapshot, pipeline and cache identity.
Partition **function requests**, not the binary or model into separate projects.
Workers must invoke low-level `pipeline run-pipe`, never `project analyze` or
`project artifact`; they do not open the project cache at all.

1. Add an explicit per-pipe partitioning contract, defaulting to serial. Initially
   whitelist a single audited pipe with one function-kind write-only output.
   Function-kind output alone is insufficient proof of independence.
2. Sort requested function IDs and assign small batches dynamically to a bounded
   worker pool. Assign each output object to exactly one batch.
3. Serialize the entire model once, plus required immutable inputs and file
   storage. Let `Pipe.prerequisites_for` establish the necessary readable objects.
   Conservatively broadcast all required readable inputs first; reduce them only
   when the pipe's dependency contract proves it safe. Preserve cross-function
   information and binary/root inputs.
4. Initialize each worker with the same pipebox, static/runtime configuration and
   native arguments. Reuse `RunnerContext._pipe_construct_cmd` and the pipebox's
   `argv_hook` to select the correct executable. Give every batch separate output
   and dependency files. Start with subprocess exec/spawn, not a fork of a parent
   holding SQLite connections and initialized native contexts.
5. Restrict writable output requests to that batch. Reject unknown output IDs,
   missing outputs, duplicate ownership and incorrect container kinds. Workers
   get independent native contexts, models and pipe instances.
6. Wait for successful completion and verification of all batches. Merge their
   serialized objects with `Container.deserialize`; merge dependency and custom
   invalidation records by argument index and output object. The existing
   `_pipe_load_results` replaces containers, so it cannot simply be called in a
   loop for several batches.
7. Return one combined `PipeDependencies` to `PipeScheduledTask`. Keep savepoint
   persistence, cache transactions, model edits and invalidation in the parent.
   Do not publish partial batches as a completed task. On error, stop remaining
   workers and preserve the parent's existing cache and containers.
8. Expose execution settings such as worker count and batch size separately from
   semantic pipe configuration. Changing only worker count should not invalidate
   otherwise identical cache entries.

This is process parallelism inside one project. It does not require isolated
function binaries or discard the common call graph/type model. Initially it
adds serialization/startup overhead; batches amortize it. Persistent workers can
follow after the protocol and recovery behavior have been validated.

## Candidate rollout

1. `emit-c` (`include/revng/CliftPipes/EmitC.h`, `lib/CliftPipes/EmitC.cpp`): reads
   per-function Clift modules, emits per-function PTML C. Audit emission globals
   and common inputs; best initial candidate for process isolation.
2. `clifter` (`include/revng/CliftPipes/Clifter.h`): LLVM function modules to Clift
   function modules, using the full model. Audit prerequisites and constructor
   behavior before enabling it.
3. Audited LLVM function pipes using `LLVMFunctionMixin`: extend to read/write
   arguments only after defining how each batch receives and returns exactly its
   owned objects while retaining all required read-only inputs.
4. Keep binary-wide pipes, DLA, ABI discovery and global model updates serial.
   `emit-c-as-single-file` remains a final ordered aggregation after per-function
   emission. A separate project is needed to parallelize DLA graph work safely;
   this design does not speed up its global graph transformations.

The current fork has newer Clift-based emission than the separately installed
game-analysis runtime. Stage names and native APIs must be verified when building
this fork; do not assume the running installation can load the new native code.

## Validation and performance gates

- Test empty requests, fewer functions than workers, uneven function costs,
  deterministic batching, multiple outputs, shared read-only root inputs,
  custom invalidations and serial fallback for unsupported pipes.
- Test worker exceptions, native aborts, timeout, interrupted batches, duplicate
  and missing IDs; no partial cache publication and no orphan workers.
- Compare worker counts 1/2/4 on a separate small project. Compare serialized
  function outputs byte for byte where deterministic, otherwise normalized IR
  and verified module semantics. Verify no function, dependency or custom
  invalidation is lost; test model edits and downstream cache invalidation.
- Compare merged model/type references and resulting C; compilation alone is
  insufficient proof of equivalence. In particular, splitting constructor work
  must not lose model-read dependencies common to every output.
- Record elapsed time, aggregate CPU, parent/worker peak RSS, serialized bytes,
  cache size and cache-hit reruns. First establish a break-even batch size.
- Bound workers by measured memory as well as available CPUs. With a 48 GiB WSL
  budget, subtract the parent, OS and safety reserve, then divide the remainder
  by measured worker peak RSS. Do not default to 24 workers or assume linear
  speedup. While another analysis is running, defer concurrent native benchmarks.

Implementation order: partition contract and mock-pipe tests; isolated subprocess
runner; strict deterministic merge and failure tests; one-pipe opt-in; native
comparison/benchmark on a separate project; then CLI exposure and more pipes.
The initial implementation and its measured limits are documented above.
