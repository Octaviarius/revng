# Parallel functions within one project: implementation study

Status: design only; no parallel execution is enabled by this branch. Source
review baseline: `352c75e3c`. The SQLite optimization is implemented separately.
Neither the installed runtime nor the running analysis has been changed.

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
No native parallel speedup has been measured or claimed by this study.
