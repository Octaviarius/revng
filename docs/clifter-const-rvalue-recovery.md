# Clifter implicit casts must produce unqualified rvalues

The native game analysis aborted in PureMLIRPasses while processing function
0x63aeb1:Code_x86. The verifier rejected clift.bitcast with a top-level const
pointer typedef as its result. The crash is reproducible with the original
sequential native runtime ba8cdb0278084da07b2655f19d1e5f1ce2ebbbdf, independently
of process parallelism.

The bad operation already exists immediately after LLVM-to-Clift import. The
first MLIR pass only exposes it through recursive verification. Therefore this
is not a failure of the LLVM/MLIR optimization list or the SQLite cache.

FunctionClifter::emitImplicitBitcast used the model's target type directly.
Model declaration/parameter types can carry top-level const, while a cast result
is a value expression and Clift_BitCastOp explicitly requires a non-const result.
The fix removes the target type's top-level qualifier before identity checking
and emission. clift::removeConst preserves the pointed-to type, its qualifiers,
typedef identity and pointer width; the model and declarations are not changed.

## Verification on the deployed native version

OpenD2's tools/inspect/reproduce_clift_failure.py extracts one saved function from
savepoint 10, runs normal LLVM preparation and Clifter, then runs the MLIR list,
model verification and C emission. check_clift_recovery.py automates the original
failure, corrected split/full pipelines and an unaffected control function.
It also checks the external native worker path used by the parallel runner.
The original input fails with SIGABRT and the exact const-result diagnostic;
the corrected input emits C. Split/full C match; control C is unchanged.
The model hash remains 197de3bd9d1e5560e3cabaaee08d72c8f70139afc91bc49d928a114baf69d2e6.

The installed native binary is not a rebuild of this checkout. For that pinned
native revision, OpenD2 builds an ABI-checked LD_PRELOAD compatibility library
with matching LLVM/MLIR headers. It intercepts the exported BitCastOp value builder
and calls the native removeConst on its result type before delegating. This
normalizes the builder's rvalue result contract without weakening verification.
The game regression requires one normalization. Unaffected control emission
must stay byte-identical. A normal native rebuild uses the source fix directly;
the compatibility library is only for the existing installed runtime.

Current checkpoint has complete LLVM results at savepoint 10 but no complete
Clift savepoint. Resume must preserve model, pipeline and cache identity, skip
DLA/ABI and reuse those LLVM modules. Clifter and later stages must run again;
there is no persisted checkpoint inside the interrupted MLIR function loop.
