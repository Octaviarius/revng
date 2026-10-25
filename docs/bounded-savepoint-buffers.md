# Bounded savepoint serialization

Savepoints previously serialized every requested object before compression
and database insertion. This retained the complete uncompressed mapping as
well as compressed output alongside the live native containers.

Savepoint writes now expose a lazy Mapping that serializes up to 128 objects
at a time. SQLite consumes it inside the existing single transaction: a
serialization failure rolls back both objects and dependencies. Cache bytes,
schema and invalidation rules are unchanged. Reads also fetch and deserialize
at most 128 objects per call. The live native container still accumulates
the requested objects; only the transient serialization buffers are bounded.

The synthetic `benchmark_savepoint_buffers.py` processes 512 one-MiB records
in separate processes. Measurements: eager 550.7 MiB peak RSS, 0.765 seconds;
streamed 165.1 MiB, 0.631 seconds. Both produced the same object count and
aggregate SHA-256 digest. This measures buffering, not whole-program memory
usage or decompilation speed.

Tests cover multi-batch round trips through memory and SQLite providers,
compressed and uncompressed records, and rollback after a later batch fails.
The main pipeline tests passed (28 cases); parallel runner tests passed
(32 cases). Deployment retains the installed cache compatibility version.

This change also mitigates transient memory peaks during the observed WSL
service crashes. It does not establish their root cause: Windows recorded
stack overflows in wslservice/wslrelay while the guest reported a machine
check. Independent host commit telemetry is required to test the memory
pressure hypothesis.
