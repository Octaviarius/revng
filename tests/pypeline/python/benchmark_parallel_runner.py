#
# This file is distributed under the MIT License. See LICENSE.md for details.
#

import argparse
import hashlib
import json
import math
import resource
import random
import sqlite3
import statistics
import subprocess
import sys
import time
from pathlib import Path
from threading import Event, Thread

import psutil


def digest(containers):
    value = hashlib.sha256()
    for container in containers:
        for obj, data in sorted(
            container.serialize(container.objects()).items(), key=lambda item: item[0].serialize()
        ):
            value.update(obj.serialize().encode())
            value.update(bytes(data))
    return value.hexdigest()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--workers", type=int, nargs="+", default=[1, 2, 4])
    parser.add_argument("--batch-size", type=int, default=256)
    parser.add_argument("--functions", type=int, default=64)
    parser.add_argument("--rounds", type=int, default=1000000)
    parser.add_argument("--repeat", type=int, default=1)
    parser.add_argument("--native-overlay", type=Path)
    parser.add_argument(
        "--pipe",
        choices=["emit-c", "collect-cfg", "isolate", "clifter", "pure-llvm-passes-pipe"],
        default="emit-c",
    )
    parser.add_argument("--model", type=Path)
    parser.add_argument("--cache", type=Path)
    parser.add_argument("--input-savepoint", type=int)
    parser.add_argument("--static-configuration", type=Path)
    parser.add_argument("--prepare-pipeline", type=Path)
    parser.add_argument(
        "--resume", action="store_true", help="Reuse an already measured serial reference"
    )
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=args.resume)
    metadata = {
        "runner_commit": subprocess.check_output(
            ["git", "rev-parse", "HEAD"], cwd=Path(__file__).resolve().parents[3], text=True
        ).strip()
    }
    if args.native_overlay:
        import revng

        revng.__path__.insert(0, str(args.native_overlay))
        from revng.internal import pipebox
        from revng.pypeline import initialize_pypeline
        from revng.pypeline.model import Model, ReadOnlyModel
        from revng.pypeline.object import ObjectID, ObjectSet
        from revng.pypeline.utils.registry import get_singleton
        from revng.pypeline.storage.file_provider import FileProvider
        from revng.pypeline.task.task import TaskArgumentAccess

        class NoFiles(FileProvider):
            def get_files(self, requests):
                assert not requests
                return {}

        pipebox.initialize([])
        initialize_pypeline()
        pipe_type = pipebox._native_pipes[args.pipe]
        signature = pipe_type.signature()
        output_index = next(
            i for i, arg in enumerate(signature) if arg.access & TaskArgumentAccess.WRITE
        )
        output_type = signature[output_index].container_type
        pipe_type.parallel_output = signature[output_index].name
        pipe_type.parallel_in_place = (
            signature[output_index].access == TaskArgumentAccess.READ_WRITE
        )
        pipe_type.parallel_inputs = {"emit-c": ("Modules",), "clifter": ("Input",)}.get(
            args.pipe, ()
        )
        pipe_type.parallel_invalidate_on_model_change = args.pipe in (
            "collect-cfg",
            "isolate",
            "clifter",
        )
        static_configuration = (
            args.static_configuration.read_text() if args.static_configuration else ""
        )
        object_type = get_singleton(ObjectID)
        container_ids = {
            "LLVMRootContainer": "llvm-root",
            "CFGMap": "cfg-map",
            "LLVMFunctionContainer": "llvm-functions",
            "CliftFunctionContainer": "clift-functions",
        }
        data_by_index = {}
        source_savepoints = {}
        with sqlite3.connect(f"file:{args.cache}?mode=ro", uri=True) as db:
            model_bytes = (
                args.model.read_bytes()
                if args.model
                else bytes(
                    db.execute("SELECT last_seen_model FROM project WHERE id=0").fetchone()[0]
                )
            )
            for i, arg in enumerate(signature):
                if not arg.access & TaskArgumentAccess.READ:
                    continue
                cid = container_ids[arg.container_type.__name__]
                savepoint = (
                    args.input_savepoint
                    if args.input_savepoint is not None and arg.container_type.kind.rank() == 1
                    else db.execute(
                        "SELECT max(savepoint_id) FROM objects WHERE container_id=?", (cid,)
                    ).fetchone()[0]
                )
                ids = [
                    row[0]
                    for row in db.execute(
                        "SELECT object_id FROM objects WHERE container_id=? AND savepoint_id=? ORDER BY object_id",
                        (cid, savepoint),
                    )
                ]
                if arg.container_type.__name__ in (
                    "LLVMFunctionContainer",
                    "CliftFunctionContainer",
                ):
                    ids = random.Random(0).sample(ids, min(args.functions, len(ids)))
                if not ids:
                    raise ValueError(f"No {cid} objects at savepoint {savepoint}")
                rows = [
                    db.execute(
                        "SELECT object_id,content FROM objects WHERE container_id=? AND savepoint_id=? AND object_id=?",
                        (cid, savepoint, obj),
                    ).fetchone()
                    for obj in ids
                ]
                compression = arg.container_type.get_compression()
                data_by_index[i] = {
                    object_type.from_bytes(obj): (
                        compression.decompress(content) if compression else content
                    )
                    for obj, content in rows
                }
                source_savepoints[cid] = savepoint
        model = ReadOnlyModel(get_singleton(Model).deserialize(model_bytes)[0])
        candidates = set(model.all_objects(output_type.kind))
        for i, data in data_by_index.items():
            if signature[i].container_type.kind == output_type.kind:
                candidates.intersection_update(data)
        selected = random.Random(0).sample(
            sorted(candidates, key=lambda obj: obj.serialize()),
            min(args.functions, len(candidates)),
        )
        requested = ObjectSet(output_type.kind, set(selected))
        if not requested:
            raise ValueError("No matching requested functions")
        if args.prepare_pipeline:
            if args.pipe not in ("clifter", "pure-llvm-passes-pipe"):
                raise ValueError("Unsupported preparation target")
            import yaml
            from revng.pypeline.runner_context import RunnerContext

            prepared = signature[0].container_type()
            prepared.deserialize(data_by_index[0])
            began_preparation = time.monotonic()
            preparation = []
            branch = "clift-functions" if args.pipe == "clifter" else "segregate-stack-accesses"
            extras = {}
            matched = False
            for task in yaml.safe_load(args.prepare_pipeline.read_text())["branches"][branch][
                "tasks"
            ]:
                if "pipe" not in task:
                    continue
                name = task["pipe"]
                same_configuration = task.get("configuration", {}) == (
                    yaml.safe_load(static_configuration) or {}
                )
                if name == args.pipe and (args.pipe == "clifter" or same_configuration):
                    matched = True
                    break
                config = yaml.safe_dump(task["configuration"]) if "configuration" in task else ""
                stage = pipebox._native_pipes[name](config)
                stage_containers = []
                for arg in stage.signature():
                    if arg.container_type is signature[0].container_type:
                        stage_containers.append(prepared)
                    else:
                        if arg.container_type not in extras:
                            container = arg.container_type()
                            cid = container_ids[arg.container_type.__name__]
                            with sqlite3.connect(f"file:{args.cache}?mode=ro", uri=True) as db:
                                sp = db.execute(
                                    "SELECT max(savepoint_id) FROM objects WHERE container_id=?",
                                    (cid,),
                                ).fetchone()[0]
                                values = db.execute(
                                    "SELECT object_id,content FROM objects WHERE container_id=? AND savepoint_id=?",
                                    (cid, sp),
                                ).fetchall()
                            compression = arg.container_type.get_compression()
                            container.deserialize(
                                {
                                    object_type.from_bytes(obj): (
                                        compression.decompress(data) if compression else data
                                    )
                                    for obj, data in values
                                }
                            )
                            extras[arg.container_type] = container
                        stage_containers.append(extras[arg.container_type])
                RunnerContext().run_pipe(
                    pipe=stage,
                    file_provider=NoFiles(),
                    model=model,
                    containers=stage_containers,
                    incoming=[c.objects() for c in stage_containers],
                    outgoing=[
                        (
                            requested
                            if arg.access & TaskArgumentAccess.WRITE
                            else ObjectSet(arg.container_type.kind)
                        )
                        for arg in stage.signature()
                    ],
                    configuration="",
                )
                preparation.append(name)
            if not matched:
                raise ValueError("Preparation did not find requested stage/configuration")
            data_by_index[0] = prepared.serialize(prepared.objects())
            metadata.update(
                preparation=preparation, preparation_seconds=time.monotonic() - began_preparation
            )
        from revng.support import get_root

        metadata.update(
            model_sha256=hashlib.sha256(model_bytes).hexdigest(),
            native_revision=(get_root() / "share/revng/component-hashes/revng").read_text().strip(),
            input_savepoints=source_savepoints,
            static_configuration=static_configuration,
        )

        def arguments():
            containers = [arg.container_type() for arg in signature]
            for i, data in data_by_index.items():
                containers[i].deserialize(data)
            return dict(
                pipe=pipe_type(static_configuration),
                file_provider=NoFiles(),
                model=ReadOnlyModel(get_singleton(Model).deserialize(model_bytes)[0]),
                containers=containers,
                incoming=[c.objects() for c in containers],
                outgoing=[
                    requested if i == output_index else ObjectSet(arg.container_type.kind)
                    for i, arg in enumerate(signature)
                ],
                configuration="",
            )

        def command_hook(command):
            from revng.pypeline.parallel_runner import native_pipe_command
            from revng.support import get_root

            return native_pipe_command(
                command, get_root() / "libexec/revng/pypeline-run-pipe", pipebox._native_libraries
            )

        scope = f"native {args.pipe}, cached inputs, complete unmodified source model"
    else:
        from test_parallel_runner import arguments as fixture_arguments, command_hook

        def arguments():
            return fixture_arguments(args.functions, {"rounds": args.rounds})

        output_index = 2
        scope = "synthetic CPU-bound per-function fixture, not native decompilation"
    from revng.pypeline.container import ContainerFormat
    from revng.pypeline.parallel_runner import ParallelRunnerContext
    from revng.pypeline.runner_context import RunnerContext
    from revng.pypeline.task.pipe import WHOLE_MODEL_INVALIDATION

    llvm_text = None
    if args.native_overlay and output_type.__name__ == "LLVMFunctionContainer":
        from revng.support import get_root
        from llvm_compare import LLVMText

        passes = (
            b"strip-nondebug,function(instcombine,reassociate,gvn)"
            if args.pipe == "pure-llvm-passes-pipe"
            else b"strip-nondebug"
        )
        llvm_text = LLVMText(get_root() / "lib64/llvm/llvm/lib", passes)
        metadata["llvm_comparison_passes"] = passes.decode()

    def output_digest(container):
        if llvm_text is None:
            return digest([container])
        if not container.verify():
            raise ValueError("LLVM output verification failed")
        value = hashlib.sha256()
        for obj, data in sorted(
            container.serialize(container.objects()).items(), key=lambda item: item[0].serialize()
        ):
            value.update(obj.serialize().encode())
            value.update(llvm_text(bytes(data)))
        return value.hexdigest()

    def normalized(result):
        dependencies = list(result.dependencies)
        size = len(pipe_type.signature()) if args.native_overlay else 3
        dependencies.extend([] for _ in range(size - len(dependencies)))
        custom = list(result.custom_invalidation)
        custom.extend([] for _ in range(size - len(custom)))
        return (
            [sorted((obj.serialize(), path) for obj, path in entries) for entries in dependencies],
            [
                sorted(
                    (obj.serialize(), bytes(data))
                    for obj, data in entries
                    if bytes(data) != WHOLE_MODEL_INVALIDATION
                )
                for entries in custom
            ],
        )

    report = {"scope": scope, "batch_size": args.batch_size, "runs": [], **metadata}
    expected = None
    if args.resume:
        saved = json.loads((args.output / "report.json").read_text())
        if len(saved["runs"]) != 1 or saved["runs"][0]["workers"] != 0:
            raise ValueError("Resume requires exactly one completed serial reference")
        for field in (
            "model_sha256",
            "native_revision",
            "input_savepoints",
            "static_configuration",
        ):
            if saved.get(field) != metadata.get(field):
                raise ValueError(f"Resume input mismatch: {field}")
        if saved["runs"][0]["functions"] != len(requested):
            raise ValueError("Resume function count mismatch")
        reference = output_type.from_file(args.output / "serial-output.tar")
        if set(reference.objects()) != set(requested):
            raise ValueError("Resume selected function IDs mismatch")
        reference_data = args.output / "reference-dependencies.json"
        if reference_data.exists():
            reference_dependencies = json.loads(reference_data.read_text())
        else:
            mismatch = json.loads((args.output / "mismatch.json").read_text())
            if not mismatch["custom_equal"]:
                raise ValueError("Cannot recover custom dependency reference")
            reference_dependencies = [mismatch["expected_dependencies"], [[] for _ in signature]]
        expected = (
            output_digest(reference),
            (
                [[tuple(item) for item in entries] for entries in reference_dependencies[0]],
                [
                    [(obj, bytes.fromhex(data)) for obj, data in entries]
                    for entries in reference_dependencies[1]
                ],
            ),
        )
        baseline = saved["runs"][0]
        baseline.update(
            output_sha256=expected[0], comparison="normalized LLVM text with native LLVM verifier"
        )
        report["runs"].append(baseline)
        report["serial_reference_reused"] = True
        report["serial_reference_original_commit"] = saved.get("runner_commit")
        print("Reusing previously measured serial reference", flush=True)
    for repeat, workers in (
        (repeat, workers) for repeat in range(args.repeat) for workers in [0, *args.workers]
    ):
        if args.resume and repeat == 0 and workers == 0:
            continue
        call = arguments()
        batch_size = args.batch_size or math.ceil(
            len(call["outgoing"][output_index]) / max(1, workers)
        )
        initial = digest(call["containers"][:output_index])
        runner = (
            RunnerContext()
            if workers == 0
            else ParallelRunnerContext(
                args.output
                / (f"attempt-{time.time_ns()}" if args.resume else ".")
                / f"repeat-{repeat}-workers-{workers}",
                command_hook,
                workers,
                batch_size,
            )
        )
        if workers:
            runner.min_functions = 0
            runner.min_functions_per_worker = 1
        done = Event()
        peak = [psutil.Process().memory_info().rss]

        def sample():
            parent = psutil.Process()
            while not done.wait(0.1):
                try:
                    rss = sum(
                        p.memory_info().rss
                        for p in [parent, *parent.children(recursive=True)]
                        if p.is_running()
                    )
                    peak[0] = max(peak[0], rss)
                except psutil.Error:
                    pass

        thread = Thread(target=sample)
        thread.start()
        cpu = resource.getrusage(resource.RUSAGE_CHILDREN)
        cpu_before = time.process_time() + cpu.ru_utime + cpu.ru_stime
        began = time.monotonic()
        try:
            result = runner.run_pipe(**call)
        finally:
            done.set()
            thread.join()
        seconds = time.monotonic() - began
        cpu = resource.getrusage(resource.RUSAGE_CHILDREN)
        cpu_seconds = time.process_time() + cpu.ru_utime + cpu.ru_stime - cpu_before
        actual = (output_digest(call["containers"][output_index]), normalized(result))
        if expected is None:
            expected = actual
            (args.output / "reference-dependencies.json").write_text(
                json.dumps(
                    [
                        actual[1][0],
                        [[(obj, data.hex()) for obj, data in entries] for entries in actual[1][1]],
                    ]
                )
            )

            call["containers"][output_index].to_file(
                args.output / "serial-output.tar", container_format=ContainerFormat.TAR
            )
        expected_dependencies = [set(entries) for entries in expected[1][0]]
        actual_dependencies = [set(entries) for entries in actual[1][0]]
        covered = len(expected_dependencies) == len(actual_dependencies) and all(
            want <= got for want, got in zip(expected_dependencies, actual_dependencies)
        )
        extra_dependencies = sum(
            len(got - want) for want, got in zip(expected_dependencies, actual_dependencies)
        )
        conservative = False
        if workers and call["pipe"].parallel_invalidate_on_model_change:

            class ProbeChange:
                def __len__(self):
                    return 1

            invalidated = call["pipe"].process_custom_invalidation(
                result.custom_invalidation, ProbeChange()
            )
            conservative = call["pipe"].requires_custom_invalidation(ProbeChange()) and set(
                invalidated[output_index]
            ) == set(call["outgoing"][output_index])
        compatible = (
            actual[0] == expected[0]
            and actual[1][1] == expected[1][1]
            and (covered or conservative)
        )
        if not (
            compatible
            if args.native_overlay and call["pipe"].parallel_invalidate_on_model_change
            else actual == expected
        ):
            call["containers"][output_index].to_file(
                args.output / "mismatched-output.tar", container_format=ContainerFormat.TAR
            )
            (args.output / "mismatch.json").write_text(
                json.dumps(
                    {
                        "outputs_equal": actual[0] == expected[0],
                        "expected_dependencies": expected[1][0],
                        "actual_dependencies": actual[1][0],
                        "custom_equal": actual[1][1] == expected[1][1],
                    },
                    indent=2,
                )
            )
            raise AssertionError(f"Output/dependency mismatch for {workers} workers")
        inputs_unchanged = digest(call["containers"][:output_index]) == initial
        if not (args.native_overlay and args.pipe == "collect-cfg" and workers == 0):
            assert inputs_unchanged
        row = {
            "repeat": repeat,
            "workers": workers,
            "seconds": round(seconds, 3),
            "cpu_seconds": round(cpu_seconds, 3),
            "peak_tree_rss_bytes": peak[0],
            "functions": len(call["outgoing"][output_index]),
            "output_sha256": actual[0],
            "raw_output_sha256": digest([call["containers"][output_index]]),
            "comparison": (
                "normalized LLVM text with native LLVM verifier"
                if args.native_overlay and output_type.__name__ == "LLVMFunctionContainer"
                else "exact serialized output"
            ),
            "outputs_and_dependencies_equal": actual == expected,
            "outputs_equal": actual[0] == expected[0],
            "dependencies_cover_serial": covered,
            "additional_dependencies": extra_dependencies,
            "missing_native_path_dependencies": sum(
                len(want - got) for want, got in zip(expected_dependencies, actual_dependencies)
            ),
            "whole_model_invalidation": conservative,
            "effective_invalidation_covers_serial": covered or conservative,
            "batch_size": batch_size,
            "inputs_unchanged": inputs_unchanged,
        }
        if report["runs"]:
            row["speedup_vs_in_process"] = round(report["runs"][0]["seconds"] / seconds, 3)
        report["runs"].append(row)
        baseline = statistics.median(
            run["seconds"] for run in report["runs"] if run["workers"] == 0
        )
        report["summary"] = {
            str(count): {
                "median_seconds": statistics.median(
                    run["seconds"] for run in report["runs"] if run["workers"] == count
                ),
                "speedup_vs_in_process": baseline
                / statistics.median(
                    run["seconds"] for run in report["runs"] if run["workers"] == count
                ),
            }
            for count in {run["workers"] for run in report["runs"]}
        }
        (args.output / "report.json").write_text(json.dumps(report, indent=2))
        print(json.dumps(row), flush=True)


if __name__ == "__main__":
    main()
