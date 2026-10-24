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
    parser.add_argument("--pipe", choices=["emit-c", "collect-cfg"], default="emit-c")
    parser.add_argument("--model", type=Path)
    parser.add_argument("--cache", type=Path)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)
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

        class NoFiles(FileProvider):
            def get_files(self, requests):
                assert not requests
                return {}

        initialize_pypeline()
        pipe_type = pipebox._native_pipes[args.pipe]
        pipe_type.parallel_output = "Output"
        pipe_type.parallel_inputs = ("Modules",) if args.pipe == "emit-c" else ()
        pipe_type.parallel_invalidate_on_model_change = args.pipe == "collect-cfg"
        input_type = pipe_type.signature()[0].container_type
        output_type = pipe_type.signature()[1].container_type
        model_bytes = args.model.read_bytes()
        model = ReadOnlyModel(get_singleton(Model).deserialize(model_bytes)[0])
        object_type = get_singleton(ObjectID)
        container_id = "clift-functions" if args.pipe == "emit-c" else "llvm-root"
        with sqlite3.connect(f"file:{args.cache}?mode=ro", uri=True) as db:
            savepoint = db.execute(
                "SELECT max(savepoint_id) FROM objects WHERE container_id=?", (container_id,)
            ).fetchone()[0]
            rows = db.execute(
                "SELECT object_id,content FROM objects WHERE container_id=? "
                "AND savepoint_id=? ORDER BY object_id LIMIT ?",
                (container_id, savepoint, args.functions),
            ).fetchall()
        if not rows:
            raise ValueError(f"No cached {container_id} inputs in the supplied read-only database")
        compression = input_type.get_compression()
        data = {
            object_type.from_bytes(obj): compression.decompress(content) if compression else content
            for obj, content in rows
        }
        source = input_type()
        source.deserialize(data)
        from revng.support import get_root

        metadata.update(
            model_sha256=hashlib.sha256(model_bytes).hexdigest(),
            input_sha256=digest([source]),
            native_revision=(get_root() / "share/revng/component-hashes/revng").read_text().strip(),
        )
        if args.pipe == "collect-cfg":
            functions = sorted(model.all_objects(output_type.kind), key=lambda obj: obj.serialize())
            selected = random.Random(0).sample(functions, min(args.functions, len(functions)))
            requested = ObjectSet(output_type.kind, set(selected))
        else:
            requested = ObjectSet(output_type.kind, set(data))

        def arguments():
            input_container = source
            input_model = model
            if args.pipe == "collect-cfg":
                input_container = input_type()
                input_container.deserialize(data)
                input_model = ReadOnlyModel(get_singleton(Model).deserialize(model_bytes)[0])
            return dict(
                pipe=pipe_type(""),
                file_provider=NoFiles(),
                model=input_model,
                containers=[input_container, output_type()],
                incoming=[input_container.objects(), ObjectSet(output_type.kind)],
                outgoing=[ObjectSet(input_type.kind), requested],
                configuration="",
            )

        def command_hook(command):
            from revng.pypeline.parallel_runner import native_pipe_command
            from revng.support import get_root

            return native_pipe_command(
                command, get_root() / "libexec/revng/pypeline-run-pipe", pipebox._native_libraries
            )

        output_index = 1
        scope = f"native {args.pipe}, cached {container_id}, complete unmodified source model"
    else:
        from test_parallel_runner import arguments as fixture_arguments, command_hook

        def arguments():
            return fixture_arguments(args.functions, {"rounds": args.rounds})

        output_index = 2
        scope = "synthetic CPU-bound per-function fixture, not native decompilation"
    from revng.pypeline.parallel_runner import ParallelRunnerContext
    from revng.pypeline.runner_context import RunnerContext
    from revng.pypeline.task.pipe import WHOLE_MODEL_INVALIDATION

    def normalized(result):
        custom = list(result.custom_invalidation)
        custom.extend([] for _ in range(len(result.dependencies) - len(custom)))
        return (
            [
                sorted((obj.serialize(), path) for obj, path in entries)
                for entries in result.dependencies
            ],
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
    for repeat, workers in (
        (repeat, workers) for repeat in range(args.repeat) for workers in [0, *args.workers]
    ):
        call = arguments()
        batch_size = args.batch_size or math.ceil(
            len(call["outgoing"][output_index]) / max(1, workers)
        )
        initial = digest(call["containers"][:output_index])
        runner = (
            RunnerContext()
            if workers == 0
            else ParallelRunnerContext(
                args.output / f"repeat-{repeat}-workers-{workers}",
                command_hook,
                workers,
                batch_size,
            )
        )
        if workers:
            runner.min_functions = 0
            runner.min_functions_per_worker = 1
        done = Event()
        peak = [0]

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
        actual = (digest([call["containers"][output_index]]), normalized(result))
        if expected is None:
            expected = actual
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
            compatible if args.native_overlay and args.pipe == "collect-cfg" else actual == expected
        ):
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
