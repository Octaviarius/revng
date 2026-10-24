#
# This file is distributed under the MIT License. See LICENSE.md for details.
#

import argparse
import hashlib
import json
import resource
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
        pipe_type = pipebox._native_pipes["emit-c"]
        pipe_type.parallel_output = "Output"
        pipe_type.parallel_inputs = ("Modules",)
        input_type = pipe_type.signature()[0].container_type
        output_type = pipe_type.signature()[1].container_type
        model_bytes = args.model.read_bytes()
        model = ReadOnlyModel(get_singleton(Model).deserialize(model_bytes)[0])
        object_type = get_singleton(ObjectID)
        with sqlite3.connect(f"file:{args.cache}?mode=ro", uri=True) as db:
            savepoint = db.execute(
                "SELECT max(savepoint_id) FROM objects WHERE container_id=?", ("clift-functions",)
            ).fetchone()[0]
            rows = db.execute(
                "SELECT object_id,content FROM objects WHERE container_id=? "
                "AND savepoint_id=? ORDER BY object_id LIMIT ?",
                ("clift-functions", savepoint, args.functions),
            ).fetchall()
        if not rows:
            raise ValueError("No cached Clift functions in the supplied read-only database")
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
        requested = ObjectSet(input_type.kind, set(data))

        def arguments():
            return dict(
                pipe=pipe_type(""),
                file_provider=NoFiles(),
                model=model,
                containers=[source, output_type()],
                incoming=[requested, ObjectSet(output_type.kind)],
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
        scope = "native emit-c, cached Clift inputs, complete unmodified source model"
    else:
        from test_parallel_runner import arguments as fixture_arguments, command_hook

        def arguments():
            return fixture_arguments(args.functions, {"rounds": args.rounds})

        output_index = 2
        scope = "synthetic CPU-bound per-function fixture, not native decompilation"
    from revng.pypeline.parallel_runner import ParallelRunnerContext
    from revng.pypeline.runner_context import RunnerContext

    def normalized(result):
        custom = list(result.custom_invalidation)
        custom.extend([] for _ in range(len(result.dependencies) - len(custom)))
        return (
            [
                sorted((obj.serialize(), path) for obj, path in entries)
                for entries in result.dependencies
            ],
            [sorted((obj.serialize(), bytes(data)) for obj, data in entries) for entries in custom],
        )

    report = {"scope": scope, "batch_size": args.batch_size, "runs": [], **metadata}
    expected = None
    for repeat, workers in (
        (repeat, workers) for repeat in range(args.repeat) for workers in [0, *args.workers]
    ):
        call = arguments()
        initial = digest(call["containers"][:output_index])
        runner = (
            RunnerContext()
            if workers == 0
            else ParallelRunnerContext(
                args.output / f"repeat-{repeat}-workers-{workers}",
                command_hook,
                workers,
                args.batch_size,
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
        if actual != expected:
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
        assert digest(call["containers"][:output_index]) == initial
        row = {
            "repeat": repeat,
            "workers": workers,
            "seconds": round(seconds, 3),
            "cpu_seconds": round(cpu_seconds, 3),
            "peak_tree_rss_bytes": peak[0],
            "functions": len(call["outgoing"][output_index]),
            "output_sha256": actual[0],
            "outputs_and_dependencies_equal": True,
            "inputs_unchanged": True,
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
