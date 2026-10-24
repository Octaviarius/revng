#
# This file is distributed under the MIT License. See LICENSE.md for details.
#

import math
import os
import signal
import subprocess
import tarfile
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from threading import Event

from revng.pypeline.object import ObjectSet
from revng.pypeline.runner_context import ArgvHook, RunnerContext
from revng.pypeline.task.pipe import Pipe, PipeDependencies
from revng.pypeline.task.task import TaskArgumentAccess
from revng.pypeline.utils import PypelineException
from revng.pypeline.utils.logger import pypeline_logger


def available_cpus():
    if hasattr(os, "process_cpu_count"):
        return os.process_cpu_count() or 1
    if hasattr(os, "sched_getaffinity"):
        return len(os.sched_getaffinity(0)) or 1
    return os.cpu_count() or 1


def native_pipe_command(command, executable, libraries):
    if command[:3] != ["pype", "pipeline", "run-pipe"]:
        raise ValueError("Expected a low-level pipe command")
    result = [str(executable), *[f"-load={path}" for path in libraries], command[3]]
    arguments = iter(command[4:])
    for argument in arguments:
        if argument == "--tar":
            continue
        if argument.startswith("--") and argument.endswith("-objects"):
            result.extend(["--objects", argument[2:-8], next(arguments)])
        else:
            result.append(argument)
    return result


class ParallelRunnerContext(RunnerContext):
    def __init__(
        self,
        base_directory: Path,
        argv_hook: ArgvHook | None = None,
        workers: int | None = None,
        batch_size: int = 256,
        timeout: float = 600,
        min_functions: int = 1024,
        min_functions_per_worker: int = 256,
        serial_context: RunnerContext | None = None,
    ):
        available = available_cpus()
        workers = available if workers is None else min(workers, available)
        if workers < 1 or batch_size < 1 or timeout <= 0 or not math.isfinite(timeout):
            raise ValueError("workers, batch_size and timeout must be positive")
        if min_functions < 0 or min_functions_per_worker < 1:
            raise ValueError("Invalid parallel thresholds")
        super().__init__(True, argv_hook, base_directory)
        self.workers = workers
        self.batch_size = batch_size
        self.timeout = timeout
        self.min_functions = min_functions
        self.min_functions_per_worker = min_functions_per_worker
        self.serial_context = serial_context or RunnerContext()

    def run_analysis(self, *args, **kwargs):
        return self.serial_context.run_analysis(*args, **kwargs)

    @staticmethod
    def _output_index(pipe: Pipe) -> int | None:
        if pipe.parallel_output is None:
            return None
        writable = [
            (i, arg)
            for i, arg in enumerate(pipe.signature())
            if arg.access & TaskArgumentAccess.WRITE
        ]
        if (
            len(writable) != 1
            or writable[0][1].access != TaskArgumentAccess.WRITE
            or writable[0][1].name != pipe.parallel_output
            or writable[0][1].container_type.kind.rank() != 1
        ):
            raise PypelineException(f"Invalid parallel output contract for {pipe.name}")
        return writable[0][0]

    @staticmethod
    def _stop(process):
        if os.name == "posix":
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
        elif process.poll() is None:
            process.kill()
        process.wait()

    def _worker(self, directory, command, cancelled):
        if cancelled.is_set():
            raise PypelineException("Parallel run cancelled")
        command = self._prepare_command(directory, command)
        with (directory / "worker.log").open("wb") as log:
            process = subprocess.Popen(
                command, cwd=directory, stdout=log, stderr=log, start_new_session=os.name == "posix"
            )
            started = time.monotonic()
            try:
                while True:
                    if cancelled.is_set():
                        raise PypelineException("Parallel run cancelled")
                    if time.monotonic() - started > self.timeout:
                        raise PypelineException(f"Worker timed out: {directory}")
                    try:
                        code = process.wait(timeout=0.1)
                        break
                    except subprocess.TimeoutExpired:
                        pass
                if code:
                    raise PypelineException(
                        f"Worker exited with {code}: {directory / 'worker.log'}"
                    )
            finally:
                self._stop(process)

    @staticmethod
    def _link_inputs(shared, directory, sliced_names):
        directory.mkdir()
        (directory / "outputs").mkdir()
        for source in shared.iterdir():
            if source.name == "outputs":
                continue
            if source.is_dir():
                target = directory / source.name
                target.mkdir()
                for file in source.iterdir():
                    if source.name == "inputs" and file.name in sliced_names:
                        continue
                    os.link(file, target / file.name)
            else:
                os.link(source, directory / source.name)

    def run_pipe(self, pipe, file_provider, model, containers, incoming, outgoing, configuration):
        started = time.monotonic()
        output_index = self._output_index(pipe)
        count = len(outgoing[output_index]) if output_index is not None else 0
        workers = min(self.workers, max(1, count // self.min_functions_per_worker))
        if output_index is None or not count or count < self.min_functions:
            return self.serial_context.run_pipe(
                pipe, file_provider, model, containers, incoming, outgoing, configuration
            )
        if self.workers > 1 and workers < 2:
            return self.serial_context.run_pipe(
                pipe, file_provider, model, containers, incoming, outgoing, configuration
            )
        requested = outgoing[output_index]
        if requested.kind != pipe.signature()[output_index].container_type.kind:
            raise PypelineException("Parallel output request has the wrong kind")
        objects = sorted(requested, key=lambda obj: obj.serialize())
        from revng.pypeline.utils.naming import normalize_flag

        sliced = {}
        for name in pipe.parallel_inputs:
            matches = [(i, arg) for i, arg in enumerate(pipe.signature()) if arg.name == name]
            if (
                len(matches) != 1
                or matches[0][1].access != TaskArgumentAccess.READ
                or matches[0][1].container_type.kind != requested.kind
            ):
                raise PypelineException(f"Invalid parallel input contract for {pipe.name}")
            sliced[matches[0][0]] = f"{normalize_flag(name)}.tar"
        directory = self._get_directory(pipe.name)
        shared = directory / "shared"
        shared.mkdir()
        self._pipe_prepare_debug_directory(
            shared,
            pipe,
            model,
            file_provider,
            containers,
            {i: ObjectSet(requested.kind) for i in sliced},
        )
        batches = []
        for offset in range(0, len(objects), self.batch_size):
            batch = ObjectSet(requested.kind, set(objects[offset : offset + self.batch_size]))
            path = directory / f"batch-{len(batches):05}"
            self._link_inputs(shared, path, set(sliced.values()))
            for index, filename in sliced.items():
                containers[index].to_file(path / "inputs" / filename, batch)
            batch_outgoing = [ObjectSet(arg.container_type.kind) for arg in pipe.signature()]
            batch_outgoing[output_index] = batch
            command = self._pipe_construct_cmd(path, pipe, batch_outgoing, configuration)
            batches.append((path, batch, command))
        pypeline_logger.log(
            f"Parallel {pipe.name}: {len(objects)} functions, "
            f"{len(batches)} batches, {min(workers, len(batches))} workers"
        )
        cancelled = Event()
        with ThreadPoolExecutor(max_workers=min(workers, len(batches))) as pool:
            futures = [
                pool.submit(self._worker, path, command, cancelled) for path, _, command in batches
            ]
            try:
                for future in as_completed(futures):
                    future.result()
            except BaseException:
                cancelled.set()
                for future in futures:
                    future.cancel()
                raise
        merged = list(containers)
        merged[output_index] = type(containers[output_index])()
        dependencies = [[] for _ in pipe.signature()]
        custom = [[] for _ in pipe.signature()]
        seen = set()
        for path, expected, _ in batches:
            argument = pipe.signature()[output_index]
            archive = path / "outputs" / f"{normalize_flag(argument.name)}.tar"
            with tarfile.open(archive) as tar:
                names = [member.name for member in tar if member.isfile()]
                if len(names) != len(set(names)) or len(names) != len(expected):
                    raise PypelineException(f"Duplicate output archive member: {path}")
            result_containers = list(containers)
            result = self._pipe_load_results(path, pipe, result_containers)
            output = result_containers[output_index]
            if not output.verify() or set(output.objects()) != set(expected):
                raise PypelineException(f"Incorrect parallel output objects: {path}")
            serialized = output.serialize(output.objects())
            if seen.intersection(serialized):
                raise PypelineException("Duplicate parallel output ownership")
            seen.update(serialized)
            merged[output_index].deserialize(serialized)
            for index, entries in enumerate(result.dependencies):
                for obj, model_path in entries:
                    if index != output_index or obj not in expected:
                        raise PypelineException(f"Dependency outside batch ownership: {path}")
                    dependencies[index].append((obj, model_path))
            for index, entries in enumerate(result.custom_invalidation):
                for obj, data in entries:
                    if index != output_index or obj not in expected:
                        raise PypelineException(f"Invalidation outside batch ownership: {path}")
                    custom[index].append((obj, data))
        if seen != set(requested) or not merged[output_index].verify():
            raise PypelineException("Incomplete parallel output")
        merged[output_index].deserialize(
            containers[output_index].serialize(containers[output_index].objects() - requested)
        )
        for entries in dependencies:
            entries.sort(key=lambda entry: (entry[0].serialize(), entry[1]))
        containers[output_index] = merged[output_index]
        self.last_metrics = {
            "functions": len(objects),
            "batches": len(batches),
            "workers": min(workers, len(batches)),
            "seconds": time.monotonic() - started,
        }
        return PipeDependencies(dependencies, custom)
