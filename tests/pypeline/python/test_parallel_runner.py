#
# This file is distributed under the MIT License. See LICENSE.md for details.
#

import json
import os
import sys
import time
from pathlib import Path

import pytest
from parallel_pipebox import ParallelFixturePipe
from pipebox import ChildDictContainer, DictModel, MyKind, MyObjectID, RootDictContainer

from revng.pypeline import initialize_pypeline
from revng.pypeline.model import ReadOnlyModel
from revng.pypeline.object import ObjectSet
from revng.pypeline.parallel_runner import ParallelRunnerContext
from revng.pypeline.parallel_runner import native_pipe_command
from revng.pypeline.runner_context import RunnerContext
from revng.pypeline.storage.file_provider import FileProvider
from revng.pypeline.utils import PypelineException

initialize_pypeline()


def runner(*args, **kwargs):
    return ParallelRunnerContext(*args, min_functions=0, min_functions_per_worker=1, **kwargs)


class NoFiles(FileProvider):
    def get_files(self, requests):
        assert not requests
        return {}


def command_hook(command):
    return [
        sys.executable,
        "-m",
        "revng.pypeline.main",
        "--pipebox",
        str(Path(__file__).with_name("parallel_pipebox.py")),
        *command[1:],
    ]


def arguments(count=7, config=None):
    objects = {MyObjectID(MyKind.CHILD, f"f{i:03}") for i in range(count)}
    root = RootDictContainer()
    root.add_object(MyObjectID(MyKind.ROOT))
    source = ChildDictContainer()
    for obj in objects:
        source.add_object(obj)
    output = ChildDictContainer()
    output.add_object(MyObjectID(MyKind.CHILD, "prior"))
    model = DictModel()
    model["/parallel/count"] = count
    return dict(
        pipe=ParallelFixturePipe(),
        file_provider=NoFiles(),
        model=ReadOnlyModel(model),
        containers=[root, source, output],
        incoming=[root.objects(), source.objects(), ObjectSet(MyKind.CHILD)],
        outgoing=[
            ObjectSet(MyKind.ROOT),
            ObjectSet(MyKind.CHILD),
            ObjectSet(MyKind.CHILD, objects),
        ],
        configuration=json.dumps({"inputs": count, **(config or {})}),
    )


def normalized(result):
    return (
        [sorted((o.serialize(), p) for o, p in entries) for entries in result.dependencies],
        [
            sorted((o.serialize(), bytes(data)) for o, data in entries)
            for entries in result.custom_invalidation
        ],
    )


@pytest.mark.parametrize("workers,batch,count", [(1, 2, 7), (2, 2, 7), (4, 1, 2), (2, 8, 1)])
def test_serial_equivalence(tmp_path, workers, batch, count):
    serial = arguments(count)
    parallel = arguments(count)
    expected = RunnerContext().run_pipe(**serial)
    actual = runner(tmp_path / "run", command_hook, workers, batch).run_pipe(**parallel)
    assert normalized(actual) == normalized(expected)
    assert parallel["containers"][2].to_dict() == serial["containers"][2].to_dict()
    assert parallel["containers"][0].to_dict() == serial["containers"][0].to_dict()
    assert parallel["containers"][1].to_dict() == serial["containers"][1].to_dict()


@pytest.mark.parametrize(
    "config",
    [
        {"fail": "/CHILD/f000"},
        {"abort": "/CHILD/f000"},
        {"missing": "/CHILD/f000"},
        {"extra": True},
        {"foreign_dependency": True},
    ],
)
def test_failure_does_not_publish(tmp_path, config):
    args = arguments(config=config)
    before = args["containers"][2]
    snapshot = before.to_dict()
    with pytest.raises(PypelineException):
        runner(tmp_path / "run", command_hook, 2, 2).run_pipe(**args)
    assert args["containers"][2] is before
    assert before.to_dict() == snapshot


def test_timeout_reaps_children(tmp_path):
    pid_directory = tmp_path / "pids"
    pid_directory.mkdir()
    args = arguments(config={"sleep": 30, "pid_directory": str(pid_directory)})
    started = time.monotonic()
    with pytest.raises(PypelineException, match="timed out"):
        runner(tmp_path / "run", command_hook, 2, 2, timeout=15).run_pipe(**args)
    assert time.monotonic() - started < 25
    pids = list(pid_directory.iterdir())
    assert pids
    for pid in pids:
        with pytest.raises(ProcessLookupError):
            os.kill(int(pid.name), 0)


def test_invalid_settings_and_contract(tmp_path):
    for setting in ({"workers": 0}, {"batch_size": 0}, {"timeout": 0}):
        with pytest.raises(ValueError):
            runner(tmp_path / "run", **setting)
    args = arguments()
    args["pipe"].parallel_output = "source"
    with pytest.raises(PypelineException, match="contract"):
        runner(tmp_path / "contract", command_hook).run_pipe(**args)


def test_empty_and_serial_fallback(tmp_path):
    args = arguments(0)
    result = runner(tmp_path / "empty", command_hook).run_pipe(**args)
    assert not result.dependencies[2]
    args = arguments(2)
    args["pipe"].parallel_output = None
    runner(tmp_path / "serial", command_hook).run_pipe(**args)
    assert not list((tmp_path / "serial").glob("*/batch-*"))


def test_sliced_input_equivalence(tmp_path):
    args = arguments(7, {"inputs": 0})
    expected = RunnerContext().run_pipe(**arguments(7, {"inputs": 0}))
    args["pipe"].parallel_inputs = ("source",)
    result = runner(tmp_path / "sliced", command_hook, 2, 4).run_pipe(**args)
    assert normalized(result) == normalized(expected)
    assert len(args["containers"][1].objects()) == 7


def test_native_command_and_cli(tmp_path, monkeypatch):
    monkeypatch.setattr(os, "process_cpu_count", lambda: 4, raising=False)
    import click
    from click.testing import CliRunner
    from types import SimpleNamespace
    from revng.pypeline.cli.common_options import debug_option

    command = native_pipe_command(
        [
            "pype",
            "pipeline",
            "run-pipe",
            "emit-c",
            "--tar",
            "--output-objects",
            "/function/1",
            "--dependencies",
            "dependencies.tar",
            "model.yml",
            "input.tar",
            "output.tar",
        ],
        "/tool/pypeline-run-pipe",
        ["/tool/pipebox.so"],
    )
    assert command == [
        "/tool/pypeline-run-pipe",
        "-load=/tool/pipebox.so",
        "emit-c",
        "--objects",
        "output",
        "/function/1",
        "--dependencies",
        "dependencies.tar",
        "model.yml",
        "input.tar",
        "output.tar",
    ]

    @click.command()
    @debug_option
    def cli(runner_context):
        click.echo(type(runner_context).__name__)

    obj = SimpleNamespace(pipebox=SimpleNamespace(), pipebox_path=Path(__file__), pipebox_args=[])
    result = CliRunner().invoke(cli, ["--parallel-workers", "2"], obj=obj)
    assert result.exit_code == 0, result.output
    assert "ParallelRunnerContext" in result.output


def test_small_work_stays_in_process(tmp_path):
    args = arguments(7)

    def forbidden(command):
        raise AssertionError("Fast task spawned a subprocess")

    context = ParallelRunnerContext(tmp_path / "threshold", forbidden, workers=64)
    context.run_pipe(**args)
    assert not list((tmp_path / "threshold").iterdir())
    args = arguments(7)
    context = ParallelRunnerContext(
        tmp_path / "per-worker",
        forbidden,
        workers=64,
        min_functions=0,
        min_functions_per_worker=256,
    )
    context.run_pipe(**args)
    assert not list((tmp_path / "per-worker").iterdir())
    args = arguments(7)
    context = ParallelRunnerContext(
        tmp_path / "one-batch",
        forbidden,
        workers=64,
        min_functions=0,
        min_functions_per_worker=1,
        batch_size=128,
    )
    context.run_pipe(**args)
    assert not list((tmp_path / "one-batch").iterdir())


def test_core_default_and_cap(tmp_path, monkeypatch):
    monkeypatch.setattr(os, "process_cpu_count", lambda: 8, raising=False)
    assert ParallelRunnerContext(tmp_path / "auto").workers == 8
    assert ParallelRunnerContext(tmp_path / "limited", workers=3).workers == 3
    assert ParallelRunnerContext(tmp_path / "capped", workers=100).workers == 8


@pytest.mark.parametrize(
    "pipe_name,changed_path",
    [
        ("ParallelFixturePipe", "/root/common"),
        ("ConservativeFixturePipe", "/unrelated/field"),
    ],
)
def test_project_cache_and_model_edit(tmp_path, pipe_name, changed_path):
    from revng.pypeline.parallel_runner import available_cpus

    if available_cpus() < 2:
        pytest.skip("Needs two available CPUs for the CLI subprocess integration")
    import subprocess

    pipeline = tmp_path / "pipeline.yml"
    pipeline.write_text(
        """
artifact-categories:
  - name: main
    show-by-default: true
containers:
  - {name: source, type: ChildDictContainer}
  - {name: root, type: RootDictContainer}
  - {name: destination, type: ChildDictContainer}
branches:
  root:
    tasks:
      - pipe: GeneratorPipe
        arguments: [source]
      - pipe: ToLowerKindPipe
        arguments: [source, root]
      - pipe: ParallelFixturePipe
        arguments: [root, source, destination]
      - savepoint: result
        containers: [destination]
        artifacts:
          - {name: functions, container: destination, category: main, filename: functions.tar}
""".replace(
            "ParallelFixturePipe", pipe_name
        )
    )
    model = tmp_path / "model.yml"
    model.write_text('"/parallel/count": 7\n')

    def run(index):
        command = command_hook(
            [
                "pype",
                "project",
                "--pipeline",
                str(pipeline),
                "artifact",
                "functions",
                "--tar",
                "-o",
                str(tmp_path / f"out-{index}.tar"),
                "--parallel-workers",
                "2",
                "--parallel-batch-size",
                "4",
                "--parallel-min-functions",
                "0",
                "--parallel-min-per-worker",
                "1",
            ]
        )
        result = subprocess.run(command, cwd=tmp_path, text=True, capture_output=True, timeout=90)
        assert result.returncode == 0, result.stderr
        return result

    first = run(1)
    assert f"Parallel {pipe_name}" in first.stderr
    second = run(2)
    assert f"Parallel {pipe_name}" not in second.stderr
    model.write_text(f'"/parallel/count": 7\n"{changed_path}": 1\n')
    third = run(3)
    assert f"Parallel {pipe_name}" in third.stderr
    output = [ChildDictContainer.from_file(tmp_path / f"out-{i}.tar").to_dict() for i in (1, 2, 3)]
    assert output[0] == output[1] == output[2]
