#
# This file is distributed under the MIT License. See LICENSE.md for details.
#

import hashlib
import json
import os
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from pipebox import *  # noqa: F403
from pipebox import ChildDictContainer, MyKind, MyObjectID, RootDictContainer

from revng.pypeline.task.pipe import Pipe, PipeDependencies
from revng.pypeline.task.task import TaskArgument, TaskArgumentAccess


class ParallelFixturePipe(Pipe):
    name = "ParallelFixturePipe"
    parallel_output = "destination"

    @classmethod
    def signature(cls):
        return (
            TaskArgument("root", RootDictContainer, TaskArgumentAccess.READ),
            TaskArgument("source", ChildDictContainer, TaskArgumentAccess.READ),
            TaskArgument("destination", ChildDictContainer, TaskArgumentAccess.WRITE),
        )

    def run(self, file_provider, model, containers, incoming, outgoing, configuration):
        config = json.loads(configuration or "{}")
        if config.get("pid_directory"):
            Path(config["pid_directory"], str(os.getpid())).touch()
        model_data = model.serialize()
        if not containers[0].objects() or len(containers[1].objects()) < config.get("inputs", 0):
            raise RuntimeError("Shared root or cross-function input is missing")
        if config.get("sleep"):
            time.sleep(config["sleep"])
        dependencies = [[], [], []]
        custom = [[], [], []]
        for obj in sorted(outgoing[2], key=lambda item: item.serialize()):
            if config.get("fail") == obj.serialize():
                raise RuntimeError("Fixture worker failure")
            if config.get("abort") == obj.serialize():
                os._exit(42)
            if config.get("missing") == obj.serialize():
                continue
            digest = hashlib.pbkdf2_hmac(
                "sha256", obj.serialize().encode(), model_data, config.get("rounds", 1)
            )
            containers[2].add_object(obj)
            dependencies[2].append((obj, "/root/common"))
            dependencies[2].append((obj, "/children/" + obj.serialize()))
            custom[2].append((obj, digest))
        if config.get("extra"):
            containers[2].add_object(MyObjectID(MyKind.CHILD, "extra"))
        if config.get("foreign_dependency"):
            dependencies[0].append((MyObjectID(MyKind.ROOT), "/foreign"))
        return PipeDependencies(dependencies, custom)

    def requires_custom_invalidation(self, diff):
        return True

    def process_custom_invalidation(self, invalidation_data, diff):
        return []
