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


class ConservativeFixturePipe(Pipe):
    name = "ConservativeFixturePipe"
    parallel_output = "destination"
    parallel_invalidate_on_model_change = True

    @classmethod
    def signature(cls):
        return ParallelFixturePipe.signature()

    def run(self, **kwargs):
        result = ParallelFixturePipe.run(self, **kwargs)
        return PipeDependencies(result.dependencies)


class ValueFunctionContainer(DictContainer):
    name = "ValueFunctionContainer"
    kind = MyKind.CHILD

    def __init__(self):
        super().__init__()
        self.values = {}

    def deserialize(self, data):
        for obj, value in data.items():
            self.add_object(obj)
            self.values[obj] = bytes(value)

    def serialize(self, objects=None):
        return {obj: self.values[obj] for obj in (self.objects() if objects is None else objects)}

    def clone(self, objects=None):
        result = type(self)()
        result.deserialize(self.serialize(objects))
        return result

    def merge(self, other):
        self.deserialize(other.serialize())

    def erase_objects(self, objects):
        for obj in objects:
            self.values.pop(obj, None)
        super().erase_objects(objects)

    def clear(self):
        self.values.clear()
        super().clear()


class InPlaceFixturePipe(Pipe):
    name = "InPlaceFixturePipe"
    parallel_output = "modules"
    parallel_in_place = True
    parallel_invalidate_on_model_change = True

    @classmethod
    def signature(cls):
        return (TaskArgument("modules", ValueFunctionContainer, TaskArgumentAccess.READ_WRITE),)

    def run(self, file_provider, model, containers, incoming, outgoing, configuration):
        config = json.loads(configuration or "{}")
        if config.get("check_sliced") and containers[0].objects() != outgoing[0]:
            raise RuntimeError("In-place worker received unowned input")
        original = containers[0].serialize(outgoing[0])
        for obj, data in original.items():
            containers[0].deserialize({obj: data + b"-changed-" + model.serialize()})
            if config.get("fail") == obj.serialize():
                raise RuntimeError("Failure after in-place mutation")
        if config.get("missing"):
            containers[0].erase_objects(ObjectSet(MyKind.CHILD, {next(iter(outgoing[0]))}))
        if config.get("extra"):
            containers[0].deserialize({MyObjectID(MyKind.CHILD, "extra"): b"foreign"})
        return PipeDependencies([[(obj, "/parallel/count") for obj in outgoing[0]]])


class BoundedInPlaceFixturePipe(Pipe):
    name = "BoundedInPlaceFixturePipe"
    serial_batch_size = 3

    def run(self, **kwargs):
        result = InPlaceFixturePipe.run(self, **kwargs)
        result.dependencies[0].extend((obj, "/unrelated/field") for obj in kwargs["outgoing"][0])
        return result

    @classmethod
    def signature(cls):
        return InPlaceFixturePipe.signature()


class ValueSeedPipe(Pipe):
    name = "ValueSeedPipe"

    @classmethod
    def signature(cls):
        return (TaskArgument("output", ValueFunctionContainer, TaskArgumentAccess.WRITE),)

    def run(self, file_provider, model, containers, incoming, outgoing, configuration):
        containers[0].deserialize({obj: b"seed" for obj in outgoing[0]})
        return PipeDependencies([[(obj, "/parallel/count") for obj in outgoing[0]]])
