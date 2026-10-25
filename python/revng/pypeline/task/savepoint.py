#
# This file is distributed under the MIT License. See LICENSE.md for details.
#

from collections.abc import Buffer
from itertools import batched
from typing import Mapping, Sequence, final

from revng.pypeline.container import ConfigurationId, ContainerDeclaration, ContainerSet
from revng.pypeline.object import ObjectID, ObjectSet
from revng.pypeline.storage.storage_provider import ContainerLocation, PipeDependencies
from revng.pypeline.storage.storage_provider import SavePointsRange, StorageProvider
from revng.pypeline.utils.logger import pypeline_logger

from .requests import Requests
from .task import TaskArgument, TaskArgumentAccess


class _SerializedObjects(Mapping[ObjectID, Buffer]):
    def __init__(self, container, objects):
        self.container = container
        self.requested = objects
        self.compression = container.get_compression()

    def __len__(self):
        return len(self.requested)

    def __iter__(self):
        return iter(self.requested)

    def __getitem__(self, key):
        if key not in self.requested:
            raise KeyError(key)
        data = self.container.serialize(ObjectSet(self.requested.kind, {key}))[key]
        return self.compression.compress(data) if self.compression else data

    def items(self):
        for chunk in batched(self.requested, 128):
            serialized = self.container.serialize(ObjectSet(self.requested.kind, set(chunk)))
            for key, data in serialized.items():
                yield key, self.compression.compress(data) if self.compression else data
            del serialized


class SavePoint:
    """
    A SavePoint is a task that satisfies its requests by looking up the requested objects in the
    container it owns.
    """

    __slots__: tuple = ("name", "arguments", "to_save")

    def __init__(
        self,
        name: str,
        to_save: Sequence[ContainerDeclaration],
    ):
        self.name = name
        self.arguments = [
            TaskArgument(
                name=declaration.name,
                container_type=declaration.container_type,
                access=TaskArgumentAccess.READ_WRITE,
            )
            for declaration in to_save
        ]
        self.to_save = to_save

    @final
    def prerequisites_for(
        self,
        requests: Requests,
        configuration_id: ConfigurationId,
        storage_provider: StorageProvider,
        savepoint_range: SavePointsRange,
    ) -> Requests:
        """
        This method has the same semantics as Pipe.prerequisites_for, but it has
        additional arguments that the Pipe should not have.
        """
        result = requests.clone()

        for decl, request in result.items():
            if decl not in self.to_save:
                # This savepoint does not care about this container
                continue
            # We have a match, so we can check if the objects are present
            location = ContainerLocation(
                savepoint_id=savepoint_range.start,
                container_id=decl.name,
                configuration_id=configuration_id,
            )
            found_objs = storage_provider.has(location, request)
            # If so, we should REMOVE the found objects from the request
            for found_obj in found_objs:
                result[decl].remove(found_obj)
        return result

    def run(
        self,
        containers: ContainerSet,
        incoming: Requests,
        outgoing: Requests,
        configuration_id: ConfigurationId,
        storage_provider: StorageProvider,
        savepoint_range: SavePointsRange,
        pipes_dependencies: list[PipeDependencies],
    ):
        """
        The savepoint will cache the incoming containers, and then fill the outgoing
        containers with the cached data.
        """

        assert set(containers.keys()).issuperset(
            set(incoming.keys()) | set(outgoing.keys())
        ), "SavePoint containers must be a subset of incoming and outgoing requests."

        objects: dict[ContainerLocation, Mapping[ObjectID, Buffer]] = {}
        for decl in self.to_save:
            location = ContainerLocation(
                savepoint_id=savepoint_range.start,
                container_id=decl.name,
                configuration_id=configuration_id,
            )
            container_incoming = incoming.get(decl)
            if len(container_incoming) > 0:
                objects[location] = _SerializedObjects(containers[decl], container_incoming)

        pypeline_logger.log(f"Saving checkpoint {self.name}: bounded batches of 128 objects")
        storage_provider.add_objects(pipes_dependencies, objects)
        pypeline_logger.log(f"Saved checkpoint {self.name}")

        for decl in self.to_save:
            # Compute the actual set of objects to load, if an object has
            # already been saved from incoming do not re-load it from storage
            container_outgoing = outgoing.get(decl) - incoming.get(decl)

            if len(container_outgoing) > 0:
                location = ContainerLocation(
                    savepoint_id=savepoint_range.start,
                    container_id=decl.name,
                    configuration_id=configuration_id,
                )
                compression = containers[decl].get_compression()
                for chunk in batched(container_outgoing, 128):
                    requested = ObjectSet(container_outgoing.kind, set(chunk))
                    container_data = storage_provider.get(location, requested)
                    if compression is not None:
                        containers[decl].deserialize(
                            {k: compression.decompress(v) for k, v in container_data.items()}
                        )
                    else:
                        containers[decl].deserialize(container_data)
