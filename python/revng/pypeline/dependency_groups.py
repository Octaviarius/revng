#
# This file is distributed under the MIT License. See LICENSE.md for details.
#

import json
import struct
import zlib

from revng.pypeline.utils import PypelineException


MAGIC = b"revng-model-paths-v1\x00"


def _encode(paths):
    return zlib.compress(json.dumps(sorted(paths), separators=(",", ":")).encode())


class SharedDependencyPaths:
    def __init__(self, common, extra):
        self.common = common
        self.extra = extra

    def __bytes__(self):
        return MAGIC + struct.pack("<I", len(self.common)) + self.common + self.extra

    def __buffer__(self, flags):
        return memoryview(bytes(self))


def pack_dependencies(entries, objects):
    paths = {obj: set() for obj in objects}
    pool = {}
    for obj, path in entries:
        if obj not in paths:
            raise PypelineException("Dependency outside bounded batch ownership")
        paths[obj].add(pool.setdefault(path, path))
    common = set.intersection(*paths.values()) if paths else set()
    encoded_common = _encode(common)
    return [
        (obj, SharedDependencyPaths(encoded_common, _encode(read - common)))
        for obj, read in paths.items()
    ]


def split_dependencies(data):
    raw = bytes(data)
    if not raw.startswith(MAGIC) or len(raw) < len(MAGIC) + 4:
        raise PypelineException("Invalid compact model dependencies")
    size = struct.unpack_from("<I", raw, len(MAGIC))[0]
    offset = len(MAGIC) + 4
    if size > len(raw) - offset:
        raise PypelineException("Truncated compact model dependencies")
    return raw[offset:offset + size], raw[offset + size:]


def unpack_dependencies(data):
    common, extra = split_dependencies(data)
    return set(json.loads(zlib.decompress(common))) | set(json.loads(zlib.decompress(extra)))


def invalidate_dependencies(entries, changed):
    common_matches = {}
    result = set()
    for obj, data in entries:
        common, extra = split_dependencies(data)
        if common not in common_matches:
            common_matches[common] = not changed.isdisjoint(json.loads(zlib.decompress(common)))
        if common_matches[common] or not changed.isdisjoint(json.loads(zlib.decompress(extra))):
            result.add(obj)
    return result
