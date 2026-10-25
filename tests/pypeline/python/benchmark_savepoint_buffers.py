import argparse
import hashlib
import json
import resource
import time

from pipebox import MyKind, MyObjectID
from revng.pypeline import initialize_pypeline
from revng.pypeline.object import ObjectSet
from revng.pypeline.task.savepoint import _SerializedObjects


parser = argparse.ArgumentParser()
parser.add_argument('--mode', choices=['eager', 'stream'], required=True)
args = parser.parse_args()
initialize_pypeline()
objects = ObjectSet(MyKind.CHILD, {MyObjectID(MyKind.CHILD, f'o{i:04}') for i in range(512)})


class SyntheticContainer:
    @staticmethod
    def get_compression():
        return None

    @staticmethod
    def serialize(requested):
        return {obj: bytes(bytearray(1024*1024)) for obj in requested}


container = SyntheticContainer()
began = time.monotonic()
serialized = container.serialize(objects) if args.mode == 'eager' else _SerializedObjects(container, objects)
digest = 0
count = 0
for obj, buffer in serialized.items():
    digest ^= int.from_bytes(hashlib.sha256(obj.serialize().encode()+buffer).digest())
    count += 1
print(json.dumps(dict(mode=args.mode, objects=count, digest=hex(digest),
                     seconds=round(time.monotonic()-began,3),
                     peak_rss_mib=round(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss/1024,1))))
