#
# This file is distributed under the MIT License. See LICENSE.md for details.
#

import ast
import json
import random
import sqlite3
import sys
import time
import unittest
from contextlib import closing
from pathlib import Path


def queries():
    root = Path(__file__).resolve().parents[3]
    source = root / "python/revng/pypeline/storage/local_provider.py"
    return {
        target.id: node.value.value
        for node in ast.parse(source.read_text()).body
        if isinstance(node, ast.Assign) and isinstance(node.value, ast.Constant)
        for target in node.targets
        if isinstance(target, ast.Name)
    }


def database():
    db = sqlite3.connect(":memory:")
    db.executescript(
        """
        CREATE TABLE objects(
            savepoint_id INT, container_id TEXT, configuration_hash TEXT,
            object_id BLOB, content BLOB,
            PRIMARY KEY(savepoint_id, container_id, configuration_hash, object_id));
        CREATE TABLE dependencies(
            savepoint_id_start INT, savepoint_id_end INT, container_id TEXT,
            configuration_hash TEXT, object_id BLOB, model_path TEXT,
            PRIMARY KEY(savepoint_id_start, savepoint_id_end, container_id,
                        configuration_hash, object_id, model_path));
        CREATE INDEX model_path_on_dependencies ON dependencies(model_path);
    """
    )
    return db


def run_invalidation(db, paths, additional, legacy=False):
    sql = queries()
    db.executescript(sql["CREATE_TABLE_INVALIDATE"].format(uuid="test"))
    db.executemany("INSERT INTO model_paths_test VALUES (?)", ((p,) for p in paths))
    db.executemany("INSERT INTO additional_objects_test VALUES (?,?,?,?,?)", additional)
    if legacy:
        statement = (Path(__file__).parent / "fixtures/legacy-invalidation.sql").read_text()
    else:
        statement = sql["INVALIDATE_QUERY"]
    db.executescript(statement.format(uuid="test", objectid_mask="x'" + "ff" * 17 + "'"))
    removed = sorted(db.execute(sql["INVALIDATE_QUERY_2"].format(uuid="test")).fetchall())
    remaining = [
        sorted(db.execute(f"SELECT * FROM {table}").fetchall())
        for table in ("objects", "dependencies")
    ]
    for table in ("model_paths_test", "additional_objects_test", "invalidated_objects_test"):
        db.execute(f"DROP TABLE {table}")
    assert db.execute("SELECT name FROM sqlite_temp_master WHERE type='table'").fetchall() == []
    return removed, remaining


class SQLiteInvalidationTest(unittest.TestCase):
    def test_custom_dependency_migration_preserves_cache(self):
        with closing(database()) as db:
            db.executescript(
                """
                CREATE TABLE custom_dependencies(pipe_id INT,configuration_hash TEXT,
                    argument_index INT,object_id BLOB,data BLOB,
                    PRIMARY KEY(pipe_id,configuration_hash));
                CREATE INDEX custom_dependencies_index ON custom_dependencies(pipe_id,configuration_hash);
                INSERT INTO custom_dependencies VALUES(1,'config',1,x'01',x'10');
                INSERT INTO objects VALUES(1,'out','config',x'01',x'10');
                INSERT INTO dependencies VALUES(1,2,'out','config',x'01','/path');
            """
            )
            before = [
                db.execute(f"SELECT * FROM {table}").fetchall()
                for table in ("objects", "dependencies", "custom_dependencies")
            ]
            migration = (
                Path(__file__).resolve().parents[3]
                / "python/revng/pypeline/storage/local_provider_migrations/v4.sql"
            )
            db.executescript(migration.read_text())
            after = [
                db.execute(f"SELECT * FROM {table}").fetchall()
                for table in ("objects", "dependencies", "custom_dependencies")
            ]
            self.assertEqual(before, after)
            db.execute("INSERT INTO custom_dependencies VALUES(1,'config',1,x'02',x'20')")
            db.execute("INSERT INTO custom_dependencies VALUES(1,'config',2,x'01',x'30')")
            self.assertEqual(
                db.execute("SELECT count(*) FROM custom_dependencies").fetchone()[0], 3
            )

    def test_matches_legacy(self):
        ids = [b"", b"\xff" * 17, b"\x80\x00"]
        ids += [bytes([i]) for i in range(1, 15)]
        ids += [bytes([i, j]) for i in range(1, 5) for j in range(1, 6)]
        for seed in range(100):
            with self.subTest(seed=seed):
                rng = random.Random(seed)
                original = database()
                optimized = database()
                for _ in range(250):
                    original.execute(
                        "INSERT OR IGNORE INTO objects VALUES (?,?,?,?,?)",
                        (
                            rng.randrange(1, 6),
                            rng.choice(["a", "b"]),
                            rng.choice(["x", "y"]),
                            rng.choice(ids),
                            b"data",
                        ),
                    )
                for _ in range(700):
                    start = rng.randrange(1, 6)
                    original.execute(
                        "INSERT OR IGNORE INTO dependencies VALUES (?,?,?,?,?,?)",
                        (
                            start,
                            rng.randrange(start, 7),
                            rng.choice(["a", "b"]),
                            rng.choice(["x", "y"]),
                            rng.choice(ids),
                            rng.choice(["/A", "/B", "/C"]),
                        ),
                    )
                original.commit()
                original.backup(optimized)
                paths = ["/A", "/B", "/A"] if seed % 3 else []
                additional = [(1, 6, "b", "x", b"\x01")] if seed % 2 else []
                self.assertEqual(
                    run_invalidation(original, paths, additional, legacy=True),
                    run_invalidation(optimized, paths, additional),
                )
                original.close()
                optimized.close()

    def test_cross_container_root_and_savepoint_boundaries(self):
        with closing(database()) as db:
            db.executemany(
                "INSERT INTO objects VALUES (?,?,?,?,?)",
                [
                    (point, container, config, obj, b"data")
                    for point in (1, 2, 3, 4)
                    for container in ("a", "b")
                    for config in ("x", "y")
                    for obj in (b"", b"\x01", b"\x01\x02", b"\x02")
                ],
            )
            removed, _ = run_invalidation(db, [], [(2, 3, "a", "x", b"\x01")])
            expected = sorted(
                (obj, container, point, "x")
                for point in (2, 3)
                for container in ("a", "b")
                for obj in (b"", b"\x01", b"\x01\x02")
            )
            self.assertEqual(removed, expected)


def benchmark():
    report = {
        "sqlite_version": sqlite3.sqlite_version,
        "functions": 12970,
        "objects": 77820,
        "dependencies": 12970,
        "synthetic": True,
    }
    for legacy in (True, False):
        db = database()
        db.executemany(
            "INSERT INTO objects VALUES (?,?,?,?,?)",
            (
                (point, "llvm", "config", i.to_bytes(4, "big"), b"data")
                for point in range(1, 7)
                for i in range(12970)
            ),
        )
        db.executemany(
            "INSERT INTO dependencies VALUES (?,?,?,?,?,?)",
            ((1, 6, "llvm", "config", i.to_bytes(4, "big"), "/changed") for i in range(12970)),
        )
        db.commit()
        began = time.monotonic()
        db.set_progress_handler(lambda: int(time.monotonic() - began > 5), 10000)
        try:
            removed, _ = run_invalidation(db, ["/changed"], [], legacy)
            result = {"status": "completed", "removed": len(removed)}
        except sqlite3.OperationalError as error:
            if str(error) != "interrupted":
                raise
            result = {"status": "interrupted", "limit_seconds": 5}
        finally:
            db.set_progress_handler(None, 0)
        result["seconds"] = round(time.monotonic() - began, 3)
        report["legacy" if legacy else "optimized"] = result
        db.close()
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    if sys.argv[1:] == ["--benchmark"]:
        benchmark()
    else:
        unittest.main()
