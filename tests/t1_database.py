#!/usr/bin/env python3
"""
t1_database.py - this file is part of S3QL.

Copyright © 2023 Nikolaus Rath <Nikolaus@rath.org>

This work can be distributed under the terms of the GNU GPLv3.
"""

if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main([__file__] + sys.argv[1:]))

import logging
import tempfile
from argparse import Namespace

import pytest
from pytest_checklogs import assert_logs
from t2_block_cache import random_data

from s3ql import sqlite3ext
from s3ql.backends import local
from s3ql.backends.common import AbstractBackend
from s3ql.database import Connection, download_metadata, upload_metadata

# Page size is 4k, so block sizes smaller than that don't make sense
BLOCKSIZE = 4096

# Make sure every row requires at least one block of storage
DUMMY_DATA = random_data(BLOCKSIZE)


def test_track_dirty():
    rows = 11
    sqlite3ext.reset()
    with tempfile.NamedTemporaryFile() as tmpfh:
        db = Connection(tmpfh.name, BLOCKSIZE)
        db.execute("CREATE TABLE foo (id INT, data BLOB);")
        for i in range(rows):
            db.execute("INSERT INTO FOO VALUES(?, ?)", (i, DUMMY_DATA))

        db.checkpoint()
        assert db.dirty_blocks.get_count() >= rows

        db.dirty_blocks.clear()
        assert db.dirty_blocks.get_count() == 0

        db.execute("UPDATE FOO SET data=? WHERE id=?", (random_data(len(DUMMY_DATA)), 0))
        db.checkpoint()
        assert rows > db.dirty_blocks.get_count() > 0

        db.close()


@pytest.fixture
def backend():
    with tempfile.TemporaryDirectory(prefix="s3ql-backend-") as backend_dir:
        yield local.Backend(Namespace(storage_url="local://" + backend_dir))


@pytest.mark.parametrize("incremental", (True, False))
def test_upload_download(backend, incremental):
    sqlite3ext.reset()
    rows = 11
    params = {"metadata-block-size": BLOCKSIZE}
    with tempfile.NamedTemporaryFile() as tmpfh:
        db = Connection(tmpfh.name, BLOCKSIZE)
        db.execute("CREATE TABLE foo (id INT, data BLOB);")
        for i in range(rows):
            db.execute("INSERT INTO foo VALUES(?, ?)", (i, DUMMY_DATA))

        db.checkpoint()
        upload_metadata(backend, db, params, incremental)

        # Shrink database
        db.execute('DELETE FROM foo WHERE id >= ?', (rows // 2,))
        db.execute('VACUUM')
        db.checkpoint()
        upload_metadata(backend, db, params, incremental)

        db.close()

        with tempfile.NamedTemporaryFile() as tmpfh2:
            download_metadata(backend, tmpfh2.name, params)

            tmpfh.seek(0)
            tmpfh2.seek(0)
            assert tmpfh.read() == tmpfh2.read()


def test_checkpoint():
    with tempfile.NamedTemporaryFile() as tmpfh:
        db = Connection(tmpfh.name)
        db.execute("CREATE TABLE foo (id INT);")
        db.execute("INSERT INTO FOO VALUES(?)", (23,))
        db.execute("INSERT INTO FOO VALUES(?)", (25,))
        db.execute("INSERT INTO FOO VALUES(?)", (30,))

        db.checkpoint()

        q = db.query('SELECT * FROM foo')
        db.execute("INSERT INTO FOO VALUES(?)", (35,))
        with assert_logs('^Unable to checkpoint WAL', count=1, level=logging.WARNING):
            with assert_logs(
                '^sqlite3: statement aborts at.+SQLITE_LOCKED', count=1, level=logging.WARNING
            ):
                db.checkpoint()

        q.close()
        db.checkpoint()
        db.close()


def get_metadata_obj_count(backend: AbstractBackend):
    i = 0
    for _ in backend.list('s3ql_metadata_'):
        i += 1
    return i


def test_truncate(backend: AbstractBackend):
    sqlite3ext.reset()
    rows = 11
    params = {"metadata-block-size": BLOCKSIZE}
    with tempfile.NamedTemporaryFile() as tmpfh:
        db = Connection(tmpfh.name, BLOCKSIZE)
        db.execute("CREATE TABLE foo (id INT, data BLOB);")
        for i in range(rows):
            db.execute("INSERT INTO FOO VALUES(?, ?)", (i, DUMMY_DATA))

        db.checkpoint()
        upload_metadata(backend, db, params)
        obj_count = get_metadata_obj_count(backend)
        assert obj_count >= rows

        # Shrink database
        db.execute('DELETE FROM foo WHERE id >= ?', (rows // 2,))
        db.execute('VACUUM')
        db.checkpoint()

        # Incremental upload should not remove objects
        upload_metadata(backend, db, params, incremental=True)
        assert get_metadata_obj_count(backend) == obj_count

        # Full upload should remove some objects
        upload_metadata(backend, db, params, incremental=False)
        assert obj_count - get_metadata_obj_count(backend) >= rows // 2

        db.close()
