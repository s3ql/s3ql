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
from unittest.mock import MagicMock

import pytest
from pytest_checklogs import assert_logs
from t2_block_cache import random_data

from s3ql import sqlite3ext
from s3ql.backends import local
from s3ql.backends.common import AbstractBackend
from s3ql.database import Connection, download_metadata, expire_objects, upload_metadata

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
    params = {"metadata-block-size": BLOCKSIZE, 'seq_no': 1}
    with tempfile.NamedTemporaryFile() as tmpfh:
        db = Connection(tmpfh.name, BLOCKSIZE)
        db.execute("CREATE TABLE foo (id INT, data BLOB);")
        for i in range(rows):
            db.execute("INSERT INTO foo VALUES(?, ?)", (i, DUMMY_DATA))

        db.checkpoint()
        upload_metadata(backend, db, params, incremental=incremental)

        # Shrink database
        db.execute('DELETE FROM foo WHERE id >= ?', (rows // 2,))
        db.execute('VACUUM')
        db.checkpoint()
        upload_metadata(backend, db, params, incremental=incremental)

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


def test_versioning(backend):
    sqlite3ext.reset()
    rows = 11
    params = {"metadata-block-size": BLOCKSIZE, 'seq_no': 1}
    versions = []
    with tempfile.NamedTemporaryFile() as tmpfh:
        db = Connection(tmpfh.name, BLOCKSIZE)
        db.execute("CREATE TABLE foo (id INT, data BLOB);")
        for i in range(rows):
            db.execute("INSERT INTO foo VALUES(?, ?)", (i, DUMMY_DATA))

        def upload():
            db.checkpoint()
            upload_metadata(backend, db, params)
            tmpfh.seek(0)
            versions.append((params.copy(), tmpfh.read()))
            params['seq_no'] += 1

        upload()

        # Make some modifications
        db.execute('INSERT INTO foo(id, data) VALUES(?, ?)', (rows + 1, b'short data'))
        upload()

        db.execute('INSERT INTO foo(id, data) VALUES(?, ?)', (2 * rows + 1, DUMMY_DATA))
        upload()

        db.execute("UPDATE FOO SET data=? WHERE id=?", (random_data(len(DUMMY_DATA)), 0))
        db.execute("UPDATE FOO SET data=? WHERE id=?", (random_data(len(DUMMY_DATA)), rows // 2))
        upload()

        db.execute('DELETE FROM foo WHERE id >= ?', (rows // 2,))
        db.execute('VACUUM')
        upload()

        db.close()

    for (params, ref_db) in versions:
        with tempfile.NamedTemporaryFile() as tmpfh2:
            download_metadata(backend, tmpfh2.name, params)
            assert tmpfh2.read() == ref_db


def test_expiration():
    def do_test(present, should_keep, versions):
        present_objs = [
            's3ql_metadata_%012x_%010x' % (blockno, seq)
            for (blockno, seqs) in enumerate(present)
            for seq in seqs
        ]
        wanted_objs = [
            's3ql_metadata_%012x_%010x' % (blockno, seq)
            for (blockno, seqs) in enumerate(should_keep)
            for seq in seqs
        ]
        to_delete = set(present_objs) - set(wanted_objs)

        def delete_multi(objs):
            assert set(objs) == to_delete

        backend = MagicMock()
        backend.list.return_value = present_objs
        backend.delete_multi.side_effect = delete_multi

        expire_objects(backend, versions_to_keep=versions)

        assert backend.delete_multi.call_count == 1

    do_test(
        present=[
            [0, 1, 2, 3, 4, 5],  # block 1
            [0, 1, 2, 3, 4, 5],  # block 2
            [0, 1, 2, 3, 4, 5],  # block 3
            [0, 1, 2, 3, 4, 5],  # block 4
        ],
        should_keep=[
            [5, 4, 3],  # block 1
            [5, 4, 3],  # block 2
            [5, 4, 3],  # block 3
            [5, 4, 3],  # block 4
        ],
        versions=3,
    )

    do_test(
        present=[
            [5, 3, 2, 1],  # block 1
            [5, 3, 1],  # block 2
            [5, 2, 1],  # block 3
        ],
        should_keep=[
            [5, 3, 2],  # block 1
            [5, 3, 1],  # block 2
            [5, 2, 2],  # block 3
        ],
        versions=3,
    )

    do_test(
        present=[
            [3, 2, 1, 0],  # block 1
            [5, 1, 0],  # block 2
            [3, 1, 0],  # block 3
        ],
        should_keep=[
            [3, 3, 2],  # block 1
            [5, 1, 1],  # block 2
            [3, 3, 1],  # block 3
        ],
        versions=3,
    )
