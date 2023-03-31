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

from s3ql.logging import logging  # Ensure use of custom logger class
import tempfile
from argparse import Namespace
import pytest
from s3ql import database, sqlite3ext
from s3ql.backends import local
from s3ql.database import Connection, download_metadata, upload_metadata
from pytest_checklogs import assert_logs


def test_track_dirty():
    blocksize = 4096
    sqlite3ext.reset()
    with tempfile.NamedTemporaryFile() as tmpfh:
        db = Connection(tmpfh.name, blocksize)
        db.execute("CREATE TABLE foo (id INT);")
        db.execute("INSERT INTO FOO VALUES(?)", (23,))
        db.execute("INSERT INTO FOO VALUES(?)", (25,))
        db.execute("INSERT INTO FOO VALUES(?)", (30,))

        assert db.dirty_blocks.get_count() >= 1

        db.dirty_blocks.clear()
        assert db.dirty_blocks.get_count() == 0

        db.execute("UPDATE FOO SET id=24 WHERE ID=23")

        assert db.dirty_blocks.get_count() >= 1


@pytest.fixture
def backend():
    with tempfile.TemporaryDirectory(prefix="s3ql-backend-") as backend_dir:
        yield local.Backend(Namespace(storage_url="local://" + backend_dir))


@pytest.mark.parametrize("incremental", (True, False))
def test_upload_download(backend, incremental):
    blocksize = 1024
    sqlite3ext.reset()
    with tempfile.NamedTemporaryFile() as tmpfh:
        db = Connection(tmpfh.name, blocksize)
        db.execute("CREATE TABLE foo (val TEXT);")
        for i in range(50):
            db.execute("INSERT INTO FOO VALUES(?)", ("foo" * i,))
        db.close()

        params = {"metadata-block-size": blocksize}
        upload_metadata(backend, db, params, incremental)

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
            db.checkpoint()

        q.close()
        db.checkpoint()
