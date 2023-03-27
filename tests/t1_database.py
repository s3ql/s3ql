#!/usr/bin/env python3
"""
t1_database.py - this file is part of S3QL.

Copyright © 2023 Nikolaus Rath <Nikolaus@rath.org>

This work can be distributed under the terms of the GNU GPLv3.
"""

if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main([__file__] + sys.argv[1:]))

import tempfile
from argparse import Namespace
from s3ql import database
from s3ql.database import upload_metadata, download_metadata, Connection
from s3ql.backends import local
import pytest


def test_track_dirty():
    blocksize = 1024
    database.vfs.reset()
    with tempfile.NamedTemporaryFile() as tmpfh:
        db = Connection(tmpfh.name, blocksize)
        db.execute("CREATE TABLE foo (id INT);")
        db.execute("INSERT INTO FOO VALUES(?)", (23,))
        db.execute("INSERT INTO FOO VALUES(?)", (25,))
        db.execute("INSERT INTO FOO VALUES(?)", (30,))

        assert len(db.dirty_blocks) > 1

        db.dirty_blocks.clear()

        db.execute("UPDATE FOO SET id=24 WHERE ID=23")

        assert len(db.dirty_blocks) >= 1


@pytest.yield_fixture
def backend():
    with tempfile.TemporaryDirectory(prefix="s3ql-backend-") as backend_dir:
        yield local.Backend(Namespace(storage_url="local://" + backend_dir))


@pytest.mark.parametrize("incremental", (True, False))
def test_upload_download(backend, incremental):
    blocksize = 1024
    database.vfs.reset()
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
