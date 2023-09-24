#!/usr/bin/env python3
'''
t3_verify.py - this file is part of S3QL.

Copyright © 2014 Nikolaus Rath <Nikolaus@rath.org>

This work can be distributed under the terms of the GNU GPLv3.
'''

if __name__ == '__main__':
    import sys

    import pytest

    sys.exit(pytest.main([__file__] + sys.argv[1:]))

import io
import logging
import shutil
import tempfile
from argparse import Namespace

import pytest
from pytest_checklogs import assert_logs

from s3ql import verify
from s3ql.backends import local
from s3ql.backends.comprenc import ComprencBackend, sha256
from s3ql.database import Connection, create_tables
from s3ql.mkfs import init_tables


@pytest.fixture()
def backend():
    backend_dir = tempfile.mkdtemp(prefix='s3ql-backend-')
    plain_backend = local.Backend(Namespace(storage_url='local://' + backend_dir))
    backend = ComprencBackend(b'schnorz', ('zlib', 6), plain_backend)
    try:
        yield backend
    finally:
        backend.close()
        shutil.rmtree(backend_dir)


@pytest.fixture()
def db():
    dbfile = tempfile.NamedTemporaryFile()
    db = Connection(dbfile.name)
    create_tables(db)
    init_tables(db)
    try:
        yield db
    finally:
        db.close()
        dbfile.close()


@pytest.mark.parametrize("full", (True, False))
def test_missing(backend, db, full):
    # Create two objects, one will be missing
    obj_ids = (22, 25)
    missing_idx = 1
    for id_ in obj_ids:
        data = b'just some data that no-one really cares about %d' % id_
        db.execute(
            'INSERT INTO objects (id, refcount, phys_size, length, hash) VALUES(?, ?, ?, ?, ?)',
            (id_, 1, 27 * id_, len(data), sha256(data)),
        )

        if id_ != obj_ids[missing_idx]:
            key = 's3ql_data_%d' % obj_ids[0]
            backend[key] = data

    # When using a single thread, we can fake the backend factory
    backend_factory = lambda: backend

    missing_fh = io.StringIO()
    corrupted_fh = io.StringIO()
    with assert_logs('^Backend seems to have lost', count=1, level=logging.WARNING):
        verify.retrieve_objects(
            db, backend_factory, corrupted_fh, missing_fh, thread_count=1, full=full
        )
    assert missing_fh.getvalue() == 's3ql_data_%d\n' % obj_ids[missing_idx]
    assert corrupted_fh.getvalue() == ''


@pytest.mark.parametrize("full", (True, False))
def test_corrupted_head(backend, db, full):
    obj_ids = (30, 31)
    corrupted_idx = 1
    for id_ in obj_ids:
        data = b'just some data that no-one really cares about %d' % id_
        db.execute(
            'INSERT INTO objects (id, refcount, phys_size, length, hash) VALUES(?, ?, ?, ?, ?)',
            (id_, 1, 27 * id_, len(data), sha256(data)),
        )

        key = 's3ql_data_%d' % id_
        backend[key] = data

    # Introduce checksum error in metadata
    key = 's3ql_data_%d' % obj_ids[corrupted_idx]
    data = b'some data that will be broken on a metadata check'
    backend.store(
        key,
        data,
        {
            'meta-key1': 'some textual data that just increases',
            'meta-key2': 'the metadata size so that we can tamper with it',
        },
    )
    meta = backend.backend.lookup(key)
    raw = bytearray(meta['data'])
    assert len(raw) > 20
    raw[-10:-6] = b'forg'
    meta['data'] = raw
    backend.backend.store(key, data, meta)

    # When using a single thread, we can fake the backend factory
    backend_factory = lambda: backend

    missing_fh = io.StringIO()
    corrupted_fh = io.StringIO()
    with assert_logs('^Object %d is corrupted', count=1, level=logging.WARNING):
        verify.retrieve_objects(
            db, backend_factory, corrupted_fh, missing_fh, thread_count=1, full=full
        )
    assert missing_fh.getvalue() == ''
    assert corrupted_fh.getvalue() == 's3ql_data_%d\n' % obj_ids[1]


@pytest.mark.parametrize("full", (True, False))
def test_corrupted_body(backend, db, full):
    obj_ids = (35, 40)
    corrupted_idx = 1
    for id_ in obj_ids:
        data = b'just some data that no-one really cares about %d' % id_
        db.execute(
            'INSERT INTO objects (id, refcount, phys_size, length, hash) VALUES(?, ?, ?, ?, ?)',
            (id_, 1, 27 * id_, len(data), sha256(data)),
        )
        backend['s3ql_data_%d' % id_] = data

    # Introduce checksum error
    key = 's3ql_data_%d' % obj_ids[corrupted_idx]
    (raw, meta) = backend.backend.fetch(key)
    raw = bytearray(raw)
    assert len(raw) > 20
    raw[-10:-6] = b'forg'
    backend.backend.store(key, raw, meta)

    # When using a single thread, we can fake the backend factory
    backend_factory = lambda: backend

    missing_fh = io.StringIO()
    corrupted_fh = io.StringIO()

    if full:
        with assert_logs('^Object %d is corrupted', count=1, level=logging.WARNING):
            verify.retrieve_objects(
                db, backend_factory, corrupted_fh, missing_fh, thread_count=1, full=full
            )
            assert missing_fh.getvalue() == ''
            assert corrupted_fh.getvalue() == 's3ql_data_%d\n' % obj_ids[corrupted_idx]
    else:
        # Should not show up when looking just at HEAD
        verify.retrieve_objects(
            db, backend_factory, corrupted_fh, missing_fh, thread_count=1, full=full
        )
        assert missing_fh.getvalue() == ''
        assert corrupted_fh.getvalue() == ''


@pytest.mark.parametrize("full", (True, False))
def test_truncated_body(backend, db, full):
    id_ = 35
    data = b'just some data that no-one really cares about'
    db.execute(
        'INSERT INTO objects (id, refcount, phys_size, length) VALUES(?, ?, ?, ?)',
        (id_, 1, 27 * id_, len(data) + 1),
    )
    key = 's3ql_data_%d' % id_
    backend[key] = data

    # When using a single thread, we can fake the backend factory
    backend_factory = lambda: backend

    missing_fh = io.StringIO()
    corrupted_fh = io.StringIO()

    if full:
        with assert_logs('^Object %d is corrupted', count=1, level=logging.WARNING):
            verify.retrieve_objects(
                db, backend_factory, corrupted_fh, missing_fh, thread_count=1, full=full
            )
            assert missing_fh.getvalue() == ''
            assert corrupted_fh.getvalue() == 's3ql_data_%d\n' % id_
    else:
        # Should not show up when looking just at HEAD
        verify.retrieve_objects(
            db, backend_factory, corrupted_fh, missing_fh, thread_count=1, full=full
        )
        assert missing_fh.getvalue() == ''
        assert corrupted_fh.getvalue() == ''


def test_corrupted_hash(backend, db):
    obj_ids = (35, 40)
    corrupted_idx = 0
    for id_ in obj_ids:
        data = b'just some data that no-one really cares about %d' % id_
        db.execute(
            'INSERT INTO objects (id, refcount, phys_size, length, hash) VALUES(?, ?, ?, ?, ?)',
            (id_, 1, 27 * id_, len(data), sha256(data)),
        )
        backend['s3ql_data_%d' % id_] = data

    # Introduce checksum error
    db.execute('UPDATE objects SET hash=? WHERE id=?', (sha256(b'foobar'), obj_ids[corrupted_idx]))

    # When using a single thread, we can fake the backend factory
    backend_factory = lambda: backend

    missing_fh = io.StringIO()
    corrupted_fh = io.StringIO()

    with assert_logs('^Object %d is corrupted', count=1, level=logging.WARNING):
        verify.retrieve_objects(
            db, backend_factory, corrupted_fh, missing_fh, thread_count=1, full=True
        )
        assert missing_fh.getvalue() == ''
        assert corrupted_fh.getvalue() == 's3ql_data_%d\n' % obj_ids[corrupted_idx]
