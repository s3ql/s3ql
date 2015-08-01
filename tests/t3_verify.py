#!/usr/bin/env python3
'''
t3_verify.py - this file is part of S3QL.

Copyright © 2014 Nikolaus Rath <Nikolaus@rath.org>

This work can be distributed under the terms of the GNU GPLv3.
'''

if __name__ == '__main__':
    import pytest
    import sys
    sys.exit(pytest.main([__file__] + sys.argv[1:]))

from s3ql.backends import local
from s3ql.backends.comprenc import ComprencBackend
from s3ql.mkfs import init_tables
from s3ql.metadata import create_tables
from s3ql.database import Connection
from s3ql import verify
from common import catch_logmsg
import io
import logging
import shutil
import tempfile
import pytest

@pytest.yield_fixture()
def backend():
    backend_dir = tempfile.mkdtemp(prefix='s3ql-backend-')
    be = local.Backend('local://' + backend_dir, None, None)
    try:
        yield be
    finally:
        be.close()
        shutil.rmtree(backend_dir)

@pytest.yield_fixture()
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

def test_retrieve(backend, db):
    plain_backend = backend
    backend = ComprencBackend(b'schnorz', ('zlib', 6), plain_backend)

    # Create a few objects in db
    obj_ids = (22, 25, 30, 31)
    for id_ in obj_ids:
        db.execute('INSERT INTO objects (id, refcount, size) VALUES(?, ?, ?)',
                   (id_, 1, 27 * id_))

    # Object one will be missing in backend

    # Object two will have a checksum error in the data
    key = 's3ql_data_%d' % obj_ids[1]
    backend[key] = b'some data that will be broken on a data check'
    (raw, meta) = plain_backend.fetch(key)
    raw = bytearray(raw)
    assert len(raw) > 20
    raw[-10:-6] = b'forg'
    plain_backend.store(key, raw, meta)

    # Object three will have a checksum error in the metadata
    key = 's3ql_data_%d' % obj_ids[2]
    backend.store(key, b'some data that will be broken on a metadata check',
                  { 'meta-key1': 'some textual data that just increases',
                    'meta-key2': 'the metadata size so that we can tamper with it' })
    meta = plain_backend.lookup(key)
    raw = bytearray(meta['data'])
    assert len(raw) > 20
    raw[-10:-6] = b'forg'
    meta['data'] = raw
    plain_backend.update_meta(key, meta)

    # Object four will be ok
    backend['s3ql_data_%d' % obj_ids[3]] = b'some data that is well'

    # When using a single thread, we can fake the backend factory
    def backend_factory():
        return backend

    missing_fh = io.StringIO()
    corrupted_fh = io.StringIO()

    with catch_logmsg('^Backend seems to have lost', count=1, level=logging.WARNING), \
         catch_logmsg('^Object %d is corrupted', count=1, level=logging.WARNING):
        verify.retrieve_objects(db, backend_factory, corrupted_fh, missing_fh,
                                thread_count=1, full=False)
    assert missing_fh.getvalue() == 's3ql_data_%d\n' % obj_ids[0]
    assert corrupted_fh.getvalue() == 's3ql_data_%d\n' % obj_ids[2]

    missing_fh = io.StringIO()
    corrupted_fh = io.StringIO()
    with catch_logmsg('^Backend seems to have lost', count=1, level=logging.WARNING), \
         catch_logmsg('^Object %d is corrupted', count=2, level=logging.WARNING):
        verify.retrieve_objects(db, backend_factory, corrupted_fh, missing_fh,
                                thread_count=1, full=True)
    assert missing_fh.getvalue() == 's3ql_data_%d\n' % obj_ids[0]
    assert corrupted_fh.getvalue() == ('s3ql_data_%d\n'*2) % obj_ids[1:3]
