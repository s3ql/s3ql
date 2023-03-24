'''
metadata.py - this file is part of S3QL.

Copyright © 2008 Nikolaus Rath <Nikolaus@rath.org>

This work can be distributed under the terms of the GNU GPLv3.
'''

from .logging import logging # Ensure use of custom logger class
from .database import Connection
from . import BUFSIZE
from .common import freeze_basic_mapping, thaw_basic_mapping, sha256_fh
from .backends.common import AbstractBackend
import os
import re
from typing import Optional

log = logging.getLogger(__name__)


def create_tables(conn):
    # Table of storage objects
    # Refcount is included for performance reasons
    # phys_size is the number of bytes stored in the backend (i.e., after compression).
    # length is the logical size in the filesystem.
    # phys_size == -1 indicates block has not been uploaded yet
    conn.execute("""
    CREATE TABLE objects (
        id          INTEGER PRIMARY KEY AUTOINCREMENT,
        hash        BLOB(32) UNIQUE,
        refcount    INT NOT NULL,
        phys_size   INT NOT NULL,
        length      INT NOT NULL
    )""")

    # Table with filesystem metadata
    # The number of links `refcount` to an inode can in theory
    # be determined from the `contents` table. However, managing
    # this separately should be significantly faster (the information
    # is required for every getattr!)
    conn.execute("""
    CREATE TABLE inodes (
        -- id has to specified *exactly* as follows to become
        -- an alias for the rowid.
        id        INTEGER PRIMARY KEY AUTOINCREMENT,
        uid       INT NOT NULL,
        gid       INT NOT NULL,
        mode      INT NOT NULL,
        mtime_ns  INT NOT NULL,
        atime_ns  INT NOT NULL,
        ctime_ns  INT NOT NULL,
        refcount  INT NOT NULL,
        size      INT NOT NULL DEFAULT 0,
        rdev      INT NOT NULL DEFAULT 0,
        locked    BOOLEAN NOT NULL DEFAULT 0
    )""")

    # Further Blocks used by inode (blockno >= 1)
    conn.execute("""
    CREATE TABLE inode_blocks (
        inode     INTEGER NOT NULL REFERENCES inodes(id),
        blockno   INT NOT NULL,
        obj_id    INTEGER NOT NULL REFERENCES objects(id),
        PRIMARY KEY (inode, blockno)
    )""")

    # Symlinks
    conn.execute("""
    CREATE TABLE symlink_targets (
        inode     INTEGER PRIMARY KEY REFERENCES inodes(id),
        target    BLOB NOT NULL
    )""")

    # Names of file system objects
    conn.execute("""
    CREATE TABLE names (
        id     INTEGER PRIMARY KEY,
        name   BLOB NOT NULL,
        refcount  INT NOT NULL,
        UNIQUE (name)
    )""")

    # Table of filesystem objects
    # rowid is used by readdir() to restart at the correct position
    conn.execute("""
    CREATE TABLE contents (
        rowid     INTEGER PRIMARY KEY AUTOINCREMENT,
        name_id   INT NOT NULL REFERENCES names(id),
        inode     INT NOT NULL REFERENCES inodes(id),
        parent_inode INT NOT NULL REFERENCES inodes(id),

        UNIQUE (parent_inode, name_id)
    )""")

    # Extended attributes
    conn.execute("""
    CREATE TABLE ext_attributes (
        inode     INTEGER NOT NULL REFERENCES inodes(id),
        name_id   INTEGER NOT NULL REFERENCES names(id),
        value     BLOB NOT NULL,

        PRIMARY KEY (inode, name_id)
    )""")

    # Shortcuts
    conn.execute("""
    CREATE VIEW contents_v AS
    SELECT * FROM contents JOIN names ON names.id = name_id
    """)
    conn.execute("""
    CREATE VIEW ext_attributes_v AS
    SELECT * FROM ext_attributes JOIN names ON names.id = name_id
    """)


class DatabaseChecksumError(RuntimeError):
    '''Raised when the downloaded database does not have the expected checksum'''

    def __init__(self, name: str, expected: str, actual: str):
        super().__init__()
        self.name = name
        self.expected = expected
        self.actual = actual

    def __str__(self):
        return f'File {self.name} has checksum {self.actual}, expected {self.expected}'


def download_metadata(backend: AbstractBackend, db_file: str,
                      params: dict, failsafe=False):
    '''Download metadata from backend into *db_file*

    If *failsafe* is True, do not truncate file and do not verify
    checksum.
    '''

    blocksize = params['metadata-block-size']
    file_size = params['db-size']

    to_remove = []
    with open(db_file, 'w+b', buffering=0) as fh:
        log.info('Downloading metadata...')
        for obj in backend.list('s3ql_metadata_'):
            hit = re.match('^s3ql_metadata_([0-9a-f]+)$', obj)
            if not hit:
                log.warning('Unexpected object in backend: %s, ignoring', obj)
                continue
            blockno = int(hit.group(1), base=16)
            off = blockno * blocksize
            if off > file_size:
                to_remove.append(obj)
                continue
            log.debug('download_metadata: storing %s at pos %d', obj, off)
            fh.seek(off)
            backend.readinto(obj, fh)

        log.debug('Deleting obsolete objects...')
        backend.delete_multi(to_remove)

        if not failsafe:
            log.debug('download_metadata: truncating file to %d bytes', file_size)
            fh.truncate(file_size)

            log.info('Calculating metadata checksum...')
            digest = sha256_fh(fh).hexdigest()
            log.debug('download_metadata: digest is %s', digest)
            if params['db-md5'] != digest:
                raise DatabaseChecksumError(db_file, params['db-md5'], digest)

    return Connection(db_file, blocksize)


def upload_metadata(
    backend: AbstractBackend, db: Connection, params: Optional[dict], incremental: bool = True
) -> None:
    '''Upload metadata to backend

    If *params* is given, calculate checksum and update *params*
    in place.
    '''

    blocksize = db.blocksize
    with open(db.file, 'rb', buffering=0) as fh:
        db_size = fh.seek(0, os.SEEK_END)
        if incremental:
            next_dirty_block = db.dirty_blocks.pop
        else:
            all_blocks = iter(range(0, db_size // blocksize + 1))

            def next_dirty_block():
                try:
                    return next(all_blocks)
                except StopIteration:
                    raise KeyError()

        while True:
            try:
                blockno = next_dirty_block()
            except KeyError:
                break

            log.debug('upload_metadata: uploading block %d', blockno)
            fh.seek(blockno * blocksize)
            backend.store('s3ql_metadata_%012x' % blockno, fh.read(blocksize))

        if params is None:
            return

        log.info('Calculating metadata checksum...')
        params['db-size'] = db_size
        digest = sha256_fh(fh).hexdigest()
        log.debug('upload_metadata: digest is %s', digest)
        params['db-md5'] = digest


def store_and_upload_params(backend, cachepath: str, params: dict):
    buf = freeze_basic_mapping(params)
    backend['s3ql_params'] = buf

    filename = cachepath + '.params'
    tmpname  = filename + '.tmp'
    with open(tmpname, 'wb') as fh:
        fh.write(buf)
        # Fsync to make sure that the updated sequence number is committed to
        # disk. Otherwise, a crash immediately after mount could result in both
        # the local and remote metadata appearing to be out of date.
        fh.flush()
        os.fsync(fh.fileno())

    # we need to flush the dirents too.
    # stackoverflow.com/a/41362774
    # stackoverflow.com/a/5809073
    os.rename(tmpname, filename)
    dirfd = os.open(os.path.dirname(filename), os.O_DIRECTORY)
    try:
        os.fsync(dirfd)
    finally:
        os.close(dirfd)


def read_params(backend, cachepath: str):
    params = thaw_basic_mapping(backend['s3ql_params'])

    filename = cachepath + '.params'
    local_params = None
    if os.path.exists(filename):
        with open(filename, 'rb') as fh:
            local_params = thaw_basic_mapping(fh.read())

    return (local_params, params)
