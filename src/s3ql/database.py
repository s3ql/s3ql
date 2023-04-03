'''
database.py - this file is part of S3QL.

Copyright © 2008 Nikolaus Rath <Nikolaus@rath.org>

This work can be distributed under the terms of the GNU GPLv3.


Module Attributes:
-----------

:initsql:      SQL commands that are executed whenever a new
               connection is created.
'''


from .logging import logging, QuietError  # Ensure use of custom logger class
from contextlib import contextmanager

from typing import Union, Optional, List
import apsw
import os
from . import BUFSIZE, CURRENT_FS_REV
from .common import freeze_basic_mapping, thaw_basic_mapping, sha256_fh
from .backends.common import AbstractBackend, NoSuchObject
import re
from . import sqlite3ext

log = logging.getLogger(__name__)

sqlite_ver = tuple([int(x) for x in apsw.sqlitelibversion().split('.')])
if sqlite_ver < (3, 7, 0):
    raise QuietError('SQLite version too old, must be 3.7.0 or newer!\n')


initsql = (
    'PRAGMA journal_mode = WAL',
    'PRAGMA synchronous = NORMAL',
    'PRAGMA foreign_keys = OFF',
    'PRAGMA locking_mode = EXCLUSIVE',
    'PRAGMA recursize_triggers = on',
    'PRAGMA temp_store = FILE',
    'PRAGMA legacy_file_format = off',
)


def sqlite3_log(errcode, message):
    if errcode == apsw.SQLITE_NOTICE:
        log.debug('sqlite3: %s', message)
    elif errcode == apsw.SQLITE_WARNING:
        log.warning('sqlite3: %s', message)
    elif errcode == apsw.SQLITE_ERROR:
        log.error('sqlite3: %s', message)
    elif errcode == apsw.SQLITE_OK:
        log.info('sqlite3: %s', message)
    else:
        errstr = apsw.mapping_extended_result_codes.get(
            errcode, apsw.mapping_result_codes[errcode & 255]
        )
        log.warning('sqlite3: : %s (%s)', message, errstr)


apsw.config(apsw.SQLITE_CONFIG_LOG, sqlite3_log)


class Connection:
    '''
    This class wraps an APSW connection object. It should be used instead of any
    native APSW cursors.

    It provides methods to directly execute SQL commands and creates apsw
    cursors dynamically.

    Instances are not thread safe. They can be passed between threads,
    but must not be called concurrently.

    Attributes
    ----------

    :conn:     apsw connection object
    '''

    def __init__(self, file_: str, blocksize: Optional[int] = None):
        if blocksize:
            sqlite3ext.set_blocksize(blocksize)
            file_ = os.path.abspath(file_)
            self.dirty_blocks = sqlite3ext.track_writes(file_)
            self.conn = apsw.Connection(file_, vfs=sqlite3ext.get_vfsname())
        else:
            self.conn = apsw.Connection(file_)
        self.file = file_
        self.blocksize = blocksize

        cur = self.conn.cursor()

        for s in initsql:
            cur.execute(s)

        # Disable bloom filter optimization when we detect that sqlite is a buggy version
        # https://sqlite.org/forum/forumpost/031e262a89b6a9d2
        if (3, 38, 0) <= sqlite_ver < (3, 38, 4):
            log.warning(
                'Your sqlite version has bugs in its bloom filter optimization. '
                'We disable the usage of some indexes (in fsck.s3ql). '
                'Consider updating sqlite to 3.38.5+.'
            )
            self.fixes = {'dbf': '+'}
            # https://sqlite.org/forum/forumpost/0d3200f4f3bcd3a3
            cur.execute('PRAGMA automatic_index = off')
        else:
            self.fixes = {'dbf': ''}

    def close(self):
        self.conn.close()

    def get_size(self):
        '''Return size of database file'''

        if self.file is not None and self.file not in ('', ':memory:'):
            return os.path.getsize(self.file)
        else:
            return 0

    def query(self, *a, **kw):
        '''Return iterator over results of given SQL statement

        If the caller does not retrieve all rows the iterator's close() method
        should be called as soon as possible to terminate the SQL statement
        (otherwise it may block execution of other statements). To this end,
        the iterator may also be used as a context manager.
        '''

        return ResultSet(self.conn.cursor().execute(*a, **kw))

    def execute(self, *a, **kw):
        '''Execute the given SQL statement. Return number of affected rows'''

        self.conn.cursor().execute(*a, **kw)
        return self.changes()

    def rowid(self, *a, **kw):
        """Execute SQL statement and return last inserted rowid"""

        self.conn.cursor().execute(*a, **kw)
        return self.conn.last_insert_rowid()

    def has_val(self, *a, **kw):
        '''Execute statement and check if it gives result rows'''

        res = self.conn.cursor().execute(*a, **kw)
        try:
            next(res)
        except StopIteration:
            return False
        else:
            # Finish the active SQL statement
            res.close()
            return True

    def checkpoint(self):
        '''Checkpoint the WAL'''

        try:
            row = self.get_row('PRAGMA main.wal_checkpoint(RESTART)')
        except apsw.LockedError:  # https://sqlite.org/forum/forumpost/9e1ad35f07
            row = (1, 0, 0)
        else:
            log.debug('checkpoint: wrote %d/%d pages to db file', row[2], row[1])
        if row[0] != 0:
            log.warning(
                'Unable to checkpoint WAL - please report at https://github.com/s3ql/s3ql/issues'
            )

    @contextmanager
    def transaction(self, rollback_on_error: bool):
        cur = self.conn.cursor()
        cur.execute('BEGIN TRANSACTION')
        try:
            yield
        except:
            if rollback_on_error:
                cur.execute('ROLLBACK')
            else:
                cur.execute('COMMIT')
            raise
        else:
            cur.execute('COMMIT')

    def get_val(self, *a, **kw):
        """Execute statement and return first element of first result row.

        If there is no result row, raises `NoSuchRowError`. If there is more
        than one row, raises `NoUniqueValueError`.
        """

        return self.get_row(*a, **kw)[0]

    def get_list(self, *a, **kw):
        """Execute select statement and returns result list"""

        return list(self.query(*a, **kw))

    def get_row(self, *a, **kw):
        """Execute select statement and return first row.

        If there are no result rows, raises `NoSuchRowError`. If there is more
        than one result row, raises `NoUniqueValueError`.
        """

        res = self.conn.cursor().execute(*a, **kw)
        try:
            row = next(res)
        except StopIteration:
            raise NoSuchRowError()
        try:
            next(res)
        except StopIteration:
            # Fine, we only wanted one row
            pass
        else:
            # Finish the active SQL statement
            res.close()
            raise NoUniqueValueError()

        return row

    def last_rowid(self):
        """Return rowid most recently inserted in the current thread"""

        return self.conn.last_insert_rowid()

    def changes(self):
        """Return number of rows affected by most recent sql statement"""

        return self.conn.changes()


def create_tables(conn):
    # Table of storage objects
    # Refcount is included for performance reasons
    # phys_size is the number of bytes stored in the backend (i.e., after compression).
    # length is the logical size in the filesystem.
    # phys_size == -1 indicates block has not been uploaded yet
    conn.execute(
        """
    CREATE TABLE objects (
        id          INTEGER PRIMARY KEY AUTOINCREMENT,
        hash        BLOB(32) UNIQUE,
        refcount    INT NOT NULL,
        phys_size   INT NOT NULL,
        length      INT NOT NULL
    )"""
    )

    # Table with filesystem metadata
    # The number of links `refcount` to an inode can in theory
    # be determined from the `contents` table. However, managing
    # this separately should be significantly faster (the information
    # is required for every getattr!)
    conn.execute(
        """
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
    )"""
    )

    # Further Blocks used by inode (blockno >= 1)
    conn.execute(
        """
    CREATE TABLE inode_blocks (
        inode     INTEGER NOT NULL REFERENCES inodes(id),
        blockno   INT NOT NULL,
        obj_id    INTEGER NOT NULL REFERENCES objects(id),
        PRIMARY KEY (inode, blockno)
    )"""
    )

    # Symlinks
    conn.execute(
        """
    CREATE TABLE symlink_targets (
        inode     INTEGER PRIMARY KEY REFERENCES inodes(id),
        target    BLOB NOT NULL
    )"""
    )

    # Names of file system objects
    conn.execute(
        """
    CREATE TABLE names (
        id     INTEGER PRIMARY KEY,
        name   BLOB NOT NULL,
        refcount  INT NOT NULL,
        UNIQUE (name)
    )"""
    )

    # Table of filesystem objects
    # rowid is used by readdir() to restart at the correct position
    conn.execute(
        """
    CREATE TABLE contents (
        rowid     INTEGER PRIMARY KEY AUTOINCREMENT,
        name_id   INT NOT NULL REFERENCES names(id),
        inode     INT NOT NULL REFERENCES inodes(id),
        parent_inode INT NOT NULL REFERENCES inodes(id),

        UNIQUE (parent_inode, name_id)
    )"""
    )

    # Extended attributes
    conn.execute(
        """
    CREATE TABLE ext_attributes (
        inode     INTEGER NOT NULL REFERENCES inodes(id),
        name_id   INTEGER NOT NULL REFERENCES names(id),
        value     BLOB NOT NULL,

        PRIMARY KEY (inode, name_id)
    )"""
    )

    # Shortcuts
    conn.execute(
        """
    CREATE VIEW contents_v AS
    SELECT * FROM contents JOIN names ON names.id = name_id
    """
    )
    conn.execute(
        """
    CREATE VIEW ext_attributes_v AS
    SELECT * FROM ext_attributes JOIN names ON names.id = name_id
    """
    )


class DatabaseChecksumError(RuntimeError):
    '''Raised when the downloaded database does not have the expected checksum'''

    def __init__(self, name: str, expected: str, actual: str):
        super().__init__()
        self.name = name
        self.expected = expected
        self.actual = actual

    def __str__(self):
        return f'File {self.name} has checksum {self.actual}, expected {self.expected}'


def download_metadata(backend: AbstractBackend, db_file: str, params: dict, failsafe=False):
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
            next_dirty_block = db.dirty_blocks.get_block
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
    tmpname = filename + '.tmp'
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
    try:
        buf = backend['s3ql_params']
    except NoSuchObject:
        # Perhaps this is an old filesystem revision
        params = backend.lookup('s3ql_metadata')
    else:
        params = thaw_basic_mapping(buf)

    filename = cachepath + '.params'
    local_params = None
    if os.path.exists(filename):
        with open(filename, 'rb') as fh:
            local_params = thaw_basic_mapping(fh.read())

    if local_params is None or params['seq_no'] > local_params['seq_no']:
        rev = params['revision']
    else:
        rev = local_params['revision']
    if rev < CURRENT_FS_REV:
        raise QuietError(
            'File system revision too old, please run `s3qladm upgrade` first.', exitcode=32
        )
    elif rev > CURRENT_FS_REV:
        raise QuietError(
            'File system revision too new, please update your S3QL installation.', exitcode=33
        )

    return (local_params, params)


class NoUniqueValueError(Exception):
    '''Raised if get_val or get_row was called with a query
    that generated more than one result row.
    '''

    def __str__(self):
        return 'Query generated more than 1 result row'


class NoSuchRowError(Exception):
    '''Raised if the query did not produce any result rows'''

    def __str__(self):
        return 'Query produced 0 result rows'


class ResultSet:
    '''
    Provide iteration over encapsulated apsw cursor. Additionally,
    `ResultSet` instances may be used as context managers to terminate
    the query before all result rows have been retrieved.
    '''

    def __init__(self, cur):
        self.cur = cur

    def __next__(self):
        return next(self.cur)

    def __iter__(self):
        return self

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.cur.close()

    def close(self):
        '''Terminate query'''

        self.cur.close()


# Load extension, needs a dummy connection
db = apsw.Connection(':memory:')
db.enableloadextension(True)
db.loadextension(sqlite3ext.__file__[:-3])
db.close()
