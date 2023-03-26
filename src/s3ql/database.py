'''
database.py - this file is part of S3QL.

Copyright © 2008 Nikolaus Rath <Nikolaus@rath.org>

This work can be distributed under the terms of the GNU GPLv3.


Module Attributes:
-----------

:initsql:      SQL commands that are executed whenever a new
               connection is created.
'''

from typing import Union, Optional, List
from .logging import logging, QuietError  # Ensure use of custom logger class
import apsw
import os

log = logging.getLogger(__name__)

sqlite_ver = tuple([int(x) for x in apsw.sqlitelibversion().split('.')])
if sqlite_ver < (3, 7, 0):
    raise QuietError('SQLite version too old, must be 3.7.0 or newer!\n')


initsql = (
    # As of 2011, WAL mode causes trouble with s3qlcp, s3qlrm and s3qllock (performance going
    # down orders of magnitude and WAL file grows unbounded) This is because WAL checkpoint
    # is suspended while there is an active SELECT query. For now we use neither journal nor
    # WAL and turn cache flushing off to get reasonable performance. However, this means
    # there is a considerable risk of data corruption if the process crashes. This is not
    # good, since the remote copy may itself be inconsistent due to incremental updates...
    'PRAGMA journal_mode = OFF',
    'PRAGMA synchronous = OFF',
    'PRAGMA foreign_keys = OFF',
    'PRAGMA locking_mode = EXCLUSIVE',
    'PRAGMA recursize_triggers = on',
    'PRAGMA temp_store = FILE',
    'PRAGMA legacy_file_format = off',
)


class Connection(object):
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
            vfs.set_blocksize(blocksize)
            file_ = os.path.abspath(file_)
            self.dirty_blocks = vfs.dirty_blocks(file_)
            self.conn = apsw.Connection(file_, vfs=VFS.vfs_name)
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


class ResultSet(object):
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


class VFS(apsw.VFS):
    '''Custom VFS class for use by SQLite.

    The only difference to the default VFS is that it this enables use of
    our custom VFSFile class.

    Not threadsafe.
    '''

    vfs_name = 'S3QL'

    def __init__(self) -> None:
        super().__init__(name=VFS.vfs_name, base='')
        self.reset()

    def reset(self):
        self._tracked_files = dict()
        self._blocksize = None

    def set_blocksize(self, blocksize: int):
        if self._blocksize is not None and self._blocksize != blocksize:
            raise RuntimeError('blocksize may only be set once')
        self._blocksize = blocksize

    def dirty_blocks(self, file: str) -> set:
        '''Track writes for *file*'''

        log.debug('VFS: Tracking writes for %s', file)
        try:
            return self._tracked_files[file]
        except KeyError:
            pass
        dirty_blocks = set()
        self._tracked_files[file] = dirty_blocks
        return dirty_blocks

    def xOpen(self, name: Union[str, apsw.URIFilename], flags: List[int]):
        if isinstance(name, apsw.URIFilename):
            pname = name.filename()
        else:
            pname = name

        dirty_blocks = self._tracked_files.get(pname)
        if dirty_blocks is not None:
            log.debug('VFS.xOpen(%s): using custom VFSFile', pname)
            if self._blocksize is None:
                raise RuntimeError('blocksize not set')
            return VFSFile(name, flags, self._blocksize, dirty_blocks)
        else:
            log.debug('VFS.xOpen(%s): using default VFSFile', pname)
            return super().xOpen(name, flags)


class VFSFile(apsw.VFSFile):
    ''' 'Custom VFSFile class for use by SQLite

    This is used to track which parts of the database are being modified
    and need to be re-uploaded.

    Not threadsafe.
    '''

    def __init__(self, name: str, flags: List[int], blocksize: int, dirty_blocks: set):
        # Inherit methods from default VFS
        # Can't use keyword parameters here since some versions of APSW use
        # filename=, while others use name=.
        # First parameter is the base VFS name.
        super().__init__('', name, flags)

        self.blocksize = blocksize
        self.dirty_blocks = dirty_blocks

    def xWrite(self, buf: bytes, off: int) -> None:
        len_ = len(buf)
        log.debug("VFSFile.write(len=%d, off=%d)", len_, off)
        super().xWrite(buf, off)
        # Only mark blocks as dirty after the write, so that it's not possible
        # to accidentally upload a block before the changes have been applied.
        for n in range(off // self.blocksize, (off + len_ - 1) // self.blocksize + 1):
            self.dirty_blocks.add(n)

    def xTruncate(self, newsize: int) -> None:
        log.debug("VFSFile.truncate(%d)", newsize)
        super().xTruncate(newsize)
        threshold = newsize // self.blocksize
        dirty = list(self.dirty_blocks)
        for blockno in dirty:
            if blockno > threshold:
                self.dirty_blocks.remove(blockno)

    # TODO: Figure out what we need to do about xShm* methods
    # (https://github.com/rogerbinns/apsw/issues/418)


# Singleton instance (can't have more than one VFS with the same name)
vfs = VFS()
