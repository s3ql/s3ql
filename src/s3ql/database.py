'''
database.py - this file is part of S3QL (http://s3ql.googlecode.com)

Copyright (C) Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU GPLv3.


Module Attributes:
-----------

:initsql:      SQL commands that are executed whenever a new
               connection is created.
'''

import logging
import apsw
import os
from .common import QuietError

log = logging.getLogger("database")

sqlite_ver = tuple([ int(x) for x in apsw.sqlitelibversion().split('.') ])
if sqlite_ver < (3, 7, 0):
    raise QuietError('SQLite version too old, must be 3.7.0 or newer!\n')

initsql = ('PRAGMA foreign_keys = OFF',
           'PRAGMA locking_mode = EXCLUSIVE',
           'PRAGMA recursize_triggers = on',
           'PRAGMA page_size = 4096',
           'PRAGMA wal_autocheckpoint = 25000',
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

    def __init__(self, file_, fast_mode=False):
        self.conn = apsw.Connection(file_)
        self.file = file_

        cur = self.conn.cursor()
        for s in initsql:
            cur.execute(s)

        self.fast_mode(fast_mode)

    def fast_mode(self, on):
        '''Switch to fast, but insecure mode
        
        In fast mode, SQLite operates as quickly as possible, but
        application and system crashes may lead to data corruption.
        '''

        # WAL mode causes trouble with e.g. copy_tree, so we
        # always disable WAL for now. See 
        # http://article.gmane.org/gmane.comp.db.sqlite.general/65243
        on = True
        cur = self.conn.cursor()
        if on:
            cur.execute('PRAGMA synchronous = OFF')
            cur.execute('PRAGMA journal_mode = OFF')
        else:
            cur.execute('PRAGMA synchronous = NORMAL')
            cur.execute('PRAGMA journal_mode = WAL')


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
        '''Execute the given SQL statement. Return number of affected rows '''

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
        return next(self)
    
    def __iter__(self):
        return self
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_value, traceback):
        self.cur.close()
        
    def close(self):
        '''Terminate query'''
        
        self.cur.close()
