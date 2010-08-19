'''
database.py - this file is part of S3QL (http://s3ql.googlecode.com)

Copyright (C) 2008-2009 Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU LGPL.

This module manages access to the SQLite database. Its main objective
is to ensure that every thread works with a thread-local connection. 
This allows to rely on SQLite to take care of locking procedures
and ensures that one can uniquely retrieve the last inserted rowid and the
number of rows affected by the last statement.

Note that threading.local() does not work when the threads are
not started by threading.Thread() but some C library (like fuse).
The python implementation in _threading_local does work, but
it is not clear if and when local objects are being destroyed.
Therefore we maintain a pool of connections that are
shared between all threads.

Public functions (not starting with an underscore) of this module
are threadsafe and safe to call concurrently from several threads.

Module Attributes:
-----------

:retrytime:    In case the database is locked by another thread,
               we wait for the lock to be released for at most
               `retrytime` milliseconds.
:pool:         List of available cursors (one for each database connection)
:provided:     Dict of currently provided ConnectionWrapper instances
:dbfile:       Filename of the database
:initsql:      SQL commands that are executed whenever a new
               connection is created.
                 
'''

from __future__ import division, print_function

import logging
import apsw
import os
import types
from .common import QuietError

__all__ = ['Connection', 'NoUniqueValueError', 'NoSuchRowError' ]

log = logging.getLogger("database")

sqlite_ver = tuple([ int(x) for x in apsw.sqlitelibversion().split('.') ])
if sqlite_ver < (3, 7, 0):
    raise QuietError('SQLite version too old, must be 3.7.0 or newer!\n')
        
initsql = ('PRAGMA foreign_keys = OFF',
           'PRAGMA synchronous = OFF',
           'PRAGMA journal_mode = OFF',
           'PRAGMA locking_mode = EXCLUSIVE',
           'PRAGMA recursize_triggers = on',
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
    
    Instances also takes care of converting bytes objects into buffer
    objects and back, so that they are stored as BLOBS in the database. If you
    want to store TEXT, you need to supply unicode objects instead. (This
    functionality is only needed under Python 2.x, under Python 3.x the apsw
    module already behaves in the correct way).
    
    Attributes
    ----------
    
    :conn:     apsw connection object
    :cur:      default cursor, to be used for all queries
               that do not return a ResultSet (i.e., that finalize
               the cursor when they return)
    '''

    def __init__(self, file_):
        self.conn = apsw.Connection(file_)
        self.cur = self.conn.cursor()
        self.file = file_
        
        for s in initsql:
            self.cur.execute(s)

    def close(self):
        self.cur.close()
        self.conn.close()
        
    def get_size(self):
        '''Return size of database file'''
    
        if self.file is not None and self.file not in ('', ':memory:'):
            return os.path.getsize(self.file)
        else:
            return 0
            
    def query(self, *a, **kw):
        '''Execute the given SQL statement. Return ResultSet.
        
        Transforms buffer() to bytes() and vice versa. If the
        caller may not retrieve all rows of the result, it
        should delete the `ResultSet` object has soon as 
        possible to terminate the SQL statement.
        '''

        return ResultSet(self._execute(self.conn.cursor(), *a, **kw))

    def execute(self, *a, **kw):
        '''Execute the given SQL statement. Return number of affected rows '''

        self._execute(self.cur, *a, **kw)
        return self.changes()

    def rowid(self, *a, **kw):
        """Execute SQL statement and return last inserted rowid"""

        self._execute(self.cur, *a, **kw)
        return self.conn.last_insert_rowid()

    def _execute(self, cur, statement, bindings=None):
        '''Execute the given SQL statement with the given cursor
        
        This method takes care of converting str/bytes to buffer
        objects.
        '''

        if isinstance(bindings, types.GeneratorType):
            bindings = list(bindings)

        # Convert bytes to buffer
        if isinstance(bindings, dict):
            newbindings = dict()
            for key in bindings:
                if isinstance(bindings[key], bytes):
                    newbindings[key] = buffer(bindings[key])
                else:
                    newbindings[key] = bindings[key]
        elif isinstance(bindings, (list, tuple)):
            newbindings = [ (val if not isinstance(val, bytes) else buffer(val))
                           for val in bindings ]
        else:
            newbindings = bindings

        if bindings is not None:
            return cur.execute(statement, newbindings)
        else:
            return cur.execute(statement)

    def has_val(self, *a, **kw):
        '''Execute statement and check if it gives result rows'''

        res = self._execute(self.cur, *a, **kw)
        try:
            res.next()
        except StopIteration:
            return False
        else:
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

        res = ResultSet(self._execute(self.cur, *a, **kw))
        try:
            row = res.next()
        except StopIteration:
            raise NoSuchRowError()
        try:
            res.next()
        except StopIteration:
            # Fine, we only wanted one row
            pass
        else:
            # Finish the active SQL statement
            del res
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
    '''Iterator over the result of an SQL query
    
    This class automatically converts back from buffer() to bytes().'''

    def __init__(self, cur):
        self.cur = cur

    def __iter__(self):
        return self

    def next(self):
        return [ (col if not isinstance(col, buffer) else bytes(col))
                  for col in self.cur.next() ]

    # Once the ResultSet goes out of scope, the cursor goes out of scope
    # too (because query() uses a fresh cursor), so we don't have to
    # take any special precautions to finish the active SQL statement.  
