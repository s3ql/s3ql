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
from contextlib import contextmanager
import apsw
import os
import types
import thread

__all__ = [ "init", 'execute', 'get_db_size', 'get_row', 'get_val', 'has_val',
           'rowid', 'write_lock', 'WrappedConnection', 'NoUniqueValueError',
           'conn' ]

log = logging.getLogger("database")

# Globals
dbfile = None
initsql = ('PRAGMA synchronous = off;'
           'PRAGMA foreign_keys = on;')
retrytime = 120000
pool = list()
provided = dict()

def init(dbfile_):
    '''Initialize Module'''
    
    global dbfile
    dbfile = dbfile_

    # http://code.google.com/p/apsw/issues/detail?id=59
    apsw.enablesharedcache(False)

    global pool
    pool = list()
    
    global provided
    provided = dict()

@contextmanager
def conn():
    '''Provide a WrappedConnection instance.
    
    This context manager acquires a connection from the pool and
    returns a WrappedConnection instance. If this function is
    called again by the same thread in the managed block, it will
    always return the same WrappedConnection instance. 
    '''

    try:
        wconn = provided[thread.get_ident()]
    except KeyError:
        pass
    else:
        yield wconn
        return

    conn_ = _pop_conn()
    provided[thread.get_ident()] = conn_
    try:
        yield conn_
    finally:
        del provided[thread.get_ident()]
        _push_conn(conn_)

@contextmanager
def write_lock():
    """Acquire WrappedConnection and run its write_lock method"""
    with conn() as wconn:
        with wconn.write_lock():
            yield wconn


def _pop_conn():
    '''Return database connection from the pool'''

    try:
        conn_ = pool.pop()
    except IndexError:
        # Need to create a new connection
        log.debug("Creating new db connection (active conns: %d",
                  len(provided))
        conn_ = apsw.Connection(dbfile)
        conn_.setbusytimeout(retrytime)
        if initsql:
            conn_.cursor().execute(initsql)
        conn_ = WrappedConnection(conn_)

    return conn_

def _push_conn(conn_):
    '''Put a database connection back into the pool'''

    pool.append(conn_)

def get_val(*a, **kw):
    """Acquire WrappedConnection and run its get_val method """

    with conn() as conn_:
        return conn_.get_val(*a, **kw)

def rowid(*a, **kw):
    """Acquire WrappedConnection and run its rowid method"""

    with conn() as conn_:
        return conn_.rowid(*a, **kw)

def has_val(*a, **kw):
    """Acquire WrappedConnection and run its has_val method"""

    with conn() as conn_:
        return conn_.has_val(*a, **kw)

def get_row(*a, **kw):
    """"Acquire WrappedConnection and run its get_row method"""

    with conn() as conn_:
        return conn_.get_row(*a, **kw)

def execute(*a, **kw):
    """"Acquire WrappedConnection and run its execute method"""

    with conn() as conn_:
        return conn_.execute(*a, **kw)

def get_db_size():
    '''Return size of database file'''

    if dbfile is not None and dbfile not in ('', ':memory:'):
        return os.path.getsize(dbfile)
    else:
        return 0

class WrappedConnection(object):
    '''
    This class wraps an APSW connection object. It should be used instead of any
    native APSW cursors.
    
    It provides methods to directly execute SQL commands and creates apsw
    cursors dynamically.
    
    WrappedConnections are not thread safe. They can be passed between threads,
    but must not be called concurrently.
    
    WrappedConnection also takes care of converting bytes objects into buffer
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
    :in_trx:   Is an active BEGIN IMMEDIATE transaction?
    '''

    def __init__(self, conn_):
        self.conn = conn_
        self.cur = conn_.cursor()
        self.in_trx = False

    @contextmanager
    def write_lock(self):
        '''Execute block with write_lock on db
    
        This context manager acquires a connection from the pool
        and ensures that a BEGIN IMMEDIATE transaction is active.
        
        The transaction will be committed when the block has
        executed, even if execution was aborted with an exception.
        '''
         
        if self.in_trx:
            yield
        else:
            self._execute(self.cur, 'BEGIN IMMEDIATE')
            try:
                yield
            finally:
                self._execute(self.cur, 'COMMIT')

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

        try:
            if bindings is not None:
                return cur.execute(statement, newbindings)
            else:
                return cur.execute(statement)
        except apsw.ConstraintError:
            log.error('Constraint error when executing %r with bindings %r',
                      statement, bindings)
            # Work around SQLite bug, http://code.google.com/p/apsw/issues/detail?id=99
            for i in range(100):
                cur.execute('SELECT null'+ ' '*i)
            raise


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
        
        If there is no result row, raises `KeyError`. If there is more
        than one row, raises `NoUniqueValueError`.
        """

        return self.get_row(*a, **kw)[0]

    def get_list(self, *a, **kw):
        """Execute select statement and returns result list"""

        return list(self.query(*a, **kw))

    def get_row(self, *a, **kw):
        """Execute select statement and return first row.
        
        If there are no result rows, raises `KeyError`. If there is more
        than one result row, raises `NoUniqueValueError`.
        """

        res = ResultSet(self._execute(self.cur, *a, **kw))
        try:
            row = res.next()
        except StopIteration:
            raise KeyError('Query returned empty result set')
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
        """Return number of rows affected by most recent sql statement in current thread"""

        return self.conn.changes()


class NoUniqueValueError(Exception):
    '''Raised if get_val or get_row was called with a query 
    that generated more than one result row.
    '''

    def __str__(self):
        return 'Query generated more than 1 result row'


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
