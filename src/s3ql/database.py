'''
database.py - this file is part of S3QL (http://s3ql.googlecode.com)

Copyright (C) 2008-2009 Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU LGPL.
'''

from __future__ import division, print_function

import logging
from s3ql.common import stacktraces
from contextlib import contextmanager
import tempfile
import apsw
import time
import os
import types
import thread
from random import randrange

__all__ = [ "ConnectionManager", 'WrappedConnection', 'NoUniqueValueError' ]

log = logging.getLogger("database")


class ConnectionManager(object):
    """Manage access to database.
    
    This class manages access to the SQLite database. Its main objective
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
    
    Attributes:
    -----------
    
    :retrytime:    In case the database is locked by another thread,
                   we wait for the lock to be released for at most
                   `retrytime` milliseconds.
    :pool:         List of available cursors (one for each database connection)
    :provided:     Dict of currently provided ConnectionWrapper instances
    :dbfile:       Filename of the database
    :initsql:      SQL commands that are executed whenever a new
                   connection is created.
    """

    def __init__(self, dbfile, retrytime=10000):
        '''Initialize object.
        
        If `initsql` is specified, it is executed as an SQL command
        whenever a new connection is created (you can use it e.g. to
        set specific pragmas for all connections).
        '''
        self.dbfile = dbfile
        self.initsql = ('PRAGMA synchronous = off;'
                        'PRAGMA foreign_keys = ON;')
        self.retrytime = retrytime
        self.pool = list()
        self.provided = dict()

        # http://code.google.com/p/apsw/issues/detail?id=59
        apsw.enablesharedcache(False)

    @contextmanager
    def __call__(self):
        '''Provide a WrappedConnection instance.
        
        This context manager acquires a connection from the pool and
        returns a WrappedConnection instance. If this function is
        called again by the same thread in the managed block, it will
        always return the same WrappedConnection instance. 
        '''

        try:
            wconn = self.provided[thread.get_ident()]
        except KeyError:
            pass
        else:
            yield wconn
            return

        conn = self._pop_conn()
        try:
            wconn = WrappedConnection(conn, self.retrytime)
            self.provided[thread.get_ident()] = wconn
            try:
                yield wconn
            finally:
                del self.provided[thread.get_ident()]
        finally:
            self._push_conn(conn)

    @contextmanager
    def transaction(self):
        '''Provide WrappedConnection and initiate transaction.
        
        This context manager acquires a connection from the pool
        and immediately sets a savepoint. It provides a WrappedConnection
        instance. If the managed block evaluates
        without exceptions, the savepoint is committed at the end.
        Otherwise it is rolled back.        
        
        If this function is
        called again in the same thread inside the managed block, it will
        always return the same WrappedConnection instance, but still
        start a new, inner transaction. 
        '''

        with self() as wconn:
            with wconn.transaction():
                yield wconn


    def _pop_conn(self):
        '''Return database connection from the pool
        '''

        try:
            conn = self.pool.pop()
        except IndexError:
            # Need to create a new connection
            log.debug("Creating new db connection (active conns: %d",
                      len(self.provided))
            conn = apsw.Connection(self.dbfile)
            conn.setbusytimeout(self.retrytime)
            if self.initsql:
                conn.cursor().execute(self.initsql)

        return conn

    def _push_conn(self, conn):
        '''Put a database connection back into the pool'''

        self.pool.append(conn)

    def get_val(self, *a, **kw):
        """Acquire WrappedConnection and run its get_val method.
        """

        with self() as conn:
            return conn.get_val(*a, **kw)

    def rowid(self, *a, **kw):
        """Acquire WrappedConnection and run its rowid method.
        """

        with self() as conn:
            return conn.rowid(*a, **kw)

    def has_val(self, *a, **kw):
        """Acquire WrappedConnection and run its has_val method.
        """

        with self() as conn:
            return conn.has_val(*a, **kw)

    def get_row(self, *a, **kw):
        """"Acquire WrappedConnection and run its get_row method.
        """

        with self() as conn:
            return conn.get_row(*a, **kw)

    def execute(self, *a, **kw):
        """"Acquire WrappedConnection and run its execute method.
        """

        with self() as conn:
            return conn.execute(*a, **kw)

    def get_db_size(self):
        '''Return size of database file'''

        if self.dbfile and self.dbfile != ':memory:':
            return os.path.getsize(self.dbfile)
        else:
            return 0

class WrappedConnection(object):
    '''This class wraps an APSW connection object. It should be
    used instead of any native APSW cursors. 
    
    It provides methods to directly execute SQL commands and
    creates apsw cursors dynamically. 
    
    WrappedConnections are not thread safe. They can be passed between
    threads, but must not be called concurrently.
    
    WrappedConnection also takes care of converting bytes objects into
    buffer objects and back, so that they are stored as BLOBS
    in the database. If you want to store TEXT, you need to
    supply unicode objects instead. (This functionality is
    only needed under Python 2.x, under Python 3.x the apsw
    module already behaves in the correct way).
    
    Attributes
    ----------
    
    :conn:     apsw connection object
    :cur:      default cursor, to be used for all queries
               that do not return a ResultSet (i.e., that finalize
               the cursor when they return)
    :retrytime: Maximum time to wait for other threads to release a
                database lock.
    :savepoint_cnt: Keeps track of the current number of encapsulated
                 savepoints. We use a running number instead of e.g.
                the address of a local object so that the apsw statement
                cache does not overflow.
    '''

    def __init__(self, conn, retrytime):
        self.conn = conn
        self.cur = conn.cursor()
        self.retrytime = retrytime
        self.savepoint_cnt = 0

    @contextmanager
    def transaction(self):
        '''Initiate a transaction
        
        This context manager creates a savepoint. If the managed block evaluates
        without exceptions, the savepoint is committed at the end.
        Otherwise it is rolled back.         
        
        If there is no enclosing transaction, a BEGIN IMMEDIATE transaction 
        is started before the saveblock.
        '''
        self.savepoint_cnt += 1
        name = 's3ql-%d' % self.savepoint_cnt

        # NOTE: If you ever add a version of this function that starts a DEFERRED transaction
        # instead, you have to make sure that the two different kinds of transactions
        # cannot be nested. Once a DEFERRED (== read only) transaction is started, the thread
        # holds a SHARED lock and must not try to obtain a RESERVED lock or deadlocks
        # will occur. In other words, once the caller has asked for a DEFERRED transaction,
        # any further attempts to set SAVEPOINTS have to produce errors.        
        if self.savepoint_cnt == 1:
            self._execute(self.cur, 'BEGIN IMMEDIATE')

        self._execute(self.cur, "SAVEPOINT '%s'" % name)
        # pylint bug
        #pylint: disable-msg=C0321
        try:
            yield
        except:
            self._execute(self.cur, "ROLLBACK TO '%s'" % name)
            raise
        finally:
            self._execute(self.cur, "RELEASE '%s'" % name)
            self.savepoint_cnt -= 1

            if self.savepoint_cnt == 0:
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
        '''Execute the given SQL statement. Return number of affected rows.
        '''

        self._execute(self.cur, *a, **kw)
        return self.changes()

    def rowid(self, *a, **kw):
        """Execute SQL statement and return last inserted rowid"""

        self._execute(self.cur, *a, **kw)
        return self.conn.last_insert_rowid()

    def _execute(self, cur, statement, bindings=None):
        '''Execute the given SQL statement with the given cursor
        
        Note that in shared cache mode we may get an SQLITE_LOCKED 
        error, which is not handled by the busy handler. Therefore
        we have to emulate this behavior.
        '''

        # There really aren't too many branches in this method
        #pylint: disable-msg=R0912

        # Convert bytes to buffer
        if isinstance(bindings, dict):
            newbindings = dict()
            for key in bindings:
                if isinstance(bindings[key], bytes):
                    newbindings[key] = buffer(bindings[key])
                else:
                    newbindings[key] = bindings[key]
        elif isinstance(bindings, (list, tuple, types.GeneratorType)):
            newbindings = [ (val if not isinstance(val, bytes) else buffer(val))
                           for val in bindings ]
        else:
            newbindings = bindings


        waited = 0
        step = 1
        #log.debug(statement)
        while True:
            curtime = time.time()
            try:
                if bindings is not None:
                    return cur.execute(statement, newbindings)
                else:
                    return cur.execute(statement)
            except apsw.LockedError:
                if waited > self.retrytime:
                    raise # We don't wait any longer 
                time.sleep(step / 1000)
                waited += step
                step = randrange(step + 1, 2 * (step + 1), 1)
            except apsw.BusyError:
                if time.time() - curtime < self.retrytime / 1000:
                    log.error('SQLite detected deadlock condition!')
                # Print stack trace
                log.error('BusyError - writing stack trace to ./s3ql_stack_trace.txt:')
                fh = open('s3ql_stack_trace.txt', 'w')
                fh.write(stacktraces())
                fh.close()
                raise
            except apsw.CantOpenError:
                # Try to open a file, this should give us a better error
                # in case we have run out of FDs
                fh = tempfile.NamedTemporaryFile()
                fh.write('Enough fds...')
                fh.close()
                raise
            except apsw.ConstraintError:
                log.error('Constraint error when executing %r with bindings %r',
                          statement, newbindings)
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
