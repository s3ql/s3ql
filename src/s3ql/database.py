"""
database.py

Copyright (C) 2008  Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU LGPL. 
"""

from __future__ import unicode_literals
import logging
from contextlib import contextmanager
import apsw
import time
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
    Therefore we maintain a pool of connection objects that are
    shared between all threads.
    
    Attributes:
    -----------
    
    :retrytime:    In case the database is locked by another thread,
                   we wait for the lock to be released for at most
                   `retrytime` milliseconds.
    :pool:         List of available database connections
    :provided:     Dict of currently provided ConnectionWrapper instances
    :dbfile:       Filename of the database
    :initsql:      SQL commands that are executed whenever a new
                   connection is created.

    """

    def __init__(self, dbfile, initsql=None, retrytime=1000):
        '''Initialize object.
        
        If `initsql` is specified, it is executed as an SQL command
        whenever a new connection is created (you can use it e.g. to
        set specific pragmas for all connections).
        '''
        self.dbfile = dbfile
        self.initsql = initsql
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
        called again in the managed block, it will
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
        called again in the managed block, it will
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
            log.debug("Creating new db connection (active conns: %d)...", len(self.provided))
            conn = apsw.Connection(self.dbfile)
            conn.setbusytimeout(self.retrytime)   
            if self.initsql:
                conn.cursor().execute(self.initsql)
                   
        return conn
    
    def _push_conn(self, conn):
        '''Put the a database connection back into the pool
        '''
        
        self.pool.append(conn)
        
    def get_val(self, *a, **kw):
        """Acquire WrappedConnection and run its get_val method.
        """
        
        with self() as conn:
            return conn.get_val(*a, **kw)
        
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
        
         
class WrappedConnection(object):
    '''This class wraps an APSW connection object. It should be
    used instead of any native APSW cursors. 
    
    It provides methods to directly execute SQL commands and
    creates apsw cursors dynamically. 
    
    WrappedConnection also takes care of converting bytes objects into
    buffer objects and back, so that they are stored as BLOBS
    in the database. If you want to store TEXT, you need to
    supply unicode objects instead. (This functionality is
    only needed under Python 2.x, under Python 3.x the apsw
    module already behaves like this).
    '''
    
    def __init__(self, conn, retrytime):
        self.conn = conn
        self.retrytime = retrytime
 
    @contextmanager
    def transaction(self):
        '''Initiate a transaction
        
        This context manager creates a savepoint. If the managed block evaluates
        without exceptions, the savepoint is committed at the end.
        Otherwise it is rolled back.         
        '''
        unique_object = object()
        name = str(id(unique_object))

        self._execute("SAVEPOINT '%s'" % name)
        try:
            yield 
        except:
            self._execute("ROLLBACK TO '%s'" % name)
            raise
        finally:
            self._execute("RELEASE '%s'" % name)
             
    def query(self, *a, **kw):
        '''Execute the given SQL statement. Return ResultSet.
        
        Transforms buffer() to bytes() and vice versa.
        '''
        
        return ResultSet(self._execute(*a, **kw))
         
    def execute(self, *a, **kw):
        '''Execute the given SQL statement. Return number of affected rows.
        '''
    
        self._execute(*a, **kw)
        return self.changes()

    def rowid(self, *a, **kw):
        """Execute SQL statement and return last inserted rowid.
        
        """
        
        self._execute(*a, **kw)
        return self.conn.last_insert_rowid()
                       
    def _execute(self, statement, bindings=None):         
        '''Execute the given SQL statement
        
        Note that in shared cache mode we may get an SQLITE_LOCKED 
        error, which is not handled by the busy handler. Therefore
        we have to emulate this behavior.
        '''
                
        # Convert bytes to buffer
        if isinstance(bindings, dict):
            newbindings = dict()
            for key in bindings:
                if isinstance(bindings[key], bytes):
                    newbindings[key] = buffer(bindings[key])
                else:
                    newbindings[key] = bindings[key]
        elif isinstance(bindings, (list, tuple)):
            newbindings = [ ( val if not isinstance(val, bytes) else buffer(val) ) 
                           for val in bindings ] 
        else:
            newbindings = bindings
            
            
        waited = 0
        step = 1
        while True:
            try:
                if bindings is not None:
                    return self.conn.cursor().execute(statement, newbindings)
                else:
                    return self.conn.cursor().execute(statement)
            except apsw.LockedError:
                if waited > self.retrytime:
                    raise # We don't wait any longer 
                time.sleep(step / 1000)
                waited += step
                step = randrange(step+1, 2*(step+1), 1)
            
            
    def get_val(self, *a, **kw):
        """Executes a select statement and returns first element of first row.
        
        If there is no result row, raises StopIteration. If there is more
        than one row, raises RuntimeError.
        """

        return self.get_row(*a, **kw)[0]

    def get_list(self, *a, **kw):
        """Executes a select statement and returns result list.
        
        """

        return list(self.execute(*a, **kw))    


    def get_row(self, *a, **kw):
        """Executes a select statement and returns first row.
        
        If there are no result rows, raises StopIteration. If there is more
        than one result row, raises RuntimeError.
        """

        res = self.query(*a, **kw)
        row = res.next()
        try:
            res.next()
        except StopIteration:
            # Fine, we only wanted one row
            pass
        else:
            raise NoUniqueValueError()
        
        return row
     
    def last_rowid(self):
        """Return rowid most recently inserted in the current thread.
        
        """
        return self.conn.last_insert_rowid()
    
    def changes(self):
        """Return number of rows affected by most recent sql statement in current thread.

        """
        return self.conn.changes()        


class NoUniqueValueError(Exception):       
    '''Raised if get_val or get_row was called with a query 
    that generated more than one result row.
    '''
    
    def __str__(self):
        return 'Query generated more than 1 result row'
    
         
class ResultSet(object):
    '''Iterator over the result of an SQL query
    
    This class automatically converts back from buffer() to bytes(). When
    all results have been retrieved, the connection is returned back to 
    the pool.
    ''' 
    
    def __init__(self, cur):
        self.cur = cur
        
    def __iter__(self):
        return self
    
    def next(self):
        return [ ( col if not isinstance(col, buffer) else bytes(col) ) 
                  for col in self.cur.next() ]

        