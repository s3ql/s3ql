"""
CursorManager.py

Copyright (C) 2008  Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU LGPL. 
"""

from __future__ import unicode_literals
import logging
from contextlib import contextmanager
import apsw
import thread
import time
from random import randrange

__all__ = [ "CursorManager" ]

log = logging.getLogger("CursorManager") 

class Dummy(object):
    '''An empty class with the only purpose of holding attributes
    '''
    
    pass

    
class CursorManager(object):
    """Manage access to database cursors.
    
    This class manages access to database cursors. Its main objective
    is to ensure that every thread works with a thread-local connection. 
    This allows SQLite to take care of the necessary locking (so we 
    automatically get a distinction between read- and write-locks) and 
    also ensures 
    that one can uniquely retrieve the last inserted rowid and the
    number of rows affected by the last statement.
    
    Instances can be used as if they were cursors itself. In addition
    to the standard `execute` method, they provide convenience methods
    like `get_val', `get_row` or `changes`. Note that all these 
    calls allocate a new cursor in order to avoid aborting 
    a result set from a previous call.
    
    CursorManager also takes care of converting bytes objects into
    buffer objects and back, so that they are stored as BLOBS
    in the database. If you want to store TEXT, you need to
    supply unicode objects instead. (This functionality is
    only needed under Python 2.x, under Python 3.x the apsw
    module already behaves like this).
    
    Note that threading.local() does not work when the threads are
    not started by threading.Thread() but some C library (like fuse).
    For that reason we use a hash on the thread ID instead. The
    thread id is not globally unique to a thread, but only unique
    among other currently running threads. This is not a problem
    because we can freely pass around Connections between threads,
    we just want to make sure that a connection is only used by
    one thread at a time.
    
    TODO: It seems that _threading_local.local() does work. Maybe 
    we should use that instead? cf. http://bugs.python.org/issue6627
    
    Attributes:
    -----------
    
    :retrytime:    In case the database is locked by another thread,
                   we wait for the lock to be released for at most
                   `retrytime` milliseconds. 
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
        self.conn = dict() # Indexed by thread id
        
        # Enable shared cache mode 
        apsw.enablesharedcache(True)
        
    @contextmanager
    def transaction(self):
        '''Create savepoint, rollback on exceptions, commit on success
        
        This context manager creates a savepoint and returns the 
        CursorManager instance itself. If the managed block evaluates
        without exceptions, the savepoint is committed at the end. Otherwise it is rolled back.         
        '''
        unique_object = object()
        name = str(id(unique_object))
        self.execute("SAVEPOINT '%s'" % name)
        try:
            yield self
        except:
            self.execute("ROLLBACK TO '%s'" % name)
            raise
        finally:
            self.execute("RELEASE '%s'" % name)
            
            
    def _get_conn(self):
        '''Return thread-local connection object
        '''
        
        try:
            conn = self.conn[thread.get_ident()]
        except KeyError:
            log.debug("Creating new db connection (active conns: %d)...", len(self.conn))
            conn = apsw.Connection(self.dbfile)
            conn.setbusytimeout(self.retrytime)   
            if self.initsql:
                conn.cursor().execute(self.initsql)
                   
            self.conn[thread.get_ident()] = conn
                
        return conn
    
    def query(self, *a, **kw):
        '''Execute the given SQL statement. Return ResultSet.
        
        Transforms buffer() to bytes() and vice versa.
        '''
        
        return ResultSet(self._execute(*a, **kw))
         
         
    def execute(self, *a, **kw):
        '''Execute the given SQL statement. Return number of affected rows.

        '''
    
        self._execute(*a, **kw).close()
        return self.changes()

                   
    def _execute(self, statement, bindings=None):         
        '''Execute the given SQL statement
        
        Note that in shared cache mode we may get an SQLITE_LOCKED 
        error, which is not handled by the busy handler. Therefore
        we have to emulate this behaviour.
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
        while True:
            try:
                if bindings is not None:
                    return self._get_conn().cursor().execute(statement, newbindings)
                else:
                    return self._get_conn().cursor().execute(statement)
            except apsw.LockedError:
                if waited > self.retrytime:
                    raise # We don't wait any longer
                if waited > self.retrytime/3:
                    log.warn('Waited for database lock for %d ms so far...', waited) 
                step = randrange(10, 100, 1)
                time.sleep(step / 1000)
                waited += step
            
            
    def get_val(self, *a, **kw):
        """Executes a select statement and returns first element of first row.
        
        If there is no result row, raises StopIteration.
        """

        return self.get_row(*a, **kw)[0]

    def get_list(self, *a, **kw):
        """Executes a select statement and returns result list.
        
        """

        return list(self.execute(*a, **kw))    


    def get_row(self, *a, **kw):
        """Executes a select statement and returns first row.
        
        If there are no result rows, raises StopIteration.
        """

        res = self.query(*a, **kw)
        row = res.next()
        try:
            res.next()
        except StopIteration:
            # Fine, we only wanted one row
            pass
        else:
            raise RuntimeError('Query returned more than one result row')
        
        return row
     
    def last_rowid(self):
        """Return rowid most recently inserted in the current thread.
        
        """
        return self._get_conn().last_insert_rowid()
    
    def changes(self):
        """Return number of rows affected by most recent sql statement in current thread.

        """
        return self._get_conn().changes()
 
    
class ResultSet(object):
    '''Iterator over the result of an SQL query
    
    This class automatically converts back from buffer() to bytes().
    ''' 
    
    def __init__(self, cur):
        self.cur = cur
        
    def __iter__(self):
        return self
    
    def next(self):
        return [ ( col if not isinstance(col, buffer) else bytes(col) ) 
                for col in self.cur.next() ]