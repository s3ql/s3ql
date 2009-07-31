"""
CursorManager.py

Copyright (C) 2008  Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU LGPL. 
"""

import threading
import logging
from contextlib import contextmanager
import apsw

__all__ = [ "CursorManager" ]

log = logging.getLogger("CursorManager") 
    
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
    """

    def __init__(self, dbfile, initsql=None):
        '''Initialize object.
        
        If `initsql` is specified, it is executed as an SQL command
        whenever a new connection is created (you can use it e.g. to
        set specific pragmas for all connections).
        '''
        self.dbfile = dbfile
        self.initsql = initsql
        self.local = threading.local()
        
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
        if not hasattr(self.local, "conn"):
            log.debug("Creating new db connection...")
            self.local.conn = apsw.Connection(self.dbfile)
            self.local.conn.setbusytimeout(1000)

            if self.initsql:
                self.local.conn.cursor().execute(self.initsql)
                
        return self.local.conn   
    
         
    def execute(self, *a, **kw):
        '''Execute the given SQL statement
        '''
        
        return self._get_conn().cursor().execute(*a, **kw)


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

        # It is ABSOLUTELY CRUCIAL that we retrieve all the rows
        # or explicitly close the used cursor.
        # Otherwise the cursor is not destroyed and the database
        # stays locked.
        cur = self._get_conn().cursor()
        res = cur.execute(*a, **kw)
        row = res.next()
        try:
            res.next()
        except StopIteration:
            # Fine, we only wanted one row
            pass
        else:
            # There are more results? That shouldn't be
            # We first finish the cursor
            cur.close()
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
    
