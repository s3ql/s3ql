#!/usr/bin/env python
#
#    Copyright (C) 2008  Nikolaus Rath <Nikolaus@rath.org>
#
#    This program can be distributed under the terms of the GNU LGPL.
#

import apsw
import tempfile
import unittest
from random   import randrange, sample

class apsw_tests(unittest.TestCase):
    """In S3QL, we rely on the fact that the result set is not affected
    by changes that we made to the database while we iterate over it.
    
    Since I could not find any documentation of this feature, I test for 
    it extensively.
    """
    
    
    def setUp(self):
        self.file = tempfile.NamedTemporaryFile()
        self.db = apsw.Connection(self.file.name)
        
    def tearDown(self):
        self.file.close()
        
    def test_remove(self):
        """SELECT unaffected by removals in result set
        """
        
        cur1 = self.db.cursor()
        cur2 = self.db.cursor()
        
        cur1.execute("CREATE TABLE foo (id INT, flag BOOLEAN);")
          
        # Fill table
        no_total = 300
        entries = sample(xrange(10**9), no_total)
        for id in entries:
            cur1.execute("INSERT INTO foo(id, flag) VALUES(?,?)", (id,False))

        # Flag some entries for retrieval
        no_flag = 200
        flag = [ entries[randrange(0,no_total)] for x in range(no_flag) ]
        for id in flag:
            cur1.execute("UPDATE foo SET flag=? WHERE id=?", (True, id))
         
        # Execute SELECT
        res = cur1.execute("SELECT id FROM foo WHERE flag = ?", (True,))
        
        # Read half the result set
        read = [ res.next()[0] for x in range(int(no_flag/2)) ]
        
        # Update some entries
        no_update = int(no_flag/2)
        update = [ flag[randrange(0,no_flag)] for x in range(no_update) ]
        for id in update:
            cur2.execute("UPDATE foo SET flag=? WHERE id=?", (False, id))
        
        # And remove some
        no_remove = int(no_flag/2)
        remove = [ flag[randrange(0,no_flag)] for x in range(no_remove) ]
        for id in remove:
            cur2.execute("DELETE FROM foo WHERE id=?", (id,))
        
        # Now read the rest of the result 
        read += [ id for (id,) in res ]
        self.assertEquals(read.sort(), flag.sort())

        # But now the first cursor is done, so we should get the reduced list
        res = cur1.execute("SELECT id FROM foo WHERE flag = ?", (True,))
        read = [ id for (id,) in res ]
        self.assertEquals(read.sort(), list(set(flag) - set(update) - set(remove)).sort() )
        
    def test_add(self):
        """SELECT unaffected by insertions in result set
        """
        
        cur1 = self.db.cursor()
        cur2 = self.db.cursor()
        
        cur1.execute("CREATE TABLE foo (id INT, flag BOOLEAN);")
          
        # Fill table
        no_total = 300
        entries = sample(xrange(10**9), no_total)
        for id in entries:
            cur1.execute("INSERT INTO foo(id, flag) VALUES(?,?)", (id,False))

        # Flag some entries for retrieval
        no_flag = 50
        flag = [ entries[randrange(0,no_total)] for x in range(no_flag) ]
        for id in flag:
            cur1.execute("UPDATE foo SET flag=? WHERE id=?", (True, id))
         
        # Execute SELECT
        res = cur1.execute("SELECT id FROM foo WHERE flag = ?", (True,))
        
        # Read half the result set
        read = [ res.next()[0] for x in range(int(no_flag/2)) ]
        
        # Update some entries
        no_update = int(no_total/2)
        update = [ flag[randrange(0,no_flag)] for x in range(no_update) ]
        for id in update:
            cur2.execute("UPDATE foo SET flag=? WHERE id=?", (True, id))
                
        # Now read the rest of the result 
        read += [ id for (id,) in res ]
        self.assertEquals(read.sort(), flag.sort())

        # But now the first cursor is done, so we should get the reduced list
        res = cur1.execute("SELECT id FROM foo WHERE flag = ?", (True,))
        read = [ id for (id,) in res ]
        self.assertEquals(read.sort(), list(set(flag).union(set(update))).sort() )        
 
    def test_updates(self):
        """SELECT unaffected by updates at current row in result set
        """
        
        cur1 = self.db.cursor()
        cur2 = self.db.cursor()
        
        cur1.execute("CREATE TABLE foo (id INT, flag BOOLEAN);")
          
        # Fill table
        no_total = 300
        entries = sample(xrange(10**9), no_total)
        for id in entries:
            cur1.execute("INSERT INTO foo(id, flag) VALUES(?,?)", (id,False))

        # Flag some entries for retrieval
        no_flag = 150
        flag = [ entries[randrange(0,no_total)] for x in range(no_flag) ]
        for id in flag:
            cur1.execute("UPDATE foo SET flag=? WHERE id=?", (True, id))
         
        # Execute SELECT
        res = cur1.execute("SELECT id FROM foo WHERE flag = ?", (True,))
        
        # Read the result set and update at the same time
        read = list()
        for (id,) in res:
            read.append(id)
            cur2.execute("UPDATE foo SET flag=? WHERE id=?", (False, id))
                
        self.assertEquals(read.sort(), flag.sort())

        # But now the first cursor is done, so we should get the reduced list
        res = cur1.execute("SELECT id FROM foo WHERE flag = ?", (True,))
        read = [ id for (id,) in res ]
        self.assertEquals(read, list())     
        
            
# Somehow important according to pyunit documentation
def suite():
    return unittest.makeSuite(s3_tests_local)


# Allow calling from command line
if __name__ == "__main__":
            unittest.main()
