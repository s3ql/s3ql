#!/usr/bin/env python
#
#    Copyright (C) 2008  Nikolaus Rath <Nikolaus@rath.org>
#
#    This program can be distributed under the terms of the GNU LGPL.
#

from __future__ import unicode_literals
import os
import types
import stat
from time import time
import tempfile
import numbers
import logging

from s3ql.common import (writefile, get_path, ROOT_INODE, unused_name, get_inode)

__all__ = [ "fsck" ]

log = logging.getLogger("fsck")

# Implementation Note
# ----------------------
#  
# In order to implement the 'dry-run' functionality (where no changes
# are made to the filesystem) without cluttering up the code too much,
# we start a transaction at the beginning of the fsck and roll back
# all the changes. This also has the advantage that later
# tests can pretend that any earlier errors have been fixed 
# (e.g., they will always be able to get the inode of the root directory).
# 
# Individual tests only need to explicitly check
# for `checkonly` if they want to modify the cache directory or the S3 bucket.
#

def fsck(cur, cachedir, bucket, checkonly=False):
    """Checks a file system
    
    Returns `False` if any errors have been found. Throws `FatalFsckError` 
    if the filesystem could not be repaired.
    """

    found_errors = False
    
    try:
        cur.execute('SAVEPOINT fsck') 
        
        checker = Checker(cur, cachedir, bucket, checkonly)
        found_errors = not checker.checkall()
        
    finally:
        if checkonly:
            # Roll back all the changes
            checker = None # Destroy active cursors
            cur.execute('ROLLBACK TO SAVEPOINT fsck')
            
        cur.execute('RELEASE SAVEPOINT fsck')
            
    return not found_errors 

class FatalFsckError(Exception):
    """An uncorrectable error has been found in the file system.

    """

    pass

class Checker(object):
    """Filesystem checker.
    
    Attributes:
    -----------
    
    :checkonly:    If specified, no changes will be made to S3 or the local 
                   cache (but the database is still changed)
    """
       
    def __init__(self, cm, cachedir, bucket, checkonly):
        self.cm = cm
        self.checkonly = checkonly
        self.cachedir = cachedir
        self.bucket = bucket
        
    def checkall(self):
         
        found_errors = False
        
        for fn in [ 
                   self.check_parameters,
                   self.check_cache,
                   self.check_lof,
                   self.check_dirs,
                   self.check_loops,
                   self.check_inode_refcount,
                   self.check_offsets,
                   self.check_s3_refcounts,
                   self.check_keylist ]:
            if not fn():
                found_errors = True 
                
        return not found_errors
    
        
    def check_parameters(self):
        """Check that file system parameters are set
    
        Returns `False` if any errors have been found.
        """
        cur = self.cm
    
        log.info('Inspecting filesystem parameters...')
        try:
            (label, blocksize, last_fsck, mountcnt, version, needs_fsck) \
                = cur.get_row("SELECT label, blocksize, last_fsck, mountcnt, "
                              " version, needs_fsck FROM parameters")
        except StopIteration:
            (label, blocksize, last_fsck, mountcnt, version, needs_fsck) \
                = (None, None, None, None, None, None)
            
        if not (isinstance(label, types.StringTypes)
             and isinstance(blocksize, numbers.Integral)
             and isinstance(last_fsck, numbers.Real)
             and isinstance(mountcnt, numbers.Integral)
             and isinstance(version, numbers.Real)
             and isinstance(needs_fsck, numbers.Integral)):
            log.error("Cannot read filesystem parameters. " 
                      "This does not appear to be a valid S3QL filesystem.")
            raise FatalFsckError()
    
        return True
       
    def check_cache(self):
        """Commit any uncommitted cache files
    
        Returns `False` if any cache files have been found
        """
    
        cur = self.cm
        found_errors = False
    
        log.info("Checking cached objects...")
        
        for s3key in os.listdir(self.cachedir):
            found_errors = True
            log.warn("Committing (potentially changed) cache for %s", s3key)
            if not self.checkonly:
                etag = self.bucket.store_from_file(s3key, self.cachedir + s3key)
                cur.execute("UPDATE s3_objects SET etag=? WHERE id=?",
                            (etag, s3key))
                os.unlink(self.cachedir + s3key)
    
        return not found_errors
    
    
    def check_lof(self):
        """Ensure that there is a lost+found directory
    
        Returns `False` if any errors have been found.
        """
        cm = self.cm             
        found_errors = False
        
        log.info('Checking lost+found...')
        
        try:
            inode_l = cm.get_val("SELECT inode FROM contents WHERE name=? AND parent_inode=?", 
                                 (b"lost+found", ROOT_INODE))
    
        except StopIteration:
            found_errors = True
            log.warn("Recreating missing lost+found directory")
            cm.execute("INSERT INTO inodes (mode,uid,gid,mtime,atime,ctime,refcount) "
                       "VALUES (?,?,?,?,?,?,?)",
                       (stat.S_IFDIR | stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR,
                        os.getuid(), os.getgid(), time(), time(), time(), 2))
            inode_l = cm.last_rowid()
            cm.execute("INSERT INTO contents (name, inode, parent_inode) VALUES(?,?,?)",
                       (b"lost+found", inode_l, ROOT_INODE))
            
            # . and .. will be added by the next checker
    
        mode = cm.get_val('SELECT mode FROM inodes WHERE id=?', (inode_l,))
        if not stat.S_ISDIR(mode):
            found_errors = True
            log.warn('/lost+found is not a directory! Old entry will be saved as '
                     '/lost+found/inode-%s', inode_l)
            # We leave the old inode unassociated, so that it will be added
            # to lost+found later on.
            cm.execute("INSERT INTO inodes (mode,uid,gid,mtime,atime,ctime,refcount) "
                       "VALUES (?,?,?,?,?,?,?)",
                       (stat.S_IFDIR | stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR,
                        os.getuid(), os.getgid(), time(), time(), time(), 2))
            inode_l = cm.last_rowid()
            cm.execute('UPDATE contents SET inode=? WHERE name=? AND parent_inode=?',
                       (inode_l, b"lost+found", ROOT_INODE))
            
        return not found_errors
    
    def check_dirs(self):
        """Ensure that directories have correct . and .. entries
    
        Returns `False` if any errors have been found.
        """

        cm = self.cm
        found_errors = False
        
        log.info('Checking directories for . and .. entries...')
    
        for (name, inode, mode, parent_inode) in \
            cm.query('SELECT name, inode, mode, parent_inode FROM contents JOIN inodes '
                       'ON id == inode'):
            
            if not stat.S_ISDIR(mode):
                continue
            
            # Checked with their parent directories
            if name == '.' or name == '..': 
                continue
            
            # .
            try:
                inode2 = cm.get_val('SELECT inode FROM contents WHERE name=? AND parent_inode=?',
                                    (b'.', inode))
            except StopIteration:   
                found_errors = True
                log.warn('Directory "%s", inode %d has no . entry', get_path(name, parent_inode, cm),
                         inode)
                cm.execute('INSERT INTO contents (name, inode, parent_inode) VALUES(?,?,?)',
                           (b'.', inode, inode))
                inode2 = inode
                
            if inode2 != inode:
                found_errors = True
                log.warn('Directory "%s", inode %d has wrong . entry', get_path(name, parent_inode, cm),
                         inode)
                cm.execute('UPDATE contents SET inode=? WHERE name=? AND parent_inode=?',
                           (inode, b'.', inode))
    
                
            # ..
            try:
                inode2 = cm.get_val('SELECT inode FROM contents WHERE name=? AND parent_inode=?',
                                    (b'..', inode))
            except StopIteration:   
                found_errors = True
                log.warn('Directory "%s", inode %d has no .. entry', get_path(name, parent_inode, cm),
                         inode)
                cm.execute('INSERT INTO contents (name, inode, parent_inode) VALUES(?,?,?)',
                           (b'..', parent_inode, inode))
                inode2 = parent_inode
                
            if inode2 != parent_inode:
                found_errors = True
                log.warn('Directory "%s", inode %d has wrong .. entry', get_path(name, parent_inode, cm),
                         inode)
                cm.execute('UPDATE contents SET inode=? WHERE name=? AND parent_inode=?',
                           (parent_inode, b'..', inode)) 
                
        return not found_errors
                
        
    def check_loops(self):
        """Ensure that all directories can be reached from root
    
        Returns `False` if any errors have been found.
        """

        cm = self.cm
        found_errors = False
        
        log.info('Checking directory reachability...')
            
        cm.execute("CREATE TEMPORARY TABLE loopcheck AS SELECT * FROM contents")
        
        def delete_tree(inode_p):
            subdirs = list()
            for (inode, mode, name) in cm.query("SELECT inode, mode, name FROM contents JOIN inodes "
                                            "ON inode == id WHERE parent_inode=?",
                                            (inode_p,)):
                if stat.S_ISDIR(mode) and not name in (b'.', b'..'):
                    subdirs.append(inode)
            cm.execute("DELETE FROM loopcheck WHERE parent_inode=?", (inode_p,))
            for inode in subdirs:
                delete_tree(inode)
                            
        delete_tree(ROOT_INODE)
        
        if cm.get_val("SELECT COUNT(inode) FROM loopcheck") > 0:
            log.warn("Found unreachable filesystem entries!")
            found_errors = True
            log.warn("This problem cannot be corrected automatically yet.")
            
        cm.execute("DROP TABLE loopcheck")     
            
        return not found_errors
    
    
    def check_inode_refcount(self):
        """Check inode reference counters
    
        Returns `False` if any errors have been found.
        """
 
        cm = self.cm
        found_errors = False
    
        log.info('Checking inodes...')
        inode_l = get_inode(b"/lost+found", cm)
        
        for (inode, refcount) in cm.query("SELECT id, refcount FROM inodes"):
             
            # No checks for root
            if inode == ROOT_INODE:
                continue
            
            refcount2 = cm.get_val("SELECT COUNT(name) FROM contents WHERE inode=?", (inode,))
                
            if refcount2 == 0:
                found_errors = True
                log.warn("Inode %d not referenced, adding to lost+found", inode)
                name =  unused_name(b"/lost+found/inode-%d" % inode, cm)         
                cm.execute("INSERT INTO contents (name, inode, parent_inode) "
                           "VALUES (?,?,?)", (name, inode, inode_l))
                cm.execute("UPDATE inodes SET refcount=? WHERE id=?",
                           (1, inode))       
                  
            elif refcount != refcount2:
                found_errors = True
                log.warn("Inode %d has wrong reference count, setting from %d to %d",
                         inode, refcount, refcount2)
                cm.execute("UPDATE inodes SET refcount=? WHERE id=?", (refcount2, inode))
                                 
    
        return not found_errors
    
    def check_offsets(self):
        """Check that s3 offsets are blocksize apart  
    
        Returns `False` if any errors have been found.
        """
 
        cm = self.cm
        found_errors = False
        
        blocksize = cm.get_val("SELECT blocksize FROM parameters")
        
        for (inode, offset, s3key) in cm.query("SELECT inode, offset, s3key FROM inode_s3key"):
            if not offset % blocksize == 0:
                found_errors = True
                log.warn("Object %s for inode %d does not start at blocksize boundary, deleting",
                         s3key, inode)
                cm.execute("DELETE FROM s3_objects WHERE s3key=?", (s3key,))
        
        return not found_errors  
        
    def check_s3_refcounts(self):
        """Check s3 object reference counts
    
        Returns `False` if any errors have been found.
        """

        cm = self.cm
        found_errors = False
    
        for (id_, refcount) in cm.query("SELECT id, refcount FROM s3_objects"):
     
            refcount2 = cm.get_val("SELECT COUNT(inode) FROM inode_s3key WHERE s3key=?",
                                   (id_,))
            if refcount != refcount2:
                log.warn("S3 object %s has invalid refcount, setting from %d to %d",
                         id_, refcount, refcount2)
                found_errors = True
                cm.execute("UPDATE s3_objects SET refcount=? WHERE id=?",
                           (refcount2, id_))
    
        return not found_errors
    
    
    def check_keylist(self):
        """Check the list of S3 objects.
    
        Checks that:
        - no s3 object is larger than the blocksize
        - all s3 objects are referred in the s3 table
        - all objects in the s3 table exist
        - etags match (update metadata in case of conflict)
    
        Returns `False` if any errors have been found.
        """
     
        cm = self.cm
        found_errors = False
    
        blocksize = cm.get_val("SELECT blocksize FROM parameters")
    
        # Create a server process in case we want to write files
        from s3ql import fs, s3cache
        cachedir = tempfile.mkdtemp() + "/"
        cache = s3cache.S3Cache(self.bucket, cachedir, 0, cm)
        server = fs.Server(cache, cm)
    
    
        # We use this table to keep track of the s3keys that we have
        # seen
        cm.execute("CREATE TEMP TABLE s3keys AS SELECT id FROM s3_objects")
    
        for (s3key, meta) in self.bucket.list_keys():
    
            # We only bother with our own objects
            if not s3key.startswith("s3ql_data_"):
                continue
    
            # Retrieve object information from database
            try:
                etag = cm.get_val("SELECT etag FROM s3_objects WHERE id=?", (s3key,))       
            
            # 
            # Handle object that exists only in S3
            # 
            except StopIteration:
                found_errors = True
                log.warn("object %s not referenced in s3 objects table, adding to lost+found",
                         s3key)
                
                # We don't directly add it, because this may introduce s3 key 
                # clashes and does not work if the object is larger than the
                # blocksize
                if not self.checkonly:
                    tmp = tempfile.NamedTemporaryFile()
                    self.bucket.fetch_to_file(s3key, tmp.name)
                    dest = unused_name(b'/lost+found/%s' % s3key, cm)
                    writefile(tmp.name, dest, server)
                    del self.bucket[s3key]
                    tmp.close()
            
                continue             
                    
            else:
                # Mark object as seen
                cm.execute("DELETE FROM s3keys WHERE id=?", (s3key,))
    
            #
            # Check Metadata
            # 
    
            if etag != meta.etag:
                found_errors = True
                log.warn("object %s has incorrect etag in metadata, adjusting" % s3key)
                cm.execute("UPDATE s3_objects SET etag=? WHERE id=?",
                           (meta.etag, s3key))   
 
            #
            # Size
            #
            if meta.size > blocksize:
                found_errors = True
                log.warn("object %s is larger than blocksize (%d > %d), "
                         "truncating (original object in lost+found)",
                         s3key, meta.size, blocksize)
                if not self.checkonly:
                    tmp = tempfile.NamedTemporaryFile()
                    self.bucket.fetch_to_file(s3key, tmp.name)
    
                    # Save full object in lost+found
                    dest = unused_name(b'/lost+found/%s' % s3key, cm)
                    writefile(tmp.name, dest, server)
    
                    # Truncate and write
                    tmp.seek(blocksize)
                    tmp.truncate()
                    etag_new = self.bucket.store_from_file(s3key, tmp.name)
                    tmp.close()
                    cm.execute("UPDATE s3_objects SET etag=? WHERE s3key=?",
                               (etag_new, s3key))
                    
                    
        # Now handle objects that only exist in s3_objects
        for (s3key,) in cm.query("SELECT id FROM s3keys"):
            found_errors = True
            log.warn("object %s only exists in table but not on s3, deleting", s3key)
            cm.execute("DELETE FROM inode_s3key WHERE s3key=?", (s3key,))
            cm.execute("DELETE FROM s3_objects WHERE id=?", (s3key,))
            
        cache.close()
        os.rmdir(cachedir)  
                                   
        return not found_errors


