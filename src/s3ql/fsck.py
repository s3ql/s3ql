'''
$Id$

Copyright (C) 2008-2009 Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU LGPL.
'''

from __future__ import unicode_literals, division, print_function
 
import os
from os.path import basename
import types
import stat
import tempfile
import time
import numbers
import logging
from s3ql.database import NoUniqueValueError
import shutil
from s3ql.common import (get_path, ROOT_INODE, CTRL_INODE, inode_for_path)

__all__ = [ "fsck", 'found_errors' ]

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

# Init globals
conn = None
checkonly = False
cachedir = None 
bucket = None
expect_errors = False
found_errors = False


def fsck(conn_, cachedir_, bucket_, checkonly_=False):
    """Check file system
    
    Sets module variable `found_errors`. Throws `FatalFsckError` 
    if the filesystem can not be repaired.
    """
    
    global conn
    global checkonly
    global cachedir
    global bucket
    global found_errors
    
    conn = conn_
    cachedir = cachedir_
    bucket = bucket_ 
    checkonly = checkonly_
    found_errors = False

    conn.execute("SAVEPOINT fsck")
    try:
        detect_fs()
        check_cache()
        check_lof()
        check_dirs()
        check_loops()
        check_inode_refcount()
        check_s3_refcounts()
        check_keylist()
    finally:
        if checkonly:
            conn.execute('ROLLBACK TO SAVEPOINT fsck')
            
        conn.execute('RELEASE SAVEPOINT fsck')


def log_error(*a, **kw):
    '''Log file system error if not expected'''
    
    if not expect_errors:
        return log.warn(*a, **kw)
    
class FatalFsckError(Exception):
    """An uncorrectable error has been found in the file system.

    """

    pass

    
def detect_fs():
    """Check that we have a valid filesystem

    Raises FatalFsckError() if no fs can be found.
    """
    
    log.info('Looking for valid filesystem...')
    try:
        (label, blocksize, last_fsck, mountcnt, version, needs_fsck) \
            = conn.get_row("SELECT label, blocksize, last_fsck, mountcnt, "
                           " version, needs_fsck FROM parameters")
    except (KeyError, NoUniqueValueError):
        log_error("Cannot read filesystem parameters. " 
                  "This does not appear to be a valid S3QL filesystem.")
        raise FatalFsckError()
    
    if not (isinstance(label, types.StringTypes)
         and isinstance(blocksize, numbers.Integral)
         and isinstance(last_fsck, numbers.Real)
         and isinstance(mountcnt, numbers.Integral)
         and isinstance(version, numbers.Real)
         and isinstance(needs_fsck, numbers.Integral)):
        log_error("Cannot read filesystem parameters. " 
                  "This does not appear to be a valid S3QL filesystem.")
        raise FatalFsckError()


def check_cache():
    """Commit uncommitted cache files"""

    global found_errors
    
    log.info("Checking cached objects...")
    
    for s3key in os.listdir(cachedir):
        found_errors = True
        log_error("Committing (potentially changed) cache for %s", s3key)
        if not checkonly:
            etag = bucket.store_fh(s3key, open(os.path.join(cachedir, s3key), 'r'))
            conn.execute("UPDATE s3_objects SET etag=? WHERE key=?",
                        (etag, s3key))
            os.unlink(os.path.join(cachedir, s3key))


def check_lof():
    """Ensure that there is a lost+found directory"""
         
    global found_errors 
    log.info('Checking lost+found...')
    
    timestamp = time.time() - time.timezone
    try:
        inode_l = conn.get_val("SELECT inode FROM contents WHERE name=? AND parent_inode=?", 
                             (b"lost+found", ROOT_INODE))

    except KeyError:
        found_errors = True
        log_error("Recreating missing lost+found directory")
        inode_l = conn.rowid("INSERT INTO inodes (mode,uid,gid,mtime,atime,ctime,refcount) "
                             "VALUES (?,?,?,?,?,?,?)",
                             (stat.S_IFDIR | stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR,
                              os.getuid(), os.getgid(), timestamp, timestamp, timestamp, 2))
        conn.execute("INSERT INTO contents (name, inode, parent_inode) VALUES(?,?,?)",
                     (b"lost+found", inode_l, ROOT_INODE))
        
        # . and .. will be added by the next checker

    mode = conn.get_val('SELECT mode FROM inodes WHERE id=?', (inode_l,))
    if not stat.S_ISDIR(mode):
        found_errors = True
        log_error('/lost+found is not a directory! Old entry will be saved as '
                  '/lost+found/inode-%s', inode_l)
        # We leave the old inode unassociated, so that it will be added
        # to lost+found later on.
        inode_l = conn.rowid("INSERT INTO inodes (mode,uid,gid,mtime,atime,ctime,refcount) "
                             "VALUES (?,?,?,?,?,?,?)",
                             (stat.S_IFDIR | stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR,
                              os.getuid(), os.getgid(), timestamp, timestamp, timestamp, 2))
        conn.execute('UPDATE contents SET inode=? WHERE name=? AND parent_inode=?',
                   (inode_l, b"lost+found", ROOT_INODE))
        


def check_dirs():
    """Ensure that directories have correct . and .. entries"""

    global found_errors 
    log.info('Checking directories for . and .. entries...')

    for (name, inode, mode, parent_inode) in \
        conn.query('SELECT name, inode, mode, parent_inode FROM contents JOIN inodes '
                   'ON id == inode'):
        
        if not stat.S_ISDIR(mode):
            continue
        
        # Checked with their parent directories
        if name == '.' or name == '..': 
            continue
        
        # .
        try:
            inode2 = conn.get_val('SELECT inode FROM contents WHERE name=? AND parent_inode=?',
                                (b'.', inode))
        except KeyError:   
            found_errors = True
            log_error('Directory "%s", inode %d has no . entry', 
                      get_path(name, parent_inode, conn), inode)
            conn.execute('INSERT INTO contents (name, inode, parent_inode) VALUES(?,?,?)',
                         (b'.', inode, inode))
            inode2 = inode
            
        if inode2 != inode:
            found_errors = True
            log_error('Directory "%s", inode %d has wrong . entry',
                      get_path(name, parent_inode, conn), inode)
            conn.execute('UPDATE contents SET inode=? WHERE name=? AND parent_inode=?',
                         (inode, b'.', inode))
            
        # ..
        try:
            inode2 = conn.get_val('SELECT inode FROM contents WHERE name=? AND parent_inode=?',
                                  (b'..', inode))
        except KeyError:   
            found_errors = True
            log_error('Directory "%s", inode %d has no .. entry',
                      get_path(name, parent_inode, conn), inode)
            conn.execute('INSERT INTO contents (name, inode, parent_inode) VALUES(?,?,?)',
                         (b'..', parent_inode, inode))
            inode2 = parent_inode
            
        if inode2 != parent_inode:
            found_errors = True
            log_error('Directory "%s", inode %d has wrong .. entry', 
                      get_path(name, parent_inode, conn), inode)
            conn.execute('UPDATE contents SET inode=? WHERE name=? AND parent_inode=?',
                         (parent_inode, b'..', inode)) 
            
    
def check_loops():
    """Ensure that all directories can be reached from root"""

    global found_errors 
    log.info('Checking directory reachability...')
        
    conn.execute("CREATE TEMPORARY TABLE loopcheck AS SELECT * FROM contents")
    
    def delete_tree(inode_p):
        subdirs = list()
        for (inode, mode, name) in conn.query("SELECT inode, mode, name FROM contents JOIN inodes "
                                        "ON inode == id WHERE parent_inode=?",
                                        (inode_p,)):
            if stat.S_ISDIR(mode) and not name in (b'.', b'..'):
                subdirs.append(inode)
        conn.execute("DELETE FROM loopcheck WHERE parent_inode=?", (inode_p,))
        for inode in subdirs:
            delete_tree(inode)
                        
    delete_tree(ROOT_INODE)
    
    if conn.get_val("SELECT COUNT(inode) FROM loopcheck") > 0:
        found_errors = True
        log_error("Found unreachable filesystem entries! "
                  "This problem cannot be corrected automatically yet.")
        
    conn.execute("DROP TABLE loopcheck")     

def check_inode_refcount():
    """Check inode reference counters"""

    global found_errors 

    log.info('Checking inodes...')
    
    for (inode, refcount) in conn.query("SELECT id, refcount FROM inodes"):
         
        # No checks for root and contral
        if inode in (ROOT_INODE, CTRL_INODE):
            continue
        
        refcount2 = conn.get_val("SELECT COUNT(name) FROM contents WHERE inode=?", (inode,))
            
        if refcount2 == 0:
            found_errors = True
            (inode_p, name) = resolve_free(b"/lost+found", b"inode-%d" % inode)
            log_error("Inode %d not referenced, adding as /lost+found/%s", inode, name)
            conn.execute("INSERT INTO contents (name, inode, parent_inode) "
                         "VALUES (?,?,?)", (basename(name), inode, inode_p))
            conn.execute("UPDATE inodes SET refcount=? WHERE id=?",
                         (1, inode))       
              
        elif refcount != refcount2:
            found_errors = True
            log_error("Inode %d has wrong reference count, setting from %d to %d",
                      inode, refcount, refcount2)
            conn.execute("UPDATE inodes SET refcount=? WHERE id=?", (refcount2, inode))

    
def check_s3_refcounts():
    """Check s3 object reference counts"""

    global found_errors
    log.info('Checking S3 object reference counts...')

    for (key, refcount) in conn.query("SELECT key, refcount FROM s3_objects"):
 
        refcount2 = conn.get_val("SELECT COUNT(inode) FROM blocks WHERE s3key=?",
                                 (key,))
        if refcount != refcount2:
            log_error("S3 object %s has invalid refcount, setting from %d to %d",
                      key, refcount, refcount2)
            found_errors = True
            if refcount2 != 0:
                conn.execute("UPDATE s3_objects SET refcount=? WHERE key=?",
                             (refcount2, key))
            else:
                # Orphaned object will be picked up by check_keylist
                conn.execute('DELETE FROM s3_objects WHERE key=?', (key,)) 
   

def check_keylist():
    """Check the list of S3 objects.

    Checks that:
    - all s3 objects are referred in the s3 table
    - all objects in the s3 table exist
    and calls check_metadata for each object
    """
 
    global found_errors
 
    # We use this table to keep track of the s3keys that we have
    # seen
    conn.execute("CREATE TEMP TABLE s3keys AS SELECT key FROM s3_objects")

    blocksize = conn.get_val("SELECT blocksize FROM parameters")
    to_delete = list() # We can't delete the object during iteration
    for (s3key, meta) in bucket.list_keys():

        # We only bother with data objects
        if not s3key.startswith("s3ql_data_"):
            continue

        # Retrieve object information from database
        try:
            (etag, size) = conn.get_row("SELECT etag, size FROM s3_objects WHERE key=?", (s3key,))
        
        # Handle object that exists only in S3
        except KeyError:
            found_errors = True 
            name = unused_filename('s3-object-%s' % s3key)
            log_error("object %s not referenced in s3 objects table, saving locally as ./%s",
                      s3key, name)
            if not checkonly:
                if not expect_errors:
                    bucket.fetch_fh(s3key, open(name, 'w'))
                to_delete.append(s3key)    
            continue
                
        # Mark object as seen
        conn.execute("DELETE FROM s3keys WHERE key=?", (s3key,))
        
        # Check Etag
        if etag != meta.etag:
            found_errors = True
            log_error("object %s has incorrect etag in metadata, adjusting" % s3key)
            conn.execute("UPDATE s3_objects SET etag=? WHERE key=?",
                         (meta.etag, s3key))  
        
        # Size agreement
        if size != meta.size:
            found_errors = True
            log_error("object %s has incorrect size in metadata, adjusting" % s3key)
            conn.execute("UPDATE s3_objects SET size=? WHERE key=?",
                         (meta.size, s3key)) 
 
        # Size <= block size
        if meta.size > blocksize:
            found_errors = True
            name = unused_filename('s3-object-%s' % s3key)
            log_error("s3 object %s is larger than blocksize (%d > %d), "
                      "truncating and saving original object locally as ./%s",
                      s3key, meta.size, blocksize, name)
            if not checkonly:
                tmp = tempfile.NamedTemporaryFile()
                if expect_errors:
                    bucket.fetch_fh(s3key, tmp)
                else:
                    bucket.fetch_fh(s3key, open(name, 'w'))
                    shutil.copyfile(name, tmp.name)
                tmp.seek(blocksize)
                tmp.truncate()
                tmp.seek(0)
                etag_new = bucket.store_fh(s3key, tmp)
                tmp.close()
    
                conn.execute("UPDATE s3_objects SET etag=?, size=? WHERE key=?",
                             (etag_new, blocksize, s3key))  
                
    # Carry out delete
    if to_delete:
        log.info('Performing deferred S3 removals...')
    for s3key in to_delete:
        del bucket[s3key]      
     
    # Now handle objects that only exist in s3_objects
    for (s3key,) in conn.query("SELECT key FROM s3keys"):
        found_errors = True
        log_error("object %s only exists in table but not on s3, deleting", s3key)
        conn.execute("DELETE FROM blocks WHERE s3key=?", (s3key,))
        conn.execute("DELETE FROM s3_objects WHERE key=?", (s3key,))
                                
    conn.execute('DROP TABLE s3keys')
    
def unused_filename(path):
    '''Append numeric suffix to (local) path until it does not exist'''
    
    if not os.path.exists(path):
        return path
    
    i = 0
    while os.path.exists("%s-%d" % (path, i)):
        i += 1
        
    return '%s-%d' % (path, i)
    
def resolve_free(path, name):
    '''Return parent inode and name of an unused directory entry
    
    The directory entry will be in `path`. If an entry `name` already
    exists there, we append a numeric suffix.
    '''
    
    if not isinstance(path, bytes):
        raise TypeError('path must be of type bytes')
    
    inode_p = inode_for_path(path, conn)
    
    i = 0
    newname = name
    name += b'-'
    try:
        while True:
            conn.get_val("SELECT inode FROM contents WHERE name=? AND parent_inode=?",
                         (newname, inode_p))       
            i += 1
            newname = name + bytes(i)
            
    except KeyError:
        pass
    
    return (inode_p, newname)