#!/usr/bin/env python
#
#    Copyright (C) 2008  Nikolaus Rath <Nikolaus@rath.org>
#
#    This program can be distributed under the terms of the GNU LGPL.
#

import os
import types
import stat
from time import time
import tempfile
import numbers
import logging

from s3ql.common import (addfile, get_path, MyCursor, unused_name, ROOT_INODE)

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

def fsck(conn, cachedir, bucket, checkonly=False):
    """Checks a file system
    
    Returns `False` if any errors have been found. Throws `FatalFsckError` 
    if the filesystem could not be repaired.
    """

    found_errors = False
    
    cur = MyCursor(conn.get_cursor())
    try:
        cur.execute('SAVEPOINT fsck') 
    
        for fn in [ 
                   check_parameters,
                   check_contents,
                   check_inodes,
                   check_inode_s3key,
                   check_s3_objects,
                   check_keylist
                    ]:
            ret = fn(conn, cachedir, bucket, checkonly)
            if not ret:
                found_errors = True
    finally:
        if checkonly:
            # Roll back all the changes
            cur.execute('ROLLBACK TO SAVEPOINT fsck')
            
        cur.execute('RELEASE')
            
    return not found_errors 

class FatalFsckError(Exception):
    """An uncorrectable error has been found in the file system.

    """

    pass

def check_parameters(conn, cachedir, bucket, checkonly):
    """Check that file system parameters are set

    Returns `False` if any errors have been found.
    """
    cur = MyCursor(conn.cursor())

    log.info('Checking parameters...')
    try:
        (label, blocksize, last_fsck, mountcnt, version, needs_fsck) \
            = cur.get_row("SELECT label, blocksize, last_fsck, mountcnt, version, needs_fsck "
                          "FROM parameters")
    except StopIteration:
        (label, blocksize, last_fsck, mountcnt, version, needs_fsck) \
            = (None, None, None, None, None, None)
        
    if not (isinstance(label, types.StringTypes)
         and isinstance(blocksize, numbers.Integral)
         and isinstance(last_fsck, numbers.Real)
         and isinstance(mountcnt, numbers.Integral)
         and isinstance(version, numbers.Real)
         and isinstance(needs_fsck, numbers.Integral)):
        log.error("Cannot read filesystem parameters. This does not appear to be a valid S3QL filesystem.")
        raise FatalFsckError()

    return True
   
def check_cache(conn, cachedir, bucket, checkonly):
    """Commit any uncommitted cache files

    Returns `False` if any cache files have been found
    """

    cur = MyCursor(conn.cursor())
    found_errors = False

    log.info("Checking objects in cache...")
    
    for s3key in os.listdir(cachedir):
        found_errors = True
        log.warn("Committing (potentially changed) cache for %s", s3key)
        if not checkonly:
            etag = bucket.store_from_file(s3key, cachedir + s3key)
            cur.execute("UPDATE s3_objects SET etag=?, last_modified=? WHERE id=?",
                        (etag, time(), s3key))
            os.unlink(cachedir + s3key)

    return not found_errors


def check_contents(conn, cachedir, bucket, checkonly):
    """Check contents table.
    
    Ensures that:
    - There is a lost+found directory
    - entries have correct types, names do not contain /
    - Every directory is reachable from root
    
    Returns `False` if any errors have been found.
    """
    c1 = MyCursor(conn.cursor())                 
    c2 = MyCursor(conn.cursor())
    found_errors = False
    
    log.info('Checking TOC...')
    
    #
    # /lost+found
    #
    try:
        inode_l = c1.get_val("SELECT inode FROM contents WHERE name=? AND parent_inode=?", 
                             (buffer("lost+found"), ROOT_INODE))

    except StopIteration:
        found_errors = True
        log.warn("Recreating missing lost+found directory")
        c1.execute("INSERT INTO inodes (mode,uid,gid,mtime,atime,ctime,refcount) "
                   "VALUES (?,?,?,?,?,?,?)",
                   (stat.S_IFDIR | stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR,
                    os.getuid(), os.getgid(), time(), time(), time(), 2))
        inode_l = c1.last_rowid()
        c1.execute("INSERT INTO contents (name, inode, parent_inode) VALUES(?,?,?)",
                   (buffer("lost+found"), inode_l, ROOT_INODE))

    mode = c1.get_val('SELECT mode FROM inodes WHERE id=?', (inode_l,))
    if not stat.S_ISDIR(mode):
        found_errors = True
        log.warn('/lost+found is not a directory! Old entry will be saved as '
                 '/lost+found/inode-%s', inode_l)
        # We leave the old inode unassociated, so that it will be added
        # to lost+found later on.
        c1.execute("INSERT INTO inodes (mode,uid,gid,mtime,atime,ctime,refcount) "
                   "VALUES (?,?,?,?,?,?,?)",
                   (stat.S_IFDIR | stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR,
                    os.getuid(), os.getgid(), time(), time(), time(), 2))
        inode_l = c1.last_rowid()
        c1.execute('UPDATE contents SET inode=? WHERE name=? AND parent_inode=?',
                   (inode_l, buffer("lost+found"), ROOT_INODE))
            
   
    #    
    # Correct entries
    #
    for (name, inode, inode_p) in c1.execute("SELECT name, inode, parent_inode FROM contents"):
        if not isinstance(name, types.StringTypes) or b"/" in name:
            log.warn("Entry %s (inode %d) has invalid filename!", get_path(name, inode_p, c2), inode)
            found_errors = True
            # FIXME: Correct the problem
            log.warn("This problem cannot be corrected automatically yet.")
    
    #
    # Reachable
    #
    c1.execute("CREATE TEMPORARY TABLE loopcheck AS SELECT * FROM contents")
    def delete_tree(inode_p):
        subdirs = list()
        for (inode,) in c1.execute("SELECT inode FROM loopcheck WHERE parent_inode=?",
                                   (inode_p,)):
            mode = c2.get_val("SELECT mode FROM inodes WHERE id=?", (inode,))
            if stat.S_ISDIR(mode):
                list.append(inode)
        c1.execute("DELETE FROM loopcheck WHERE parent_inode=?", (inode_p,))
        for inode in subdirs:
            delete_tree(inode)
    delete_tree(ROOT_INODE)
    
    if c1.get_val("SELECT COUNT(inode) FROM loopcheck") > 0:
        log.warn("Found unreachable filesystem entries!")
        found_errors = True
        # TODO: Correct the problem
        log.warn("This problem cannot be corrected automatically yet.")
        
    c1.execute("DROP TABLE loopcheck")     
        

    return not found_errors



def check_inodes(conn, cachedir, bucket, checkonly):
    """Check inode table

    Checks that:
    - entries have correct types
    - refcounts are correct (recovers orphaned inodes)
    - there are no directory hardlinks

    Returns `False` if any errors have been found.
    """

    c1 = MyCursor(conn.cursor())
    c2 = MyCursor(conn.cursor())
    c3 = MyCursor(conn.cursor())
    found_errors = False

    log.info('Checking inodes...')
    
    inode_l = c1.get_val("SELECT inode FROM contents WHERE name=? AND parent_inode=?",
                         (buffer("lost+found"), ROOT_INODE))

    #
    # Correct types
    #
    # Note that some checks are already enforced by sqlite
    for (id, uid, gid, mode, mtime, atime, ctime, refcount, target, size, rdev) in \
        c1.execute("SELECT id, uid, gid, mode, mtime, atime, ctime, refcount, target, size, rdev "
                   "FROM inodes"):
        if not (
                isinstance(uid, numbers.Integral) and
                isinstance(gid, numbers.Integral) and 
                isinstance(mode, numbers.Integral) and 
                isinstance(mtime, numbers.Real) and
                isinstance(atime, numbers.Real) and
                isinstance(ctime, numbers.Real) and
                isinstance(refcount, numbers.Integral) and
                (target is None or isinstance(target, buffer)) and
                (size is None or isinstance(size, numbers.Integral)) and
                (rdev is None or isinstance(rdev, numbers.Integral))):
            log.warn("Inode %d has invalid attributes", id)
            found_errors = True
            # TODO: Correct this problem
            log.warn("This problem cannot be corrected automatically yet.")
            
    #
    # Reference counts
    #
    for (inode, refcount, mode) in c1.execute("SELECT id, refcount, mode FROM inodes WHERE inode != ?",
                                              (ROOT_INODE,)):
        
        # Make sure the inode is referenced
        try:
            refcount2 = c2.get_val("SELECT COUNT(name) FROM contents WHERE inode=?", (inode,))
            
        except StopIteration:
            found_errors = True
            log.warn("Inode %s not referenced, adding to lost+found", inode)
            name =  unused_name(c2, "inode-" + str(inode), inode_l)
            c2.execute("INSERT INTO contents (name, inode, parent_inode) "
                       "VALUES (?,?,?)", (buffer(name), inode, inode_l))
            c2.execute("UPDATE inodes SET refcount=? WHERE id=?",
                       (1, inode))

        # Check for directory hardlinks
        if stat.S_ISDIR(mode) and refcount2 > 1:
            found_errors = True
            linklist = c2.get_list('SELECT name, parent_inode FROM contents WHERE inode=?', (inode,))
            (name, inode_p) = linklist[0] # First entry is kept
            path = get_path(name, inode_p, c3)
            for (name, inode_p) in linklist[1:]:
                log.warn("Deleting directory hardlink %s (keeping %s)" % (get_path(name, inode_p, c3), path))
                c2.execute('DELETE FROM contents WHERE name=?, inode_p=?', (buffer(name), inode_p))
         
        # Check directory reference count
        if stat.S_ISDIR(mode):
            
            # Count number of subdirectories
            res2 = c2.execute('SELECT mode FROM contents JOIN inodes ON id = inode '
                              'WHERE parent_inode=? AND inode != parent_inode', (inode,))
            no = 2
            for (mode2,) in res2:
                if stat.S_ISDIR(mode2):
                    no += 1

            if no != refcount:
                found_errors = True
                log.warn("Fixing reference count of directory %s from %d to %d"
                     % (name, refcount, no))
                c2.execute("UPDATE inodes SET refcount=? WHERE id=?",
                           (no, inode))

        # Check file reference count
        if not stat.S_ISDIR(mode) and refcount != refcount2:
            found_errors = True
            log.warn("Fixing reference count of file %s from %d to %d",
                     (name, refcount, refcount2))
            c2.execute("UPDATE inodes SET refcount=? WHERE id=?",
                        (refcount2, inode))
             

    return not found_errors

def check_inode_s3key(conn, cachedir, bucket, checkonly):
    """Check inode_s3key table.

    Checks that:
    - entries have correct types
    - offsets are blocksize apart  

    Returns `False` if any errors have been found.
    """
    c1 = MyCursor(conn.cursor())
    c2 = MyCursor(conn.cursor())
    found_errors = False
    
    
def check_s3_objects(conn, cachedir, bucket, checkonly):
    """Checks s3_objects table.

    Checks that:
    - entries have correct types
    - refcounts are correct (recovers orphaned objects)

    Returns `False` if any errors have been found.
    """
    c1 = MyCursor(conn.cursor())
    c2 = MyCursor(conn.cursor())
    found_errors = False

    # Find blocksize
    blocksize = c1.get_val("SELECT blocksize FROM parameters")

    res = c1.execute("SELECT s3key, inode, offset FROM s3_objects")

    for (s3key, inode, offset) in res:

        # Check blocksize
        if offset % blocksize != 0:
            found_errors = True

            # Try to shift upward or downward
            offset_d = blocksize * int(offset/blocksize)
            offset_u = blocksize * (int(offset/blocksize)+1)
            if not list(c2.execute("SELECT s3key FROM s3_objects WHERE inode=? AND offset=?",
                                   (inode, offset_d))):
                log.warn("Object %s does not start at blocksize boundary, moving downwards"
                     % s3key)
                if not checkonly:
                    c2.execute("UPDATE s3_objects SET offset=? WHERE s3key=?",
                               (offset_d, s3key))

            elif not list(c2.execute("SELECT s3key FROM s3_objects WHERE inode=? AND offset=?",
                                     (inode, offset_u))):
                log.warn("Object %s does not start at blocksize boundary, moving upwards"
                     % s3key)
                if not checkonly:
                    c2.execute("UPDATE s3_objects SET offset=? WHERE s3key=?",
                               (offset_u, s3key))

            else:
                log.warn("Object %s does not start at blocksize boundary, deleting"
                     % s3key)
                if not checkonly:
                    c2.execute("DELETE FROM s3_objects WHERE s3key=?", (s3key,))


    return not found_errors


def check_keylist(conn, cachedir, bucket, checkonly):
    """Checks the list of S3 objects.

    Checks that:
    - no s3 object is larger than the blocksize
    - all s3 objects are referred in the s3 table
    - all objects in the s3 table exist
    - etags match (update metadata in case of conflict)

    Returns `False` if any errors have been found.
    """
    c1 = MyCursor(conn.cursor())
    c2 = MyCursor(conn.cursor())
    found_errors = False


    inode_l = c1.get_val("SELECT inode FROM contents WHERE name=? AND parent_inode=?",
                        (buffer("lost+found"), ROOT_INODE))

    # Find blocksize
    blocksize = c1.get_val("SELECT blocksize FROM parameters")

    # We use this table to keep track of the s3keys that we have
    # seen
    c1.execute("CREATE TEMP TABLE s3keys AS SELECT s3key FROM s3_objects")

    for (s3key, meta) in bucket.list_keys():

        # We only bother with our own objects
        if not s3key.startswith("s3ql_data_"):
            continue

        c1.execute("DELETE FROM s3keys WHERE s3key=?", (s3key,))

        # Size
        if meta.size > blocksize:
            found_errors = True
            log.warn("object %s is larger than blocksize (%d > %d), truncating (original object in lost+found)"
                 % (s3key, meta.size, blocksize))
            if not checkonly:
                tmp = tempfile.NamedTemporaryFile()
                bucket.fetch_to_file(s3key, tmp.name)

                # Save full object in lost+found
                addfile(unused_name(c1, s3key, inode_l), tmp, inode_l, c1, bucket)

                # Truncate and readd
                tmp.seek(blocksize)
                tmp.truncate()
                etag_new = bucket.store_from_file(s3key, tmp.name)
                tmp.close()
                c1.execute("UPDATE s3_objects SET etag=? WHERE s3key=?",
                           (etag_new, s3key))

        # Is it referenced in object table?
        res = list(c1.execute("SELECT etag,size FROM s3_objects WHERE s3key=?",
                   (s3key,)))

        # Object is not listed in object table
        if not res:
            found_errors = True
            log.warn("object %s not in referenced in table, adding to lost+found" % s3key)
            if not checkonly:
                lfname = unused_name(c1, s3key, inode_l)
                c1.execute("INSERT INTO inodes (mode,uid,gid,mtime,atime,ctime,refcount) "
                           "VALUES (?,?,?,?,?,?,?)",
                           (stat.S_IFREG | stat.S_IRUSR | stat.S_IWUSR,
                            os.getuid(), os.getgid(), time(), time(), time(), 1))
                inode = conn.last_insert_rowid()
                c1.execute("INSERT INTO contents (name, inode, parent_inode) VALUES(?,?,?)",
                           (buffer(lfname), inode, inode_l))

                # Now we need to assign the s3 object to this inode, but this
                # unfortunately means that we have to change the s3key.
                s3key_new = fs.io2s3key(inode,0)
                bucket.copy(s3key, s3key_new)
                del bucket[s3key]

                c1.execute("INSERT INTO s3_objects (inode,offset,s3key,size,etag) "
                           "VALUES (?,?,?,?)", (inode, 0, buffer(s3key_new),
                                                os.stat(tmp).st_size, meta.etag))

        # Object is in object table, check metadata
        else:
            (etag,size) = res[0]

            if not size == meta.size:
                found_errors = True
                log.warn("object %s has incorrect size in metadata, adjusting" % s3key)

                if not checkonly:
                    c1.execute("UPDATE s3_objects SET size=? WHERE s3key=?",
                               (meta.size, s3key))

            if not etag == meta.etag:
                found_errors = True
                log.warn("object %s has incorrect etag in metadata, adjusting" % s3key)

                if not checkonly:
                    c1.execute("UPDATE s3_objects SET etag=? WHERE s3key=?",
                               (meta.etag, s3key))


    # Now handle objects that only exist in s3_objects
    res = c2.execute("SELECT s3key FROM s3keys")
    for (s3key,) in res:
        found_errors = True
        log.warn("object %s only exists in table but not on s3, deleting" % s3key)
        if not checkonly:
            c1.execute("DELETE FROM s3_objects WHERE s3key=?", (buffer(s3key),))

    return not found_errors


