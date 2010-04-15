'''
fsck.py - this file is part of S3QL (http://s3ql.googlecode.com)

Copyright (C) 2008-2009 Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU LGPL.
'''

from __future__ import division, print_function

import os
from os.path import basename
import types
import stat
import time
import numbers
import logging
import re
from s3ql.database import NoUniqueValueError
from s3ql.common import (ROOT_INODE, CTRL_INODE, inode_for_path, sha256_fh)

__all__ = [ "fsck" ]

log = logging.getLogger("fsck")

# Init globals
conn = None
cachedir = None
bucket = None
expect_errors = False
found_errors = False

S_IFMT = (stat.S_IFDIR | stat.S_IFREG | stat.S_IFSOCK | stat.S_IFBLK |
          stat.S_IFCHR | stat.S_IFIFO | stat.S_IFLNK)

def fsck(dbcm, cachedir_, bucket_):
    """Check file system
    
    Sets module variable `found_errors`. Throws `FatalFsckError` 
    if the filesystem can not be repaired.
    """

    global conn
    global cachedir
    global bucket
    global found_errors

    cachedir = cachedir_
    bucket = bucket_
    found_errors = False

    with dbcm.transaction() as conn_:
        conn = conn_

        detect_fs()
        check_cache()
        check_lof()
        check_loops()
        check_inode_refcount()
        check_inode_unix()
        check_s3_refcounts()
        check_keylist()


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
        (label, blocksize, last_fsck, mountcnt, needs_fsck) \
 = conn.get_row("SELECT label, blocksize, last_fsck, mountcnt, "
                           "needs_fsck FROM parameters")
    except (KeyError, NoUniqueValueError):
        log_error("Cannot read filesystem parameters. "
                  "This does not appear to be a valid S3QL filesystem.")
        raise FatalFsckError()

    if not (isinstance(label, types.StringTypes)
         and isinstance(blocksize, numbers.Integral)
         and isinstance(last_fsck, numbers.Real)
         and isinstance(mountcnt, numbers.Integral)
         and isinstance(needs_fsck, numbers.Integral)):
        log_error("Cannot read filesystem parameters. "
                  "This does not appear to be a valid S3QL filesystem.")
        raise FatalFsckError()


def check_cache():
    """Commit uncommitted cache files"""

    global found_errors

    log.info("Checking cached objects...")
    for filename in os.listdir(cachedir):
        found_errors = True

        match = re.match('^inode_(\\d+)_block_(\\d+)$', filename)
        if match:
            (inode, blockno) = [ int(match.group(i)) for i in (1, 2) ]
        else:
            raise RuntimeError('Strange file in cache directory: %s' % filename)

        log_error("Committing (potentially changed) cache for inode %d, block %d",
                  inode, blockno)

        fh = open(os.path.join(cachedir, filename), "rb")
        fh.seek(0, 2)
        size = fh.tell()
        fh.seek(0)
        hash_ = sha256_fh(fh)

        try:
            s3key = conn.get_val('SELECT id FROM objects WHERE hash=?', (hash_,))

        except KeyError:
            s3key = conn.rowid('INSERT INTO objects (refcount, hash, size) VALUES(?, ?, ?)',
                               (1, hash_, size))
            bucket.store_fh('s3ql_data_%d' % s3key, fh)

        else:
            conn.execute('UPDATE objects SET refcount=refcount+1 WHERE id=?',
                         (s3key,))

        try:
            old_s3key = conn.get_val('SELECT s3key FROM blocks WHERE inode=? AND blockno=?',
                                     (inode, blockno))
        except KeyError:
            conn.execute('INSERT INTO blocks (s3key, inode, blockno) VALUES(?,?,?)',
                         (s3key, inode, blockno))
        else:
            conn.execute('UPDATE blocks SET s3key=? WHERE inode=? AND blockno=?',
                         (s3key, inode, blockno))

            refcount = conn.get_val('SELECT refcount FROM objects WHERE id=?',
                                    (old_s3key,))
            if refcount > 1:
                conn.execute('UPDATE objects SET refcount=refcount-1 WHERE id=?',
                             (old_s3key,))
            else:
                # Don't delete yet, maybe it's still referenced
                pass


        fh.close()
        os.unlink(os.path.join(cachedir, filename))

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



def check_loops():
    """Ensure that all directories can be reached from root"""

    global found_errors
    log.info('Checking directory reachability...')

    conn.execute('CREATE TEMPORARY TABLE loopcheck (inode INTEGER PRIMARY KEY, '
                 'parent_inode INTEGER)')
    conn.execute('CREATE INDEX ix_loopcheck_parent_inode ON loopcheck(parent_inode)')
    conn.execute('INSERT INTO loopcheck (inode, parent_inode) '
                 'SELECT inode, parent_inode FROM contents JOIN inodes ON inode == id '
                 'WHERE mode & ? == ?', (S_IFMT, stat.S_IFDIR))
    conn.execute('CREATE TEMPORARY TABLE loopcheck2 (inode INTEGER PRIMARY KEY)')
    conn.execute('INSERT INTO loopcheck2 (inode) SELECT inode FROM loopcheck')

    def delete_tree(inode_p):
        for (inode,) in conn.query("SELECT inode FROM loopcheck WHERE parent_inode=?",
                                   (inode_p,)):
            delete_tree(inode)
        conn.execute('DELETE FROM loopcheck2 WHERE inode=?', (inode_p,))

    delete_tree(ROOT_INODE)

    if conn.has_val("SELECT 1 FROM loopcheck2"):
        found_errors = True
        log_error("Found unreachable filesystem entries!\n"
                  "This problem cannot be corrected automatically yet.")

    conn.execute("DROP TABLE loopcheck")
    conn.execute("DROP TABLE loopcheck2")

def check_inode_refcount():
    """Check inode reference counters"""

    global found_errors

    log.info('Checking inodes (refcounts)...')

    for (inode, refcount_cached) in conn.query("SELECT id, refcount FROM inodes"):

        # No checks for root and control
        if inode in (ROOT_INODE, CTRL_INODE):
            continue

        refcount_actual = conn.get_val("SELECT COUNT(name) FROM contents WHERE inode=?", (inode,))

        if refcount_actual == 0:
            found_errors = True
            (inode_p, name) = resolve_free(b"/lost+found", b"inode-%d" % inode)
            log_error("Inode %d not referenced, adding as /lost+found/%s", inode, name)
            conn.execute("INSERT INTO contents (name, inode, parent_inode) "
                         "VALUES (?,?,?)", (basename(name), inode, inode_p))
            conn.execute("UPDATE inodes SET refcount=? WHERE id=?",
                         (1, inode))

        elif refcount_cached != refcount_actual:
            found_errors = True
            log_error("Inode %d has wrong reference count, setting from %d to %d",
                      inode, refcount_cached, refcount_actual)
            conn.execute("UPDATE inodes SET refcount=? WHERE id=?", (refcount_actual, inode))


def check_inode_unix():
    """Check inode attributes for agreement with UNIX conventions
    
    This means:
    - Only directories should have child entries
    - Only regular files should have data blocks and a size
    - Only symlinks should have a target
    - Only devices should have a device number
    - nlink_off should correspond to the number of child directories + 1
    
    Note that none of this is enforced by S3QL. However, as long
    as S3QL only communicates with the UNIX FUSE module, none of
    the above should happen (and if it does, it would probably 
    confuse the system quite a lot).
    """

    global found_errors

    log.info('Checking inodes (types)...')

    for (inode, mode, size, target, nlink_off,
         rdev) in conn.query("SELECT id, mode, size, target, nlink_off, rdev FROM inodes"):

        if stat.S_ISDIR(mode):
            # Count sub directories
            subdir_cnt = conn.get_val('SELECT COUNT(name) FROM contents JOIN inodes '
                                      'ON inode == id WHERE mode & ? == ? AND parent_inode = ?',
                                      (S_IFMT, stat.S_IFDIR, inode))
            if nlink_off != subdir_cnt + 1:
                found_errors = True
                log_error("Inode %d has wrong nlink_off, setting from %d to %d",
                          inode, nlink_off, subdir_cnt + 1)
                conn.execute("UPDATE inodes SET nlink_off=? WHERE id=?",
                             (subdir_cnt + 1, inode))

        else:
            if nlink_off != 0:
                found_errors = True
                log_error("Inode %d has wrong nlink_off, fixing.", inode)
                conn.execute("UPDATE inodes SET nlink_off=? WHERE id=?",
                             (0, inode))

        if size != 0 and not stat.S_ISREG(mode):
            found_errors = True
            log_error('Inode %d is not regular file but has non-zero size. '
                      'Don\'t know what to do.', inode)

        if target is not None and not stat.S_ISLNK(mode):
            found_errors = True
            log_error('Inode %d is not symlink but has symlink target. '
                      'Don\'t know what to do.', inode)

        if rdev != 0 and not (stat.S_ISBLK(mode) or stat.S_ISCHR(mode)):
            found_errors = True
            log_error('Inode %d is not device but has device number. '
                      'Don\'t know what to do.', inode)

        has_children = conn.has_val('SELECT 1 FROM contents WHERE parent_inode=? LIMIT 1',
                                    (inode,))
        if has_children and not stat.S_ISDIR(mode):
            found_errors = True
            log_error('Inode %d is not a directory but has child entries. '
                      'Don\'t know what to do.', inode)

        has_blocks = conn.has_val('SELECT 1 FROM blocks WHERE inode=? LIMIT 1',
                                  (inode,))
        if has_blocks and not stat.S_ISREG(mode):
            found_errors = True
            log_error('Inode %d is not a regualr file but has data blocks. '
                      'Don\'t know what to do.', inode)


def check_s3_refcounts():
    """Check object reference counts"""

    global found_errors
    log.info('Checking object reference counts...')

    for (key, refcount) in conn.query("SELECT id, refcount FROM objects"):

        refcount2 = conn.get_val("SELECT COUNT(inode) FROM blocks WHERE s3key=?",
                                 (key,))
        if refcount != refcount2:
            log_error("Object %s has invalid refcount, setting from %d to %d",
                      key, refcount, refcount2)
            found_errors = True
            if refcount2 != 0:
                conn.execute("UPDATE objects SET refcount=? WHERE id=?",
                             (refcount2, key))
            else:
                # Orphaned object will be picked up by check_keylist
                conn.execute('DELETE FROM objects WHERE id=?', (key,))


def check_keylist():
    """Check the list of objects.

    Checks that:
    - all objects are referred in the object table
    - all objects in the object table exist
    - object has correct hash
    """

    log.info('Checking object list...')
    global found_errors

    # We use this table to keep track of the s3keys that we have
    # seen
    conn.execute("CREATE TEMP TABLE s3keys AS SELECT id FROM objects")

    to_delete = list() # We can't delete the object during iteration
    for (i, s3key) in enumerate(bucket):

        if i % 5000 == 0:
            log.info('..processed %d objects so far..', i)

        # We only bother with data objects
        if not s3key.startswith("s3ql_data_"):
            continue
        else:
            s3key = int(s3key[len('s3ql_data_'):])

        # Retrieve object information from database
        try:
            conn.get_val("SELECT hash FROM objects WHERE id=?", (s3key,))

        # Handle object that exists only in S3
        except KeyError:
            found_errors = True
            name = unused_filename('s3-object-%s' % s3key)
            log_error("object %s not referenced in objects table, saving locally as ./%s",
                      s3key, name)
            if not expect_errors:
                bucket.fetch_fh('s3ql_data_%d' % s3key, open(name, 'wb'))
            to_delete.append(s3key)
            continue

        # Mark object as seen
        conn.execute("DELETE FROM s3keys WHERE id=?", (s3key,))


    # Carry out delete
    if to_delete:
        log.info('Performing deferred object removals...')
    for s3key in to_delete:
        del bucket['s3ql_data_%d' % s3key]

    # Now handle objects that only exist in objects
    for (s3key,) in conn.query("SELECT id FROM s3keys"):
        found_errors = True
        log_error("object %s only exists in table but not on s3, deleting", s3key)
        conn.execute("DELETE FROM blocks WHERE s3key=?", (s3key,))
        conn.execute("DELETE FROM objects WHERE id=?", (s3key,))

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
