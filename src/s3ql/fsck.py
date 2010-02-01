'''
$Id$

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
from s3ql.common import (get_path, ROOT_INODE, CTRL_INODE, inode_for_path, sha256_fh)

__all__ = [ "fsck" ]

log = logging.getLogger("fsck")

# Init globals
conn = None
cachedir = None
bucket = None
expect_errors = False
found_errors = False


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
        check_dirs()
        check_loops()
        check_inode_refcount()
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
            s3key = conn.get_val('SELECT id FROM s3_objects WHERE hash=?', (hash_,))

        except KeyError:
            s3key = conn.rowid('INSERT INTO s3_objects (refcount, hash, size) VALUES(?, ?, ?)',
                               (1, hash_, size))
            bucket.store_fh('s3ql_data_%d' % s3key, fh, { 'hash': hash_ })

        else:
            conn.execute('UPDATE s3_objects SET refcount=refcount+1 WHERE id=?',
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

            refcount = conn.get_val('SELECT refcount FROM s3_objects WHERE id=?',
                                    (old_s3key,))
            if refcount > 1:
                conn.execute('UPDATE s3_objects SET refcount=refcount-1 WHERE id=?',
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

    for (key, refcount) in conn.query("SELECT id, refcount FROM s3_objects"):

        refcount2 = conn.get_val("SELECT COUNT(inode) FROM blocks WHERE s3key=?",
                                 (key,))
        if refcount != refcount2:
            log_error("S3 object %s has invalid refcount, setting from %d to %d",
                      key, refcount, refcount2)
            found_errors = True
            if refcount2 != 0:
                conn.execute("UPDATE s3_objects SET refcount=? WHERE id=?",
                             (refcount2, key))
            else:
                # Orphaned object will be picked up by check_keylist
                conn.execute('DELETE FROM s3_objects WHERE id=?', (key,))


def check_keylist():
    """Check the list of S3 objects.

    Checks that:
    - all s3 objects are referred in the s3 table
    - all objects in the s3 table exist
    - object has correct hash
    """

    log.info('Checking S3 key list...')
    global found_errors

    # We use this table to keep track of the s3keys that we have
    # seen
    conn.execute("CREATE TEMP TABLE s3keys AS SELECT id FROM s3_objects")

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
            conn.get_val("SELECT hash FROM s3_objects WHERE id=?", (s3key,))

        # Handle object that exists only in S3
        except KeyError:
            found_errors = True
            name = unused_filename('s3-object-%s' % s3key)
            log_error("object %s not referenced in s3 objects table, saving locally as ./%s",
                      s3key, name)
            if not expect_errors:
                bucket.fetch_fh('s3ql_data_%d' % s3key, open(name, 'w'))
            to_delete.append(s3key)
            continue

        # Mark object as seen
        conn.execute("DELETE FROM s3keys WHERE id=?", (s3key,))


    # Carry out delete
    if to_delete:
        log.info('Performing deferred S3 removals...')
    for s3key in to_delete:
        del bucket['s3ql_data_%d' % s3key]

    # Now handle objects that only exist in s3_objects
    for (s3key,) in conn.query("SELECT id FROM s3keys"):
        found_errors = True
        log_error("object %s only exists in table but not on s3, deleting", s3key)
        conn.execute("DELETE FROM blocks WHERE s3key=?", (s3key,))
        conn.execute("DELETE FROM s3_objects WHERE id=?", (s3key,))

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
