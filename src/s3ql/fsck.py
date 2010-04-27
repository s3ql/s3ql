'''
fsck.py - this file is part of S3QL (http://s3ql.googlecode.com)

Copyright (C) 2008-2009 Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU LGPL.
'''

from __future__ import division, print_function

import os
from os.path import basename
import stat
import time
import logging
import re
from .common import (ROOT_INODE, CTRL_INODE, inode_for_path, sha256_fh)

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

        check_cache()
        check_lof()
        check_loops()
        check_inode_refcount()
        check_inode_unix()
        check_obj_refcounts()
        check_keylist()


def log_error(*a, **kw):
    '''Log file system error if not expected'''

    if not expect_errors:
        return log.warn(*a, **kw)

class FatalFsckError(Exception):
    """An uncorrectable error has been found in the file system.

    """

    pass


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
            obj_id = conn.get_val('SELECT id FROM objects WHERE hash=?', (hash_,))

        except KeyError:
            obj_id = conn.rowid('INSERT INTO objects (refcount, hash, size) VALUES(?, ?, ?)',
                               (1, hash_, size))
            bucket.store_fh('s3ql_data_%d' % obj_id, fh)

        else:
            conn.execute('UPDATE objects SET refcount=refcount+1 WHERE id=?',
                         (obj_id,))

        try:
            old_obj_id = conn.get_val('SELECT obj_id FROM blocks WHERE inode=? AND blockno=?',
                                     (inode, blockno))
        except KeyError:
            conn.execute('INSERT INTO blocks (obj_id, inode, blockno) VALUES(?,?,?)',
                         (obj_id, inode, blockno))
        else:
            conn.execute('UPDATE blocks SET obj_id=? WHERE inode=? AND blockno=?',
                         (obj_id, inode, blockno))

            refcount = conn.get_val('SELECT refcount FROM objects WHERE id=?',
                                    (old_obj_id,))
            if refcount > 1:
                conn.execute('UPDATE objects SET refcount=refcount-1 WHERE id=?',
                             (old_obj_id,))
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
        inode_l = conn.rowid("INSERT INTO inodes (mode,uid,gid,mtime,atime,ctime,refcount, nlink_off) "
                             "VALUES (?,?,?,?,?,?,?, ?)",
                             (stat.S_IFDIR | stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR,
                              os.getuid(), os.getgid(), timestamp, timestamp, timestamp, 1, 1))
        conn.execute("INSERT INTO contents (name, inode, parent_inode) VALUES(?,?,?)",
                     (b"lost+found", inode_l, ROOT_INODE))
        conn.execute('UPDATE inodes SET nlink_off=nlink_off+1 WHERE id=? AND nlink_off != 0',
                     (ROOT_INODE,))


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

    conn.execute('CREATE TEMPORARY TABLE refcounts '
                 '(id INTEGER PRIMARY KEY, refcount INTEGER NOT NULL)')
    try:
        conn.execute('INSERT INTO refcounts (id, refcount) '
                     'SELECT inode, COUNT(name) FROM contents GROUP BY inode')
        
        conn.execute('''
           CREATE TEMPORARY TABLE wrong_refcounts AS 
           SELECT id, refcounts.refcount, inodes.refcount 
             FROM inodes LEFT JOIN refcounts USING (id) 
            WHERE inodes.refcount != refcounts.refcount 
               OR refcounts.refcount IS NULL''')
        
        for (id_, cnt, cnt_old) in conn.query('SELECT * FROM wrong_refcounts'):
            # No checks for root and control
            if id_ in (ROOT_INODE, CTRL_INODE):
                continue
            
            found_errors = True
            if cnt is None:
                (id_p, name) = resolve_free(b"/lost+found", b"inode-%d" % id_)
                log_error("Inode %d not referenced, adding as /lost+found/%s", id_, name)
                conn.execute("INSERT INTO contents (name, inode, parent_inode) "
                             "VALUES (?,?,?)", (basename(name), id_, id_p))
                conn.execute("UPDATE inodes SET refcount=? WHERE id=?", (1, id_))
    
            else: 
                log_error("Inode %d has wrong reference count, setting from %d to %d",
                          id_, cnt_old, cnt)
                conn.execute("UPDATE inodes SET refcount=? WHERE id=?", (cnt, id_))
    finally:
        conn.execute('DROP TABLE refcounts')
        conn.execute('DROP TABLE IF EXISTS wrong_refcounts')


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


def check_obj_refcounts():
    """Check object reference counts"""

    global found_errors
    log.info('Checking object reference counts...')
    
    conn.execute('CREATE TEMPORARY TABLE refcounts '
                 '(id INTEGER PRIMARY KEY, refcount INTEGER NOT NULL)')
    try:
        conn.execute('INSERT INTO refcounts (id, refcount) '
                     'SELECT obj_id, COUNT(inode) FROM blocks GROUP BY obj_id')
        
        conn.execute('''
           CREATE TEMPORARY TABLE wrong_refcounts AS 
           SELECT id, refcounts.refcount, objects.refcount 
             FROM objects LEFT JOIN refcounts USING (id) 
            WHERE objects.refcount != refcounts.refcount 
               OR refcounts.refcount IS NULL''')
        
        for (id_, cnt, cnt_old) in conn.query('SELECT * FROM wrong_refcounts'):
            log_error("Object %s has invalid refcount, setting from %d to %d",
                        id_, cnt_old, cnt)
            found_errors = True
            if cnt is not None:
                conn.execute("UPDATE objects SET refcount=? WHERE id=?",
                             (cnt, id_))
            else:
                # Orphaned object will be picked up by check_keylist
                conn.execute('DELETE FROM objects WHERE id=?', (id_,))
    finally:
        conn.execute('DROP TABLE refcounts')
        conn.execute('DROP TABLE IF EXISTS wrong_refcounts')

def check_keylist():
    """Check the list of objects.

    Checks that:
    - all objects are referred in the object table
    - all objects in the object table exist
    - object has correct hash
    """

    log.info('Checking object list...')
    global found_errors

    # We use this table to keep track of the object that we have
    # seen
    conn.execute("CREATE TEMP TABLE obj_ids (id INTEGER PRIMARY KEY)")
    try:
        for (i, obj_name) in enumerate(bucket):
    
            if i % 5000 == 0:
                log.info('..processed %d objects so far..', i)
    
            # We only bother with data objects
            if not obj_name.startswith("s3ql_data_"):
                continue
            else:
                obj_id = int(obj_name[len('s3ql_data_'):])
    
            conn.execute('INSERT INTO obj_ids VALUES(?)', (obj_id,))
        
        for (obj_id,) in conn.query('SELECT id FROM obj_ids '
                                    'EXCEPT SELECT id FROM objects'):
            found_errors = True
            name = unused_filename('object-%s' % obj_id)
            log_error("object %s not referenced in objects table, saving locally as ./%s",
                      obj_id, name)
            if not expect_errors:
                bucket.fetch_fh('s3ql_data_%d' % obj_id, open(name, 'wb'))
            del bucket['s3ql_data_%d' % obj_id]
    
        conn.execute('CREATE TEMPORARY TABLE missing AS '
                     'SELECT id FROM objects EXCEPT SELECT id FROM obj_ids')
        for (obj_id,) in conn.query('SELECT * FROM missing'):
            found_errors = True
            log_error("object %s only exists in table but not in bucket, deleting", obj_id)
            conn.execute("DELETE FROM blocks WHERE obj_id=?", (obj_id,))
            conn.execute("DELETE FROM objects WHERE id=?", (obj_id,))
    finally:
        conn.execute('DROP TABLE obj_ids')
        conn.execute('DROP TABLE IF EXISTS missing')

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
