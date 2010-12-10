'''
fsck.py - this file is part of S3QL (http://s3ql.googlecode.com)

Copyright (C) 2008-2009 Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU LGPL.
'''

from __future__ import division, print_function, absolute_import

import os
from os.path import basename
import stat
import time
import logging
import re
from .database import NoSuchRowError
from .backends.common import NoSuchObject
from .common import (ROOT_INODE, CTRL_INODE, inode_for_path, sha256_fh, get_path)

__all__ = [ "fsck" ]

log = logging.getLogger("fsck")

# Init globals
# FIXME: Get rid of the globals
#pylint: disable=W0603
cachedir = None
bucket = None
expect_errors = False
found_errors = False
blocksize = None

S_IFMT = (stat.S_IFDIR | stat.S_IFREG | stat.S_IFSOCK | stat.S_IFBLK | 
          stat.S_IFCHR | stat.S_IFIFO | stat.S_IFLNK)

def fsck(cachedir_, bucket_, param, db):
    """Check file system
    
    Sets module variable `found_errors`.
    """

    global cachedir
    global bucket
    global found_errors
    global blocksize

    cachedir = cachedir_
    bucket = bucket_
    found_errors = False
    blocksize = param['blocksize']

    check_cache(db)
    check_lof(db)
    check_loops(db)
    check_inode_refcount(db)
    check_inode_sizes(db)
    check_inode_unix(db)
    check_obj_refcounts(db)
    check_keylist(db)


def log_error(*a, **kw):
    '''Log file system error if not expected'''

    if not expect_errors:
        return log.warn(*a, **kw)

def check_cache(conn):
    """Commit uncommitted cache files"""

    global found_errors

    log.info("Checking cached objects...")
    for filename in os.listdir(cachedir):
        found_errors = True

        match = re.match('^inode_(\\d+)_block_(\\d+)(\\.d)?$', filename)
        if match:
            inode = int(match.group(1))
            blockno = int(match.group(2))
            dirty = match.group(3) == '.d'
        else:
            raise RuntimeError('Strange file in cache directory: %s' % filename)
        
        if not dirty:
            log_error('Removing cached block %d of inode %d', blockno, inode)
            os.unlink(os.path.join(cachedir, filename))
            continue

        log_error("Committing changed block %d of inode %d to backend",
                  blockno, inode)

        fh = open(os.path.join(cachedir, filename), "rb")
        fh.seek(0, 2)
        size = fh.tell()
        fh.seek(0)
        hash_ = sha256_fh(fh)

        try:
            obj_id = conn.get_val('SELECT id FROM objects WHERE hash=?', (hash_,))
            
        except NoSuchRowError:
            obj_id = conn.rowid('INSERT INTO objects (refcount, hash, size) VALUES(?, ?, ?)',
                               (1, hash_, size))
            bucket.store_fh('s3ql_data_%d' % obj_id, fh)

        else:
            # Verify that this object actually exists
            if bucket.contains('s3ql_data_%d' % obj_id):
                conn.execute('UPDATE objects SET refcount=refcount+1 WHERE id=?',
                             (obj_id,))    
            else:
                # We don't want to delete the vanished object here, because
                # check_keylist will do a better job and print all affected
                # inodes. However, we need to reset the hash so that we can
                # insert the new object.
                conn.execute('UPDATE objects SET hash=NULL WHERE id=?', (obj_id,))
                obj_id = conn.rowid('INSERT INTO objects (refcount, hash, size) VALUES(?, ?, ?)',
                                    (1, hash_, size))
                bucket.store_fh('s3ql_data_%d' % obj_id, fh)

        try:
            old_obj_id = conn.get_val('SELECT obj_id FROM blocks WHERE inode=? AND blockno=?',
                                     (inode, blockno))
        except NoSuchRowError:
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

def check_lof(conn):
    """Ensure that there is a lost+found directory"""

    global found_errors
    log.info('Checking lost+found...')

    timestamp = time.time() - time.timezone
    try:
        inode_l = conn.get_val("SELECT inode FROM contents WHERE name=? AND parent_inode=?",
                             (b"lost+found", ROOT_INODE))

    except NoSuchRowError:
        found_errors = True
        log_error("Recreating missing lost+found directory")
        inode_l = conn.rowid("INSERT INTO inodes (mode,uid,gid,mtime,atime,ctime,refcount) "
                             "VALUES (?,?,?,?,?,?,?)",
                             (stat.S_IFDIR | stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR,
                              os.getuid(), os.getgid(), timestamp, timestamp, timestamp, 1))
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



def check_loops(conn):
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

def check_inode_sizes(conn):
    """Check if inode sizes agree with blocks"""

    global found_errors

    log.info('Checking inodes (sizes)...')

    conn.execute('CREATE TEMPORARY TABLE min_sizes '
                 '(id INTEGER PRIMARY KEY, min_size INTEGER NOT NULL)')
    try:
        conn.execute('''INSERT INTO min_sizes (id, min_size) 
                     SELECT inode, MAX(blockno * ? + size) 
                     FROM blocks JOIN objects ON obj_id == id 
                     GROUP BY inode''',
                     (blocksize,))
        
        conn.execute('''
           CREATE TEMPORARY TABLE wrong_sizes AS 
           SELECT id, size, min_size
             FROM inodes JOIN min_sizes USING (id) 
            WHERE size < min_size''')
        
        for (id_, size_old, size) in conn.query('SELECT * FROM wrong_sizes'):
            
            found_errors = True
            log_error("Size of inode %d (%s) does not agree with number of blocks, "
                      "setting from %d to %d",
                      id_, get_path(id_, conn), size_old, size)
            conn.execute("UPDATE inodes SET size=? WHERE id=?", (size, id_))
    finally:
        conn.execute('DROP TABLE min_sizes')
        conn.execute('DROP TABLE IF EXISTS wrong_sizes')
        
def check_inode_refcount(conn):
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
                (id_p, name) = resolve_free(b"/lost+found", b"inode-%d" % id_, conn)
                log_error("Inode %d not referenced, adding as /lost+found/%s", id_, name)
                conn.execute("INSERT INTO contents (name, inode, parent_inode) "
                             "VALUES (?,?,?)", (basename(name), id_, id_p))
                conn.execute("UPDATE inodes SET refcount=? WHERE id=?", (1, id_))
    
            else: 
                log_error("Inode %d (%s) has wrong reference count, setting from %d to %d",
                          id_, get_path(id_, conn), cnt_old, cnt)
                conn.execute("UPDATE inodes SET refcount=? WHERE id=?", (cnt, id_))
    finally:
        conn.execute('DROP TABLE refcounts')
        conn.execute('DROP TABLE IF EXISTS wrong_refcounts')


def check_inode_unix(conn):
    """Check inode attributes for agreement with UNIX conventions
    
    This means:
    - Only directories should have child entries
    - Only regular files should have data blocks and a size
    - Only symlinks should have a target
    - Only devices should have a device number
    
    Note that none of this is enforced by S3QL. However, as long
    as S3QL only communicates with the UNIX FUSE module, none of
    the above should happen (and if it does, it would probably 
    confuse the system quite a lot).
    """

    global found_errors

    log.info('Checking inodes (types)...')

    for (inode, mode, size, target, rdev) \
        in conn.query("SELECT id, mode, size, target, rdev FROM inodes"):

        if size != 0 and not stat.S_ISREG(mode):
            found_errors = True
            log_error('Inode %d (%s) is not regular file but has non-zero size. '
                      'This is probably going to confuse your system!',
                      inode, get_path(inode, conn))

        if target is not None and not stat.S_ISLNK(mode):
            found_errors = True
            log_error('Inode %d (%s) is not symlink but has symlink target. '
                      'This is probably going to confuse your system!',
                       inode, get_path(inode, conn))

        if rdev != 0 and not (stat.S_ISBLK(mode) or stat.S_ISCHR(mode)):
            found_errors = True
            log_error('Inode %d (%s) is not device but has device number. '
                      'This is probably going to confuse your system!',
                      inode, get_path(inode, conn))

        has_children = conn.has_val('SELECT 1 FROM contents WHERE parent_inode=? LIMIT 1',
                                    (inode,))
        if has_children and not stat.S_ISDIR(mode):
            found_errors = True
            log_error('Inode %d (%s) is not a directory but has child entries. '
                      'This is probably going to confuse your system!',
                      inode, get_path(inode, conn))

        has_blocks = conn.has_val('SELECT 1 FROM blocks WHERE inode=? LIMIT 1',
                                  (inode,))
        if has_blocks and not stat.S_ISREG(mode):
            found_errors = True
            log_error('Inode %d (%s) is not a regular file but has data blocks. '
                      'This is probably going to confuse your system!',
                       inode, get_path(inode, conn))


def check_obj_refcounts(conn):
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
                      id_, cnt_old, cnt or 0)
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

def check_keylist(conn):
    """Check the list of objects.

    Checks that:
    - all objects are referred in the object table
    - all objects in the object table exist
    - object has correct hash
    """

    log.info('Checking object list...')
    global found_errors

    lof_id = conn.get_val("SELECT inode FROM contents WHERE name=? AND parent_inode=?",
                          (b"lost+found", ROOT_INODE))

    # We use this table to keep track of the objects that we have
    # seen
    conn.execute("CREATE TEMP TABLE obj_ids (id INTEGER PRIMARY KEY)")
    try:
        for (i, obj_name) in enumerate(bucket.list('s3ql_data_')):
    
            if i != 0 and i % 5000 == 0:
                log.info('..processed %d objects so far..', i)
    
            # We only bother with data objects
            obj_id = int(obj_name[10:])
    
            conn.execute('INSERT INTO obj_ids VALUES(?)', (obj_id,))
        
        for (obj_id,) in conn.query('SELECT id FROM obj_ids '
                                    'EXCEPT SELECT id FROM objects'):
            found_errors = True
            log_error("Deleting spurious object %d",  obj_id)
            try:
                del bucket['s3ql_data_%d' % obj_id]
            except NoSuchObject:
                if bucket.read_after_write_consistent():
                    raise
    
        conn.execute('CREATE TEMPORARY TABLE missing AS '
                     'SELECT id FROM objects EXCEPT SELECT id FROM obj_ids')
        for (obj_id,) in conn.query('SELECT * FROM missing'):
            found_errors = True
            log_error("object %s only exists in table but not in bucket, deleting", obj_id)
            log_error("The following files may lack data and have been moved to /lost+found:")
            for (id_,) in conn.query('SELECT inode FROM blocks WHERE obj_id=?', (obj_id,)):
                for (name, id_p) in conn.query('SELECT name, parent_inode FROM contents '
                                               'WHERE inode=?', (id_,)):
                    path = get_path(id_p, conn, name)
                    log_error(path)
                    newname = path[1:].replace('_', '__').replace('/', '_')
                    
                    # Maybe there is a problem in this code path
                    # See http://code.google.com/p/s3ql/issues/detail?id=217
                    assert len(newname) < 10240    
                        
                    conn.execute('UPDATE contents SET name=?, parent_inode=? '
                                 'WHERE inode=?', (newname, lof_id, id_))
                    
            conn.execute("DELETE FROM blocks WHERE obj_id=?", (obj_id,))
            conn.execute("DELETE FROM objects WHERE id=?", (obj_id,))
    finally:
        conn.execute('DROP TABLE obj_ids')
        conn.execute('DROP TABLE IF EXISTS missing')


def resolve_free(path, name, conn):
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

    except NoSuchRowError:
        pass

    return (inode_p, newname)
