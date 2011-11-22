'''
fsck.py - this file is part of S3QL (http://s3ql.googlecode.com)

Copyright (C) 2008-2009 Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU GPLv3.
'''

from __future__ import division, print_function, absolute_import
from .backends.common import NoSuchObject
from .common import ROOT_INODE, CTRL_INODE, inode_for_path, sha256_fh, get_path, BUFSIZE
from .database import NoSuchRowError
from os.path import basename
from random import randint
from .inode_cache import MIN_INODE, MAX_INODE, OutOfInodesError
import apsw
import logging
import os
import re
import shutil
import stat
import time

log = logging.getLogger("fsck")

S_IFMT = (stat.S_IFDIR | stat.S_IFREG | stat.S_IFSOCK | stat.S_IFBLK | 
          stat.S_IFCHR | stat.S_IFIFO | stat.S_IFLNK)

class Fsck(object):

    def __init__(self, cachedir_, bucket_, param, conn):

        self.cachedir = cachedir_
        self.bucket = bucket_
        self.expect_errors = False
        self.found_errors = False
        self.uncorrectable_errors = False
        self.blocksize = param['blocksize']
        self.conn = conn     
        
        # Set of blocks that have been unlinked by check_cache.
        # check_block_refcounts() will not report errors if these blocks still
        # exist even though they have refcount=0
        self.unlinked_blocks = set()
        
        # Similarly for objects
        self.unlinked_objects = set()
    
    def check(self):
        """Check file system
        
        Sets instance variable `found_errors`.
        """
        
        # Create indices required for reference checking
        log.info('Creating temporary extra indices...')
        self.conn.execute('DROP INDEX IF EXISTS tmp1')
        self.conn.execute('DROP INDEX IF EXISTS tmp2')
        self.conn.execute('DROP INDEX IF EXISTS tmp3')
        self.conn.execute('CREATE INDEX tmp1 ON blocks(obj_id)')
        self.conn.execute('CREATE INDEX tmp2 ON inode_blocks(block_id)')
        self.conn.execute('CREATE INDEX tmp3 ON contents(inode)')
        try:
            self.check_foreign_keys()
            self.check_cache()
            self.check_lof()
            self.check_name_refcount()
            self.check_contents()
            self.check_inode_refcount()            
            self.check_loops()
            self.check_inode_sizes()
            self.check_inode_unix()
            self.check_block_refcount()
            self.check_obj_refcounts()
            self.check_keylist()
        finally:
            log.info('Dropping temporary indices...')
            for idx in ('tmp1', 'tmp2', 'tmp3'):
                self.conn.execute('DROP INDEX %s' % idx)
    
    def log_error(self, *a, **kw):
        '''Log file system error if not expected'''
    
        if not self.expect_errors:
            return log.warn(*a, **kw)
        
    def check_foreign_keys(self):
        '''Check for referential integrity
        
        Checks that all foreign keys in the SQLite tables actually resolve.
        This is necessary, because we disable runtime checking by SQLite 
        for performance reasons.
        '''
        
        log.info("Checking referential integrity...")
        
        errors_found = True
        while errors_found:
            errors_found = False
            
            for (table,) in self.conn.query("SELECT name FROM sqlite_master WHERE type='table'"):
                for row in self.conn.query('PRAGMA foreign_key_list(%s)' % table):
                    sql_objs = { 'src_table': table,
                                'dst_table': row[2],
                                'src_col': row[3],
                                'dst_col': row[4] }
                    to_delete = []
                    for (val,) in self.conn.query('SELECT %(src_table)s.%(src_col)s '
                                                  'FROM %(src_table)s LEFT JOIN %(dst_table)s '
                                                  'ON %(src_table)s.%(src_col)s = %(dst_table)s.%(dst_col)s '
                                                  'WHERE %(dst_table)s.%(dst_col)s IS NULL '
                                                  'AND %(src_table)s.%(src_col)s IS NOT NULL'
                                                  % sql_objs):
                        self.found_errors = True
                        sql_objs['val'] = val
                        self.log_error('%(src_table)s.%(src_col)s refers to non-existing key %(val)s '
                                       'in %(dst_table)s.%(dst_col)s, deleting.', sql_objs)
                        to_delete.append(val)
                        
                    for val in to_delete:
                        self.conn.execute('DELETE FROM %(src_table)s WHERE %(src_col)s = ?'
                                          % sql_objs, (val,))
                    if to_delete:
                        errors_found = True     
        
    def check_cache(self):
        """Commit uncommitted cache files"""
    
        log.info("Checking cached objects...")
        if not os.path.exists(self.cachedir):
            return
        
        for filename in os.listdir(self.cachedir):
            self.found_errors = True
    
            match = re.match('^(\\d+)-(\\d+)$', filename)
            if match:
                inode = int(match.group(1))
                blockno = int(match.group(2))
            else:
                raise RuntimeError('Strange file in cache directory: %s' % filename)
    
            self.log_error("Committing block %d of inode %d to backend",
                      blockno, inode)
    
            fh = open(os.path.join(self.cachedir, filename), "rb")
            size = os.fstat(fh.fileno()).st_size
            hash_ = sha256_fh(fh)
    
            try:
                (block_id, obj_id) = self.conn.get_row('SELECT id, obj_id FROM blocks WHERE hash=?', (hash_,))
                
            except NoSuchRowError:
                obj_id = self.conn.rowid('INSERT INTO objects (refcount) VALUES(1)')
                block_id = self.conn.rowid('INSERT INTO blocks (refcount, hash, obj_id, size) '
                                           'VALUES(?, ?, ?, ?)', (1, hash_, obj_id, size))
                def do_write(obj_fh):
                    fh.seek(0)
                    shutil.copyfileobj(fh, obj_fh, BUFSIZE)
                    return obj_fh
                                        
                obj_size = self.bucket.perform_write(do_write, 's3ql_data_%d' % obj_id).get_obj_size()

                self.conn.execute('UPDATE objects SET size=? WHERE id=?', 
                                  (obj_size, obj_id))
    
            else:
                self.conn.execute('UPDATE blocks SET refcount=refcount+1 WHERE id=?', (block_id,))                         
    
    
            try:
                old_block_id = self.conn.get_val('SELECT block_id FROM inode_blocks '
                                                 'WHERE inode=? AND blockno=?', (inode, blockno))
            except NoSuchRowError:
                self.conn.execute('INSERT INTO inode_blocks (block_id, inode, blockno) VALUES(?,?,?)',
                                  (block_id, inode, blockno))
            else:
                self.conn.execute('UPDATE inode_blocks SET block_id=? WHERE inode=? AND blockno=?',
                                  (block_id, inode, blockno))
    
                # We just decrease the refcount, but don't take any action
                # because the reference count might be wrong 
                self.conn.execute('UPDATE blocks SET refcount=refcount-1 WHERE id=?', (old_block_id,))
                self.unlinked_blocks.add(old_block_id)
                
                fh.close()
            os.unlink(os.path.join(self.cachedir, filename))
            
    
    def check_lof(self):
        """Ensure that there is a lost+found directory"""
    
        log.info('Checking lost+found...')
    
        timestamp = time.time() - time.timezone
        try:
            (inode_l, name_id) = self.conn.get_row("SELECT inode, name_id FROM contents_v "
                                                   "WHERE name=? AND parent_inode=?", (b"lost+found", ROOT_INODE))
    
        except NoSuchRowError:
            self.found_errors = True
            self.log_error("Recreating missing lost+found directory")
            inode_l = self.create_inode(mode=stat.S_IFDIR|stat.S_IRUSR|stat.S_IWUSR|stat.S_IXUSR,
                                        atime=timestamp, ctime=timestamp, mtime=timestamp,
                                        refcount=1)
            self.conn.execute("INSERT INTO contents (name_id, inode, parent_inode) VALUES(?,?,?)",
                              (self._add_name(b"lost+found"), inode_l, ROOT_INODE))
    
    
        mode = self.conn.get_val('SELECT mode FROM inodes WHERE id=?', (inode_l,))
        if not stat.S_ISDIR(mode):
            self.found_errors = True
            self.log_error('/lost+found is not a directory! Old entry will be saved as '
                           '/lost+found/inode-%s*', inode_l)
            # We leave the old inode unassociated, so that it will be added
            # to lost+found later on.
            inode_l = self.create_inode(mode=stat.S_IFDIR|stat.S_IRUSR|stat.S_IWUSR|stat.S_IXUSR,
                                        atime=timestamp, ctime=timestamp, mtime=timestamp,
                                        refcount=1)
            self.conn.execute('UPDATE contents SET inode=? WHERE name_id=? AND parent_inode=?',
                              (inode_l, name_id, ROOT_INODE))
    
    def check_contents(self):
        """Check direntry names"""

        log.info('Checking directory entry names...')
        
        for (name, id_p) in self.conn.query('SELECT name, parent_inode FROM contents_v '
                                            'WHERE LENGTH(name) > 255'):
            path = get_path(id_p, self.conn, name)
            self.log_error('Entry name %s... in %s has more than 255 characters, '
                           'this could cause problems', name[:40], path[:-len(name)])
            self.found_errors = True            
            
    
    def check_loops(self):
        """Ensure that all directories can be reached from root"""

        log.info('Checking directory reachability...')
    
        self.conn.execute('CREATE TEMPORARY TABLE loopcheck (inode INTEGER PRIMARY KEY, '
                     'parent_inode INTEGER)')
        self.conn.execute('CREATE INDEX ix_loopcheck_parent_inode ON loopcheck(parent_inode)')
        self.conn.execute('INSERT INTO loopcheck (inode, parent_inode) '
                     'SELECT inode, parent_inode FROM contents JOIN inodes ON inode == id '
                     'WHERE mode & ? == ?', (S_IFMT, stat.S_IFDIR))
        self.conn.execute('CREATE TEMPORARY TABLE loopcheck2 (inode INTEGER PRIMARY KEY)')
        self.conn.execute('INSERT INTO loopcheck2 (inode) SELECT inode FROM loopcheck')
    
        def delete_tree(inode_p):
            for (inode,) in self.conn.query("SELECT inode FROM loopcheck WHERE parent_inode=?",
                                       (inode_p,)):
                delete_tree(inode)
            self.conn.execute('DELETE FROM loopcheck2 WHERE inode=?', (inode_p,))
    
        delete_tree(ROOT_INODE)
    
        if self.conn.has_val("SELECT 1 FROM loopcheck2"):
            self.found_errors = True
            self.uncorrectable_errors = True
            self.log_error("Found unreachable filesystem entries!\n"
                           "This problem cannot be corrected automatically yet.")
    
        self.conn.execute("DROP TABLE loopcheck")
        self.conn.execute("DROP TABLE loopcheck2")
    
    def check_inode_sizes(self):
        """Check if inode sizes agree with blocks"""
    
        log.info('Checking inodes (sizes)...')
    
        self.conn.execute('CREATE TEMPORARY TABLE min_sizes '
                          '(id INTEGER PRIMARY KEY, min_size INTEGER NOT NULL)')
        try:
            self.conn.execute('''
            INSERT INTO min_sizes (id, min_size) 
            SELECT inode, MAX(blockno * ? + size) 
            FROM inode_blocks JOIN blocks ON block_id == blocks.id 
            GROUP BY inode''', (self.blocksize,))
            
            self.conn.execute('''
               CREATE TEMPORARY TABLE wrong_sizes AS 
               SELECT id, size, min_size
                 FROM inodes JOIN min_sizes USING (id)
                WHERE size < min_size''')
            
            for (id_, size_old, size) in self.conn.query('SELECT * FROM wrong_sizes'):
                
                self.found_errors = True
                self.log_error("Size of inode %d (%s) does not agree with number of blocks, "
                               "setting from %d to %d",
                               id_, get_path(id_, self.conn), size_old, size)
                self.conn.execute("UPDATE inodes SET size=? WHERE id=?", (size, id_))
        finally:
            self.conn.execute('DROP TABLE min_sizes')
            self.conn.execute('DROP TABLE IF EXISTS wrong_sizes')
            
    def _add_name(self, name):
        '''Get id for *name* and increase refcount
        
        Name is inserted in table if it does not yet exist.
        '''
        
        try:
            name_id = self.conn.get_val('SELECT id FROM names WHERE name=?', (name,))
        except NoSuchRowError:
            name_id = self.conn.rowid('INSERT INTO names (name, refcount) VALUES(?,?)',
                                      (name, 1))
        else:
            self.conn.execute('UPDATE names SET refcount=refcount+1 WHERE id=?', (name_id,))
        return name_id

    def _del_name(self, name_id):
        '''Decrease refcount for name_id, remove if it reaches 0'''
        
        self.conn.execute('UPDATE names SET refcount=refcount-1 WHERE id=?', (name_id,))
        self.conn.execute('DELETE FROM names WHERE refcount=0 AND id=?', (name_id,))
                            
    def check_inode_refcount(self):
        """Check inode reference counters"""
    
        log.info('Checking inodes (refcounts)...')
        
        self.conn.execute('CREATE TEMPORARY TABLE refcounts '
                          '(id INTEGER PRIMARY KEY, refcount INTEGER NOT NULL)')
        try:
            self.conn.execute('INSERT INTO refcounts (id, refcount) '
                              'SELECT inode, COUNT(name_id) FROM contents GROUP BY inode')
            
            self.conn.execute('''
               CREATE TEMPORARY TABLE wrong_refcounts AS 
               SELECT id, refcounts.refcount, inodes.refcount 
                 FROM inodes LEFT JOIN refcounts USING (id) 
                WHERE inodes.refcount != refcounts.refcount 
                   OR refcounts.refcount IS NULL''')
            
            for (id_, cnt, cnt_old) in self.conn.query('SELECT * FROM wrong_refcounts'):
                # No checks for root and control
                if id_ in (ROOT_INODE, CTRL_INODE):
                    continue
                
                self.found_errors = True
                if cnt is None:
                    (id_p, name) = self.resolve_free(b"/lost+found", b"inode-%d" % id_)
                    self.log_error("Inode %d not referenced, adding as /lost+found/%s", id_, name)
                    self.conn.execute("INSERT INTO contents (name_id, inode, parent_inode) "
                                      "VALUES (?,?,?)", (self._add_name(basename(name)), id_, id_p))
                    self.conn.execute("UPDATE inodes SET refcount=? WHERE id=?", (1, id_))
        
                else: 
                    self.log_error("Inode %d (%s) has wrong reference count, setting from %d to %d",
                              id_, get_path(id_, self.conn), cnt_old, cnt)
                    self.conn.execute("UPDATE inodes SET refcount=? WHERE id=?", (cnt, id_))
        finally:
            self.conn.execute('DROP TABLE refcounts')
            self.conn.execute('DROP TABLE IF EXISTS wrong_refcounts')
            
        
    def check_block_refcount(self):
        """Check block reference counters"""
    
        log.info('Checking blocks (refcounts)...')
    
        self.conn.execute('CREATE TEMPORARY TABLE refcounts '
                          '(id INTEGER PRIMARY KEY, refcount INTEGER NOT NULL)')
        try:
            self.conn.execute('''
               INSERT INTO refcounts (id, refcount) 
                 SELECT block_id, COUNT(blockno)  
                 FROM inode_blocks
                 GROUP BY block_id
            ''')
            
            self.conn.execute('''
               CREATE TEMPORARY TABLE wrong_refcounts AS 
               SELECT id, refcounts.refcount, blocks.refcount, obj_id 
                 FROM blocks LEFT JOIN refcounts USING (id) 
                WHERE blocks.refcount != refcounts.refcount 
                   OR refcounts.refcount IS NULL''')
            
            for (id_, cnt, cnt_old, obj_id) in self.conn.query('SELECT * FROM wrong_refcounts'):                 
                if cnt is None and id_ in self.unlinked_blocks and cnt_old == 0:
                    # Block was unlinked by check_cache and can now really be
                    # removed (since we have checked that there are truly no
                    # other references)
                    self.conn.execute('DELETE FROM blocks WHERE id=?', (id_,))
                    
                    # We can't remove associated objects yet, because their refcounts
                    # might be wrong, too.
                    self.conn.execute('UPDATE objects SET refcount=refcount-1 WHERE id=?', (obj_id,))
                    self.unlinked_objects.add(obj_id)
                    
                elif cnt is None:
                    self.found_errors = True
                    (id_p, name) = self.resolve_free(b"/lost+found", b"block-%d" % id_)
                    self.log_error("Block %d not referenced, adding as /lost+found/%s", id_, name)
                    timestamp = time.time() - time.timezone
                    size = self.conn.get_val('SELECT size FROM blocks WHERE id=?', (id_,))
                    inode = self.create_inode(mode=stat.S_IFREG|stat.S_IRUSR|stat.S_IWUSR|stat.S_IXUSR,
                                              mtime=timestamp, atime=timestamp, ctime=timestamp, 
                                              refcount=1, size=size)
                    self.conn.execute('INSERT INTO inode_blocks (inode, blockno, block_id) VALUES(?,?,?)',
                                      (inode, 0, id_))
                    self.conn.execute("INSERT INTO contents (name_id, inode, parent_inode) VALUES (?,?,?)", 
                                      (self._add_name(basename(name)), inode, id_p))
                    self.conn.execute("UPDATE blocks SET refcount=? WHERE id=?", (1, id_))
                
                else: 
                    self.found_errors = True
                    self.log_error("Block %d has wrong reference count, setting from %d to %d",
                                   id_, cnt_old, cnt)
                    self.conn.execute("UPDATE blocks SET refcount=? WHERE id=?", (cnt, id_))
        finally:
            self.conn.execute('DROP TABLE refcounts')
            self.conn.execute('DROP TABLE IF EXISTS wrong_refcounts')    
    
                    
    def create_inode(self, mode, uid=os.getuid(), gid=os.getgid(),
                     mtime=None, atime=None, ctime=None, refcount=None,
                     size=0):
        '''Create inode with id fitting into 32bit'''
        
        for _ in range(100):
            id_ = randint(MIN_INODE, MAX_INODE)
            try:
                self.conn.execute('INSERT INTO inodes (id, mode,uid,gid,mtime,atime,ctime,'
                                  'refcount,size) VALUES (?,?,?,?,?,?,?,?,?)',
                                   (id_, mode, uid, gid, mtime, atime, ctime, refcount, size))
            except apsw.ConstraintError:
                pass
            else:
                break
        else:
            raise OutOfInodesError()
                
        return id_
    
    def check_name_refcount(self):
        """Check name reference counters"""
    
        log.info('Checking names (refcounts)...')
    
        self.conn.execute('CREATE TEMPORARY TABLE refcounts '
                          '(id INTEGER PRIMARY KEY, refcount INTEGER NOT NULL)')
        try:
            self.conn.execute('INSERT INTO refcounts (id, refcount) '
                              'SELECT id, 0 FROM names')
            self.conn.execute('UPDATE refcounts SET refcount='
                              '(SELECT COUNT(name_id) FROM contents WHERE name_id = refcounts.id)'
                              '+ (SELECT COUNT(name_id) FROM ext_attributes '
                              '   WHERE name_id = refcounts.id)')
                
            self.conn.execute('''
               CREATE TEMPORARY TABLE wrong_refcounts AS 
               SELECT id, refcounts.refcount, names.refcount 
                 FROM names LEFT JOIN refcounts USING (id) 
                WHERE names.refcount != refcounts.refcount 
                   OR refcounts.refcount IS NULL''')
            
            for (id_, cnt, cnt_old) in self.conn.query('SELECT * FROM wrong_refcounts'):                
                self.found_errors = True
                if cnt is None:
                    self.log_error("Name %d not referenced, removing (old refcount: %d)",
                                   id_, cnt_old)
                    self.conn.execute('DELETE FROM names WHERE id=?', (id_,))
                else:
                    self.log_error("Name %d has wrong reference count, setting from %d to %d",
                                   id_, cnt_old, cnt)
                    self.conn.execute("UPDATE names SET refcount=? WHERE id=?", (cnt, id_))
        finally:
            self.conn.execute('DROP TABLE refcounts')
            self.conn.execute('DROP TABLE IF EXISTS wrong_refcounts')    
                
    def check_inode_unix(self):
        """Check inode attributes for agreement with UNIX conventions
        
        This means:
        - Only directories should have child entries
        - Only regular files should have data blocks and a size
        - Only symlinks should have a target
        - Only devices should have a device number
        - symlink size is length of target
        
        Note that none of this is enforced by S3QL. However, as long
        as S3QL only communicates with the UNIX FUSE module, none of
        the above should happen (and if it does, it would probably 
        confuse the system quite a lot).
        """
    
        log.info('Checking inodes (types)...')
    
        for (inode, mode, size, target, rdev) \
            in self.conn.query("SELECT id, mode, size, target, rdev "
                               "FROM inodes LEFT JOIN symlink_targets ON id = inode"):
    
            if stat.S_ISLNK(mode) and target is None: 
                self.found_errors = True
                self.log_error('Inode %d (%s): symlink does not have target. '
                               'This is probably going to confuse your system!',
                               inode, get_path(inode, self.conn)) 
                            
            if stat.S_ISLNK(mode) and target is not None and size != len(target):
                self.found_errors = True
                self.log_error('Inode %d (%s): symlink size (%d) does not agree with target '
                               'length (%d). This is probably going to confuse your system!',
                               inode, get_path(inode, self.conn), size, len(target))                
    
            if size != 0 and (not stat.S_ISREG(mode) 
                              and not stat.S_ISLNK(mode)
                              and not stat.S_ISDIR(mode)):
                self.found_errors = True
                self.log_error('Inode %d (%s) is not regular file but has non-zero size. '
                               'This is may confuse your system!',
                               inode, get_path(inode, self.conn))
    
            if target is not None and not stat.S_ISLNK(mode):
                self.found_errors = True
                self.log_error('Inode %d (%s) is not symlink but has symlink target. '
                               'This is probably going to confuse your system!',
                               inode, get_path(inode, self.conn))
    
            if rdev != 0 and not (stat.S_ISBLK(mode) or stat.S_ISCHR(mode)):
                self.found_errors = True
                self.log_error('Inode %d (%s) is not device but has device number. '
                               'This is probably going to confuse your system!',
                               inode, get_path(inode, self.conn))
    
            has_children = self.conn.has_val('SELECT 1 FROM contents WHERE parent_inode=? LIMIT 1',
                                             (inode,))
            if has_children and not stat.S_ISDIR(mode):
                self.found_errors = True
                self.log_error('Inode %d (%s) is not a directory but has child entries. '
                               'This is probably going to confuse your system!',
                               inode, get_path(inode, self.conn))
    
            if (not stat.S_ISREG(mode) and 
                self.conn.has_val('SELECT 1 FROM inode_blocks WHERE inode=?', (inode,))):
                self.found_errors = True
                self.log_error('Inode %d (%s) is not a regular file but has data blocks. '
                               'This is probably going to confuse your system!',
                               inode, get_path(inode, self.conn))
    
    
    def check_obj_refcounts(self):
        """Check object reference counts"""
    
        log.info('Checking object reference counts...')
        
        self.conn.execute('CREATE TEMPORARY TABLE refcounts '
                          '(id INTEGER PRIMARY KEY, refcount INTEGER NOT NULL)')
        try:
            self.conn.execute('INSERT INTO refcounts (id, refcount) '
                              'SELECT obj_id, COUNT(obj_id) FROM blocks GROUP BY obj_id')
            
            self.conn.execute('''
               CREATE TEMPORARY TABLE wrong_refcounts AS 
               SELECT id, refcounts.refcount, objects.refcount 
                 FROM objects LEFT JOIN refcounts USING (id) 
                WHERE objects.refcount != refcounts.refcount 
                   OR refcounts.refcount IS NULL''')
            
            for (id_, cnt, cnt_old) in self.conn.query('SELECT * FROM wrong_refcounts'):
                if cnt is None and id_ in self.unlinked_objects and cnt_old == 0:
                    # Object was unlinked by check_block_refcounts
                    self.conn.execute('DELETE FROM objects WHERE id=?', (id_,))
                    
                else:
                    self.found_errors = True
                    self.log_error("Object %s has invalid refcount, setting from %d to %d",
                                   id_, cnt_old, cnt or 0)

                    if cnt is not None:
                        self.conn.execute("UPDATE objects SET refcount=? WHERE id=?",
                                          (cnt, id_))
                    else:
                        # Orphaned object will be picked up by check_keylist
                        self.conn.execute('DELETE FROM objects WHERE id=?', (id_,))
        finally:
            self.conn.execute('DROP TABLE refcounts')
            self.conn.execute('DROP TABLE IF EXISTS wrong_refcounts')
            
                        
    def check_keylist(self):
        """Check the list of objects.
    
        Checks that:
        - all objects are referred in the object table
        - all objects in the object table exist
        - object has correct hash
        """
    
        log.info('Checking object list...')
    
        lof_id = self.conn.get_val("SELECT inode FROM contents_v "
                                   "WHERE name=? AND parent_inode=?", (b"lost+found", ROOT_INODE))
    
        # We use this table to keep track of the objects that we have seen
        self.conn.execute("CREATE TEMP TABLE obj_ids (id INTEGER PRIMARY KEY)")
        try:
            for (i, obj_name) in enumerate(self.bucket.list('s3ql_data_')):
        
                if i != 0 and i % 5000 == 0:
                    log.info('..processed %d objects so far..', i)
        
                # We only bother with data objects
                try:
                    obj_id = int(obj_name[10:])
                except ValueError:
                    log.warn("Ignoring unexpected object %r", obj_name)
                    continue
        
                self.conn.execute('INSERT INTO obj_ids VALUES(?)', (obj_id,))
            
            for (obj_id,) in self.conn.query('SELECT id FROM obj_ids '
                                             'EXCEPT SELECT id FROM objects'):
                try:
                    if obj_id in self.unlinked_objects:
                        del self.bucket['s3ql_data_%d' % obj_id]
                    else:
                        # TODO: Save the data in lost+found instead
                        del self.bucket['s3ql_data_%d' % obj_id]
                        self.found_errors = True
                        self.log_error("Deleted spurious object %d",  obj_id)               
                except NoSuchObject:
                    pass
        
            self.conn.execute('CREATE TEMPORARY TABLE missing AS '
                              'SELECT id FROM objects EXCEPT SELECT id FROM obj_ids')
            moved_inodes = set()
            for (obj_id,) in self.conn.query('SELECT * FROM missing'):
                if (not self.bucket.is_list_create_consistent() 
                    and ('s3ql_data_%d' % obj_id) in self.bucket):
                    # Object was just not in list yet
                    continue 
                    
                self.found_errors = True
                self.log_error("object %s only exists in table but not in bucket, deleting", obj_id)
                
                for (id_,) in self.conn.query('SELECT inode FROM inode_blocks JOIN blocks ON block_id = id '
                                              'WHERE obj_id=? ', (obj_id,)):

                    # Same file may lack several blocks, but we want to move it 
                    # only once
                    if id_ in moved_inodes:
                        continue
                    moved_inodes.add(id_)
                      
                    for (name, name_id, id_p) in self.conn.query('SELECT name, name_id, parent_inode '
                                                                 'FROM contents_v WHERE inode=?', (id_,)):
                        path = get_path(id_p, self.conn, name)
                        self.log_error("File may lack data, moved to /lost+found: %s", path)
                        (_, newname) = self.resolve_free(b"/lost+found",
                                                            path[1:].replace('_', '__').replace('/', '_')) 
                            
                        self.conn.execute('UPDATE contents SET name_id=?, parent_inode=? '
                                          'WHERE name_id=? AND parent_inode=?', 
                                          (self._add_name(newname), lof_id, name_id, id_p))
                        self._del_name(name_id)
                        
                # Unlink missing blocks
                for (block_id,) in self.conn.query('SELECT id FROM blocks WHERE obj_id=?', (obj_id,)):
                    self.conn.execute('DELETE FROM inode_blocks WHERE block_id=?', (block_id,))
                    
                self.conn.execute("DELETE FROM blocks WHERE obj_id=?", (obj_id,))
                self.conn.execute("DELETE FROM objects WHERE id=?", (obj_id,))
        finally:
            self.conn.execute('DROP TABLE obj_ids')
            self.conn.execute('DROP TABLE IF EXISTS missing')
    
    
    def resolve_free(self, path, name):
        '''Return parent inode and name of an unused directory entry
        
        The directory entry will be in `path`. If an entry `name` already
        exists there, we append a numeric suffix.
        '''
    
        if not isinstance(path, bytes):
            raise TypeError('path must be of type bytes')
    
        inode_p = inode_for_path(path, self.conn)
    
        # Debugging http://code.google.com/p/s3ql/issues/detail?id=217
        # and http://code.google.com/p/s3ql/issues/detail?id=261
        if len(name) > 255-4:
            name = '%s ... %s' % (name[0:120], name[-120:])
            
        i = 0
        newname = name
        name += b'-'
        try:
            while True:
                self.conn.get_val("SELECT inode FROM contents_v "
                                  "WHERE name=? AND parent_inode=?", (newname, inode_p))
                i += 1
                newname = name + bytes(i)
    
        except NoSuchRowError:
            pass
    
        return (inode_p, newname)
