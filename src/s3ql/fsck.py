'''
fsck.py - this file is part of S3QL (http://s3ql.googlecode.com)

Copyright (C) 2008-2009 Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU GPLv3.
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

__all__ = [ "Fsck" ]

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
        
        # List of blocks that fsck orphaned, so they are expected to
        # have refcount==0
        self.orphaned_blocks = [] 
    
    def check(self):
        """Check file system
        
        Sets instance variable `found_errors`.
        """
        
        # Create indices required for reference checking
        log.info('Creating temporary extra indices...')
        self.conn.execute('CREATE INDEX tmp1 ON blocks(obj_id)')
        self.conn.execute('CREATE INDEX tmp2 ON inode_blocks(block_id)')
        self.conn.execute('CREATE INDEX tmp3 ON inodes(block_id)')
        self.conn.execute('CREATE INDEX tmp4 ON contents(inode)')
        try:
            self.check_foreign_keys()
            self.check_cache()
            self.check_lof()
            self.check_name_refcount()
            self.check_contents()
            self.check_loops()
            self.check_inode_sizes()
            self.check_inode_unix()
            self.check_inode_refcount()
            self.check_block_refcount()
            self.check_obj_refcounts()
            self.check_keylist()
        finally:
            log.info('Dropping temporary indices...')
            for idx in ('tmp1', 'tmp2', 'tmp3', 'tmp4'):
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
        
        for (table,) in self.conn.query("SELECT name FROM sqlite_master WHERE type='table'"):
            for row in self.conn.query('PRAGMA foreign_key_list(%s)' % table):
                sql_objs = { 'src_table': table,
                            'dst_table': row[2],
                            'src_col': row[3],
                            'dst_col': row[4] }
                for (val,) in self.conn.query('SELECT %(src_table)s.%(src_col)s '
                                              'FROM %(src_table)s LEFT JOIN %(dst_table)s '
                                              'ON %(src_table)s.%(src_col)s = %(dst_table)s.%(dst_col)s '
                                              'WHERE %(dst_table)s.%(dst_col)s IS NULL '
                                              'AND %(src_table)s.%(src_col)s IS NOT NULL'
                                              % sql_objs):
                    self.found_errors = True
                    self.uncorrectable_errors = True
                    sql_objs['val'] = val
                    self.log_error('%(src_table)s.%(src_col)s refers to non-existing key %(val)s '
                                   'in %(dst_table)s.%(dst_col)s!', sql_objs)
                    self.log_error("Don't know how to fix this problem!")
        
        
    def check_cache(self):
        """Commit uncommitted cache files"""
    
        log.info("Checking cached objects...")
        if not os.path.exists(self.cachedir):
            return
        
        for filename in os.listdir(self.cachedir):
            self.found_errors = True
    
            match = re.match('^inode_(\\d+)_block_(\\d+)(\\.d)?$', filename)
            if match:
                inode = int(match.group(1))
                blockno = int(match.group(2))
                dirty = match.group(3) == '.d'
            else:
                raise RuntimeError('Strange file in cache directory: %s' % filename)
            
            if not dirty:
                self.log_error('Removing cached block %d of inode %d', blockno, inode)
                os.unlink(os.path.join(self.cachedir, filename))
                continue
    
            self.log_error("Committing changed block %d of inode %d to backend",
                      blockno, inode)
    
            fh = open(os.path.join(self.cachedir, filename), "rb")
            fh.seek(0, 2)
            size = fh.tell()
            fh.seek(0)
            hash_ = sha256_fh(fh)
    
            try:
                (block_id, obj_id) = self.conn.get_val('SELECT id, obj_id FROM blocks WHERE hash=?', (hash_,))
                
            except NoSuchRowError:
                obj_id = self.conn.rowid('INSERT INTO objects (refcount) VALUES(1)')
                block_id = self.conn.rowid('INSERT INTO blocks (refcount, hash, obj_id, size) '
                                           'VALUES(?, ?, ?, ?)', (1, hash_, obj_id, size))
                self.conn.execute('UPDATE objects SET compr_size=? WHERE id=?', 
                                  (self.bucket.store_fh('s3ql_data_%d' % obj_id, fh), obj_id))
    
            else:
                self.conn.execute('UPDATE blocks SET refcount=refcount+1 WHERE id=?', (block_id,))                         
    
            try:
                old_block_id = self.conn.get_val('SELECT block_id FROM inode_blocks_v '
                                                 'WHERE inode=? AND blockno=?', (inode, blockno))
            except NoSuchRowError:
                if blockno == 0:
                    self.conn.execute('UPDATE inodes SET block_id=? WHERE id=?',
                                      (block_id, inode))
                else:
                    self.conn.execute('INSERT INTO inode_blocks (block_id, inode, blockno) VALUES(?,?,?)',
                                      (block_id, inode, blockno))
            else:
                if blockno == 0:
                    self.conn.execute('UPDATE inodes SET block_id=? WHERE id=?',
                                      (block_id, inode))                    
                else:
                    self.conn.execute('UPDATE inode_blocks SET block_id=? WHERE inode=? AND blockno=?',
                                      (block_id, inode, blockno))
    
                # We just decrease the refcount, but don't take any action
                # because the reference count might be wrong 
                self.conn.execute('UPDATE blocks SET refcount=refcount-1 WHERE id=?',
                                  (old_block_id,))
                self.orphaned_blocks.append(old_block_id)
                
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
            inode_l = self.conn.rowid("INSERT INTO inodes (mode,uid,gid,mtime,atime,ctime,refcount) "
                                      "VALUES (?,?,?,?,?,?,?)",
                                      (stat.S_IFDIR | stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR,
                                       os.getuid(), os.getgid(), timestamp, timestamp, timestamp, 1))
            self.conn.execute("INSERT INTO contents (name_id, inode, parent_inode) VALUES(?,?,?)",
                              (self._add_name(b"lost+found"), inode_l, ROOT_INODE))
    
    
        mode = self.conn.get_val('SELECT mode FROM inodes WHERE id=?', (inode_l,))
        if not stat.S_ISDIR(mode):
            self.found_errors = True
            self.log_error('/lost+found is not a directory! Old entry will be saved as '
                           '/lost+found/inode-%s*', inode_l)
            # We leave the old inode unassociated, so that it will be added
            # to lost+found later on.
            inode_l = self.conn.rowid("INSERT INTO inodes (mode,uid,gid,mtime,atime,ctime,refcount) "
                                      "VALUES (?,?,?,?,?,?,?)",
                                      (stat.S_IFDIR | stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR,
                                       os.getuid(), os.getgid(), timestamp, timestamp, timestamp, 2))
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
            FROM inode_blocks_v JOIN blocks ON block_id == blocks.id 
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
        self.conn.execute('DELETE FROM names WHERE refcount=0 AND name_id=?', (name_id,))
                            
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
    
        # TODO: Now check for refcount<=0, delete such entries and report as error
        # Store detached block_ids in self.orphaned_blocks[id] += 1
        
    def check_block_refcount(self):
        """Check block reference counters"""
    
        log.info('Checking blocks (refcounts)...')
    
        self.conn.execute('CREATE TEMPORARY TABLE refcounts '
                          '(id INTEGER PRIMARY KEY, refcount INTEGER NOT NULL)')
        try:
            self.conn.execute('''
               INSERT INTO refcounts (id, refcount) 
                 SELECT block_id, COUNT(blockno)  
                 FROM inode_blocks_v
                 GROUP BY block_id
            ''')
            
            self.conn.execute('''
               CREATE TEMPORARY TABLE wrong_refcounts AS 
               SELECT id, refcounts.refcount, blocks.refcount 
                 FROM blocks LEFT JOIN refcounts USING (id) 
                WHERE blocks.refcount != refcounts.refcount 
                   OR refcounts.refcount IS NULL''')
            
            for (id_, cnt, cnt_old) in self.conn.query('SELECT * FROM wrong_refcounts'):                
                self.found_errors = True
                if cnt is None:
                    (id_p, name) = self.resolve_free(b"/lost+found", b"block-%d" % id_)
                    self.log_error("Block %d not referenced, adding as /lost+found/%s", id_, name)
                    timestamp = time.time() - time.timezone
                    inode = self.conn.rowid("""
                        INSERT INTO inodes (mode,uid,gid,mtime,atime,ctime,refcount,block_id) 
                               VALUES (?,?,?,?,?,?,?,?)""",
                        (stat.S_IFREG | stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR,
                        os.getuid(), os.getgid(), timestamp, timestamp, timestamp, 1, id_))
                    self.conn.execute("INSERT INTO contents (name_id, inode, parent_inode) VALUES (?,?,?)", 
                                      (self._add_name(basename(name)), inode, id_p))
                    self.conn.execute("UPDATE blocks SET refcount=? WHERE id=?", (1, id_))

                else: 
                    self.log_error("Block %d has wrong reference count, setting from %d to %d",
                                   id_, cnt_old, cnt)
                    self.conn.execute("UPDATE blocks SET refcount=? WHERE id=?", (cnt, id_))
        finally:
            self.conn.execute('DROP TABLE refcounts')
            self.conn.execute('DROP TABLE IF EXISTS wrong_refcounts')    
    
        # TODO: Now check for refcount<=0, delete such entries and report as error
        # unless refcount ==  1- self.orphaned_blocks[id]
        # Store detached obj_ids in self.orphaned_objs[id] += 1
        
    def check_name_refcount(self):
        """Check name reference counters"""
    
        log.info('Checking names (refcounts)...')
    
        self.conn.execute('CREATE TEMPORARY TABLE refcounts '
                          '(id INTEGER PRIMARY KEY, refcount INTEGER NOT NULL)')
        try:
            self.conn.execute('''
               INSERT INTO refcounts (id, refcount) 
                 SELECT name_id, COUNT(name_id) FROM contents GROUP BY name_id''')
            
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
                
        # TODO: Now check for refcount<=0, delete such entries and report as error
                
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
                self.conn.has_val('SELECT 1 FROM inode_blocks_v WHERE inode=?', (inode,))):
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
                self.log_error("Object %s has invalid refcount, setting from %d to %d",
                               id_, cnt_old, cnt or 0)
                self.found_errors = True
                if cnt is not None:
                    self.conn.execute("UPDATE objects SET refcount=? WHERE id=?",
                                      (cnt, id_))
                else:
                    # Orphaned object will be picked up by check_keylist
                    self.conn.execute('DELETE FROM objects WHERE id=?', (id_,))
        finally:
            self.conn.execute('DROP TABLE refcounts')
            self.conn.execute('DROP TABLE IF EXISTS wrong_refcounts')
    
        # TODO: Now check for refcount<=0, delete such entries and report as error
        # unless refcount ==  1- self.orphaned_objs[id]
            
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
                obj_id = int(obj_name[10:])
        
                self.conn.execute('INSERT INTO obj_ids VALUES(?)', (obj_id,))
            
            for (obj_id,) in self.conn.query('SELECT id FROM obj_ids '
                                             'EXCEPT SELECT id FROM objects'):
                try:
                    del self.bucket['s3ql_data_%d' % obj_id]
                    self.found_errors = True
                    self.log_error("Deleted spurious object %d",  obj_id)                    
                except NoSuchObject:
                    if self.bucket.read_after_delete_consistent():
                        raise
        
            self.conn.execute('CREATE TEMPORARY TABLE missing AS '
                              'SELECT id FROM objects EXCEPT SELECT id FROM obj_ids')
            moved_inodes = set()
            for (obj_id,) in self.conn.query('SELECT * FROM missing'):
                self.found_errors = True
                self.log_error("object %s only exists in table but not in bucket, deleting", obj_id)
                
                for (id_,) in self.conn.query('SELECT inode FROM inode_blocks_v JOIN blocks ON block_id = id '
                                              'WHERE obj_id=?', (obj_id,)):

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
