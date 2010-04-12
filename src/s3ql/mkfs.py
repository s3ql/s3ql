'''
$Id: mkfs.py 798 2010-02-13 22:51:37Z nikratio $

Copyright (C) 2008-2009 Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU LGPL.
'''

from __future__ import division, print_function

import stat
import os
import time

from s3ql.common import ROOT_INODE, CTRL_INODE

__all__ = [ "setup_db" ]

def setup_db(conn, blocksize, label=u"unnamed s3qlfs"):
    """Creates the metadata tables
    """

    # Create a list of valid inode types
    types = {"S_IFDIR": stat.S_IFDIR, "S_IFREG": stat.S_IFREG,
             "S_IFSOCK": stat.S_IFSOCK, "S_IFBLK": stat.S_IFBLK,
             "S_IFCHR": stat.S_IFCHR, "S_IFIFO": stat.S_IFIFO,
             "S_IFLNK": stat.S_IFLNK }
    # Note that we cannot just use sum(types.itervalues()), because 
    # e.g. S_IFDIR | S_IFREG == S_IFSOCK  
    ifmt = 0
    for i in types.itervalues():
        ifmt |= i
    types["S_IFMT"] = ifmt

    # Filesystem parameters
    conn.execute("""
    CREATE TABLE parameters (
        label       TEXT NOT NULL
                    CHECK (typeof(label) == 'text'),
        blocksize   INT NOT NULL
                    CHECK (typeof(blocksize) == 'integer'),
        last_fsck   REAL NOT NULL
                    CHECK (typeof(last_fsck) == 'real'),
        mountcnt    INT NOT NULL
                    CHECK (typeof(mountcnt) == 'integer'),
        needs_fsck  BOOLEAN NOT NULL
                    CHECK (typeof(needs_fsck) == 'integer')
    );
    INSERT INTO parameters(label,blocksize,last_fsck,mountcnt,needs_fsck)
        VALUES(?,?,?,?,?)
    """, (unicode(label), blocksize, time.time() - time.timezone, 0, False))

    # Table with filesystem metadata
    # The number of links `refcount` to an inode can in theory
    # be determined from the `contents` table. However, managing
    # this separately should be significantly faster (the information
    # is required for every getattr!)
    conn.execute("""
    CREATE TABLE inodes (
        -- id has to specified *exactly* as follows to become
        -- an alias for the rowid.
        -- inode_t may be restricted to 32 bits, so we need to constrain the
        -- rowid. Also, as long as we don't store a separate generation no,
        -- we can't reuse old rowids. Therefore we will run out of inodes after
        -- 49 days if we insert 1000 rows per second. 
        id        INTEGER PRIMARY KEY 
                  CHECK (id < 4294967296),
        uid       INT NOT NULL 
                  CHECK (typeof(uid) == 'integer'),
        gid       INT NOT NULL 
                  CHECK (typeof(gid) == 'integer'),
        mode      INT NOT NULL 
                  CHECK (typeof(mode) == 'integer'),
        mtime     REAL NOT NULL 
                  CHECK (typeof(mtime) == 'real' AND mtime >= 0),
        atime     REAL NOT NULL 
                  CHECK (typeof(atime) == 'real' AND atime >= 0),
        ctime     REAL NOT NULL 
                  CHECK (typeof(ctime) == 'real' AND ctime >= 0),
        refcount  INT NOT NULL
                  CHECK (typeof(refcount) == 'integer' AND refcount > 0),
        target    BLOB(256) 
                  CHECK (typeof(target) IN ('blob', 'null')),
        size      INT NOT NULL DEFAULT 0
                  CHECK (typeof(size) == 'integer' AND size >= 0),
        rdev      INT NOT NULL DEFAULT 0
                  CHECK (typeof(rdev) == 'integer'),
                                    
        -- Correction term to add to refcount to get st_nlink
        nlink_off INT NOT NULL DEFAULT 0
                  CHECK (typeof(nlink_off) == 'integer')
    )
    """.format(**types))

    # Table of filesystem objects
    conn.execute("""
    CREATE TABLE contents (
        name      BLOB(256) NOT NULL
                  CHECK (typeof(name) == 'blob'),
        inode     INT NOT NULL REFERENCES inodes(id),
        parent_inode INT NOT NULL REFERENCES inodes(id),
        
        PRIMARY KEY (name, parent_inode)
    );
    CREATE INDEX ix_contents_parent_inode ON contents(parent_inode);
    CREATE INDEX ix_contents_inode ON contents(inode);
    """)

    # Extended attributes
    conn.execute("""
    CREATE TABLE ext_attributes (
        inode     INTEGER NOT NULL REFERENCES inodes(id),
        name      BLOB NOT NULL
                  CHECK (typeof(name) == 'blob'),
        value     BLOB NOT NULL
                  CHECK (typeof(value) == 'blob'),
 
        PRIMARY KEY (inode, name)               
    );
    CREATE INDEX ix_ext_attributes_inode ON ext_attributes(inode);
    """)

    # Maps file data chunks to S3 objects
    # Refcount is included for performance reasons, for directories, the
    # refcount also includes the implicit '.' entry
    conn.execute("""
    CREATE TABLE s3_objects (
        id        INTEGER PRIMARY KEY AUTOINCREMENT,
        refcount  INT NOT NULL
                  CHECK (typeof(refcount) == 'integer' AND refcount > 0),
                  
        -- hash and size is only updated when the object is committed
        hash      BLOB(16) UNIQUE
                  CHECK (typeof(hash) IN ('blob', 'null')),
        size      INT NOT NULL
                  CHECK (typeof(size) == 'integer' AND size >= 0)                  
    );
    CREATE INDEX ix_s3_objects_hash ON s3_objects(hash);
    """)

    # Maps file data chunks to S3 objects
    conn.execute("""
    CREATE TABLE blocks (
        inode     INTEGER NOT NULL REFERENCES inodes(id),
        blockno   INT NOT NULL
                  CHECK (typeof(blockno) == 'integer' AND blockno >= 0),
        s3key     INTEGER NOT NULL REFERENCES s3_objects(id),
 
        PRIMARY KEY (inode, blockno)
    );
    CREATE INDEX ix_blocks_s3key ON blocks(s3key);
    CREATE INDEX ix_blocks_inode ON blocks(inode);
    """)

    # Insert root directory
    timestamp = time.time() - time.timezone
    conn.execute("INSERT INTO inodes (id,mode,uid,gid,mtime,atime,ctime,refcount,nlink_off) "
                   "VALUES (?,?,?,?,?,?,?,?,?)",
                   (ROOT_INODE, stat.S_IFDIR | stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR
                   | stat.S_IRGRP | stat.S_IXGRP | stat.S_IROTH | stat.S_IXOTH,
                    os.getuid(), os.getgid(), timestamp, timestamp, timestamp, 1, 2))

    # Insert control inode, the actual values don't matter that much 
    conn.execute("INSERT INTO inodes (id,mode,uid,gid,mtime,atime,ctime,refcount) "
                 "VALUES (?,?,?,?,?,?,?,?)",
                 (CTRL_INODE, stat.S_IFIFO | stat.S_IRUSR | stat.S_IWUSR,
                  0, 0, timestamp, timestamp, timestamp, 42))

    # Insert lost+found directory
    inode = conn.rowid("INSERT INTO inodes (mode,uid,gid,mtime,atime,ctime,refcount,nlink_off) "
                       "VALUES (?,?,?,?,?,?,?,?)",
                       (stat.S_IFDIR | stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR,
                        os.getuid(), os.getgid(), timestamp, timestamp, timestamp, 1, 1))
    conn.execute("INSERT INTO contents (name, inode, parent_inode) VALUES(?,?,?)",
                 (b"lost+found", inode, ROOT_INODE))


