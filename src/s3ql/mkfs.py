'''
mkfs.py - this file is part of S3QL (http://s3ql.googlecode.com)

Copyright (C) 2008-2009 Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU LGPL.
'''

from __future__ import division, print_function, absolute_import

import stat
import os
import time
from . import database as dbcm

from .common import ROOT_INODE, CTRL_INODE

__all__ = [ "setup_tables", 'init_tables', 'create_indices' ]

def init_tables():
    # Insert root directory
    timestamp = time.time() - time.timezone
    dbcm.execute("INSERT INTO inodes (id,mode,uid,gid,mtime,atime,ctime,refcount) "
                   "VALUES (?,?,?,?,?,?,?,?)",
                   (ROOT_INODE, stat.S_IFDIR | stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR
                   | stat.S_IRGRP | stat.S_IXGRP | stat.S_IROTH | stat.S_IXOTH,
                    os.getuid(), os.getgid(), timestamp, timestamp, timestamp, 1))

    # Insert control inode, the actual values don't matter that much 
    dbcm.execute("INSERT INTO inodes (id,mode,uid,gid,mtime,atime,ctime,refcount) "
                 "VALUES (?,?,?,?,?,?,?,?)",
                 (CTRL_INODE, stat.S_IFIFO | stat.S_IRUSR | stat.S_IWUSR,
                  0, 0, timestamp, timestamp, timestamp, 42))

    # Insert lost+found directory
    inode = dbcm.rowid("INSERT INTO inodes (mode,uid,gid,mtime,atime,ctime,refcount) "
                       "VALUES (?,?,?,?,?,?,?)",
                       (stat.S_IFDIR | stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR,
                        os.getuid(), os.getgid(), timestamp, timestamp, timestamp, 1))
    dbcm.execute("INSERT INTO contents (name, inode, parent_inode) VALUES(?,?,?)",
                 (b"lost+found", inode, ROOT_INODE))

def setup_tables():
    # Table with filesystem metadata
    # The number of links `refcount` to an inode can in theory
    # be determined from the `contents` table. However, managing
    # this separately should be significantly faster (the information
    # is required for every getattr!)
    dbcm.execute("""
    CREATE TABLE inodes (
        -- id has to specified *exactly* as follows to become
        -- an alias for the rowid.
        id        INTEGER PRIMARY KEY,
        uid       INT NOT NULL,
        gid       INT NOT NULL,
        mode      INT NOT NULL,
        mtime     REAL NOT NULL,
        atime     REAL NOT NULL,
        ctime     REAL NOT NULL,
        refcount  INT NOT NULL,
        target    BLOB(256) ,
        size      INT NOT NULL DEFAULT 0,
        rdev      INT NOT NULL DEFAULT 0
    )
    """)

    # Table of filesystem objects
    # id is used by readdir() to restart at the correct
    # position
    dbcm.execute("""
    CREATE TABLE contents (
        rowid     INTEGER PRIMARY KEY AUTOINCREMENT,
        name      BLOB(256) NOT NULL,
        inode     INT NOT NULL REFERENCES inodes(id),
        parent_inode INT NOT NULL REFERENCES inodes(id),
        
        UNIQUE (name, parent_inode)
    )""")

    # Extended attributes
    dbcm.execute("""
    CREATE TABLE ext_attributes (
        inode     INTEGER NOT NULL REFERENCES inodes(id),
        name      BLOB NOT NULL,
        value     BLOB NOT NULL,
 
        PRIMARY KEY (inode, name)               
    )""")

    # Refcount is included for performance reasons
    dbcm.execute("""
    CREATE TABLE objects (
        id        INTEGER PRIMARY KEY AUTOINCREMENT,
        refcount  INT NOT NULL,
                  
        -- hash and size is only updated when the object is committed
        hash      BLOB(16) UNIQUE,
        size      INT NOT NULL                 
    )""")


    # Maps blocks to objects
    dbcm.execute("""
    CREATE TABLE blocks (
        inode     INTEGER NOT NULL REFERENCES inodes(id),
        blockno   INT NOT NULL,
        obj_id    INTEGER NOT NULL REFERENCES objects(id),
 
        PRIMARY KEY (inode, blockno)
    )""")
    

def create_indices():
    dbcm.execute('CREATE INDEX ix_contents_parent_inode ON contents(parent_inode)')
    dbcm.execute('CREATE INDEX ix_contents_inode ON contents(inode)')
    dbcm.execute('CREATE INDEX ix_ext_attributes_inode ON ext_attributes(inode)')
    dbcm.execute('CREATE INDEX ix_objects_hash ON objects(hash)')
    dbcm.execute('CREATE INDEX ix_blocks_obj_id ON blocks(obj_id)')
    dbcm.execute('CREATE INDEX ix_blocks_inode ON blocks(inode)')

