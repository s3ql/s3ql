'''
metadata.py - this file is part of S3QL (http://s3ql.googlecode.com)

Copyright (C) Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU GPLv3.
'''



from .deltadump import (INTEGER, BLOB, TIME, dump_table, load_table)
import logging

log = logging.getLogger('metadata')

# Has to be kept in sync with create_tables()!
DUMP_SPEC = [
             ('objects', 'id', (('id', INTEGER, 1),
                                ('size', INTEGER),
                                ('refcount', INTEGER))),

             ('blocks', 'id', (('id', INTEGER, 1),
                             ('hash', BLOB, 32),
                             ('size', INTEGER),
                             ('obj_id', INTEGER, 1),
                             ('refcount', INTEGER))),

             ('inodes', 'id', (('id', INTEGER, 1),
                               ('uid', INTEGER),
                               ('gid', INTEGER),
                               ('mode', INTEGER),
                               ('mtime', TIME),
                               ('atime', TIME),
                               ('ctime', TIME),
                               ('size', INTEGER),
                               ('rdev', INTEGER),
                               ('locked', INTEGER),
                               ('refcount', INTEGER))),

             ('inode_blocks', 'inode, blockno',
              (('inode', INTEGER),
               ('blockno', INTEGER, 1),
               ('block_id', INTEGER, 1))),

             ('symlink_targets', 'inode', (('inode', INTEGER, 1),
                                           ('target', BLOB))),

             ('names', 'id', (('id', INTEGER, 1),
                              ('name', BLOB),
                              ('refcount', INTEGER))),

             ('contents', 'parent_inode, name_id',
              (('name_id', INTEGER, 1),
               ('inode', INTEGER, 1),
               ('parent_inode', INTEGER))),

             ('ext_attributes', 'inode', (('inode', INTEGER),
                                          ('name_id', INTEGER),
                                          ('value', BLOB))),
]



def restore_metadata(fh, db):
    '''Read metadata from *fh* and write into *db*
    
    *fh* must be able to return an actual file descriptor from
    its `fileno` method.
    '''

    create_tables(db)
    for (table, _, columns) in DUMP_SPEC:
        log.info('..%s..', table)
        load_table(table, columns, db=db, fh=fh)
    db.execute('ANALYZE')

def cycle_metadata(backend):
    from .backends.common import NoSuchObject

    log.info('Backing up old metadata...')
    for i in reversed(list(range(10))):
        try:
            backend.copy("s3ql_metadata_bak_%d" % i, "s3ql_metadata_bak_%d" % (i + 1))
        except NoSuchObject:
            pass

    backend.copy("s3ql_metadata", "s3ql_metadata_bak_0")

def dump_metadata(db, fh):
    '''Dump metadata into fh
    
    *fh* must be able to return an actual file descriptor from
    its `fileno` method.
    '''

    for (table, order, columns) in DUMP_SPEC:
        log.info('..%s..', table)
        dump_table(table, order, columns, db=db, fh=fh)

def create_tables(conn):
    # Table of storage objects
    # Refcount is included for performance reasons
    conn.execute("""
    CREATE TABLE objects (
        id        INTEGER PRIMARY KEY AUTOINCREMENT,
        refcount  INT NOT NULL, 
        size      INT NOT NULL
    )""")

    # Table of known data blocks
    # Refcount is included for performance reasons
    conn.execute("""
    CREATE TABLE blocks (
        id        INTEGER PRIMARY KEY,
        hash      BLOB(16) UNIQUE,
        refcount  INT,
        size      INT NOT NULL,    
        obj_id    INTEGER NOT NULL REFERENCES objects(id)
    )""")

    # Table with filesystem metadata
    # The number of links `refcount` to an inode can in theory
    # be determined from the `contents` table. However, managing
    # this separately should be significantly faster (the information
    # is required for every getattr!)
    conn.execute("""
    CREATE TABLE inodes (
        -- id has to specified *exactly* as follows to become
        -- an alias for the rowid.
        id        INTEGER PRIMARY KEY AUTOINCREMENT,
        uid       INT NOT NULL,
        gid       INT NOT NULL,
        mode      INT NOT NULL,
        mtime     REAL NOT NULL,
        atime     REAL NOT NULL,
        ctime     REAL NOT NULL,
        refcount  INT NOT NULL,
        size      INT NOT NULL DEFAULT 0,
        rdev      INT NOT NULL DEFAULT 0,
        locked    BOOLEAN NOT NULL DEFAULT 0
    )""")

    # Further Blocks used by inode (blockno >= 1)
    conn.execute("""
    CREATE TABLE inode_blocks (
        inode     INTEGER NOT NULL REFERENCES inodes(id),
        blockno   INT NOT NULL,
        block_id    INTEGER NOT NULL REFERENCES blocks(id),
        PRIMARY KEY (inode, blockno)
    )""")

    # Symlinks
    conn.execute("""
    CREATE TABLE symlink_targets (
        inode     INTEGER PRIMARY KEY REFERENCES inodes(id),
        target    BLOB NOT NULL
    )""")

    # Names of file system objects
    conn.execute("""
    CREATE TABLE names (
        id     INTEGER PRIMARY KEY,
        name   BLOB NOT NULL,
        refcount  INT NOT NULL,
        UNIQUE (name)
    )""")

    # Table of filesystem objects
    # rowid is used by readdir() to restart at the correct position
    conn.execute("""
    CREATE TABLE contents (
        rowid     INTEGER PRIMARY KEY AUTOINCREMENT,
        name_id   INT NOT NULL REFERENCES names(id),
        inode     INT NOT NULL REFERENCES inodes(id),
        parent_inode INT NOT NULL REFERENCES inodes(id),
        
        UNIQUE (parent_inode, name_id)
    )""")

    # Extended attributes
    conn.execute("""
    CREATE TABLE ext_attributes (
        inode     INTEGER NOT NULL REFERENCES inodes(id),
        name_id   INTEGER NOT NULL REFERENCES names(id),
        value     BLOB NOT NULL,
 
        PRIMARY KEY (inode, name_id)               
    )""")

    # Shortcuts
    conn.execute("""
    CREATE VIEW contents_v AS
    SELECT * FROM contents JOIN names ON names.id = name_id       
    """)
    conn.execute("""
    CREATE VIEW ext_attributes_v AS
    SELECT * FROM ext_attributes JOIN names ON names.id = name_id
    """)
