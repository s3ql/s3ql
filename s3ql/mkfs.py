#!/usr/bin/env python
#
#    Copyright (C) 2008  Nikolaus Rath <Nikolaus@rath.org>
#
#    This program can be distributed under the terms of the GNU LGPL.
#

import stat
import apsw
import os
from time import time
from string import Template

__all__ = [ "setup_bucket", "setup_db" ]

def setup_db(dbfile, blocksize, label="unnamed s3qlfs"):
    """Creates the metadata tables
    """

    conn=apsw.Connection(dbfile)
    cursor=conn.cursor()

    # This command creates triggers that ensure referential integrity
    trigger_cmd = Template("""
    CREATE TRIGGER fki_${src_table}_${src_key}
      BEFORE INSERT ON ${src_table}
      FOR EACH ROW BEGIN
          SELECT RAISE(ABORT, 'insert in column $src_key of table $src_table violates foreign key constraint')
          WHERE NEW.$src_key IS NOT NULL AND
                (SELECT $ref_key FROM $ref_table WHERE $ref_key = NEW.$src_key) IS NULL;
      END;

    CREATE TRIGGER fku_${src_table}_${src_key}
      BEFORE UPDATE ON ${src_table}
      FOR EACH ROW BEGIN
          SELECT RAISE(ABORT, 'update in column $src_key of table $src_table violates foreign key constraint')
          WHERE NEW.$src_key IS NOT NULL AND
                (SELECT $ref_key FROM $ref_table WHERE $ref_key = NEW.$src_key) IS NULL;
      END;


    CREATE TRIGGER fkd_${src_table}_${src_key}
      BEFORE DELETE ON $ref_table
      FOR EACH ROW BEGIN
          SELECT RAISE(ABORT, 'delete on table $ref_table violates foreign key constraint of column $src_key in table ${src_table}')
          WHERE (SELECT $src_key FROM $src_table WHERE $src_key = OLD.$ref_key) IS NOT NULL;
      END;
    """)


    # Filesystem parameters
    cursor.execute("""
    CREATE TABLE parameters (
        label       TEXT,
        blocksize   INT NOT NULL,
        last_fsck   REAL NOT NULL,
        mountcnt    INT NOT NULL,
        version     INT NOT NULL,
        needs_fsck  BOOLEAN NOT NULL
    );
    INSERT INTO parameters(label,blocksize,last_fsck,mountcnt,needs_fsck,version)
        VALUES(?,?,?,?,?, ?)
    """, (label, blocksize, time(), 0, False, 1))

    # Table of filesystem objects
    cursor.execute("""
    CREATE TABLE contents (
        name      BLOB(256) NOT NULL,
        inode     INT NOT NULL REFERENCES inodes(id),
        parent_inode INT NOT NULL REFERENCES inodes(id),
        
        PRIMARY KEY (name, parent_inode)
    );
    CREATE INDEX ix_contents_parent_inode ON contents(parent_inode);
    CREATE INDEX ix_contents_inode ON contents(inode);
    """)

    # Table with filesystem metadata
    # The number of links `refcount` to an inode can in theory
    # be determined from the `contents` table. However, managing
    # this separately should be significantly faster (the information
    # is required for every getattr!)
    cursor.execute("""
    CREATE TABLE inodes (
        -- id has to specified *exactly* as follows to become
        -- an alias for the rowid
        id        INTEGER PRIMARY KEY,
        uid       INT NOT NULL,
        gid       INT NOT NULL,
        mode      INT NOT NULL,
        mtime     REAL NOT NULL,
        atime     REAL NOT NULL,
        ctime     REAL NOT NULL,
        refcount  INT NOT NULL,

        -- for symlinks only
        target    BLOB,

        -- for files only
        size      INT CHECK (size >= 0),

        -- device nodes
        rdev      INT
    );
    """)
    cursor.execute(trigger_cmd.substitute({ "src_table": "contents",
                                            "src_key": "inode",
                                            "ref_table": "inodes",
                                            "ref_key": "id" }))
    cursor.execute(trigger_cmd.substitute({ "src_table": "contents",
                                            "src_key": "parent_inode",
                                            "ref_table": "inodes",
                                            "ref_key": "id" }))
    
    # Maps file data chunks to S3 objects
    # Refcount is included for performance reasons
    cursor.execute("""
    CREATE TABLE s3_objects (
        id        TEXT PRIMARY KEY,
        last_modified REAL,
        etag      TEXT,
        refcount  INT NOT NULL
    );
    """);
    
    # Maps file data chunks to S3 objects
    cursor.execute("""
    CREATE TABLE inode_s3key (
        inode     INTEGER NOT NULL REFERENCES inodes(id),
        offset    INT NOT NULL CHECK (offset >= 0),
        s3key     INTEGER NOT NULL REFERENCES s3_objects(id),
 
        PRIMARY KEY (inode, offset)
    );
    """);
    cursor.execute(trigger_cmd.substitute({ "src_table": "inode_s3key",
                                            "src_key": "inode",
                                            "ref_table": "inodes",
                                            "ref_key": "id" }))
    cursor.execute(trigger_cmd.substitute({ "src_table": "inode_s3key",
                                            "src_key": "s3key",
                                            "ref_table": "s3_objects",
                                            "ref_key": "id" }))




    # Insert root directory
    # Refcount = 3: parent (when mounted), ".", lost+found
    cursor.execute("INSERT INTO inodes (mode,uid,gid,mtime,atime,ctime,refcount) "
                   "VALUES (?,?,?,?,?,?,?)",
                   (stat.S_IFDIR | stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR
                   | stat.S_IRGRP | stat.S_IXGRP | stat.S_IROTH | stat.S_IXOTH,
                    os.getuid(), os.getgid(), time(), time(), time(), 3))
    root_inode = conn.last_insert_rowid()
    cursor.execute("INSERT INTO contents (name, inode, parent_inode) VALUES(?,?,?)",
                   (buffer("/"), root_inode, root_inode))

    # Insert lost+found directory
    # refcount = 2: parent, "."
    cursor.execute("INSERT INTO inodes (mode,uid,gid,mtime,atime,ctime,refcount) "
                   "VALUES (?,?,?,?,?,?,?)",
                   (stat.S_IFDIR | stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR,
                    os.getuid(), os.getgid(), time(), time(), time(), 2))
    inode = conn.last_insert_rowid()
    cursor.execute("INSERT INTO contents (name, inode, parent_inode) VALUES(?,?,?)",
                   (buffer("lost+found"), inode, root_inode))

    # Done setting up metadata table
    conn.close()


def setup_bucket(bucket, dbfile):
    """Creates a bucket and uploads metadata.
    """
    bucket.store_from_file(key='s3ql_metadata', file=dbfile)
    bucket.store(key='s3ql_dirty', val="no")
