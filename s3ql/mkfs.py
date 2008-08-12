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


def setup_db(dbfile, blocksize, label=None):
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
        last_fsck   INT NOT NULL,
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
        name      BLOB(256) NOT NULL PRIMARY KEY,
        inode     INT NOT NULL REFERENCES inodes(id),
        parent_inode INT NOT NULL REFERENCES inodes(id)
    );
    CREATE INDEX ix_contents_parent_inode ON contents(parent_inode);
    """)

    # Table with filesystem metadata
    # The number of links `refcount` to an inode can in theory
    # be determined from the `contents` table. However, there is no efficient
    # statement to do so (because we have to count `..` references of all
    # subdirectories), so we manage this separately.
    cursor.execute("""
    CREATE TABLE inodes (
        -- id has to specified *exactly* as follows to become
        -- an alias for the rowid
        id        INTEGER PRIMARY KEY,
        uid       INT NOT NULL,
        gid       INT NOT NULL,
        mode      INT NOT NULL,
        mtime     INT NOT NULL,
        atime     INT NOT NULL,
        ctime     INT NOT NULL,
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
    cursor.execute("""
    CREATE TABLE s3_objects (
        inode     INTEGER NOT NULL REFERENCES inodes(id),
        offset    INT NOT NULL CHECK (offset >= 0),
        s3key     TEXT NOT NULL UNIQUE,
        etag      TEXT,
        size      INT NOT NULL,

        -- for caching
        fd        INTEGER,
        atime     INTEGER NOT NULL,
        dirty     BOOLEAN,
        cachefile TEXT UNIQUE
                  CHECK ((fd IS NULL AND cachefile IS NULL)
                         OR (fd IS NOT NULL AND cachefile IS NOT NULL)),

        PRIMARY KEY (inode, offset)
    );
    CREATE INDEX ix_s3 ON s3_objects(s3key);
    CREATE INDEX ix_dirty ON s3_objects(dirty);
    CREATE INDEX ix_atime ON s3_objects(atime);
    """);
    cursor.execute(trigger_cmd.substitute({ "src_table": "s3_objects",
                                            "src_key": "inode",
                                            "ref_table": "inodes",
                                            "ref_key": "id" }))


    # Create a view of the whole fs with all information
    cursor.execute("""
    CREATE VIEW contents_ext AS
        SELECT * FROM contents JOIN inodes ON id == inode;
    """)


    # Insert root directory
    # Refcount = 3: parent, ".", lost+found
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
                   (buffer("/lost+found"), inode, root_inode))

    # Done setting up metadata table
    conn.close()


def setup_bucket(bucket, dbfile):
    """Creates a bucket and uploads metadata.
    """
    bucket.store_from_file(key='metadata', file=dbfile)
    bucket.store(key='dirty', val="no")
