#
#    Copyright (C) 2008  Nikolaus Rath <Nikolaus@rath.org>
#
#    This program can be distributed under the terms of the GNU LGPL.
#

from __future__ import unicode_literals
import stat
import os
import time

from s3ql.common import ROOT_INODE

__all__ = [ "setup_db" ]

def setup_db(conn, blocksize, label="unnamed s3qlfs"):
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
        
    # This command creates triggers that ensure referential integrity
    trigger_cmd = """
    CREATE TRIGGER fki_{src_table}_{src_key}
      BEFORE INSERT ON {src_table}
      FOR EACH ROW BEGIN
          SELECT RAISE(ABORT, 'insert in column {src_key} of table {src_table} violates foreign key constraint')
          WHERE NEW.{src_key} IS NOT NULL AND
                (SELECT {ref_key} FROM {ref_table} WHERE {ref_key} = NEW.{src_key}) IS NULL;
      END;

    CREATE TRIGGER fku_{src_table}_{src_key}
      BEFORE UPDATE ON {src_table}
      FOR EACH ROW BEGIN
          SELECT RAISE(ABORT, 'update in column {src_key} of table {src_table} violates foreign key constraint')
          WHERE NEW.{src_key} IS NOT NULL AND
                (SELECT {ref_key} FROM {ref_table} WHERE {ref_key} = NEW.{src_key}) IS NULL;
      END;


    CREATE TRIGGER fkd_{src_table}_{src_key}
      BEFORE DELETE ON {ref_table}
      FOR EACH ROW BEGIN
          SELECT RAISE(ABORT, 'delete on table {ref_table} violates foreign key constraint of column {src_key} in table {src_table}')
          WHERE (SELECT {src_key} FROM {src_table} WHERE {src_key} = OLD.{ref_key}) IS NOT NULL;
      END;
    """

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
        version     REAL NOT NULL
                    CHECK (typeof(version) == 'real'),
        needs_fsck  BOOLEAN NOT NULL
                    CHECK (typeof(needs_fsck) == 'integer')
    );
    INSERT INTO parameters(label,blocksize,last_fsck,mountcnt,needs_fsck,version)
        VALUES(?,?,?,?,?, ?)
    """, (label, blocksize, time.time() - time.timezone, 0, False, 1.0))

    # Table of filesystem objects
    conn.execute("""
    CREATE TABLE contents (
        name      BLOB(256) NOT NULL
                  CHECK( typeof(name) == 'blob' AND name NOT LIKE '%/%' ),
        inode     INT NOT NULL REFERENCES inodes(id),
        parent_inode INT NOT NULL REFERENCES inodes(id),
        
        PRIMARY KEY (name, parent_inode)
    );
    CREATE INDEX ix_contents_parent_inode ON contents(parent_inode);
    CREATE INDEX ix_contents_inode ON contents(inode);
    """)
    
    # Make sure that parent inodes are directories
    conn.execute("""
    CREATE TRIGGER contents_check_parent_inode_insert
      BEFORE INSERT ON contents
      FOR EACH ROW BEGIN
          SELECT RAISE(ABORT, 'parent_inode does not refer to a directory')
          WHERE NEW.parent_inode != 0 AND
               (SELECT mode FROM inodes WHERE id = NEW.parent_inode) & {S_IFMT} != {S_IFDIR};
      END;

    CREATE TRIGGER contents_check_parent_inode_update
      BEFORE UPDATE ON contents
      FOR EACH ROW BEGIN
          SELECT RAISE(ABORT, 'parent_inode does not refer to a directory')
          WHERE NEW.parent_inode != 0 AND
               (SELECT mode FROM inodes WHERE id = NEW.parent_inode) & {S_IFMT} != {S_IFDIR};
      END;  
    """.format(**types)) 
    

    # Table with filesystem metadata
    # The number of links `refcount` to an inode can in theory
    # be determined from the `contents` table. However, managing
    # this separately should be significantly faster (the information
    # is required for every getattr!)
    conn.execute("""
    CREATE TABLE inodes (
        -- id has to specified *exactly* as follows to become
        -- an alias for the rowid
        id        INTEGER PRIMARY KEY,
        uid       INT NOT NULL CHECK( typeof(uid) == 'integer' AND uid >= 0),
        gid       INT NOT NULL CHECK( typeof(gid) == 'integer' AND gid >= 0),
        -- make sure that an entry has only one type. Note that 
        -- S_IFDIR | S_IFREG == S_IFSOCK
        mode      INT NOT NULL CONSTRAINT mode_type
                  CHECK ( typeof(mode) == 'integer' AND
                          (mode & {S_IFMT}) IN ({S_IFDIR}, {S_IFREG}, {S_IFLNK},
                                                {S_IFBLK}, {S_IFCHR}, {S_IFIFO}, {S_IFSOCK}) ),
        mtime     REAL NOT NULL CHECK( typeof(mtime) == 'real' AND mtime >= 0 ),
        atime     REAL NOT NULL CHECK( typeof(atime) == 'real' AND atime >= 0 ),
        ctime     REAL NOT NULL CHECK( typeof(ctime) == 'real' AND ctime >= 0 ),
        refcount  INT NOT NULL CONSTRAINT refcount_type
                  CHECK ( typeof(refcount) == 'integer' AND refcount > 0),

        -- for symlinks only
        target    BLOB(256)
                  CHECK ( (mode & {S_IFMT} == {S_IFLNK} AND typeof(target) == 'blob') OR
                          (mode & {S_IFMT} != {S_IFLNK} AND target IS NULL) ),

        -- for files only
        size      INT
                  CHECK ( (mode & {S_IFMT} == {S_IFREG} AND typeof(size) == 'integer' AND size >= 0) OR
                          (mode & {S_IFMT} != {S_IFREG} AND size IS NULL) ),

        -- device nodes
        rdev      INT
                  CHECK ( ( mode & {S_IFMT} IN ({S_IFBLK}, {S_IFCHR})
                               AND typeof(rdev) == 'integer' AND rdev >= 0) OR
                          ( mode & {S_IFMT} NOT IN ({S_IFBLK}, {S_IFCHR}) AND rdev IS NULL ) )    
             
    )
    """.format(**types))
    conn.execute(trigger_cmd.format(**{ "src_table": "contents",
                                         "src_key": "inode",
                                         "ref_table": "inodes",
                                         "ref_key": "id" }))
    conn.execute(trigger_cmd.format(**{ "src_table": "contents",
                                         "src_key": "parent_inode",
                                         "ref_table": "inodes",
                                         "ref_key": "id" }))
    
    # Make sure that we cannot change a directory into something
    # else as long as it has entries
    conn.execute("""  
    CREATE TRIGGER inodes_check_parent_inode_update
      BEFORE UPDATE ON inodes
      FOR EACH ROW BEGIN
          SELECT RAISE(ABORT, 'cannot change directory with entries into something else')
          WHERE NEW.mode & {S_IFMT} != {S_IFDIR} AND
               (SELECT name FROM contents WHERE parent_inode = NEW.id LIMIT 1) IS NOT NULL;
      END;      
    """.format(**types)) 
    
    
    # Maps file data chunks to S3 objects
    # Refcount is included for performance reasons
    conn.execute("""
    CREATE TABLE s3_objects (
        id        TEXT PRIMARY KEY,
        refcount  INT NOT NULL
                  CHECK ( typeof(refcount) == 'integer' ),
                  
        -- etag and size is only updated when the object is committed
        etag      TEXT
                  CHECK ( typeof(etag) IN ('blob', 'null') ),
        size      INT
                  CHECK ( typeof(size) IN ('integer', 'null') )
    )
    """)
    
    # Maps file data chunks to S3 objects
    conn.execute("""
    CREATE TABLE inode_s3key (
        inode     INTEGER NOT NULL REFERENCES inodes(id),
        offset    INT NOT NULL CHECK (typeof(offset) == 'integer' AND offset >= 0),
        s3key     TEXT NOT NULL REFERENCES s3_objects(id),
 
        PRIMARY KEY (inode, offset)
    )
    """)
    conn.execute(trigger_cmd.format(**{ "src_table": "inode_s3key",
                                            "src_key": "inode",
                                            "ref_table": "inodes",
                                            "ref_key": "id" }))
    conn.execute(trigger_cmd.format(**{ "src_table": "inode_s3key",
                                            "src_key": "s3key",
                                            "ref_table": "s3_objects",
                                            "ref_key": "id" }))

    # Make sure that inodes refer to files
    conn.execute("""
    CREATE TRIGGER inodes_1
      BEFORE INSERT ON inode_s3key
      FOR EACH ROW BEGIN
          SELECT RAISE(ABORT, 'inode does not refer to a file')
          WHERE (SELECT mode FROM inodes WHERE id = NEW.inode) & {S_IFMT} != {S_IFREG};
      END;

    CREATE TRIGGER inodes_2
      BEFORE UPDATE ON inode_s3key
      FOR EACH ROW BEGIN
          SELECT RAISE(ABORT, 'inode does not refer to a file')
          WHERE (SELECT mode FROM inodes WHERE id = NEW.inode) & {S_IFMT} != {S_IFREG};
      END;  
      
    CREATE TRIGGER inodes_3
      BEFORE UPDATE ON inodes
      FOR EACH ROW BEGIN
          SELECT RAISE(ABORT, 'cannot change file with s3 objects into something else')
          WHERE NEW.mode & {S_IFMT} != {S_IFREG} AND
               (SELECT inode FROM inode_s3key WHERE inode = NEW.id LIMIT 1) IS NOT NULL;
      END;    
    """.format(**types)) 
    
    # Insert root directory
    # Refcount = 4: ".", "..", "lost+found", "lost+found/.."
    timestamp = time.time() - time.timezone
    conn.execute("INSERT INTO inodes (id, mode,uid,gid,mtime,atime,ctime,refcount) "
                   "VALUES (?,?,?,?,?,?,?,?)", 
                   (ROOT_INODE, stat.S_IFDIR | stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR
                   | stat.S_IRGRP | stat.S_IXGRP | stat.S_IROTH | stat.S_IXOTH,
                    os.getuid(), os.getgid(), timestamp, timestamp, timestamp, 4))
    conn.execute("INSERT INTO contents(name, inode, parent_inode) VALUES(?,?,?)",
                   (b".", ROOT_INODE, ROOT_INODE))
    conn.execute("INSERT INTO contents(name, inode, parent_inode) VALUES(?,?,?)",
                   (b"..", ROOT_INODE, ROOT_INODE))            

    # Insert lost+found directory
    # refcount = 2: /, "."
    conn.execute("INSERT INTO inodes (mode,uid,gid,mtime,atime,ctime,refcount) "
                   "VALUES (?,?,?,?,?,?,?)",
                   (stat.S_IFDIR | stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR,
                    os.getuid(), os.getgid(), timestamp, timestamp, timestamp, 2))
    inode = conn.last_rowid()
    conn.execute("INSERT INTO contents (name, inode, parent_inode) VALUES(?,?,?)",
                   (b"lost+found", inode, ROOT_INODE))
    conn.execute("INSERT INTO contents (name, inode, parent_inode) VALUES(?,?,?)",
                   (b".", inode, inode))
    conn.execute("INSERT INTO contents (name, inode, parent_inode) VALUES(?,?,?)",
                   (b"..", ROOT_INODE, inode))
        
