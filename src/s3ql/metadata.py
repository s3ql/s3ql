'''
metadata.py - this file is part of S3QL.

Copyright © 2008 Nikolaus Rath <Nikolaus@rath.org>

This work can be distributed under the terms of the GNU GPLv3.
'''

from .logging import logging # Ensure use of custom logger class
from .database import Connection
from . import BUFSIZE
from .common import pretty_print_size
from .deltadump import INTEGER, BLOB, dump_table, load_table
from .backends.common import NoSuchObject, CorruptedObjectError
import os
import tempfile
import bz2
import stat

log = logging.getLogger(__name__)

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
                               ('mtime_ns', INTEGER),
                               ('atime_ns', INTEGER),
                               ('ctime_ns', INTEGER),
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



def restore_metadata(fh, dbfile):
    '''Read metadata from *fh* and write into *dbfile*

    Return database connection to *dbfile*.

    *fh* must be able to return an actual file descriptor from
    its `fileno` method.

    *dbfile* will be created with 0600 permissions. Data is
    first written into a temporary file *dbfile* + '.tmp', and
    the file is renamed once all data has been loaded.
    '''

    tmpfile = dbfile + '.tmp'
    fd = os.open(tmpfile, os.O_RDWR | os.O_CREAT | os.O_TRUNC,
                 stat.S_IRUSR | stat.S_IWUSR)
    try:
        os.close(fd)

        db = Connection(tmpfile)
        db.execute('PRAGMA locking_mode = NORMAL')
        db.execute('PRAGMA synchronous = OFF')
        db.execute('PRAGMA journal_mode = OFF')
        create_tables(db)

        for (table, _, columns) in DUMP_SPEC:
            log.info('..%s..', table)
            load_table(table, columns, db=db, fh=fh)
        db.execute('ANALYZE')

        # We must close the database to rename it
        db.close()
    except:
        os.unlink(tmpfile)
        raise

    os.rename(tmpfile, dbfile)

    return Connection(dbfile)

def cycle_metadata(backend, keep=10):
    '''Rotate metadata backups'''

    # Since we always overwrite the source afterwards, we can
    # use either copy or rename - so we pick whatever is faster.
    if backend.has_native_rename:
        cycle_fn = backend.rename
    else:
        cycle_fn = backend.copy

    log.info('Backing up old metadata...')
    for i in range(keep)[::-1]:
        try:
            cycle_fn("s3ql_metadata_bak_%d" % i, "s3ql_metadata_bak_%d" % (i + 1))
        except NoSuchObject:
            pass

    # If we use backend.rename() and crash right after this instruction,
    # we will end up without an s3ql_metadata object. However, fsck.s3ql
    # is smart enough to use s3ql_metadata_new in this case.
    try:
        cycle_fn("s3ql_metadata", "s3ql_metadata_bak_0")
    except NoSuchObject:
        # In case of mkfs, there may be no metadata object yet
        pass
    cycle_fn("s3ql_metadata_new", "s3ql_metadata")

    # Note that we can't compare with "is" (maybe because the bound-method
    # is re-created on the fly on access?)
    if cycle_fn == backend.copy:
        backend.delete('s3ql_metadata_new')

def dump_metadata(db, fh):
    '''Dump metadata into fh

    *fh* must be able to return an actual file descriptor from
    its `fileno` method.
    '''

    locking_mode = db.get_val('PRAGMA locking_mode')
    try:
        # Ensure that we don't hold a lock on the db
        # (need to access DB to actually release locks)
        db.execute('PRAGMA locking_mode = NORMAL')
        db.has_val('SELECT rowid FROM %s LIMIT 1' % DUMP_SPEC[0][0])

        for (table, order, columns) in DUMP_SPEC:
            log.info('..%s..', table)
            dump_table(table, order, columns, db=db, fh=fh)

    finally:
        db.execute('PRAGMA locking_mode = %s' % locking_mode)


def create_tables(conn):
    # Table of storage objects
    # Refcount is included for performance reasons
    # size == -1 indicates block has not been uploaded yet
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
        mtime_ns  INT NOT NULL,
        atime_ns  INT NOT NULL,
        ctime_ns  INT NOT NULL,
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

def stream_write_bz2(ifh, ofh):
    '''Compress *ifh* into *ofh* using bz2 compression'''

    compr = bz2.BZ2Compressor(9)
    while True:
        buf = ifh.read(BUFSIZE)
        if not buf:
            break
        buf = compr.compress(buf)
        if buf:
            ofh.write(buf)
    buf = compr.flush()
    if buf:
        ofh.write(buf)

def stream_read_bz2(ifh, ofh):
    '''Uncompress bz2 compressed *ifh* into *ofh*'''

    decompressor = bz2.BZ2Decompressor()
    while True:
        buf = ifh.read(BUFSIZE)
        if not buf:
            break
        buf = decompressor.decompress(buf)
        if buf:
            ofh.write(buf)

    if decompressor.unused_data or ifh.read(1) != b'':
        raise CorruptedObjectError('Data after end of bz2 stream')

def download_metadata(backend, db_file, name='s3ql_metadata'):
    with tempfile.TemporaryFile() as tmpfh:
        def do_read(fh):
            tmpfh.seek(0)
            tmpfh.truncate()
            stream_read_bz2(fh, tmpfh)

        log.info('Downloading and decompressing metadata...')
        backend.perform_read(do_read, name)

        log.info("Reading metadata...")
        tmpfh.seek(0)
        return restore_metadata(tmpfh, db_file)

def dump_and_upload_metadata(backend, db, param):
    with tempfile.TemporaryFile() as fh:
        log.info('Dumping metadata...')
        dump_metadata(db, fh)
        upload_metadata(backend, fh, param)

def upload_metadata(backend, fh, param):
    log.info("Compressing and uploading metadata...")
    def do_write(obj_fh):
        fh.seek(0)
        stream_write_bz2(fh, obj_fh)
        return obj_fh
    obj_fh = backend.perform_write(do_write, "s3ql_metadata_new",
                                   metadata=param, is_compressed=True)
    log.info('Wrote %s of compressed metadata.',
             pretty_print_size(obj_fh.get_obj_size()))

    log.info('Cycling metadata backups...')
    cycle_metadata(backend)
