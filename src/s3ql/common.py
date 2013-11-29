'''
common.py - this file is part of S3QL (http://s3ql.googlecode.com)

Copyright (C) 2008-2009 Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU GPLv3.
'''


from .logging import logging, QuietError # Ensure use of custom logger class
from llfuse import ROOT_INODE
import subprocess
import bz2
import errno
import hashlib
import llfuse
import os
import posixpath
import stat
import sys

# Buffer size when writing objects
BUFSIZE = 256 * 1024

# Pickle protocol version to use.
PICKLE_PROTOCOL = 2

# Name and inode of the special s3ql control file
CTRL_NAME = '.__s3ql__ctrl__'
CTRL_INODE = 2

log = logging.getLogger(__name__)

file_system_encoding = sys.getfilesystemencoding()
def path2bytes(s):
    return s.encode(file_system_encoding, 'surrogateescape')
def bytes2path(s):
    return s.decode(file_system_encoding, 'surrogateescape')

def get_seq_no(backend):
    '''Get current metadata sequence number'''
    from .backends.common import NoSuchObject

    seq_nos = list(backend.list('s3ql_seq_no_'))
    if not seq_nos:
        # Maybe list result is outdated
        seq_nos = [ 's3ql_seq_no_1' ]

    if (seq_nos[0].endswith('.meta')
        or seq_nos[0].endswith('.dat')):
        raise QuietError('Old file system revision, please run `s3qladm upgrade` first.')

    seq_nos = [ int(x[len('s3ql_seq_no_'):]) for x in seq_nos ]
    seq_no = max(seq_nos)

    # Make sure that object really exists
    while ('s3ql_seq_no_%d' % seq_no) not in backend:
        seq_no -= 1
        if seq_no == 0:
            raise QuietError('No S3QL file system found at given storage URL.')
    while ('s3ql_seq_no_%d' % seq_no) in backend:
        seq_no += 1
    seq_no -= 1

    # Delete old seq nos
    for i in [ x for x in seq_nos if x < seq_no - 10 ]:
        try:
            del backend['s3ql_seq_no_%d' % i]
        except NoSuchObject:
            pass # Key list may not be up to date

    return seq_no

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
        raise ChecksumError('Data after end of bz2 stream')

    
def is_mounted(storage_url):
    '''Try to determine if *storage_url* is mounted

    Note that the result may be wrong.. this is really just
    a best-effort guess.
    '''

    match = storage_url + ' '
    if os.path.exists('/proc/mounts'):
        with open('/proc/mounts', 'r') as fh:
            for line in fh:
                if line.startswith(match):
                    return True
            return False

    try:
        for line in subprocess.check_output(['mount'], stderr=subprocess.STDOUT,
                                            universal_newlines=True):
            if line.startswith(match):
                return True
    except subprocess.CalledProcessError:
        log.warning('Warning! Unable to check if file system is mounted '
                    '(/proc/mounts missing and mount call failed)')
    
    return False

        
class ChecksumError(Exception):
    """
    Raised if there is a checksum error in the data that we received.
    """

    def __init__(self, str_):
        super().__init__()
        self.str = str_

    def __str__(self):
        return self.str

def inode_for_path(path, conn):
    """Return inode of directory entry at `path`
    
     Raises `KeyError` if the path does not exist.
    """
    from .database import NoSuchRowError

    if not isinstance(path, bytes):
        raise TypeError('path must be of type bytes')

    # Remove leading and trailing /
    path = path.lstrip(b"/").rstrip(b"/")

    # Traverse
    inode = ROOT_INODE
    for el in path.split(b'/'):
        try:
            inode = conn.get_val("SELECT inode FROM contents_v WHERE name=? AND parent_inode=?",
                                 (el, inode))
        except NoSuchRowError:
            raise KeyError('Path %s does not exist' % path) from None

    return inode

def get_path(id_, conn, name=None):
    """Return a full path for inode `id_`.
    
    If `name` is specified, it is appended at the very end of the
    path (useful if looking up the path for file name with parent
    inode).
    """

    if name is None:
        path = list()
    else:
        if not isinstance(name, bytes):
            raise TypeError('name must be of type bytes')
        path = [ name ]

    maxdepth = 255
    while id_ != ROOT_INODE:
        # This can be ambiguous if directories are hardlinked
        (name2, id_) = conn.get_row("SELECT name, parent_inode FROM contents_v "
                                    "WHERE inode=? LIMIT 1", (id_,))
        path.append(name2)
        maxdepth -= 1
        if maxdepth == 0:
            raise RuntimeError('Failed to resolve name "%s" at inode %d to path',
                               name, id_)

    path.append(b'')
    path.reverse()

    return b'/'.join(path)


def _escape(s):
    '''Escape '/', '=' and '\0' in s'''

    s = s.replace('=', '=3D')
    s = s.replace('/', '=2F')
    s = s.replace('\0', '=00')

    return s

def get_backend_cachedir(storage_url, cachedir):
    if not os.path.exists(cachedir):
        try:
            os.mkdir(cachedir, stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR)
        except PermissionError:
            raise QuietError('No permission to create cache directory (%s)' % cachedir) from None
        
    if not os.access(cachedir, os.R_OK | os.W_OK | os.X_OK):
        raise QuietError('No permission to access cache directory (%s)' % cachedir)
    
    return os.path.join(cachedir, _escape(storage_url))


def sha256_fh(fh):
    fh.seek(0)

    # Bogus error about hashlib not having a sha256 member
    #pylint: disable=E1101
    sha = hashlib.sha256()

    while True:
        buf = fh.read(BUFSIZE)
        if not buf:
            break
        sha.update(buf)

    return sha.digest()

def md5sum(buf):
    '''Return md5 sum for *buf*'''
    
    if not isinstance(buf, (bytes, bytearray, memoryview)):
        raise TypeError("Expected binary data, got %s" % type(buf))

    md5 = hashlib.md5()
    md5.update(buf)
    return md5.digest()

def assert_s3ql_fs(path):
    '''Raise `QuietError` if *path* is not on an S3QL file system
    
    Returns name of the S3QL control file.
    '''

    try:
        os.stat(path)
    except FileNotFoundError:
        raise QuietError('%s does not exist' % path) from None
    except OSError as exc:
        if exc.errno is errno.ENOTCONN:
            raise QuietError('File system appears to have crashed.') from None
        raise

    ctrlfile = os.path.join(path, CTRL_NAME)
    if not (CTRL_NAME not in llfuse.listdir(path)
            and os.path.exists(ctrlfile)):
        raise QuietError('%s is not on an S3QL file system' % path)

    return ctrlfile
    
    
def assert_fs_owner(path, mountpoint=False):
    '''Raise `QuietError` if user is not owner of S3QL fs at *path*
    
    Implicitly calls `assert_s3ql_fs` first. Returns name of the 
    S3QL control file.
    
    If *mountpoint* is True, also call `assert_s3ql_mountpoint`, i.e.
    fail if *path* is not the mount point of the file system.
    '''
        
    if mountpoint:
        ctrlfile = assert_s3ql_mountpoint(path)
    else:
        ctrlfile = assert_s3ql_fs(path)
    
    if os.stat(ctrlfile).st_uid != os.geteuid() and os.geteuid() != 0:
        raise QuietError('Permission denied. %s is was not mounted by you '
                         'and you are not root.' % path)

    return ctrlfile
    
def assert_s3ql_mountpoint(mountpoint):
    '''Raise QuietError if *mountpoint* is not an S3QL mountpoint
    
    Implicitly calls `assert_s3ql_fs` first. Returns name of the 
    S3QL control file.
    '''

    ctrlfile = assert_s3ql_fs(mountpoint)
    if not posixpath.ismount(mountpoint):
        raise QuietError('%s is not a mount point' % mountpoint)

    return ctrlfile
