'''
interface.py - this file is part of S3QL (http://s3ql.googlecode.com)

Copyright (C) 2008-2010 Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU LGPL.


This module defines the interface between the FUSE C and Python API. The actual
file system is implemented as an `Operations` instance whose methods will be
called by the fuse library.

Note that all "string-like" quantities (e.g. file names, extended attribute
names & values) are represented as bytes, since POSIX doesn't require any of
them to be valid unicode strings.

This module uses the `global_lock` module. Methods of the operations
instance will be called with an acquired global lock. If the instance
wants to process several requests concurrently, it may explicitly
release the lock.

Exception Handling
------------------

Since Python exceptions cannot be forwarded to the FUSE kernel module, the FUSE
Python API catches all exceptions that are generated during request processing.

If the exception is of type `FUSEError`, the appropriate errno is returned to
the kernel module and the exception is discarded.

For any other exceptions, a warning is logged and a generic error signaled to
the kernel module. Then the `handle_exc` method of the `Operations` instance is
called, so that the file system itself has a chance to react to the problem
(e.g. by marking the file system as needing a check).

The return value and any raised exceptions of `handle_exc` are ignored.
'''

# Since we are using ctype Structures, we often have to
# access attributes that are not defined in __init__
# (since they are defined in _fields_ instead)
#pylint: disable=W0212

# We need globals
#pylint: disable=W0603

# And many unused arguments can not be avoided
#pylint: disable=W0613

from __future__ import division, print_function, absolute_import

from . import ctypes_api as libfuse
from ctypes import c_char_p, sizeof, create_string_buffer, addressof, string_at, POINTER, c_char, cast
from functools import partial
import errno
import logging
import sys
import stat as fstat
from global_lock import lock

__all__ = [ 'FUSEError', 'ENOATTR', 'ENOTSUP', 'init', 'main', 'close',
            'fuse_version', 'invalidate_entry', 'invalidate_inode',
            'ROOT_INODE' ]

# These should really be defined in the errno module, but
# unfortunately they are missing
ENOATTR = libfuse.ENOATTR
ENOTSUP = libfuse.ENOTSUP

ROOT_INODE = 1

log = logging.getLogger("fuse")

# Init globals
operations = None
fuse_ops = None
mountpoint = None
session = None
channel = None

class DiscardedRequest(Exception):
    '''Request was interrupted and reply discarded. '''
    pass

class ReplyError(Exception):
    '''Unable to send reply to fuse kernel module'''
    pass

class FUSEError(Exception):
    '''Wrapped errno value to be returned to the fuse kernel module

    This exception can store only an errno. Request handlers should raise
    it to return a specific errno to the fuse kernel module.
    '''

    __slots__ = [ 'errno' ]

    def __init__(self, errno_):
        super(FUSEError, self).__init__()
        self.errno = errno_

    def __str__(self):
        # errno may not have strings for all error codes
        return errno.errorcode.get(self.errno, str(self.errno))



def check_reply_result(result, func, *args):
    '''Check result of a call to a fuse_reply_* foreign function
    
    If `result` is 0, it is assumed that the call succeeded and the function
    does nothing.
    
    If result is `-errno.ENOENT`, this means that the request has been discarded
    and `DiscardedRequest` is raised.
    
    In all other cases,  `ReplyError` is raised.
    '''

    if result == 0:
        return None

    elif result == -errno.ENOENT:
        raise DiscardedRequest()

    elif result > 0:
        raise ReplyError('Foreign function %s returned unexpected value %d'
                         % (func.name, result))
    elif result < 0:
        raise ReplyError('Foreign function %s returned error %s'
                         % (func.name, errno.errorcode.get(-result, str(-result))))


# Set return checker for common ctypes calls
reply_functions = [ 'fuse_reply_err', 'fuse_reply_entry',
                   'fuse_reply_create', 'fuse_reply_readlink', 'fuse_reply_open',
                   'fuse_reply_write', 'fuse_reply_attr', 'fuse_reply_buf',
                   'fuse_reply_iov', 'fuse_reply_statfs', 'fuse_reply_xattr',
                   'fuse_reply_lock' ]
for fname in reply_functions:
    getattr(libfuse, fname).errcheck = check_reply_result


def dict_to_entry(attr):
    '''Convert dict to fuse_entry_param'''

    entry = libfuse.fuse_entry_param()

    entry.ino = attr.st_ino
    entry.generation = attr.generation
    entry.entry_timeout = attr.entry_timeout
    entry.attr_timeout = attr.attr_timeout

    entry.attr = dict_to_stat(attr)

    return entry

def dict_to_stat(attr):
    '''Convert dict to struct stat'''

    stat = libfuse.stat()

    # Determine correct way to store times
    if hasattr(stat, 'st_atim'): # Linux
        get_timespec_key = lambda key: key[:-1]
    elif hasattr(stat, 'st_atimespec'): # FreeBSD
        get_timespec_key = lambda key: key + 'spec'
    else:
        get_timespec_key = False

    # Raises exception if there are any unknown keys
    for key in ('st_ino', 'st_mode', 'st_nlink', 'st_uid', 'st_gid',
                'st_rdev', 'st_size', 'st_blksize', 'st_blocks',
                'st_atime', 'st_mtime', 'st_ctime'):
        val = getattr(attr, key)
        if get_timespec_key and key in  ('st_atime', 'st_mtime', 'st_ctime'):
            key = get_timespec_key(key)
            spec = libfuse.timespec()
            spec.tv_sec = int(val)
            spec.tv_nsec = int((val - int(val)) * 10 ** 9)
            val = spec
        setattr(stat, key, val)

    return stat


def stat_to_dict(stat):
    '''Convert ``struct stat`` to dict'''

    attr = dict()
    for (name, dummy) in libfuse.stat._fields_:
        if name.startswith('__'):
            continue

        if name in ('st_atim', 'st_mtim', 'st_ctim'):
            key = name + 'e'
            attr[key] = getattr(stat, name).tv_sec + getattr(stat, name).tv_nsec / 10 ** 9
        elif name in ('st_atimespec', 'st_mtimespec', 'st_ctimespec'):
            key = name[:-4]
            attr[key] = getattr(stat, name).tv_sec + getattr(stat, name).tv_nsec / 10 ** 9
        else:
            attr[name] = getattr(stat, name)

    return attr


def op_wrapper(func, req, *args):
    '''Catch all exceptions and call fuse_reply_err instead
    
    (We do not try to call `fuse_reply_err` in case of a ReplyError, because the
    first reply function may have already invalidated the `req` object and it
    seems better to (possibly) let the request pend than to crash the server
    application.)
    '''

    try:
        func(req, *args)
    except FUSEError as e:
        log.debug('op_wrapper caught FUSEError, calling reply_err(%s)',
                  errno.errorcode.get(e.errno, str(e.errno)))
        try:
            libfuse.fuse_reply_err(req, e.errno)
        except DiscardedRequest:
            pass
    except Exception as exc:
        log.exception('FUSE handler raised exception.')

        # Report error to filesystem
        if hasattr(operations, 'handle_exc'):
            try:
                with lock:
                    operations.handle_exc(exc)
            except:
                pass

        # Send error reply, unless the error occurred when replying
        if (not isinstance(exc, ReplyError) 
            and func is not fuse_destroy 
            and func is not fuse_init):
            log.debug('Calling reply_err(EIO)')
            try:
                libfuse.fuse_reply_err(req, errno.EIO)
            except DiscardedRequest:
                pass


def fuse_version():
    '''Return version of loaded fuse library'''

    return libfuse.fuse_version()


def init(operations_, mountpoint_, args):
    '''Initialize and mount FUSE file system
            
    `operations_` has to be an instance of the `Operations` class (or another
    class defining the same methods).
    
    `args` has to be a list of strings. Valid options are listed in struct
    fuse_opt fuse_mount_opts[] (mount.c:68) and struct fuse_opt fuse_ll_opts[]
    (fuse_lowlevel_c:1526).
    
    If `lock` is specified (it has to be a `Lock` instance), this lock is
    acquired before a request handler is called. This can be used to serialize
    request handling by default. Concurrency can still be achieved for parts of
    the code by releasing and reacquiring the lock in the request handlers.
    '''

    log.debug('Initializing llfuse')

    global operations
    global fuse_ops
    global mountpoint
    global session
    global channel

    # Give operations instance a chance to check and change
    # the FUSE options
    operations_.check_args(args)

    mountpoint = mountpoint_
    operations = operations_
    fuse_ops = libfuse.fuse_lowlevel_ops()
    fuse_args = make_fuse_args(args)

    # Init fuse_ops
    module = globals()
    for (name, prototype) in libfuse.fuse_lowlevel_ops._fields_:
        if hasattr(operations, name):
            method = partial(op_wrapper, module['fuse_' + name])
            setattr(fuse_ops, name, prototype(method))

    log.debug('Calling fuse_mount')
    channel = libfuse.fuse_mount(mountpoint, fuse_args)
    if not channel:
        raise RuntimeError('fuse_mount failed')
    try:
        log.debug('Calling fuse_lowlevel_new')
        session = libfuse.fuse_lowlevel_new(fuse_args, fuse_ops, sizeof(fuse_ops), None)
        if not session:
            raise RuntimeError("fuse_lowlevel_new() failed")
        try:
            log.debug('Calling fuse_set_signal_handlers')
            if libfuse.fuse_set_signal_handlers(session) == -1:
                raise RuntimeError("fuse_set_signal_handlers() failed")
            try:
                log.debug('Calling fuse_session_add_chan')
                libfuse.fuse_session_add_chan(session, channel)
                session = session
                channel = channel
                return

            except:
                log.debug('Calling fuse_remove_signal_handlers')
                libfuse.fuse_remove_signal_handlers(session)
                raise

        except:
            log.debug('Calling fuse_session_destroy')
            libfuse.fuse_session_destroy(session)
            raise
    except:
        log.debug('Calling fuse_unmount')
        libfuse.fuse_unmount(mountpoint, channel)
        raise

def invalidate_inode(inode, attr_only=False):
    '''Invalidate cache for `inode`
    
    Tells the FUSE kernel module to forgot cached attributes and
    data (unless `attr_only` is True) for `inode.
    '''

    log.debug('Invalidating inode %d', inode)
    if attr_only:
        libfuse.fuse_lowlevel_notify_inval_inode(channel, inode, -1, 0)
    else:
        libfuse.fuse_lowlevel_notify_inval_inode(channel, inode, 0, 0)

def invalidate_entry(inode_p, name):
    '''Invalidate directory entry `name` in directory `inode_p`'''

    log.debug('Invalidating entry %r for inode %d', name, inode_p)
    libfuse.fuse_lowlevel_notify_inval_entry(channel, inode_p, name, len(name))

def make_fuse_args(args):
    '''Create fuse_args Structure for given mount options'''

    args1 = [ sys.argv[0] ]
    for opt in args:
        args1.append(b'-o')
        args1.append(opt)

    # Init fuse_args struct
    fuse_args = libfuse.fuse_args()
    fuse_args.allocated = 0
    fuse_args.argc = len(args1)
    fuse_args.argv = (POINTER(c_char) * len(args1))(*[cast(c_char_p(x), POINTER(c_char))
                                                      for x in args1])
    return fuse_args

def main(single=False):
    '''Run FUSE main loop'''

    if not session:
        raise RuntimeError('Need to call init() before main()')

    if single:
        log.debug('Calling fuse_session_loop')
        if libfuse.fuse_session_loop(session) != 0:
            raise RuntimeError("fuse_session_loop() failed")
    else:
        log.debug('Calling fuse_session_loop_mt')
        lock.release()
        try:
            if libfuse.fuse_session_loop_mt(session) != 0:
                raise RuntimeError("fuse_session_loop_mt() failed")
        finally:
            lock.acquire()

def close():
    '''Unmount file system and clean up'''

    global operations
    global fuse_ops
    global mountpoint
    global session
    global channel

    log.debug('Calling fuse_session_remove_chan')
    libfuse.fuse_session_remove_chan(channel)
    log.debug('Calling fuse_remove_signal_handlers')
    libfuse.fuse_remove_signal_handlers(session)
    log.debug('Calling fuse_session_destroy')
    libfuse.fuse_session_destroy(session)
    log.debug('Calling fuse_unmount')
    libfuse.fuse_unmount(mountpoint, channel)

    operations = None
    fuse_ops = None
    mountpoint = None
    session = None
    channel = None


def fuse_lookup(req, parent_inode, name):
    '''Look up a directory entry by name and get its attributes'''

    log.debug('lookup(%d, %s): start', parent_inode, string_at(name))

    with lock:
        attr = operations.lookup(parent_inode, string_at(name))
    entry = dict_to_entry(attr)

    log.debug('lookup(%d, %s): calling reply_entry(ino=%d)',
              parent_inode, string_at(name), attr.id)
    try:
        libfuse.fuse_reply_entry(req, entry)
    except DiscardedRequest:
        pass

def fuse_init(userdata_p, conn_info_p):
    '''Initialize Operations'''
    
    log.debug('init(): start')
    with lock:
        operations.init()
    log.debug('init(): end')

def fuse_destroy(userdata_p):
    '''Cleanup Operations'''
    log.debug('destroy(): start')
    with lock:
        operations.destroy()
    log.debug('destroy(): end')

def fuse_getattr(req, ino, _unused):
    '''Get attributes for `ino`'''

    log.debug('getattr(%d): start', ino)

    with lock:
        attr = operations.getattr(ino)

    attr_timeout = attr.attr_timeout
    stat = dict_to_stat(attr)

    log.debug('getattr(%d): calling reply_attr', ino)
    try:
        libfuse.fuse_reply_attr(req, stat, attr_timeout)
    except DiscardedRequest:
        pass

def fuse_access(req, ino, mask):
    '''Check if calling user has `mask` rights for `ino`'''

    log.debug('access(%d, %o): start', ino, mask)

    # Get UID
    ctx = libfuse.fuse_req_ctx(req).contents

    # Define a function that returns a list of the GIDs
    def get_gids():
        # Get GID list if FUSE supports it
        # Weird syntax to prevent PyDev from complaining
        getgroups = getattr(libfuse, "fuse_req_getgroups")
        gid_t = getattr(libfuse, 'gid_t')
        no = 10
        buf = (gid_t * no)(range(no))
        ret = getgroups(req, no, buf)
        if ret > no:
            no = ret
            buf = (gid_t * no)(range(no))
            ret = getgroups(req, no, buf)

        return [ buf[i].value for i in range(ret) ]

    with lock:
        ret = operations.access(ino, mask, ctx, get_gids)

    log.debug('access(%d, %o): calling reply_err', ino, mask)
    try:
        if ret:
            libfuse.fuse_reply_err(req, 0)
        else:
            libfuse.fuse_reply_err(req, errno.EPERM)
    except DiscardedRequest:
        pass


def fuse_create(req, ino_parent, name, mode, fi):
    '''Create and open a file'''

    log.debug('create(%d, %s, %o): start', ino_parent, string_at(name), mode)

    # FUSE doesn't set any type information
    mode = (mode & ~fstat.S_IFMT(mode)) | fstat.S_IFREG

    with lock:
        (fh, attr) = operations.create(ino_parent, string_at(name), mode,
                                       libfuse.fuse_req_ctx(req).contents)
    fi.contents.fh = fh
    fi.contents.keep_cache = 1
    entry = dict_to_entry(attr)

    log.debug('create(%d, %s, %o): calling reply_create(fh=%i)',
              ino_parent, string_at(name), mode, fh)
    try:
        libfuse.fuse_reply_create(req, entry, fi)
    except DiscardedRequest:
        with lock:
            operations.release(fh)


def fuse_flush(req, ino, fi):
    '''Handle close() system call
    
    May be called multiple times for the same open file.
    '''

    log.debug('flush(%d): start', fi.contents.fh)
    with lock:
        operations.flush(fi.contents.fh)
    log.debug('flush(%d): calling reply_err', fi.contents.fh)
    try:
        libfuse.fuse_reply_err(req, 0)
    except DiscardedRequest:
        pass


def fuse_fsync(req, ino, datasync, fi):
    '''Flush buffers for `ino`
    
    If the datasync parameter is non-zero, then only the user data
    is flushed (and not the meta data).
    '''

    log.debug('fsync(%d, %s): start', fi.contents.fh, datasync != 0)
    with lock:
        operations.fsync(fi.contents.fh, datasync != 0)
    log.debug('fsync(%d, %s): calling reply_err',
              fi.contents.fh, datasync != 0)
    try:
        libfuse.fuse_reply_err(req, 0)
    except DiscardedRequest:
        pass


def fuse_fsyncdir(req, ino, datasync, fi):
    '''Synchronize directory contents
    
    If the datasync parameter is non-zero, then only the directory contents
    are flushed (and not the meta data about the directory itself).
    '''

    log.debug('fsyncdir(%d, %s): start', fi.contents.fh, datasync != 0)
    with lock:
        operations.fsyncdir(fi.contents.fh, datasync != 0)
    log.debug('fsyncdir(%d, %s): calling reply_err',
              fi.contents.fh, datasync != 0)
    try:
        libfuse.fuse_reply_err(req, 0)
    except DiscardedRequest:
        pass


def fuse_getxattr(req, ino, name, size):
    '''Get an extended attribute.
    '''

    log.debug('getxattr(%d, %r, %d): start', ino, string_at(name), size)
    with lock:
        val = operations.getxattr(ino, string_at(name))
    if not isinstance(val, bytes):
        raise TypeError("getxattr return value must be of type bytes")

    try:
        if size == 0:
            log.debug('getxattr(%d, %r, %d): calling reply_xattr',
                      ino, string_at(name), size)
            libfuse.fuse_reply_xattr(req, len(val))
        elif size >= len(val):
            log.debug('getxattr(%d, %r, %d): calling reply_buf',
                      ino, string_at(name), size)
            libfuse.fuse_reply_buf(req, val, len(val))
        else:
            raise FUSEError(errno.ERANGE)
    except DiscardedRequest:
        pass


def fuse_link(req, ino, new_parent_ino, new_name):
    '''Create a hard link'''

    log.debug('link(%d, %d, %s): start',
              ino, new_parent_ino, string_at(new_name))
    with lock:
        attr = operations.link(ino, new_parent_ino, string_at(new_name))
    entry = dict_to_entry(attr)

    log.debug('link(%d, %d, %s): calling reply_entry',
              ino, new_parent_ino, string_at(new_name))
    try:
        libfuse.fuse_reply_entry(req, entry)
    except DiscardedRequest:
        pass

def fuse_listxattr(req, inode, size):
    '''List extended attributes for `inode`'''

    log.debug('listxattr(%d): start', inode)
    with lock:
        names = operations.listxattr(inode)

    if not all([ isinstance(name, bytes) for name in names]):
        raise TypeError("listxattr return value must be list of bytes")

    if names:
        # Size of the \0 separated buffer 
        buf_size = len(names) + sum([ len(name) for name in names ])
        buf = b'\0'.join(names) + b'\0'
    else:
        buf_size = 0
        buf = ''

    if size == 0:
        try:
            log.debug('listxattr(%d): calling reply_xattr', inode)
            libfuse.fuse_reply_xattr(req, buf_size)
        except DiscardedRequest:
            pass

    elif buf_size > size:
        raise FUSEError(errno.ERANGE)

    else:
        try:
            log.debug('listxattr(%d): calling reply_buf', inode)
            libfuse.fuse_reply_buf(req, buf, buf_size)
        except DiscardedRequest:
            pass


def fuse_mkdir(req, inode_parent, name, mode):
    '''Create directory'''

    log.debug('mkdir(%d, %s, %o): start',
              inode_parent, string_at(name), mode)

    # FUSE doesn't set any type information
    mode = (mode & ~fstat.S_IFMT(mode)) | fstat.S_IFDIR
    with lock:
        attr = operations.mkdir(inode_parent, string_at(name), mode,
                                libfuse.fuse_req_ctx(req).contents)
    entry = dict_to_entry(attr)

    log.debug('mkdir(%d, %s, %o): calling reply_entry(ino=%d)',
              inode_parent, string_at(name), mode, attr.id)
    try:
        libfuse.fuse_reply_entry(req, entry)
    except DiscardedRequest:
        pass

def fuse_mknod(req, inode_parent, name, mode, rdev):
    '''Create (possibly special) file'''

    log.debug('mknod(%d, %s, %o, %d): start',
              inode_parent, string_at(name), mode, rdev)
    with lock:
        attr = operations.mknod(inode_parent, string_at(name), mode, rdev,
                                libfuse.fuse_req_ctx(req).contents)
    entry = dict_to_entry(attr)

    log.debug('mknod(%d, %s, %o, %d): calling reply_entry(ino=%d)',
              inode_parent, string_at(name), mode, rdev, attr.id)
    try:
        libfuse.fuse_reply_entry(req, entry)
    except DiscardedRequest:
        pass

def fuse_open(req, inode, fi):
    '''Open a file'''
    
    log.debug('open(%d, %d): start', inode, fi.contents.flags)
    with lock:
        fi.contents.fh = operations.open(inode, fi.contents.flags)
    fi.contents.keep_cache = 1

    log.debug('open(%d, %d): calling reply_open',
              inode, fi.contents.flags)
    try:
        libfuse.fuse_reply_open(req, fi)
    except DiscardedRequest:
        with lock:
            operations.release(inode, fi.contents.fh)

def fuse_opendir(req, inode, fi):
    '''Open a directory'''

    log.debug('opendir(%d): start', inode)
    with lock:
        fi.contents.fh = operations.opendir(inode)

    log.debug('opendir(%d): calling reply_open(fh=%d)',
               inode, fi.contents.fh)
    try:
        libfuse.fuse_reply_open(req, fi)
    except DiscardedRequest:
        with lock:
            operations.releasedir(fi.contents.fh)


def fuse_read(req, ino, size, off, fi):
    '''Read data from file'''

    log.debug('read(ino=%d, off=%d, size=%d): start',
              fi.contents.fh, off, size)
    with lock:
        data = operations.read(fi.contents.fh, off, size)

    if not isinstance(data, bytes):
        raise TypeError("read() must return bytes")

    if len(data) > size:
        raise ValueError('read() must not return more than `size` bytes')

    log.debug('read(ino=%d, off=%d, size=%d): calling reply_buf',
              fi.contents.fh, off, size)
    try:
        libfuse.fuse_reply_buf(req, data, len(data))
    except DiscardedRequest:
        pass


def fuse_readlink(req, inode):
    '''Read target of symbolic link'''

    log.debug('readlink(%d): start', inode)
    with lock:
        target = operations.readlink(inode)
    log.debug('readlink(%d): calling reply_readlink', inode)
    try:
        libfuse.fuse_reply_readlink(req, target)
    except DiscardedRequest:
        pass


def fuse_readdir(req, ino, bufsize, off, fi):
    '''Read directory entries'''

    log.debug('readdir(%d, %d, %d, %d): start',
              ino, bufsize, off, fi.contents.fh)

    # Collect as much entries as we can return
    entries = list()
    size = 0
    with lock:
        for (name, attr, next_) in operations.readdir(fi.contents.fh, off):
            if not isinstance(name, bytes):
                raise TypeError("readdir() must return entry names as bytes")
    
            stat = dict_to_stat(attr)
    
            entry_size = libfuse.fuse_add_direntry(req, None, 0, name, stat, 0)
            if size + entry_size > bufsize:
                break
    
            entries.append((name, stat, next_))
            size += entry_size

    log.debug('readdir(%d, %d, %d, %d): gathered %d entries, total size %d',
              ino, bufsize, off, fi.contents.fh, len(entries), size)

    # If there are no entries left, return empty buffer
    if not entries:
        try:
            log.debug('readdir(%d, %d, %d, %d): calling reply_buf',
                      ino, bufsize, off, fi.contents.fh)
            libfuse.fuse_reply_buf(req, None, 0)
        except DiscardedRequest:
            pass
        return

    # Create and fill buffer
    buf = create_string_buffer(size)
    addr_off = 0
    for (name, stat, next_) in entries:
        addr_off += libfuse.fuse_add_direntry(req, cast(addressof(buf) + addr_off, POINTER(c_char)),
                                              bufsize, name, stat, next_)

    # Return buffer
    log.debug('readdir(%d, %d, %d, %d): calling reply_buf',
              ino, bufsize, off, fi.contents.fh)
    try:
        libfuse.fuse_reply_buf(req, buf, size)
    except DiscardedRequest:
        pass


def fuse_release(req, inode, fi):
    '''Release open file'''

    log.debug('release(%d): start', fi.contents.fh)
    with lock:
        operations.release(fi.contents.fh)
    log.debug('release(%d): calling reply_err', fi.contents.fh)
    try:
        libfuse.fuse_reply_err(req, 0)
    except DiscardedRequest:
        pass

def fuse_releasedir(req, inode, fi):
    '''Release open directory'''

    log.debug('releasedir(%d): start', fi.contents.fh)
    with lock:
        operations.releasedir(fi.contents.fh)
    log.debug('releasedir(%d): calling reply_err', fi.contents.fh)
    try:
        libfuse.fuse_reply_err(req, 0)
    except DiscardedRequest:
        pass

def fuse_removexattr(req, inode, name):
    '''Remove extended attribute'''

    log.debug('removexattr(%d, %s): start', inode, string_at(name))
    with lock:
        operations.removexattr(inode, string_at(name))
    log.debug('removexattr(%d, %s): calling reply_err',
              inode, string_at(name))
    try:
        libfuse.fuse_reply_err(req, 0)
    except DiscardedRequest:
        pass

def fuse_rename(req, parent_inode_old, name_old, parent_inode_new, name_new):
    '''Rename a directory entry'''

    log.debug('rename(%d, %r, %d, %r): start', parent_inode_old, string_at(name_old),
              parent_inode_new, string_at(name_new))
    with lock:
        operations.rename(parent_inode_old, string_at(name_old), parent_inode_new,
                          string_at(name_new))
    log.debug('rename(%d, %r, %d, %r): calling reply_err', parent_inode_old,
              string_at(name_old), parent_inode_new, string_at(name_new))
    try:
        libfuse.fuse_reply_err(req, 0)
    except DiscardedRequest:
        pass

def fuse_rmdir(req, inode_parent, name):
    '''Remove a directory'''

    log.debug('rmdir(%d, %r): start', inode_parent, string_at(name))
    with lock:
        operations.rmdir(inode_parent, string_at(name))
    log.debug('rmdir(%d, %r): calling reply_err',
              inode_parent, string_at(name))
    try:
        libfuse.fuse_reply_err(req, 0)
    except DiscardedRequest:
        pass

def fuse_setattr(req, inode, stat, to_set, fi):
    '''Change directory entry attributes'''

    log.debug('fuse_setattr(%d): start', inode)

    # Note: We can't check if we know all possible flags,
    # because the part of to_set that is not "covered"
    # by flags seems to be undefined rather than zero.

    attr_all = stat_to_dict(stat.contents)
    attr = dict()

    if (to_set & libfuse.FUSE_SET_ATTR_MTIME) != 0:
        attr['st_mtime'] = attr_all['st_mtime']

    if (to_set & libfuse.FUSE_SET_ATTR_ATIME) != 0:
        attr['st_atime'] = attr_all['st_atime']

    if (to_set & libfuse.FUSE_SET_ATTR_MODE) != 0:
        attr['st_mode'] = attr_all['st_mode']

    if (to_set & libfuse.FUSE_SET_ATTR_UID) != 0:
        attr['st_uid'] = attr_all['st_uid']

    if (to_set & libfuse.FUSE_SET_ATTR_GID) != 0:
        attr['st_gid'] = attr_all['st_gid']

    if (to_set & libfuse.FUSE_SET_ATTR_SIZE) != 0:
        attr['st_size'] = attr_all['st_size']

    with lock:
        attr = operations.setattr(inode, attr)

    attr_timeout = attr.attr_timeout
    stat = dict_to_stat(attr)

    log.debug('fuse_setattr(%d): calling reply_attr', inode)
    try:
        libfuse.fuse_reply_attr(req, stat, attr_timeout)
    except DiscardedRequest:
        pass

import traceback
import os
def log_stacktraces():
    '''Log stack trace for every running thread'''
    
    # Access to protected member
    #pylint: disable=W0212
    code = list()
    for threadId, frame in sys._current_frames().items():
        code.append("\n# ThreadID: %s" % threadId)
        for filename, lineno, name, line in traceback.extract_stack(frame):
            code.append('%s:%d, in %s' % (os.path.basename(filename), lineno, name))
            if line:
                code.append("    %s" % (line.strip()))

    log.error("\n".join(code))
    
def fuse_setxattr(req, inode, name, val, size, flags):
    '''Set an extended attribute'''

    log.debug('setxattr(%d, %r, %r, %d): start', inode, string_at(name),
              string_at(val, size), flags)

    if inode == ROOT_INODE and string_at(name) == 'fuse_stacktrace':
        log_stacktraces()
        libfuse.fuse_reply_err(req, 0)
        return
    
    # Make sure we know all the flags
    if (flags & ~(libfuse.XATTR_CREATE | libfuse.XATTR_REPLACE)) != 0:
        raise ValueError('unknown flag')

    with lock:
        if (flags & libfuse.XATTR_CREATE) != 0:
            try:
                operations.getxattr(inode, string_at(name))
            except FUSEError as e:
                if e.errno == ENOATTR:
                    pass
                raise
            else:
                raise FUSEError(errno.EEXIST)
        elif (flags & libfuse.XATTR_REPLACE) != 0:
            # Exception can be passed on if the attribute does not exist
            operations.getxattr(inode, string_at(name))
    
        operations.setxattr(inode, string_at(name), string_at(val, size))

    log.debug('setxattr(%d, %r, %r, %d): calling reply_err',
              inode, string_at(name), string_at(val, size), flags)
    try:
        libfuse.fuse_reply_err(req, 0)
    except DiscardedRequest:
        pass

def fuse_statfs(req, inode):
    '''Return filesystem statistics'''

    log.debug('statfs(%d): start', inode)
    with lock:
        attr = operations.statfs()
    statfs = libfuse.statvfs()

    for (key, val) in attr.iteritems():
        setattr(statfs, key, val)

    log.debug('statfs(%d): calling reply_statfs', inode)
    try:
        libfuse.fuse_reply_statfs(req, statfs)
    except DiscardedRequest:
        pass

def fuse_symlink(req, target, parent_inode, name):
    '''Create a symbolic link'''

    log.debug('symlink(%d, %r, %r): start',
              parent_inode, string_at(name), string_at(target))
    with lock:
        attr = operations.symlink(parent_inode, string_at(name), string_at(target),
                                  libfuse.fuse_req_ctx(req).contents)
    entry = dict_to_entry(attr)

    log.debug('symlink(%d, %r, %r): calling reply_entry(ino=%d)',
              parent_inode, string_at(name), string_at(target), attr.id)
    try:
        libfuse.fuse_reply_entry(req, entry)
    except DiscardedRequest:
        pass


def fuse_unlink(req, parent_inode, name):
    '''Delete a file'''

    log.debug('unlink(%d, %r): start', parent_inode, string_at(name))
    with lock:
        operations.unlink(parent_inode, string_at(name))
    log.debug('unlink(%d, %r): calling reply_err',
              parent_inode, string_at(name))
    try:
        libfuse.fuse_reply_err(req, 0)
    except DiscardedRequest:
        pass

def fuse_write(req, inode, buf, size, off, fi):
    '''Write into an open file handle'''

    log.debug('write(fh=%d, off=%d, size=%d): start',
              fi.contents.fh, off, size)
    with lock:
        written = operations.write(fi.contents.fh, off, string_at(buf, size))

    log.debug('write(fh=%d, off=%d, size=%d): calling reply_write',
              fi.contents.fh, off, size)
    try:
        libfuse.fuse_reply_write(req, written)
    except DiscardedRequest:
        pass
    
# Enforce correct version
if fuse_version() < 28:
    raise SystemExit('FUSE version too old, must be 2.8 or newer!\n')    
