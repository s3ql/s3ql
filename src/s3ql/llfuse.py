'''
Interface between the C and Python API. The actual file system
is implemented as an `Operations` instance whose methods will
be called by the `Server` instance within the `mount_and_serve`
loop.

Exception Handling
------------------

Since Python exceptions cannot be forwarded to the FUSE kernel module,
the FUSE Python API catches all exceptions that are generated during
request processing.

If the exception is of type `FUSEError`, the appropriate errno is returned
to the kernel module and the exception is discarded.

For any other exceptions, a warning is logged and a generic error signaled
to the kernel module. Then the `handle_exc` method of the `Operations` 
instance is called, so that the file system itself has a chance to react
to the problem (e.g. by marking the file system as needing a check).

The return value and any raised exceptions of `handle_exc` are ignored.

'''

# We do want to use the global statement
#pylint: disable-msg=W0603

# The global variables are only initialized in init()
# and not at module level
#pylint: disable-msg=W0601

# Since we are using ctype Structures, we often have to
# access attributes that are not defined in __init__
# (since they are defined in _fields_ instead)
#pylint: disable-msg=W0201    


from __future__ import division
from __future__ import unicode_literals
import fuse_ctypes as libfuse
from ctypes import (string_at, byref, c_char_p, sizeof, create_string_buffer, addressof,
                    )
from functools import partial
import logging
import errno

# Public API
__all__ = [ 'init', 'main', 'FUSEError', 'struct_stat', 'struct_entry_param' ]

# Export important structures
struct_stat = libfuse.stat
struct_entry_param = libfuse.fuse_entry_param

log = logging.getLogger("fuse") 


#
# Exceptions
#

class DiscardedRequest(Exception):
    '''Request was interrupted and reply discarded.
    
    '''
    
    pass

class ReplyError(Exception):
    '''Unable to send reply to fuse kernel module.

    '''
    
    pass

class FUSEError(Exception):
    '''Wrapped errno value to be returned to the fuse kernel module

    This exception can store only an errno. Request handlers should raise
    to return a specific errno to the fuse kernel module.
    '''
    
    __slots__ = [ 'errno' ]
    
    def __init__(self, errno_):
        super(FUSEError, self).__init__()
        self.errno = errno_

    def __str__(self):
        return errno.errorcode[self.errno]


#
# Define FUSE reply function error checkers
#

def check_reply_result(result, func, *_unused_args):
    '''Check result of a call to a fuse_reply_* foreign function
    
    If `result` is 0, it is assumed that the call succeeded and the function does nothing.
    
    If result is `-errno.ENOENT`, this means that the request has been discarded and `DiscardedRequest`
    is raised.
    
    In all other cases,  `ReplyError` is raised.
    
    (We do not try to call `fuse_reply_err` or any other reply method as well, because the first reply
    function may have already invalidated the `req` object and it seems better to (possibly) let the
    request pend than to crash the server application.)
    '''
    
    if result == 0:
        return None
    
    elif result == -errno.ENOENT:
        raise DiscardedRequest()
    
    elif result > 0:
        raise ReplyError('Foreign function %s returned unexpected value %d'
                           % (func.__name__, result))    
    elif result < 0:
        raise ReplyError('Foreign function %s returned error %s'
                  % (func.__name__, errno.errorcode[-result]))
    
    
#
# Set return checker for common ctypes calls
#  
reply_functions = [ 'fuse_reply_err', 'fuse_reply_none', 'fuse_reply_entry',
                   'fuse_reply_create', 'fuse_reply_readlink', 'fuse_reply_open',
                   'fuse_reply_write', 'fuse_reply_attr', 'fuse_reply_buf',
                   'fuse_reply_iov', 'fuse_reply_statfs', 'fuse_reply_xattr',
                   'fuse_reply_lock', 'fuse_reply_bmap' ]
for fname in reply_functions:
    getattr(libfuse, fname).errcheck = check_reply_result
       

def op_wrapper(func, req, *args):
    '''Catch all exceptions and call fuse_reply_err instead
    '''
    try:
        func(req, *args)
    except FUSEError as e:
        libfuse.fuse_reply_err(req, -e.errno)
    except Exception as exc:
        log.exception('FUSE handler raised exception.')
        
        # Report error to filesystem
        if hasattr(operations, 'handle_exc'):
            try:
                operations.handle_exc(exc)
            except:
                pass
        
        # Report error to filesystem
        if hasattr(operations, 'handle_exc'):
            try:
                operations.handle_exc(exc)
            except:
                pass
            
        # Send error reply, unless the error occured when replying
        if not isinstance(exc, ReplyError):
            libfuse.fuse_reply_err(req, -errno.EFAULT)
            
    return None


def init(operations_):
    '''Initialize FUSE file system.
    
    `operations_` has to be an instance of the `Operations` class (or another
    class defining the same methods).
    '''
    
    global operations
    global fuse_ops

    operations = operations_
    fuse_ops = libfuse.fuse_lowlevel_ops()

    # Access to protected member _fields_ is alright
    #pylint: disable-msg=W0212    
    module = globals()
    for name, prototype in libfuse.fuse_lowlevel_ops._fields_:
        name = "fuse_" + name
        if hasattr(operations, name):
            method = partial(op_wrapper, module[name])
            setattr(fuse_ops, name, prototype(method))
            
def main(mountpoint, args):
    '''Mount file system and start event loop
    
    `args` has to be a list of strings. Valid options are listed in struct fuse_opt fuse_mount_opts[]
    (fuse_mount.c:68) and struct fuse_opt fuse_ll_opts[] (fuse_lowlevel_c:1526)
    '''
                   
    # Init fuse_args struct
    fuse_args = libfuse.fuse_args()
    fuse_args.allocated = 0
    fuse_args.argc = len(args)
    fuse_args.argv = (c_char_p * len(args))(*args)
    
    channel = libfuse.fuse_mount(mountpoint, args)
    try:
        session = libfuse.fuse_lowlevel_new(args, byref(fuse_ops), sizeof(fuse_ops), None)
        if session is None:
            raise RuntimeError("fuse_lowlevel_new() failed")
        try:
            if libfuse.fuse_set_signal_handlers(session) == -1:
                raise RuntimeError("fuse_set_signal_handlers() failed")
            try:
                libfuse.fuse_session_add_chan(session, channel)
                try:
                    if libfuse.fuse_session_loop_mt(session) != 0:
                        raise RuntimeError("fuse_session_loop() failed")
                finally:
                    libfuse.fuse_session_remove_chan(channel)
            finally:
                libfuse.fuse_remove_signal_handlers(session)
                
        finally:
            libfuse.fuse_session_destroy(session)
    finally:
        libfuse.fuse_unmount(mountpoint, channel)


        
               
def fuse_lookup(req, parent_inode, name):
    '''Look up a directory entry by name and get its attributes
    
    '''
    
    attr = operations.lookup(parent_inode, name)
    
    ret = libfuse.fuse_entry_param()
    ret.attr = libfuse.stat()
    
    ret.ino = attr['st_ino']
    ret.generation = attr.pop('generation')
    ret.entry_timeout = attr.pop('entry_timeout')
    ret.attr_timeout = attr.pop('attr_timeout')
    
    # Raises exception if there are keys in attr that
    # can't be stored in struct stat.
    for (key, val) in attr.iteritems():
        setattr(ret.attr, key, val)
    
    try:
        libfuse.fuse_reply_entry(req, byref(ret))
    except DiscardedRequest:
        pass
    
    
def fuse_getattr(req, ino, _unused):
    '''Get attributes for `ino`.
    
    '''
    attr = operations.getattr(ino)
    
    attr_timeout = attr.pop('attr_timeout')
    ret = libfuse.stat()
    
    # Raises exception if there are keys in attr that
    # can't be stored in struct stat.
    for (key, val) in attr.iteritems():
        setattr(ret, key, val)
    
    try:
        libfuse.fuse_reply_attr(req, byref(ret), attr_timeout)
    except DiscardedRequest:
        pass

def fuse_access(req, ino, mask):
    '''Check if calling user has `mask` rights for `ino`
    
    ''' 
    
    # Get UID
    ctx = libfuse.fuse_req_ctx(req)
    
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
        
    ret = operations.access(ino, mask, ctx.uid, ctx.gid, get_gids)
    try:
        if ret:
            libfuse.fuse_reply_err(req, 0)
        else:
            libfuse.fuse_reply_err(req, errno.EPERM)
    except DiscardedRequest:
        pass

def fuse_create(req, ino_parent, name, mode, fi):
    '''Create and open a file
    
    '''
    
    (fh, attr) = operations.create(ino_parent, name, mode)
    fi.fh = fh

    ret = libfuse.fuse_entry_param()
    ret.attr = libfuse.stat()
    
    ret.ino = attr['st_ino']
    ret.generation = attr.pop('generation')
    ret.entry_timeout = attr.pop('entry_timeout')
    ret.attr_timeout = attr.pop('attr_timeout')
    
    # Raises exception if there are keys in attr that
    # can't be stored in struct stat.
    for (key, val) in attr.iteritems():
        setattr(ret.attr, key, val)
    
    try:
        libfuse.fuse_reply_create(req, byref(ret), byref(fi))
    except DiscardedRequest:
        operations.release(fh)
    
def fuse_flush(req, ino, fi):
    '''Handle close() system call
    
    May be called multiple times for the same open file.
    '''
    
    operations.flush(ino, fi.fh)
    libfuse.fuse_reply_none(req)
    
def fuse_fsync(req, ino, datasync, fi):
    '''Flush buffers for `ino`
    
    If the datasync parameter is non-zero, then only the user data
    is flushed (and not the meta data).
    '''
    
    operations.fsync(ino, datasync != 0, fi.fh)
    libfuse.fuse_reply_none(req)
    
def fuse_fsyncdir(req, ino, datasync, fi):
    '''Synchronize directory contents
    
    If the datasync parameter is non-zero, then only the directory contents
    is flushed (and not the meta data about the directory itself).
    '''
    
    operations.fsyncdir(ino, datasync != 0, fi.fh)
    libfuse.fuse_reply_none(req)
    
def fuse_getxattr(req, ino, name, size):
    '''Get an extended attribute.
    '''
    
    val = operations.getxattr(ino, name)
    if not isinstance(val, bytes):
        raise TypeError("getxattr return value must be of type bytes")
    
    try:
        if size.value == 0:
            libfuse.fuse_reply_xattr(req, len(val))
        elif size.value <= len(val):
            libfuse.fuse_reply_buf(req, byref(val), len(val))
        else:
            raise FUSEError(errno.ERANGE)
    except DiscardedRequest:
        pass
    
def fuse_open(req, ino, fi):
    fi.contents.fh = operations.open(ino, fi.contents.flags)
    libfuse.fuse_reply_open(req, fi)

def fuse_read(req, ino, size, off, fi):
    ret = operations.read(ino, size, off, fi.contents.fh)
    return libfuse.fuse_reply_buf(req, ret, len(ret))

def fuse_readdir(req, ino, size, off, _unused_fi):
    ret = operations.readdir(ino)
    bufsize = sum(libfuse.fuse_add_direntry(req, None, 0, name, None, 0) for name, ino in ret)
    buf = create_string_buffer(bufsize)
    next_ = 0
    for name, ino in ret:
        addr = addressof(buf) + next
        dirsize = libfuse.fuse_add_direntry(req, None, 0, name, None, 0)
        next_ += dirsize
        attr = libfuse.stat(st_ino=ino)
        libfuse.fuse_add_direntry(req, addr, dirsize, name, byref(attr), next_)
    if off < bufsize:
        libfuse.fuse_reply_buf(req, addressof(buf) + off, min(bufsize - off, size))
    else:
        libfuse.fuse_reply_buf(req, None, 0)


class Operations(object):
    '''
    This is a dummy class that just documents the possible methods that
    a file system may declare.
    '''
    
    # This is a dummy class, so all the methods could of course
    # be functions
    #pylint: disable-msg=R0201
    
    def handle_exc(self, exc):
        '''Handle exceptions that occured during request processing. 
        
        This method returns nothing and does not raise any exceptions itself.
        '''
        
        pass
    
    def lookup(self, parent_inode, name):
        '''Look up a directory entry by name and get its attributes.
    
        Returns a hash with keys corresponding to the elements in 
        ``struct stat`` and the following additional keys:
        
        :generation: The inode generation number
        :attr_timeout: Validity timeout (in seconds) for the attributes
        :entry_timeout: Validity timeout (in seconds) for the name 
        
        If the entry does not exist, raises `FUSEError(errno.ENOENT)`.
        '''
        
        raise FUSEError(errno.ENOSYS)
    
    def getattr(self, inode):
        '''Get attributes for `inode`
    
        Returns a hash with keys corresponding to the elements in 
        ``struct stat`` and the following additional keys:
        
        :attr_timeout: Validity timeout (in seconds) for the attributes
        '''
        
        raise FUSEError(errno.ENOSYS)
    
    def access(self, inode, uid, gid, mode, get_sup_gids):
        '''Check if `uid` has `mode` rights in `inode`. 
        
        Returns a boolean value. `get_sup_gids` must be a function that
        returns a list of the supplementary group ids of `uid`.
        '''
        
        raise FUSEError(errno.ENOSYS)
    
    def create(self, inode_parent, name, mode):
        '''Create a file and open it
    
        Returns a tuple of the form ``(fh, attr)``. `fh` should be an
        integer file handle that is used to identify the open file.
        
        ``attr`` must be a hash with keys corresponding to the elements in 
        ``struct stat`` and the following additional keys:
        
        :generation: The inode generation number
        :attr_timeout: Validity timeout (in seconds) for the attributes
        :entry_timeout: Validity timeout (in seconds) for the name 
        '''
        
        raise FUSEError(errno.ENOSYS)

    def flush(self, inode, fh):
        '''Handle close() syscall.
        
        May be called multiple times for the same open file (e.g. if the file handle
        has been duplicated).
                                                             
        If the filesystem supports file locking operations, all locks belonging
        to the file handle's owner are cleared. 
        '''
        
        raise FUSEError(errno.ENOSYS)
    
    def fsync(self, inode, datasync, fh):
        '''Flush buffers for `inode`
        
        If `datasync` is true, only the user data is flushed (and no meta data). 
        '''
        
        raise FUSEError(errno.ENOSYS)