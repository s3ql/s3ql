'''
Interface between the C and Python API. The actual file system
is implemented as an `Operations` instance whose methods will
be called by the `Server` instance within the `mount_and_serve`
loop.

Note that all "string-like" quantities (e.g. file names, extended attribute names & values) are
represented as bytes, since POSIX doesn't require any of them to be valid unicode strings.


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
from ctypes import (c_char_p, sizeof, create_string_buffer, addressof, string_at)
from functools import partial
import logging
import errno

# Public API
__all__ = [ 'init', 'main', 'FUSEError', 'ENOATTR' ]

ENOATTR = libfuse.ENOATTR

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
        session = libfuse.fuse_lowlevel_new(args, fuse_ops, sizeof(fuse_ops), None)
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
      
        
def dict_to_entry(attr):
    '''Convert dict to fuse_entry_param'''
        
    entry = libfuse.fuse_entry_param()
    
    entry.ino = attr['st_ino']
    entry.generation = attr.pop('generation')
    entry.entry_timeout = attr.pop('entry_timeout')
    entry.attr_timeout = attr.pop('attr_timeout')
    
    entry.attr = dict_to_stat(attr)
        
    return entry
    
                   
def fuse_lookup(req, parent_inode, name):
    '''Look up a directory entry by name and get its attributes'''
    
    attr = operations.lookup(parent_inode, string_at(name))
    entry = dict_to_entry(attr)
    
    try:
        libfuse.fuse_reply_entry(req, entry)
    except DiscardedRequest:
        pass
    
    
def fuse_getattr(req, ino, _unused):
    '''Get attributes for `ino`'''
    
    attr = operations.getattr(ino)
    
    attr_timeout = attr.pop('attr_timeout')
    stat = dict_to_stat(attr)
    
    try:
        libfuse.fuse_reply_attr(req, stat, attr_timeout)
    except DiscardedRequest:
        pass

def fuse_access(req, ino, mask):
    '''Check if calling user has `mask` rights for `ino`''' 
    
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
    '''Create and open a file'''
    
    (fh, attr) = operations.create(ino_parent, string_at(name), mode)
    fi.fh = fh
    entry = dict_to_entry(attr)
    
    try:
        libfuse.fuse_reply_create(req, entry, fi)
    except DiscardedRequest:
        operations.release(fh)
    
    
def fuse_flush(req, ino, fi):
    '''Handle close() system call
    
    May be called multiple times for the same open file.
    '''
    
    operations.flush(fi.fh)
    libfuse.fuse_reply_none(req)
    
    
def fuse_fsync(req, ino, datasync, fi):
    '''Flush buffers for `ino`
    
    If the datasync parameter is non-zero, then only the user data
    is flushed (and not the meta data).
    '''
    
    operations.fsync(fi.fh, datasync != 0)
    libfuse.fuse_reply_none(req)
    
    
def fuse_fsyncdir(req, ino, datasync, fi):
    '''Synchronize directory contents
    
    If the datasync parameter is non-zero, then only the directory contents
    is flushed (and not the meta data about the directory itself).
    '''
    
    operations.fsyncdir(fi.fh, datasync != 0)
    libfuse.fuse_reply_none(req)
    
    
def fuse_getxattr(req, ino, name, size):
    '''Get an extended attribute.
    '''
    
    val = operations.getxattr(ino, string_at(name))
    if not isinstance(val, bytes):
        raise TypeError("getxattr return value must be of type bytes")
    
    try:
        if size == 0:
            libfuse.fuse_reply_xattr(req, len(val))
        elif size <= len(val):
            libfuse.fuse_reply_buf(req, val, len(val))
        else:
            raise FUSEError(errno.ERANGE)
    except DiscardedRequest:
        pass
    
    
def fuse_link(req, ino, new_parent_ino, new_name):
    '''Create a hard link'''
    
    attr = operations.link(ino, new_parent_ino, string_at(new_name))
    entry = dict_to_entry(attr)
    
    try:
        libfuse.fuse_reply_entry(req, entry)
    except DiscardedRequest:
        pass
    
def fuse_listxattr(req, inode, size):
    '''List extended attributes for `inode`'''
    
    names = operations.listxattr(inode)
    
    if not all([ isinstance(name, bytes) for name in names]):
        raise TypeError("listxattr return value must be list of bytes")
    
    # Size of the \0 separated buffer 
    act_size = (len(names)-1) + sum( [ len(name) for name in names ])
     
    if size == 0:
        try:
            libfuse.fuse_reply_xattr(req, len(names))
        except DiscardedRequest:
            pass
    
    elif act_size > size:
        raise FUSEError(errno.ERANGE)
    
    else:
        try:
            libfuse.fuse_reply_buf(req, b'\0'.join(names), act_size)
        except DiscardedRequest:
            pass
            
            
def fuse_mkdir(req, inode_parent, name, mode):
    '''Create directory'''
    
    attr = operations.mkdir(inode_parent, string_at(name), mode)
    entry = dict_to_entry(attr)
    
    try:
        libfuse.fuse_reply_entry(req, entry)
    except DiscardedRequest:
        pass

def fuse_mknod(req, inode_parent, name, mode, rdev):
    '''Create (possibly special) file'''
    
    attr = operations.mknod(inode_parent, string_at(name), mode, rdev)
    entry = dict_to_entry(attr)
    
    try:
        libfuse.fuse_reply_entry(req, entry)
    except DiscardedRequest:
        pass

def fuse_open(req, inode, fi):
    '''Open a file'''
    
    fi.fh = operations.open(inode, fi.flags)
    
    try:
        libfuse.fuse_reply_open(req, fi)
    except DiscardedRequest:
        operations.release(inode, fi.fh)

def fuse_opendir(req, inode, fi):
    '''Open a directory'''
    
    fi.fh = operations.open(inode)
    
    try:
        libfuse.fuse_reply_open(req, fi)
    except DiscardedRequest:
        operations.releasedir(inode, fi.fh)


def fuse_read(req, ino, size, off, fi):
    '''Read data from file'''
    
    data = operations.read(fi.fh, off, size)
    
    if not isinstance(data, bytes):
        raise TypeError("read() must return bytes")
    
    if len(data) > size:
        raise ValueError('read() must not return more than `size` bytes')
    
    try:
        libfuse.fuse_reply_buf(req, data, len(data))
    except DiscardedRequest:
        pass


def fuse_readlink(req, inode):
    '''Read target of symbolic link'''
    
    target = operations.readlink(inode)
    try:
        libfuse.fuse_reply_readlink(req, target)
    except DiscardedRequest:
        pass
    
    
def fuse_readdir(req, ino, bufsize, off, fi):
    '''Read directory entries'''       
        
    # Lots of variables, but no simplification possible
    #pylint: disable-msg=R0914
    
    # Collect as much entries as we can return
    entries = list()
    size = 0
    for (name, attr) in operations.readdir(fi.fh, off):
        if not isinstance(name, bytes):
            raise TypeError("readdir() must return entry names as bytes")
    
        stat = dict_to_stat(attr)
            
        entry_size = libfuse.fuse_add_direntry(req, None, 0, name, stat, 0)
        if size+entry_size > bufsize:
            break

        entries.append((name, stat))
        size += entry_size
    
    # If there are no entries left, return empty buffer
    if not entries:
        try:
            libfuse.fuse_reply_buf(req, None, 0)
        except DiscardedRequest:
            pass
        return
    
    # Create ad fill buffer
    buf = create_string_buffer(size)
    next_ = off
    addr_off = 0
    for (name, stat) in entries:
        next_ += 1
        addr_off += libfuse.fuse_add_direntry(req, addressof(buf) + addr_off, bufsize, 
                                              name, stat, next_)
    
    # Return buffer
    try:
        libfuse.fuse_reply_buf(req, buf, size)
    except DiscardedRequest:
        pass
    
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
    for (key, val) in attr.iteritems():      
        if get_timespec_key and key in  ('st_atime', 'st_mtime', 'st_ctime'):
            key = get_timespec_key(key)
            spec = libfuse.timespec()
            spec.tv_sec = int(val)
            spec.tv_nsec = int((val - int(val)) * 10**9)
            val = spec      
        setattr(stat, key, val)
   
    return stat   

    
def stat_to_dict(stat):
    '''Convert ``struct stat`` to dict'''
        
    attr = dict()
    # Yes, protected member
    #pylint: disable-msg=W0212
    for field in type(stat)._fields_:
        if field.startswith('__'):
            continue
        
        if field in ('st_atim', 'st_mtim', 'st_ctim'):
            key = field + 'e' 
            attr[key] = getattr(stat, field).tv_sec + getattr(stat, field).tv_nsec / 10**9  
        elif field in ('st_atimespec', 'st_mtimespec', 'st_ctimespec'):
            key = field[:-4]
            attr[key] = getattr(stat, field).tv_sec + getattr(stat, field).tv_nsec / 10**9     
        else:
            attr[field] = getattr(stat, field)
            
    return attr
                
                
def fuse_release(req, inode, fi):
    '''Release open file'''

    operations.release(fi.fh)
    libfuse.fuse_reply_none(req)
    

def fuse_releasedir(req, inode, fi):
    '''Release open directory'''

    operations.releasedir(fi.fh)
    libfuse.fuse_reply_none(req)
        
def fuse_removexattr(req, inode, name):
    '''Remove extended attribute'''
    
    operations.removexattr(inode, string_at(name))
    libfuse.fuse_reply_none(req)
    
def fuse_rename(req, parent_inode_old, name_old, parent_inode_new, name_new):
    '''Rename a directory entry'''
    
    operations.rename(parent_inode_old, string_at(name_old), parent_inode_new,
                      string_at(name_new))
    libfuse.fuse_reply_none(req)
    
def fuse_rmdir(req, inode_parent, name):
    '''Remove a directory'''
    
    operations.rmdir(inode_parent, string_at(name))
    libfuse.fuse_reply_none(req)
    
def fuse_setattr(req, inode, stat, to_set, fi):
    '''Change directory entry attributes'''
    
    # Make sure we know all the flags
    if (to_set & ~(libfuse.FUSE_SET_ATTR_ATIME | libfuse.FUSE_SET_ATTR_GID |
                   libfuse.FUSE_SET_ATTR_MODE | libfuse.FUSE_SET_ATTR_MTIME |
                   libfuse.FUSE_SET_ATTR_SIZE | libfuse.FUSE_SET_ATTR_SIZE |
                   libfuse.FUSE_SET_ATTR_UID)) != 0:
        raise ValueError('unknown flag')
    
    attr_all = stat_to_dict(stat)
    attr = dict()
    
    if (to_set & libfuse.FUSE_SET_ATTR_MTIME) != 0:
        attr['st_mtime'] = attr_all['st_mtime']
        
    if (to_set & libfuse.FUSE_SET_ATTR_ATIME) != 0:
        attr['st_atime'] = attr_all['st_atime']
        
    if (to_set & libfuse.FUSE_SET_ATTR_MODE) != 0:
        attr['st_mode'] = stat.st_mode
        
    if (to_set & libfuse.FUSE_SET_ATTR_UID) != 0:
        attr['st_uid'] = stat.st_uid
        
    if (to_set & libfuse.FUSE_SET_ATTR_GID) != 0:
        attr['st_gid'] = stat.st_gid            
    
    if (to_set & libfuse.FUSE_SET_ATTR_SIZE) != 0:
        attr['st_size'] = stat.st_size
    

        
    operations.setattr(inode, attr)
    libfuse.fuse_reply_none(req)   
            
def fuse_setxattr(req, inode, name, val, size, flags):
    '''Set an extended attribute'''
        
    # Make sure we know all the flags
    if (flags & ~(libfuse.XATTR_CREATE | libfuse.XATTR_REPLACE)) != 0:
        raise ValueError('unknown flag')
    
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
    
    libfuse.fuse_reply_none(req)    
    
def fuse_statfs(req, inode):
    '''Return filesystem statistics'''
    
    attr = operations.statfs()
    statfs = libfuse.statvfs()
    
    for (key, val) in attr:
        setattr(statfs, key, val)
        
    try:
        libfuse.fuse_reply_statfs(req, statfs)
    except DiscardedRequest:
        pass

def fuse_symlink(req, target, parent_inode, name):
    '''Create a symbolic link'''
    
    attr = operations.symlink(parent_inode, string_at(name), string_at(target))
    entry = dict_to_entry(attr)
    
    try:
        libfuse.fuse_reply_entry(req, entry)
    except DiscardedRequest:
        pass
      
      
def fuse_unlink(req, parent_inode, name):
    '''Delete a file'''
    
    operations.unlink(parent_inode, string_at(name))
    libfuse.fuse_reply_none(req)
    
def fuse_write(req, inode, buf, size, off, fi):
    '''Write into an open file handle'''
    
    written = operations.write(fi.fh, off, string_at(buf, size))
    
    try:
        libfuse.fuse_reply_write(req, written)
    except DiscardedRequest:
        pass
    
    
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
    
    def readdir(self, fh, off):
        '''Read directory entries
        
        This method returns an iterator over the contents of directory `fh`,
        starting at entry `off`. The iterator yields tuples of the form
        ``(name, attr)``, where ``attr` is a dict with keys corresponding to
        the elements of ``struct stat``.
         
        Iteration may be stopped as soon as enough elements have been
        retrieved and does not have to be continued until `StopIteration`
        is raised.
        '''
        
        raise FUSEError(errno.ENOSYS)
    
        
    def read(self, fh, off, size):
        '''Read `size` bytes from `fh` at position `off`
        
        Unless the file has been opened in direct_io mode or EOF is reached,
        this function  returns exactly `size` bytes. 
        '''
        
        raise FUSEError(errno.ENOSYS)
    
    def link(self, inode, new_parent_inode, new_name):
        '''Create a hard link.
    
        Returns a dict with the attributes of the newly created directory
        entry. The keys are the same as for `lookup`.
        '''
        
        raise FUSEError(errno.ENOSYS)
    
    def open(self, inode, flags):
        '''Open a file.
        
        Returns an (integer) file handle. `flags` is a bitwise or of the open flags
        described in open(2) and defined in the `os` module (with the exception of 
        ``O_CREAT``, ``O_EXCL``, ``O_NOCTTY`` and ``O_TRUNC``)
        '''
        
        raise FUSEError(errno.ENOSYS)
    
    def opendir(self, inode):
        '''Open a directory.
        
        Returns an (integer) file handle. 
        '''
        
        raise FUSEError(errno.ENOSYS)

    
    def mkdir(self, parent_inode, name, mode):
        '''Create a directory
    
        Returns a dict with the attributes of the newly created directory
        entry. The keys are the same as for `lookup`.
        '''
        
        raise FUSEError(errno.ENOSYS)

    def mknod(self, parent_inode, name, mode, rdev):
        '''Create (possibly special) file
    
        Returns a dict with the attributes of the newly created directory
        entry. The keys are the same as for `lookup`.
        '''
        
        raise FUSEError(errno.ENOSYS)

    
    def lookup(self, parent_inode, name):
        '''Look up a directory entry by name and get its attributes.
    
        Returns a dict with keys corresponding to the elements in 
        ``struct stat`` and the following additional keys:
        
        :generation: The inode generation number
        :attr_timeout: Validity timeout (in seconds) for the attributes
        :entry_timeout: Validity timeout (in seconds) for the name 
        
        Note also that the ``st_Xtime`` entries support floating point numbers 
        to allow for nano second resolution.
        
        If the entry does not exist, raises `FUSEError(errno.ENOENT)`.
        '''
        
        raise FUSEError(errno.ENOSYS)

    def listxattr(self, inode):
        '''Get list of extended attribute names'''
        
        raise FUSEError(errno.ENOSYS)
    
    def getattr(self, inode):
        '''Get attributes for `inode`
    
        Returns a dict with keys corresponding to the elements in 
        ``struct stat`` and the following additional keys:
        
        :attr_timeout: Validity timeout (in seconds) for the attributes
        
        Note also that the ``st_Xtime`` entries support floating point numbers 
        to allow for nano second resolution.
        '''
        
        raise FUSEError(errno.ENOSYS)
    
    def getxattr(self, inode, name):
        '''Return extended attribute value
        
        If the attribute does not exist, raises `FUSEError(ENOATTR)`
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
        
        ``attr`` must be a dict with keys corresponding to the elements in 
        ``struct stat`` and the following additional keys:
        
        :generation: The inode generation number
        :attr_timeout: Validity timeout (in seconds) for the attributes
        :entry_timeout: Validity timeout (in seconds) for the name 
        '''
        
        raise FUSEError(errno.ENOSYS)

    def flush(self, fh):
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
    
    def readlink(self, inode):
        '''Return target of symbolic link'''
        
        raise FUSEError(errno.ENOSYS)
    
    def release(self, inode, fh):
        '''Release open file
        
        This method must be called exactly once for each `open` call.
        '''
        
        raise FUSEError(errno.ENOSYS)
    
    def releasedir(self, inode, fh):
        '''Release open directory
        
        This method must be called exactly once for each `opendir` call.
        '''
        
        raise FUSEError(errno.ENOSYS)
    
    def removexattr(self, inode, name):
        '''Remove extended attribute'''
        
        raise FUSEError(errno.ENOSYS)
    
    def rename(self, inode_parent_old, name_old, inode_parent_new, name_new):
        '''Rename a directory entry'''
        
        raise FUSEError(errno.ENOSYS)
    
    def rmdir(self, inode_parent, name):
        '''Remove a directory'''
        
        raise FUSEError(errno.ENOSYS)
    
    def setattr(self, inode, attr):
        '''Change directory entry attributes
        
        `attr` must be a dict with keys corresponding to the attributes of 
        ``struct stat``. `attr` may also include a new value for ``st_size`` which
        means that the file should be truncated or extended.
        '''
        
        raise FUSEError(errno.ENOSYS)
    
    def setxattr(self, inode, name, value):
        '''Set an extended attribute.
        
        The attribute may or may not exist already.
        '''
        
        raise FUSEError(errno.ENOSYS)
    
    def statfs(self):
        '''Get file system statistics
        
        Returns a `dict` with keys corresponding to the attributes of 
        ``struct statfs``.
        '''
        
        raise FUSEError(errno.ENOSYS)
    
    def symlink(self, inode_parent, name, target):
        '''Create a symbolic link
        
           
        Returns a dict with the attributes of the newly created directory
        entry. The keys are the same as for `lookup`.
        '''
        
        raise FUSEError(errno.ENOSYS)
    
    def unlink(self, parent_inode, name):
        '''Remove a (possibly special) file'''
        
        raise FUSEError(errno.ENOSYS)
    
    def write(self, fh, off, data):
        '''Write data into an open file
        
        Returns the number of bytes written.
        Unless the file was opened in ``direct_io`` mode, this is always equal to
        `len(data)`. 
        '''
        
        raise FUSEError(errno.ENOSYS)