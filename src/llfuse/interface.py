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

# Since we are using ctype Structures, we often have to
# access attributes that are not defined in __init__
# (since they are defined in _fields_ instead)
#pylint: disable-msg=W0212


from __future__ import division
from __future__ import unicode_literals
import fuse_ctypes as libfuse
from ctypes import (c_char_p, sizeof, create_string_buffer, addressof, string_at,
                    POINTER, c_char, cast, pointer, c_void_p, CFUNCTYPE )
from functools import partial
import logging
import errno
import sys

# Public API
__all__ = [ 'init', 'main', 'FUSEError' ]

# These should really be befined in the errno module, but
# unfortunately they are missing
errno.ENOATTR = libfuse.ENOATTR
errno.ENOTSUP = libfuse.ENOTSUP

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
        # errno may not have strings for all error codes
        return errno.errorcode.get(self.errno, str(self.errno))


#
# Define FUSE reply function error checkers
#

def check_reply_result(result, func, *args):
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
                         % (func.name, result))    
    elif result < 0:
        raise ReplyError('Foreign function %s returned error %s'
                         % (func.name, errno.errorcode.get(-result, str(-result))))
    
    
#
# Set return checker for common ctypes calls
#  
reply_functions = [ 'fuse_reply_err', 'fuse_reply_entry',
                   'fuse_reply_create', 'fuse_reply_readlink', 'fuse_reply_open',
                   'fuse_reply_write', 'fuse_reply_attr', 'fuse_reply_buf',
                   'fuse_reply_iov', 'fuse_reply_statfs', 'fuse_reply_xattr',
                   'fuse_reply_lock' ]
for fname in reply_functions:
    getattr(libfuse, fname).errcheck = check_reply_result
    
    # Name isn't stored by ctypes
    getattr(libfuse, fname).name = fname
 

# This wrapper makes sure that there is only one instance
# of the _interface class and that it is destroyed 
# after the main loop exits.
def main(operations, mountpoint, args, single=False, foreground=False):
    '''Mount FUSE file system and run main loop.
       
    `operations_` has to be an instance of the `Operations` class (or another
    class defining the same methods).
       
    `args` has to be a list of strings. Valid options are listed in struct fuse_opt fuse_mount_opts[]
    (mount.c:68) and struct fuse_opt fuse_ll_opts[] (fuse_lowlevel_c:1526)
    '''
    
    _interface(operations, mountpoint, args, single, foreground)
    
    return
    
def dict_to_entry(attr):
    '''Convert dict to fuse_entry_param'''
        
    entry = libfuse.fuse_entry_param()
    
    entry.ino = attr['st_ino']
    entry.generation = attr.pop('generation')
    entry.entry_timeout = attr.pop('entry_timeout')
    entry.attr_timeout = attr.pop('attr_timeout')
    
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
            
class _interface(object):
    '''
    This class must be treated as a singleton, because every instance
    modifies the state of the same FUSE C library.
    
    '''
    
    __slots__ = [ 'operations', 'fuse_ops' ]
        
    def op_wrapper(self, func, req, *args):
        '''Catch all exceptions and call fuse_reply_err instead'''
            
        try:
            func(req, *args)
        except FUSEError as e:
            log.debug('op_wrapper caught FUSEError, calling fuse_reply_err(%s)', 
                      errno.errorcode.get(e.errno, str(e.errno)))
            libfuse.fuse_reply_err(req, e.errno)
        except Exception as exc:
            log.exception('FUSE handler raised exception.')
            
            # Report error to filesystem
            if hasattr(self.operations, 'handle_exc'):
                try:
                    self.operations.handle_exc(exc)
                except:
                    pass
                
            # Send error reply, unless the error occured when replying
            if not isinstance(exc, ReplyError):
                libfuse.fuse_reply_err(req, errno.EIO)
    
    
    def __init__(self, operations, mountpoint, args, foreground=False, single=False):
        '''Initialize FUSE file system and run main loop
                
        `operations_` has to be an instance of the `Operations` class (or another
        class defining the same methods).
        
        `args` has to be a list of strings. Valid options are listed in struct fuse_opt fuse_mount_opts[]
        (mount.c:68) and struct fuse_opt fuse_ll_opts[] (fuse_lowlevel_c:1526).
        
        If `foreground` is true, the process is not daemonized.
        '''
        
        # Too many branches, too many statements, too many variables.
        # But this function can't be split up any more
        #pylint: disable-msg=R0912,R0915,R0914
        
        log.debug('Initializing llfuse')
    
        self.operations = operations
        fuse_ops = libfuse.fuse_lowlevel_ops()
    
        for (name, prototype) in libfuse.fuse_lowlevel_ops._fields_:
            method = partial(self.op_wrapper, getattr(self, 'fuse_' + name))
            setattr(fuse_ops, name, prototype(method))
        
        # Give operations instance a chance to check and change
        # the FUSE options
        operations.check_args(args)
        
        # Construct commandline
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
                    try:
                        if single:
                            log.debug('Calling fuse_session_loop')
                            if libfuse.fuse_session_loop(session) != 0:
                                raise RuntimeError("fuse_session_loop() failed")
                        else:
                            log.debug('Calling fuse_session_loop_mt')
                            if libfuse.fuse_session_loop_mt(session) != 0:
                                raise RuntimeError("fuse_session_loop_mt() failed")
                    finally:
                        log.debug('Calling fuse_session_remove_chan')
                        libfuse.fuse_session_remove_chan(channel)
                finally:
                    log.debug('Calling fuse_remove_signal_handlers')
                    libfuse.fuse_remove_signal_handlers(session)
                    
            finally:
                log.debug('Calling fuse_session_destroy')
                libfuse.fuse_session_destroy(session)
        finally:
            log.debug('Calling fuse_unmount')
            libfuse.fuse_unmount(mountpoint, channel)
          
                       
    def fuse_lookup(self, req, parent_inode, name):
        '''Look up a directory entry by name and get its attributes'''
        
        log.debug('Handling lookup(%d, %s)', parent_inode, string_at(name))
        
        attr = self.operations.lookup(parent_inode, string_at(name))
        entry = dict_to_entry(attr)
        
        log.debug('Calling fuse_reply_entry')
        try:
            libfuse.fuse_reply_entry(req, entry)
        except DiscardedRequest:
            pass
        
        
    def fuse_getattr(self, req, ino, _unused):
        '''Get attributes for `ino`'''
        
        log.debug('Handling getattr(%d)', ino)
        
        attr = self.operations.getattr(ino)
        
        attr_timeout = attr.pop('attr_timeout')
        stat = dict_to_stat(attr)
        
        log.debug('Calling fuse_reply_attr')
        try:
            libfuse.fuse_reply_attr(req, stat, attr_timeout)
        except DiscardedRequest:
            pass
    
    def fuse_access(self, req, ino, mask):
        '''Check if calling user has `mask` rights for `ino`''' 
        
        log.debug('Handling access(%d, %o)', ino, mask)
        
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
            
        ret = self.operations.access(ino, mask, ctx, get_gids)
        
        log.debug('Calling fuse_reply_err')
        try:
            if ret:
                libfuse.fuse_reply_err(req, 0)
            else:
                libfuse.fuse_reply_err(req, errno.EPERM)
        except DiscardedRequest:
            pass
    
    
    def fuse_create(self, req, ino_parent, name, mode, fi):
        '''Create and open a file'''
        
        log.debug('Handling create(%d, %s, %o)', ino_parent, string_at(name), mode)
        (fh, attr) = self.operations.create(ino_parent, string_at(name), mode, 
                                       libfuse.fuse_req_ctx(req))
        fi.contents.fh = fh
        entry = dict_to_entry(attr)
        
        log.debug('Calling fuse_reply_create')
        try:
            libfuse.fuse_reply_create(req, entry, fi)
        except DiscardedRequest:
            self.operations.release(fh)
        
        
    def fuse_flush(self, req, ino, fi):
        '''Handle close() system call
        
        May be called multiple times for the same open file.
        '''
        
        log.debug('Handling flush(%d)', fi.contents.fh)
        self.operations.flush(fi.contents.fh)
        log.debug('Calling fuse_reply_none')
        libfuse.fuse_reply_none(req)
        
        
    def fuse_fsync(self, req, ino, datasync, fi):
        '''Flush buffers for `ino`
        
        If the datasync parameter is non-zero, then only the user data
        is flushed (and not the meta data).
        '''
        
        log.debug('Handling fsync(%d, %s)', fi.contents.fh, datasync != 0)
        self.operations.fsync(fi.contents.fh, datasync != 0)
        log.debug('Calling fuse_reply_none')
        libfuse.fuse_reply_none(req)
        
        
    def fuse_fsyncdir(self, req, ino, datasync, fi):
        '''Synchronize directory contents
        
        If the datasync parameter is non-zero, then only the directory contents
        are flushed (and not the meta data about the directory itself).
        '''
        
        log.debug('Handling fsyncdir(%d, %s)', fi.contents.fh, datasync != 0)   
        self.operations.fsyncdir(fi.contents.fh, datasync != 0) 
        log.debug('Calling fuse_reply_none')
        libfuse.fuse_reply_none(req)
        
        
    def fuse_getxattr(self, req, ino, name, size):
        '''Get an extended attribute.
        '''
        
        log.debug('Handling getxattr(%d, %s)', ino, string_at(name))
        val = self.operations.getxattr(ino, string_at(name))
        if not isinstance(val, bytes):
            raise TypeError("getxattr return value must be of type bytes")
        
        try:
            if size == 0:
                log.debug('Calling fuse_reply_xattr')
                libfuse.fuse_reply_xattr(req, len(val))
            elif size <= len(val):
                log.debug('Calling fuse_reply_buf')
                libfuse.fuse_reply_buf(req, val, len(val))
            else:
                raise FUSEError(errno.ERANGE)
        except DiscardedRequest:
            pass
        
        
    def fuse_link(self, req, ino, new_parent_ino, new_name):
        '''Create a hard link'''
        
        log.debug('Handling fuse_link(%d, %d, %s)', ino, new_parent_ino, string_at(new_name))
        attr = self.operations.link(ino, new_parent_ino, string_at(new_name))
        entry = dict_to_entry(attr)
        
        log.debug('Calling fuse_reply_entry')
        try:
            libfuse.fuse_reply_entry(req, entry)
        except DiscardedRequest:
            pass
        
    def fuse_listxattr(self, req, inode, size):
        '''List extended attributes for `inode`'''
        
        log.debug('Handling listxattr(%d)', inode)
        names = self.operations.listxattr(inode)
        
        if not all([ isinstance(name, bytes) for name in names]):
            raise TypeError("listxattr return value must be list of bytes")
        
        # Size of the \0 separated buffer 
        act_size = (len(names)-1) + sum( [ len(name) for name in names ])
         
        if size == 0:
            try:
                log.debug('Calling fuse_reply_xattr')
                libfuse.fuse_reply_xattr(req, len(names))
            except DiscardedRequest:
                pass
        
        elif act_size > size:
            raise FUSEError(errno.ERANGE)
        
        else:
            try:
                log.debug('Calling fuse_reply_buf')
                libfuse.fuse_reply_buf(req, b'\0'.join(names), act_size)
            except DiscardedRequest:
                pass
                
                
    def fuse_mkdir(self, req, inode_parent, name, mode):
        '''Create directory'''
        
        log.debug('Handling mkdir(%d, %s, %o)', inode_parent, string_at(name), mode)
        attr = self.operations.mkdir(inode_parent, string_at(name), mode,
                                libfuse.fuse_req_ctx(req))
        entry = dict_to_entry(attr)
        
        log.debug('Calling fuse_reply_entry')
        try:
            libfuse.fuse_reply_entry(req, entry)
        except DiscardedRequest:
            pass
    
    def fuse_mknod(self, req, inode_parent, name, mode, rdev):
        '''Create (possibly special) file'''
        
        log.debug('Handling mknod(%d, %s, %o, %d)', inode_parent, string_at(name),
                  mode, rdev)
        attr = self.operations.mknod(inode_parent, string_at(name), mode, rdev,
                                libfuse.fuse_req_ctx(req))
        entry = dict_to_entry(attr)
        
        log.debug('Calling fuse_reply_entry')
        try:
            libfuse.fuse_reply_entry(req, entry)
        except DiscardedRequest:
            pass
    
    def fuse_open(self, req, inode, fi):
        '''Open a file'''
        log.debug('Handling open(%d, %d)', inode, fi.contents.flags)
        fi.contents.fh = self.operations.open(inode, fi.contents.flags)
        
        log.debug('Calling fuse_reply_open')
        try:
            libfuse.fuse_reply_open(req, fi)
        except DiscardedRequest:
            self.operations.release(inode, fi.contents.fh)
    
    def fuse_opendir(self, req, inode, fi):
        '''Open a directory'''
        
        log.debug('Handling opendir(%d)', inode)
        fi.contents.fh = self.operations.opendir(inode)
    
        log.debug('Calling fuse_reply_open')
        try:
            libfuse.fuse_reply_open(req, fi)
        except DiscardedRequest:
            self.operations.releasedir(inode, fi.contents.fh)
    
    
    def fuse_read(self, req, ino, size, off, fi):
        '''Read data from file'''
        
        log.debug('Handling read(%d, %d, %d)', fi.contents.fh, off, size)
        data = self.operations.read(fi.contents.fh, off, size)
        
        if not isinstance(data, bytes):
            raise TypeError("read() must return bytes")
        
        if len(data) > size:
            raise ValueError('read() must not return more than `size` bytes')
        
        log.debug('Calling fuse_reply_buf')
        try:
            libfuse.fuse_reply_buf(req, data, len(data))
        except DiscardedRequest:
            pass
    
    
    def fuse_readlink(self, req, inode):
        '''Read target of symbolic link'''
        
        log.debug('Handling readlink(%d)', inode)
        target = self.operations.readlink(inode)
        log.debug('Calling fuse_reply_readlink')
        try:
            libfuse.fuse_reply_readlink(req, target)
        except DiscardedRequest:
            pass
        
        
    def fuse_readdir(self, req, ino, bufsize, off, fi):
        '''Read directory entries'''       
            
        # Lots of variables, but no simplification possible
        #pylint: disable-msg=R0914
        
        log.debug('Handling readdir(%d, %d, %d, %d)', ino, bufsize, off, fi.contents.fh)
        
        # Collect as much entries as we can return
        entries = list()
        size = 0
        for (name, attr) in self.operations.readdir(fi.contents.fh, off):
            if not isinstance(name, bytes):
                raise TypeError("readdir() must return entry names as bytes")
        
            stat = dict_to_stat(attr)
                
            entry_size = libfuse.fuse_add_direntry(req, None, 0, name, stat, 0)
            if size+entry_size > bufsize:
                break
    
            entries.append((name, stat))
            size += entry_size
        
        log.debug('Gathered %d entries, total size %d', len(entries), size)
        
        # If there are no entries left, return empty buffer
        if not entries:
            try:
                log.debug('Calling fuse_reply_buf')
                libfuse.fuse_reply_buf(req, None, 0)
            except DiscardedRequest:
                pass
            return
        
        # Create and fill buffer
        log.debug('Adding entries to buffer')
        buf = create_string_buffer(size)
        next_ = off
        addr_off = 0
        for (name, stat) in entries:
            next_ += 1
            addr_off += libfuse.fuse_add_direntry(req, addressof(buf) + addr_off, bufsize, 
                                                  name, stat, next_)
        
        # Return buffer
        log.debug('Calling fuse_reply_buf')
        try:
            libfuse.fuse_reply_buf(req, buf, size)
        except DiscardedRequest:
            pass
                    
                    
    def fuse_release(self, req, inode, fi):
        '''Release open file'''
    
        log.debug('Handling release(%d)', fi.contents.fh)
        self.operations.release(fi.contents.fh)
        log.debug('Calling fuse_reply_none')
        libfuse.fuse_reply_none(req)
        
    
    def fuse_releasedir(self, req, inode, fi):
        '''Release open directory'''
    
        log.debug('Handling releasedir(%d)', fi.contents.fh)
        self.operations.releasedir(fi.contents.fh)
        log.debug('Calling fuse_reply_none')
        libfuse.fuse_reply_none(req)
            
    def fuse_removexattr(self, req, inode, name):
        '''Remove extended attribute'''
        
        log.debug('Handling removexattr(%d, %s)', inode, string_at(name))
        self.operations.removexattr(inode, string_at(name))
        log.debug('Calling fuse_reply_none')
        libfuse.fuse_reply_none(req)
        
    def fuse_rename(self, req, parent_inode_old, name_old, parent_inode_new, name_new):
        '''Rename a directory entry'''
        
        self.operations.rename(parent_inode_old, string_at(name_old), parent_inode_new,
                          string_at(name_new))
        log.debug('Calling fuse_reply_none')
        libfuse.fuse_reply_none(req)
        
    def fuse_rmdir(self, req, inode_parent, name):
        '''Remove a directory'''
        
        self.operations.rmdir(inode_parent, string_at(name))
        log.debug('Calling fuse_reply_none')
        libfuse.fuse_reply_none(req)
        
    def fuse_setattr(self, req, inode, stat, to_set, fi):
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
            
        self.operations.setattr(inode, attr)
        log.debug('Calling fuse_reply_none')
        libfuse.fuse_reply_none(req)   
                
    def fuse_setxattr(self, req, inode, name, val, size, flags):
        '''Set an extended attribute'''
            
        log.debug('Handling setxattr(%d, %r, %r, %d)', inode, string_at(name), 
                  string_at(val, size), flags)
        
        # Make sure we know all the flags
        if (flags & ~(libfuse.XATTR_CREATE | libfuse.XATTR_REPLACE)) != 0:
            raise ValueError('unknown flag')
        
        if (flags & libfuse.XATTR_CREATE) != 0:
            try:
                self.operations.getxattr(inode, string_at(name))
            except FUSEError as e:
                if e.errno == errno.ENOATTR:
                    pass
                raise
            else:
                raise FUSEError(errno.EEXIST)
        elif (flags & libfuse.XATTR_REPLACE) != 0:
            # Exception can be passed on if the attribute does not exist
            self.operations.getxattr(inode, string_at(name))
        
        self.operations.setxattr(inode, string_at(name), string_at(val, size))
        
        log.debug('Calling fuse_reply_none')
        libfuse.fuse_reply_none(req)    
        
    def fuse_statfs(self, req, inode):
        '''Return filesystem statistics'''
        
        log.debug('Handling statfs(%d)', inode)
        attr = self.operations.statfs()
        statfs = libfuse.statvfs()
        
        for (key, val) in attr:
            setattr(statfs, key, val)
            
        log.debug('Calling fuse_reply_statfs')
        try:
            libfuse.fuse_reply_statfs(req, statfs)
        except DiscardedRequest:
            pass
    
    def fuse_symlink(self, req, target, parent_inode, name):
        '''Create a symbolic link'''
        
        log.debug('Handling symlink(%d, %r, %r)', parent_inode, string_at(name), string_at(target))
        attr = self.operations.symlink(parent_inode, string_at(name), string_at(target),
                                  libfuse.fuse_req_ctx(req))
        entry = dict_to_entry(attr)
        
        log.debug('Calling fuse_reply_entry')
        try:
            libfuse.fuse_reply_entry(req, entry)
        except DiscardedRequest:
            pass
          
          
    def fuse_unlink(self, req, parent_inode, name):
        '''Delete a file'''
        
        log.debug('Handling unlink(%d, %r)', parent_inode, string_at(name))
        self.operations.unlink(parent_inode, string_at(name))
        log.debug('Calling fuse_reply_none')
        libfuse.fuse_reply_none(req)
        
    def fuse_write(self, req, inode, buf, size, off, fi):
        '''Write into an open file handle'''
        
        written = self.operations.write(fi.contents.fh, off, string_at(buf, size))
        
        log.debug('Calling fuse_reply_write')
        try:
            libfuse.fuse_reply_write(req, written)
        except DiscardedRequest:
            pass