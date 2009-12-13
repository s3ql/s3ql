'''
Interface between the C and Python API. The actual file system
is implemented as an `Operations` instance whose methods will
be called by the `Server` instance within the `mount_and_serve`
loop.
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
from unix_types import (c_flock_t, c_stat_t, c_mode_t, c_dev_t, c_off_t,
                        c_iovec_t, c_statfs_t)
from ctypes import (c_long, Structure,  c_byte, c_char_p, c_int,  
                    c_size_t,  c_ulong,  c_uint, string_at,
                    CDLL, CFUNCTYPE, POINTER, c_voidp, c_uint64, 
                     sizeof, create_string_buffer, c_double, byref, addressof)
from ctypes.util import find_library
from functools import partial
import logging
import errno

__all__ = [ 'init', 'main', 'FUSEError' ]

libfuse = CDLL(find_library("fuse"))

# Check FUSE version
libfuse.fuse_version.restype = c_int
fuse_version = libfuse.fuse_version()
fuse_major_version = fuse_version // 10
fuse_minor_version = fuse_version - fuse_major_version * 10

if fuse_major_version != 2:
    raise StandardError("Incompatible FUSE library installed. Reported major "
                        "version: %d, required major version: 2" % fuse_major_version)

log = logging.getLogger("fuse") 


#
# Define FUSE C data types
#

c_fuse_ino_t = c_ulong
c_fuse_req_t = c_voidp
      
class c_fuse_file_info_t(Structure):
    _fields_ = [
        ('flags', c_int),
        ('fh_old', c_ulong),
        ('writepage', c_int),
        ('direct_io', c_uint, 1),
        ('keep_cache', c_uint, 1),
        ('flush', c_uint, 1),
        ('padding', c_uint, 29),
        ('fh', c_uint64),
        ('lock_owner', c_uint64)
    ]
    

class c_fuse_entry_param_t(Structure):
    _fields_ = [
        ('ino', c_fuse_ino_t),
        ('generation', c_ulong),
        ('attr', c_stat_t),
        ('attr_timeout', c_double),
        ('entry_timeout', c_double)
    ]

class c_fuse_args_t(Structure):
    _fields_ = [
                ('argc', c_int),
                ('argv', POINTER(c_char_p)),
                ('allocated', c_int)
                ]

class c_fuse_lowlevel_ops_t(Structure):
    _fields_ = [
                ('init', CFUNCTYPE(c_voidp, c_voidp, c_voidp)),
                ('destroy', CFUNCTYPE(c_voidp, c_voidp)),
                ('lookup', CFUNCTYPE(c_voidp, c_fuse_req_t, c_fuse_ino_t, c_char_p)),
                ('forget', CFUNCTYPE(c_voidp, c_fuse_req_t, c_fuse_ino_t, c_ulong)),
                ('getattr', CFUNCTYPE(c_voidp, c_fuse_req_t, c_fuse_ino_t, 
                                      POINTER(c_fuse_file_info_t))),
                ('setattr', CFUNCTYPE(c_voidp, c_fuse_req_t, c_fuse_ino_t, c_stat_t, c_int,
                                      POINTER(c_fuse_file_info_t))),
                ('readlink', CFUNCTYPE(c_voidp, c_fuse_req_t, c_fuse_ino_t)),
                ('mknod', CFUNCTYPE(c_voidp, c_fuse_req_t, c_fuse_ino_t, c_char_p, 
                                    c_mode_t, c_dev_t)),
                ('mkdir', CFUNCTYPE(c_voidp, c_fuse_req_t, c_fuse_ino_t, c_char_p, c_mode_t)),
                ('unlink', CFUNCTYPE(c_voidp, c_fuse_req_t, c_fuse_ino_t, c_char_p)),
                ('rmdir', CFUNCTYPE(c_voidp, c_fuse_req_t, c_fuse_ino_t, c_char_p)),
                ('symlink', CFUNCTYPE(c_voidp, c_fuse_req_t, c_char_p, c_fuse_ino_t, c_char_p)),
                ('rename', CFUNCTYPE(c_voidp, c_fuse_req_t, c_fuse_ino_t, c_char_p, c_fuse_ino_t,
                                     c_char_p)),
                ('link', CFUNCTYPE(c_voidp, c_fuse_req_t, c_fuse_ino_t, c_fuse_ino_t, c_char_p)),
                ('open', CFUNCTYPE(c_voidp, c_fuse_req_t, c_fuse_ino_t, 
                                   POINTER(c_fuse_file_info_t))),
                ('read', CFUNCTYPE(c_voidp, c_fuse_req_t, c_fuse_ino_t, c_size_t, c_off_t, 
                                   POINTER(c_fuse_file_info_t))),
                ('write', CFUNCTYPE(c_voidp, c_fuse_req_t, c_fuse_ino_t, POINTER(c_byte), c_size_t, 
                                    c_off_t,
                                    POINTER(c_fuse_file_info_t))),
                ('flush', CFUNCTYPE(c_voidp, c_fuse_req_t, c_fuse_ino_t, 
                                    POINTER(c_fuse_file_info_t))),
                ('release', CFUNCTYPE(c_voidp, c_fuse_req_t, c_fuse_ino_t, 
                                      POINTER(c_fuse_file_info_t))),
                ('fsync', CFUNCTYPE(c_voidp, c_fuse_req_t, c_fuse_ino_t, c_int, 
                                    POINTER(c_fuse_file_info_t))),
                ('opendir', CFUNCTYPE(c_voidp, c_fuse_req_t, c_fuse_ino_t, 
                                      POINTER(c_fuse_file_info_t))),
                ('readdir', CFUNCTYPE(c_voidp, c_fuse_req_t, c_fuse_ino_t, c_size_t, c_long, 
                                      POINTER(c_fuse_file_info_t))),
                ('releasedir', CFUNCTYPE(c_voidp, c_fuse_req_t, c_fuse_ino_t, 
                                         POINTER(c_fuse_file_info_t))),
                ('fsyncdir', CFUNCTYPE(c_voidp, c_fuse_req_t, c_fuse_ino_t, c_int, 
                                      POINTER(c_fuse_file_info_t))),
                ('statfs', CFUNCTYPE(c_voidp, c_fuse_req_t, c_fuse_ino_t)),
                ('setxattr', CFUNCTYPE(c_voidp, c_fuse_req_t, c_fuse_ino_t, c_char_p, c_char_p, 
                                       c_size_t, c_int)),
                ('getxattr', CFUNCTYPE(c_voidp, c_fuse_req_t, c_fuse_ino_t, c_char_p, c_size_t)),
                ('listxattr', CFUNCTYPE(c_voidp, c_fuse_req_t, c_fuse_ino_t, c_size_t)),
                ('removexattr', CFUNCTYPE(c_voidp, c_fuse_req_t, c_fuse_ino_t, c_char_p)),
                ('access', CFUNCTYPE(c_voidp, c_fuse_req_t, c_fuse_ino_t, c_int)),
                ('create', CFUNCTYPE(c_voidp, c_fuse_req_t, c_fuse_ino_t, c_char_p, c_mode_t, 
                                     POINTER(c_fuse_file_info_t))),
                ('bmap', CFUNCTYPE(c_voidp, c_fuse_req_t, c_fuse_ino_t, c_size_t, c_uint64)),
                ('ioctl', CFUNCTYPE(c_voidp, c_fuse_req_t, c_fuse_ino_t, c_int, c_voidp, 
                                    POINTER(c_fuse_file_info_t), c_uint, c_voidp, c_size_t, c_size_t)),
                ('getlk', CFUNCTYPE(c_voidp, c_fuse_req_t, c_fuse_ino_t, POINTER(c_fuse_file_info_t), 
                                     POINTER(c_flock_t))),
                ('setlk', CFUNCTYPE(c_voidp, c_fuse_req_t, c_fuse_ino_t, POINTER(c_fuse_file_info_t), 
                                     POINTER(c_flock_t), c_int)),
                ('poll',
                  # Since we do not support these functions anyway, we just claim that
                  # the last argument is a void pointer (so we don't have to define the
                  # actual structures)
                  #CFUNCTYPE(c_voidp, c_fuse_req_t, c_fuse_ino_t, POINTER(c_fuse_file_info_t), 
                  #          POINTER(fuse_pollhandle))
                  CFUNCTYPE(c_voidp, c_fuse_req_t, c_fuse_ino_t, POINTER(c_fuse_file_info_t), c_voidp))
                ]

#
# Define Exceptions
#

class DiscardedRequest(Exception):
    '''
    Raised if the FUSE request that we wanted to send a reply to
    has been interrupted and the reply discarded.
    '''
    
    pass

class FUSEError(Exception):
    """Exception representing FUSE Errors to be returned to the kernel.

    This exception can store only an errno. It is meant to return error codes to
    the kernel, which can only be done in this limited form.
    """
    
    __slots__ = [ 'errno' ]
    
    def __init__(self, errno_):
        super(FUSEError, self).__init__()
        self.errno = errno_

    def __str__(self):
        return errno.errorcode[self.errno]


#
# Define FUSE function prototypes
#

def check_reply_result(result, func, req, *args):
    '''Check result of a call to a fuse_reply_* foreign function
    
    If `result` is 0, it is assumed that the call succeeded and the
    function does nothing.
    
    If result is `-errno.ENOENT`, this means that the request has
    been discarded and `DiscardedRequest` is raised.
    
    In all other cases, an error is logged and the function
    tries to call `fuse_reply_err` instead. If that is the
    function that actually failed, `fuse_reply_none` is called
    as a last-resort. Any errors in these last two calls are ignored.
    '''
    # The args argument is unused, but we have no control over that
    #pylint: disable-msg=W0613
    
    if result == 0:
        return None
    
    elif result == -errno.ENOENT:
        raise DiscardedRequest()
    
    elif result > 0:
        log.error('Foreign function %s returned unexpected value %d'
                  % (func.__name__, result))    
    elif result < 0:
        log.error('Foreign function %s returned error %s'
                  % (func.__name__, errno.errorcode[-result]))
    
    # Record that something went wrong
    global encountered_errors
    encountered_errors = True
    
    # Try to return an error instead
    if func.__name__ == 'fuse_reply_none':
        # there is really nothing we can do
        pass
    elif func.__name__ == 'fuse_reply_err':
        libfuse.fuse_reply_none(req)
    else:
        libfuse.fuse_reply_err(req, errno.EFAULT)
    
   
libfuse.fuse_reply_err.argtypes = [ c_fuse_req_t, c_int ]
libfuse.fuse_reply_err.restype = c_int
libfuse.fuse_reply_err.errcheck = check_reply_result

libfuse.fuse_reply_none.argtypes = [ c_fuse_req_t ]
libfuse.fuse_reply_none.restype = c_int
libfuse.fuse_reply_none.errcheck = check_reply_result

libfuse.fuse_reply_entry.argtypes = [ c_fuse_req_t, POINTER(c_fuse_entry_param_t) ]
libfuse.fuse_reply_entry.restype = c_int
libfuse.fuse_reply_entry.errcheck = check_reply_result

libfuse.fuse_reply_create.argtypes = [ c_fuse_req_t, POINTER(c_fuse_entry_param_t),
                                      POINTER(c_fuse_file_info_t) ]
libfuse.fuse_reply_create.restype = c_int
libfuse.fuse_reply_create.errcheck = check_reply_result

libfuse.fuse_reply_attr.argtypes = [ c_fuse_req_t, POINTER(c_stat_t), c_double ]
libfuse.fuse_reply_attr.restype = c_int
libfuse.fuse_reply_attr.errcheck = check_reply_result

libfuse.fuse_reply_readlink.argtypes = [ c_fuse_req_t, c_char_p ]
libfuse.fuse_reply_readlink.restype = c_int
libfuse.fuse_reply_readlink.errcheck = check_reply_result

libfuse.fuse_reply_open.argtypes = [ c_fuse_req_t, POINTER(c_fuse_file_info_t) ]
libfuse.fuse_reply_open.restype = c_int
libfuse.fuse_reply_open.errcheck = check_reply_result

libfuse.fuse_reply_write.argtypes = [ c_fuse_req_t, c_size_t ]
libfuse.fuse_reply_write.restype = c_int
libfuse.fuse_reply_write.errcheck = check_reply_result

libfuse.fuse_reply_buf.argtypes = [ c_fuse_req_t, c_char_p, c_size_t ]
libfuse.fuse_reply_buf.restype = c_int
libfuse.fuse_reply_buf.errcheck = check_reply_result

libfuse.fuse_reply_iov.argtypes = [ c_fuse_req_t, POINTER(c_iovec_t), c_int ]
libfuse.fuse_reply_iov.restype = c_int
libfuse.fuse_reply_iov.errcheck = check_reply_result

libfuse.fuse_reply_statfs.argtypes = [ c_fuse_req_t, POINTER(c_statfs_t) ]
libfuse.fuse_reply_statfs.restype = c_int
libfuse.fuse_reply_statfs.errcheck = check_reply_result

libfuse.fuse_reply_xattr.argtypes = [ c_fuse_req_t, c_size_t ]
libfuse.fuse_reply_xattr.restype = c_int
libfuse.fuse_reply_xattr.errcheck = check_reply_result

libfuse.fuse_reply_lock.argtypes = [ c_fuse_req_t, POINTER(c_flock_t) ]
libfuse.fuse_reply_lock.restype = c_int
libfuse.fuse_reply_lock.errcheck = check_reply_result

libfuse.fuse_reply_bmap.argtypes = [ c_fuse_req_t, c_uint64 ]
libfuse.fuse_reply_bmap.restype = c_int
libfuse.fuse_reply_bmap.errcheck = check_reply_result


#
# Define public functions
#
def init(operations_):
    '''Initialize FUSE file system.
    '''
    
    global operations
    global encountered_errors
    global fuse_ops
    
    operations = operations_
    encountered_errors = False
    
    def op_wrapper(func, req, *args):
        '''Catch all exceptions and call fuse_reply_err instead
        '''
        try:
            func(req, *args)
        except FUSEError as e:
            libfuse.fuse_reply_err(req, -e.errno)
        except:
            global encountered_errors
            encountered_errors = True
            log.exception('FUSE handler generated exception.')
            
            # We may already have send a reply, but there is
            # nothing else we can do.
            libfuse.fuse_reply_err(req, -errno.EFAULT)
        return None

    
    fuse_ops = c_fuse_lowlevel_ops_t()

    # Access to protected member _fields_ is alright
    #pylint: disable-msg=W0212    
    module = globals()
    for name, prototype in c_fuse_lowlevel_ops_t._fields_:
        name = "fuse_" + name
        if hasattr(operations, name):
            method = partial(op_wrapper, module[name])
            setattr(fuse_ops, name, prototype(method))
            
def main(mountpoint, args):
    '''Mount file system and start event loop
    
    `args` has to be a list of strings, whose format is not
    entirely clear.  
    '''
                   
    # Init fuse_args struct
    fuse_args = c_fuse_args_t()
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


#
# Define internal FUSE request handlers
#   
    
               
def fuse_lookup(req, parent, name):
    '''Look up a directory entry by name and get its attributes
    
    The corresponding `Operations` method must return an instance
    of `c_fuse_entry_param_t`.
    '''
    
    entry = operations.lookup(parent.value, string_at(name))
    libfuse.fuse_reply_entry(req, byref(entry))
    
def fuse_getattr(req, ino, fi):
    '''
    '''
    ret = operations.getattr(ino)
    attr = c_stat_t(**ret)
    attr.st_ino = ino
    libfuse.fuse_reply_attr(req, byref(attr), c_double(1))


def fuse_open(req, ino, fi):
    fi.contents.fh = operations.open(ino, fi.contents.flags)
    libfuse.fuse_reply_open(req, fi)

def fuse_read(req, ino, size, off, fi):
    ret = operations.read(ino, size, off, fi.contents.fh)
    return libfuse.fuse_reply_buf(req, ret, len(ret))

def fuse_readdir(req, ino, size, off, fi):
    ret = operations.readdir(ino)
    bufsize = sum(libfuse.fuse_add_direntry(req, None, 0, name, None, 0) for name, ino in ret)
    buf = create_string_buffer(bufsize)
    next = 0
    for name, ino in ret:
        addr = addressof(buf) + next
        dirsize = libfuse.fuse_add_direntry(req, None, 0, name, None, 0)
        next += dirsize
        attr = c_stat_t(st_ino=ino)
        libfuse.fuse_add_direntry(req, addr, dirsize, name, byref(attr), next)
    if off < bufsize:
        libfuse.fuse_reply_buf(req, addressof(buf) + off, min(bufsize - off, size))
    else:
        libfuse.fuse_reply_buf(req, None, 0)


