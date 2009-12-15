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
import fuse_ctypes as libfuse
from ctypes import (string_at, byref, c_char_p, sizeof, create_string_buffer, addressof,
                    c_double)
from functools import partial
import logging
import errno

__all__ = [ 'init', 'main', 'FUSEError' ]

log = logging.getLogger("fuse") 


#
# Exceptions
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
# Define FUSE reply function error checkers
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
    
  
reply_functions = [ 'fuse_reply_err', 'fuse_reply_none', 'fuse_reply_entry',
                   'fuse_reply_create', 'fuse_reply_readlink', 'fuse_reply_open',
                   'fuse_reply_write', 'fuse_reply_attr', 'fuse_reply_buf',
                   'fuse_reply_iov', 'fuse_reply_statfs', 'fuse_reply_xattr',
                   'fuse_reply_lock', 'fuse_reply_bmap' ]

for name in reply_functions:
    getattr(libfuse, name).errcheck = check_reply_result
       

#
# Define public API
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
    
    `args` has to be a list of strings, whose format is not
    entirely clear.  
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
    attr = libfuse.stat(**ret)
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
        attr = libfuse.stat(st_ino=ino)
        libfuse.fuse_add_direntry(req, addr, dirsize, name, byref(attr), next)
    if off < bufsize:
        libfuse.fuse_reply_buf(req, addressof(buf) + off, min(bufsize - off, size))
    else:
        libfuse.fuse_reply_buf(req, None, 0)


