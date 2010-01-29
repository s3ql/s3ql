'''
$Id$

Copyright (C) 2008-2009 Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU LGPL.
'''

from __future__ import division, print_function

import sys
import os
import stat
import traceback
import hashlib
import threading
import logging
from time import sleep
from getpass import getpass

__all__ = [ "get_cachedir", "init_logging", 'sha256', 'sha256_fh',
           "get_credentials", "get_dbfile", "inode_for_path", "get_path",
           "waitfor", "ROOT_INODE", "ExceptionStoringThread",
           "EmbeddedException", 'CTRL_NAME', 'CTRL_INODE', 'unlock_bucket',
           'stacktraces', 'init_logging_from_options', 'QuietError' ]

def unlock_bucket(bucket):
    '''Ask for passphrase if bucket requires one'''
    
    if bucket.has_key('s3ql_passphrase'):
        if sys.stdin.isatty():
            wrap_pw = getpass("Enter encryption password: ")
        else:
            wrap_pw = sys.stdin.readline().rstrip()
        bucket.passphrase = wrap_pw
        data_pw = bucket['s3ql_passphrase']
        bucket.passphrase = data_pw
               

class QuietError(SystemExit):
    '''QuietError is an exception that should not result in a
    stack trace being printed. It is typically raised if the
    exception can generally only be handled by the user invoking
    a non-interactive program.
    The exception argument should be a string containing sufficient
    information about the problem.
    '''
    pass
    
    
def stacktraces():
    '''Return stack trace for every running thread'''
    
    code = []
    for threadId, frame in sys._current_frames().items():
        code.append("\n# ThreadID: %s" % threadId)
        for filename, lineno, name, line in traceback.extract_stack(frame):
            code.append('%s:%d, in %s' % (os.path.basename(filename), lineno, name))
            if line:
                code.append("    %s" % (line.strip()))
 
    return "\n".join(code)

class Filter(object):
    """
    For use with the logging module as a message filter.
    
    This filter accepts all messages which have the specified priority 
    and come from a configured list of loggers.    
    """
   
    def __init__(self, acceptnames=None, acceptlevel=logging.DEBUG):
        """Initializes a Filter object.
        
        Passes through all messages with priority higher than
        `acceptlevel` or coming from a logger in `acceptnames`. 
        """
        if acceptnames is None:
            acceptnames = list()
            
        self.acceptlevel = acceptlevel
        self.acceptnames = [ x.lower() for x in acceptnames ]
        
    def filter(self, record):
        '''Determine if the log message should be printed'''
        
        if record.levelno > self.acceptlevel:
            return True
        
        if record.name.lower() in self.acceptnames:
            return True
        
        return False
    
class Formatter(logging.Formatter):
    
    def format(self, record):
        '''Format log message.
        
        Splits the message by newlines and calls the base class method for each
        line.'''
        
        # First format the exception
        if record.exc_info and record.exc_text is None:
            # Prevent logging.Formatter from adding the traceback again
            record.exc_text = ''
            if record.msg[-1:] == "\n":
                record.msg += logging.Formatter.formatException(self, record.exc_info)
            else:
                record.msg += '\n' + logging.Formatter.formatException(self, record.exc_info)
                
        formatted_msgs = list()
        full_msg = record.msg
        exc = record.exc_info
        record.exc_info = None
        for msg in full_msg.split('\n'):
            record.msg = msg
            # Can't use super with old style class
            formatted_msgs.append(logging.Formatter.format(self, record))
        record.msg = full_msg
        record.exc_info = exc
            
        return '\n'.join(formatted_msgs)

def init_logging_from_options(options, daemon=False):
    '''Call init_logging according to command line arguments
    
    `options` should have attributes ``quiet``, ``debug`` and ``debuglog``.
    '''
    
    if options.debug and not options.debuglog:
        level = logging.DEBUG
    elif options.quiet:
        level = logging.WARN
    else:
        level = logging.INFO
        
    if options.debug and 'all' in options.debug:
        init_logging(level, daemon, logfile=options.debuglog)
    else:
        init_logging(level, daemon, debug_logger=options.debug, logfile=options.debuglog) 
        
    
def init_logging(level=logging.INFO, daemon=False, debug_logger=None, logfile=None,
                 logfile_level=logging.DEBUG):
    """Initialize logging
    
    If `daemon` is true, logging messages are send to syslog. Otherwise
    to stdout. 
    
    `debug_logger` can be set to a list of logger names. In that case, debug
    messages are only logged from these loggers.
    
    If `logfile` is set to a file name, all log messages will also be
    written into this file.
    
     Does nothing if logging has already been initialized
    """
                  
    # TODO: Change logging 
    #  - Do not log to syslog
    #  - Always log INFO to ~/.s3ql/log
    #  - On Debug, log DEBUG to ~/.s3ql/log
    #  - Depending on quiet, log INFO or WARN to stdout
    #  - Get rid of fg setting
    #  - Make ~/.s3ql/log a rotating logfile
    
    root_logger = logging.getLogger()
    
    # Remove existing handlers. We have to copy the list
    # since it is going to change during iteration
    for hdlr in list(root_logger.handlers): 
        root_logger.removeHandler(hdlr)
        
    if level <= logging.DEBUG:
        formatter = logging.Formatter('%(asctime)s,%(msecs)03d %(threadName)s: [%(name)s] %(message)s',
                                      datefmt="%H:%M:%S")
    else:
        formatter = logging.Formatter('[%(name)s] %(message)s')
    
    if daemon:
        handler = logging.handlers.SysLogHandler(b"/dev/log")
        handler.setFormatter(logging.Formatter('s3ql[%(process)d]: [%(name)s] %(message)s'))
    else:  
        handler = logging.StreamHandler()
        handler.setFormatter(formatter)       
    
    root_logger.setLevel(level)
    handler.setLevel(level)
    root_logger.addHandler(handler)

    if debug_logger:
        handler.addFilter(Filter(acceptnames=debug_logger, acceptlevel=logging.DEBUG))
        
    if logfile:
        file_handler = logging.handlers.WatchedFileHandler(logfile)
        file_handler.setFormatter(formatter)

        if debug_logger:
            file_handler.addFilter(Filter(acceptnames=debug_logger, acceptlevel=logging.DEBUG))
        
        file_handler.setLevel(logfile_level)
        root_logger.addHandler(file_handler)
        root_logger.setLevel(min(level, logfile_level))
        
   
   
def inode_for_path(path, conn):
    """Return inode of directory entry at `path`
    
     Raises `KeyError` if the path does not exist.
    """
    
    if not isinstance(path, bytes):
        raise TypeError('path must be of type bytes')
    
    # Remove leading and trailing /
    path = path.lstrip(b"/").rstrip(b"/") 
    
    # Traverse
    inode = ROOT_INODE
    for el in path.split(b'/'):
        try:
            inode = conn.get_val("SELECT inode FROM contents WHERE name=? AND parent_inode=?",
                                (el, inode))
        except KeyError:
            raise KeyError('Path %s does not exist' % path)
    
    return inode

    
def get_path(name, inode_p, conn):
    """Return the full path of `name` with parent inode `inode_p`"""
    
    if not isinstance(name, bytes):
        raise TypeError('name must be of type bytes')
    
    path = [ name ] 
    
    maxdepth = 255
    while inode_p != ROOT_INODE:
        # This can be ambigious if directories are hardlinked
        (name2, inode_p) = conn.get_row("SELECT name, parent_inode FROM contents "
                                      "WHERE inode=? AND name != ? AND name != ?",
                                       (inode_p, b'.', b'..')) 
        path.append(name2)
        maxdepth -= 1
        if maxdepth == 0:
            raise RuntimeError('Failed to resolve name "%s" at inode %d to path',
                               name, inode_p)
        
    path.append(b'')
    path.reverse()
    
    return b'/'.join(path)


def get_cachedir(bucketname, path):
    """get directory to put cache files in.
    """

    if not os.path.exists(path):
        os.mkdir(path)

    return path + ("/%s-cache/" % bucketname)


def get_dbfile(bucketname, path):
    """get filename for metadata db.
    """

    if not os.path.exists(path):
        os.mkdir(path)

    return path + ("/%s.db" % bucketname)


def get_credentials(keyfile, key=None):
    """Get AWS credentials.

    If `key` has been specified, use this as access key and
    read the password from stdin. Otherwise, tries to read
    ~/.awssecret.
    """
    
    pw = None
    
    if key:
        if sys.stdin.isatty():
            pw = getpass("Enter AWS password: ")
        else:
            pw = sys.stdin.readline().rstrip()
            
    else:
        
        if os.path.isfile(keyfile):
            mode = os.stat(keyfile).st_mode
            kfile = open(keyfile, "r")
            key = kfile.readline().rstrip()

            if mode & (stat.S_IRGRP | stat.S_IROTH):
                sys.stderr.write("~/.awssecret has insecure permissions, "
                                 "reading password from terminal instead!\n")        
            else:    
                pw = kfile.readline().rstrip()
            kfile.close()    
            
        if not key:
            if sys.stdin.isatty():
                print("Enter AWS access key: ", end='')
            key = sys.stdin.readline().rstrip()
           
        if not pw:
            if sys.stdin.isatty():
                pw = getpass("Enter AWS password: ")
            else:
                pw = sys.stdin.readline().rstrip()

    return (key, pw)

# TODO: Replace waitfor() by retry(), retry() executes until
# the function return value is something true and then returns
# that value. Otherwise it raises a TimeoutError.
def waitfor(timeout, fn, *a, **kw):
    """Wait for fn(*a, **kw) to return True.
    
    Waits in increasing periods. Returns False if a timeout occurs, 
    True otherwise.
    """
    
    if fn(*a, **kw):
        return True
    
    step = 0.2
    while timeout > 0:    
        sleep(step)
        timeout -= step
        step *= 2
        if fn(*a, **kw):
            return True
        
    return False

        
# Define inode of root directory
ROOT_INODE = 1

# Name and inode of the special s3ql control file
CTRL_NAME = b'.__s3ql__ctrl__'
CTRL_INODE = 2

class ExceptionStoringThread(threading.Thread):
    '''Catch all exceptions and store them
    '''
    
    def __init__(self, target, args=(), kwargs={}):
        # Default value isn't dangerous
        #pylint: disable-msg=W0102
        super(ExceptionStoringThread, self).__init__()
        if target is not None:
            self.target = target
        self.exc = None
        self.tb = None
        self.joined = False
        self.args = args
        self.kwargs = kwargs
        
    def run(self):
        try:
            self.target(*self.args, **self.kwargs)
        except BaseException as exc:
            self.exc = exc
            self.tb = sys.exc_info()[2] # This creates a circular reference chain
    
    def join_get_exc(self):
        self.joined = True
        self.join()
        return self.exc
         
    def join_and_raise(self):
        '''Wait for the thread to finish, raise any occured exceptions
        '''
        self.joined = True
        self.join()
        if self.exc is not None:
            # Break reference chain
            tb = self.tb
            del self.tb
            raise EmbeddedException(self.exc, tb, self.name)

                
    def __del__(self):
        if not self.joined:
            raise RuntimeError("ExceptionStoringThread instance was destroyed "
                               "without calling join_and_raise()!")                

class EmbeddedException(Exception):
    '''Encapsulates an exception that happened in a different thread
    '''
    
    def __init__(self, exc, tb, threadname):
        super(EmbeddedException, self).__init__()
        self.exc = exc
        self.tb = tb
        self.threadname = threadname
        
    def __str__(self):
        return ''.join(['caused by an exception in thread %s.\n' % self.threadname,
                       'Original/inner traceback (most recent call last): \n' ] +
                       traceback.format_tb(self.tb) +
                       traceback.format_exception_only(type(self.exc), self.exc))


def sha256_fh(fh):
    fh.seek(0)
    sha = hashlib.sha256()
    
    while True:
        buf = fh.read(128*1024)
        if not buf:
            break
        sha.update(buf)
        
    return sha.digest()


def sha256(s):
    return hashlib.sha256(s).digest()        
