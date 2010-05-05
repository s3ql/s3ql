'''
operations.py - this file is part of S3QL (http://s3ql.googlecode.com)

Copyright (C) 2008-2010 Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU LGPL.
''' 

from __future__ import division, print_function, absolute_import

from .interface import FUSEError
import errno
  
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
    
    def init(self):
        '''Initialize operations
        
        This function has to be called before any request has been received,
        but after the mountpoint has been set up and the process has
        daemonized.
        '''
        
        pass
    
    def destroy(self):
        '''Clean up operations.
        
        This method has to be called after the last request has been
        received, when the file system is about to be unmounted.
        '''
        
        pass
    
    def check_args(self, fuse_args):
        '''Review FUSE arguments
        
        This method checks if the FUSE options `fuse_args` are compatible
        with the way that the file system operations are implemented.
        It raises an exception if incompatible options are encountered and
        silently adds required options if they are missing.
        '''
        
        pass
    
    def readdir(self, fh, off):
        '''Read directory entries
        
        This method returns an iterator over the contents of directory `fh`,
        starting at entry `off`. The iterator yields tuples of the form
        ``(name, attr)``, where ``attr` is an object with attributes corresponding to
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
    
        Returns an object with the attributes of the newly created directory
        entry. The attributes are the same as for `lookup`.
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

    
    def mkdir(self, parent_inode, name, mode, ctx):
        '''Create a directory
    
        `ctx` must be a context object that contains pid, uid and primary gid of
        the requesting process.
        
        Returns an object with the attributes of the newly created directory
        entry. The attributes are the same as for `lookup`.
        '''
        
        raise FUSEError(errno.ENOSYS)

    def mknod(self, parent_inode, name, mode, rdev, ctx):
        '''Create (possibly special) file
    
        `ctx` must be a context object that contains pid, uid and primary gid of
        the requesting process.
        
        Returns an object with the attributes of the newly created directory
        entry. The attributes are the same as for `lookup`.
        '''
        
        raise FUSEError(errno.ENOSYS)

    
    def lookup(self, parent_inode, name):
        '''Look up a directory entry by name and get its attributes.
    
        Returns an object with attributes corresponding to the elements in
        ``struct stat`` as well as
        
        :generation: The inode generation number
        :attr_timeout: Validity timeout (in seconds) for the attributes
        :entry_timeout: Validity timeout (in seconds) for the name 
        
        Note that the ``st_Xtime`` entries support floating point numbers to
        allow for nano second resolution.
        
        The returned object must not be modified by the caller as this would
        affect the internal state of the file system.
        
        If the entry does not exist, raises `FUSEError(errno.ENOENT)`.
        '''
        
        raise FUSEError(errno.ENOSYS)

    def listxattr(self, inode):
        '''Get list of extended attribute names'''
        
        raise FUSEError(errno.ENOSYS)
    
    def getattr(self, inode):
        '''Get attributes for `inode`
    
        Returns an object with attributes corresponding to the elements in 
        ``struct stat`` as well as
        
        :attr_timeout: Validity timeout (in seconds) for the attributes
        
        The returned object must not be modified by the caller as this would
        affect the internal state of the file system.
        
        Note that the ``st_Xtime`` entries support floating point numbers to
        allow for nano second resolution.
        '''
        
        raise FUSEError(errno.ENOSYS)
    
    def getxattr(self, inode, name):
        '''Return extended attribute value
        
        If the attribute does not exist, raises `FUSEError(ENOATTR)`
        '''
        
        raise FUSEError(errno.ENOSYS)
 
    def access(self, inode, mode, ctx, get_sup_gids):
        '''Check if requesting process has `mode` rights on `inode`. 
        
        Returns a boolean value. `get_sup_gids` must be a function that returns
        a list of the supplementary group ids of the requester.
        
        `ctx` must be a context object that contains pid, uid and primary gid of
        the requesting process.
        '''
        
        raise FUSEError(errno.ENOSYS)
    
    def create(self, inode_parent, name, mode, ctx):
        '''Create a file and open it
                
        `ctx` must be a context object that contains pid, uid and 
        primary gid of the requesting process.
        
        Returns a tuple of the form ``(fh, attr)``. `fh` is
        integer file handle that is used to identify the open file and
        `attr` is an object similar to the one returned by `lookup`.
        '''
        
        raise FUSEError(errno.ENOSYS)

    def flush(self, fh):
        '''Handle close() syscall.
        
        May be called multiple times for the same open file (e.g. if the file handle
        has been duplicated).
                                                             
        This method also clears all locks belonging to the file handle's owner.
        '''
        
        raise FUSEError(errno.ENOSYS)
    
    def fsync(self, fh, datasync):
        '''Flush buffers for file `fh`
        
        If `datasync` is true, only the user data is flushed (and no meta data). 
        '''
        
        raise FUSEError(errno.ENOSYS)
    
    
    def fsyncdir(self, fh, datasync):  
        '''Flush buffers for directory `fh`
        
        If the `datasync` is true, then only the directory contents are flushed
        (and not the meta data about the directory itself).
        '''
        
        raise FUSEError(errno.ENOSYS)
        
    def readlink(self, inode):
        '''Return target of symbolic link'''
        
        raise FUSEError(errno.ENOSYS)
    
    def release(self, fh):
        '''Release open file
        
        This method must be called exactly once for each `open` call.
        '''
        
        raise FUSEError(errno.ENOSYS)
    
    def releasedir(self, fh):
        '''Release open directory
        
        This method must be called exactly once for each `opendir` call.
        '''
        
        raise FUSEError(errno.ENOSYS)
    
    def removexattr(self, inode, name):
        '''Remove extended attribute
        
        If the attribute does not exist, raises `FUSEError(ENOATTR)`
        '''
        
        raise FUSEError(errno.ENOSYS)
    
    def rename(self, inode_parent_old, name_old, inode_parent_new, name_new):
        '''Rename a directory entry'''
        
        raise FUSEError(errno.ENOSYS)
    
    def rmdir(self, inode_parent, name):
        '''Remove a directory'''
        
        raise FUSEError(errno.ENOSYS)
    
    def setattr(self, inode, attr):
        '''Change directory entry attributes
        
        `attr` must be an object with attributes corresponding to the attributes
        of ``struct stat``. `attr` may also include a new value for ``st_size``
        which means that the file should be truncated or extended.
        
        Returns an object with the new attributs of the directory entry, similar
        to the one returned by `getattr()`
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
    
    def symlink(self, inode_parent, name, target, ctx):
        '''Create a symbolic link
        
        `ctx` must be a context object that contains pid, uid and 
        primary gid of the requesting process.
        
        Returns an object with the attributes of the newly created directory
        entry, similar to the one returned by `lookup`.
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
    
