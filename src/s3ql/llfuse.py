# Copyright (c) 2009 Giorgos Verigakis <verigak@gmail.com>
# 
# Permission to use, copy, modify, and distribute this software for any
# purpose with or without fee is hereby granted, provided that the above
# copyright notice and this permission notice appear in all copies.
# 
# THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
# WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
# MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
# ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
# WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
# ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
# OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.

# We have no control over the arguments, so we
# disable warnings about unused arguments
#pylint: disable-msg=W0613


from __future__ import division
from ctypes import (c_long, Structure, c_int32, c_byte, c_char_p, c_int, c_int64, 
                    c_size_t, c_uint16, c_ulong, c_ulonglong, c_uint, c_short, 
                    CDLL, CFUNCTYPE, POINTER, RTLD_GLOBAL, c_voidp, c_uint64, 
                     sizeof, create_string_buffer, c_double, byref, addressof)
from ctypes.util import find_library
from platform import machine, system


c_blkcnt_t = c_int64
c_blksize_t = c_long
c_fsblkcnt_t = c_ulong
c_fsfilcnt_t = c_ulong
c_gid_t = c_uint
c_ino_t = c_ulong
c_off_t = c_int64
c_time_t = c_long
c_uid_t = c_uint


class c_timespec(Structure):
    _fields_ = [('tv_sec', c_time_t), ('tv_nsec', c_long)]


_system = system()

if _system == 'Darwin':
    c_dev_t = c_int32
    c_mode_t = c_uint16
    c_nlink_t = c_uint16
    
    class c_stat(Structure):
        _fields_ = [
            ('st_dev', c_dev_t),
            ('st_ino', c_ino_t),
            ('st_mode', c_mode_t),
            ('st_nlink', c_nlink_t),
            ('st_uid', c_uid_t),
            ('st_gid', c_gid_t),
            ('st_rdev', c_dev_t),
            ('st_atimespec', c_timespec),
            ('st_mtimespec', c_timespec),
            ('st_ctimespec', c_timespec),
            ('st_size', c_off_t),
            ('st_blocks', c_blkcnt_t),
            ('st_blksize', c_blksize_t),
        ]
elif _system == 'Linux':
    c_dev_t = c_ulonglong
    c_mode_t = c_uint
    c_nlink_t = c_ulong
    
    if machine() == 'x86_64':
        class c_stat(Structure):
            _fields_ = [
                ('st_dev', c_dev_t),
                ('st_ino', c_ino_t),
                ('st_nlink', c_nlink_t),
                ('st_mode', c_mode_t),
                ('st_uid', c_uid_t),
                ('st_gid', c_gid_t),
                ('__pad0', c_int),
                ('st_rdev', c_dev_t),
                ('st_size', c_off_t),
                ('st_blksize', c_blksize_t),
                ('st_blocks', c_blkcnt_t),
                ('st_atimespec', c_timespec),
                ('st_mtimespec', c_timespec),
                ('st_ctimespec', c_timespec),
            ]
    else:
        class c_stat(Structure):
            _fields_ = [
                ('st_dev', c_dev_t),
                ('__pad1', c_short),
                ('st_ino', c_ino_t),
                ('st_mode', c_mode_t),
                ('st_nlink', c_nlink_t),
                ('st_uid', c_uid_t),
                ('st_gid', c_gid_t),
                ('st_rdev', c_dev_t),
                ('__pad2', c_short),
                ('st_size', c_off_t),
                ('st_blksize', c_blksize_t),
                ('st_blocks', c_blkcnt_t),
                ('st_atimespec', c_timespec),
                ('st_mtimespec', c_timespec),
                ('st_ctimespec', c_timespec)
            ]


class fuse_file_info(Structure):
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
    

fuse_ino_t = c_ulong
fuse_req_t = c_voidp

class fuse_entry_param(Structure):
    _fields_ = [
        ('ino', fuse_ino_t),
        ('generation', c_ulong),
        ('attr', c_stat),
        ('attr_timeout', c_double),
        ('entry_timeout', c_double)
    ]

init_t = CFUNCTYPE(c_voidp, c_voidp, c_voidp)
destroy_t = CFUNCTYPE(c_voidp, c_voidp)
lookup_t = CFUNCTYPE(c_voidp, fuse_req_t, fuse_ino_t, c_char_p)
forget_t = CFUNCTYPE(c_voidp, fuse_req_t, fuse_ino_t, c_ulong)
getattr_t = CFUNCTYPE(c_voidp, fuse_req_t, fuse_ino_t, POINTER(fuse_file_info))
setattr_t = CFUNCTYPE(c_voidp, fuse_req_t, fuse_ino_t, c_stat, c_int, POINTER(fuse_file_info))
readlink_t = CFUNCTYPE(c_voidp, fuse_req_t, fuse_ino_t)
mknod_t = CFUNCTYPE(c_voidp, fuse_req_t, fuse_ino_t, c_char_p, c_mode_t, c_dev_t)
mkdir_t = CFUNCTYPE(c_voidp, fuse_req_t, fuse_ino_t, c_char_p, c_mode_t)
unlink_t = CFUNCTYPE(c_voidp, fuse_req_t, fuse_ino_t, c_char_p)
rmdir_t = CFUNCTYPE(c_voidp, fuse_req_t, fuse_ino_t, c_char_p)
symlink_t = CFUNCTYPE(c_voidp, fuse_req_t, c_char_p, fuse_ino_t, c_char_p)
rename_t = CFUNCTYPE(c_voidp, fuse_req_t, fuse_ino_t, c_char_p, fuse_ino_t, c_char_p)
link_t = CFUNCTYPE(c_voidp, fuse_req_t, fuse_ino_t, fuse_ino_t, c_char_p)
open_t = CFUNCTYPE(c_voidp, fuse_req_t, fuse_ino_t, POINTER(fuse_file_info))
read_t = CFUNCTYPE(c_voidp, fuse_req_t, fuse_ino_t, c_size_t, c_off_t, POINTER(fuse_file_info))
write_t = CFUNCTYPE(c_voidp, fuse_req_t, fuse_ino_t, POINTER(c_byte), c_size_t, c_off_t, POINTER(fuse_file_info))
flush_t = CFUNCTYPE(c_voidp, fuse_req_t, fuse_ino_t, POINTER(fuse_file_info))
release_t = CFUNCTYPE(c_voidp, fuse_req_t, fuse_ino_t, POINTER(fuse_file_info))
fsync_t = CFUNCTYPE(c_voidp, fuse_req_t, fuse_ino_t, c_int, POINTER(fuse_file_info))
opendir_t = CFUNCTYPE(c_voidp, fuse_req_t, fuse_ino_t, POINTER(fuse_file_info))
readdir_t = CFUNCTYPE(c_voidp, fuse_req_t, fuse_ino_t, c_size_t, c_long, POINTER(fuse_file_info))
releasedir_t = CFUNCTYPE(c_voidp, fuse_req_t, fuse_ino_t, POINTER(fuse_file_info))
fsyncdir_t = CFUNCTYPE(c_voidp, fuse_req_t, fuse_ino_t, c_int, POINTER(fuse_file_info))
statfs_t = CFUNCTYPE(c_voidp, fuse_req_t, fuse_ino_t)

class fuse_lowlevel_ops(Structure):
    _fields_ = [
        ('init', init_t),
        ('destroy', destroy_t),
        ('lookup', lookup_t),
        ('forget', forget_t),
        ('getattr', getattr_t),
        ('setattr', setattr_t),
        ('readlink', readlink_t),
        ('mknod', mknod_t),
        ('mkdir', mkdir_t),
        ('unlink', unlink_t),
        ('rmdir', rmdir_t),
        ('symlink', symlink_t),
        ('rename', rename_t),
        ('link', link_t),
        ('open', open_t),
        ('read', read_t),
        ('write', write_t),
        ('flush', flush_t),
        ('release', release_t),
        ('fsync', fsync_t),
        ('opendir', opendir_t),
        ('readdir', readdir_t),
        ('releasedir', releasedir_t),
        ('fsyncdir', fsyncdir_t),
        ('statfs', statfs_t)
    ]


def fuse_op(func):
    def wrapper(self, req, *args):
        print func.__name__, args
        try:
            func(self, req, *args)
        except OSError, e:
            self.libfuse.fuse_reply_err(req, e.message)
    return wrapper


class LLFUSE:
    def __init__(self, operations, mountpoint='/mnt'):
        if _system == 'Darwin':
            libiconv = CDLL(find_library("iconv"), RTLD_GLOBAL)
        self.libfuse = CDLL(find_library("fuse"))
        
        self.operations = operations
        
        fuse_ops = fuse_lowlevel_ops()
        for name, prototype in fuse_lowlevel_ops._fields_:
            method = getattr(self, name, None)
            if method:
                setattr(fuse_ops, name, prototype(method))
        
        argv = None
        ch = self.libfuse.fuse_mount(mountpoint, argv)
        se = self.libfuse.fuse_lowlevel_new(argv, byref(fuse_ops), sizeof(fuse_ops), None)
        self.libfuse.fuse_set_signal_handlers(se)
        self.libfuse.fuse_session_add_chan(se, ch)
        err = self.libfuse.fuse_session_loop(se)
        self.libfuse.fuse_remove_signal_handlers(se)
        self.libfuse.fuse_session_remove_chan(ch)
        self.libfuse.fuse_session_destroy(se)
        self.libfuse.fuse_unmount(mountpoint, ch)
    
    @fuse_op
    def getattr(self, req, ino, fi):
        ret = self.operations.getattr(ino)
        attr = c_stat(**ret)
        attr.st_ino = ino
        self.libfuse.fuse_reply_attr(req, byref(attr), c_double(1))
    
    @fuse_op
    def lookup(self, req, parent, name):
        ret = self.operations.lookup(parent, name)
        attr = ret.pop('attr')
        e = fuse_entry_param(**ret)
        e.attr.st_ino = e.ino
        for key, val in attr.items():
            setattr(e.attr, key, val)
        self.libfuse.fuse_reply_entry(req, byref(e))
    
    @fuse_op
    def open(self, req, ino, fi):
        fi.contents.fh = self.operations.open(ino, fi.contents.flags)
        self.libfuse.fuse_reply_open(req, fi)
    
    @fuse_op
    def read(self, req, ino, size, off, fi):
        ret = self.operations.read(ino, size, off, fi.contents.fh)
        return self.libfuse.fuse_reply_buf(req, ret, len(ret))
    
    @fuse_op
    def readdir(self, req, ino, size, off, fi):
        libfuse = self.libfuse
        ret = self.operations.readdir(ino)
        bufsize = sum(libfuse.fuse_add_direntry(req, None, 0, name, None, 0) for name, ino in ret)
        buf = create_string_buffer(bufsize)
        next = 0
        for name, ino in ret:
            addr = addressof(buf) + next
            dirsize = libfuse.fuse_add_direntry(req, None, 0, name, None, 0)
            next += dirsize
            attr = c_stat(st_ino=ino)
            libfuse.fuse_add_direntry(req, addr, dirsize, name, byref(attr), next)
        if off < bufsize:
            libfuse.fuse_reply_buf(req, addressof(buf) + off, min(bufsize - off, size))
        else:
            libfuse.fuse_reply_buf(req, None, 0)


from errno import EACCES, ENOENT
from os import O_RDONLY
from stat import S_IFDIR, S_IFREG

class LLOperations(object):
    def getattr(self, ino):
        if ino == 1:
            return dict(st_mode=(S_IFDIR | 0755), st_nlink=2)
        elif ino == 2:
            return dict(st_mode=(S_IFREG | 0444), st_nlink=1, st_size=len('Hello world!\n'))
        else:
            raise OSError(ENOENT)
        
    def lookup(self, parent, name):
        if parent != 1 or name != 'hello':
            raise OSError(ENOENT)
        
        attr = dict(st_mode=(S_IFREG | 0444), st_nlink=1, st_size=len('Hello world!\n'))
        return dict(ino=2, attr_timeout=1, entry_timeout=1, attr=attr)
    
    def open(self, ino, flags):
        if flags & 3 != O_RDONLY:
            raise OSError(EACCES)
        return 0
    
    def read(self, ino, size, off, fh):
        return 'Hello world!\n'[off : off + size]
    
    def readdir(self, ino):
        if ino != 1:
            raise OSError(ENOENT)
        return [('.', 1), ('..', 1), ('hello', 2)]


if __name__ == "__main__":        
    fuse = LLFUSE(LLOperations())