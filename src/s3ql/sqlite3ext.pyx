
# distutils: language = c++
# cython: language_level=3

from libcpp.unordered_map cimport unordered_map
from libcpp.unordered_set cimport unordered_set
from libcpp.string cimport string
from cython.operator cimport dereference as deref

import os


cdef extern from "_sqlite3ext.cpp":
    ctypedef int size_t
    ctypedef unordered_set[size_t] block_map_t
    cdef unordered_map[string, block_map_t] file_block_map
    cdef size_t blocksize
    cdef string vfsname


cdef class WriteTracker:
    cdef block_map_t* block_map

    # Constructors can only accept Python objects, so we need a factory method to construct from a
    # C++ object.
    # https://cython.readthedocs.io/en/stable/src/userguide/extension_types.html#existing-pointers-instantiation
    @staticmethod
    cdef WriteTracker make(block_map_t* block_map):
        # Call to __new__ bypasses __init__ constructor
        cdef WriteTracker tracker = WriteTracker.__new__(WriteTracker)
        tracker.block_map = block_map
        return tracker

    def get_block(self):
        it = self.block_map.begin()
        if it == self.block_map.end():
            raise KeyError()
        cdef size_t blockno = deref(it)
        self.block_map.erase(it)
        return blockno

    def get_count(self):
        return self.block_map.size()

    def clear(self):
        self.block_map.clear()

def track_writes(fpath: str):
    cdef string path_c = os.fsencode(fpath)
    return WriteTracker.make(&file_block_map[path_c])

def set_blocksize(size: int):
    global blocksize
    if blocksize != 0 and blocksize != size:
        raise ValueError('block size may only be set once')
    blocksize = size

def reset():
    global blocksize
    blocksize = 0
    file_block_map.clear()

def get_vfsname():
    return vfsname.decode('utf-8')