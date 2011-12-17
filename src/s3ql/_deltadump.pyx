'''
_deltadump.pyx - this file is part of S3QL (http://s3ql.googlecode.com)

Copyright (C) Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU GPLv3.
'''

# Analysis of Cython code not really working yet
#@PydevCodeAnalysisIgnore

from __future__ import print_function, division

from cpython.long cimport PyLong_AsVoidPtr
from cpython.exc cimport PyErr_NoMemory
from libc.stdio cimport (FILE, const_char, const_void, fclose as fclose_c,
                         fwrite as fwrite_c, fread as fread_c, ftell)
from libc.string cimport strerror
from libc.errno cimport errno
from libc.stdlib cimport calloc as calloc_c, free as free_c
from libc.stdint cimport (int64_t, uint8_t, uint16_t, uint32_t, uint64_t)
from posix.unistd cimport dup, lseek, SEEK_SET

cdef extern from 'stdint.h' nogil:
    enum: UINT8_MAX
    enum: UINT16_MAX
    enum: UINT32_MAX

cdef extern from 'stdio.h' nogil:
    FILE * fdopen(int fd, const_char * mode)
    int fileno(FILE * stream)

cdef extern from 'endian.h' nogil:
    uint64_t htole64(uint64_t host_64bits)
    uint64_t le64toh(uint64_t little_endian_64bits)

cdef extern from 'sqlite3.h' nogil:
    ctypedef int sqlite3
    ctypedef int sqlite3_stmt
    ctypedef int64_t sqlite3_int64

    int sqlite3_prepare_v2(sqlite3 * db,
                           char * zSql,
                           int nByte,
                           sqlite3_stmt ** ppStmt,
                           char ** pzTail)
    int sqlite3_step(sqlite3_stmt *)
    sqlite3_int64 sqlite3_column_int64(sqlite3_stmt * , int iCol)
    const_void * sqlite3_column_blob(sqlite3_stmt * , int iCol)
    int sqlite3_column_bytes(sqlite3_stmt * , int iCol)
    int sqlite3_bind_blob(sqlite3_stmt * , int iCol, const_void * , int n, void(*)(void *))
    int sqlite3_bind_int64(sqlite3_stmt * , int iCol, sqlite3_int64)
    int sqlite3_reset(sqlite3_stmt * pStmt)
    int sqlite3_finalize(sqlite3_stmt * pStmt)
    int sqlite3_column_type(sqlite3_stmt * , int iCol)
    double sqlite3_column_double(sqlite3_stmt * , int iCol)
    int sqlite3_bind_double(sqlite3_stmt * , int, double)

    void SQLITE_TRANSIENT(void *)

    enum:
        SQLITE_OK
        SQLITE_DONE
        SQLITE_ROW
        SQLITE_NULL

from .cleanup_manager import CleanupManager
import apsw
import os
import logging

log = logging.getLogger('deltadump')

# Column types
cdef int _INTEGER = 1
cdef int _BLOB = 2
cdef int _TIME = 3

# Make column types available as Python objects
INTEGER = _INTEGER
BLOB = _BLOB
TIME = _TIME

# Integer length codes
cdef uint8_t INT8 = 127
cdef uint8_t INT16 = 126
cdef uint8_t INT32 = 125
cdef uint8_t INT64 = 124

# Maximum size of BLOBs
MAX_BLOB_SIZE = 4096

# Scale factor from time floats to integers
# 1e9 would be perfect, but introduces rounding errors
cdef double time_scale = 1 << 30

cdef inline int fwrite(const_void * buf, size_t len_, FILE * fp) except -1:
    '''Call libc's fwrite() and raise exception on failure'''

    if fwrite_c(buf, len_, 1, fp) != 1:
        raise IOError(errno, strerror(errno))

cdef inline int fread(void * buf, size_t len_, FILE * fp) except -1:
    '''Call libc's fread() and raise exception on failure'''

    if fread_c(buf, len_, 1, fp) != 1:
        raise IOError(errno, strerror(errno))

cdef int free(void * ptr) except -1:
    '''Call libc.free() and return None'''

    free_c(ptr)

cdef int sqlite3_finalize_p(sqlite3_stmt * stmt) except -1:
    '''Call sqlite3_finalize and raise exception on failure'''

    rc = sqlite3_finalize(stmt)
    if rc != SQLITE_OK:
        raise apsw.exceptionfor(rc)

cdef int fclose(FILE * fp) except -1:
    '''Call libc.fclose() and raise exception on failure'''

    cdef ssize_t off

    # Reposition FD to position of FILE*, otherwise next read from FD will miss
    # data currently in stream buffer. It seems that call to fflush() achieves
    # the same thing, but this does not seem to be documented so we don't rely
    # on it.
    off = ftell(fp)
    if off == -1:
        raise OSError(errno, strerror(errno))

    if lseek(fileno(fp), off, SEEK_SET) != off:
        raise OSError(errno, strerror(errno))

    if fclose_c(fp) != 0:
        raise OSError(errno, strerror(errno))

cdef void * calloc(size_t cnt, size_t size) except NULL:
    '''Call libc.calloc and raise exception on failure'''

    cdef void * ptr

    ptr = calloc_c(cnt, size)

    if ptr is NULL:
        PyErr_NoMemory()

    return ptr

def dump_table(table, order, columns, db, fh):
    '''Dump *columns* of *table* into *fh*

    *order* specifies the order in which the rows are written and must be a
    string that can be inserted after the "ORDER BY" clause in an SQL SELECT
    statement.
    
    *db* is an `s3ql.Connection` instance for the database.
    
    *columns* must a list of 3-tuples, one for each column that should be
    stored. The first element of the tuple must contain the column name and the
    second element the type of data stored in the column (`INTEGER`, `TIME`
    or `BLOB`). Times will be converted to nanosecond integers.
    
    For integers and seconds, the third tuple element specifies the expected
    change of the values between rows. For blobs it can be either zero
    (indicating variable length columns) or an integer specifying the length of
    the column values in bytes.
    '''

    return _dump_or_load(table, order, columns, db, fh)

def load_table(table, columns, db, fh):
    '''Load *columns* of *table* from *fh*

    Parameters are described in the docstring of the `dump_table` function.
    '''

    return _dump_or_load(table, None, columns, db, fh)


def _dump_or_load(table, order, columns, db, fh):
    '''Dump or load *columns* of *table*
    
    If *order* is None, load data from *fh* into *db*.
    
    If *order* is not None, data will be read from *db* and written
    into *fh*. In this case, *order* specifies the order in which
    the rows are written and must be a string that can be inserted
    after the "ORDER BY" clause in an SQL SELECT statement.
    
    *db* is an `s3ql.Connection` instance for the database.
    
    *columns* must a list of 3-tuples, one for each column that should be stored
    or retrieved. The first element of the tuple must contain the column name
    and the second element the type of data stored in the column (`INTEGER`,
    `TIME` or `BLOB`). Times will be converted to nanosecond integers.
    
    For integers and times, the third tuple element specifies the expected
    change of the values between rows. For blobs it can be either zero
    (indicating variable length columns) or an integer specifying the length of
    the column values in bytes.    
    '''

    cdef sqlite3 * sqlite3_db
    cdef sqlite3_stmt * stmt
    cdef int * col_types, *col_args, col_count, rc, fd
    cdef int64_t * int64_prev
    cdef FILE * fp
    cdef void * buf
    cdef int64_t row_count

    sqlite3_db = < sqlite3 *> PyLong_AsVoidPtr(db.conn.sqlite3pointer())

    with CleanupManager(log) as cleanup:
        fd = dup(fh.fileno())
        if fd == -1:
            raise OSError(errno, strerror(errno))
        fp = fdopen(fd, 'r+b')
        if fp == NULL:
            raise OSError(errno, strerror(errno))
        cleanup.register(lambda: fclose(fp))

        # Allocate col_args and col_types 
        col_count = len(columns)
        col_types = < int *> calloc(col_count, sizeof(int))
        cleanup.register(lambda: free(col_types))
        col_args = < int *> calloc(col_count, sizeof(int))
        cleanup.register(lambda: free(col_args))

        # Initialize col_args and col_types   
        for i in range(col_count):
            if columns[i][1] not in (BLOB, INTEGER, TIME):
                raise ValueError("Invalid type for column %d" % i)
            col_types[i] = columns[i][1]

            if len(columns[i]) == 3:
                col_args[i] = columns[i][2]
            else:
                col_args[i] = 0

        # Allocate int64_prev
        int64_prev = < int64_t *> calloc(len(columns), sizeof(int64_t))
        cleanup.register(lambda: free(int64_prev))

        # Prepare statement
        col_names = [ x[0] for x in columns ]
        if order is None:
            query = ("INSERT INTO %s (%s) VALUES(%s)"
                 % (table,
                    ', '.join(col_names),
                    ', '.join('?' * col_count)))
        else:
            query = ("SELECT %s FROM %s ORDER BY %s " %
                     (', '.join(col_names), table, order))
        rc = sqlite3_prepare_v2(sqlite3_db, query, -1, & stmt, NULL)
        if rc != SQLITE_OK:
            raise apsw.exceptionfor(rc)
        cleanup.register(lambda: sqlite3_finalize_p(stmt))

        # Dump or load data as requested
        if order is None:
            buf = calloc(MAX_BLOB_SIZE, 1)
            cleanup.register(lambda: free(buf))
            read_integer(& row_count, fp)
            log.debug('_dump_or_load(%s): reading %d rows', table, row_count)
            _load_table(col_types, col_args, int64_prev, col_count,
                        row_count, stmt, fp, buf)
        else:
            row_count = db.get_val("SELECT COUNT(rowid) FROM %s" % table)
            log.debug('_dump_or_load(%s): writing %d rows', table, row_count)
            write_integer(row_count, fp)
            _dump_table(col_types, col_args, int64_prev, col_count, stmt, fp)


cdef _dump_table(int * col_types, int * col_args, int64_t * int64_prev,
                 int col_count, sqlite3_stmt * stmt, FILE * fp):

    cdef const_void * buf
    cdef int rc, i, len_
    cdef int64_t int64, tmp

    # Iterate through rows
    while True:
        rc = sqlite3_step(stmt)
        if rc == SQLITE_DONE:
            break
        elif rc != SQLITE_ROW:
            raise apsw.exceptionfor(rc)

        for i in range(col_count):
            if sqlite3_column_type(stmt, i) is SQLITE_NULL:
                raise ValueError("Can't dump NULL values")

            if col_types[i] == _INTEGER:
                int64 = sqlite3_column_int64(stmt, i)
                tmp = int64
                int64 -= int64_prev[i] + col_args[i]
                int64_prev[i] = tmp
                write_integer(int64, fp)

            elif col_types[i] == _TIME:
                int64 = < int64_t > (sqlite3_column_double(stmt, i) * time_scale)
                tmp = int64
                int64 -= int64_prev[i] + col_args[i]
                int64_prev[i] = tmp
                write_integer(int64, fp)

            elif col_types[i] == _BLOB:
                buf = sqlite3_column_blob(stmt, i)
                len_ = sqlite3_column_bytes(stmt, i)
                if len_ > MAX_BLOB_SIZE:
                        raise ValueError('Can not dump BLOB of size %d (max: %d)',
                                         len_, MAX_BLOB_SIZE)
                if col_args[i] == 0:
                    write_integer(len_ - int64_prev[i], fp)
                    int64_prev[i] = len_
                elif len_ != col_args[i]:
                    raise ValueError("Length %d != %d in column %d" % (len_, col_args[i], i))

                if len_ != 0:
                    fwrite(buf, len_, fp)

cdef _load_table(int * col_types, int * col_args, int64_t * int64_prev,
                 int col_count, int row_count, sqlite3_stmt * stmt,
                 FILE * fp, void * buf):

    cdef int64_t int64
    cdef int rc, len_, i, j

    # Iterate through rows
    for i in range(row_count):
        for j in range(col_count):
            if col_types[j] == _INTEGER:
                read_integer(& int64, fp)
                int64 += col_args[j] + int64_prev[j]
                int64_prev[j] = int64
                rc = sqlite3_bind_int64(stmt, j + 1, int64)
                if rc != SQLITE_OK:
                    raise apsw.exceptionfor(rc)

            if col_types[j] == _TIME:
                read_integer(& int64, fp)
                int64 += col_args[j] + int64_prev[j]
                int64_prev[j] = int64
                rc = sqlite3_bind_double(stmt, j + 1, int64 / time_scale)
                if rc != SQLITE_OK:
                    raise apsw.exceptionfor(rc)

            elif col_types[j] == _BLOB:
                if col_args[j] == 0:
                    read_integer(& int64, fp)
                    len_ = int64_prev[j] + int64
                    int64_prev[j] = len_
                else:
                    len_ = col_args[j]

                if len_ > MAX_BLOB_SIZE:
                    raise RuntimeError('BLOB too large to read (%d vs %d)', len_, MAX_BLOB_SIZE)

                if len_ != 0:
                    fread(buf, len_, fp)

                rc = sqlite3_bind_blob(stmt, j + 1, buf, len_, SQLITE_TRANSIENT)
                if rc != SQLITE_OK:
                    raise apsw.exceptionfor(rc)

        rc = sqlite3_step(stmt)
        if rc != SQLITE_DONE:
            raise apsw.exceptionfor(rc)

        rc = sqlite3_reset(stmt)
        if rc != SQLITE_OK:
            raise apsw.exceptionfor(rc)

cdef inline int write_integer(int64_t int64, FILE * fp) except -1:
    '''Write *int64* into *fp*, using as little space as possible'''

    cdef uint8_t int8
    cdef size_t len_
    cdef uint64_t uint64

    if int64 < 0:
        uint64 = < uint64_t > -int64
        int8 = < uint8_t > 0x80 # Highest bit set
    else:
        uint64 = < uint64_t > int64
        int8 = 0

    if uint64 < 0x80 and uint64 not in (INT8, INT16, INT32, INT64):
        len_ = 0
        int8 += < uint8_t > uint64
    elif uint64 < UINT8_MAX:
        len_ = 1
        int8 += INT8
    elif uint64 < UINT16_MAX:
        len_ = 2
        int8 += INT16
    elif uint64 < UINT32_MAX:
        len_ = 4
        int8 += INT32
    else:
        len_ = 8
        int8 += INT64

    fwrite(& int8, 1, fp)
    if len_ != 0:
        uint64 = htole64(uint64)
        fwrite(& uint64, len_, fp)

cdef inline int read_integer(int64_t * out, FILE * fp) except -1:
    '''Read integer written using `write_integer` from *fp*'''

    cdef uint8_t int8
    cdef size_t len_
    cdef uint64_t uint64
    cdef char negative

    fread(& int8, 1, fp)

    if int8 & 0x80 != 0:
        negative = 1
        int8 = int8 & (~0x80)
    else:
        negative = 0

    if int8 == INT8:
        len_ = 1
    elif int8 == INT16:
        len_ = 2
    elif int8 == INT32:
        len_ = 4
    elif int8 == INT64:
        len_ = 8
    else:
        len_ = 0
        uint64 = int8

    if len_ != 0:
        uint64 = 0
        fread(& uint64, len_, fp)
        uint64 = le64toh(uint64)

    if negative == 1:
        out[0] = - < int64_t > uint64
    else:
        out[0] = < int64_t > uint64
