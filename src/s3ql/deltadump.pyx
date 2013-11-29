'''
deltadump.pyx - this file is part of S3QL (http://s3ql.googlecode.com)

Copyright (C) Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU GPLv3.
'''

# Analysis of Cython code not really working yet
#@PydevCodeAnalysisIgnore

from cpython.long cimport PyLong_AsVoidPtr
from cpython.exc cimport PyErr_NoMemory
from libc.stdio cimport (FILE, const_char, const_void, fclose as fclose_c,
                         fwrite as fwrite_c, fread as fread_c, ftell)
from libc.string cimport strerror
from libc.errno cimport errno
from libc.stdlib cimport calloc as calloc_c, free as free_c
from libc.stdint cimport (int64_t, uint8_t, uint16_t, uint32_t, uint64_t)
from posix.unistd cimport dup, lseek, SEEK_SET

# These import are not (yet?) in the Cython provided cpython module
cdef extern from *:
    object PyUnicode_FromString(const_char *u)

cdef extern from 'stdint.h' nogil:
    enum: UINT8_MAX
    enum: UINT16_MAX
    enum: UINT32_MAX

cdef extern from 'stdio.h' nogil:
    FILE * fdopen(int fd, const_char * mode)
    int fflush(FILE * stream)
    int fileno(FILE * stream)

cdef extern from 'endian_indep.h' nogil:
    uint64_t htole64(uint64_t host_64bits)
    uint64_t le64toh(uint64_t little_endian_64bits)

cdef extern from 'sqlite3.h' nogil:
    ctypedef int sqlite3
    ctypedef int sqlite3_stmt
    ctypedef int64_t sqlite3_int64

    const_char *sqlite3_errmsg(sqlite3*)
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
    const_char *sqlite3_compileoption_get(int N)
    const_char *sqlite3_libversion()
    int sqlite3_close(sqlite3*)
    int sqlite3_open_v2(const_char *filename, sqlite3 **ppDb,
                        int flags, const_char *zVfs)
    int sqlite3_extended_result_codes(sqlite3*, int onoff)
    void SQLITE_TRANSIENT(void *)

    enum:
        SQLITE_OK
        SQLITE_DONE
        SQLITE_ROW
        SQLITE_NULL
        SQLITE_OPEN_READWRITE
        SQLITE_OPEN_READONLY

from contextlib import ExitStack
import apsw
import os
from .logging import logging # Ensure use of custom logger class
import itertools
import sys

log = logging.getLogger(__name__)

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
        raise_from_errno(IOError)

    return len_

cdef inline int fread(void * buf, size_t len_, FILE * fp) except -1:
    '''Call libc's fread() and raise exception on failure'''

    if fread_c(buf, len_, 1, fp) != 1:
        raise_from_errno(IOError)
    return len_

cdef free(void * ptr):
    '''Call libc.free()

    This is a Python wrapper, so that we can call free in e.g.
    a lambda expression.
    '''

    free_c(ptr)
    return None

cdef int raise_from_errno(err_class=OSError) except -1:
    '''Raise OSError for current errno value'''

    raise err_class(errno, PyUnicode_FromString(strerror(errno)))
    
cdef int fclose(FILE * fp) except -1:
    '''Call libc.fclose() and raise exception on failure'''

    cdef ssize_t off

    # Explicitly flush data that needs to be written. This is
    # important, so that we can safely reposition the fd position
    # below (which is necessary in case there is cached input data)
    if fflush(fp) != 0:
        raise_from_errno()
    
    # Reposition FD to position of FILE*, otherwise next read from FD will miss
    # data currently in stream buffer. It seems that call to fflush() achieves
    # the same thing, but this does not seem to be documented so we don't rely
    # on it.
    off = ftell(fp)
    if off == -1:
        raise_from_errno()

    if lseek(fileno(fp), off, SEEK_SET) != off:
        raise_from_errno()

    if fclose_c(fp) != 0:
        raise_from_errno()

    return 0

cdef void * calloc(size_t cnt, size_t size) except NULL:
    '''Call libc.calloc and raise exception on failure'''

    cdef void * ptr

    ptr = calloc_c(cnt, size)

    if ptr is NULL:
        PyErr_NoMemory()

    return ptr


cdef int SQLITE_CHECK_RC(int rc, int success, sqlite3* db) except -1:
    '''Raise correct exception if *rc* != *success*'''
    
    if rc != success:
        exc = apsw.exceptionfor(rc)
        raise type(exc)(PyUnicode_FromString(sqlite3_errmsg(db)))

    return 0


cdef int prep_columns(columns, int** col_types_p, int** col_args_p) except -1:
    '''Allocate col_types and col_args, return number of columns

    Both arrays are allocated dynamically, caller has to ensure
    that they're freed again.
    '''
    cdef int col_count, *col_types, *col_args
    
    col_count = len(columns)
    col_types = < int *> calloc(col_count, sizeof(int))
    col_args = < int *> calloc(col_count, sizeof(int))

    # Initialize col_args and col_types   
    for i in range(col_count):
        if columns[i][1] not in (BLOB, INTEGER, TIME):
            raise ValueError("Invalid type for column %d" % i)
        col_types[i] = columns[i][1]

        if len(columns[i]) == 3:
            col_args[i] = columns[i][2]
        else:
            col_args[i] = 0

    col_types_p[0] = col_types
    col_args_p[0] = col_args
    return col_count

cdef FILE* dup_to_fp(fh, const_char* mode) except NULL:
    '''Duplicate fd from *fh* and open as FILE*'''

    cdef int fd

    fd = dup(fh.fileno())
    if fd == -1:
        raise_from_errno()

    fp = fdopen(fd, mode)
    if fp == NULL:
        raise_from_errno()

    return fp

def check_sqlite():
    '''Check if deltadump and apsw module use compatible SQLite code.

    This functions look at versions and compile options of the SQLite
    code used by the *apsw* module and the *deltadump* module. If they
    do not match exactly, a `RuntimeError` is raised.

    Only if both modules use the same SQLite version compiled with the
    same options can the database object be shared between *apsw* and
    *deltadump*.
    '''
    
    cdef const_char *buf

    apsw_sqlite_version = apsw.sqlitelibversion()
    s3ql_sqlite_version = PyUnicode_FromString(sqlite3_libversion())
    log.debug('apsw sqlite version: %s, '
              's3ql sqlite version: %s',
              apsw_sqlite_version,
              s3ql_sqlite_version)
    if apsw_sqlite_version != s3ql_sqlite_version:
        raise RuntimeError('SQLite version mismatch between APSW and S3QL '
                           '(%s vs %s)' % (apsw_sqlite_version, s3ql_sqlite_version))
    
    apsw_sqlite_options = set(apsw.compile_options)
    s3ql_sqlite_options = set()
    for idx in itertools.count(0):
        buf = sqlite3_compileoption_get(idx)
        if buf is NULL:
            break
        s3ql_sqlite_options.add(PyUnicode_FromString(buf))

    log.debug('apsw sqlite compile options: %s, '
              's3ql sqlite compile options: %s',
              apsw_sqlite_options,
              s3ql_sqlite_options)
    if apsw_sqlite_options != s3ql_sqlite_options:
        raise RuntimeError('SQLite code used by APSW was compiled with different '
                           'options than SQLite code available to S3QL! '
                           'Differing settings: + %s, - %s' %
                           (apsw_sqlite_options - s3ql_sqlite_options,
                           s3ql_sqlite_options - apsw_sqlite_options))

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

    This function will open a separate connection to the database, so
    the *db* connection should not be in EXCLUSIVE locking mode.
    (Using a separate connection avoids the requirement on the *apsw*
    and *deltadump* modules be linked against against binary
    compatible SQLite libraries).
    '''

    cdef sqlite3 *sqlite3_db
    cdef sqlite3_stmt *stmt
    cdef int *col_types, *col_args, col_count, rc, i, len_
    cdef int64_t *int64_prev, int64, tmp
    cdef FILE *fp
    cdef const_void *buf
    cdef int64_t row_count

    if db.file == ':memory:':
        raise ValueError("Can't access in-memory databases")

    with ExitStack() as cm:
        # Get SQLite connection
        log.debug('Opening connection to %s', db.file)
        dbfile_b = db.file.encode(sys.getfilesystemencoding(), 'surrogateescape')
        SQLITE_CHECK_RC(sqlite3_open_v2(dbfile_b, &sqlite3_db,
                                        SQLITE_OPEN_READONLY, NULL),
                        SQLITE_OK, sqlite3_db)
        cm.callback(lambda: SQLITE_CHECK_RC(sqlite3_close(sqlite3_db),
                                            SQLITE_OK, sqlite3_db))
        SQLITE_CHECK_RC(sqlite3_extended_result_codes(sqlite3_db, 1),
                        SQLITE_OK, sqlite3_db)

        # Get FILE* for buffered reading from *fh*
        fp = dup_to_fp(fh, b'wb')
        cm.callback(lambda: fclose(fp))

        # Allocate col_args and col_types 
        col_count = prep_columns(columns, &col_types, &col_args)
        cm.callback(lambda: free(col_args))
        cm.callback(lambda: free(col_types))
        
        # Allocate int64_prev
        int64_prev = < int64_t *> calloc(len(columns), sizeof(int64_t))
        cm.callback(lambda: free(int64_prev))

        # Prepare statement
        col_names = [ x[0] for x in columns ]
        query = ("SELECT %s FROM %s ORDER BY %s " %
                 (', '.join(col_names), table, order)).encode('utf-8')
        SQLITE_CHECK_RC(sqlite3_prepare_v2(sqlite3_db, query, -1, &stmt, NULL),
                        SQLITE_OK, sqlite3_db)
        cm.callback(lambda: SQLITE_CHECK_RC(sqlite3_finalize(stmt),
                                            SQLITE_OK, sqlite3_db))

        row_count = db.get_val("SELECT COUNT(rowid) FROM %s" % table)
        log.debug('dump_table(%s): writing %d rows', table, row_count)
        write_integer(row_count, fp)

        # Iterate through rows
        while True:
            rc = sqlite3_step(stmt)
            if rc == SQLITE_DONE:
                break
            SQLITE_CHECK_RC(rc, SQLITE_ROW, sqlite3_db)

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

def load_table(table, columns, db, fh, trx_rows=5000):
    '''Load *columns* of *table* from *fh*

    *db* is an `s3ql.Connection` instance for the database.
    
    *columns* must be the same list of 3-tuples that was passed to
    `dump_table` when creating the dump stored in *fh*.

    This function will open a separate connection to the database, so
    the *db* connection should not be in EXCLUSIVE locking mode.
    (Using a separate connection avoids the requirement on the *apsw*
    and *deltadump* modules be linked against against binary
    compatible SQLite libraries).

    When writing into the table, a new transaction will be started
    every *trx_rows* rows.
    '''

    cdef sqlite3 *sqlite3_db
    cdef sqlite3_stmt *stmt, *begin_stmt, *commit_stmt
    cdef int *col_types, *col_args, col_count, rc, len_, i, j
    cdef int64_t *int64_prev
    cdef FILE *fp
    cdef void *buf
    cdef int64_t row_count, int64

    if db.file == ':memory:':
        raise ValueError("Can't access in-memory databases")

    with ExitStack() as cm:
        # Get SQLite connection
        log.debug('Opening connection to %s', db.file)
        dbfile_b = db.file.encode(sys.getfilesystemencoding(), 'surrogateescape')
        SQLITE_CHECK_RC(sqlite3_open_v2(dbfile_b, &sqlite3_db,
                                        SQLITE_OPEN_READWRITE, NULL),
                        SQLITE_OK, sqlite3_db)
        cm.callback(lambda: SQLITE_CHECK_RC(sqlite3_close(sqlite3_db),
                                            SQLITE_OK, sqlite3_db))
        SQLITE_CHECK_RC(sqlite3_extended_result_codes(sqlite3_db, 1),
                        SQLITE_OK, sqlite3_db)

        # Copy settings
        for pragma in ('synchronous', 'foreign_keys'):
            val = db.get_val('PRAGMA %s' % pragma)
            cmd = ('PRAGMA %s = %s' % (pragma, val)).encode('utf-8')
            SQLITE_CHECK_RC(sqlite3_prepare_v2(sqlite3_db, cmd, -1, &stmt, NULL),
                            SQLITE_OK, sqlite3_db)
            try:
                rc = sqlite3_step(stmt)
                if rc == SQLITE_ROW:
                    rc = sqlite3_step(stmt)
                SQLITE_CHECK_RC(rc, SQLITE_DONE, sqlite3_db)
            finally:
                SQLITE_CHECK_RC(sqlite3_finalize(stmt), SQLITE_OK, sqlite3_db)

        # Get FILE* for buffered reading from *fh*
        fp = dup_to_fp(fh, b'rb')
        cm.callback(lambda: fclose(fp))

        # Allocate col_args and col_types 
        col_count = prep_columns(columns, &col_types, &col_args)
        cm.callback(lambda: free(col_args))
        cm.callback(lambda: free(col_types))

        # Allocate int64_prev
        int64_prev = < int64_t *> calloc(len(columns), sizeof(int64_t))
        cm.callback(lambda: free(int64_prev))

        # Prepare INSERT statement
        col_names = [ x[0] for x in columns ]
        query = ("INSERT INTO %s (%s) VALUES(%s)"
                 % (table, ', '.join(col_names),
                    ', '.join('?' * col_count))).encode('utf-8')
        SQLITE_CHECK_RC(sqlite3_prepare_v2(sqlite3_db, query, -1, &stmt, NULL),
                        SQLITE_OK, sqlite3_db)
        cm.callback(lambda: SQLITE_CHECK_RC(sqlite3_finalize(stmt),
                                                 SQLITE_OK, sqlite3_db))

        # Prepare BEGIN statement
        query = b'BEGIN TRANSACTION'
        SQLITE_CHECK_RC(sqlite3_prepare_v2(sqlite3_db, query, -1, &begin_stmt, NULL),
                        SQLITE_OK, sqlite3_db)
        cm.callback(lambda: SQLITE_CHECK_RC(sqlite3_finalize(begin_stmt),
                                                 SQLITE_OK, sqlite3_db))

        # Prepare COMMIT statement
        query = b'COMMIT TRANSACTION'
        SQLITE_CHECK_RC(sqlite3_prepare_v2(sqlite3_db, query, -1, &commit_stmt, NULL),
                        SQLITE_OK, sqlite3_db)
        cm.callback(lambda: SQLITE_CHECK_RC(sqlite3_finalize(commit_stmt),
                                                 SQLITE_OK, sqlite3_db))

        buf = calloc(MAX_BLOB_SIZE, 1)
        cm.callback(lambda: free(buf))
        read_integer(&row_count, fp)
        log.debug('load_table(%s): reading %d rows', table, row_count)

        # Start transaction
        SQLITE_CHECK_RC(sqlite3_step(begin_stmt), SQLITE_DONE, sqlite3_db)
        cm.callback(lambda: SQLITE_CHECK_RC(sqlite3_step(commit_stmt),
                                                 SQLITE_DONE, sqlite3_db))
        SQLITE_CHECK_RC(sqlite3_reset(begin_stmt), SQLITE_OK, sqlite3_db)

        # Iterate through rows
        for i in range(row_count):
            for j in range(col_count):
                if col_types[j] == _INTEGER:
                    read_integer(&int64, fp)
                    int64 += col_args[j] + int64_prev[j]
                    int64_prev[j] = int64
                    SQLITE_CHECK_RC(sqlite3_bind_int64(stmt, j + 1, int64),
                                    SQLITE_OK, sqlite3_db)

                if col_types[j] == _TIME:
                    read_integer(&int64, fp)
                    int64 += col_args[j] + int64_prev[j]
                    int64_prev[j] = int64
                    SQLITE_CHECK_RC(sqlite3_bind_double(stmt, j + 1, int64 / time_scale),
                                    SQLITE_OK, sqlite3_db)

                elif col_types[j] == _BLOB:
                    if col_args[j] == 0:
                        read_integer(&int64, fp)
                        len_ = int64_prev[j] + int64
                        int64_prev[j] = len_
                    else:
                        len_ = col_args[j]

                    if len_ > MAX_BLOB_SIZE:
                        raise RuntimeError('BLOB too large to read (%d vs %d)', len_, MAX_BLOB_SIZE)

                    if len_ != 0:
                        fread(buf, len_, fp)
                        
                    SQLITE_CHECK_RC(sqlite3_bind_blob(stmt, j + 1, buf, len_, SQLITE_TRANSIENT),
                                    SQLITE_OK, sqlite3_db)
                
            SQLITE_CHECK_RC(sqlite3_step(stmt), SQLITE_DONE, sqlite3_db)
            SQLITE_CHECK_RC(sqlite3_reset(stmt), SQLITE_OK, sqlite3_db)

            # Commit every once in a while
            if i % trx_rows == 0:
                # This isn't 100% ok -- if we have an exception in step(begin_stmt),
                # we the cleanup handler will execute the commit statement again
                # without an active transaction.
                SQLITE_CHECK_RC(sqlite3_step(commit_stmt), SQLITE_DONE, sqlite3_db)
                SQLITE_CHECK_RC(sqlite3_step(begin_stmt), SQLITE_DONE, sqlite3_db)
                SQLITE_CHECK_RC(sqlite3_reset(commit_stmt), SQLITE_OK, sqlite3_db)
                SQLITE_CHECK_RC(sqlite3_reset(begin_stmt), SQLITE_OK, sqlite3_db)
                

cdef inline int write_integer(int64_t int64, FILE * fp) except -1:
    '''Write *int64* into *fp*, using as little space as possible

    Return the number of bytes written, or -1 on error.
    '''

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

    fwrite(&int8, 1, fp)
    if len_ != 0:
        uint64 = htole64(uint64)
        fwrite(&uint64, len_, fp)

    return len_ + 1

cdef inline int read_integer(int64_t * out, FILE * fp) except -1:
    '''Read integer written using `write_integer` from *fp*

    Return the number of bytes read, or -1 on error.
    '''

    cdef uint8_t int8
    cdef size_t len_
    cdef uint64_t uint64
    cdef char negative

    fread(&int8, 1, fp)

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
        fread(&uint64, len_, fp)
        uint64 = le64toh(uint64)

    if negative == 1:
        out[0] = - < int64_t > uint64
    else:
        out[0] = < int64_t > uint64

    return len_ + 1

