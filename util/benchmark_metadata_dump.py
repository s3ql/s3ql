#!/usr/bin/env python

from __future__ import print_function, division, absolute_import

from s3ql.deltadump import INTEGER, BLOB, TIME, dump_table
import logging
import zlib
import bz2
import cPickle as pickle
import lzma
import tempfile
import apsw
from s3ql.database import Connection
import time
import os
import sys

BUFSIZE = 128 * 1024

logging.basicConfig()
log = logging.getLogger()

db = Connection(sys.argv[1])

tables_to_dump = [('objects', 'id'), ('blocks', 'id'),
                  ('inode_blocks', 'inode, blockno'),
                  ('inodes', 'id'), ('symlink_targets', 'inode'),
                  ('names', 'id'), ('contents', 'parent_inode, name_id'),
                  ('ext_attributes', 'inode, name_id')
                  ]

DUMP_SPEC = [
             ('objects', 'id', (('id', INTEGER, 1),
                                ('size', INTEGER))),
             
             ('blocks', 'id', (('id', INTEGER, 1),
                             ('hash', BLOB, 32),
                             ('size', INTEGER),
                             ('obj_id', INTEGER, 1))),
             
             ('inodes', 'id', (('id', INTEGER, 1),
                               ('uid', INTEGER),
                               ('gid', INTEGER),
                               ('mode', INTEGER),
                               ('mtime', TIME),
                               ('atime', TIME),
                               ('ctime', TIME),
                               ('size', INTEGER),
                               ('rdev', INTEGER),
                               ('locked', INTEGER))),
             
             ('inode_blocks', 'inode, blockno', 
              (('inode', INTEGER),
               ('blockno', INTEGER, 1),
               ('block_id', INTEGER, 1))),
             
             ('symlink_targets', 'inode', (('inode', INTEGER, 1),
                                           ('target', BLOB))),
           
             ('names', 'id', (('id', INTEGER, 1),
                              ('name', BLOB))),
           
             ('contents', 'parent_inode, name_id',
              (('name_id', INTEGER, 1),
               ('inode', INTEGER, 1),
               ('parent_inode', INTEGER))),
             
             ('ext_attributes', 'inode', (('inode', INTEGER),
                                          ('name_id', INTEGER),
                                          ('value', BLOB))),      
] 
DUMP_SPEC2 = [
             ('objects', 'id', (('id', INTEGER, 1),
                                ('size', INTEGER),
                                ('refcount', INTEGER))),
             
             ('blocks', 'id', (('id', INTEGER, 1),
                             ('hash', BLOB, 32),
                             ('size', INTEGER),('refcount', INTEGER),
                             ('obj_id', INTEGER, 1))),
             
             ('inodes', 'id', (('id', INTEGER, 1),
                               ('uid', INTEGER),
                               ('gid', INTEGER),
                               ('mode', INTEGER),
                               ('mtime', TIME),
                               ('atime', TIME),
                               ('ctime', TIME),('refcount', INTEGER),
                               ('size', INTEGER),
                               ('rdev', INTEGER),
                               ('locked', INTEGER))),
             
             ('inode_blocks', 'inode, blockno', 
              (('inode', INTEGER),
               ('blockno', INTEGER, 1),
               ('block_id', INTEGER, 1))),
             
             ('symlink_targets', 'inode', (('inode', INTEGER, 1),
                                           ('target', BLOB))),
           
             ('names', 'id', (('id', INTEGER, 1),('refcount', INTEGER),
                              ('name', BLOB))),
           
             ('contents', 'parent_inode, name_id',
              (('name_id', INTEGER, 1),
               ('inode', INTEGER, 1),
               ('parent_inode', INTEGER))),
             
             ('ext_attributes', 'inode', (('inode', INTEGER),
                                          ('name_id', INTEGER),
                                          ('value', BLOB))),      
] 

def dump_pickle(db, tables_to_dump):
    fh = tempfile.TemporaryFile()
    pickler = pickle.Pickler(fh, pickle.HIGHEST_PROTOCOL)
    bufsize = 256
    buf = range(bufsize)

    columns = dict()
    for (table, _) in tables_to_dump:
        columns[table] = list()
        for row in db.query('PRAGMA table_info(%s)' % table):
            columns[table].append(row[1])

    pickler.dump((tables_to_dump, columns))
    
    for (table, order) in tables_to_dump:
        log.info('..%s..' % table)
        pickler.clear_memo()
        i = 0
        for row in db.query('SELECT %s FROM %s ORDER BY %s' 
                            % (','.join(columns[table]), table, order)):
            buf[i] = row
            i += 1
            if i == bufsize:
                pickler.dump(buf)
                pickler.clear_memo()
                i = 0

        if i != 0:
            pickler.dump(buf[:i])
        
        pickler.dump(None)

    return fh

def dump_delta(db, tables_to_dump):
    fh = tempfile.TemporaryFile()
    for (table, order, columns) in DUMP_SPEC:
        print(table)
        dump_table(table, order, columns, db=db, fh=fh)
    return fh

def dump_delta_full(db, tables_to_dump):
    fh = tempfile.TemporaryFile()
    for (table, order, columns) in DUMP_SPEC2:
        print(table)
        dump_table(table, order, columns, db=db, fh=fh)
    return fh

def dump_apsw(db, tables_to_dump):
    fh = tempfile.NamedTemporaryFile()
    sh = apsw.Shell(db=db.conn)
    sh.process_command('.output %s' % fh.name)
    for (table, _) in tables_to_dump:
        sh.process_command('.dump %s' % table)
    del sh
    return fh

def dump_apsw_csv(db, tables_to_dump):
    fh = tempfile.NamedTemporaryFile()
    sh = apsw.Shell(db=db.conn)
    sh.process_command('.output %s' % fh.name)
    sh.process_command('.mode csv')
    for (table, order) in tables_to_dump:
        sh.process_sql('select * from %s ORDER BY %s' % (table, order))
    del sh
    return fh
    

stamp = time.time()
db.execute('CREATE INDEX tmp1 ON blocks(obj_id)')
db.execute('CREATE INDEX tmp2 ON inode_blocks(block_id)')
db.execute('CREATE INDEX tmp3 ON contents(inode)')
db.execute('CREATE INDEX tmp4 ON contents(name_id)')
db.execute('UPDATE objects SET refcount='
             '(SELECT COUNT(obj_id) FROM blocks WHERE obj_id = objects.id)')
db.execute('UPDATE blocks SET refcount='
             '(SELECT COUNT(block_id) FROM inode_blocks WHERE block_id = blocks.id)')
db.execute('UPDATE inodes SET refcount='
             '(SELECT COUNT(inode) FROM contents WHERE inode = inodes.id)')
db.execute('UPDATE names SET refcount='
             '(SELECT COUNT(name_id) FROM contents WHERE name_id = names.id)'
             '+ (SELECT COUNT(name_id) FROM ext_attributes WHERE name_id = names.id)')
for idx in ('tmp1', 'tmp2', 'tmp3', 'tmp4'):
    db.execute('DROP INDEX %s' % idx)
load_time = time.time() - stamp
print('%-16s: %6.2f sek' % ('Reference recovery', load_time))

for (fn, label) in ((dump_delta_full, 'delta_f'),
                    (dump_delta, 'delta'),
                    (dump_pickle, 'pickle'),
                    (dump_apsw_csv, 'apsw_csv'),
                    (dump_apsw, 'apsw_sql')
                    ):
    #prof = cProfile.Profile()
    stamp = time.time()
    #fh = prof.runcall(fn, db, tables_to_dump)
    #cProfile.runctx("fh = fn(db, tables_to_dump)", globals(), locals(),
    #                "%s_profile.prof" % label)
    fh = fn(db, tables_to_dump)
    dump_time = time.time() - stamp
    fh.seek(0, os.SEEK_END)
    print('%-16s: %6.2f sek, %6.2f MB' % (label, dump_time, fh.tell() / 1024**2))

    #tmp = tempfile.NamedTemporaryFile()
    #prof.dump_stats(tmp.name)
    #out = open('%s_profile.txt' % label, 'w')
    #p = pstats.Stats('%s_profile.prof' % label, stream=out)
    #p.strip_dirs()
    #p.sort_stats('cumulative')
    #p.print_stats(50)
    #p.sort_stats('time')
    #p.print_stats(50)
    #out.close()
    
    for (compress, label2) in ((zlib.compressobj(9), 'zlib'),
                               (bz2.BZ2Compressor(9), 'BZIP2'),
                               (lzma.LZMACompressor(options={ 'level': 7 }), 'LZMA')
                               ):
        size = 0
        fh.seek(0)
        stamp = time.time()
        while True:
            buf = fh.read(BUFSIZE)
            if not buf:
                break
            buf = compress.compress(buf)
            if buf:
                size += len(buf)
        buf = compress.flush()
        if buf:
            size += len(buf)
        compr_time = time.time() - stamp
        print('%-16s: %6.2f sek, %6.2f MB' % ('%s + %s' % (label, label2),
                                           compr_time + dump_time,
                                           size / 1024**2))
        
    
