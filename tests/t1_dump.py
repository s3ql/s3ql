'''
t1_dump.py - this file is part of S3QL (http://s3ql.googlecode.com)

Copyright (c) Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU GPLv3.
'''

from __future__ import division, print_function, absolute_import

import unittest2 as unittest
from s3ql import deltadump
import tempfile
from s3ql.database import Connection
import random
import time

class DumpTests(unittest.TestCase):
    def setUp(self):
        self.src = Connection(":memory:")
        self.dst = Connection(":memory:")
        self.fh = tempfile.TemporaryFile()

        self.create_table(self.src)
        self.create_table(self.dst)

    def test_1_vals_1(self):
        self.fill_vals(self.src)
        dumpspec = (('id', deltadump.INTEGER, 0),)
        deltadump.dump_table(table='test', order='id', columns=dumpspec,
                             db=self.src, fh=self.fh)
        self.fh.seek(0)
        deltadump.load_table(table='test', columns=dumpspec, db=self.dst,
                             fh=self.fh)
        self.compare_tables(self.src, self.dst)

    def test_1_vals_2(self):
        self.fill_vals(self.src)
        dumpspec = (('id', deltadump.INTEGER, 1),)
        deltadump.dump_table(table='test', order='id', columns=dumpspec,
                             db=self.src, fh=self.fh)
        self.fh.seek(0)
        deltadump.load_table(table='test', columns=dumpspec, db=self.dst,
                             fh=self.fh)
        self.compare_tables(self.src, self.dst)

    def test_1_vals_3(self):
        self.fill_vals(self.src)
        dumpspec = (('id', deltadump.INTEGER, -1),)
        deltadump.dump_table(table='test', order='id', columns=dumpspec,
                             db=self.src, fh=self.fh)
        self.fh.seek(0)
        deltadump.load_table(table='test', columns=dumpspec, db=self.dst,
                             fh=self.fh)
        self.compare_tables(self.src, self.dst)

    def test_2_buf_auto(self):
        self.fill_vals(self.src)
        self.fill_buf(self.src)
        dumpspec = (('id', deltadump.INTEGER),
                    ('buf', deltadump.BLOB))
        deltadump.dump_table(table='test', order='id', columns=dumpspec,
                             db=self.src, fh=self.fh)
        self.fh.seek(0)
        deltadump.load_table(table='test', columns=dumpspec, db=self.dst,
                             fh=self.fh)
        self.compare_tables(self.src, self.dst)

    def test_2_buf_fixed(self):
        BUFLEN = 32
        self.fill_vals(self.src)
        self.fill_buf(self.src, BUFLEN)
        dumpspec = (('id', deltadump.INTEGER),
                    ('buf', deltadump.BLOB, BUFLEN))
        deltadump.dump_table(table='test', order='id', columns=dumpspec,
                             db=self.src, fh=self.fh)
        self.fh.seek(0)
        deltadump.load_table(table='test', columns=dumpspec, db=self.dst,
                             fh=self.fh)
        self.compare_tables(self.src, self.dst)

    def test_3_deltas_1(self):
        self.fill_deltas(self.src)
        dumpspec = (('id', deltadump.INTEGER, 0),)
        deltadump.dump_table(table='test', order='id', columns=dumpspec,
                             db=self.src, fh=self.fh)
        self.fh.seek(0)
        deltadump.load_table(table='test', columns=dumpspec, db=self.dst,
                             fh=self.fh)
        self.compare_tables(self.src, self.dst)

    def test_3_deltas_2(self):
        self.fill_deltas(self.src)
        dumpspec = (('id', deltadump.INTEGER, 1),)
        deltadump.dump_table(table='test', order='id', columns=dumpspec,
                             db=self.src, fh=self.fh)
        self.fh.seek(0)
        deltadump.load_table(table='test', columns=dumpspec, db=self.dst,
                             fh=self.fh)
        self.compare_tables(self.src, self.dst)

    def test_3_deltas_3(self):
        self.fill_deltas(self.src)
        dumpspec = (('id', deltadump.INTEGER, -1),)
        deltadump.dump_table(table='test', order='id', columns=dumpspec,
                             db=self.src, fh=self.fh)
        self.fh.seek(0)
        deltadump.load_table(table='test', columns=dumpspec, db=self.dst,
                             fh=self.fh)
        self.compare_tables(self.src, self.dst)

    def test_4_time(self):
        self.fill_vals(self.src)

        t1 = 0.5 * time.time()
        t2 = 2 * time.time()
        for (id_,) in self.src.query('SELECT id FROM test'):
            val = random.uniform(t1, t2)
            self.src.execute('UPDATE test SET buf=? WHERE id=?', (val, id_))

        dumpspec = (('id', deltadump.INTEGER),
                    ('buf', deltadump.TIME))

        deltadump.dump_table(table='test', order='id', columns=dumpspec,
                             db=self.src, fh=self.fh)
        self.fh.seek(0)
        deltadump.load_table(table='test', columns=dumpspec, db=self.dst,
                             fh=self.fh)

        self.compare_tables(self.src, self.dst)


    def test_5_multi(self):
        self.fill_vals(self.src)
        dumpspec = (('id', deltadump.INTEGER, 0),)
        deltadump.dump_table(table='test', order='id', columns=dumpspec,
                             db=self.src, fh=self.fh)
        deltadump.dump_table(table='test', order='id', columns=dumpspec,
                             db=self.src, fh=self.fh)
        self.fh.seek(0)
        deltadump.load_table(table='test', columns=dumpspec, db=self.dst,
                             fh=self.fh)
        self.dst.execute('DELETE FROM test')
        deltadump.load_table(table='test', columns=dumpspec, db=self.dst,
                             fh=self.fh)
        self.compare_tables(self.src, self.dst)


    def compare_tables(self, db1, db2):
        i1 = db1.query('SELECT id, buf FROM test ORDER BY id')
        i2 = db2.query('SELECT id, buf FROM test ORDER BY id')

        for (id1, buf1) in i1:
            (id2, buf2) = i2.next()

            self.assertEqual(id1, id2)
            if isinstance(buf1, float):
                self.assertAlmostEqual(buf1, buf2, places=9)
            else:
                self.assertEqual(buf1, buf2)

        self.assertRaises(StopIteration, i2.next)

    def fill_buf(self, db, len_=None):
        rfh = open('/dev/urandom', 'rb')

        first = True
        for (id_,) in db.query('SELECT id FROM test'):
            if len_ is None and first:
                val = '' # We always want to check this case
                first = False
            elif len_ is None:
                val = rfh.read(random.randint(0, 140))
            else:
                val = rfh.read(len_)

            db.execute('UPDATE test SET buf=? WHERE id=?', (val, id_))

    def fill_vals(self, db):
        vals = []
        for exp in [7, 8, 9, 15, 16, 17, 31, 32, 33, 62]:
            vals += range(2 ** exp - 5, 2 ** exp + 6)
        vals += range(2 ** 63 - 5, 2 ** 63)
        vals += [ -v for v  in vals ]
        vals.append(-(2 ** 63))

        for val in vals:
            db.execute('INSERT INTO test (id) VALUES(?)', (val,))

    def fill_deltas(self, db):
        deltas = []
        for exp in [7, 8, 9, 15, 16, 17, 31, 32, 33]:
            deltas += range(2 ** exp - 5, 2 ** exp + 6)
        deltas += [ -v for v  in deltas ]

        last = 0
        for delta in deltas:
            val = last + delta
            last = val
            db.execute('INSERT INTO test (id) VALUES(?)', (val,))

    def create_table(self, db):
        db.execute('''CREATE TABLE test (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            buf BLOB)''')



# Somehow important according to pyunit documentation
def suite():
    return unittest.makeSuite(DumpTests)
