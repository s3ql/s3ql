#!/usr/bin/env python3
'''
t1_dump.py - this file is part of S3QL.

Copyright © 2008 Nikolaus Rath <Nikolaus@rath.org>

This work can be distributed under the terms of the GNU GPLv3.
'''

if __name__ == '__main__':
    import pytest
    import sys
    sys.exit(pytest.main([__file__] + sys.argv[1:]))

import unittest
from s3ql import deltadump
import tempfile
from s3ql.database import Connection
import random
import time

class DumpTests(unittest.TestCase):
    def setUp(self):
        self.tmpfh1 = tempfile.NamedTemporaryFile()
        self.tmpfh2 = tempfile.NamedTemporaryFile()
        self.src = Connection(self.tmpfh1.name)
        self.dst = Connection(self.tmpfh2.name)
        self.fh = tempfile.TemporaryFile()

        # Disable exclusive locking for all tests
        self.src.execute('PRAGMA locking_mode = NORMAL')
        self.dst.execute('PRAGMA locking_mode = NORMAL')

        self.create_table(self.src)
        self.create_table(self.dst)

    def tearDown(self):
        self.src.close()
        self.dst.close()
        self.tmpfh1.close()
        self.tmpfh2.close()
        self.fh.close()

    def test_transactions(self):
        self.fill_vals(self.src)
        dumpspec = (('id', deltadump.INTEGER, 0),)
        deltadump.dump_table(table='test', order='id', columns=dumpspec,
                             db=self.src, fh=self.fh)
        self.fh.seek(0)
        self.dst.execute('PRAGMA journal_mode = WAL')

        deltadump.load_table(table='test', columns=dumpspec, db=self.dst,
                             fh=self.fh, trx_rows=10)
        self.compare_tables(self.src, self.dst)

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
            (id2, buf2) = next(i2)

            self.assertEqual(id1, id2)
            if isinstance(buf1, float):
                self.assertAlmostEqual(buf1, buf2, places=9)
            else:
                self.assertEqual(buf1, buf2)

        self.assertRaises(StopIteration, i2.__next__)

    def fill_buf(self, db, len_=None):
        with open('/dev/urandom', 'rb') as rfh:
            first = True
            for (id_,) in db.query('SELECT id FROM test'):
                if len_ is None and first:
                    val = b'' # We always want to check this case
                    first = False
                elif len_ is None:
                    val = rfh.read(random.randint(0, 140))
                else:
                    val = rfh.read(len_)

                db.execute('UPDATE test SET buf=? WHERE id=?', (val, id_))

    def fill_vals(self, db):
        vals = []
        for exp in [7, 8, 9, 15, 16, 17, 31, 32, 33, 62]:
            vals += list(range(2 ** exp - 5, 2 ** exp + 6))
        vals += list(range(2 ** 63 - 5, 2 ** 63))
        vals += [ -v for v  in vals ]
        vals.append(-(2 ** 63))

        for val in vals:
            db.execute('INSERT INTO test (id) VALUES(?)', (val,))

    def fill_deltas(self, db):
        deltas = []
        for exp in [7, 8, 9, 15, 16, 17, 31, 32, 33]:
            deltas += list(range(2 ** exp - 5, 2 ** exp + 6))
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
