#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Python Bindings for LZMA
#
# Copyright (c) 2008 Per Øyvind Karlsen <peroyvind@mandriva.org>
# liblzma Copyright (C) 2007-2008  Lasse Collin
# LZMA SDK Copyright (C) 1999-2007 Igor Pavlov
# Based much on regression tests for pylzma by Joachim Bauch
# <mail@joachim-bauch.de> & bz2 by Gustavo Niemeyer <niemeyer@niemeyer.net>
#
# This library is free software; you can redistribute it and/or
# modify it under the terms of the GNU Lesser General Public
# License as published by the Free Software Foundation; either
# version 3 of the License, or (at your option) any later version.
#
# This library is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
# Lesser General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public
# License along with this library; if not, write to the Free Software
# Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
#

#pylint: disable-all
#@PydevCodeAnalysisIgnore

import sys, random
from s3ql import lzma
import unittest
import os
from test.test_support import TESTFN

from hashlib import md5
from binascii import unhexlify, hexlify
from cStringIO import StringIO
from StringIO import StringIO as PyStringIO

ALL_CHARS = ''.join([chr(x) for x in xrange(256)])

# cache random strings to speed up tests
_random_strings = {}
def generate_random(size, choice=random.choice, ALL_CHARS=ALL_CHARS):
    global _random_strings
    if _random_strings.has_key(size):
        return _random_strings[size]

    s = ''.join([choice(ALL_CHARS) for x in xrange(size)])
    _random_strings[size] = s
    return s

class TestLZMA(unittest.TestCase):

    def setUp(self):
        self.plain = 'Her kommer kaptein klem, superhelten som aldri er slem! kos&klem! :o)'
        self.compressed_xz = unhexlify('fd377a585a0000016922de3602c0474521011600c9bd806ce00044003f5d0024194a4202f3d35297250824d0d20f4082855bf62811d85c5b316e30b927eafc568162d33f18013144b99ae29e2067e09d60c69fac0b5caaf44a0a0718bc0000008617931700015745a7cbcad39042990d010000000001595a')
        self.compressed_stream_xz = unhexlify('fd377a585a0000016922de360200210116000000742fe5a3e00044003f5d0024194a4202f3d35297250824d0d20f4082855bf62811d85c5b316e30b927eafc568162d33f18013144b99ae29e2067e09d60c69fac0b5caaf44a0a0718bc0000008617931700015745a7cbcad39042990d010000000001595a')
        self.compressed_alone = unhexlify('5d00008000ffffffffffffffff0024194a4202f3d35297250824d0d20f4082855bf62811d85c5b316e30b927eafc568162d33f18013144b99ae29e2067e09d60c69fac0b5caaf44a0a0fbe1563d7fb115800')
        self.compressed_stream_alone = unhexlify('5d00008000ffffffffffffffff0024194a4202f3d35297250824d0d20f4082855bf62811d85c5b316e30b927eafc568162d33f18013144b99ae29e2067e09d60c69fac0b5caaf44a0a0fbe1563d7fb115800')
        self.data_large = "kosogklem" * (1 << 18)


    def test_compression(self):
        compressed = lzma.compress(self.plain, options={'format':'xz'})
        self.assertEqual(compressed, self.compressed_xz)

    def test_decompression(self):
        decompressed = lzma.decompress(self.compressed_xz)
        self.assertEqual(decompressed, self.plain)

    def test_compression_decompression(self, dict_size=1 << 23):
        # call compression and decompression on random data of various sizes
        for i in xrange(18):
            size = 1 << i
            original = generate_random(size)
            # TO FIX:
            """
            result = lzma.decompress(lzma.compress(original, options={'dict_size':dict_size, 'format':'alone'}))
            self.assertEqual(len(result), size)
            self.assertEqual(md5(original).hexdigest(), md5(result).hexdigest())
            """
            result = lzma.decompress(lzma.compress(original, options={'dict_size':dict_size, 'format':'xz'}))
            self.assertEqual(len(result), size)
            self.assertEqual(md5(original).hexdigest(), md5(result).hexdigest())

    def test_multi(self):
        # call compression and decompression multiple times to detect memory leaks...
        for x in xrange(4):
            self.test_compression_decompression(dict_size=1 << 26)

    def test_decompression_stream(self):
        # test decompression object in one steps
        decompress = lzma.LZMADecompressor()
        data = decompress.decompress(self.compressed_xz)
        data += decompress.flush()
        self.assertEqual(data, self.plain)

    def test_decompression_stream_two(self):
        # test decompression in two steps
        decompress = lzma.LZMADecompressor()
        data = decompress.decompress(self.compressed_stream_xz[:10])
        data += decompress.decompress(self.compressed_stream_xz[10:])
        data += decompress.flush()
        self.assertEqual(data, self.plain)

    def test_decompression_stream_props(self):
        # test decompression with properties in separate step
        decompress = lzma.LZMADecompressor()
        data = decompress.decompress(self.compressed_stream_xz[:5])
        data += decompress.decompress(self.compressed_stream_xz[5:])
        data += decompress.flush()
        self.assertEqual(data, self.plain)

    def test_decompression_stream_reset(self):
        # test reset
        decompress = lzma.LZMADecompressor()
        data = decompress.decompress(self.compressed_stream_xz[:10])
        decompress.reset()
        data = decompress.decompress(self.compressed_stream_xz[:15])
        data += decompress.decompress(self.compressed_stream_xz[15:])
        data += decompress.flush()
        self.assertEqual(data, self.plain)

    def test_decompression_streaming(self):
        # test decompressing with one byte at a time...
        decompress = lzma.LZMADecompressor()
        infile = StringIO(self.compressed_stream_xz)
        outfile = StringIO()
        while 1:
            data = infile.read(1)
            if not data: break
            outfile.write(decompress.decompress(data))
        outfile.write(decompress.flush())
        self.assertEqual(outfile.getvalue(), self.plain)

    def test_compression_stream(self):
        # test compression object in one steps
        compress = lzma.LZMACompressor(options={'format':'alone'})
        data = compress.compress(self.plain)
        data += compress.flush()
        self.assertEqual(data, self.compressed_stream_alone)
        compress.reset(options={'format':'xz'})
        data = compress.compress(self.plain)
        data += compress.flush()
        self.assertEqual(data, self.compressed_stream_xz)

    def test_compression_stream_two(self):
        # test compression in two steps
        compress = lzma.LZMACompressor(options={'format':'alone'})
        data = compress.compress(self.plain[:10])
        data += compress.compress(self.plain[10:])
        data += compress.flush()
        self.assertEqual(data, self.compressed_stream_alone)
        compress.reset(options={'format':'xz'})
        data = compress.compress(self.plain[:10])
        data += compress.compress(self.plain[10:])
        data += compress.flush()
        self.assertEqual(data, self.compressed_stream_xz)

    def test_compression_stream_props(self):
        # test compression with properties in separate step
        compress = lzma.LZMACompressor(options={'format':'alone'})
        data = compress.compress(self.plain[:5])
        data += compress.compress(self.plain[5:])
        data += compress.flush()
        self.assertEqual(data, self.compressed_stream_alone)
        compress.reset(options={'format':'xz'})
        data = compress.compress(self.plain[:5])
        data += compress.compress(self.plain[5:])
        data += compress.flush()
        self.assertEqual(data, self.compressed_stream_xz)


    def test_compression_stream_reset(self):
        # test reset
        compress = lzma.LZMACompressor(options={'format':'xz'})
        data = compress.compress(self.plain[:10])
        compress.reset(options={'format':'xz'})
        data = compress.compress(self.plain[:15])
        data += compress.compress(self.plain[15:])
        data += compress.flush()
        self.assertEqual(data, self.compressed_stream_xz)

    def test_compression_streaming(self):
        # test compressing with one byte at a time...
        compress = lzma.LZMACompressor(options={'format':'alone'})
        infile = StringIO(self.plain)
        outfile = StringIO()
        while 1:
            data = infile.read(1)
            if not data: break
            outfile.write(compress.compress(data))
        outfile.write(compress.flush())
        self.assertEqual(outfile.getvalue(), self.compressed_stream_alone)
        compress.reset(options={'format':'xz'})
        infile = StringIO(self.plain)
        outfile = StringIO()
        while 1:
            data = infile.read(1)
            if not data: break
            outfile.write(compress.compress(data))
        outfile.write(compress.flush())
        self.assertEqual(outfile.getvalue(), self.compressed_stream_xz)


    def test_compress_large_string(self):
        # decompress large block of repeating data, string version
        compressed = lzma.compress(self.data_large)
        self.failUnless(self.data_large == lzma.decompress(compressed))

    def test_decompress_large_stream(self):
        # decompress large block of repeating data, stream version
        decompress = lzma.LZMADecompressor()
        infile = StringIO(lzma.compress(self.data_large, options={'format':'alone'}))
        outfile = StringIO()
        while 1:
            tmp = infile.read(1)
            if not tmp: break
            outfile.write(decompress.decompress(tmp))
        outfile.write(decompress.flush())
        self.failUnless(self.data_large == outfile.getvalue())
        decompress.reset()
        infile = StringIO(lzma.compress(self.data_large, options={'format':'xz'}))
        outfile = StringIO()
        while 1:
            tmp = infile.read(1)
            if not tmp: break
            outfile.write(decompress.decompress(tmp))
        outfile.write(decompress.flush())
        self.failUnless(self.data_large == outfile.getvalue())

    def test_decompress_large_stream_bigchunks(self):
        # decompress large block of repeating data, stream version with big chunks
        decompress = lzma.LZMADecompressor()
        infile = StringIO(lzma.compress(self.data_large))
        outfile = StringIO()
        while 1:
            tmp = infile.read(1024)
            if not tmp: break
            outfile.write(decompress.decompress(tmp))
        outfile.write(decompress.flush())
        self.failUnless(self.data_large == outfile.getvalue())

    def test_compress_large_stream(self):
        # compress large block of repeating data, stream version
        compress = lzma.LZMACompressor(options={'format':'alone'})
        infile = StringIO(self.data_large)
        outfile = StringIO()
        while 1:
            tmp = infile.read(1)
            if not tmp: break
            outfile.write(compress.compress(tmp))
        outfile.write(compress.flush())
        self.failUnless(lzma.compress(self.data_large, options={'format':'alone'}) == outfile.getvalue())
        compress.reset(options={'format':'xz'})
        infile = StringIO(self.data_large)
        outfile = StringIO()
        while 1:
            tmp = infile.read(1)
            if not tmp: break
            outfile.write(compress.compress(tmp))
        outfile.write(compress.flush())
        self.failUnless(self.data_large == lzma.decompress(outfile.getvalue()))

    def test_compress_large_stream_bigchunks(self):
        # compress large block of repeating data, stream version with big chunks
        compress = lzma.LZMACompressor(options={'format':'alone'})
        infile = StringIO(self.data_large)
        outfile = StringIO()
        while 1:
            tmp = infile.read(1024)
            if not tmp: break
            outfile.write(compress.compress(tmp))
        outfile.write(compress.flush())
        self.failUnless(self.data_large == lzma.decompress(outfile.getvalue()))
        compress.reset(options={'format':'xz'})
        infile = StringIO(self.data_large)
        outfile = StringIO()
        while 1:
            tmp = infile.read(1024)
            if not tmp: break
            outfile.write(compress.compress(tmp))
        outfile.write(compress.flush())
        self.failUnless(self.data_large == lzma.decompress(outfile.getvalue()))

class TestLZMAOptions(unittest.TestCase):
    def setUp(self):
        self.data = "kosogklem" * (1 << 10)

    def test_preset_levels(self):
        for lvl in xrange(lzma.options.level[0], lzma.options.level[1] + 1):
            result = lzma.compress(self.data, options={'level':lvl})
            self.assertEqual(self.data, lzma.decompress(result))
        self.failUnlessRaises(ValueError, lzma.compress, self.data, options={'level':lzma.options.level[1] + 1})
        self.failUnlessRaises(ValueError, lzma.compress, self.data, options={'level':lzma.options.level[0] - 1})

    def test_dict_size(self):
        dict = lzma.options.dict_size[0]
        while dict <= 1 << 26: # lzma.options.dict_size[1]: Since using very large dictionaries requires
                             # very large amount of memory, let's not go beyond 64mb for testing..
            result = lzma.compress(self.data, options={'dict_size':dict})
            self.assertEqual(self.data, lzma.decompress(result))
            dict = dict * 2
        self.failUnlessRaises(ValueError, lzma.compress, self.data, options={'dict_size':lzma.options.dict_size[1] + 1})
        self.failUnlessRaises(ValueError, lzma.compress, self.data, options={'dict_size':lzma.options.dict_size[0] - 1})

    def test_nice_len(self):
        for nl in xrange(lzma.options.nice_len[0], lzma.options.nice_len[1] + 1):
            result = lzma.compress(self.data, options={'nice_len':nl})
            self.assertEqual(self.data, lzma.decompress(result))
        self.failUnlessRaises(ValueError, lzma.compress, self.data, options={'nice_len':lzma.options.nice_len[1] + 1})
        self.failUnlessRaises(ValueError, lzma.compress, self.data, options={'nice_len':lzma.options.nice_len[0] - 1})

    def test_lclp(self):
        for lcb in xrange(lzma.options.lc[0], lzma.options.lc[1] + 1):
                for lpb in xrange(lzma.options.lc[1] - lcb):
                        result = lzma.compress(self.data, options={'lc':lcb, 'lp':lpb})
                        self.assertEqual(self.data, lzma.decompress(result))
        self.failUnlessRaises(ValueError, lzma.compress, self.data, options={'lc':lzma.options.lc[0] - 1})
        self.failUnlessRaises(ValueError, lzma.compress, self.data, options={'lc':lzma.options.lc[1] + 1})
        self.failUnlessRaises(ValueError, lzma.compress, self.data, options={'lp':lzma.options.lp[0] - 1})
        self.failUnlessRaises(ValueError, lzma.compress, self.data, options={'lp':lzma.options.lp[1] + 1})

    def test_pb(self):
        for pb in xrange(lzma.options.pb[0], lzma.options.pb[1] + 1):
            result = lzma.compress(self.data, options={'pb':pb})
            self.assertEqual(self.data, lzma.decompress(result))
        self.failUnlessRaises(ValueError, lzma.compress, self.data, options={'pb':lzma.options.pb[0] - 1})
        self.failUnlessRaises(ValueError, lzma.compress, self.data, options={'pb':lzma.options.pb[1] + 1})

    def test_mode(self):
        for md in lzma.options.mode:
            result = lzma.decompress(lzma.compress(self.data, options={'mode':md}))
            self.assertEqual(self.data, result)
        self.failUnlessRaises(ValueError, lzma.compress, self.data, options={'mode':'foo'})

    def test_mf(self):
        for match_finder in lzma.options.mf:
            result = lzma.decompress(lzma.compress(self.data, options={'mf':match_finder}))
            self.assertEqual(self.data, result)
        self.failUnlessRaises(ValueError, lzma.compress, self.data, options={'mf':'1234'})

    def test_depth(self):
        for d in xrange(lzma.options.depth, 20):
            result = lzma.decompress(lzma.compress(self.data, options={'depth':d}))
            self.assertEqual(self.data, result)
        self.failUnlessRaises(ValueError, lzma.compress, self.data, options={'depth':-1})

    def test_format(self):
        for format in lzma.options.format:
            result = lzma.decompress(lzma.compress(self.data, options={'format':format}))
            self.assertEqual(self.data, result)
        self.failUnlessRaises(ValueError, lzma.compress, self.data, options={'format':'foo'})

class TestLZMAFile(unittest.TestCase):
    "Test lzma.LZMAFile type miscellaneous methods."

    TEXT = "root:x:0:0:root:/root:/bin/bash\nbin:x:1:1:bin:/bin:\ndaemon:x:2:2:daemon:/sbin:\nadm:x:3:4:adm:/var/adm:\nlp:x:4:7:lp:/var/spool/lpd:\nsync:x:5:0:sync:/sbin:/bin/sync\nshutdown:x:6:0:shutdown:/sbin:/sbin/shutdown\nhalt:x:7:0:halt:/sbin:/sbin/halt\nmail:x:8:12:mail:/var/spool/mail:\nnews:x:9:13:news:/var/spool/news:\nuucp:x:10:14:uucp:/var/spool/uucp:\noperator:x:11:0:operator:/root:\ngames:x:12:100:games:/usr/games:\ngopher:x:13:30:gopher:/usr/lib/gopher-data:\nftp:x:14:50:FTP User:/var/ftp:/bin/bash\nnobody:x:65534:65534:Nobody:/home:\npostfix:x:100:101:postfix:/var/spool/postfix:\nniemeyer:x:500:500::/home/niemeyer:/bin/bash\npostgres:x:101:102:PostgreSQL Server:/var/lib/pgsql:/bin/bash\nmysql:x:102:103:MySQL server:/var/lib/mysql:/bin/bash\nwww:x:103:104::/var/www:/bin/false\n"

    DATA = "]\x00\x00\x80\x00\x02\x03\x00\x00\x00\x00\x00\x00\x009\x1b\xec\xe8:-\x7f\xca\\\xf7\xb4C\xb1\xf1<<\xaf5\x10\x92\xd2\x14,\x13+\xef\xf7\x8cCGl\xb1\x97\x00\x00\xb5j\x9a\xdc\x1e\xf2X\x8b\xd9\xebM8\x8d\\l'c\xd6t\xd5\x861\x8e\xc5W7\xdd\x8c\x8d\x01\xec^\xbc\xdb\xf6\xde\xda\xdc\x93\xc3\x0c|E\x8d\xb2DD\xac1\x84\xfck\xa1_i\x7f\xcb\xd4\x99\n\xe9\x9b\xa86\xces\xb3\xd2f\xd8r^8 \x95\x98\xeb\xdb\\\xd3\xfbY\xef\xcfW]\x13|9\xdb\x92C\xc6\xf2W\xd9h\xe8^i \xd6\x88n\xf1\xcf\x83H\xd8\xfd\x1bz\xce\xe6\xfc\xb4\xa0\xbb\x9c\xde,\x96\x88\xa0\xe7\x80\xa8K\xdb]Wy\xb4\xbc\xfac)!\xfcS\xef\x07\xb8\xfbx\xe7\xe5\x02\xd54;+\xb3m5\xd2\x00V\x8b\x9a\x11H\x8d\xa8e>\xddd\xc4xH\x90\xa4Y\x97\xab\x9d\x9e\x9e$\xa2.#?G\x8d\xfd\xefn\xcb\xb5mjB\t@\xc7H\x07/<=?\x08@\xca\xca\x85\xd0Nb\xd1\xfe\x83s\xbb\x14\xa1\te\xcf\x1d?\x077n\x14%\x02\xc5\xf2c\xfe\x12H\x19\xea@\x0b\xc2\xf2\x8d^\x93w\x9f\x9a\x1bw\xf4\xcb\xc7z\xb7Iep\x17u\xa7\x1d\xaa:0\xdc\xaf\x80h\x93+\xbdg\xbf\x16\xce\x93\xcc\x8b\xecus<\xc6+,%UJ0\x8e\xfb4\x85\x11D\xf5j\xf0{\xa8\x0c\xb0U\\[\x18fJ{\xf6A\xfb\xfd\x19\xe8\xf4\xa3k;\x08\x07\x07\n[\xfd\x7f\x91\xfe\x8a\xaf\x1fC}4x:\x8e\xd1^Sw\x18\x1c}g\xb07"

    DATA_CRLF = ']\x00\x00\x80\x00\xff\xff\xff\xff\xff\xff\xff\xff\x009\x1b\xec\xe8:-\x7f\xca\\\xf7\xb4C\xb1\xf1<<\xaf5\x10\x92\xd2\x14,>\xc4&\xef\x90[\xa1\xa6%\xfaS\x17\xf6/\xd1\xa8\x87\xbb\xb3b\xe7F\x10\nu\xb4\x96\x8bs\xe1\xda\xfdQ~-\xb5T\xa5i-;\x01m$\xebPl[\xe0\x14F\x12\xc2\x88\xbc\xdb\x85\xfc\x90\x1dz\xb7\xfe\xca\xec\xe3\x92\x14\xdb\x88\x16?\x9e\xdaU\x9cJR\x0eF\x0e]u^\xbe\xcbb\xbe\xb7\xbc;\x8b\xfb\xd2\xc1\xd5\xe3\x99\x80\xeb\xcaP\xb8\x9e\xd4\xd7\xd5\x0e\x93$\xff\x81G\xd85\xba\xa1~{\xb3U\x1dV\xf0\x0eA\t=\x95fT/#0\xe1\xfa`\xb64>D\x05=pp\xc6\xd6\xdbnL>\xba\x03\xf6F\x8e W\xdb\x13T\xff+~\x96\xf1-=?\xfc\xe4\xa8PP`\xef\xce\xe9\xb5\xe5O\xff\xaf\xb7$\xc2?m\xdb\x13\x7f[U\x17\x16\nO\xaf\xf2\xa4\xbd)$\x8f\x86\x8b\x0e\x8f\xc7\x96\xba\xf6\xfe\xa4\x06\xd2q\xf5\x03I?\x8f\xf8\xf0$`\xc7\xc8\xcb\xa6\xcd\xea\x8b[k\xb4_c\x1c|H\xcf\x12\x8b\xec\x85s\xde\xa1\xce\xe83W\x87\x03E\x16\x10\xf7\x94\x80\xc3R\xae\xb0\xce\xc5\x05\x9d\x06I\xa9\xbcW\x1f\x8b\xe1\xbc\x83\xea\xfaNJE\t\xa54\xfa`B\xf9\x17;Z4\xfa\xf5\x81f\xbe&\xe6^}<\t^\x9b\xc7\x9a\xa7\x99E\x8e\xbc\xe4\xa2\x04\x91\xf2S\x06#\x9c\x88\xd1\x9c]\xf4\xc3\xa7\x80\x15*#7fTZ\xa0\xe3\x85\xc4k\xac\xf7L\x1b\xc0\xf8\xa7\x0b\xe84\xdbf\x04\xd7\x087Pl\xff\xd2\x9c\x8cl'

    def setUp(self):
        self.filename = TESTFN

    def tearDown(self):
        if os.path.isfile(self.filename):
            os.unlink(self.filename)

    def createTempFile(self, crlf=0):
        f = open(self.filename, "wb")
        if crlf:
            data = self.DATA_CRLF
        else:
            data = self.DATA
        f.write(data)
        f.close()

    def testRead(self):
        # "Test lzma.LZMAFile.read()"
        self.createTempFile()
        lzmaf = lzma.LZMAFile(self.filename)
        self.assertRaises(TypeError, lzmaf.read, None)
        self.assertEqual(lzmaf.read(), self.TEXT)
        lzmaf.close()

    def testReadChunk10(self):
        # "Test lzma.LZMAFile.read() in chunks of 10 bytes"
        self.createTempFile()
        lzmaf = lzma.LZMAFile(self.filename)
        text = ''
        while 1:
            str = lzmaf.read(10)
            if not str:
                break
            text += str
        self.assertEqual(text, text)
        lzmaf.close()

    def testRead100(self):
        # "Test lzma.LZMAFile.read(100)"
        self.createTempFile()
        lzmaf = lzma.LZMAFile(self.filename)
        self.assertEqual(lzmaf.read(100), self.TEXT[:100])
        lzmaf.close()

    def testReadLine(self):
        # "Test lzma.LZMAFile.readline()"
        self.createTempFile()
        lzmaf = lzma.LZMAFile(self.filename)
        self.assertRaises(TypeError, lzmaf.readline, None)
        sio = StringIO(self.TEXT)
        for line in sio.readlines():
            self.assertEqual(lzmaf.readline(), line)
        lzmaf.close()

    def testReadLines(self):
        # "Test lzma.LZMAFile.readlines()"
        self.createTempFile()
        lzmaf = lzma.LZMAFile(self.filename)
        self.assertRaises(TypeError, lzmaf.readlines, None)
        sio = StringIO(self.TEXT)
        self.assertEqual(lzmaf.readlines(), sio.readlines())
        lzmaf.close()

    def testIterator(self):
        # "Test iter(lzma.LZMAFile)"
        self.createTempFile()
        lzmaf = lzma.LZMAFile(self.filename)
        sio = StringIO(self.TEXT)
        self.assertEqual(list(iter(lzmaf)), sio.readlines())
        lzmaf.close()

    def testXReadLines(self):
        # "Test lzma.LZMAFile.xreadlines()"
        self.createTempFile()
        lzmaf = lzma.LZMAFile(self.filename)
        sio = StringIO(self.TEXT)
        self.assertEqual(list(lzmaf.xreadlines()), sio.readlines())
        lzmaf.close()

    def testUniversalNewlinesLF(self):
        # "Test lzma.LZMAFile.read() with universal newlines (\\n)"
        self.createTempFile()
        lzmaf = lzma.LZMAFile(self.filename, "rU")
        self.assertEqual(lzmaf.read(), self.TEXT)
        self.assertEqual(lzmaf.newlines, "\n")
        lzmaf.close()

    def testUniversalNewlinesCRLF(self):
        # "Test lzma.LZMAFile.read() with universal newlines (\\r\\n)"
        self.createTempFile(crlf=1)
        lzmaf = lzma.LZMAFile(self.filename, "rU")
        self.assertEqual(lzmaf.read(), self.TEXT)
        self.assertEqual(lzmaf.newlines, "\r\n")
        lzmaf.close()

    def testWrite(self):
        # "Test lzma.LZMAFile.write()"
        lzmaf = lzma.LZMAFile(self.filename, "w")
        self.assertRaises(TypeError, lzmaf.write)
        lzmaf.write(self.TEXT)
        lzmaf.close()
        f = open(self.filename, 'rb')
        self.assertEqual(lzma.decompress(f.read()), self.TEXT)
        f.close()

    def testWriteChunks10(self):
        # "Test lzma.LZMAFile.write() with chunks of 10 bytes"
        lzmaf = lzma.LZMAFile(self.filename, "w")
        n = 0
        while 1:
            str = self.TEXT[n * 10:(n + 1) * 10]
            if not str:
                break
            lzmaf.write(str)
            n += 1
        lzmaf.close()
        f = open(self.filename, 'rb')
        self.assertEqual(lzma.decompress(f.read()), self.TEXT)
        f.close()

    def testWriteLines(self):
        # "Test lzma.LZMAFile.writelines()"
        lzmaf = lzma.LZMAFile(self.filename, "w")
        self.assertRaises(TypeError, lzmaf.writelines)
        sio = StringIO(self.TEXT)
        lzmaf.writelines(sio.readlines())
        lzmaf.close()
        # patch #1535500
        self.assertRaises(ValueError, lzmaf.writelines, ["a"])
        f = open(self.filename, 'rb')
        self.assertEqual(lzma.decompress(f.read()), self.TEXT)
        f.close()

    def testWriteMethodsOnReadOnlyFile(self):
        lzmaf = lzma.LZMAFile(self.filename, "w")
        lzmaf.write("abc")
        lzmaf.close()

        lzmaf = lzma.LZMAFile(self.filename, "r")
        self.assertRaises(IOError, lzmaf.write, "a")
        self.assertRaises(IOError, lzmaf.writelines, ["a"])

    def testSeekForward(self):
        # "Test lzma.LZMAFile.seek(150, 0)"
        self.createTempFile()
        lzmaf = lzma.LZMAFile(self.filename)
        self.assertRaises(TypeError, lzmaf.seek)
        lzmaf.seek(150)
        self.assertEqual(lzmaf.read(), self.TEXT[150:])
        lzmaf.close()

    def testSeekBackwards(self):
        # "Test lzma.LZMAFile.seek(-150, 1)"
        self.createTempFile()
        lzmaf = lzma.LZMAFile(self.filename)
        lzmaf.read(500)
        lzmaf.seek(-150, 1)
        self.assertEqual(lzmaf.read(), self.TEXT[500 - 150:])
        lzmaf.close()

    def testSeekBackwardsFromEnd(self):
        # "Test lzma.LZMAFile.seek(-150, 2)"
        self.createTempFile()
        lzmaf = lzma.LZMAFile(self.filename)
        lzmaf.seek(-150, 2)
        self.assertEqual(lzmaf.read(), self.TEXT[len(self.TEXT) - 150:])
        lzmaf.close()

    def testSeekPostEnd(self):
        # "Test lzma.LZMAFile.seek(150000)"
        self.createTempFile()
        lzmaf = lzma.LZMAFile(self.filename)
        lzmaf.seek(150000)
        self.assertEqual(lzmaf.tell(), len(self.TEXT))
        self.assertEqual(lzmaf.read(), "")
        lzmaf.close()

    def testSeekPostEndTwice(self):
        # "Test lzma.LZMAFile.seek(150000) twice"
        self.createTempFile()
        lzmaf = lzma.LZMAFile(self.filename)
        lzmaf.seek(150000)
        lzmaf.seek(150000)
        self.assertEqual(lzmaf.tell(), len(self.TEXT))
        self.assertEqual(lzmaf.read(), "")
        lzmaf.close()

    def testSeekPreStart(self):
        # "Test lzma.LZMAFile.seek(-150, 0)"
        self.createTempFile()
        lzmaf = lzma.LZMAFile(self.filename)
        lzmaf.seek(-150)
        self.assertEqual(lzmaf.tell(), 0)
        self.assertEqual(lzmaf.read(), self.TEXT)
        lzmaf.close()

    def testOpenDel(self):
        # "Test opening and deleting a file many times"
        self.createTempFile()
        for i in xrange(10000):
            o = lzma.LZMAFile(self.filename)
            del o

    def testOpenNonexistent(self):
        # "Test opening a nonexistent file"
        self.assertRaises(IOError, lzma.LZMAFile, "/non/existent")

    def testModeU(self):
        # Bug #1194181: lzma.lzma.LZMAFile opened for write with mode "U"
        self.createTempFile()
        lzmaf = lzma.LZMAFile(self.filename, "U")
        lzmaf.close()
        f = file(self.filename)
        f.seek(0, 2)
        self.assertEqual(f.tell(), len(self.DATA))
        f.close()

    def testBug1191043(self):
        # readlines() for files containing no newline
        data = ']\x00\x00\x80\x00\x04\x00\x00\x00\x00\x00\x00\x00\x00*\x19Jgkp8\x80'
        f = open(self.filename, "wb")
        f.write(data)
        f.close()
        lzmaf = lzma.LZMAFile(self.filename)
        lines = lzmaf.readlines()
        lzmaf.close()
        self.assertEqual(lines, ['Test'])
        lzmaf = lzma.LZMAFile(self.filename)
        xlines = list(lzmaf.xreadlines())
        lzmaf.close()
        self.assertEqual(xlines, ['Test'])

class ChecksumTestCase(unittest.TestCase):
    # checksum test cases
    def test_crc32start(self):
        self.assertEqual(lzma.crc32(""), lzma.crc32("", 0))
        self.assert_(lzma.crc32("abc", 0xffffffff))

    def test_crc32empty(self):
        self.assertEqual(lzma.crc32("", 0), 0)
        self.assertEqual(lzma.crc32("", 1), 1)
        self.assertEqual(lzma.crc32("", 432), 432)

    def assertEqual32(self, seen, expected):
        # 32-bit values masked -- checksums on 32- vs 64- bit machines
        # This is important if bit 31 (0x08000000L) is set.
        self.assertEqual(seen & 0x0FFFFFFFFL, expected & 0x0FFFFFFFFL)

    def test_penguins32(self):
        self.assertEqual32(lzma.crc32("penguin", 0), 0x0e5c1a120L)
        self.assertEqual32(lzma.crc32("penguin", 1), 0x43b6aa94)

        self.assertEqual(lzma.crc32("penguin"), lzma.crc32("penguin", 0))

    # These crc64 tests needs to be reviewed..
    def test_crc64start(self):
        self.assertEqual(lzma.crc64(""), lzma.crc64("", 0))
        self.assert_(lzma.crc64("abc", 0xffffffff))

    def test_crc64empty(self):
        self.assertEqual(lzma.crc64("", 0), 0)
        self.assertEqual(lzma.crc64("", 1), 1)
        self.assertEqual(lzma.crc64("", 432), 432)

    def assertEqual64(self, seen, expected):
        self.assertEqual(seen & 0xFFFFFFFFFFFFFFFFL, expected & 0xFFFFFFFFFFFFFFFFL)

    def test_penguins64(self):
        self.assertEqual64(lzma.crc64("penguin", 0), 0x9285a18e774b3258)
        self.assertEqual64(lzma.crc64("penguin", 1), 0xb06aacd743b256b4L)

        self.assertEqual(lzma.crc64("penguin"), lzma.crc64("penguin", 0))

def test_main():
    from test import test_support
    test_support.run_unittest(TestLZMA)
    test_support.run_unittest(TestLZMAOptions)
    test_support.run_unittest(TestLZMAFile)
    test_support.run_unittest(ChecksumTestCase)

if __name__ == "__main__":
    test_main()
