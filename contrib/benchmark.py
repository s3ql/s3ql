#!/usr/bin/env python
'''
benchmark.py - this file is part of S3QL (http://s3ql.googlecode.com)

Benchmark compression and upload performance and recommend compression
algorithm that maximizes throughput.

---
Copyright (C) 2010 Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU LGPL.
'''

from __future__ import division, print_function, absolute_import

import sys
import time
import os
import logging
import lzma
import zlib
import bz2

# We are running from the S3QL source directory, make sure
# that we use modules from this directory
basedir = os.path.abspath(os.path.join(os.path.dirname(sys.argv[0]), '..'))
if (os.path.exists(os.path.join(basedir, 'setup.py')) and
    os.path.exists(os.path.join(basedir, 'src', 's3ql', '__init__.py'))):
    sys.path = [os.path.join(basedir, 'src')] + sys.path

from s3ql.backends.common import compress_encrypt_fh
from s3ql.common import (get_backend, QuietError,setup_logging)
from s3ql.parse_args import ArgumentParser
import argparse

log = logging.getLogger('benchmark')

def parse_args(args):
    '''Parse command line'''

    parser = ArgumentParser(
        usage="%prog [options] <storage-url> <test-file>\n"
              "%prog --help",
        description="Transfers and compresses the test file and gives a recommendation "
                    "for the compression algorithm to use.")

    parser.add_homedir()
    parser.add_quiet()
    parser.add_debug()
    parser.add_version()
    parser.add_ssl()
    
    parser.add_storage_url()
    parser.add_argument('file', metavar='<file>', type=argparse.FileType(mode='rb'),
                        help='File to transfer')
    parser.add_argument("--compression-threads", action="store", type=int,
                      default=1, metavar='<no>',
                      help='Number of parallel compression and encryption threads '
                           'to use (default: %(default)s).')
    return parser.parse_args(args)


def main(args=None):
    if args is None:
        args = sys.argv[1:]

    options = parse_args(args)
    setup_logging(options)

    with get_backend(options.storage_url, options.homedir,
                     options.ssl) as (conn, bucketname):

        if not bucketname in conn:
            raise QuietError("Bucket does not exist.")
        bucket = conn.get_bucket(bucketname)

        ifh = options.testfile
        ofh = open('/dev/null', 'r+b')
        size = os.fstat(ifh.fileno()).st_size / 1024
        log.info('Test file size: %.2f MB', (size / 1024))

        log.info('Compressing with LZMA...')
        stamp = time.time()
        compress_encrypt_fh(ifh, ofh, 'foobar', 'nonce',
                            lzma.LZMACompressor(options={ 'level': 7 }))
        seconds = time.time() - stamp
        lzma_speed = size / seconds
        log.info('done. LZMA Compression Speed:  %.2f KB per second', lzma_speed)

        log.info('Compressing with BZip2...')
        ifh.seek(0)
        stamp = time.time()
        compress_encrypt_fh(ifh, ofh, 'foobar', 'nonce',
                            bz2.BZ2Compressor(9))
        seconds = time.time() - stamp
        bzip2_speed = size / seconds
        log.info('done. Bzip2 Compression Speed:  %.2f KB per second', bzip2_speed)

        log.info('Compressing with zlib...')
        ifh.seek(0)
        stamp = time.time()
        compress_encrypt_fh(ifh, ofh, 'foobar', 'nonce',
                            zlib.compressobj(9))
        seconds = time.time() - stamp
        zlib_speed = size / seconds
        log.info('done. zlib Compression Speed:  %.2f KB per second', zlib_speed)

        log.info('Transferring to backend...')
        ifh.seek(0)
        stamp = time.time()
        bucket.raw_store(options.testfile, ifh, dict())
        seconds = time.time() - stamp
        net_speed = size / seconds
        log.info('done. Network Uplink Speed:  %.2f KB per second', net_speed)


    print('Assuming mount.s3ql will be called with --compression-threads %d'
          % options.compression_threads)
    lzma_speed *= options.compression_threads
    bzip2_speed *= options.compression_threads
    zlib_speed *= options.compression_threads
    
    if lzma_speed > net_speed:
        print('You should use LZMA compression.')
    elif bzip2_speed > net_speed:
        print('You should use BZip2 compression.')
    elif zlib_speed > net_speed:
        print('You should use zlib compression.')
    else:
        print('You should use zlib compression, but even that is not fast\n'
              'enough to saturate your network connection.')


if __name__ == '__main__':
    main(sys.argv[1:])
