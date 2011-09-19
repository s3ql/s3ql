#!/usr/bin/env python
'''
benchmark.py - this file is part of S3QL (http://s3ql.googlecode.com)

Benchmark compression and upload performance and recommend compression
algorithm that maximizes throughput.

---
Copyright (C) 2010 Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU GPLv3.
'''

from __future__ import division, print_function, absolute_import
import argparse
import atexit
import logging
import os
import sys
import tempfile
import math
import time
import shutil

# We are running from the S3QL source directory, make sure
# that we use modules from this directory
basedir = os.path.abspath(os.path.join(os.path.dirname(sys.argv[0]), '..'))
if (os.path.exists(os.path.join(basedir, 'setup.py')) and
    os.path.exists(os.path.join(basedir, 'src', 's3ql', '__init__.py'))):
    sys.path = [os.path.join(basedir, 'src')] + sys.path

from s3ql.backends.common import get_bucket, BetterBucket
from s3ql.backends.local import Bucket
from s3ql.common import setup_logging
from s3ql.parse_args import ArgumentParser

log = logging.getLogger('benchmark')

def parse_args(args):
    '''Parse command line'''

    parser = ArgumentParser(
        description="Transfers and compresses the test file and gives a recommendation "
                    "for the compression algorithm to use.")

    parser.add_authfile()
    parser.add_quiet()
    parser.add_debug()
    parser.add_version()
    parser.add_storage_url()
    parser.add_argument('file', metavar='<file>', type=argparse.FileType(mode='rb'),
                        help='File to transfer')
    return parser.parse_args(args)


def main(args=None):
    if args is None:
        args = sys.argv[1:]

    options = parse_args(args)
    setup_logging(options)
        
    size = os.fstat(options.file.fileno()).st_size 
    log.info('Test file size: %.2f MB', (size / 1024**2))
    
    # Upload random data to prevent effects of compression
    # on the network layer
    log.info('Uploading..')
    bucket = get_bucket(options, plain=True)
    with bucket.open_write('s3ql_testdata') as dst:
        with open('/dev/urandom', 'rb', 0) as src:
            stamp = time.time()
            copied = 0
            while copied < size:
                buf = src.read(256*1024)
                dst.write(buf)
                copied += len(buf)
            upload_speed = copied / (time.time() - stamp)
    log.info('Upload speed: %.2f KB/sec', upload_speed / 1024)
    bucket.delete('s3ql_testdata')
    
    bucket_dir = tempfile.mkdtemp()
    atexit.register(shutil.rmtree, bucket_dir)
    src = options.file
    
    log.info('Compressing with LZMA...')
    bucket = BetterBucket('pass', 'lzma', Bucket(bucket_dir, None, None))
    with bucket.open_write('s3ql_testdata') as dst:
        src.seek(0)
        stamp = time.time()
        while True:
            buf = src.read(256*1024)
            if not buf:
                break
            dst.write(buf)
        lzma_speed = size / (time.time() - stamp)
    log.info('LZMA Compression Speed:  %.2f KB per second', lzma_speed / 1024)

    log.info('Compressing with BZIP2...')
    bucket = BetterBucket('pass', 'bzip2', Bucket(bucket_dir, None, None))
    with bucket.open_write('s3ql_testdata') as dst:
        src.seek(0)
        stamp = time.time()
        while True:
            buf = src.read(256*1024)
            if not buf:
                break
            dst.write(buf)
        bzip2_speed = size / (time.time() - stamp)
    log.info('BZIP2 Compression Speed:  %.2f KB per second', bzip2_speed / 1024)
    
    log.info('Compressing with zlib...')
    bucket = BetterBucket('pass', 'zlib', Bucket(bucket_dir, None, None))
    with bucket.open_write('s3ql_testdata') as dst:
        src.seek(0)
        stamp = time.time()
        while True:
            buf = src.read(256*1024)
            if not buf:
                break
            dst.write(buf)
        zlib_speed = size / (time.time() - stamp)
    log.info('ZLIB Compression Speed:  %.2f KB per second', zlib_speed / 1024)    


    print('To saturate your network connection, you would need to run:',
          ' - %d parallel uploads with LZMA compression' 
            % math.ceil(upload_speed / lzma_speed),
          ' - %d parallel uploads with BZIP2 compression'
            % math.ceil(upload_speed / bzip2_speed),
          ' - %d parallel uploads with zlib compression'
            % math.ceil(upload_speed / zlib_speed),
            '',
            'Note 1: increasing the number of parallel uploads above the number of',
            'available processors does not increase performance.',
            '',
            'Note 2: To reach x parallel uploads, you should start about 2*x',
            'uploads threads to compensate for network latency.',
            sep='\n')

if __name__ == '__main__':
    main(sys.argv[1:])
