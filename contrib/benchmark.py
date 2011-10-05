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
import subprocess
import shutil

# We are running from the S3QL source directory, make sure
# that we use modules from this directory
basedir = os.path.abspath(os.path.join(os.path.dirname(sys.argv[0]), '..'))
if (os.path.exists(os.path.join(basedir, 'setup.py')) and
    os.path.exists(os.path.join(basedir, 'src', 's3ql', '__init__.py'))):
    sys.path = [os.path.join(basedir, 'src')] + sys.path

from s3ql.backends.common import get_bucket, BetterBucket
from s3ql.backends.local import Bucket
from s3ql.common import setup_logging, BUFSIZE
from s3ql.parse_args import ArgumentParser

log = logging.getLogger('benchmark')

def parse_args(args):
    '''Parse command line'''

    parser = ArgumentParser(
                description='Measure S3QL write performance, uplink bandwidth and '
                            'compression speed and determine limiting factor.')

    parser.add_authfile()
    parser.add_quiet()
    parser.add_debug()
    parser.add_version()
    parser.add_storage_url()
    parser.add_argument('file', metavar='<file>', type=argparse.FileType(mode='rb'),
                        help='File to transfer')
    parser.add_cachedir()
    return parser.parse_args(args)


def main(args=None):
    if args is None:
        args = sys.argv[1:]

    args = ['local:///home/nikratio/tmp/bucket', '/home/nikratio/tmp/testfile0']

    options = parse_args(args)
    setup_logging(options)

    size = 100*1024*1024 # KB
    log.info('Measuring throughput to cache...')
    bucket_dir = tempfile.mkdtemp()
    mnt_dir = tempfile.mkdtemp()
    atexit.register(shutil.rmtree, bucket_dir)
    atexit.register(shutil.rmtree, mnt_dir)
    subprocess.check_call(['mkfs.s3ql', '--plain', 'local://%s' % bucket_dir,
                           '--quiet', '--cachedir', options.cachedir])
    subprocess.check_call(['mount.s3ql', '--threads', '1', '--quiet', 
                           '--cachesize', '%d' % (2 * size/1024), '--log',
                           '%s/mount.log' % bucket_dir, '--cachedir', options.cachedir,
                           'local://%s' % bucket_dir, mnt_dir])
    with open('/dev/urandom', 'rb', 0) as src:
        with open('%s/bigfile' % mnt_dir, 'wb', 0) as dst:
            stamp = time.time()
            copied = 0
            while copied < size:
                buf = src.read(BUFSIZE)
                dst.write(buf)
                copied += len(buf)
            fuse_speed = copied / (time.time() - stamp)
    os.unlink('%s/bigfile' % mnt_dir)
    subprocess.check_call(['umount.s3ql', mnt_dir])                           
    log.info('Cache throughput: %.2f KB/sec', fuse_speed / 1024)

    # Upload random data to prevent effects of compression
    # on the network layer
    log.info('Measuring raw backend throughput..')
    bucket = get_bucket(options, plain=True)
    with bucket.open_write('s3ql_testdata') as dst:
        with open('/dev/urandom', 'rb', 0) as src:
            stamp = time.time()
            copied = 0
            while copied < size:
                buf = src.read(BUFSIZE)
                dst.write(buf)
                copied += len(buf)
            upload_speed = copied / (time.time() - stamp)
    log.info('Backend throughput: %.2f KB/sec', upload_speed / 1024)
    bucket.delete('s3ql_testdata')
    
    src = options.file
    size = os.fstat(options.file.fileno()).st_size 
    log.info('Test file size: %.2f MB', (size / 1024**2))
        
    times = dict()
    out_sizes = dict()
    for alg in ('lzma', 'bzip2', 'zlib'):
        log.info('compressing with %s...', alg)
        bucket = BetterBucket('pass', alg, Bucket(bucket_dir, None, None))
        with bucket.open_write('s3ql_testdata') as dst:
            src.seek(0)
            stamp = time.time()
            while True:
                buf = src.read(BUFSIZE)
                if not buf:
                    break
                dst.write(buf)
            times[alg] = time.time() - stamp
            out_sizes[alg] = dst.get_obj_size()
        log.info('%s compression speed: %.2f KB/sec (in)', alg, size/times[alg]/1024)
        log.info('%s compression speed: %.2f KB/sec (out)', alg, 
                 out_sizes[alg] / times[alg] / 1024)

    print('')
    
    req = dict()
    for alg in ('lzma', 'bzip2', 'zlib'):
        backend_req = math.ceil(upload_speed * times[alg] / out_sizes[alg])
        fuse_req = math.ceil(fuse_speed * times[alg] / size)
        req[alg] = min(backend_req, fuse_req)
        print('When using %s compression, incoming writes can keep up to %d threads\n'
              'busy. The backend can handle data from up to %d threads. Therefore,\n'
              'the maximum achievable throughput is %.2f KB/sec with %d threads.\n'
              % (alg, fuse_req, backend_req, min(upload_speed, fuse_speed)/1024, req[alg]))
        
    print('All numbers assume that the test file is representative and that',
          'there are enough processor cores to run all threads in parallel.',
          'To compensate for network latency, you should start about twice as',
          'many upload threads as you need for optimal performance.\n', sep='\n')
    
    cores = os.sysconf('SC_NPROCESSORS_ONLN')
    best_size = None
    max_threads = cores
    while best_size is None:
        for alg in out_sizes:
            if req[alg] > max_threads:
                continue
            if best_size is None or out_sizes[alg] < best_size:
                best_size = out_sizes[alg]
                best_alg = alg
                threads = req[alg]
                
        max_threads = min(req.itervalues())
    
    print('This system appears to have %d cores, so best performance with maximum\n'
          'compression ratio would be achieved by using %s compression with %d\n'
          'upload threads.' % (cores, best_alg,
                                2 * threads if cores >= threads else 2 * cores))    

if __name__ == '__main__':
    main(sys.argv[1:])
