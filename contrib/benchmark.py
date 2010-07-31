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
from s3ql.common import (get_backend, QuietError, add_stdout_logging, setup_excepthook)
from s3ql.optparse import OptionParser

log = logging.getLogger('benchmark')

def parse_args(args):
    '''Parse command line'''

    parser = OptionParser(
        usage="%prog [options] <storage-url> <test-file>\n"
              "%prog --help",
        description="Transfers and compresses the test file and gives a recommendation "
                    "for the compression algorithm to use.")

    parser.add_option("--homedir", type="string",
                      default=os.path.expanduser("~/.s3ql"),
                      help='Directory for log files, cache and authentication info. '
                      'Default: ~/.s3ql')
    parser.add_option("--debug", action="store_true",
                      help="Activate debugging output")
    parser.add_option("--quiet", action="store_true", default=False,
                      help="Be really quiet")
    (options, pps) = parser.parse_args(args)

    #
    # Verify parameters
    #
    if len(pps) != 2:
        parser.error("Incorrect number of arguments.")
    options.storage_url = pps[0]
    options.testfile = pps[1]

    return options


def main(args=None):
    if args is None:
        args = sys.argv[1:]

    try:
        import psyco
        psyco.profile()
    except ImportError:
        pass

    options = parse_args(args)

    # Initialize logging if not yet initialized
    root_logger = logging.getLogger()
    if not root_logger.handlers:
        handler = add_stdout_logging(options.quiet)
        setup_excepthook()  
        if options.debug:
            root_logger.setLevel(logging.DEBUG)
            handler.setLevel(logging.DEBUG)
        else:
            root_logger.setLevel(logging.INFO)         
    else:
        log.info("Logging already initialized.")

    if not os.path.exists(options.testfile):
        raise QuietError('Mountpoint does not exist.')

    with get_backend(options) as (conn, bucketname):

        if not bucketname in conn:
            raise QuietError("Bucket does not exist.")
        bucket = conn.get_bucket(bucketname)

        ifh = open(options.testfile, 'rb')
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
