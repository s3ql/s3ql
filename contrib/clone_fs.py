#!/usr/bin/env python3
'''
clone_fs.py - this file is part of S3QL (http://s3ql.googlecode.com)

Clone an S3QL file system from one backend into another, without
recompressing or reencrypting.

Copyright © 2008 Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU GPLv3.
'''

import os
import sys
import tempfile
import time
from queue import Queue

# We are running from the S3QL source directory, make sure
# that we use modules from this directory
basedir = os.path.abspath(os.path.join(os.path.dirname(sys.argv[0]), '..'))
if (os.path.exists(os.path.join(basedir, 'setup.py')) and
    os.path.exists(os.path.join(basedir, 'src', 's3ql', '__init__.py'))):
    sys.path = [os.path.join(basedir, 'src')] + sys.path

from s3ql.logging import logging, setup_logging, QuietError
from s3ql.common import get_backend, AsyncFn
from s3ql.backends.common import DanglingStorageURLError
from s3ql import BUFSIZE
from s3ql.parse_args import ArgumentParser, storage_url_type

log = logging.getLogger(__name__)

def parse_args(args):
    '''Parse command line'''

    parser = ArgumentParser(
                description='Clone an S3QL file system.')

    parser.add_authfile()
    parser.add_quiet()
    parser.add_debug()
    parser.add_backend_options()
    parser.add_version()

    parser.add_argument("src_storage_url", metavar='<source-storage-url>',
                        type=storage_url_type,
                        help='Storage URL of the source backend that contains the file system')

    parser.add_argument("dst_storage_url", metavar='<destination-storage-url>',
                        type=storage_url_type,
                        help='Storage URL of the destination backend')

    parser.add_argument("--threads", type=int, default=3,
                        help='Number of threads to use')

    return parser.parse_args(args)


def copy_loop(queue, src_backend, dst_backend):
    '''Copy keys arriving in *queue* from *src_backend* to *dst_backend*

    Terminate when None is received.
    '''

    tmpfh = tempfile.TemporaryFile()
    while True:
        key = queue.get()
        if key is None:
            break

        log.debug('reading object %s', key)
        def do_read(fh):
            tmpfh.seek(0)
            tmpfh.truncate()
            while True:
                buf = fh.read(BUFSIZE)
                if not buf:
                    break
                tmpfh.write(buf)
            return fh.metadata
        metadata = src_backend.perform_read(do_read, key)

        log.debug('writing object %s', key)
        def do_write(fh):
            tmpfh.seek(0)
            while True:
                buf = tmpfh.read(BUFSIZE)
                if not buf:
                    break
                fh.write(buf)
        dst_backend.perform_write(do_write, key, metadata)

def main(args=None):
    if args is None:
        args = sys.argv[1:]

    options = parse_args(args)
    setup_logging(options)

    src_backends = []
    dst_backends = []

    try:
        options.storage_url = options.src_storage_url
        for _ in range(options.threads+1):
            src_backends.append(get_backend(options, plain=True))

        options.storage_url = options.dst_storage_url
        for _ in range(options.threads):
            dst_backends.append(get_backend(options, plain=True))
    except DanglingStorageURLError as exc:
        raise QuietError(str(exc)) from None

    queue = Queue(maxsize=options.threads)
    threads = []
    for (src_backend, dst_backend) in zip(src_backends, dst_backends):
        t = AsyncFn(copy_loop, queue, src_backend, dst_backend)
        # Don't wait for worker threads, gives deadlock if main thread
        # terminates with exception
        t.daemon = True
        t.start()
        threads.append(t)

    if sys.stdout.isatty():
        stamp1 = time.time()
    else:
        stamp1 = None
    for (i, key) in enumerate(src_backends[-1]):
        stamp2 = time.time()
        if stamp1 is not None and stamp2 - stamp1 > 1:
            stamp1 = stamp2
            sys.stdout.write('\rCopied %d objects so far...' % i)
            sys.stdout.flush()

            # Terminate early if any thread failed with an exception
            for t in threads:
                if not t.is_alive():
                    t.join_and_raise()
        queue.put(key)

    queue.maxsize += len(threads)
    for t in threads:
        queue.put(None)

    for t in threads:
        t.join_and_raise()

    if sys.stdout.isatty():
        sys.stdout.write('\n')

if __name__ == '__main__':
    main(sys.argv[1:])
