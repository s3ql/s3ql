#!/usr/bin/env python3
'''
clone_fs.py - this file is part of S3QL.

Clone an S3QL file system from one backend into another, without
recompressing or reencrypting.

Copyright © 2008 Nikolaus Rath <Nikolaus@rath.org>

This work can be distributed under the terms of the GNU GPLv3.
'''

import os
import sys
import tempfile
import time
import argparse
from queue import Queue, Full as QueueFull

# We are running from the S3QL source directory, make sure
# that we use modules from this directory
basedir = os.path.abspath(os.path.join(os.path.dirname(sys.argv[0]), '..'))
if (os.path.exists(os.path.join(basedir, 'setup.py')) and
    os.path.exists(os.path.join(basedir, 'src', 's3ql', '__init__.py'))):
    sys.path = [os.path.join(basedir, 'src')] + sys.path

from s3ql.logging import logging, setup_logging, QuietError
from s3ql.common import AsyncFn, handle_on_return
from s3ql.backends.common import DanglingStorageURLError, NoSuchObject
from s3ql import BUFSIZE
from s3ql.parse_args import ArgumentParser, storage_url_type

log = logging.getLogger(__name__)

def parse_args(args):
    '''Parse command line'''

    parser = ArgumentParser(
                description='Clone an S3QL file system.')

    parser.add_quiet()
    parser.add_log()
    parser.add_debug()
    parser.add_backend_options()
    parser.add_version()

    parser.add_argument("--threads", type=int, default=3,
                        help='Number of threads to use')

    # Can't use parser.add_storage_url(), because we need both a source
    # and destination.
    parser.add_argument("--authfile", type=str, metavar='<path>',
                      default=os.path.expanduser("~/.s3ql/authinfo2"),
                      help='Read authentication credentials from this file '
                      '(default: `~/.s3ql/authinfo2)`')
    parser.add_argument("src_storage_url", metavar='<source-storage-url>',
                        type=storage_url_type,
                        help='Storage URL of the source backend that contains the file system')
    parser.add_argument("dst_storage_url", metavar='<destination-storage-url>',
                        type=storage_url_type,
                        help='Storage URL of the destination backend')


    options = parser.parse_args(args)
    setup_logging(options)

    # Print message so that the user has some idea what credentials are
    # wanted (if not specified in authfile).
    log.info('Connecting to source backend...')
    options.storage_url = options.src_storage_url
    parser._init_backend_factory(options)
    src_options = argparse.Namespace()
    src_options.__dict__.update(options.__dict__)
    options.src_backend_factory = lambda: src_options.backend_class(src_options)

    log.info('Connecting to destination backend...')
    options.storage_url = options.dst_storage_url
    parser._init_backend_factory(options)
    dst_options = argparse.Namespace()
    dst_options.__dict__.update(options.__dict__)
    options.dst_backend_factory = lambda: dst_options.backend_class(dst_options)
    del options.storage_url
    del options.backend_class

    return options

@handle_on_return
def copy_loop(queue, src_backend_factory, dst_backend_factory, on_return):
    '''Copy keys arriving in *queue* from *src_backend* to *dst_backend*

    Terminate when None is received.
    '''

    src_backend = on_return.enter_context(src_backend_factory())
    dst_backend = on_return.enter_context(dst_backend_factory())
    tmpfh = on_return.enter_context(tempfile.TemporaryFile())
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
    options = parse_args(args)

    src_backend_factory = options.src_backend_factory
    dst_backend_factory = options.dst_backend_factory

    # Try to access both backends before starting threads
    try:
        src_backend_factory().lookup('s3ql_metadata')
        try:
            dst_backend_factory().lookup('some random object')
        except NoSuchObject:
            pass
    except DanglingStorageURLError as exc:
        raise QuietError(str(exc)) from None

    queue = Queue(maxsize=options.threads)
    threads = []
    for _ in range(options.threads):
        t = AsyncFn(copy_loop, queue, src_backend_factory,
                    dst_backend_factory)
        # Don't wait for worker threads, gives deadlock if main thread
        # terminates with exception
        t.daemon = True
        t.start()
        threads.append(t)

    with src_backend_factory() as backend:
        stamp1 = 0
        for (i, key) in enumerate(backend):
            stamp2 = time.time()
            if stamp2 - stamp1 > 1:
                stamp1 = stamp2
                sys.stdout.write('\rCopied %d objects so far...' % i)
                sys.stdout.flush()

                # Terminate early if any thread failed with an exception
                for t in threads:
                    if not t.is_alive():
                        t.join_and_raise()

            # Avoid blocking if all threads terminated
            while True:
                try:
                    queue.put(key, timeout=1)
                except QueueFull:
                    pass
                else:
                    break
                for t in threads:
                    if not t.is_alive():
                        t.join_and_raise()
    sys.stdout.write('\n')

    queue.maxsize += len(threads)
    for t in threads:
        queue.put(None)

    for t in threads:
        t.join_and_raise()

if __name__ == '__main__':
    main(sys.argv[1:])
