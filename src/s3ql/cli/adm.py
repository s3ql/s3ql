'''
adm.py - this file is part of S3QL (http://s3ql.googlecode.com)

Copyright (C) 2008-2009 Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU LGPL.
'''

from __future__ import division, print_function, absolute_import

import logging
from s3ql.common import (get_backend, QuietError, unlock_bucket, LoggerFilter,
                      cycle_metadata, dump_metadata, restore_metadata, canonicalize_storage_url,
                      add_file_logging, add_stdout_logging, setup_excepthook)
from s3ql.optparse import (OptionParser, ArgumentGroup)
from s3ql import CURRENT_FS_REV
from s3ql.mkfs import create_indices
from getpass import getpass
import sys
from s3ql.backends import s3, local, sftp
from s3ql.backends.common import ChecksumError
import os
import s3ql.database as dbcm
import tempfile
import errno
import textwrap

log = logging.getLogger("adm")

def parse_args(args):
    '''Parse command line'''

    parser = OptionParser(
        usage="%prog [options] <action> <storage-url>\n"
              "%prog --help",
        description="Manage S3QL Buckets.")

    group = ArgumentGroup(parser, "<action> may be either of")
    group.add_argument("passphrase", "Change bucket passphrase")
    group.add_argument("upgrade", "Upgrade file system to newest revision.")
    group.add_argument("delete", "Completely delete a bucket with all contents.")
    parser.add_option_group(group)
    
    parser.add_option("--debug", action="append", metavar='<module>',
                      help="Activate debugging output from <module>. Use `all` "
                           "to get debug messages from all modules. This option can be "
                           "specified multiple times.")
    parser.add_option("--quiet", action="store_true", default=False,
                      help="Be really quiet")
    parser.add_option("--homedir", type="string", metavar='<path>',
                      default=os.path.expanduser("~/.s3ql"),
                      help='Directory for log files, cache and authentication info. '
                      'Default: ~/.s3ql')

    (options, pps) = parser.parse_args(args)
    if len(pps) != 2:
        parser.error("Incorrect number of arguments.")

    options.action = pps[0]
    options.storage_url = canonicalize_storage_url(pps[1])

    if options.action not in group.arguments:
        parser.error("Invalid <action>: %s" % options.action)
        
    if not os.path.exists(options.homedir):
        os.mkdir(options.homedir, 0700)
        
    return options

def main(args=None):
    '''Change or show S3QL file system parameters'''

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
        add_stdout_logging(options.quiet)
        add_file_logging(os.path.join(options.homedir, 'adm.log'))
        setup_excepthook()
        
        if options.debug:
            root_logger.setLevel(logging.DEBUG)
            if 'all' not in options.debug:
                root_logger.addFilter(LoggerFilter(options.debug, logging.INFO))
        else:
            root_logger.setLevel(logging.INFO) 
    else:
        log.info("Logging already initialized.")


    with get_backend(options.storage_url, options.homedir) as (conn, bucketname):
        if not bucketname in conn:
            raise QuietError("Bucket does not exist.")
        bucket = conn.get_bucket(bucketname)

        if options.action == 'delete':
            return delete_bucket(conn, bucketname)

        try:
            unlock_bucket(options.homedir, options.storage_url, bucket)
        except ChecksumError:
            raise QuietError('Checksum error - incorrect password?')

        if options.action == 'passphrase':
            return change_passphrase(bucket)

        if options.action == 'upgrade':
            return upgrade(conn, bucket)


def change_passphrase(bucket):
    '''Change bucket passphrase'''

    if 's3ql_passphrase' not in bucket:
        raise QuietError('Bucket is not encrypted.')

    data_pw = bucket.passphrase

    if sys.stdin.isatty():
        wrap_pw = getpass("Enter new encryption password: ")
        if not wrap_pw == getpass("Confirm new encryption password: "):
            raise QuietError("Passwords don't match")
    else:
        wrap_pw = sys.stdin.readline().rstrip()

    bucket.passphrase = wrap_pw
    bucket['s3ql_passphrase'] = data_pw

def delete_bucket(conn, bucketname):
    print('I am about to delete the bucket %s with ALL contents.' % bucketname,
          'Please enter "yes" to continue.', '> ', sep='\n', end='')

    if sys.stdin.readline().strip().lower() != 'yes':
        raise QuietError(1)

    log.info('Deleting...')
    conn.delete_bucket(bucketname, recursive=True)

    print('Bucket deleted.')
    if isinstance(conn, s3.Connection):
        print('Note that it may take a while until the removal becomes visible.')

def upgrade(conn, bucket):
    '''Upgrade file system to newest revision'''

    print(textwrap.dedent('''
        I am about to update the file system to the newest revision. Note that:
        
         - You should make very sure that this command is not interrupted and
           that no one else tries to mount, fsck or upgrade the file system at
           the same time.
        
         - You will not be able to access the file system with any older version
           of S3QL after this operation. 
           
         - The upgrade can only be done from the *previous* revision. If you
           skipped an intermediate revision, you have to use an intermediate
           version of S3QL to first upgrade the file system to the previous
           revision.
        '''))

    print('Please enter "yes" to continue.', '> ', sep='\n', end='')

    if sys.stdin.readline().strip().lower() != 'yes':
        raise QuietError(1)

    log.info('Getting file system parameters..')
    seq_nos = [ int(x[len('s3ql_seq_no_'):]) for x in bucket.list('s3ql_seq_no_') ]
    if not seq_nos:
        raise QuietError('File system revision too old to upgrade.')
    seq_no = max(seq_nos)
    param = bucket.lookup('s3ql_metadata')

    # Check revision
    if param['revision'] < CURRENT_FS_REV - 1:
        raise QuietError('File system revision too old to upgrade.')

    elif param['revision'] >= CURRENT_FS_REV:
        print('File system already at most-recent revision')
        return

    # Check that the fs itself is clean
    if param['needs_fsck']:
        raise QuietError("File system damaged, run fsck!")

    # Check for unclean shutdown on other computer
    if param['seq_no'] < seq_no and False:
        if isinstance(bucket, s3.Bucket):
            raise QuietError(textwrap.fill(textwrap.dedent('''
                It appears that the file system is still mounted somewhere else. If this is not
                the case, the file system may have not been unmounted cleanly or the data from
                the most-recent mount may have not yet propagated through S3. In the later case,
                waiting for a while should fix the problem, in the former case you should try to
                run fsck on the computer where the file system has been mounted most recently.
                ''')))
        else:
            raise QuietError(textwrap.fill(textwrap.dedent('''
                It appears that the file system is still mounted somewhere else. If this is not
                the case, the file system may have not been unmounted cleanly and you should try
                to run fsck on the computer where the file system has been mounted most recently.
                ''')))
    elif param['seq_no'] > seq_no:
        raise RuntimeError('param[seq_no] > seq_no, this should not happen.')

    # Download metadata
    log.info("Downloading & uncompressing metadata...")
    if param['DB-Format'] == 'dump':
        dbfile = tempfile.NamedTemporaryFile()
        dbcm.init(dbfile.name)
        fh = tempfile.TemporaryFile()
        bucket.fetch_fh("s3ql_metadata", fh)
        fh.seek(0)
        log.info('Reading metadata...')
        restore_metadata(fh)
        fh.close()
    elif param['DB-Format'] == 'sqlite':
        dbfile = tempfile.NamedTemporaryFile()
        bucket.fetch_fh("s3ql_metadata", dbfile)
        dbfile.flush()
        dbcm.init(dbfile.name)
    else:
        raise RuntimeError('Unsupported DB format: %s' % param['DB-Format'])
    
    log.info("Indexing...")
    create_indices(dbcm)
    dbcm.execute('ANALYZE')
    
    log.info('Upgrading from revision %d to %d...', CURRENT_FS_REV - 1,
             CURRENT_FS_REV)
    param['revision'] = CURRENT_FS_REV
    
    # Update local backend directory structure
    if isinstance(bucket, local.Bucket):
        for name in os.listdir(bucket.name):
            if not name.endswith('.dat'):
                continue
            name = name[:-4]
            newname = bucket.key_to_path(local.unescape(name))
            oldname = os.path.join(bucket.name, name)
            if name == newname:
                continue
            try:
                os.rename(oldname + '.dat', newname + '.dat')
            except OSError as exc:
                if exc.errno != errno.ENOENT:
                    raise
                os.makedirs(os.path.dirname(newname))
                os.rename(oldname + '.dat', newname + '.dat')
            os.rename(oldname + '.meta', newname + '.meta')
    elif isinstance(bucket, sftp.Bucket):
        for name in bucket.conn.sftp.listdir(bucket.name):
            if not name.endswith('.dat'):
                continue
            name = name[:-4]
            newname = bucket.key_to_path(local.unescape(name))
            oldname = os.path.join(bucket.name, name)
            if oldname == newname:
                continue
            try:
                bucket.conn.sftp.rename(oldname + '.dat', newname + '.dat')
            except IOError as exc:
                if exc.errno != errno.ENOENT:
                    raise
                bucket._makedirs(os.path.dirname(newname))
                bucket.conn.sftp.rename(oldname + '.dat', newname + '.dat')
            bucket.conn.sftp.rename(oldname + '.meta', newname + '.meta')
                                

    # Increase metadata sequence no
    param['seq_no'] += 1
    bucket.store('s3ql_seq_no_%d' % param['seq_no'], 'Empty')
    for i in seq_nos:
        if i < param['seq_no'] - 5:
            del bucket['s3ql_seq_no_%d' % i ]

    # Upload metadata in dump format, so that we have the newest
    # table definitions on the next mount.
    param['DB-Format'] = 'dump'
    fh = tempfile.TemporaryFile()
    dump_metadata(fh)
    fh.seek(0)
    log.info("Uploading database..")
    cycle_metadata(bucket)
    bucket.store_fh("s3ql_metadata", fh, param)
    fh.close()


if __name__ == '__main__':
    main(sys.argv[1:])
