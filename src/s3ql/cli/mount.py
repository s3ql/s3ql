'''
mount.py - this file is part of S3QL (http://s3ql.googlecode.com)

Copyright (C) 2008-2009 Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU LGPL.
'''

from __future__ import division, print_function, absolute_import

# We can't use relative imports because this file may
# be directly executed.
import sys
from s3ql import fs, CURRENT_FS_REV
from s3ql.mkfs import create_indices
from s3ql.daemonize import daemonize
from s3ql.backends.common import (ChecksumError, NoSuchObject)
from s3ql.common import (setup_logging, get_backend, get_bucket_home,
                         QuietError, unlock_bucket,  
                         cycle_metadata, dump_metadata, restore_metadata, 
                         ExceptionStoringThread, copy_metadata)
from s3ql.argparse import ArgumentParser
from s3ql.database import Connection
import llfuse
import tempfile
import textwrap
import os
import stat
import time
import threading
import logging
import cPickle as pickle

__all__ = [ 'main' ]

log = logging.getLogger("mount")

def main(args=None):
    '''Mount S3QL file system'''

    try:
        import psyco
        psyco.profile()
    except ImportError:
        pass

    if args is None:
        args = sys.argv[1:]

    options = parse_args(args)

    # Save handler so that we can remove it when daemonizing
    stdout_log_handler = setup_logging(options, 'mount.log')

    if not os.path.exists(options.mountpoint):
        raise QuietError('Mountpoint does not exist.')

    if options.profile:
        import cProfile
        import pstats
        prof = cProfile.Profile()

    with get_backend(options.storage_url, options.homedir) as (conn, bucketname):

        if not bucketname in conn:
            raise QuietError("Bucket does not exist.")
        bucket = conn.get_bucket(bucketname, compression=options.compress)

        # Unlock bucket
        try:
            unlock_bucket(options.homedir, options.storage_url, bucket)
        except ChecksumError:
            raise QuietError('Checksum error - incorrect password?')

        # Get paths
        home = get_bucket_home(options.storage_url, options.homedir)

        # Check for unclean shutdown on this computer
        if (os.path.exists(home + '.db')
            or os.path.exists(home + '.params')
            or os.path.exists(home + '-cache')):
            raise QuietError('Local cache files exist, file system has not been unmounted\n'
                             'cleanly. You need to run fsck.s3ql.')

        lock = threading.Lock()
        fuse_opts = get_fuse_opts(options)

        # Get file system parameters
        log.info('Getting file system parameters..')
        seq_nos = [ int(x[len('s3ql_seq_no_'):]) for x in bucket.list('s3ql_seq_no_') ]
        if not seq_nos:
            raise QuietError('Old file system revision, please run `s3qladm upgrade` first.')
        seq_no = max(seq_nos)
        param = bucket.lookup('s3ql_metadata')

        # Check revision
        if param['revision'] < CURRENT_FS_REV:
            raise QuietError('File system revision too old, please run `s3qladm upgrade` first.')
        elif param['revision'] > CURRENT_FS_REV:
            raise QuietError('File system revision too new, please update your '
                             'S3QL installation.')

        # Check that the fs itself is clean
        if param['needs_fsck']:
            raise QuietError("File system damaged, run fsck!")

        # Check for unclean shutdown on other computer
        if param['seq_no'] < seq_no:
            if bucket.read_after_write_consistent():
                raise QuietError(textwrap.fill(textwrap.dedent('''\
                    It appears that the file system is still mounted somewhere else. If this is not
                    the case, the file system may have not been unmounted cleanly and you should try
                    to run fsck on the computer where the file system has been mounted most recently.
                    ''')))
            else:                
                raise QuietError(textwrap.fill(textwrap.dedent('''\
                    It appears that the file system is still mounted somewhere else. If this is not the
                    case, the file system may have not been unmounted cleanly or the data from the 
                    most-recent mount may have not yet propagated through the backend. In the later case,
                    waiting for a while should fix the problem, in the former case you should try to run
                    fsck on the computer where the file system has been mounted most recently.
                    ''')))

        elif param['seq_no'] > seq_no:
            raise RuntimeError('param[seq_no] > seq_no, this should not happen.')

        if (time.time() - time.timezone) - param['last_fsck'] > 60 * 60 * 24 * 31:
            log.warn('Last file system check was more than 1 month ago, '
                     'running fsck.s3ql is recommended.')

        # Download metadata
        log.info("Downloading & uncompressing metadata...")
        fh = os.fdopen(os.open(home + '.db', os.O_RDWR | os.O_CREAT,
                               stat.S_IRUSR | stat.S_IWUSR), 'w+b')
        try:
            if param['DB-Format'] == 'dump':
                fh.close()
                db = Connection(home + '.db')
                fh = tempfile.TemporaryFile()
                bucket.fetch_fh("s3ql_metadata", fh)
                fh.seek(0)
                log.info('Reading metadata...')
                restore_metadata(fh, db)
                fh.close()
            elif param['DB-Format'] == 'sqlite':
                bucket.fetch_fh("s3ql_metadata", fh)
                fh.close()
                db = Connection(home + '.db')
            else:
                raise RuntimeError('Unsupported DB format: %s' % param['DB-Format'])
        except:
            # Don't keep file if it doesn't contain anything sensible
            os.unlink(home + '.db')
            raise
        
        log.info('Indexing...')
        create_indices(db)
        db.execute('ANALYZE')
        
        # Increase metadata sequence no, save parameters 
        param['seq_no'] += 1
        try:
            pickle.dump(param, open(home + '.params', 'wb'), 2)
            bucket.store('s3ql_seq_no_%d' % param['seq_no'], 'Empty')
        except:
            os.unlink(home + '.params')
            os.unlink(home + '.db')
            raise
        param['seq_no'] -= 1
                    
        for i in seq_nos:
            if i < param['seq_no'] - 5:
                try:
                    del bucket['s3ql_seq_no_%d' % i ]
                except NoSuchObject:
                    pass # Key list may not be up to date

        operations = fs.Operations(bucket, db, cachedir=home + '-cache', lock=lock,
                                   blocksize=param['blocksize'],
                                   cache_size=options.cachesize * 1024,
                                   cache_entries=options.max_cache_entries)
 
        metadata_upload_thread = MetadataUploadThread(bucket, options, param, lock,
                                                      db)

        log.info('Mounting filesystem...')
        llfuse.init(operations, options.mountpoint, fuse_opts, lock)
        try:
            if not options.fg:
                conn.prepare_fork()
                me = threading.current_thread()
                for t in threading.enumerate():
                    if t is me:
                        continue
                    log.error('Waiting for thread %s', t)
                    t.join()
  
                if stdout_log_handler:
                    logging.getLogger().removeHandler(stdout_log_handler)
                daemonize(options.homedir)
                conn.finish_fork()
            
            metadata_upload_thread.start()
            if options.profile:
                prof.runcall(llfuse.main, options.single)
            else:
                llfuse.main(options.single)

        finally:
            llfuse.close()

        metadata_upload_thread.stop()
        db_mtime = metadata_upload_thread.db_mtime
        
        if operations.encountered_errors:
            param['needs_fsck'] = True
        else:       
            param['seq_no'] += 1

        if db_mtime == os.stat(home + '.db').st_mtime:
            log.info('File system unchanged, not uploading metadata.')
            del bucket['s3ql_seq_no_%d' % param['seq_no']]     
        else:
            if options.strip_meta:
                log.info('Saving metadata...')
                param['DB-Format'] = 'dump'
                fh = tempfile.TemporaryFile()
                dump_metadata(fh, db) 
            else:
                param['DB-Format'] = 'sqlite'
                db.execute('VACUUM')
                db.close()
                fh = open(db.file, 'rb')   
                    
            log.info("Compressing & uploading metadata..")
            cycle_metadata(bucket)
            fh.seek(0)
            bucket.store_fh("s3ql_metadata", fh, param)
            fh.close()
        
        if not operations.encountered_errors:
            log.debug("Cleaning up...")
            os.unlink(home + '.db')
            os.unlink(home + '.params')

    if options.profile:
        tmp = tempfile.NamedTemporaryFile()
        prof.dump_stats(tmp.name)
        fh = open('s3ql_profile.txt', 'w')
        p = pstats.Stats(tmp.name, stream=fh)
        tmp.close()
        p.strip_dirs()
        p.sort_stats('cumulative')
        p.print_stats(50)
        p.sort_stats('time')
        p.print_stats(50)
        fh.close()

    if operations.encountered_errors:
        raise QuietError('Some errors were encountered while the file system was mounted,\n'
                         'you should run fsck.s3ql and examine ~/.s3ql/mount.log.')


def get_fuse_opts(options):
    '''Return fuse options for given command line options'''

    fuse_opts = [ b"nonempty", b'fsname=%s' % options.storage_url,
                  'subtype=s3ql' ]

    if options.allow_other:
        fuse_opts.append(b'allow_other')
    if options.allow_root:
        fuse_opts.append(b'allow_root')
    if options.allow_other or options.allow_root:
        fuse_opts.append(b'default_permissions')

    return fuse_opts



def parse_args(args):
    '''Parse command line
    
    This function writes to stdout/stderr and may call `system.exit()` instead 
    of throwing an exception if it encounters errors.
    '''

    # Parse fstab-style -o options
    if '--' in args:
        max_idx = args.index('--')
    else:
        max_idx = len(args)
    if '-o' in args[:max_idx]:
        pos = args.index('-o')
        val = args[pos + 1]
        del args[pos]
        del args[pos]
        for opt in reversed(val.split(',')):
            if '=' in opt:
                (key, val) = opt.split('=')
                args.insert(pos, val)
                args.insert(pos, '--' + key)
            else:
                if opt in ('rw', 'defaults', 'auto', 'noauto', 'user', 'nouser', 'dev', 'nodev',
                           'suid', 'nosuid', 'atime', 'diratime', 'exec', 'noexec', 'group',
                           'mand', 'nomand', '_netdev', 'nofail', 'norelatime', 'strictatime',
                           'owner', 'users', 'nobootwait'):
                    continue
                elif opt == 'ro':
                    raise QuietError('Read-only mounting not supported.')
                args.insert(pos, '--' + opt)

    parser = ArgumentParser(
        description="Mount an S3QL file system.")

    parser.add_homedir()
    parser.add_debug_modules()
    parser.add_quiet()
    parser.add_version()
    parser.add_storage_url()
    
    parser.add_argument("mountpoint", metavar='<mountpoint>',
                        type=(lambda x: x.rstrip('/')),
                        help='Where to mount the file system')
        
    parser.add_argument("--cachesize", type=int, default=102400, metavar='<size>', 
                      help="Cache size in kb (default: 102400 (100 MB)). Should be at least 10 times "
                      "the blocksize of the filesystem, otherwise an object may be retrieved and "
                      "written several times during a single write() or read() operation.")
    parser.add_argument("--max-cache-entries", type=int, default=768, metavar='<num>',
                      help="Maximum number of entries in cache (default: %(default)d). "
                      'Each cache entry requires one file descriptor, so if you increase '
                      'this number you have to make sure that your process file descriptor '
                      'limit (as set with `ulimit -n`) is high enough (at least the number ' 
                      'of cache entries + 100).')
    parser.add_argument("--allow-other", action="store_true", default=False, help=
                      'Normally, only the user who called `mount.s3ql` can access the mount '
                      'point. This user then also has full access to it, independent of '
                      'individual file permissions. If the `--allow-other` option is '
                      'specified, other users can access the mount point as well and '
                      'individual file permissions are taken into account for all users.')
    parser.add_argument("--allow-root", action="store_true", default=False,
                      help='Like `--allow-other`, but restrict access to the mounting '
                           'user and the root user.')
    parser.add_argument("--fg", action="store_true", default=False,
                      help="Do not daemonize, stay in foreground")
    parser.add_argument("--single", action="store_true", default=False,
                      help="Run in single threaded mode. If you don't understand this, "
                           "then you don't need it.")
    parser.add_argument("--profile", action="store_true", default=False,
                      help="Create profiling information. If you don't understand this, "
                           "then you don't need it.")
    parser.add_argument("--compress", action="store", default='lzma', metavar='<name>',
                      choices=('lzma', 'bzip2', 'zlib', 'none'),
                      help="Compression algorithm to use when storing new data. Allowed "
                           "values: `lzma`, `bzip2`, `zlib`, none. (default: `%(default)s`)")
    parser.add_argument("--strip-meta", action="store_true", default=False,
                      help='Strip metadata of all redundancies (like indices) before '
                      'uploading. This will significantly reduce the size of the data '
                      'at the expense of additional CPU time during the next unmount '
                      'and mount.')
    parser.add_argument("--metadata-upload-interval", action="store", type=int,
                      default=24*60*60, metavar='<seconds>',
                      help='Interval in seconds between complete metadata uploads. '
                      'default: 24h.')
    parser.add_argument("--compression-threads", action="store", type=int,
                      default=1, metavar='<no>',
                      help='Number of parallel compression and encryption threads '
                           'to use (default: %(default)s).')
    
    options = parser.parse_args(args)

    if options.allow_other and options.allow_root:
        parser.error("--allow-other and --allow-root are mutually exclusive.")

    if options.profile:
        options.single = True

    if options.compress == 'none':
        options.compress = None

    if options.strip_meta and options.compress is None:
        parser.error('--strip-meta and --compress=none does not make much sense and '
                     'is really not a good idea.')
        
    if not os.path.exists(options.homedir):
        os.mkdir(options.homedir, 0700)
                
    from .. import upload_manager
    upload_manager.MAX_COMPRESS_THREADS = options.compression_threads
    
    return options

class MetadataUploadThread(ExceptionStoringThread):

    def __init__(self, bucket, options, param, lock, db):
        super(MetadataUploadThread, self).__init__()
        self.bucket = bucket
        self.options = options
        self.param = param
        self.lock = lock
        self.db = db
        self.daemon = True
        self.db_mtime = os.stat(db.file).st_mtime 
        self.stop_event = threading.Event()
        self.name = 'Metadata-Upload-Thread'
           
    def run_protected(self):
        log.debug('MetadataUploadThread: start')
        
        while not self.stop_event.is_set():
            self.stop_event.wait(self.options.metadata_upload_interval)
            
            with self.lock:
                new_mtime = os.stat(self.db.file).st_mtime 
                if self.db_mtime == new_mtime:
                    log.info('File system unchanged, not uploading metadata.')
                    continue
                
                log.info('Saving metadata...')
                if self.options.strip_meta:
                    self.param['DB-Format'] = 'dump'
                    fh = tempfile.TemporaryFile()
                    dump_metadata(fh, self.db) 
                else:
                    self.param['DB-Format'] = 'sqlite'
                    fh = tempfile.NamedTemporaryFile()
                    copy_metadata(fh, self.db)
                        
            log.info("Compressing & uploading metadata..")
            cycle_metadata(self.bucket)
            fh.seek(0)
            self.bucket.store_fh("s3ql_metadata", fh, self.param)
            self.db_mtime = new_mtime    

        log.debug('MetadataUploadThread: end')    
        
    def stop(self):
        '''Wait for thread to finish, raise any occurred exceptions'''
        
        self.stop_event.set()
        self.join_and_raise()

if __name__ == '__main__':
    main(sys.argv[1:])
