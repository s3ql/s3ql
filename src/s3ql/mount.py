'''
mount.py - this file is part of S3QL (http://s3ql.googlecode.com)

Copyright (C) 2008-2009 Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU GPLv3.
'''

from .logging import logging, setup_logging, QuietError
from . import fs, CURRENT_FS_REV
from .backends.common import get_backend_factory, BackendPool, DanglingStorageURLError
from .block_cache import BlockCache
from .common import (get_backend_cachedir, get_seq_no, stream_write_bz2, stream_read_bz2,
                     PICKLE_PROTOCOL)
from .daemonize import daemonize
from .database import Connection
from .inode_cache import InodeCache
from .metadata import cycle_metadata, dump_metadata, restore_metadata
from .parse_args import ArgumentParser
from threading import Thread
from contextlib import ExitStack
import _thread
import argparse
import faulthandler
import llfuse
import os
import pickle
import re
import signal
import sys
import tempfile
import threading
import time
import atexit

log = logging.getLogger(__name__)

def install_thread_excepthook():
    """work around sys.excepthook thread bug
    
    See http://bugs.python.org/issue1230540.

    Call once from __main__ before creating any threads. If using
    psyco, call psyco.cannotcompile(threading.Thread.run) since this
    replaces a new-style class method.
    """

    init_old = threading.Thread.__init__
    def init(self, *args, **kwargs):
        init_old(self, *args, **kwargs)
        run_old = self.run
        def run_with_except_hook(*args, **kw):
            try:
                run_old(*args, **kw)
            except SystemExit:
                raise
            except:
                sys.excepthook(*sys.exc_info())
        self.run = run_with_except_hook

    threading.Thread.__init__ = init
install_thread_excepthook()

def main(args=None):
    '''Mount S3QL file system'''

    if args is None:
        args = sys.argv[1:]

    options = parse_args(args)
    
    # Save handler so that we can remove it when daemonizing
    stdout_log_handler = setup_logging(options)

    if options.threads is None:
        options.threads = determine_threads(options)

    if not os.path.exists(options.mountpoint):
        raise QuietError('Mountpoint does not exist.')

    if options.profile:
        import cProfile
        import pstats
        prof = cProfile.Profile()

    backend_factory = get_backend_factory(options)
    backend_pool = BackendPool(backend_factory)
    atexit.register(backend_pool.flush)
    
    # Get paths
    cachepath = get_backend_cachedir(options.storage_url, options.cachedir)

    # Retrieve metadata
    try:
        with backend_pool() as backend:
            (param, db) = get_metadata(backend, cachepath)
    except DanglingStorageURLError as exc:
        raise QuietError(str(exc)) from None

    if param['max_obj_size'] < options.min_obj_size:
        raise QuietError('Maximum object size must be bigger than minimum object size.')

    if options.nfs:
        # NFS may try to look up '..', so we have to speed up this kind of query
        log.info('Creating NFS indices...')
        db.execute('CREATE INDEX IF NOT EXISTS ix_contents_inode ON contents(inode)')

    else:
        db.execute('DROP INDEX IF EXISTS ix_contents_inode')

    metadata_upload_thread = MetadataUploadThread(backend_pool, param, db,
                                                  options.metadata_upload_interval)
    block_cache = BlockCache(backend_pool, db, cachepath + '-cache',
                             options.cachesize * 1024, options.max_cache_entries)
    commit_thread = CommitThread(block_cache)
    operations = fs.Operations(block_cache, db, max_obj_size=param['max_obj_size'],
                               inode_cache=InodeCache(db, param['inode_gen']),
                               upload_event=metadata_upload_thread.event)

    with ExitStack() as cm:
        log.info('Mounting filesystem...')
        try:
            llfuse.init(operations, options.mountpoint, get_fuse_opts(options))
        except RuntimeError as exc:
            raise QuietError(str(exc)) from None
        
        unmount_clean = False
        def unmount():
            log.info("Unmounting file system...")
            # Acquire lock so that Operations.destroy() is called with the
            # global lock like all other handlers
            with llfuse.lock:
                llfuse.close(unmount=unmount_clean)
        cm.callback(unmount)
        
        if options.fg:
            faulthandler.enable()
        else:
            if stdout_log_handler:
                logging.getLogger().removeHandler(stdout_log_handler)
            global crit_log_fh
            crit_log_fh = open(os.path.join(options.cachedir, 'mount.s3ql_crit.log'), 'a')
            faulthandler.enable(crit_log_fh)
            daemonize(options.cachedir)

        mark_metadata_dirty(backend, cachepath, param)
        
        block_cache.init(options.threads)
        cm.callback(block_cache.destroy)
        
        metadata_upload_thread.start()
        cm.callback(metadata_upload_thread.join)
        cm.callback(metadata_upload_thread.stop)

        commit_thread.start()
        cm.callback(commit_thread.join)
        cm.callback(commit_thread.stop)
        
        if options.upstart:
            os.kill(os.getpid(), signal.SIGSTOP)

        exc_info = setup_exchook()
        if options.profile:
            prof.runcall(llfuse.main, options.single)
        else:
            llfuse.main(options.single)

        # Allow operations to terminate while block_cache is still available
        # (destroy() will be called again when from llfuse.close(), but at that
        # point the block cache is no longer available).
        with llfuse.lock:
            operations.destroy()

        # Re-raise if main loop terminated due to exception in other thread
        if exc_info:
            (tmp0, tmp1, tmp2) = exc_info
            exc_info[:] = []
            raise tmp0(tmp1).with_traceback(tmp2)

        log.info("FUSE main loop terminated.")

        unmount_clean = True

    # At this point, there should be no other threads left

    # Do not update .params yet, dump_metadata() may fail if the database is
    # corrupted, in which case we want to force an fsck.
    param['max_inode'] = db.get_val('SELECT MAX(id) FROM inodes')
    if operations.failsafe:
        log.warn('File system errors encountered, marking for fsck.')
        param['needs_fsck'] = True
    with backend_pool() as backend:
        seq_no = get_seq_no(backend)
        if metadata_upload_thread.db_mtime == os.stat(cachepath + '.db').st_mtime:
            log.info('File system unchanged, not uploading metadata.')
            del backend['s3ql_seq_no_%d' % param['seq_no']]
            param['seq_no'] -= 1
            with open(cachepath + '.params', 'wb') as fh:
                pickle.dump(param, fh, PICKLE_PROTOCOL)
        elif seq_no == param['seq_no']:
            param['last-modified'] = time.time()

            log.info('Dumping metadata...')
            with tempfile.TemporaryFile() as fh:
                dump_metadata(db, fh)
                def do_write(obj_fh):
                    fh.seek(0)
                    stream_write_bz2(fh, obj_fh)
                    return obj_fh

                log.info("Compressing and uploading metadata...")
                obj_fh = backend.perform_write(do_write, "s3ql_metadata_new",
                                               metadata=param, is_compressed=True)
            log.info('Wrote %.2f MiB of compressed metadata', obj_fh.get_obj_size() / 1024 ** 2)
            log.info('Cycling metadata backups...')
            cycle_metadata(backend)
            with open(cachepath + '.params', 'wb') as fh:
                pickle.dump(param, fh, PICKLE_PROTOCOL)
        else:
            log.error('Remote metadata is newer than local (%d vs %d), '
                      'refusing to overwrite!', seq_no, param['seq_no'])
            log.error('The locally cached metadata will be *lost* the next time the file system '
                      'is mounted or checked and has therefore been backed up.')
            for name in (cachepath + '.params', cachepath + '.db'):
                for i in range(4)[::-1]:
                    if os.path.exists(name + '.%d' % i):
                        os.rename(name + '.%d' % i, name + '.%d' % (i + 1))
                os.rename(name, name + '.0')

    log.info('Cleaning up local metadata...')
    db.execute('ANALYZE')
    db.execute('VACUUM')
    db.close()

    if options.profile:
        with tempfile.NamedTemporaryFile() as tmp, \
            open('s3ql_profile.txt', 'w') as fh:
            prof.dump_stats(tmp.name)
            p = pstats.Stats(tmp.name, stream=fh)
            p.strip_dirs()
            p.sort_stats('cumulative')
            p.print_stats(50)
            p.sort_stats('time')
            p.print_stats(50)

    log.info('All done.')

# Memory required for LZMA compression in MB (from xz(1))
LZMA_MEMORY = { 0: 3, 1: 9, 2: 17, 3: 32, 4: 48,
                5: 94, 6: 94, 7: 186, 8: 370, 9: 674 }

def determine_threads(options):
    '''Return optimum number of upload threads'''

    cores = os.sysconf('SC_NPROCESSORS_ONLN')
    memory = os.sysconf('SC_PHYS_PAGES') * os.sysconf('SC_PAGESIZE')

    if options.compress[0] == 'lzma':
        # Keep this in sync with compression level in backends/common.py
        # Memory usage according to man xz(1)
        mem_per_thread = LZMA_MEMORY[options.compress[1]] * 1024 ** 2
    else:
        # Only check LZMA memory usage
        mem_per_thread = 0

    if cores == -1 or memory == -1:
        log.warning("Can't determine number of cores, using 2 upload threads.")
        return 1
    elif 2 * cores * mem_per_thread > (memory / 2):
        threads = min(int((memory / 2) // mem_per_thread), 10)
        if threads > 0:
            log.info('Using %d upload threads (memory limited).', threads)
        else:
            log.warning('Warning: compression will require %d MiB memory '
                     '(%d%% of total system memory', mem_per_thread / 1024 ** 2,
                     mem_per_thread * 100 / memory)
            threads = 1
        return threads
    else:
        threads = min(2 * cores, 10)
        log.info("Using %d upload threads.", threads)
        return threads

def get_metadata(backend, cachepath):
    '''Retrieve metadata'''

    seq_no = get_seq_no(backend)

    # Check for cached metadata
    db = None
    if os.path.exists(cachepath + '.params'):
        with open(cachepath + '.params', 'rb') as fh:
            param = pickle.load(fh)
        if param['seq_no'] < seq_no:
            log.info('Ignoring locally cached metadata (outdated).')
            param = backend.lookup('s3ql_metadata')
        elif param['seq_no'] > seq_no:
            raise QuietError("File system not unmounted cleanly, run fsck!")            
        else:
            log.info('Using cached metadata.')
            db = Connection(cachepath + '.db')
    else:
        param = backend.lookup('s3ql_metadata')

    # Check for unclean shutdown
    if param['seq_no'] < seq_no:
        raise QuietError('Backend reports that fs is still mounted elsewhere, aborting.')       

    # Check revision
    if param['revision'] < CURRENT_FS_REV:
        raise QuietError('File system revision too old, please run `s3qladm upgrade` first.')
    elif param['revision'] > CURRENT_FS_REV:
        raise QuietError('File system revision too new, please update your '
                         'S3QL installation.')

    # Check that the fs itself is clean
    if param['needs_fsck']:
        raise QuietError("File system damaged or not unmounted cleanly, run fsck!")
    if time.time() - param['last_fsck'] > 60 * 60 * 24 * 31:
        log.warning('Last file system check was more than 1 month ago, '
                 'running fsck.s3ql is recommended.')

    if  param['max_inode'] > 2 ** 32 - 50000:
        raise QuietError('Insufficient free inodes, fsck run required.')
    elif param['max_inode'] > 2 ** 31:
        log.warning('Few free inodes remaining, running fsck is recommended')

    # Download metadata
    if not db:
        with tempfile.TemporaryFile() as tmpfh:
            def do_read(fh):
                tmpfh.seek(0)
                tmpfh.truncate()
                stream_read_bz2(fh, tmpfh)

            log.info('Downloading and decompressing metadata...')
            backend.perform_read(do_read, "s3ql_metadata")
            
            log.info("Reading metadata...")
            tmpfh.seek(0)
            db = restore_metadata(tmpfh, cachepath + '.db')

    with open(cachepath + '.params', 'wb') as fh:
        pickle.dump(param, fh, PICKLE_PROTOCOL)
            
    return (param, db)

def mark_metadata_dirty(backend, cachepath, param):
    '''Mark metadata as dirty and increase sequence number'''
    
    param['seq_no'] += 1
    param['needs_fsck'] = True
    backend['s3ql_seq_no_%d' % param['seq_no']] = b'Empty'
    with open(cachepath + '.params', 'wb') as fh:
        pickle.dump(param, fh, PICKLE_PROTOCOL)
    param['needs_fsck'] = False


def get_fuse_opts(options):
    '''Return fuse options for given command line options'''

    fuse_opts = [ "nonempty", 'fsname=%s' % options.storage_url,
                  'subtype=s3ql' ]

    if options.allow_other:
        fuse_opts.append('allow_other')
    if options.allow_root:
        fuse_opts.append('allow_root')
    if options.allow_other or options.allow_root:
        fuse_opts.append('default_permissions')

    return fuse_opts


def parse_args(args):
    '''Parse command line'''

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

    def compression_type(s):
        hit = re.match(r'^([a-z]+)(?:-([0-9]))?$', s)
        if not hit:
            raise argparse.ArgumentTypeError('%s is not a valid --compress value' % s)
        alg = hit.group(1)
        lvl = hit.group(2)
        if alg not in ('none', 'zlib', 'bzip2', 'lzma'):
            raise argparse.ArgumentTypeError('Invalid compression algorithm: %s' % alg)
        if lvl is None:
            lvl = 6
        else:
            lvl = int(lvl)
        if alg == 'none':
            alg =  None
        return (alg, lvl)
        
    parser = ArgumentParser(
        description="Mount an S3QL file system.")

    parser.add_log('~/.s3ql/mount.log')
    parser.add_cachedir()
    parser.add_authfile()
    parser.add_debug_modules()
    parser.add_quiet()
    parser.add_ssl()
    parser.add_version()
    parser.add_storage_url()
    parser.add_fatal_warnings()

    parser.add_argument("mountpoint", metavar='<mountpoint>', type=os.path.abspath,
                        help='Where to mount the file system')
    parser.add_argument("--cachesize", type=int, default=102400, metavar='<size>',
                      help="Cache size in KiB (default: 102400 (100 MiB)). Should be at least 10 times "
                      "the maximum object size of the filesystem, otherwise an object may be retrieved "
                      "and written several times during a single write() or read() operation.")
    parser.add_argument("--max-cache-entries", type=int, default=768, metavar='<num>',
                      help="Maximum number of entries in cache (default: %(default)d). "
                      'Each cache entry requires one file descriptor, so if you increase '
                      'this number you have to make sure that your process file descriptor '
                      'limit (as set with `ulimit -n`) is high enough (at least the number '
                      'of cache entries + 100).')
    parser.add_argument("--min-obj-size", type=int, default=512, metavar='<size>',
                        help=argparse.SUPPRESS)
#                      help="Minimum size of storage objects in KiB. Files smaller than this "
#                           "may be combined into groups that are stored as single objects "
#                           "in the storage backend. Default: %(default)d KB.")
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
    parser.add_argument("--upstart", action="store_true", default=False,
                      help="Stay in foreground and raise SIGSTOP once mountpoint "
                           "is up.")
    parser.add_argument("--profile", action="store_true", default=False,
                      help="Create profiling information. If you don't understand this, "
                           "then you don't need it.")
    parser.add_argument("--compress", action="store", default='lzma-6',
                        metavar='<algorithm>[-<lvl>]', type=compression_type,
                        help="Compression algorithm and compression level to use when "
                             "storing new data. *algorithm* may be any of `lzma`, `bzip2`, "
                             "`zlib`, or none. *lvl* may be any integer from 0 (fastest) "
                             "to 9 (slowest). Default: `%(default)s`")
    parser.add_argument("--metadata-upload-interval", action="store", type=int,
                      default=24 * 60 * 60, metavar='<seconds>',
                      help='Interval in seconds between complete metadata uploads. '
                           'Set to 0 to disable. Default: 24h.')
    parser.add_argument("--threads", action="store", type=int,
                      default=None, metavar='<no>',
                      help='Number of parallel upload threads to use (default: auto).')
    parser.add_argument("--nfs", action="store_true", default=False,
                      help='Enable some optimizations for exporting the file system '
                           'over NFS. (default: %(default)s)')

    options = parser.parse_args(args)

    if options.allow_other and options.allow_root:
        parser.error("--allow-other and --allow-root are mutually exclusive.")

    if not options.log and not options.fg:
        parser.error("Please activate logging to a file or syslog, or use the --fg option.")

    if options.profile:
        options.single = True

    if options.upstart:
        options.fg = True

    if options.metadata_upload_interval == 0:
        options.metadata_upload_interval = None

    return options

class MetadataUploadThread(Thread):
    '''
    Periodically upload metadata. Upload is done every `interval`
    seconds, and whenever `event` is set. To terminate thread,
    set `quit` attribute as well as `event` event.
    
    This class uses the llfuse global lock. When calling objects
    passed in the constructor, the global lock is acquired first.    
    '''

    def __init__(self, backend_pool, param, db, interval):
        super().__init__()
        self.backend_pool = backend_pool
        self.param = param
        self.db = db
        self.interval = interval
        self.daemon = True
        self.db_mtime = os.stat(db.file).st_mtime
        self.event = threading.Event()
        self.quit = False
        self.name = 'Metadata-Upload-Thread'

    def run(self):
        log.debug('MetadataUploadThread: start')

        while not self.quit:
            self.event.wait(self.interval)
            self.event.clear()

            if self.quit:
                break

            with llfuse.lock:
                if self.quit:
                    break
                new_mtime = os.stat(self.db.file).st_mtime
                if self.db_mtime == new_mtime:
                    log.info('File system unchanged, not uploading metadata.')
                    continue

                log.info('Dumping metadata...')
                fh = tempfile.TemporaryFile()
                dump_metadata(self.db, fh)


            with self.backend_pool() as backend:
                seq_no = get_seq_no(backend)
                if seq_no != self.param['seq_no']:
                    log.error('Remote metadata is newer than local (%d vs %d), '
                              'refusing to overwrite!', seq_no, self.param['seq_no'])
                    fh.close()
                    continue

                fh.seek(0)
                self.param['last-modified'] = time.time()

                # Temporarily decrease sequence no, this is not the final upload
                self.param['seq_no'] -= 1
                def do_write(obj_fh):
                    fh.seek(0)
                    stream_write_bz2(fh, obj_fh)
                    return obj_fh
                log.info("Compressing and uploading metadata...")
                obj_fh = backend.perform_write(do_write, "s3ql_metadata_new",
                                               metadata=self.param, is_compressed=True)
                log.info('Wrote %.2f MiB of compressed metadata.',
                         obj_fh.get_obj_size() / 1024 ** 2)
                log.info('Cycling metadata backups...')
                cycle_metadata(backend)
                self.param['seq_no'] += 1

                fh.close()
                self.db_mtime = new_mtime

        log.debug('MetadataUploadThread: end')

    def stop(self):
        '''Signal thread to terminate'''

        log.debug('MetadataUploadThread: stop() called')
        self.quit = True
        self.event.set()

def setup_exchook():
    '''Send SIGTERM if any other thread terminates with an exception
    
    The exc_info will be saved in the list object returned
    by this function.
    '''

    this_thread = _thread.get_ident()
    old_exchook = sys.excepthook
    exc_info = []

    def exchook(type_, val, tb):
        if (_thread.get_ident() != this_thread
            and not exc_info):
            os.kill(os.getpid(), signal.SIGTERM)
            exc_info.append(type_)
            exc_info.append(val)
            exc_info.append(tb)

            old_exchook(type_, val, tb)

        # If the main thread re-raised exception, there is no need to call
        # excepthook again
        elif not (_thread.get_ident() == this_thread
                  and exc_info == [type_, val, tb]):
            old_exchook(type_, val, tb)

    sys.excepthook = exchook

    return exc_info



class CommitThread(Thread):
    '''
    Periodically upload dirty blocks.
    
    This class uses the llfuse global lock. When calling objects
    passed in the constructor, the global lock is acquired first.
    '''


    def __init__(self, block_cache):
        super().__init__()
        self.block_cache = block_cache
        self.stop_event = threading.Event()
        self.name = 'CommitThread'

    def run(self):
        log.debug('CommitThread: start')

        while not self.stop_event.is_set():
            did_sth = False
            stamp = time.time()
            for el in self.block_cache.entries.values():
                if stamp - el.last_access < 10:
                    break
                if not (el.dirty and (el.inode, el.blockno) not in self.block_cache.in_transit):
                    continue

                # Acquire global lock to access UploadManager instance
                with llfuse.lock:
                    if self.stop_event.is_set():
                        break
                    # Object may have been accessed while waiting for lock
                    if not (el.dirty and (el.inode, el.blockno) not in self.block_cache.in_transit):
                        continue
                    self.block_cache.upload(el)
                did_sth = True

                if self.stop_event.is_set():
                    break

            if not did_sth:
                self.stop_event.wait(5)

        log.debug('CommitThread: end')

    def stop(self):
        '''Signal thread to terminate'''

        log.debug('CommitThread: stop() called')
        self.stop_event.set()

if __name__ == '__main__':
    main(sys.argv[1:])
