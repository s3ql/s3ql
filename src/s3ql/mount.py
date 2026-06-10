'''
mount.py - this file is part of S3QL.

Copyright © 2008 Nikolaus Rath <Nikolaus@rath.org>

This work can be distributed under the terms of the GNU GPLv3.
'''

from __future__ import annotations

import _thread
import faulthandler
import logging
import os
import platform
import re
import resource
import shutil
import signal
import socket
import subprocess
import sys
import threading
import time
from collections.abc import Sequence
from contextlib import AsyncExitStack
from types import FrameType, TracebackType
from typing import TYPE_CHECKING, Annotated

import pyfuse3
import trio
import typer

from s3ql.backends.comprenc import AsyncComprencBackend

from . import fs
from .authinfo import Authinfo, CompressAlgorithm, CompressSpec
from .backends import open_backend
from .block_cache import BlockCache
from .common import is_mounted
from .daemonize import daemonize
from .database import (
    Connection,
    FsAttributes,
    download_metadata,
    expire_objects,
    read_cached_params,
    read_remote_params,
    upload_metadata,
    upload_params,
    write_params,
)
from .inode_cache import InodeCache
from .logging import QuietError, setup_logging
from .parse_args import (
    DEFAULT_AUTHFILE,
    AuthFile,
    BackendOptions,
    CacheDir,
    Compress,
    DebugFlag,
    DebugModules,
    LogDest,
    MaxConnections,
    MaxThreads,
    QuietFlag,
    StorageUrl,
    init_cachedir,
    make_app,
    pick,
    run_app,
    trio_command,
)

if TYPE_CHECKING:
    from .fs import Operations

log = logging.getLogger(__name__)

DEFAULT_MOUNT_LOG = os.path.expanduser('~/.s3ql/mount.log')

app = make_app()


def systemd_notify(message: str) -> None:
    notify_socket_path = os.environ.get('NOTIFY_SOCKET')
    if not notify_socket_path:
        raise RuntimeError("NOTIFY_SOCKET environment variable not set.")

    # Systemd expects an abstract namespace socket if the path starts with '@' On Linux, this means
    # the first byte is null.
    if notify_socket_path.startswith('@'):
        # Replace '@' with a null byte for abstract namespace socket The path length includes the
        # null byte.
        notify_socket_path = '\0' + notify_socket_path[1:]
        logging.debug("Using abstract namespace socket: %r", notify_socket_path)
    else:
        logging.debug("Using filesystem socket: %s", notify_socket_path)

    with socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM | socket.SOCK_CLOEXEC) as sock:
        sock.connect(notify_socket_path)
        sock.sendall(message.encode('utf-8'))


def install_thread_excepthook() -> None:
    """work around sys.excepthook thread bug

    See http://bugs.python.org/issue1230540.

    Call once from __main__ before creating any threads. If using
    psyco, call psyco.cannotcompile(threading.Thread.run) since this
    replaces a new-style class method.
    """

    init_old = threading.Thread.__init__

    def init(self: threading.Thread, *args, **kwargs) -> None:
        init_old(self, *args, **kwargs)
        run_old = self.run

        def run_with_except_hook(*args, **kw) -> None:
            try:
                run_old(*args, **kw)
            except SystemExit:
                raise
            except:  # noqa: E722
                exc_type, exc_val, exc_tb = sys.exc_info()
                assert exc_type is not None and exc_val is not None
                sys.excepthook(exc_type, exc_val, exc_tb)

        self.run = run_with_except_hook  # type: ignore[method-assign]

    threading.Thread.__init__ = init  # type: ignore[method-assign]


@app.command()
@trio_command
async def mount(
    storage_url: StorageUrl,
    mountpoint: Annotated[
        str, typer.Argument(metavar='<mountpoint>', help='Where to mount the file system')
    ],
    authfile: AuthFile = None,
    cachedir: CacheDir = None,
    backend_options: BackendOptions = None,
    compress: Compress = None,
    cachesize: Annotated[
        int | None,
        typer.Option(metavar='<size>', help='Cache size in KiB (default: autodetect).'),
    ] = None,
    max_cache_entries: Annotated[
        int | None,
        typer.Option(
            metavar='<num>',
            help='Maximum number of entries in cache (default: autodetect). Each cache entry '
            'requires one file descriptor, so if you increase this number you have to make sure '
            'that your process file descriptor limit (as set with `ulimit -n`) is high enough (at '
            'least the number of cache entries + 100).',
        ),
    ] = None,
    keep_cache: Annotated[
        bool, typer.Option('--keep-cache', help='Do not purge locally cached files on exit.')
    ] = False,
    allow_other: Annotated[
        bool,
        typer.Option(
            '--allow-other',
            help='Normally, only the user who called `mount.s3ql` can access the mount point. This '
            'user then also has full access to it, independent of individual file permissions. If '
            'the --allow-other option is specified, other users can access the mount point as well '
            'and individual file permissions are taken into account for all users.',
        ),
    ] = False,
    allow_root: Annotated[
        bool,
        typer.Option(
            '--allow-root',
            help='Like --allow-other, but restrict access to the mounting user and the root user.',
        ),
    ] = False,
    dirty_block_upload_delay: Annotated[
        int,
        typer.Option(
            metavar='<seconds>',
            help='Upload delay for dirty blocks in seconds (default: 10 seconds).',
        ),
    ] = 10,
    fg: Annotated[bool, typer.Option('--fg', help='Do not daemonize, stay in foreground')] = False,
    fs_name: Annotated[
        str | None,
        typer.Option(
            help='Mount name passed to fuse, the name will be shown in the first column of the '
            'system mount command output. If not specified your storage url is used.',
        ),
    ] = None,
    systemd: Annotated[
        bool,
        typer.Option(
            '--systemd',
            help='Run as systemd unit. Consider specifying --log none as well to make use of '
            'journald.',
        ),
    ] = False,
    metadata_backup_interval: Annotated[
        int,
        typer.Option(
            metavar='<seconds>',
            help='Interval between metadata backups. Should the filesystem crash while mounted, '
            'modifications made after the most recent metadata backup may be lost. During backups, '
            'the filesystem will be unresponsive. Default: 6h',
        ),
    ] = 6 * 60 * 60,
    max_connections: MaxConnections = None,
    max_threads: MaxThreads = None,
    nfs: Annotated[
        bool,
        typer.Option(
            '--nfs', help='Enable some optimizations for exporting the file system over NFS.'
        ),
    ] = False,
    log_target: LogDest = DEFAULT_MOUNT_LOG,
    debug: DebugFlag = False,
    debug_modules: DebugModules = None,
    quiet: QuietFlag = False,
) -> None:
    '''Mount an S3QL file system.'''
    # Save handler so that we can remove it when daemonizing
    stdout_log_handler = setup_logging(
        quiet=quiet,
        log=log_target,
        debug=debug,
        debug_modules=debug_modules,
        systemd=systemd,
    )

    if allow_other and allow_root:
        raise typer.BadParameter('--allow-other and --allow-root are mutually exclusive.')
    if (log_target is None or log_target.lower() == 'none') and not (fg or systemd):
        raise typer.BadParameter(
            'Please activate logging to a file or syslog, or use the --fg option.'
        )

    mountpoint = os.path.abspath(mountpoint)

    if not os.path.exists(mountpoint):
        raise QuietError('Mountpoint does not exist.', exitcode=36)

    # Check if fs is mounted on this computer. This is not foolproof but should prevent
    # common mistakes.
    if is_mounted(storage_url):
        raise QuietError('File system already mounted elsewhere on this machine.', exitcode=40)

    authinfo = Authinfo.from_file(pick(authfile, DEFAULT_AUTHFILE), storage_url)
    compress_spec = CompressSpec.parse(compress) if compress is not None else authinfo.compress
    max_conns = pick(max_connections, authinfo.max_connections)
    cache_dir = pick(cachedir, authinfo.cachedir)
    cachepath = init_cachedir(cache_dir, storage_url)

    threads = pick(max_threads, authinfo.max_threads) or determine_threads(compress_spec)
    AsyncComprencBackend.set_max_threads(threads)

    cachedir_abs = os.path.abspath(cache_dir)
    cache_entries = _determine_max_cache_entries(max_cache_entries, max_conns)
    backup_interval = None if metadata_backup_interval == 0 else metadata_backup_interval

    backend = await open_backend(
        storage_url,
        authinfo,
        backend_options=backend_options,
        max_connections=max_conns,
        compress=compress_spec,
    )
    (param, db) = await get_metadata(backend, cachepath)

    # Handle --cachesize (all values in KiB)
    rec_cachesize = cache_entries * param.data_block_size / 2 / 1024
    avail_cache = shutil.disk_usage(os.path.dirname(cachepath))[2] / 1024
    if cachesize is None:
        eff_cachesize = min(rec_cachesize, 0.8 * avail_cache)
        log.info('Setting cache size to %d MB', eff_cachesize / 1024)
    else:
        eff_cachesize = cachesize
        if eff_cachesize > avail_cache:
            log.warning(
                'Requested cache size %d MB, but only %d MB available',
                eff_cachesize / 1024,
                avail_cache / 1024,
            )

    if nfs:
        # NFS may try to look up '..', so we have to speed up this kind of query
        log.info('Creating NFS indices...')
        db.execute('CREATE INDEX IF NOT EXISTS ix_contents_inode ON contents(inode)')

    else:
        db.execute('DROP INDEX IF EXISTS ix_contents_inode')

    metadata_upload_task = MetadataUploadTask(
        param,
        backend,
        db,
        metadata_backup_interval=backup_interval,
        cachepath=cachepath,
    )

    async with AsyncExitStack() as cm:
        nursery = await cm.enter_async_context(trio.open_nursery())
        block_cache = await BlockCache.create(
            backend,
            db,
            cachepath + '-cache',
            int(eff_cachesize * 1024),
            max_entries=cache_entries,
            nursery=nursery,
        )
        cm.push_async_callback(block_cache.destroy, keep_cache)

        operations = fs.Operations(
            block_cache,
            db,
            max_obj_size=param.data_block_size,
            inode_cache=InodeCache(db, param.inode_gen),
            upload_task=metadata_upload_task,
        )
        cm.push_async_callback(operations.destroy)
        block_cache.fs = operations
        metadata_upload_task.fs = operations

        log.info('Mounting %s at %s...', storage_url, mountpoint)
        try:
            fuse_opts = get_fuse_opts(
                fs_name=fs_name,
                storage_url=storage_url,
                allow_other=allow_other,
                allow_root=allow_root,
            )
            pyfuse3.init(operations, mountpoint, fuse_opts)
        except RuntimeError as exc:
            raise QuietError(str(exc), exitcode=39)

        unmount_clean = False

        def unmount() -> None:
            log.info("Unmounting file system...")
            pyfuse3.close(unmount=unmount_clean)

        cm.callback(unmount)

        if fg or systemd:
            faulthandler.enable()
            faulthandler.register(signal.SIGUSR1)
        else:
            if stdout_log_handler:
                logging.getLogger().removeHandler(stdout_log_handler)
            crit_log_fd = os.open(
                os.path.join(cachedir_abs, 'mount.s3ql_crit.log'),
                flags=os.O_APPEND | os.O_CREAT | os.O_WRONLY,
                mode=0o644,
            )
            faulthandler.enable(crit_log_fd)
            faulthandler.register(signal.SIGUSR1, file=crit_log_fd)
            daemonize(cachedir_abs)

        param.seq_no += 1
        param.is_mounted = True
        write_params(cachepath, param)

        await upload_params(backend, param)

        nursery.start_soon(metadata_upload_task.run, name='metadata-upload-task')
        cm.callback(metadata_upload_task.stop)

        commit_task = CommitTask(block_cache, dirty_block_upload_delay)
        nursery.start_soon(commit_task.run, name='commit-task')
        cm.callback(commit_task.stop)

        exc_info = setup_exchook()

        received_signals: list[signal.Signals] = []

        def signal_handler(
            signum: int, frame: FrameType | None, _main_thread: int = _thread.get_ident()
        ) -> None:
            log.debug('Signal %d received', signum)
            if pyfuse3.trio_token is None:
                log.debug('FUSE loop not running')
                if callable(old_handler):
                    log.debug('Calling parent handler...')
                    old_handler(signum, frame)
                    return
            log.debug('Calling pyfuse3.terminate()...')
            if _thread.get_ident() == _main_thread:
                pyfuse3.terminate()
            else:
                trio.from_thread.run_sync(pyfuse3.terminate, trio_token=pyfuse3.trio_token)
            received_signals.append(signal.Signals(signum))

        signal.signal(signal.SIGTERM, signal_handler)
        old_handler = signal.signal(signal.SIGINT, signal_handler)

        if systemd:
            systemd_notify('READY=1')

        await pyfuse3.main()

        if received_signals == [signal.SIGINT]:
            log.info('SIGINT received, exiting normally.')
        elif received_signals:
            log.info('Received termination signal, exiting without data upload.')
            raise RuntimeError('Received signal(s) %s, terminating' % (received_signals,))

        # Re-raise if main loop terminated due to exception in other thread
        if exc_info:
            exc_inst = exc_info[0]
            exc_tb = exc_info[1]
            assert isinstance(exc_inst, BaseException)
            assert exc_tb is None or isinstance(exc_tb, TracebackType)
            raise exc_inst.with_traceback(exc_tb)

        log.info("FUSE main loop terminated.")

        unmount_clean = True

    # At this point, there should be no other threads left
    param.is_mounted = False
    db.close()

    # Do not update .params yet, dump_metadata() may fail if the database is
    # corrupted, in which case we want to force an fsck.
    if operations.failsafe:
        log.warning('File system errors encountered, marking for fsck.')
        param.needs_fsck = True

    param.last_modified = time.time()
    await upload_metadata(backend, db, param)
    write_params(cachepath, param)
    await upload_params(backend, param)
    await expire_objects(backend)

    await backend.close()

    log.info('All done.')


def main(args: Sequence[str] | None = None) -> None:
    '''Mount S3QL file system'''

    install_thread_excepthook()
    args = _expand_fstab_options(list(args if args is not None else sys.argv[1:]))
    run_app(app, args, prog_name='mount.s3ql')


def _determine_max_cache_entries(max_cache_entries: int | None, max_connections: int) -> int:
    '''Return the number of cache entries to use, bounded by available file descriptors.'''

    avail_fd = resource.getrlimit(resource.RLIMIT_NOFILE)[1]
    if avail_fd == resource.RLIM_INFINITY:
        avail_fd = 4096
    resource.setrlimit(resource.RLIMIT_NOFILE, (avail_fd, avail_fd))

    # Subtract some fd's for random things we forgot, and a fixed number for each upload
    # connection (because each connection is using at least one socket and at least one
    # temporary file)
    avail_fd -= 32 + 3 * max_connections

    if max_cache_entries is None:
        if avail_fd <= 64:
            raise QuietError("Not enough available file descriptors.", exitcode=37)
        log.info('Autodetected %d file descriptors available for cache entries', avail_fd)
        return avail_fd

    if max_cache_entries > avail_fd:
        log.warning(
            "Up to %d cache entries requested, but detected only %d available file descriptors.",
            max_cache_entries,
            avail_fd,
        )
        return avail_fd

    return max_cache_entries


def get_system_memory() -> int:
    '''Attempt to determine total system memory

    If amount cannot be determined, emits warning and
    returns -1.
    '''

    # MacOS X doesn't support sysconf('SC_PHYS_PAGES')
    if platform.system() == 'Darwin':
        try:
            out = subprocess.check_output(['sysctl', 'hw.memsize'], universal_newlines=True)
        except subprocess.CalledProcessError as exc:
            log.warning('Cannot determine system memory, sysctl failed with %s', exc.output)
            return -1

        # output of sysctl is 'hw.memsize: #'. Strip the prefix.
        hit = re.match(r'^hw.memsize: ([0-9]+)$', out)
        if not hit:
            log.warning('Cannot determine system memory, unable to parse sysctl output.')
            return -1

        return int(hit.group(1))

    else:
        try:
            return os.sysconf('SC_PHYS_PAGES') * os.sysconf('SC_PAGESIZE')
        except ValueError:
            log.warning('Unable to determine system memory (sysconf failed).')
            return -1


# Memory required for LZMA compression in MB (from xz(1))
LZMA_MEMORY: dict[int, int] = {
    0: 3,
    1: 9,
    2: 17,
    3: 32,
    4: 48,
    5: 94,
    6: 94,
    7: 186,
    8: 370,
    9: 674,
}


def determine_threads(compress: CompressSpec | None) -> int:
    '''Return optimum number of upload threads.

    *compress* is the `CompressSpec` that compression will use, or None if the caller does
    not perform bulk compressed uploads (e.g. mkfs and adm). When None, the result is sized
    purely from the CPU count.
    '''

    try:
        cores = os.sysconf('SC_NPROCESSORS_ONLN')
    except ValueError:
        log.warning('Unable to determine number of CPU cores (sysconf failed).')
        cores = -1

    memory = get_system_memory()

    if compress is not None and compress.algorithm is CompressAlgorithm.LZMA:
        # Keep this in sync with compression level in backends/common.py
        # Memory usage according to man xz(1)
        mem_per_thread = LZMA_MEMORY[compress.level] * 1024**2
    else:
        # Only check LZMA memory usage
        mem_per_thread = 0

    if cores == -1:
        log.warning("Can't determine number of cores, using 2 upload threads.")
        return 2
    elif memory == -1 and mem_per_thread != 0:
        log.warning("Can't determine available memory, using 2 upload threads.")
        return 2
    elif mem_per_thread > 0 and 2 * cores * mem_per_thread > (memory / 2):
        threads = min(int((memory / 2) // mem_per_thread), 10)
        if threads > 0:
            log.info('Using %d upload threads (memory limited).', threads)
        else:
            log.warning(
                'Compression will require %d MiB memory (%d%% of total system memory)',
                mem_per_thread / 1024**2,
                mem_per_thread * 100 / memory,
            )
            threads = 1
        return threads
    else:
        threads = min(2 * cores, 10)
        log.info("Using %d upload threads.", threads)
        return threads


async def get_metadata(
    backend: AsyncComprencBackend, cachepath: str
) -> tuple[FsAttributes, Connection]:
    '''Retrieve metadata, downloading blocks in parallel via *backend*.'''

    db = None
    param = await read_remote_params(backend)
    local_param = read_cached_params(cachepath, min_seq=param.seq_no)
    if local_param is not None:
        if local_param.seq_no > param.seq_no:
            raise QuietError("File system not unmounted cleanly, run fsck!", exitcode=30)
        log.info('Using cached metadata.')
        assert local_param == param
        db = Connection(cachepath + '.db', param.metadata_block_size)

    if param.is_mounted:
        raise QuietError(
            'Backend reports that fs is still mounted elsewhere, aborting.', exitcode=31
        )

    if param.needs_fsck:
        raise QuietError("File system damaged or not unmounted cleanly, run fsck!", exitcode=30)
    if time.time() - param.last_fsck > 60 * 60 * 24 * 31:
        log.warning(
            'Last file system check was more than 1 month ago, running fsck.s3ql is recommended.'
        )

    # Download metadata
    if not db:
        log.info('Downloading metadata...')
        db = await download_metadata(backend, cachepath + '.db', param)

        # Drop cache
        if os.path.exists(cachepath + '-cache'):
            shutil.rmtree(cachepath + '-cache')

    assert db is not None
    return (param, db)


def get_fuse_opts(
    fs_name: str | None, storage_url: str, allow_other: bool, allow_root: bool
) -> set[str]:
    '''Return fuse options for the given mount configuration'''

    fsname = fs_name
    if not fsname:
        fsname = storage_url
    fuse_opts: set[str] = {'fsname=%s' % fsname, 'subtype=s3ql'}

    if allow_other:
        fuse_opts.add('allow_other')
    if allow_root:
        fuse_opts.add('allow_root')
    if allow_other or allow_root:
        fuse_opts.add('default_permissions')

    return fuse_opts


def _expand_fstab_options(args: list[str]) -> list[str]:
    '''Translate fstab-style `-o key=val,flag` options into long-form arguments.'''

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
                if opt in (
                    'rw',
                    'defaults',
                    'auto',
                    'noauto',
                    'user',
                    'nouser',
                    'dev',
                    'nodev',
                    'suid',
                    'nosuid',
                    'atime',
                    'diratime',
                    'exec',
                    'noexec',
                    'group',
                    'mand',
                    'nomand',
                    '_netdev',
                    'nofail',
                    'norelatime',
                    'strictatime',
                    'owner',
                    'users',
                    'nobootwait',
                ):
                    continue
                elif opt == 'ro':
                    raise QuietError('Read-only mounting not supported.', exitcode=35)
                args.insert(pos, '--' + opt)

    return args


class MetadataUploadTask:
    '''
    Periodically upload metadata. Upload is done every `interval`
    seconds, and whenever `event` is set. To terminate thread,
    set `quit` attribute as well as `event` event.
    '''

    backend: AsyncComprencBackend
    db: Connection
    event: trio.Event
    quit: bool
    params: FsAttributes
    metadata_backup_interval: int | None
    cachepath: str
    fs: Operations | None

    def __init__(
        self,
        params: FsAttributes,
        backend: AsyncComprencBackend,
        db: Connection,
        *,
        metadata_backup_interval: int | None,
        cachepath: str,
    ) -> None:
        super().__init__()
        self.backend = backend
        self.db = db
        self.event = trio.Event()
        self.quit = False
        self.params = params
        self.metadata_backup_interval = metadata_backup_interval
        self.cachepath = cachepath
        self.fs = None

    async def run(self) -> None:
        log.debug('started')

        while not self.quit:
            if self.metadata_backup_interval:
                with trio.move_on_after(self.metadata_backup_interval):
                    await self.event.wait()
            else:
                await self.event.wait()

            if self.quit:
                break

            self.event = trio.Event()  # reset

            # Upload twice without blocking writes, to reduce the amount of data left for
            # the final upload (which is needed for a consistent snapshot).
            self.db.sync_checkpoint()
            for _ in range(2):
                await upload_metadata(
                    self.backend,
                    self.db,
                    self.params,
                    update_params=False,
                    incremental=True,
                )

            self.params.last_modified = time.time()

            # Now upload with writes inhibited to get a consistent snapshot. inhibit_writes() blocks
            # WAL checkpointing (writes go to WAL only), ensuring the main database file remains
            # consistent while we read it.
            async with self.db.inhibit_writes():
                await upload_metadata(
                    self.backend,
                    self.db,
                    self.params,
                    update_params=True,
                    incremental=True,
                )
                write_params(self.cachepath, self.params)
                await upload_params(self.backend, self.params)

                # Write a new params file immediately, so that we're in the same state as right
                # after mounting and there is no window where we could have metadata_* objects
                # with a sequence number for which there is no corresponding s3ql_params_*
                # object.
                self.params.seq_no += 1
                write_params(self.cachepath, self.params)
                await upload_params(self.backend, self.params)

                await expire_objects(self.backend)

        log.debug('finished')

    def stop(self) -> None:
        '''Signal thread to terminate'''

        log.debug('started')
        self.quit = True
        self.event.set()


def setup_exchook() -> list[BaseException | TracebackType | None]:
    '''Terminate FUSE main loop if any thread terminates with an exception

    The exc_info will be saved in the list object returned by this function.
    '''

    main_thread = _thread.get_ident()
    old_exchook = sys.excepthook
    exc_info: list[BaseException | TracebackType | None] = []

    def exchook(
        exc_type: type[BaseException], exc_inst: BaseException, tb: TracebackType | None
    ) -> None:
        reporting_thread = _thread.get_ident()
        if reporting_thread != main_thread:
            if exc_info:
                log.warning("Unhandled top-level exception during shutdown (will not be re-raised)")
            else:
                log.error("Unhandled exception in thread, terminating", exc_info=True)
                exc_info.append(exc_inst)
                exc_info.append(tb)
                trio.from_thread.run_sync(pyfuse3.terminate, trio_token=pyfuse3.trio_token)

            old_exchook(exc_type, exc_inst, tb)

        # If the main thread re-raised exception, there is no need to call
        # excepthook again
        elif exc_info and exc_info[0] is exc_inst:
            log.debug('Suppressing exception hook for re-raised %s', exc_inst)
        else:
            old_exchook(exc_type, exc_inst, tb)

    sys.excepthook = exchook

    return exc_info


class CommitTask:
    '''
    Periodically upload dirty blocks.
    '''

    block_cache: BlockCache
    stop_event: trio.Event
    dirty_block_upload_delay: int

    def __init__(self, block_cache: BlockCache, dirty_block_upload_delay: int) -> None:
        super().__init__()
        self.block_cache = block_cache
        self.stop_event = trio.Event()
        self.dirty_block_upload_delay = dirty_block_upload_delay

    async def run(self) -> None:
        log.debug('started')

        while not self.stop_event.is_set():
            did_sth = False
            stamp = time.time()
            for el in self.block_cache.cache.mutable_values():
                if (
                    self.stop_event.is_set()
                    or stamp - el.last_write < self.dirty_block_upload_delay
                ):
                    break
                if el.dirty and not el.in_transit:
                    await self.block_cache.upload_if_dirty(el)
                    did_sth = True

            if not did_sth:
                with trio.move_on_after(5):
                    await self.stop_event.wait()

        log.debug('finished')

    def stop(self) -> None:
        '''Signal thread to terminate'''

        log.debug('started')
        self.stop_event.set()


if __name__ == '__main__':
    main(sys.argv[1:])
