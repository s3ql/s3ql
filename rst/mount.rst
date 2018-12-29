.. -*- mode: rst -*-

==========
 Mounting
==========


A S3QL file system is mounted with the :program:`mount.s3ql`
command. It has the following syntax::

  mount.s3ql [options] <storage url> <mountpoint>

.. NOTE::

   S3QL is not a network file system like `NFS
   <http://en.wikipedia.org/wiki/Network_File_System_%28protocol%29>`_
   or `CIFS <http://en.wikipedia.org/wiki/CIFS>`_. It can only be
   mounted on one computer at a time.

This command accepts the following options:

.. pipeinclude:: python ../bin/mount.s3ql --help
   :start-after: show this help message and exit


Permission Checking
===================

If the file system is mounted with neither the :cmdopt:`allow-root`
nor :cmdopt:`allow-other` option, the mounting user has full
permissions on the S3QL file system (he is effectively root). If one
(or both) of the options is used, standard unix permission checks
apply, i.e. only the real root user has full access and all other
users (including the mounting user) are subject to permission checks.


Compression Algorithms
======================

S3QL supports three compression algorithms, LZMA, Bzip2 and zlib (with
LZMA being the default). The compression algorithm can be specified
freely whenever the file system is mounted, since it affects only the
compression of new data blocks.

Roughly speaking, LZMA is slower but achieves better compression
ratios than Bzip2, while Bzip2 in turn is slower but achieves better
compression ratios than zlib.

For maximum file system performance, the best algorithm therefore
depends on your network connection speed: the compression algorithm
should be fast enough to saturate your network connection.

To find the optimal algorithm and number of parallel compression
threads for your system, S3QL ships with a program called
`benchmark.py` in the `contrib` directory. You should run this program
on a file that has a size that is roughly equal to the block size of
your file system and has similar contents. It will then determine the
compression speeds for the different algorithms and the upload speeds
for the specified backend and recommend the best algorithm that is
fast enough to saturate your network connection.

Obviously you should make sure that there is little other system load
when you run `benchmark.py` (i.e., don't compile software or encode
videos at the same time).


Notes about Caching
===================

S3QL maintains a local cache of the file system data to speed up
access. The cache is block based, so it is possible that only parts of
a file are in the cache.

Maximum Number of Cache Entries
-------------------------------

The maximum size of the cache can be configured with the
:cmdopt:`--cachesize` option. In addition to that, the maximum number
of objects in the cache is limited by the
:cmdopt:`--max-cache-entries` option, so it is possible that the cache
does not grow up to the maximum cache size because the maximum number
of cache elements has been reached. The reason for this limit is that
each cache entry requires one open file descriptor, and Linux
distributions usually limit the total number of file descriptors per
process to about a thousand.

If you specify a value for :cmdopt:`--max-cache-entries`, you should
therefore make sure to also configure your system to increase the
maximum number of open file handles. This can be done temporarily with
the :program:`ulimit -n` command. The method to permanently change this limit
system-wide depends on your distribution.



Cache Flushing and Expiration
-----------------------------

S3QL flushes changed blocks in the cache to the backend whenever a block
has not been accessed for at least 10 seconds. Note that when a block is
flushed, it still remains in the cache.

Cache expiration (i.e., removal of blocks from the cache) is only done
when the maximum cache size is reached. S3QL always expires the least
recently used blocks first.

NFS Support
===========

S3QL filesystems can be exported over NFS. The :cmdopt:`--nfs` option
is recommended to improve performance when NFS is used, but no harm
will occur when it is not specified.

NFS supports persistence of client mounts across server restarts. This
means that if a client has mounted an S3QL file system over NFS, the
server may unmount and remount the S3QL filesystem (or even reboot)
without the client being affected beyond temporarily becoming
unavailable. This poses several challenges, but is supported by S3QL
as long as no `fsck.s3ql` operation is run:

.. WARNING::

   If `fsck.s3ql` modifies a file system in any way, all NFS
   clients must unmount and re-mount the NFS share before the
   S3QL file system is re-mounted on the server.


Failure Modes
=============

Once an S3QL file system has been mounted, there is a multitude of
problems that can occur when communicating with the remote
server. Generally, :program:`mount.s3ql` always tries to keep the file
system as accessible as possible under the circumstances. That means
that if network connectivity is lost, data can still be written as
long as there is space in the local cache. Attempts to read data not
already present in the cache, however, will block until connection is
re-established. If any sort of data corruption is detected, the file
system will switch to read-only mode. Attempting to read files that
are affected by the corruption will return an input/output error
(*errno* set to ``EIO``).

In case of other unexpected or fatal problems, :program:`mount.s3ql`
terminates, but does not unmount the file system. Any attempt to
access the mountpoint will result in a "Transport endpoint not
connected" error (*errno* set to ``ESHUTDOWN``). This ensures that a
mountpoint whose :program:`mount.s3ql` process has terminated can not
be confused with a mountpoint containing an empty file system (which
would be fatal if e.g. the mountpoint is automatically mirrored). When
this has happened, the mountpoint can be cleared by using the
:program:`fusermount` command (provided by FUSE) with the ``-u``
parameter.

:program:`mount.s3ql` will automatically try to re-establish the
connection to the server if network connectivity is lost, and retry
sending a request when the connection is established but the remote
server signals a temporary problem. These attempts will be made at
increasing intervals for a period up to 24 hours, with retry intervals
starting at 20 ms and increasing up to 5 minutes. After 24 hours,
:program:`mount.s3ql` will give up and terminate, leaving the
mountpoint inaccessible as described above.

Generally, :program:`mount.s3ql` will also emit log messages for any
unusual conditions that it encounters. The destination for these
messages can be set with the :cmdopt:`--log` parameter. It is highly
recommended to periodically check these logs, for example with a tool
like logcheck_. Many potential issues that :program:`mount.s3ql` may
encounter do not justify restricting access to the file system, but
should nevertheless be investigated if they occur. Checking the log
messages is the only way to find out about them.

.. _logcheck: http://sourceforge.net/projects/logcheck/


Automatic Mounting
==================

If you want to mount and umount an S3QL file system automatically at
system startup and shutdown, you should do so with a dedicated S3QL
init job (instead of using :file:`/etc/fstab`. When using systemd,
:program:`mount.s3ql` can be started with :cmdopt:`--systemd` to run
as a systemd service of type ``notify``.

.. NOTE::

   In principle, it is also possible to automatically mount an S3QL
   file system with an appropriate entry in `/etc/fstab`. However,
   this is not recommended for several reasons:

   * file systems mounted in :file:`/etc/fstab` will be unmounted with the
     :program:`umount` command, so your system will not wait until all data has
     been uploaded but shutdown (or restart) immediately (this is a
     FUSE limitation, cf https://github.com/libfuse/libfuse/issues/1).

   * There is no way to tell the system that mounting S3QL requires a
     Python interpreter to be available, so it may attempt to run
     :program:`mount.s3ql` before it has mounted the volume containing
     the Python interpreter.

   * There is no standard way to tell the system that internet
     connection has to be up before the S3QL file system can be
     mounted.
