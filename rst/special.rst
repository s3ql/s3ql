.. -*- mode: rst -*-


========================
 Advanced S3QL Features
========================

.. _s3qlcp:

Snapshotting and Copy-on-Write
==============================

The command `s3qlcp` can be used to duplicate a directory tree without
physically copying the file contents. This is possible due to the data
de-duplication feature of S3QL.

The syntax of `s3qlcp` is::

  s3qlcp [options] <src> <target>

This will replicate the contents of the directory `<src>` in the
directory `<target>`. `<src>` has to be an existing directory and
`<target>` must not exist. Moreover, both directories have to be
within the same S3QL file system.

The replication will not take any additional space. Only if one of
directories is modified later on, the modified data will take
additional storage space.

`s3qlcp` can only be called by the user that mounted the file system
and (if the file system was mounted with `--allow-other` or `--allow-root`)
the root user. This limitation might be removed in the future (see `issue 155
<http://code.google.com/p/s3ql/issues/detail?id=155>`_).

Note that:

* After the replication, both source and target directory will still
  be completely ordinary directories. You can regard `<src>` as a
  snapshot of `<target>` or vice versa. However, the most common
  usage of `s3qlcp` is to regularly duplicate the same source
  directory, say `documents`, to different target directories. For a
  e.g. monthly replication, the target directories would typically be
  named something like `documents_Januray` for the replication in
  January, `documents_February` for the replication in February etc.
  In this case it is clear that the target directories should be
  regarded as snapshots of the source directory.

* Exactly the same effect could be achieved by an ordinary copy
  program like `cp -a`. However, this procedure would be orders of
  magnitude slower, because `cp` would have to read every file
  completely (so that S3QL had to fetch all the data over the network
  from the backend) before writing them into the destination folder.

* Before starting with the replication, S3QL has to flush the local
  cache. So if you just copied lots of new data into the file system
  that has not yet been uploaded, replication will take longer than
  usual.


For a full list of available options, run `s3qlcp --help`.


Snapshotting vs Hardlinking
---------------------------

Snapshot support in S3QL is inspired by the hardlinking feature that
is offered by programs like `rsync <http://www.samba.org/rsync>`_ or
`storeBackup <http://savannah.nongnu.org/projects/storebackup>`_.
These programs can create a hardlink instead of copying a file if an
identical file already exists in the backup. However, using hardlinks
has two large disadvantages:

* backups and restores always have to be made with a special program
  that takes care of the hardlinking. The backup must not be touched
  by any other programs (they may make changes that inadvertently
  affect other hardlinked files)

* special care needs to be taken to handle files which are already
  hardlinked (the restore program needs to know that the hardlink was
  not just introduced by the backup program to safe space)

S3QL snapshots do not have these problems, and they can be used with
any backup program.

.. _s3qlstat:

Getting Statistics
==================

You can get more information about a mounted S3QL file system with the
`s3qlstat` command. It has the following syntax::

  s3qlstat [options] <mountpoint>

Probably the most interesting numbers are the total size of your data,
the total size after duplication, and the final size after
de-duplication and compression.

`s3qlstat` can only be called by the user that mounted the file system
and (if the file system was mounted with `--allow-other` or `--allow-root`)
the root user. This limitation might be removed in the future (see `issue 155
<http://code.google.com/p/s3ql/issues/detail?id=155>`_).

For a full list of available options, run `s3qlstat --help`.

.. _s3qllock:


Immutable Trees
===============

The command ``s3qllock`` makes a directory tree immutable. Immutable
trees can no longer be changed in any way whatsoever. You can not add
new files or directories and you can not change or delete existing
files and directories. The only way to get rid of an immutable tree is
to use the ``s3qlrm`` command (see below).

To make the directory tree beneath the directory ``2010-04-21``
immutable, execute ::

  s3qllock 2010-04-21

Immutability is a feature designed for backups. Traditionally, backups
have been made on external tape drives. Once a backup was made, the
tape drive was removed and locked somewhere in a shelf. This has the
great advantage that the contents of the backup are now permanently
fixed. Nothing (short of physical destruction) can change or delete
files in the backup.

In contrast, when backing up into an online storage system like S3QL,
all backups are available every time the file system is mounted.
Nothing prevents a file in an old backup from being changed again
later on. In the worst case, this may make your entire backup system
worthless. Imagine that your system gets infected by a nasty virus
that simply deletes all files it can find -- if the virus is active
while the backup file system is mounted, the virus will destroy all
your old backups as well! 

Even if the possibility of a malicious virus or trojan horse is
excluded, being able to change a backup after it has been made is
generally not a good idea. A common S3QL use case is to keep the file
system mounted at all times and periodically create backups with
``rsync -a``. This allows every user to recover her files from a
backup without having to call the system administrator. However, this
also allows every user to accidentally change or delete files *in* one
of the old backups.

Making a backup immutable protects you against all these problems.
Unless you happen to run into a virus that was specifically programmed
to attack S3QL file systems, backups can be neither deleted nor
changed after they have been made immutable.

.. _s3qlrm:

Fast Recursive Removal
======================

The ``s3qlrm`` command can be used to recursively delete files and
directories on an S3QL file system. Although ``s3qlrm`` is faster than
using e.g. ``rm -r``, the main reason for its existence is that it
allows you to delete immutable trees as well. The syntax is rather
simple::

  s3qlrm <directory>

Be warned that there is no additional confirmation. The directory will
be removed entirely and immediately.

.. _s3qlctrl:

Runtime Configuration
=====================


The `s3qlctrl` can be used to control a mounted S3QL file system. Its
syntax is ::

 s3qlctrl [options] <action> <mountpoint> ...

`<mountpoint>` must be the location of a mounted S3QL file system. 
For a list of valid options, run `s3qlctrl --help`. `<action>`
may be either of:

  :flushcache:
              Flush file system cache. The command blocks until the cache has
              been flushed.
  :log:
              Change log level.
  :cachesize:
              Change file system cache size.
  :upload-meta:
              Trigger a metadata upload. 

