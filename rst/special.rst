.. -*- mode: rst -*-


========================
 Advanced S3QL Features
========================

.. _s3qlcp:

Snapshotting and Copy-on-Write
==============================

The command `s3qlcp` can be used to duplicate a directory tree without
physically copying the file contents. This is made possible by the
data de-duplication feature of S3QL.

The syntax of `s3qlcp` is::

  s3qlcp [options] <src> <target>

This will replicate the contents of the directory `<src>` in the
directory `<target>`. `<src>` has to be an existing directory and
`<target>` must not exist. Moreover, both directories have to be
within the same S3QL file system.

.. include:: man/cp.rst
   :start-after: begin_main_content
   :end-before: end_main_content

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

The command :program:`s3qllock` can be used to make a directory tree
immutable. Immutable trees can no longer be changed in any way
whatsoever. You can not add new files or directories and you can not
change or delete existing files and directories. The only way to get
rid of an immutable tree is to use the :program:`s3qlrm` command (see
below).

For example, to make the directory tree beneath the directory
``2010-04-21`` immutable, execute ::

  s3qllock 2010-04-21

.. include:: man/lock.rst
   :start-after: begin_main_content
   :end-before: end_main_content


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


