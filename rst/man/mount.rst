.. -*- mode: rst -*-

=====================
The |command| command
=====================


Synopsis
========

::

   mount.s3ql [options] <storage url> <mount point>


Description
===========

.. include:: ../include/about.rst

The |command| command mounts the S3QL file system stored in *storage
url* in the directory *mount point*. The storage url depends on the
backend that is used. The S3QL User's Guide should be consulted for a
description of the available backends.


Options
=======

The |command| command accepts the following options.

.. pipeinclude:: python ../../bin/mount.s3ql --help --log none
   :start-after: show this help message and exit


Exit Codes
==========

|command| may terminate with the following exit codes:

.. include:: ../include/exitcodes.rst

:3:
   Invalid backend option.

:10:
   Could not open log file for writing.

:11:
   No such backend.

:12:
   Authentication file has insecure permissions.

:13:
   Unable to parse proxy settings.

:14:
   Invalid credentials (Authentication failed).

:15:
   No permission to access backend (Authorization denied).

:16:
   Invalid storage URL, specified location does not exist in backend.

:17:
   Wrong file system passphrase.

:18:
   No S3QL file system found at given storage URL.

:19:
   Unable to connect to backend, can't resolve hostname.

:30:
   File system was not unmounted cleanly.

:31:
   File system appears to be mounted elsewhere.

:32:
   Unsupported file system revision (too old).

:33:
   Unsupported file system revision (too new).

:34:
   Insufficient free nodes, need to run :program:`fsck.s3ql`.

:35:
   Attempted to mount read-only, this is not supported.

:36:
   Mountpoint does not exist.

:37:
   Not enough available file descriptors.

:39:
   Unable to bind file system to mountpoint.

:45:
   Unable to access cache directory.


.. include:: ../include/postman.rst

.. |command| replace:: :program:`mount.s3ql`
