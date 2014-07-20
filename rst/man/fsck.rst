.. -*- mode: rst -*-

=====================
The |command| command
=====================


Synopsis
========

::

   fsck.s3ql [options] <storage url>

Description
===========

.. include:: ../include/about.rst

The |command| command checks the new file system in the location
specified by *storage url* for errors and attempts to repair any
problems. The storage url depends on the backend that is used. The
S3QL User's Guide should be consulted for a description of the
available backends.

Options
=======

The |command| command accepts the following options.

.. pipeinclude:: python ../../bin/fsck.s3ql --help --log none
   :start-after: show this help message and exit


Exit Codes
==========

If |command| found any errors, the exit code will be 128 plus one of
the codes listed below. If no errors were found, the following exit
codes are used:

.. include:: ../include/exitcodes.rst

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

:32:
   Unsupported file system revision (too old).

:33:
   Unsupported file system revision (too new).

:40:
   Cannot check mounted file system.

:41:
   User input required, but running in batch mode.

:42:
   File system check aborted by user.

:43:
   Local metadata is corrupted.

:44:
   Uncorrectable errors found.

:45:
   Unable to access cache directory.

:128:
   This error code will be *added* to one of the codes above if
   errors have been found.

.. include:: ../include/postman.rst

.. |command| replace:: :program:`fsck.s3ql`
