.. -*- mode: rst -*-

=====================
The |command| command
=====================


Synopsis
========

::

   s3ql_verify [options] <storage url>

Description
===========

.. include:: ../include/about.rst

The |command| command verifies all data in the file system.  In
contrast to :program:`fsck.s3ql`, |command| does not trust the object
listing returned by the backend, but actually attempts to retrieve
every object. It therefore takes a lot longer.

The format of :var:`<storage url>` depends on the backend that is
used. The S3QL User's Guide should be consulted for a description of
the available backends.

Options
=======

The |command| command accepts the following options.

.. pipeinclude:: python ../../bin/s3ql_verify --help --log none
   :start-after: show this help message and exit


Exit Codes
==========

|command| may terminate with the following exit codes:

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

:45:
   Unable to access cache directory.

:46:
   The file system data was verified, and some objects were found
   to be missing or corrupted.

.. include:: ../include/postman.rst

.. |command| replace:: :program:`s3ql_verify`
