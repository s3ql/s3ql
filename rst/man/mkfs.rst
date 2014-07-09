.. -*- mode: rst -*-

=====================
The |command| command
=====================


Synopsis
========

::

   mkfs.s3ql [options] <storage url>

Description
===========

.. include:: ../include/about.rst

The |command| command creates a new file system in the location
specified by *storage url*. The storage url depends on the backend
that is used. The S3QL User's Guide should be consulted for a
description of the available backends.

Unless you have specified the `--plain` option, `mkfs.s3ql` will ask
you to enter an encryption password. This password will *not* be read
from an authentication file specified with the :cmdopt:`--authfile`
option to prevent accidental creation of an encrypted file system.


Options
=======

The |command| command accepts the following options.

.. pipeinclude:: python ../../bin/mkfs.s3ql --help
   :start-after: show this help message and exit


Exit Codes
==========

|command| may terminate with the following exit codes:

.. include:: ../include/exitcodes.rst

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

:45:
   Unable to access cache directory.


.. include:: ../include/postman.rst

.. |command| replace:: :program:`mkfs.s3ql`
