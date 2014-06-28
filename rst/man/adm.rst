.. -*- mode: rst -*-

=====================
The |command| command
=====================


Synopsis
========

::

   s3qladm [options] <action> <storage url>

where :var:`action` may be either of :program:`passphrase`,
:program:`upgrade`, :program:`delete` or :program:`download-metadata`.

Description
===========

.. include:: ../include/about.rst

The |command| command performs various operations on *unmounted* S3QL
file systems. The file system *must not be mounted* when using
|command| or things will go wrong badly.

The storage url depends on the backend that is used. The S3QL User's
Guide should be consulted for a description of the available backends.

Options
=======

The |command| command accepts the following options.

.. pipeinclude:: python ../../bin/s3qladm --help
   :start-after: show this help message and exit

Actions
=======

The following actions may be specified:

passphrase
  Changes the encryption passphrase of the file system.

upgrade
  Upgrade the file system to the newest revision.

delete
  Delete the file system with all the stored data.

download-metadata
  Interactively download backups of the file system metadata.


Exit Codes
==========

|command| may terminate with the following exit codes:

.. include:: ../include/exitcodes.rst

:10:
   Could not open log file for writing.


.. include:: ../include/postman.rst

.. |command| replace:: :program:`s3qladm`
