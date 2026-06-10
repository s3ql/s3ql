.. -*- mode: rst -*-

=====================
The |command| command
=====================


Synopsis
========

::

   s3qladm [options] <action> <storage url>

where :var:`action` may be either of :program:`passphrase`, :program:`recover-key`,
:program:`clear`, :program:`shrink-db`, :program:`upgrade` or :program:`restore-metadata`.

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

.. pipeinclude:: s3qladm --help
   :start-after: Options:
   :end-before: Commands:

Actions
=======

The following actions may be specified. Every action takes the *storage url* of the file
system as its argument. Individual actions may accept additional options; these are listed
below where they exist, but you can always run ``s3qladm <action> --help`` to see the full
set of options accepted by a particular action. Please consult the S3QL User's Guide for
more detailed information.


passphrase
----------

Change the encryption passphrase of the file system.


recover-key
-----------

Recover the master encryption key from an external backup.


clear
-----

Delete the file system with all the stored data.


shrink-db
---------

Recover unused space in the metadata database.


upgrade
-------

Upgrade the file system to the newest revision. This action accepts the following option:

.. pipeinclude:: s3qladm upgrade --help
   :start-after: Options:
   :end-before: --help


restore-metadata
----------------

Interactively restore backups of the file system metadata.




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

:45:
   Unable to access cache directory.


.. include:: ../include/postman.rst

.. |command| replace:: :program:`s3qladm`
