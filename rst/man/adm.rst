.. -*- mode: rst -*-

==============================
The :program:`s3qladm` command
==============================

Synopsis
========

::

   s3qladm [options] <action> <storage url>

where :var:`action` may be either of :program:`passphrase`,
:program:`upgrade`, :program:`delete` or :program:`download-metadata`.
  
Description
===========

.. include:: ../include/about.rst

The |command| command performs various operations on S3QL buckets.
The file system contained in the bucket *must not be mounted* when
using |command| or things will go wrong badly.

.. include:: ../include/backends.rst


Options
=======

The |command| command accepts the following options.

.. pipeinclude:: ../../bin/s3qladm --help
   :start-after: show this help message and exit

Actions
=======

The following actions may be specified:

passphrase
  Changes the encryption passphrase of the bucket.

upgrade
  Upgrade the file system contained in the bucket to the newest revision.

delete
  Delete the bucket and all its contents.

download-metadata
  Interactively download backups of the file system metadata.


Files
=====

Authentication data for backends and bucket encryption passphrases are
read from :file:`authinfo` in :file:`~/.s3ql` or the directory
specified with :cmdopt:`--homedir`. Log files are placed in the same
directory.
  

.. include:: ../include/postman.rst

.. |command| replace:: :program:`s3qladm`
