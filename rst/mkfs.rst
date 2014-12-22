.. -*- mode: rst -*-

====================
File System Creation
====================

A S3QL file system is created with the :program:`mkfs.s3ql` command. It has the
following syntax::

  mkfs.s3ql [options] <storage url>

This command accepts the following options:

.. pipeinclude:: python ../bin/mkfs.s3ql --help
   :start-after: show this help message and exit

Unless you have specified the :cmdopt:`--plain` option,
:program:`mkfs.s3ql` will ask you to enter an encryption
password. This password will *not* be read from an authentication file
specified with the :cmdopt:`--authfile` option to prevent accidental
creation of an encrypted file system.

Note that:

* All data that is stored under the given storage url is assumed to
  managed exclusively by S3QL. Trying to manually save additional
  objects (or remove or manipulate existing objects) will lead to file
  system corruption, and :program:`fsck.s3ql` may delete objects that
  do not belong to the file system.

* With most storage backends, slashes in the storage url prefix do not
  have special meaning. For example, the storage urls
  ``s3://mybucket/myprefix/`` and ``s3://mybucket/myprefix`` are
  distinct. In the first case, the prefix is ``myprefix/``, while in
  the second it is ``myprefix``.

* S3QL file systems can not be "stacked", i.e. you cannot have one
  file system stored at ``s3://bucketname/outerprefix`` and a second
  one at ``s3://bucketname/outerprefix/innerprefix``.
