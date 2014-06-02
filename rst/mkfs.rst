.. -*- mode: rst -*-

====================
File System Creation
====================

A S3QL file system is created with the `mkfs.s3ql` command. It has the
following syntax::

  mkfs.s3ql [options] <storage url>

This command accepts the following options:

.. pipeinclude:: python ../bin/mkfs.s3ql --help
   :start-after: show this help message and exit

Unless you have specified the `--plain` option, `mkfs.s3ql` will ask
you to enter an encryption password. This password will *not* be read
from an authentication file specified with the :cmdopt:`--authfile`
option to prevent accidental creation of an encrypted file system.
