.. -*- mode: rst -*-

====================
File System Creation
====================

A S3QL file system is created with the `mkfs.s3ql` command. It has the
following syntax::

  mkfs.s3ql [options] <storage url>

If you want to overwrite an existing bucket, you have to delete the
bucket with `s3qladm --delete` first.

This command accepts the following options:

.. pipeinclude:: ../bin/mkfs.s3ql --help
   :start-after: show this help message and exit

Unless you have specified the `--plain` option, `mkfs.s3ql` will ask you
to enter an encryption password. If you do not want to enter this
password every time that you mount the file system, you can store it
in the `~/.s3ql/authinfo` file, see :ref:`bucket_pw`.
