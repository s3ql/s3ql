.. -*- mode: rst -*-


Managing Buckets
=====================


The `s3qladm` command performs various operations on S3QL buckets.
The file system contained in the bucket *must not be mounted* when
using `s3qladm` or things will go wrong badly.

The syntax is ::

 s3qladm [options] <action> <storage-url>

For a list of valid options, run `s3qladm --help`. The available
actions are explained in the following subsections.


Changing the Passphrase
-----------------------

To change the passphrase a bucket, use the `s3qladm` command::

  s3qladm passphrase  <storage url>

The passphrase can only be changed when the bucket is not mounted.

Upgrading the file system
-------------------------

If you have installed a new version of S3QL, it may sometimes be
necessary to upgrade the file system metadata as well. Note that in
this case the file system can no longer be accessed with older
versions of S3QL after the upgrade.

During the upgrade you have to make sure that the command is not
interrupted, and that no one else tries to mount, check or upgrade the
file system at the same time.

To upgrade a file system from the previous to the current revision,
execute ::

  s3qladm upgrade <storage url>


Deleting a file system
----------------------

A file system can be deleted with::

  s3qladm delete <storage url>

This physically deletes all the data and file system structures.


Restoring Metadata Backups
--------------------------

If the most-recent copy of the file system metadata has been damaged
irreparably, it is possible to restore one of the automatically
created backup copies.

The command ::

  s3qladm download-metadata <storage url>

will give you a list of the available metadata backups and allow you
to download them. This will create two new files in the current
directory, ending in ``.db`` and ``.params``. To actually use the
downloaded backup, you need to move these files into the ``~/.s3ql/``
directory and run ``fsck.s3ql``.

.. WARNING::

   You should probably not use this functionality without having asked
   for help on the mailing list first (see :ref:`resources`).
