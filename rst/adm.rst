.. -*- mode: rst -*-


Managing File Systems
=====================

The `s3qladm` command performs various operations on *unmounted* S3QL
file systems. The file system *must not be mounted* when using
`s3qladm` or things will go wrong badly.

The syntax is ::

 s3qladm [options] <action> <storage-url>

where :var:`action` may be either of :program:`passphrase`,
:program:`upgrade`, :program:`clear` or :program:`download-metadata`.

The :program:`s3qladm` accepts the following general options, no
matter what specific action is being invoked:

.. pipeinclude:: python ../bin/s3qladm --help
   :start-after: show this help message and exit


Changing the Passphrase
-----------------------

To change the passphrase of a file system, use the `passphrase`
subcommand::

  s3qladm passphrase  <storage url>

That this will not actually re-encrypt all the stored data with the
new passphrase. Rather, S3QL generates an internal "master key" when
the file system is created and uses this key to encrypt all stored
data. The user-supplied passphrase is used to encrypt/decrypt only the
master key, and can therefore be changed. This means, however, that if
a passphrase has been disclosed to someone untrusted, changing it
afterwards will **not revoke access to people who had access to the
old passphrase** (because they could have decrypted the master key and
retained a copy).


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

  s3qladm clear <storage url>

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
