.. -*- mode: rst -*-

=======
Manpage
=======

Synopsis
========

::

   umount.s3ql [options] <mount point>

  
Description
===========

S3QL is a file system for online data storage. Before using S3QL, make
sure to consult the full documentation (rather than just the man pages
which only briefly document the available userspace commands).

The |command| command unmounts the S3QL file system mounted in the
directory *mount point* and blocks until all data has been uploaded to
the storage backend.

Only the user who mounted the file system with :command:`mount.s3ql`
is able to unmount it with |command|. If you are root and want to
unmount an S3QL file system mounted by an ordinary user, you have to
use the :command:`fusermount -u` or :command:`umount` command instead.
Note that these commands do not block until all data has been
uploaded, so if you use them instead of `umount.s3ql` then you should
manually wait for the `mount.s3ql` process to terminate before
shutting down the system.


Options
=======

The |command| command accepts the following options.

.. include:: ../autogen/umount-help.rst
   :start-after: show this help message and exit

.. include:: ../include/postman.rst

.. |command| replace:: :command:`umount.s3ql` 
