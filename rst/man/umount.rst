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

.. include:: ../include/about.rst

The |command| command unmounts the S3QL file system mounted in the
directory *mount point* and blocks until all data has been uploaded to
the storage backend.

Only the user who mounted the file system with :program:`mount.s3ql`
is able to unmount it with |command|. If you are root and want to
unmount an S3QL file system mounted by an ordinary user, you have to
use the :program:`fusermount -u` or :command:`umount` command instead.
Note that these commands do not block until all data has been
uploaded, so if you use them instead of :program:`umount.s3ql` then
you should manually wait for the :program:`mount.s3ql` process to
terminate before shutting down the system.


Options
=======

The |command| command accepts the following options.

.. pipeinclude:: ../../bin/umount.s3ql --help
   :start-after: show this help message and exit

.. include:: ../include/postman.rst

.. |command| replace:: :command:`umount.s3ql` 
