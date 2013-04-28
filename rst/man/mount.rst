.. -*- mode: rst -*-


=================================
The :program:`mount.s3ql` command
=================================

Synopsis
========

::

   mount.s3ql [options] <storage url> <mount point>

  
Description
===========

.. include:: ../include/about.rst

The |command| command mounts the S3QL file system stored in *storage
url* in the directory *mount point*. The storage url depends on the
backend that is used. The S3QL User's Guide should be consulted for a
description of the available backends.


Options
=======

The |command| command accepts the following options.

.. pipeinclude:: python ../../bin/mount.s3ql --help --log none
   :start-after: show this help message and exit


.. include:: ../include/postman.rst


.. |command| replace:: :command:`mount.s3ql` 

