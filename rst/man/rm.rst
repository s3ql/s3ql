.. -*- mode: rst -*-


=================================
The :program:`s3qlrm` command
=================================

Synopsis
========

::

   s3qlrm [options] <directory>
  
Description
===========

.. include:: ../include/about.rst

The |command| command recursively deletes files and directories on an
S3QL file system. Although |command| is faster than using e.g.
:command:`rm -r``, the main reason for its existence is that it allows
you to delete immutable trees (which can be created with
:program:`s3qllock`) as well.

Be warned that there is no additional confirmation. The directory will
be removed entirely and immediately.
  

Options
=======

The |command| command accepts the following options:

.. pipeinclude:: ../../bin/s3qlrm --help
   :start-after: show this help message and exit

.. include:: ../include/postman.rst

.. |command| replace:: :command:`s3qlrm` 

