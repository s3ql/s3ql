.. -*- mode: rst -*-

=======
Manpage
=======

Synopsis
========

::

   mkfs.s3ql [options] <storage url>

  
Description
===========

S3QL is a file system for online data storage. Before using S3QL, make
sure to consult the full documentation (rather than just the man pages
which only briefly document the available userspace commands).

The |command| command creates a new file system in the location
specified by *storage url*.

.. include:: ../include/backends.rst
 

Options
=======

The |command| command accepts the following options.

.. pipeinclude:: ../../bin/mkfs.s3ql --help
   :start-after: show this help message and exit

Files
=====

Authentication data for backends and bucket encryption passphrases are
read from `authinfo` in `~/.s3ql` or the directory specified with
*--homedir*. Log files are placed in the same directory.

.. include:: ../include/postman.rst

.. |command| replace:: :command:`mkfs.s3ql` 
