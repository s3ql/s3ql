.. -*- mode: rst -*-

================================
The :program:`fsck.s3ql` command
================================

Synopsis
========

::

   fsck.s3ql [options] <storage url>

Description
===========
  
.. include:: ../include/about.rst

The |command| command checks the new file system in the location
specified by *storage url* for errors and attempts to repair any
problems.

.. include:: ../include/backends.rst
 

Options
=======

The |command| command accepts the following options.

.. pipeinclude:: ../../bin/fsck.s3ql --help
   :start-after: show this help message and exit

Files
=====

Authentication data for backends and bucket encryption passphrases are
read from :file:`authinfo` in :file:`~/.s3ql` or the directory
specified with :cmdopt:`--homedir`. Log files are placed in the same
directory.

.. include:: ../include/postman.rst

.. |command| replace:: :command:`mkfs.s3ql` 
