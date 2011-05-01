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
  
.. include:: ../include/about.rst

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
read from :file:`authinfo` in :file:`~/.s3ql` or the directory
specified with :cmdopt:`--homedir`. Log files are placed in the same
directory.

.. include:: ../include/postman.rst

.. |command| replace:: :command:`mkfs.s3ql` 
