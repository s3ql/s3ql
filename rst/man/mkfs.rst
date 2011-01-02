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

.. include:: ../autogen/mkfs-help.rst
   :start-after: show this help message and exit


Exit Status
===========

|command| returns exit code 0 if the operation succeeded
and -1 if some error occured.


Files
=====

Authentication data for backends and bucket encryption passphrases are
read from `authinfo` in `~/.s3ql` or the directory specified with
*--homedir*. Log files are placed in the same directory.


See Also
========

The full S3QL documentation should be available somewhere on your
system, conventional locations are `/usr/share/doc/s3ql` or
`/usr/local/doc/s3ql`.


.. |command| replace:: :command:`mkfs.s3ql` 
