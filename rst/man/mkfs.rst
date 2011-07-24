.. -*- mode: rst -*-

================================
The :program:`mkfs.s3ql` command
================================

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


.. include:: ../include/postman.rst

.. |command| replace:: :command:`mkfs.s3ql` 
