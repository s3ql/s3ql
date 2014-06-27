.. -*- mode: rst -*-

=====================
The |command| command
=====================


Synopsis
========

::

   fsck.s3ql [options] <storage url>

Description
===========

.. include:: ../include/about.rst

The |command| command checks the new file system in the location
specified by *storage url* for errors and attempts to repair any
problems. The storage url depends on the backend that is used. The
S3QL User's Guide should be consulted for a description of the
available backends.

Options
=======

The |command| command accepts the following options.

.. pipeinclude:: python ../../bin/fsck.s3ql --help --log none
   :start-after: show this help message and exit


Exit Codes
==========

|command| may terminate with the following exit codes:

.. include:: ../include/exitcodes.rst


.. include:: ../include/postman.rst

.. |command| replace:: :program:`fsck.s3ql`
