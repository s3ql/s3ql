.. -*- mode: rst -*-

=====================
The |command| command
=====================

Synopsis
========

::

   s3qlstat [options] <mountpoint>

Description
===========

.. include:: ../include/about.rst

The |command| command prints statistics about the S3QL file system mounted
at :var:`mountpoint`.

|command| can only be called by the user that mounted the file system
and (if the file system was mounted with :cmdopt:`--allow-other` or
:cmdopt:`--allow-root`) the root user.


Options
=======

The |command| command accepts the following options:

.. pipeinclude:: s3qlstat --help
   :start-after: show this help message and exit


Exit Codes
==========

|command| may terminate with the following exit codes:

.. include:: ../include/exitcodes.rst


.. include:: ../include/postman.rst

.. |command| replace:: :program:`s3qlstat`
