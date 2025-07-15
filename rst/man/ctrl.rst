.. -*- mode: rst -*-

=====================
The |command| command
=====================


Synopsis
========

::

   s3qlctrl [options] <action> <mountpoint> ...

where :var:`action` may be either of :program:`flushcache`, :program:`dropcache`,
:program:`log`, :program:`cachesize` or :program:`backup-metadata`.

Description
===========

.. include:: ../include/about.rst

.. begin_main_content

The :program:`s3qlctrl` command performs various actions on the S3QL file system mounted
in :var:`mountpoint`.

:command:`s3qlctrl` can only be called by the user that mounted the file system and (if
the file system was mounted with :cmdopt:`--allow-other` or :cmdopt:`--allow-root`) the
root user.

The following actions may be specified:

  :flushcache:
              Write all modified blocks to the backend. The command blocks until the
              cache is clean.

  :dropcache:
              Flush the filesystem cache and then drop all contents (i.e., make the cache
              empty).

  :log:
              Change the amount of information that is logged. The complete syntax is::

                s3qlctrl [options] log <mountpoint> <level> [<module> ...]

              here :var:`level` is the desired new log level and may be either of *debug*,
              *info* or *warn*. One or more :var:`module` may only be specified with the
              *debug* level and allow to restrict the debug output to just the listed
              modules.

  :cachesize:
              Changes the cache size of the file
              system. This action requires an additional argument that specifies the new
              cache size in KiB, so the complete command line is::

                s3qlctrl [options] cachesize <mountpoint> <new-cache-size>

  :backup-metadata:
              Trigger a metadata backup.


.. end_main_content


Options
=======

The |command| command also accepts the following options, no matter
what specific action is being invoked:

.. pipeinclude:: s3qlctrl --help
   :start-after: show this help message and exit


Exit Codes
==========

|command| may terminate with the following exit codes:

.. include:: ../include/exitcodes.rst

.. include:: ../include/postman.rst


.. |command| replace:: :program:`s3qlctrl`
