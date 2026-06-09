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

Every subcommand takes the *mountpoint* of the file system as its first argument, plus
any additional arguments described below. The following subcommands are available:


flushcache
----------

Write all modified blocks to the backend. The command blocks until the cache is clean::

  s3qlctrl [options] flushcache <mountpoint>


dropcache
---------

Flush the file system cache and then drop all contents (i.e., make the cache empty)::

  s3qlctrl [options] dropcache <mountpoint>


backup-metadata
---------------

Trigger an immediate metadata backup::

  s3qlctrl [options] backup-metadata <mountpoint>


cachesize
---------

Change the cache size of the file system::

  s3qlctrl [options] cachesize <mountpoint> <size>

:var:`size` is the new cache size in KiB.


log
---

Change the amount of information that is logged by the mounted file system::

  s3qlctrl [options] log <mountpoint> <level> [<module> ...]

:var:`level` is the desired new log level and may be either of *debug*, *info* or *warn*.
One or more :var:`module` may only be specified together with the *debug* level and
restrict the debug output to just the listed modules. Specify *all* to enable debugging
for every module.


.. end_main_content


Options
=======

The |command| command also accepts the following options, no matter
what specific action is being invoked:

.. pipeinclude:: s3qlctrl --help
   :start-after: Options:
   :end-before: Commands:


Exit Codes
==========

|command| may terminate with the following exit codes:

.. include:: ../include/exitcodes.rst

.. include:: ../include/postman.rst


.. |command| replace:: :program:`s3qlctrl`
