.. -*- mode: rst -*-

=====================
The |command| command
=====================


Synopsis
========

::

   s3qlctrl [options] <action> <mountpoint> ...

where :var:`action` may be either of :program:`flushcache`,
:program:`upload-meta`, :program:`cachesize` or
:program:`log-metadata`.
  
Description
===========

.. include:: ../include/about.rst

The |command| command performs various actions on the S3QL file system mounted
in :var:`mountpoint`.

|command| can only be called by the user that mounted the file system
and (if the file system was mounted with :cmdopt:`--allow-other` or
:cmdopt:`--allow-root`) the root user.

The following actions may be specified:

flushcache
  Uploads all changed file data to the backend.

upload-meta
  Upload metadata to the backend. All file system operations will
  block while a snapshot of the metadata is prepared for upload.

cachesize
  Changes the cache size of the file system. This action requires an
  additional argument that specifies the new cache size in KiB, so the
  complete command line is::
  
   s3qlctrl [options] cachesize <mountpoint> <new-cache-size>

log
  Change the amount of information that is logged into 
  :file:`~/.s3ql/mount.log` file. The complete syntax is::

    s3qlctrl [options] log <mountpoint> <level> [<module> [<module> ...]]

  here :var:`level` is the desired new log level and may be either of
  *debug*, *info* or *warn*. One or more :var:`module` may only be
  specified with the *debug* level and allow to restrict the debug
  output to just the listed modules. 
  

Options
=======

The |command| command also accepts the following options, no matter
what specific action is being invoked:

.. pipeinclude:: python ../../bin/s3qlctrl --help
   :start-after: show this help message and exit


.. include:: ../include/postman.rst


.. |command| replace:: :program:`s3qlctrl` 

