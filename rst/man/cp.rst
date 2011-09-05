.. -*- mode: rst -*-


=================================
The :program:`s3qlcp` command
=================================

Synopsis
========

::

   s3qlcp [options] <source-dir> <dest-dir> 
  
Description
===========

.. include:: ../include/about.rst

The |command| command duplicates the directory tree :var:`source-dir`
into :var:`dest-dir` without physically copying the file contents.
Both source and destination must lie inside the same S3QL file system.

.. begin_main_content

The replication will not take any additional space. Only if one of
directories is modified later on, the modified data will take
additional storage space.

`s3qlcp` can only be called by the user that mounted the file system
and (if the file system was mounted with `--allow-other` or `--allow-root`)
the root user. This limitation might be removed in the future (see `issue 155
<http://code.google.com/p/s3ql/issues/detail?id=155>`_).

Note that:

* After the replication, both source and target directory will still
  be completely ordinary directories. You can regard `<src>` as a
  snapshot of `<target>` or vice versa. However, the most common
  usage of `s3qlcp` is to regularly duplicate the same source
  directory, say `documents`, to different target directories. For a
  e.g. monthly replication, the target directories would typically be
  named something like `documents_January` for the replication in
  January, `documents_February` for the replication in February etc.
  In this case it is clear that the target directories should be
  regarded as snapshots of the source directory.

* Exactly the same effect could be achieved by an ordinary copy
  program like `cp -a`. However, this procedure would be orders of
  magnitude slower, because `cp` would have to read every file
  completely (so that S3QL had to fetch all the data over the network
  from the backend) before writing them into the destination folder.

* Before starting with the replication, S3QL has to flush the local
  cache. So if you just copied lots of new data into the file system
  that has not yet been uploaded, replication will take longer than
  usual.



Snapshotting vs Hardlinking
---------------------------

Snapshot support in S3QL is inspired by the hardlinking feature that
is offered by programs like `rsync <http://www.samba.org/rsync>`_ or
`storeBackup <http://savannah.nongnu.org/projects/storebackup>`_.
These programs can create a hardlink instead of copying a file if an
identical file already exists in the backup. However, using hardlinks
has two large disadvantages:

* backups and restores always have to be made with a special program
  that takes care of the hardlinking. The backup must not be touched
  by any other programs (they may make changes that inadvertently
  affect other hardlinked files)

* special care needs to be taken to handle files which are already
  hardlinked (the restore program needs to know that the hardlink was
  not just introduced by the backup program to safe space)

S3QL snapshots do not have these problems, and they can be used with
any backup program.


.. end_main_content
  

Options
=======

The |command| command accepts the following options:

.. pipeinclude:: ../../bin/s3qlcp --help
   :start-after: show this help message and exit


.. include:: ../include/postman.rst


.. |command| replace:: :command:`s3qlcp` 

