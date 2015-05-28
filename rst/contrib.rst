.. -*- mode: rst -*-

=====================
Contributed Programs
=====================

S3QL comes with a few contributed programs that are not part of the
core distribution (and are therefore not installed automatically by
default), but which may nevertheless be useful. These programs are in
the `contrib` directory of the source distribution or in
`/usr/share/doc/s3ql/contrib` if you installed S3QL from a package.


benchmark.py
============

This program measures S3QL write performance, uplink bandwidth and
compression speed to determine the limiting factor. It also gives
recommendation for compression algorithm and number of upload threads
to achieve maximum performance.


clone_fs.py
==========

This program physically clones an S3QL file system from one backend
into another, without recompressing or reencrypting.  It can be used to
migrate S3 buckets to a different storage region or storage class
(standard or reduced redundancy).

.. _pcp:

pcp.py
======

``pcp.py`` is a wrapper program that starts several rsync processes to
copy directory trees in parallel. This is important because
transferring files in parallel significantly enhances performance when
copying data from an S3QL file system (see :ref:`copy_performance` for
details).

To recursively copy the directory ``/mnt/home-backup`` into
``/home/joe`` using 8 parallel processes and preserving permissions,
you would execute ::

  pcp.py -a --processes=8 /mnt/home-backup/ /home/joe


s3ql_backup.sh
============

This is an example script that demonstrates how to set up a simple but
powerful backup solution using S3QL and `rsync
<http://samba.org/rsync>`_.

The `s3ql_backup.sh` script automates the following steps:

#. Mount the file system
#. Replicate the previous backup with :ref:`s3qlcp <s3qlcp>`
#. Update the new copy with the data from the backup source using rsync
#. Make the new backup immutable with :ref:`s3qllock <s3qllock>`
#. Delete old backups that are no longer needed
#. Unmount the file system

The backups are stored in directories of the form
`YYYY-MM-DD_HH:mm:SS` and the `expire_backups.py`_ command is used to
delete old backups.


expire_backups.py
=================

:program:`expire_backups.py` is a program to intelligently remove old
backups that are no longer needed.

.. include:: man/expire_backups.rst
   :start-after: begin_main_content
   :end-before: end_main_content

For a full list of available options, run :program:`expire_backups.py
--help`.

.. _remove_objects:

remove_objects.py
=================

:program:`remove_objects.py` is a program to remove a list of objects
from a storage backend. Since it acts on the backend-level, the
backend need not contain an S3QL file system.
