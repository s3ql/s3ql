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

This program measures your uplink bandwidth and compression speed and
recommends a compression algorithm for optimal throughput.


s3_copy.py
==========

This program physically duplicates Amazon S3 bucket. It can be used to
migrate buckets to a different storage region or storage class
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


s3_backup.sh
============

This is an example script that demonstrates how to set up a simple but
powerful backup solution using S3QL and `rsync
<http://samba.org/rsync>`_.

The `s3_backup.sh` script automates the following steps:

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

To define what backups you want to keep for how long, you define a
number of *age ranges*. :program:`expire_backups` ensures that you
will have at least one backup in each age range at all times. It will
keep exactly as many backups as are required for that and delete any
backups that become redundant.

Age ranges are specified by giving a list of range boundaries in terms
of backup cycles. Every time you create a new backup, the existing
backups age by one cycle.

Example: when :program:`expire_backups` is called with the age range
definition ``1 3 7 14 31``, it will guarantee that you always have the
following backups available:

#. A backup that is 0 to 1 cycles old (i.e, the most recent backup)
#. A backup that is 1 to 3 cycles old
#. A backup that is 3 to 7 cycles old
#. A backup that is 7 to 14 cycles old
#. A backup that is 14 to 31 cycles old

.. NOTE::

  If you do backups in fixed intervals, then one cycle will be
  equivalent to the backup interval. The advantage of specifying the
  age ranges in terms of backup cycles rather than days or weeks is
  that it allows you to gracefully handle irregular backup intervals.
  Imagine that for some reason you do not turn on your computer for
  one month. Now all your backups are at least a month old, and if you
  had specified the above backup strategy in terms of absolute ages,
  they would all be deleted! Specifying age ranges in terms of backup
  cycles avoids these sort of problems.
  
:program:`expire_backups` usage is simple. It requires backups to have
names of the forms ``year-month-day_hour:minute:seconds``
(``YYYY-MM-DD_HH:mm:ss``) and works on all backups in the current
directory. So for the above backup strategy, the correct invocation
would be::

  expire_backups.py 1 3 7 14 31

When storing your backups on an S3QL file system, you probably want to
specify the ``--use-s3qlrm`` option as well. This tells
:program:`expire_backups` to use the :ref:`s3qlrm <s3qlrm>` command to
delete directories.


:program:`expire_backups` uses a "state file" to keep track which
backups are how many cycles old (since this cannot be inferred from
the dates contained in the directory names). The standard name for
this state file is :file:`.expire_backups.dat`. If this file gets
damaged or deleted, :program:`expire_backups` no longer knows the ages
of the backups and refuses to work. In this case you can use the
*--reconstruct-state* option to try to reconstruct the state from the
backup dates. However, the accuracy of this reconstruction depends
strongly on how rigorous you have been with making backups (it is only
completely correct if the time between subsequent backups has always
been exactly the same), so it's generally a good idea not to tamper
with the state file. 


For a full list of available options, run ``expire_backups.py
--help``.


s3ql.conf
=========

``s3ql.conf`` is an example upstart job definition file. It defines a
job that automatically mounts an S3QL file system on system start, and
properly unmounts it when the system is shut down.

