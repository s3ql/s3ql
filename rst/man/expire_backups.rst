.. -*- mode: rst -*-


=======================================
 The :program:`expire_backups` command
=======================================

Synopsis
========

::

   expire_backups [options] <age> [<age> ...]

   
Description
===========

The |command| command intelligently remove old backups that are no
longer needed.

.. begin_main_content

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
:cmdopt:`--reconstruct-state` option to try to reconstruct the state
from the backup dates. However, the accuracy of this reconstruction
depends strongly on how rigorous you have been with making backups (it
is only completely correct if the time between subsequent backups has
always been exactly the same), so it's generally a good idea not to
tamper with the state file.

.. end_main_content


Options
=======

The |command| command accepts the following options:

.. pipeinclude:: ../../contrib/expire_backups.py --help
   :start-after: show this help message and exit

Exit Status
===========

|command| returns exit code 0 if the operation succeeded and 1 if some
error occured.


See Also
========

|command| is shipped as part of S3QL, http://code.google.com/p/s3ql/.

.. |command| replace:: :command:`expire_backups` 

