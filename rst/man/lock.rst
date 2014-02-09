.. -*- mode: rst -*-


=================================
The :program:`s3qllock` command
=================================

Synopsis
========

::

   s3qllock [options] <directory>
  
Description
===========

.. include:: ../include/about.rst

The :program:`s3qllock` command makes a directory tree in an S3QL file
system immutable. Immutable trees can no longer be changed in any way
whatsoever. You can not add new files or directories and you can not
change or delete existing files and directories. The only way to get
rid of an immutable tree is to use the :program:`s3qlrm` command.

|command| can only be called by the user that mounted the file system
and (if the file system was mounted with :cmdopt:`--allow-other` or
:cmdopt:`--allow-root`) the root user. 

Rationale
=========

.. begin_main_content

Immutability is a feature designed for backups. Traditionally, backups
have been made on external tape drives. Once a backup was made, the
tape drive was removed and locked somewhere in a shelf. This has the
great advantage that the contents of the backup are now permanently
fixed. Nothing (short of physical destruction) can change or delete
files in the backup.

In contrast, when backing up into an online storage system like S3QL,
all backups are available every time the file system is mounted.
Nothing prevents a file in an old backup from being changed again
later on. In the worst case, this may make your entire backup system
worthless. Imagine that your system gets infected by a nasty virus
that simply deletes all files it can find -- if the virus is active
while the backup file system is mounted, the virus will destroy all
your old backups as well! 

Even if the possibility of a malicious virus or trojan horse is
excluded, being able to change a backup after it has been made is
generally not a good idea. A common S3QL use case is to keep the file
system mounted at all times and periodically create backups with
:program:`rsync -a`. This allows every user to recover her files from a
backup without having to call the system administrator. However, this
also allows every user to accidentally change or delete files *in* one
of the old backups.

Making a backup immutable protects you against all these problems.
Unless you happen to run into a virus that was specifically programmed
to attack S3QL file systems, backups can be neither deleted nor
changed after they have been made immutable.


.. end_main_content
  

Options
=======

The |command| command accepts the following options:

.. pipeinclude:: python ../../bin/s3qllock --help
   :start-after: show this help message and exit


.. include:: ../include/postman.rst


.. |command| replace:: :command:`s3qllock` 

