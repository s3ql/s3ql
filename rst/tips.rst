.. -*- mode: rst -*-

=============
Tips & Tricks
=============


.. _copy_performance:


Permanently mounted backup file system
======================================

If you use S3QL as a backup file system, it can be useful to mount the
file system permanently (rather than just mounting it for a backup and
unmounting it afterwards). Especially if your file system becomes
large, this saves you long mount- and unmount times if you only want
to restore a single file.

If you decide to do so, you should make sure to

* Use :ref:`s3qllock <s3qllock>` to ensure that backups are immutable
  after they have been made.

* Call :ref:`s3qlctrl upload-meta <s3qlctrl>` right after a every backup to
  make sure that the newest metadata is stored safely (if you do
  backups often enough, this may also allow you to set the
  *--metadata-upload-interval* option of :program:`mount.s3ql`
  to zero).



Improving copy performance
==========================

If you want to copy a lot of smaller files *from* an S3QL file system
(e.g. for a system restore) you will probably notice that the
performance is rather bad.

The reason for this is intrinsic to the way S3QL works. Whenever you
read a file, S3QL first has to retrieve this file over the network
from the storage backend. This takes a minimum amount of time (the
network latency), no matter how big or small the file is. So when you
copy lots of small files, 99% of the time is actually spend waiting
for network data.

Theoretically, this problem is easy to solve: you just have to copy
several files at the same time. In practice, however, almost all unix
utilities (``cp``, ``rsync``, ``tar`` and friends) insist on copying
data one file at a time. This makes a lot of sense when copying data
on the local hard disk, but in case of S3QL this is really
unfortunate.

The best workaround that has been found so far is to copy files by
starting several rsync processes at once and use exclusion rules to
make sure that they work on different sets of files.

For example, the following script will start 3 rsync instances. The
first instance handles all filenames starting with a-f, the second the
filenames from g-l and the third covers the rest. The ``+ */`` rule
ensures that every instance looks into all directories. ::

  #!/bin/bash

  RSYNC_ARGS="-aHv /mnt/s3ql/ /home/restore/"

  rsync -f "+ */" -f "-! [a-f]*" $RSYNC_ARGS &
  rsync -f "+ */" -f "-! [g-l]*" $RSYNC_ARGS &
  rsync -f "+ */" -f "- [a-l]*" $RSYNC_ARGS &

  wait

The optimum number of parallel processes depends on your network
connection and the size of the files that you want to transfer.
However, starting about 10 processes seems to be a good compromise
that increases performance dramatically in almost all situations.

S3QL comes with a script named ``pcp.py`` in the ``contrib`` directory
that can be used to transfer files in parallel without having to write
an explicit script first. See the description of :ref:`pcp` for
details.
