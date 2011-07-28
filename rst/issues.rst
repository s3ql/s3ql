.. -*- mode: rst -*-

============
Known Issues
============

* S3QL is rather slow when an application tries to write data in
  unreasonably small chunks. If a 1 MB file is copied in chunks of 1
  KB, this will take more than 10 times as long as when it's copied
  with the (recommended) chunk size of 128 KB.

  This is a limitation of the FUSE library (which does not yet support
  write caching) which will hopefully be addressed in some future FUSE
  version.

  Most applications, including e.g. GNU `cp` and `rsync`, use
  reasonably large buffers and are therefore not affected by this
  problem and perform very efficient on S3QL file systems.

  However, if you encounter unexpectedly slow performance with a
  specific program, this might be due to the program using very small
  write buffers. Although this is not really a bug in the program,
  it might be worth to ask the program's authors for help.

* S3QL always updates file and directory access times as if the ``relatime``
  mount option has been specified: the access time ("atime") is only updated
  if it is currently earlier than either the status change time
  ("ctime") or modification time ("mtime"). 

* S3QL directories always have an `st_nlink` value of 1. This may confuse
  programs that rely on directories having `st_nlink` values of *(2 +
  number of sub directories)*.

  Note that this is not a bug in S3QL. Including sub directories in
  the `st_nlink` value is a Unix convention, but by no means a
  requirement. If an application blindly relies on this convention
  being followed, then this is a bug in the application.

  A prominent example are early versions of GNU find, which required
  the `--noleaf` option to work correctly on S3QL file systems. This
  bug has already been fixed in recent find versions.


* In theory, S3QL is not fully compatible with NFS. Since S3QL does
  not support *inode generation numbers*, NFS clients may (once again,
  in theory) accidentally read or write the wrong file in the
  following situation:

  #. An S3QL file system is exported over NFS
  #. NFS client 1 opens a file A
  #. Another NFS client 2 (or the server itself) deletes file A (without
     client 1 knowing about this)
  #. A new file B is created by either of the clients or the server
  #. NFS client 1 tries to read or write file A (which has actually already been deleted).

  In this situation it is possible that NFS client 1 actually writes
  or reads the newly created file B instead. The chances of this are 1
  to (2^32 - *n*) where *n* is the total number of directory entries
  in the S3QL file system (as displayed by `s3qlstat`).

  Luckily enough, as long as you have less than about 2 thousand
  million directory entries (2^31), the chances for this are totally
  irrelevant and you don't have to worry about it.

* The `umount` and `fusermount -u` commands will *not* block until all
  data has been uploaded to the backend. (this is a FUSE limitation
  that will hopefully be removed in the future, see `issue 159
  <http://code.google.com/p/s3ql/issues/detail?id=159>`_). If you use
  either command to unmount an S3QL file system, you have to take care
  to explicitly wait for the `mount.s3ql` process to terminate before
  you shut down or restart the system. Therefore it is generally not a
  good idea to mount an S3QL file system in `/etc/fstab` (you should
  use a dedicated init script instead).

* S3QL relies on the backends not to run out of space. This is a given
  for big storage providers like Amazon S3, but you may stumble upon
  this if you store buckets e.g. on smaller servers or servies.

  If there is no space left in the backend, attempts to write more
  data into the S3QL file system will fail and the file system will be
  in an inconsistent state and require a file system check (and you
  should make sure to make space available in the backend before
  running the check).

  Unfortunately, there is no way to handle insufficient space in the
  backend without leaving the file system inconsistent. Since
  S3QL first writes data into the cache, it can no longer return an
  error when it later turns out that the cache can not be committed to
  the backend.
