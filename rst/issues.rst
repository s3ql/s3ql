.. -*- mode: rst -*-

============
Known Issues
============

* S3QL de-duplicates data blocks based solely only on SHA256
  checksums, without doing a byte-by-byte comparison of the blocks.
  Since it is possible for two data blocks to have the same checksum
  despite having different contents, this can lead to problems. If two
  such blocks are stored in an S3QL file system, the data in one block
  will be lost and replaced by the data in the other block. However,
  the chances of this occuring for any two blocks are about 1 in 10^77
  (2^256). For a file system that holds a total of 10^34 blocks, the
  chances of a collision increase to about 1 in 10^9. Storing more
  than 10^34 blocks (or about 10^25 TB with an (extremely small) block
  size of 4 kB) is therefore not recommended. Being exceptionally
  unlucky may also be a disadvantage.

* S3QL does not support Access Control Lists (ACLs). There is nothing
  fundamentally that prevents this, someone just has to write the
  necessary code.

* As of Linux kernel 3.5 S3QL file systems do not implement the "write
  protect" bit on directories. In other words, even if a directory has
  the write protect bit set, the owner of the directory can delete any
  files and (empty) subdirectories inside it. This is a bug in the
  FUSE kernel module
  (cf. https://github.com/libfuse/libfuse/issues/23) and needs to be
  fixed in the kernel.  Unfortunately it does not look as if this is
  going to be fixed anytime soon (as of 2016/2/28).

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

* The `umount` and `fusermount -u` commands will *not* block until all
  data has been uploaded to the backend. (this is a FUSE limitation,
  cf. https://github.com/libfuse/libfuse/issues/1). If you use either
  command to unmount an S3QL file system, you have to take care to
  explicitly wait for the `mount.s3ql` process to terminate before you
  shut down or restart the system. Therefore it is generally not a
  good idea to mount an S3QL file system in `/etc/fstab` (you should
  use a dedicated init script instead).

* S3QL relies on the backends not to run out of space. This is a given
  for big storage providers like Amazon S3 or Google Storage, but you
  may stumble upon this if you use your own server or smaller providers.

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

* When using python-dugong versions 3.3 or earlier, S3QL supports only
  CONNECT-style proxying, which may cause issues with some proxy
  servers when using plain HTTP. Upgrading to python-dugong 3.4 or
  newer removes this limitation.
