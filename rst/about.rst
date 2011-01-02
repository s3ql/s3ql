.. -*- mode: rst -*-

============
 About S3QL
============

S3QL is a file system that stores all its data online. It supports
`Amazon S3 <http://aws.amazon.com/s3 Amazon S3>`_ as well as arbitrary
SFTP servers and effectively provides you with a hard disk of dynamic,
infinite capacity that can be accessed from any computer with internet
access.

S3QL is providing a standard, full featured UNIX file system that is
conceptually indistinguishable from any local file system.
Furthermore, S3QL has additional features like compression,
encryption, data de-duplication, immutable trees and snapshotting
which make it especially suitable for online backup and archival.

S3QL is designed to favor simplicity and elegance over performance and
feature-creep. Care has been taken to make the source code as
readable and serviceable as possible. Solid error detection and error
handling have been included from the very first line, and S3QL comes
with extensive automated test cases for all its components.

Features
========


* **Transparency.** Conceptually, S3QL is indistinguishable from a
  local file system. For example, it supports hardlinks, symlinks,
  full unix permissions, extended attributes and file sizes up to 2
  TB.

* **Dynamic Size.** The size of an S3QL file system grows and shrinks
  dynamically as required. 

* **Compression.** Before storage, all data may compressed with the
  LZMA, BZIP2 or LZ algorithm.

* **Encryption.** After compression (but before upload), all data can
  AES encrypted with a 256 bit key. An additional SHA256 HMAC checksum
  is used to protect the data against manipulation.

* **Data De-duplication.** If several files have identical contents,
  the redundant data will be stored only once. This works across all
  files stored in the file system, and also if only some parts of the
  files are identical while other parts differ.

* **Immutable Trees.** Directory trees can be made immutable, so that
  their contents can no longer be changed in any way whatsoever. This
  can be used to ensure that backups can not be modified after they
  have been made.

* **Copy-on-Write/Snapshotting.** S3QL can replicate entire directory
  trees without using any additional storage space. Only if one of the
  copies is modified, the part of the data that has been modified will
  take up additional storage space. This can be used to create
  intelligent snapshots that preserve the state of a directory at
  different points in time using a minimum amount of space.

* **High Performance independent of network latency.** All operations
  that do not write or read file contents (like creating directories
  or moving, renaming, and changing permissions of files and
  directories) are very fast because they are carried out without any
  network transactions.

  S3QL achieves this by saving the entire file and directory structure
  in a database. This database is locally cached and the remote
  copy updated asynchronously.

* **Support for low bandwidth connections.** S3QL splits file contents
  into smaller blocks and caches blocks locally. This minimizes both
  the number of network transactions required for reading and writing
  data, and the amount of data that has to be transferred when only
  parts of a file are read or written.



Development Status
==================

**S3QL is in beta stage.** This means that:

* When using S3QL you should be subscribed to the `mailing list
  <http://groups.google.com/group/s3ql>`_.
 
* There are still plenty of bugs in the code and you should be ready
  to deal with them. Nevertheless, you can be reasonably confident
  that the bugs (although inconvenient) will not endanger your stored
  data.

* The author is using this version of S3QL as his main backup system
  on several computers and stores about 300 GB of data. Daily
  incremental backups have a volume of about 200 MB.

  Byte-by-byte comparisons of the backup against the original data are
  done every once in a while and so far have shown no discrepancies.

* If you intend to use S3QL as an archive file system (i.e. the data
  will only be stored in S3QL and nowhere else) it is recommended that
  you still maintain secondary copies until S3QL reaches production
  stage.

* Future versions of S3QL will be backwards compatible.

Please report any problems on the `mailing list
<http://groups.google.com/group/s3ql>`_ or the `issue tracker
<http://code.google.com/p/s3ql/issues/list>`_.
