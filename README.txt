About S3QL
----------

S3QL is a FUSE file system that stores all its data in the Amazon
Simple Storage Service ("S3", http://aws.amazon.com/s3). It
effectively allows you to use S3 as a harddisk with infinite capacity
that can be accessed from any computer with internet access.

S3QL has been designed mainly to store backups, and many of its
features are geared towards this purpose. However, since S3QL is
providing a standard UNIX file system, it can not only be used with
every backup program but also for any other purpose.

S3QL is designed to favor simplicity and elegance over performance and
feature-richness. Care has been taken to make the source code as
readable and serviceable as possible. Solid error detection and error
handling have been included from the very first line, and S3QL comes
with extensive automated test cases for all its components.

Features
--------

    * Compression. Before being stored in S3, all data can be
      compressed using the highly efficient Bz2 algorithm (the same
      algorithm that is used by the bzip2 -9 command on Unix).

    * Encryption. After compression (but still before storage in S3),
      all data can be AES encrypted using 256 bit key. This protects
      the data from being seen by anyone else. Additionally, a SHA256
      HMAC checksum protects the data against unauthorized
      modifications.

    * Data De-duplication. If several files have identical contents,
      the duplicate data will be stored only once. This works across
      all files stored in the file system, and also if only some parts
      of the files are identical while other parts are different.

    * Writable Snapshots. Inside an S3QL file system, you can
      duplicate entire directory trees without using any additional
      storage space. Only if one of the copies is modified, the part
      of the data that has been modified will take up additional
      storage space.

      This feature can be used to create snapshots of the contents of
      a given directory at a given point in time.

    * Performance independent of network latency. S3QL saves the file
      and directory structure in a database that is downloaded from S3
      when the file system is mounted and uploaded again when the file
      system is unmounted. The file contents are transferred on demand
      in blocks of a configurable size and are cached locally. Most
      file system operations can therefore be carried out without any
      (potentially slow) network access at all.


For more detailed installation and usage instructions, please take a
look at the extensive documentation at http://code.google.com/p/s3ql/wiki/about.


Development Status
------------------

*S3QL is in alpha stage.* This means that most of the time you can
create, store and retrieve small amounts of data (~100 MB) without
problems. You can probably also work with hundreds of gigabytes, but
testing is only done with small datasets so you may run into
unexpected problems. Unrelated to how much you store, all your data
may become inaccessible overnight because backwards-incompatible
changes to the code are made without warning.

Please report any problems or bugs that you may encounter on the issue
tracker at http://code.google.com/p/s3ql/issues/list


Author
------

S3QL is maintained by Nikolaus Rath (Nikolaus@rath.org).
