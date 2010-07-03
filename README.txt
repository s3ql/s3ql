About S3QL
-----------

S3QL is a file system that stores all its data online. It supports
Amazon S3 as well as arbitrary SFTP servers and effectively provides
you with a hard disk of infinite capacity that can be accessed from
any computer with internet access.

S3QL has been designed mainly for backup and archival purposes and many of its
features are especially geared towards this. However, since S3QL is providing a
standard UNIX file system, it can in principle be used for any other
application as well.

S3QL is designed to favor simplicity and elegance over performance and
feature-richness. Care has been taken to make the source code as readable and
serviceable as possible. Solid error detection and error handling have been
included from the very first line, and S3QL comes with extensive automated test
cases for all its components.

Homepage: http://code.google.com/p/s3ql/


Features

  • Transparency. Generally, you will not be able to distinguish S3QL from a
    local file system like ext3 or NTFS. S3QL removes any limits of the
    underlying storage system (e.g. the 5 GB file size limit of Amazon S3 or
    the inability to make hard links over FTP).

  • Compression. Before storage, all data is compressed with the LZMA algorithm
    (which is roughly 15% more efficient than the bzip2 -9 command on Unix).

  • Encryption. After compression (but still before storage), all data is AES
    encrypted with a 256 bit key. An additional SHA256 HMAC checksum is used to
    protect the data against manipulation.

  • Data De-duplication. If several files have identical contents, the
    redundant data will be stored only once. This works across all files stored
    in the file system, and also if only some parts of the files are identical
    while other parts differ.

  • Writable Snapshots. Inside an S3QL file system, you can duplicate entire
    directory trees without using any additional storage space. Only if one of
    the copies is modified, the part of the data that has been modified will
    take up additional storage space (Copy on Write).

    This feature can be used to create snapshots of the contents of a given
    directory at a given point in time.

  • Support for high latency, low bandwidth connections. S3QL saves the file
    and directory structure in a database that is downloaded once when the file
    system is mounted and uploaded again when the file system is unmounted. All
    operations that do not directly write or read file contents can therefore
    be carried out very fast without any network transactions. (Such operations
    are directory creation, moving and renaming files, changing permissions,
    etc).

    File contents are cached, so reading or writing to a file requires network
    access only if the data is not yet in the cache. Each file is also split
    into small blocks that are stored separately, and if data is needs to be
    transferred over the network, only the required block is written or
    retrieved. Therefore making a small change (or reading a small part) in a
    large file does not require the entire file to be transferred.


Development Status
------------------

S3QL is in beta stage. This means that:

  • There are still plenty of bugs in the code and you should be ready to deal
    with them. Please report problems on the mailing list or issue tracker,
    usually they are resolved in less than a day.

  • However, you can be reasonably confident that the bugs (although
    inconvenient) will not endanger your stored data.

  • The author is using this version of S3QL as his main backup system on
    several computers and stores about 10 GB of data daily. Byte-by-byte
    comparisons of the backup against the original data are done every once in
    a while and so far have shown no problems.

  • If you intend to use S3QL as an archive file system (i.e. the data will
    only be stored in S3QL and nowhere else) it is recommended that you still
    maintain secondary copies until S3QL reaches production stage.

  • Future versions of S3QL will be backwards compatible, so you will not have
    to recreate the file system when you upgrade to a newer version of S3QL.

Please report any problems or bugs that you may encounter on the Issue Tracker.
