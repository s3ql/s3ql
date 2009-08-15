About S3QL
----------

S3QL is a Linux file system that stores all its data on Amazon S3
(http://aws.amazon.com/s3/). It effectively allows you to use S3 as a
harddisk with infinite capacity that can be accessed from any computer
with internet access. S3QL is not a network file system though, it can
only be mounted on one computer at a time.

S3QL provides full-fledged POSIX file system semantics. File contents
are stored in individual S3 objects (for large files, the contents are
split into several S3 objects with a configurable block size). The
filesystem metadata (file names, locations, permissions, etc) is
managed in a database and stored in one single additional S3 object.

S3QL has been created primarily to store backups. Planned features for
future releases are mostly designed to facilitate backups and include
automatic compression, encryption, data de-duplication and zero-io
copying. Since S3QL is simply providing a standard UNIX file system,
it can be used in combination with any backup program.

S3QL is designed to favor simplicity and elegance over performance and
feature-richness. Care has been taken to make the source code as
readable and serviceable as possible. Solid error detection and error
handling have been included from the very first line, and S3QL comes
with extensive automated test cases for all its components.


For installation instructions and documentation please refer to
http://code.google.com/docreader/#p=s3ql

S3QL is maintained by Nikolaus Rath (Nikolaus@rath.org).
