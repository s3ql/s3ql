About S3QL
----------

S3QL mounts an Amazon S3 bucket as a filesystem. This allows you to store data
on Amazon S3 as if it were on a local harddisk with infinite capacity. If
desired, the data can also be stored on S3 in compressed and encrypted form.

S3QL provides full-fledged POSIX filesystem semantics. The file contents are
stored in individual S3 objects (for large files, the contents are split into
several S3 objects with a configurable block size). The filesystem metadata
(file names, locations, permissions, etc) is managed in a database and stored
in a single additional S3 object.

S3QL has been written and is maintained by Nikolaus Rath (Nikolaus@rath.org). 


For installation instructions and documentation please refer to
http://code.google.com/docreader/#p=s3ql
