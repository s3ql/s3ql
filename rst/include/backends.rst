.. -*- mode: rst -*-

The form of the storage url depends on the backend that is used. The
following backends are supported:

Amazon S3
---------

To store your file system in an Amazon S3 bucket, use a storage URL of
the form `s3://<bucketname>`. Bucket names must conform to the S3 Bucket
Name Restrictions.


Local
------

The local backend stores file system data in a directory on your
computer. The storage URL for the local backend has the form
`local://<path>`. Note that you have to write three consecutive
slashes to specify an absolute path, e.g. `local:///var/archive`.

SFTP
----

The storage URL for SFTP connections has the form ::

  sftp://<host>[:port]/<path>

