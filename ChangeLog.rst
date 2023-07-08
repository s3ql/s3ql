UNRELEASED CHANGES
==================

* The internal file system revision has changed. File systems created with this version of
   S3QL are NOT COMPATIBLE with prior S3QL versions.

  Existing file systems must be upgraded before they can be used with current
  S3QL versions. This procedure is NOT REVERSIBLE.

  To update an existing file system, use the `s3qladm upgrade` command. This upgrade
  should not take longer than a regular mount + unmount sequence.

* S3QL no longer supports storage backends that do not provide immediate consistency.

* S3QL no longer maintains entire filesystem metadata in a single storage object. Instead,
  the database file is distributed across multiple backend objects with a block size
  configured at mkfs time. This means that (1) S3QL also no longer needs to upload the
  entire metadata object on unmount; and (2) there is no longer a size limit on the
  metadata.

* The Google Storage backend now retries on network errors when doing the initial
  validation of the bucket.


OLDER RELEASES
==============

Please see `Changes.txt` (shipped in S3QL older releases).
