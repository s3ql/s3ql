S3QL 5.1.1 (2023-08-06)
=======================

* Fixed a DATA LOSS issue: Metadata upload now works correctly if the cache directory
  contains a symlink in its path. In S3QL 5.0.0 and 5.1.0, this would result in metadata
  silently not being uploaded to the backend.


S3QL 5.1.0 (2023-08-01)
=======================

* The user's guide has been fully reviewed and updated.

* fsck.s3ql now confirms that the most recent metadata backups are intact.

* The Backblaze B2 backend may no longer be working correctly. This is because in this
  release cycle nobody was available to test it. If you are using this backend, consider
  making yourself known on the S3QL mailing list or the backend may get removed due to
  apparent unuse.

* The `s3ql_verify` command now also checks if the contents of blocks hash to the
  expected value.

* There should be no more `fuse_lowlevel_notify_inval_entry returned: No such file or
  directory` errors in `mount.log` when shortly after running *s3qlrm* the system gets
  under memory pressure (or the filesystem is unmounted).

* Reading and modifying existing data that is already cached no longer blocks if the cache
  is full.


S3QL 5.0.0 (2023-07-08)
=======================

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
