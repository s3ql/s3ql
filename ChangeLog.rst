S3QL 5.2.3 (2024-11-02)
=======================

* Make release tarballs compatible with Python 3.12 by using a newer Cython version
  to generate the necessary C++ extension.
* Minor bugfixes and improvements (see git history for details)

S3QL 5.2.2 (2024-09-01)
=======================

* Make S3QL's HTTP client library more forgiving when HTTP servers do not behave
  according to HTTP/1.1 specification. This will make filesystems with
  OVHcloud's OpenStack Swift implementation more robust.

* Fix a bug where, on a temporary network error, S3QL would immediately retry
  once and crash if not successful.

S3QL 5.2.1 (2024-07-05)
======================

* Fixed a crash with `TypeError: CacheEntry.seek() takes 2 positional arguments
  but 3 were given` when using neither encryption nor compression.

* Fixed a crash with `s3ql.http.StateError: No pending requests`.

S3QL 5.2.0 (2024-04-19)
=======================

* S3QL now needs Python 3.8+. Python 3.7 is end of life as of 2023-06-27.

* S3QL does not depend on packaging anymore. It was an undocumented dependency
  for a simple version compare of the Swift backend. This compare is not
  necessary anymore.

* There is a new s3c4 backend, suitable for storage providers offering an
  S3 compatible API with v4 signatures.

S3QL 5.1.3 (2023-12-08)
=======================

* fsck.s3ql no longer attempts to verify unclean metadata backups, which
  in the past led to spurious warnings and crashes.

* Fixed a crash in the b2 backend.

S3QL 5.1.2 (2023-09-26)
=======================

* Various small bugfixes, b2 backend should be working again.

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
