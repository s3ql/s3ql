.. -*- mode: rst -*-

.. _durability:

=======================================
 Important Rules to Avoid Losing Data
=======================================

Most S3QL backends store data in distributed storage systems. These
systems differ from a traditional, local hard disk in several
important ways. In order to avoid losing data, this section should be
read very carefully.

Rules in a Nutshell
===================

To avoid losing your data, obey the following rules:

#. Know what durability you can expect from your chosen storage
   provider. The durability describes how likely it is that a stored
   object becomes damaged over time. Such data corruption can never be
   prevented completely, techniques like geographic replication and
   RAID storage just reduce the likelihood of it to happen (i.e.,
   increase the durability).

#. When choosing a backend and storage provider, keep in mind that
   when using S3QL, the effective durability of the file system data
   will be reduced because of S3QL's data de-duplication feature.

#. Make sure that your storage provider provides "immediate consistency" when reading, writing, and
   listing storage objects. Some providers (including, in the past, Amazon S3) only support
   *eventual consistency* which is *insufficient for S3QL* and can lead to complete
   data-loss. (Eventual consistency enables higher availability, but means that changes
   to an object may not be immediately visible to readers).

#. Do not attempt to mount the same file system on different computers (or on
   the same computer but with different :cmdopt:`--cachedir` directories)
   at the same time.

#. Do not attempt to mount (or fsck) a file system on a different computer (or on
   the same computer but with a different :cmdopt:`--cachedir` directory)
   when it was not cleanly unmounted.

.. _backend_reliability:

Data Durability
===============

The durability of a storage service is measure of the
probability of a storage object to become corrupted over time. The
lower the chance of data loss, the higher the durability. Storage
services like Amazon S3 claim to achieve a durability of up to
99.999999999% over a year, i.e. if you store 100000000 objects for 100
years, you can expect that at the end of that time at most one object will be
corrupted or lost.

S3QL is designed to reduce redundancy and store data in the smallest
possible form. Therefore, S3QL is generally not able to compensate for
any such losses, and when choosing a storage service you should
carefully review if the offered durability matches your requirements.
When doing this, there are two factors that should be kept in mind.

Firstly, even though S3QL is not able to compensate for storage
service failures, it is able to detect them: when trying to access
data that has been lost or corrupted by the storage service, an IO
error will be returned.

Secondly, the consequences of a data loss by the storage service can
be significantly more severe than you may expect because of S3QL's
data de-duplication feature: a data loss in the storage service at
time *x* may cause data that is written *after* time *x* to be lost as
well. Consider the following scenario:

#. You store an important file in the S3QL file system.
#. The storage service loses the data blocks of this file. As long as you
   do not access the file or run :program:`s3ql_verify`, S3QL is not
   aware that the data has been lost by the storage service.
#. You save an additional copy of the important file in a different
   location on the same S3QL file system.
#. S3QL detects that the contents of the new file are identical to the
   data blocks that have been stored earlier. Since at this point S3QL
   is not aware that these blocks have been lost by the storage service, it
   does not save another copy of the file contents in the storage service but
   relies on the (presumably) existing blocks instead.
#. Therefore, even though you saved another copy, you still do not
   have a backup of the important file (since both copies refer to the
   same data blocks that have been lost by the storage service).

For some storage services, :program:`fsck.s3ql` can mitigate this
effect. When :program:`fsck.s3ql` runs, it asks the storage service
for a list of all stored objects. If objects are missing, it can then
mark the damaged files and prevent the problem from spreading forwards
in time. Figuratively speaking, this establishes a "checkpoint": data
loss that occurred before running :program:`fsck.s3ql` can not affect
any file system operations that are performed after the check.
Unfortunately, many storage services only "discover" that objects are
missing or broken when the object actually needs to be retrieved. In
this case, :program:`fsck.s3ql` will not learn anything by just
querying the list of objects.

The problem be further mitigated by using the :program:`s3ql_verify` command in addition
to :program:`fsck.s3ql`. :program:`s3ql_verify` asks the storage service to look up every
stored object and may therefore take much longer than running :program:`fsck.s3ql`, but
can also offer a much stronger assurance that no data has been lost by the storage
service. To "recover" from damaged storage objects in the backend, the damaged objects
found by :program:`s3ql_verify` have to be explicitly deleted (so that a successive
:program:`fsck.s3ql` is able detect them as missing, correct the file system metadata, and
move any affected files to :file:`lost+found`). This procedure is currently not automated,
so it is generally a good idea to choose a storage service where the expected data
durability is high enough so that the possibility of a lost object (and thus the need to
run any full checks) can be neglected over long periods of time.
