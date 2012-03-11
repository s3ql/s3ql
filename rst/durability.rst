.. -*- mode: rst -*-

.. _durability:

=======================================
 Important Rules to Avoid Losing Data
=======================================

Most S3QL backends store data in distributed storage systems. These
systems differ from a traditional, local hard disk in several
important ways. In order to avoid loosing data, this section should be
read very carefully.

Rules in a Nutshell
===================

To avoid losing your data, obey the following rules:

#. Know what durability you can expect from your chosen storage
   service. The durability describes how likely it is that a stored
   object becomes damaged over time. Such data corruption can never be
   prevented completely, techniques like geographic replication and
   RAID storage just reduce the likelihood of it to happen (i.e.,
   increase the durability).

#. When choosing a storage service, keep in mind that when using S3QL,
   the effective durability of the file system data will be reduced
   because of S3QL's data de-duplication feature.

#. Determine your storage service's consistency window. The
   consistency window that is important for S3QL is the smaller of the
   times for which:

   - a newly created object may not yet be included in the list of
     stored objects

   - an attempt to read a newly created object may fail with the
     storage service reporting that the object does not exist

   If *one* of the above times is zero, we say that as far as S3QL is
   concerned the storage service has *immediate* consistency.

   If your storage provider claims that *neither* of the above can
   ever happen, while at the same time promising high durability, you
   should choose a respectable service instead.

#. When mounting the same file system on different computers (or on
   the same computer but with different ``--cachedir`` directories),
   the time that passes between the first and second of invocation of
   :program:`mount.s3ql` must be at least as long as your storage
   service's consistency window. If your storage service offers
   immediate consistency, you do not need to wait at all.

#. Before running :program:`fsck.s3ql` or :program:`s3qladm`, the file system
   must have been left untouched for the length of the consistency
   window. If your storage service offers immediate consistency, you
   do not need to wait at all.

The rest of this section explains the above rules and the reasons for
them in more detail. It also contains a list of the consistency
windows for a number of larger storage providers.


Consistency Window List
=======================

The following is a list of the consistency windows (as far as S3QL is
concerned) for a number of storage providers. This list doesn't come
with any guarantees and may be outdated. If your storage service is
not included, or if you need more reliable information, check with
your storage provider.

=======================================   ===================
Storage Service                           Consistency
=======================================   ===================
Amazon S3 in the US standard region       Eventual
Amazon S3 in other regions                Immediate
Google Storage                            Immediate
RackSpace CloudFiles                      Eventual
=======================================   ===================


Storage Service Consistency
===========================

In contrast to the typical hard disk, most storage services do not
guarantee *immediate consistency* of written data. This means that:

* after an object has been stored, requests to read this object may
  still fail or return the prior contents for a little while.

* after an object has been deleted, attempts to read it may still
  return the (old) data for some time, and it may still remain in the
  list of stored objects for some time.

* after a new object has been created, it may still not be included
  when retrieving the list of stored objects for some time.

Of course, none of this is acceptable for a file system, and S3QL
generally handles any of the above situations internally so that it
always provides a fully consistent file system to the user. However,
there are some situations where an S3QL user nevertheless needs to be
aware of the peculiarities of his chosen storage service. 

Suppose that you mount the file system, store some new data, delete
some old data and unmount it. If you then mount the file system again
right away on another computer, there is no guarantee that S3QL will
see any of the changes that the first S3QL process has made. At least
in theory it is therefore possible that on the second mount, S3QL does
not see any of the changes that you have done and presents you an "old
version" of the file system without them. Even worse, if you notice
the problem and unmount the file system, S3QL will upload the old
status (which S3QL necessarily has to consider as current) and thereby
permanently override the newer version (even though this change may
not become immediately visible either). S3QL uses several techniques
to reduce the likelihood of this to happen (see :ref:`impl_details`
for more information on this), but without support from the storage
service, the possibility cannot be eliminated completely.

The same problem of course also applies when checking the file system.
If the storage service provides S3QL with only partially updated data,
S3QL has no way to find out if this a real consistency problem that
needs to be fixed or if it is only a temporary problem that will
resolve itself automatically (because there are still changes that
have not become visible yet).

This is where the so called *consistency window* comes in. The
consistency window is the maximum time (after writing or deleting the
object) for which any of the above "outdated responses" may be
received. If the consistency window is zero, i.e. all changes are
immediately effective, the storage service is said to have *immediate
consistency*. If the window is infinite, i.e. there is no upper bound
on the time it may take for changes to become effect, the storage
service is said to be *eventually consistent*. Note that often there
are different consistency windows for the different operations. For
example, Google Storage offers immediate consistency when reading
data, but only eventual consistency for the list of stored objects.

To prevent the problem of S3QL working with an outdated copy of the
file system data, it is therefore sufficient to simply wait for the
consistency window to pass before mounting the file system again (or
running a file system check). The length of the consistency window
changes from storage service to storage service, and if your service
is not included in the list below, you should check the web page or
ask the technical support of your storage provider. The window that is
important for S3QL is the smaller of the times for which

- a newly created object may not yet be included in the list of
  stored objects

- an attempt to read a newly created object may fail with the
  storage service reporting that the object does not exist


Unfortunately, many storage providers are hesitant to guarantee
anything but eventual consistency, i.e. the length of the consistency
window is potentially infinite. In that case you simply have to pick a
length that you consider "safe enough". For example, even though
Amazon is only guaranteeing eventual consistency, the ordinary
consistency window for data stored in S3 is just a few seconds, and
only in exceptional circumstances (i.e., core network outages) it may
rise up to hours (`source
<http://forums.aws.amazon.com/message.jspa?messageID=38471#38471>`_).


.. _backend_reliability:

Storage Service Durability
==========================

The durability of a storage service a measure of the average
probability of a storage object to become corrupted over time. The
lower the chance of data loss, the higher the durability. Storage
services like Amazon S3 claim to achieve a durability of up to
99.999999999% over a year, i.e. if you store 100000000 objects for 100
years, you can expect that at the end of that time one object will be
corrupted or lost.

S3QL is designed to reduce redundancy and store data in the smallest
possible form. Therefore, S3QL is generally not able to compensate for
any such losses, and when choosing a storage service you should
carefully review if the offered durability matches your requirements.
When doing this, there are two factors that should be kept in mind.

Firstly, even though S3QL is not able to compensate for storage
service failures, it is able to detect them: when trying to access
data that has been lost or corrupted by the storage service, an IO
error will be returned and the mount point will become inaccessible to
ensure that the problem is noticed.

Secondly, the consequences of a data loss by the storage service can
be significantly more severe than you may expect because of S3QL's
data de-duplication feature: a data loss in the storage service at
time *x* may cause data that is written *after* time *x* to be lost as
well. Consider the following scenario:

#. You store an important file in the S3QL file system.
#. The storage service looses the data blocks of this file. As long as you
   do not access the file or run :program:`fsck.s3ql`, S3QL is not
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

In the future, :program:`fsck.s3ql` will have an additional
"full-check" mode, in which it attempts to retrieve every single
object. However, this is expected to be rather time consuming and
expensive. Therefore, it is generally a better choice to choose a
storage service where the expected data durability is so high that the
possibility of a lost object (and thus the need to run any full
checks) can be neglected over long periods of time.

To some degree, :program:`fsck.s3ql` can mitigate this effect. When
used with the ``--full-check`` option, :program:`fsck.s3ql` asks the
storage service to look up every stored object. This way, S3QL learns
about any missing and, depending on the storage service, corrupted
objects. It can then mark the damaged files and prevent the problem
from spreading forwards in time. Figuratively speaking, this
establishes a "checkpoint": data loss that occurred before running
:program:`fsck.s3ql` with ``--full-check`` can not affect any file
system operations that are performed after the check.

Unfortunately, a full check is rather time consuming and expensive
because of the need to check every single stored object. It is
generally a better choice to choose a storage service where the
expected data durability is so high that the possibility of a lost
object (and thus the need to run any full checks) can be neglected
over long periods of time.
