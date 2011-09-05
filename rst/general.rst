.. -*- mode: rst -*-

===================
General Information
===================

Terminology
===========

S3QL can store data at different providers and using different
protocols. A *backend* implements all

use different protocols to store the file system data.


Independent of the backend that you use, the place where your file
system data is being stored is called a *bucket*. (This is mostly for
historical reasons, since initially S3QL supported only the Amazon S3
backend).



The :file:`authinfo` file
=========================

TODO


.. _backend_reliability:

On Backend Reliability
======================

S3QL has been designed for use with a storage backend where data loss
is so infrequent that it can be completely neglected (e.g. the Amazon
S3 backend). If you decide to use a less reliable backend, you should
keep the following warning in mind and read this section carefully.

.. WARNING::

  S3QL is not able to compensate for any failures of the backend. In
  particular, it is not able reconstruct any data that has been lost
  or corrupted by the backend. The persistence and durability of data
  stored in an S3QL file system is limited and determined by the
  backend alone.
  

On the plus side, if a backend looses or corrupts some of the stored
data, S3QL *will* detect the problem. Missing data will be detected
when running `fsck.s3ql` or when attempting to access the data in the
mounted file system. In the later case you will get an IO Error, and
on unmounting S3QL will warn you that the file system is damaged and
you need to run `fsck.s3ql`.

`fsck.s3ql` will report all the affected files and move them into the
`/lost+found` directory of the file system.

You should be aware that, because of S3QL's data de-duplication
feature, the consequences of a data loss in the backend can be
significantly more severe than you may expect. More concretely, a data
loss in the backend at time *x* may cause data that is written *after*
time *x* to be lost as well. What may happen is this:

#. You store an important file in the S3QL file system.
#. The backend looses the data blocks of this file. As long as you
   do not access the file or run `fsck.s3ql`, S3QL
   is not aware that the data has been lost by the backend.
#. You save an additional copy of the important file in a different
   location on the same S3QL file system.
#. S3QL detects that the contents of the new file are identical to the
   data blocks that have been stored earlier. Since at this point S3QL
   is not aware that these blocks have been lost by the backend, it
   does not save another copy of the file contents in the backend but
   relies on the (presumably) existing blocks instead.
#. Therefore, even though you saved another copy, you still do not
   have a backup of the important file (since both copies refer to the
   same data blocks that have been lost by the backend).

As one can see, this effect becomes the less important the more often
one runs `fsck.s3ql`, since `fsck.s3ql` will make S3QL aware of any
blocks that the backend may have lost. Figuratively, this establishes
a "checkpoint": data loss in the backend that occurred before running
`fsck.s3ql` can not affect any file system operations performed after
running `fsck.s3ql`.

Nevertheless, the recommended way to use S3QL is in combination with a
sufficiently reliable storage backend. In that case none of the above
will ever be a concern.
