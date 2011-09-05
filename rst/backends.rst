.. -*- mode: rst -*-

==================
 Storage Backends
==================

The following backends are currently available in S3QL:


Amazon S3
=========

`Amazon S3 <http://aws.amazon.com/s3>`_ is the online storage service
offered by `Amazon Web Services (AWS) <http://aws.amazon.com/>`_. To
use the S3 backend, you first need to sign up for an AWS account. The
account is free, you will pay only for the amount of storage that you
actually use. After that, you need to create a bucket that will hold
the S3QL file system, e.g. using the `AWS Management Console`_. For
best performance, it is recommend to create the bucket in the
geographically closest storage region, but not the classic/US-east
region (see below).

The storage URL for accessing S3 buckets in S3QL has the form ::

    s3://<bucketname>/<prefix>

Here *bucketname* is the name of the bucket, and *prefix* can be
an arbitrary prefix that will be prepended to all object names used by
S3QL. This allows you to store several S3QL file systems in the same
S3 bucket.

Note that the backend login and password for accessing S3 are not the
user id and password that you use to log into the Amazon Webpage, but
the "AWS access key id" and "AWS secret access key" shown under `My
Account/Access Identifiers
<https://aws-portal.amazon.com/gp/aws/developer/account/index.html?ie=UTF8&action=access-key>`_.

If you would like S3QL to connect using HTTPS instead of standard
HTTP, start the storage url with ``s3s://` instead of ``s3://``. Note
that, as of May 2011, Amazon S3 is faster when accessed using a
standard HTTP connection, and that S3QL does not perform any server
certificate validation (see `issue xx`_).


Reduced Redundancy Storage (RRS)
--------------------------------

S3QL does not allow the use of `reduced redundancy storage
<http://aws.amazon.com/s3/#protecting>`_. The reason for that is a
combination of three factors:

* RRS has a relatively low reliability, on average you loose one
  out of every ten-thousand objects a year. So you can expect to
  occasionally loose some data.

* When `fsck.s3ql` asks S3 for a list of the stored objects, this list
  includes even those objects that have been lost. Therefore
  `fsck.s3ql` *can not detect lost objects* and lost data will only
  become apparent when you try to actually read from a file whose data
  has been lost. This is a (very unfortunate) peculiarity of Amazon
  S3.

* Due to the data de-duplication feature of S3QL, unnoticed lost
  objects may cause subsequent data loss later in time (see
  :ref:`backend_reliability` for details).


Potential issues when using the *classic* storage region
--------------------------------------------------------

In the *classic* (or US east) storage region, Amazon S3 does not
guarantee read after create consistency. This means that after a new
object has been stored, requests to read this object may still fail
for a little while. While the file system is mounted, S3QL is able
to automatically handle all issues related to this so-called
eventual consistency. However, problems may arise during the mount
process and when the file system is checked:

Suppose that you mount the file system, store some new data, delete
some old data and unmount it again. Now there is no guarantee that
these changes will be visible immediately. At least in theory it is
therefore possible that if you mount the file system again, S3QL
does not see any of the changes that you have done and presents you
an "old version" of the file system without them. Even worse, if you
notice the problem and unmount the file system, S3QL will upload the
old status (which S3QL necessarily has to consider as current) and
thereby permanently override the newer version (even though this
change may not become immediately visible either).

The same problem applies when checking the file system. If S3
provides S3QL with only partially updated data, S3QL has no way to
find out if this a real consistency problem that needs to be fixed or
if it is only a temporary problem that will resolve itself
automatically (because there are still changes that have not become
visible yet).

The likelihood of this to happen is rather low. In practice, most
objects are ready for retrieval just a few seconds after they have
been stored, so to trigger this problem one would have to unmount
and remount the file system in a very short time window. However,
since S3 does not place any upper limit on the length of this
window, it is recommended to not place S3QL buckets in the
classic/US-east storage region. As of May 2011, all other storage
regions provide stronger consistency guarantees that completely
eliminate any of the described problems.



Local
=====

The local backend stores file system data in a directory on your
computer. The storage URL for the local backend has the form
`local://<path>`. Note that you have to write three consecutive
slashes to specify an absolute path, e.g. `local:///var/archive`.

The local backend provides read-after-write consistency.


SSH/SFTP
========

Previous versions of S3QL included an SSH/SFTP backend. With newer
S3QL versions, it is recommended to instead combine the local backend
with `sshfs <http://fuse.sourceforge.net/sshfs.html>`_ (cf. :ref:`ssh_tipp`).

