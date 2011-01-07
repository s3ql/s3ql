.. -*- mode: rst -*-

==================
 Storage Backends
==================

S3QL can use different protocols to store the file system data.
Independent of the backend that you use, the place where your file
system data is being stored is called a *bucket*. (This is mostly for
historical reasons, since initially S3QL supported only the Amazon S3
backend).


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


Nevertheless, (as said at the beginning) the recommended way to use
S3QL is in combination with a sufficiently reliable storage backend.
In that case none of the above will ever be a concern.


The `authinfo` file
===================

Most backends first try to read the file `~/.s3ql/authinfo` to determine
the username and password for connecting to the remote host. If this
fails, both username and password are read from the terminal.

The `authinfo` file has to contain entries of the form ::

  backend <backend> machine <host> login <user> password <password>

So to use the login `joe` with password `jibbadup` when using the FTP
backend to connect to the host `backups.joesdomain.com`, you would
specify ::

  backend ftp machine backups.joesdomain.com login joe password jibbadup

  
Consistency Guarantees
======================
  
The different backends provide different types of *consistency
guarantees*. Informally, a consistency guarantee tells you how fast
the backend will apply changes to the stored data. 

S3QL defines the following three levels:

* **Read-after-Write Consistency.** This is the strongest consistency
  guarantee. If a backend offers read-after-write consistency, it
  guarantees that as soon as you have committed any changes to the
  backend, subsequent requests will take into account these changes.

* **Read-after-Create Consistency.** If a backend provides only
  read-after-create consistency, only the creation of a new object is
  guaranteed to be taken into account for subsequent requests. This
  means that, for example, if you overwrite data in an existing
  object, subsequent requests may still return the old data for a
  certain period of time.

* **Eventual consistency.** This is the lowest consistency level.
  Basically, any changes that you make to the backend may not be
  visible for a certain amount of time after the change has been made.
  However, you are guaranteed that no change will be lost. All changes
  will *eventually* become visible.

  .


As long as your backend provides read-after-write or read-after-create
consistency, you do not have to worry about consistency guarantees at
all. However, if you plan to use a backend with only eventual
consistency, you have to be a bit careful in some situations.


.. _eventual_consistency:

Dealing with Eventual Consistency
---------------------------------

.. NOTE::

  The following applies only to storage backends that do not provide
  read-after-create or read-after-write consistency. Currently,
  this is only the Amazon S3 backend *if used with the US-Standard
  storage region*. If you use a different storage backend, or the S3
  backend with a different storage region, this section does not apply
  to you.

While the file system is mounted, S3QL is able to automatically handle
all issues related to the weak eventual consistency guarantee.
However, some issues may arise during the mount process and when the
file system is checked.

Suppose that you mount the file system, store some new data, delete
some old data and unmount it again. Now remember that eventual
consistency means that there is no guarantee that these changes will
be visible immediately. At least in theory it is therefore possible
that if you mount the file system again, S3QL does not see any of the
changes that you have done and presents you an "old version" of the
file system without them. Even worse, if you notice the problem and
unmount the file system, S3QL will upload the old status (which S3QL
necessarily has to consider as current) and thereby permanently
override the newer version (even though this change may not become
immediately visible either).

The same problem applies when checking the file system. If the backend
provides S3QL with only partially updated data, S3QL has no way to
find out if this a real consistency problem that needs to be fixed or
if it is only a temporary problem that will resolve itself
automatically (because there are still changes that have not become
visible yet).

While this may seem to be a rather big problem, the likelihood of it
to occur is rather low. In practice, most storage providers rarely
need more than a few seconds to apply incoming changes, so to trigger
this problem one would have to unmount and remount the file system in
a very short time window. Many people therefore make sure that they
wait a few minutes between successive mounts (or file system checks)
and decide that the remaining risk is negligible.

Nevertheless, the eventual consistency guarantee does not impose an
upper limit on the time that it may take for change to become visible.
Therefore there is no "totally safe" waiting time that would totally
eliminate this problem; a theoretical possibility always remains.



The Amazon S3 Backend
=====================

To store your file system in an Amazon S3 bucket, use a storage URL of
the form `s3://<bucketname>`. Bucket names must conform to the `S3
Bucket Name Restrictions`_.

The S3 backend offers exceptionally strong reliability guarantees. As
of August 2010, Amazon guarantees a durability of 99.999999999% per
year. In other words, if you store a thousand million objects then on
average you would loose less than one object in a hundred years. 

The Amazon S3 backend provides read-after-create consistency for the
EU, Asia-Pacific and US-West storage regions. *For the US-Standard
storage region, Amazon S3 provides only eventual consistency* (please
refer to :ref:`eventual_consistency` for information about
what this entails).

When connecting to Amazon S3, S3QL uses an unencrypted HTTP
connection, so if you want your data to stay confidential, you have
to create the S3QL file system with encryption (this is also the default).

When reading the authentication information for the S3 backend from
the `authinfo` file, the `host` field is ignored, i.e. the first entry
with `s3` as a backend will be used. For example ::

   backend s3 machine any login myAWSaccessKeyId password myAwsSecretAccessKey

Note that the bucket names come from a global pool, so chances are
that your favorite name has already been taken by another S3 user.
Usually a longer bucket name containing some random numbers, like
`19283712_yourname_s3ql`, will work better. 

If you do not already have one, you need to obtain an Amazon S3
account from `Amazon AWS <http://aws.amazon.com/>`_. The account is
free, you will pay only for the amount of storage that you actually
use.

Note that the login and password for accessing S3 are not the user id
and password that you use to log into the Amazon Webpage, but the "AWS
access key id" and "AWS secret access key" shown under `My
Account/Access Identifiers
<https://aws-portal.amazon.com/gp/aws/developer/account/index.html?ie=UTF8&action=access-key>`_.

.. _`S3 Bucket Name Restrictions`: http://docs.amazonwebservices.com/AmazonS3/2006-03-01/dev/BucketRestrictions.html

.. NOTE::

   S3QL also allows you to use `reduced redundancy storage
   <http://aws.amazon.com/s3/#protecting>`_ by using ``s3rr://``
   instead of ``s3://`` in the storage url. However, this not
   recommended. The reason is a combination of three factors:

   * RRS has a relatively low reliability, on average you loose one
     out of every ten-thousand objects a year. So you can expect to
     occasionally loose some data.

   * When `fsck.s3ql` asks Amazon S3 for a list of the stored objects,
     this list includes even those objects that have been lost.
     Therefore `fsck.s3ql` *can not detect lost objects* and lost data
     will only become apparent when you try to actually read from a
     file whose data has been lost. This is a (very unfortunate)
     peculiarity of Amazon S3.

   * Due to the data de-duplication feature of S3QL, unnoticed lost
     objects may cause subsequent data loss later in time (see `On
     Backend Reliability`_ for details).

   In other words, you should really only store an S3QL file system
   using RRS if you know exactly what you are getting into.
     



The Local Backend
=================

The local backend stores file system data in a directory on your
computer. The storage URL for the local backend has the form
`local://<path>`. Note that you have to write three consecutive
slashes to specify an absolute path, e.g. `local:///var/archive`.

The local backend provides read-after-write consistency.

The SFTP Backend
================

The SFTP backend uses the SFTP protocol, which is a file transfer
protocol similar to ftp, but uses an encrypted SSH connection. 
It provides read-after-write consistency.

Note that the SFTP backend is rather slow and has not been tested
as extensively as the S3 and Local backends.

The storage URL for SFTP connections has the form ::

  sftp://<host>[:port]/<path>

The SFTP backend will always ask you for a password if you haven't
defined one in `~/.s3ql/authinfo`. However, public key authentication
is tried first and the password will only be used if the public key
authentication fails.

The public and private keys will be read from the standard files in
`~/.ssh/`. Note that S3QL will refuse to connect to a computer with
unknown host key; to add the key to your local keyring you have to
establish a connection to that computer with the standard SSH command
line programs first.



