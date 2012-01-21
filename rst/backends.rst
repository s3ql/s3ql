.. -*- mode: rst -*-

.. _storage_backends:

==================
 Storage Backends
==================

The following backends are currently available in S3QL:

Google Storage
==============

`Google Storage <http://code.google.com/apis/storage/>`_ is an online
storage service offered by Google. It is the most feature-rich service
supported by S3QL and S3QL offers the best performance when used with
the Google Storage backend.

To use the Google Storage backend, you need to have (or sign up for) a
Google account, and then `activate Google Storage
<http://code.google.com/apis/storage/docs/signup.html>`_ for your
account. The account is free, you will pay only for the amount of
storage and traffic that you actually use. Once you have created the
account, make sure to `activate legacy access
<http://code.google.com/apis/storage/docs/reference/v1/apiversion1.html#enabling>`_.

To create a Google Storage bucket, you can use e.g. the `Google
Storage Manager
<https://sandbox.google.com/storage/>`_. The
storage URL for accessing the bucket in S3QL is then ::

   gs://<bucketname>/<prefix>

Here *bucketname* is the name of the bucket, and *prefix* can be
an arbitrary prefix that will be prepended to all object names used by
S3QL. This allows you to store several S3QL file systems in the same
Google Storage bucket.

Note that the backend login and password for accessing your Google
Storage bucket are not your Google account name and password, but the
*Google Storage developer access key* and *Google Storage developer
secret* that you can manage with the `Google Storage key management
tool
<https://code.google.com/apis/console/#:storage:legacy>`_.

If you would like S3QL to connect using HTTPS instead of standard
HTTP, start the storage url with ``gss://`` instead of ``gs://``. Note
that at this point S3QL does not perform any server certificate
validation (see `issue 267
<http://code.google.com/p/s3ql/issues/detail?id=267>`_).


Amazon S3
=========

`Amazon S3 <http://aws.amazon.com/s3>`_ is the online storage service
offered by `Amazon Web Services (AWS) <http://aws.amazon.com/>`_. To
use the S3 backend, you first need to sign up for an AWS account. The
account is free, you will pay only for the amount of storage and
traffic that you actually use. After that, you need to create a bucket
that will hold the S3QL file system, e.g. using the `AWS Management
Console <https://console.aws.amazon.com/s3/home>`_. For best
performance, it is recommend to create the bucket in the
geographically closest storage region, but not the US Standard
region (see below).

The storage URL for accessing S3 buckets in S3QL has the form ::

    s3://<bucketname>/<prefix>

Here *bucketname* is the name of the bucket, and *prefix* can be
an arbitrary prefix that will be prepended to all object names used by
S3QL. This allows you to store several S3QL file systems in the same
S3 bucket.

Note that the backend login and password for accessing S3 are not the
user id and password that you use to log into the Amazon Webpage, but
the *AWS access key id* and *AWS secret access key* shown under `My
Account/Access Identifiers
<https://aws-portal.amazon.com/gp/aws/developer/account/index.html?ie=UTF8&action=access-key>`_.

If you would like S3QL to connect using HTTPS instead of standard
HTTP, start the storage url with ``s3s://`` instead of ``s3://``. Note
that, as of May 2011, Amazon S3 is faster when accessed using a
standard HTTP connection, and that S3QL does not perform any server
certificate validation (see `issue 267
<http://code.google.com/p/s3ql/issues/detail?id=267>`_).


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


Potential issues when using the US Standard storage region
----------------------------------------------------------

In the US Standard storage region, Amazon S3 does not guarantee read
after create consistency. This means that after a new object has been
stored, requests to read this object may still fail for a little
while. While the file system is mounted, S3QL is able to automatically
handle all issues related to this so-called eventual consistency.
However, problems may arise during the mount process and when the file
system is checked:

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
been stored, so to trigger this problem one would have to unmount and
remount the file system in a very short time window. However, since S3
does not place any upper limit on the length of this window, it is
recommended to not place S3QL buckets in the US Standard storage
region. As of May 2011, all other storage regions provide stronger
consistency guarantees that completely eliminate any of the described
problems.


OpenStack/Swift
===============

OpenStack_ is an open-source cloud server application suite. Swift_ is
the cloud storage module of OpenStack. Swift/OpenStack storage is
offered by many different companies.

The storage URL for the OpenStack backend has the form ::
  
   swift://<hostname>[:<port>]/<container>[/<prefix>]

Note that the storage container must already exist. Most OpenStack
providers offer a web frontend that you can use to create storage
containers. *prefix* can be an arbitrary prefix that will be prepended
to all object names used by S3QL. This allows you to store several
S3QL file systems in the same container.

The OpenStack backend always uses HTTPS connections. Note, however,
that at this point S3QL does not verify the server certificate (cf.
`issue 267 <http://code.google.com/p/s3ql/issues/detail?id=267>`_).

.. _OpenStack: http://www.openstack.org/
.. _Swift: http://openstack.org/projects/storage/


RackSpace CloudFiles
====================

RackSpace_ CloudFiles uses OpenStack internally, so you can use the
OpenStack/Swift backend (see above). The hostname for CloudFiles
containers is ``auth.api.rackspacecloud.com``. Use your normal
RackSpace user name for the backend login, and your RackSpace API key
as the backend passphrase. You can create a storage container for S3QL
using the `Control Panel <https://manage.rackspacecloud.com/>`_ (go to
*Cloud Files* under *Hosting*).

You should note that opinions about RackSpace differ widely among S3QL
users and developers. On the one hand, people praise RackSpace for
their backing of the (open source) OpenStack project. On the other
hand, their heavily advertised "fanatical support" is in practice
often not only `less than helpful
<http://code.google.com/p/s3ql/issues/detail?id=243#c5>`_, but their
support agents also seem to be `downright incompetent
<http://code.google.com/p/s3ql/issues/detail?id=243#c11>`_. However,
there are reports that the support quality increases dramatically once
you are a customer and use the "Live Chat" link when you are logged
into the control panel.

.. _RackSpace: http://www.rackspace.com/


S3 compatible
=============

S3QL is also able to access other, S3 compatible storage services for
which no specific backend exists. Note that when accessing such
services, only the lowest common denominator of available features can
be used, so it is generally recommended to use a service specific
backend instead.

The storage URL for accessing an arbitrary S3 compatible storage
service is ::

   s3c://<hostname>:<port>/<bucketname>/<prefix>

or ::

   s3cs://<hostname>:<port>/<bucketname>/<prefix>

to use HTTPS connections. Note, however, that at this point S3QL does
not verify the server certificate (cf. `issue 267
<http://code.google.com/p/s3ql/issues/detail?id=267>`_).


Local
=====

S3QL is also able to store its data on the local file system. This can
be used to backup data on external media, or to access external
services that S3QL can not talk to directly (e.g., it is possible to
store data over SSH by first mounting the remote system using
`sshfs`_, then using the local backend to store the data in the sshfs
mountpoint).

The storage URL for local storage is ::

   local://<path>
   
Note that you have to write three consecutive slashes to specify an
absolute path, e.g. `local:///var/archive`. Also, relative paths will
automatically be converted to absolute paths before the authentication
file is read, i.e. if you are in the `/home/john` directory and try to
mount `local://bucket`, the corresponding section in the
authentication file must match the storage url
`local:///home/john/bucket`.

SSH/SFTP
========

Previous versions of S3QL included an SSH/SFTP backend. With newer
S3QL versions, it is recommended to instead combine the local backend
with `sshfs <http://fuse.sourceforge.net/sshfs.html>`_ (cf. :ref:`ssh_tipp`).

