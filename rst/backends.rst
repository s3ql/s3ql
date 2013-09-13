.. -*- mode: rst -*-

.. _storage_backends:

==================
 Storage Backends
==================

S3QL supports different *backends* to store data at different service
providers and using different protocols. A *storage url* specifies a
backend together with some backend-specific information and uniquely
identifies an S3QL file system. The form of the storage url depends on
the backend and is described for every backend below.

All storage backends respect the ``http_proxy`` and ``https_proxy``
environment variables.

Google Storage
==============

`Google Storage <http://code.google.com/apis/storage/>`_ is an online
storage service offered by Google. To use the Google Storage backend,
you need to have (or sign up for) a Google account, and then `activate
Google Storage <http://code.google.com/apis/storage/docs/signup.html>`_
for your account. The account is free, you will pay only for the
amount of storage and traffic that you actually use. Once you have
created the account, make sure to `activate legacy access
<http://code.google.com/apis/storage/docs/reference/v1/apiversion1.html#enabling>`_.

To create a Google Storage bucket, you can use e.g. the `Google
Storage Manager <https://sandbox.google.com/storage/>`_. The storage
URL for accessing the bucket in S3QL is then ::

   gs://<bucketname>/<prefix>

Here *bucketname* is the name of the bucket, and *prefix* can be
an arbitrary prefix that will be prepended to all object names used by
S3QL. This allows you to store several S3QL file systems in the same
Google Storage bucket.

Note that the backend login and password for accessing your Google
Storage bucket are not your Google account name and password, but the
*Google Storage developer access key* and *Google Storage developer
secret* that you can manage with the `Google Storage key management
tool <https://code.google.com/apis/console/#:storage:legacy>`_.


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
geographically closest storage region, but not the US Standard region
(see :ref:`durability` for the reason).

The storage URL for accessing S3 buckets in S3QL has the form ::

    s3://<bucketname>/<prefix>

Here *bucketname* is the name of the bucket, and *prefix* can be an
arbitrary prefix that will be prepended to all object names used by
S3QL. This allows you to store several S3QL file systems in the same
S3 bucket.

Note that the backend login and password for accessing S3 are not the
user id and password that you use to log into the Amazon Webpage, but
the *AWS access key id* and *AWS secret access key* shown under `My
Account/Access Identifiers
<https://aws-portal.amazon.com/gp/aws/developer/account/index.html?ie=UTF8&action=access-key>`_.


Reduced Redundancy Storage (RRS)
--------------------------------

S3QL does not allow the use of `reduced redundancy storage
<http://aws.amazon.com/s3/#protecting>`_. The reason for that is a
combination of three factors:

* RRS has a relatively low reliability, on average you lose one
  out of every ten-thousand objects a year. So you can expect to
  occasionally lose some data.

* When `fsck.s3ql` asks S3 for a list of the stored objects, this list
  includes even those objects that have been lost. Therefore
  `fsck.s3ql` *can not detect lost objects* and lost data will only
  become apparent when you try to actually read from a file whose data
  has been lost. This is a (very unfortunate) peculiarity of Amazon
  S3.

* Due to the data de-duplication feature of S3QL, unnoticed lost
  objects may cause subsequent data loss later in time (see
  :ref:`backend_reliability` for details).


OpenStack/Swift
===============

OpenStack_ is an open-source cloud server application suite. Swift_ is
the cloud storage module of OpenStack. Swift/OpenStack storage is
offered by many different companies.

There are two different storage URL for the OpenStack backend that
make use of different authentication APIs. For legacy (v1)
authentication, the storage URL is ::

   swift://<hostname>[:<port>]/<container>[/<prefix>]

for keystore (v2) authentication, the storage URL is ::

   swiftks://<hostname>[:<port>]/<region>:<container>[/<prefix>]

Note that when using keystore authentication, you can (and have to)
specify the storage region of the container as well.

In both cases, *hostname* name should be the name of the
authentication server.  The storage container must already exist (most
OpenStack providers offer either a web frontend or a command line tool
for creating containers). *prefix* can be an arbitrary prefix that
will be prepended to all object names used by S3QL, which can be used
to store multiple S3QL file systems in the same container.

When using legacy authentication, the backend login and password
correspond to the OpenStack username and API Access Key. When using
keystore authentication, the backend password is your regular
OpenStack password and the backend login combines you OpenStack
username and tenant name in the form `<tenant>:<user>`. If no tenant
is required, the OpenStack username alone may be used as backend
login.

.. _OpenStack: http://www.openstack.org/
.. _Swift: http://openstack.org/projects/storage/


Rackspace CloudFiles
====================

Rackspace_ CloudFiles uses OpenStack_ internally, so it is possible to
just use the OpenStack/Swift backend (see above) with
``auth.api.rackspacecloud.com`` as the host name. For convenince,
there is also a special ``rackspace`` backend that uses a storage URL
of the form ::

   rackspace://<region>/<container>[/<prefix>]

The storage container must already exist in the selected
region. *prefix* can be an arbitrary prefix that will be prepended to
all object names used by S3QL and can be used to store several S3QL
file systems in the same container.

You can create a storage container for S3QL using the `Cloud Control
Panel <https://mycloud.rackspace.com/>`_ (click on *Files* in the
topmost menu bar).


.. NOTE::

   As of January 2012, Rackspace does not give any durability or
   consistency guarantees (see :ref:`durability` for why this is
   important).  However, Rackspace support agents seem prone to claim
   very high guarantees.  Unless explicitly backed by their terms of
   service, any such statement should thus be viewed with
   suspicion. S3QL developers have also `repeatedly experienced
   <http://www.rath.org/Tales%20from%20the%20Rackspace%20Support>`_
   similar issues with the credibility and competence of the Rackspace
   support.


.. _Rackspace: http://www.rackspace.com/


S3 compatible
=============

The S3 compatible backend allows S3QL to access any storage service
that uses the same protocol as Amazon S3. The storage URL has the form ::

   s3c://<hostname>:<port>/<bucketname>/<prefix>

Here *bucketname* is the name of an (existing) bucket, and *prefix*
can be an arbitrary prefix that will be prepended to all object names
used by S3QL. This allows you to store several S3QL file systems in
the same bucket.


Local
=====

S3QL is also able to store its data on the local file system. This can
be used to backup data on external media, or to access external
services that S3QL can not talk to directly (e.g., it is possible to
store data over SSH by first mounting the remote system using sshfs_
and then using the local backend to store the data in the sshfs
mountpoint).

The storage URL for local storage is ::

   local://<path>
   
Note that you have to write three consecutive slashes to specify an
absolute path, e.g. `local:///var/archive`. Also, relative paths will
automatically be converted to absolute paths before the authentication
file (see :ref:`authinfo`) is read, i.e. if you are in the
`/home/john` directory and try to mount `local://s3ql`, the
corresponding section in the authentication file must match the
storage url `local:///home/john/s3ql`.


.. _sshfs: http://fuse.sourceforge.net/sshfs.html
