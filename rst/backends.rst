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

Furthermore, every S3QL commands that accepts a storage url also
accepts a :cmdopt:`--backend-options` parameter than can be used to
pass backend-specific options to the backend module. The available
options are documented with the respective backends below.

All storage backends respect the :envvar:`!http_proxy` (for plain HTTP
connections) and :envvar:`!https_proxy` (for SSL connections)
environment variables.

.. note::

   Storage backends are not necessarily compatible. Don't expect that
   you can e.g. the data stored by the local backend into Amazon S3
   and then access it with the S3 backend. If you want to copy file
   systems from one backend to another, you need to use the
   :file:`clone_fs.py` script (from the :file:`contrib` directory in
   the S3QL tarball).

Google Storage
==============

.. program:: gs_backend

`Google Storage <http://code.google.com/apis/storage/>`_ is an online
storage service offered by Google. To use the Google Storage backend,
you need to have (or sign up for) a Google account, and then `activate
Google Storage <http://code.google.com/apis/storage/docs/signup.html>`_
for your account. The account is free, you will pay only for the
amount of storage and traffic that you actually use. There are two
ways to access Google storage:

#. Use S3-like authentication. To do this, first `set a  default
   project
   <https://developers.google.com/storage/docs/migrating#defaultproj>`_.
   Then use the `key management tool
   <https://code.google.com/apis/console/#:storage:legacy>`_ to
   retrieve your *Google Storage developer access key* and *Google
   Storage developer secret* and use that as backend login and backend
   password.

#. Use OAuth2 authentication. In this case you need to use ``oauth2``
   as the backend login, and a valid OAuth2 refresh token as the
   backend password. To obtain a refresh token, you can use the
   :ref:`s3ql_oauth_client <oauth_client>` program. It will instruct
   you to open a specific URL in your browser, enter a code and
   authenticate with your Google account. Once this procedure is
   complete, :ref:`s3ql_oauth_client <oauth_client>` will print out
   the refresh token. Note that you need to do this procedure only
   once, the refresh token will remain valid until you explicitly
   revoke it.

To create a Google Storage bucket, you can use e.g. the `Google
Storage Manager`_. The storage URL for accessing the bucket in S3QL is
then ::

   gs://<bucketname>/<prefix>

Here *bucketname* is the name of the bucket, and *prefix* can be an
arbitrary prefix that will be prepended to all object names used by
S3QL. This allows you to store several S3QL file systems in the same
Google Storage bucket.

The Google Storage backend accepts the following backend options:

.. option:: no-ssl

   Disable encrypted (https) connections and use plain HTTP instead.

.. option:: ssl-ca-path=<path>

   Instead of using the system's default certificate store, validate
   the server certificate against the specified CA
   certificates. :var:`<path>` may be either a file containing
   multiple certificates, or a directory containing one certificate
   per file.

.. option:: tcp-timeout

   Specifies the timeout used for TCP connections. If no data can be
   exchanged with the remote server for longer than this period, the
   TCP connection is closed and re-established (default: 20 seconds).

.. _`Google Storage Manager`: https://sandbox.google.com/storage/

Amazon S3
=========

.. program:: s3_backend

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

The Amazon S3 backend accepts the following backend options:

.. option:: no-ssl

   Disable encrypted (https) connections and use plain HTTP instead.

.. option:: ssl-ca-path=<path>

   Instead of using the system's default certificate store, validate
   the server certificate against the specified CA
   certificates. :var:`<path>` may be either a file containing
   multiple certificates, or a directory containing one certificate
   per file.

.. option:: tcp-timeout

   Specifies the timeout used for TCP connections. If no data can be
   exchanged with the remote server for longer than this period, the
   TCP connection is closed and re-established (default: 20 seconds).

.. option:: sse

    Enable server side encryption. Both costs & benefits of S3 server
    side encryption are probably rather small, and this option does
    *not* affect any client side encryption performed by S3QL itself.

.. option:: rrs

   Enable reduced redundancy storage for newly created objects.

   When enabling this option, it is strongly recommended to
   periodically run :ref:`s3ql_verify <s3ql_verify>`, because objects
   that are lost by the storage backend may cause subsequent data loss
   even later in time due to the data de-duplication feature of S3QL (see
   :ref:`backend_reliability` for details).


.. _openstack_backend:

OpenStack/Swift
===============

.. program:: swift_backend

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

The OpenStack backend accepts the following backend options:

.. option:: no-ssl

   Use plain HTTP to connect to the authentication server. This option
   does not directly affect the connection to the storage
   server. Whether HTTPS or plain HTTP is used to connect to the
   storage server is determined by the authentication server.

.. option:: ssl-ca-path=<path>

   Instead of using the system's default certificate store, validate
   the server certificate against the specified CA
   certificates. :var:`<path>` may be either a file containing
   multiple certificates, or a directory containing one certificate
   per file.

.. option:: tcp-timeout

   Specifies the timeout used for TCP connections. If no data can be
   exchanged with the remote server for longer than this period, the
   TCP connection is closed and re-established (default: 20 seconds).

.. option:: disable-expect100

   If this option is specified, S3QL does not use the ``Expect:
   continue`` header (cf. `RFC2616, section 8.2.3`__) when uploading
   data to the server. This can be used to work around broken storage
   servers that don't fully support HTTP 1.1, but may decrease
   performance as object data will be transmitted to the server more
   than once in some circumstances.

.. __: http://tools.ietf.org/html/rfc2616#section-8.2.3
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

The Rackspace backend accepts the same backend options as the
:ref:`OpenStack backend <openstack_backend>`.

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

.. program:: s3c_backend

The S3 compatible backend allows S3QL to access any storage service
that uses the same protocol as Amazon S3. The storage URL has the form ::

   s3c://<hostname>:<port>/<bucketname>/<prefix>

Here *bucketname* is the name of an (existing) bucket, and *prefix*
can be an arbitrary prefix that will be prepended to all object names
used by S3QL. This allows you to store several S3QL file systems in
the same bucket.

The S3 compatible backend accepts the following backend options:

.. option:: no-ssl

   Disable encrypted (https) connections and use plain HTTP instead.

.. option:: ssl-ca-path=<path>

   Instead of using the system's default certificate store, validate
   the server certificate against the specified CA
   certificates. :var:`<path>` may be either a file containing
   multiple certificates, or a directory containing one certificate
   per file.

.. option:: tcp-timeout

   Specifies the timeout used for TCP connections. If no data can be
   exchanged with the remote server for longer than this period, the
   TCP connection is closed and re-established (default: 20 seconds).

.. option:: disable-expect100

   If this option is specified, S3QL does not use the ``Expect:
   continue`` header (cf. `RFC2616, section 8.2.3`__) when uploading
   data to the server. This can be used to work around broken storage
   servers that don't fully support HTTP 1.1, but may decrease
   performance as object data will be transmitted to the server more
   than once in some circumstances.

.. __: http://tools.ietf.org/html/rfc2616#section-8.2.3

.. option:: dumb-copy

   If this option is specified, S3QL assumes that a COPY request to
   the storage server has succeeded as soon as the server returns a
   ``200 OK`` status. The `S3 COPY API`_ specifies that the
   storage server may still return an error in the request body (see
   the `copy proposal`__ for the rationale), so this
   option should only be used if you are certain that your storage
   server only returns ``200 OK`` when the copy operation has been
   completely and successfully carried out. Using this option may be
   neccessary if your storage server does not return a valid response
   body for a succesfull copy operation.

.. _`S3 COPY API`: http://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectCOPY.html
.. __: https://doc.s3.amazonaws.com/proposals/copy.html


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

The local backend does not accept any backend options.

.. _sshfs: http://fuse.sourceforge.net/sshfs.html
