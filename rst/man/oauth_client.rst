.. -*- mode: rst -*-

=====================
The |command| command
=====================


Synopsis
========

::

   s3ql_oauth_client [options]
  
Description
===========

.. include:: ../include/about.rst

The |command| command may be used to obtain OAuth2 authentication
tokens for use with Google Storage. It requests "user code" from
Google which has to be pasted into the browser to complete the
authentication process interactively. Once authentication in the
browser has been completed, |command| displays the OAuth2 refresh
token.

When combined with the special username ``oauth2``, the refresh token
can be used as a backend passphrase when using the Google Storage S3QL
backend.
  

Options
=======

The |command| command accepts the following options:

.. pipeinclude:: python ../../bin/s3ql_oauth_client --help
   :start-after: show this help message and exit

.. include:: ../include/postman.rst

.. |command| replace:: :program:`s3ql_oauth_client`

