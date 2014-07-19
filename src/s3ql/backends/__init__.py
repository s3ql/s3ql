'''
backends/__init__.py - this file is part of S3QL (http://s3ql.googlecode.com)

Copyright © 2008 Nikolaus Rath <Nikolaus.org>

This program can be distributed under the terms of the GNU GPLv3.
'''

from ..logging import logging, LOG_ONCE # Ensure use of custom logger class
from ..common import QuietError, get_ssl_context
from . import local, s3, gs, s3c, swift, rackspace, swiftks
from .common import (AuthenticationError, DanglingStorageURLError, AuthorizationError,
                     NoSuchObject, BetterBackend, ChecksumError)
from getpass import getpass
import configparser
import re
import stat
import sys
import os

log = logging.getLogger(__name__)

#: Mapping from storage URL prefixes to backend classes
prefix_map = { 's3': s3.Backend,
               'local': local.Backend,
               'gs': gs.Backend,
               's3c': s3c.Backend,
               'swift': swift.Backend,
               'swiftks': swiftks.Backend,
               'rackspace': rackspace.Backend }

__all__ = [ 'common' ] + list(prefix_map.keys())


def get_backend(options, plain=False):
    '''Return backend for given storage-url

    If *plain* is true, don't attempt to unlock and don't wrap into
    BetterBackend.
    '''

    return get_backend_factory(options, plain)()


def get_backend_factory(options, plain=False):
    '''Return factory producing backend objects for given storage-url

    If *plain* is true, don't attempt to unlock and don't wrap into
    BetterBackend.
    '''

    hit = re.match(r'^([a-zA-Z0-9]+)://', options.storage_url)
    if not hit:
        raise QuietError('Unable to parse storage url "%s"' % options.storage_url,
                         exitcode=2)

    backend = hit.group(1)
    try:
        backend_class = prefix_map[backend]
    except KeyError:
        raise QuietError('No such backend: %s' % backend, exitcode=11)

    # Read authfile
    config = configparser.ConfigParser()
    if os.path.isfile(options.authfile):
        mode = os.stat(options.authfile).st_mode
        if mode & (stat.S_IRGRP | stat.S_IROTH):
            raise QuietError("%s has insecure permissions, aborting." % options.authfile,
                             exitcode=12)
        config.read(options.authfile)

    backend_login = None
    backend_passphrase = None
    fs_passphrase = None
    for section in config.sections():
        def getopt(name):
            try:
                return config.get(section, name)
            except configparser.NoOptionError:
                return None

        pattern = getopt('storage-url')

        if not pattern or not options.storage_url.startswith(pattern):
            continue

        backend_login = getopt('backend-login') or backend_login
        backend_passphrase = getopt('backend-password') or backend_passphrase
        fs_passphrase = getopt('fs-passphrase') or fs_passphrase
        if getopt('fs-passphrase') is None and getopt('bucket-passphrase') is not None:
            fs_passphrase = getopt('bucket-passphrase')
            log.warning("Warning: the 'bucket-passphrase' configuration option has been "
                        "renamed to 'fs-passphrase'! Please update your authinfo file.")

    if not backend_login and backend_class.needs_login:
        if sys.stdin.isatty():
            backend_login = getpass("Enter backend login: ")
        else:
            backend_login = sys.stdin.readline().rstrip()

    if not backend_passphrase and backend_class.needs_login:
        if sys.stdin.isatty():
            backend_passphrase = getpass("Enter backend passphrase: ")
        else:
            backend_passphrase = sys.stdin.readline().rstrip()

    ssl_context = get_ssl_context(options)
    if ssl_context is None:
        proxy_env = 'http_proxy'
    else:
        proxy_env = 'https_proxy'

    if proxy_env in os.environ:
        proxy = os.environ[proxy_env]
        hit = re.match(r'^(https?://)?([a-zA-Z0-9.-]+)(:[0-9]+)?/?$', proxy)
        if not hit:
            raise QuietError('Unable to parse proxy setting %s=%r' %
                             (proxy_env, proxy), exitcode=13)

        if hit.group(1) == 'https://':
            log.warning('HTTPS connection to proxy is probably pointless and not supported, '
                        'will use standard HTTP', extra=LOG_ONCE)

        if hit.group(3):
            proxy_port = int(hit.group(3)[1:])
        else:
            proxy_port = 80

        proxy_host = hit.group(2)
        log.info('Using CONNECT proxy %s:%d', proxy_host, proxy_port,
                 extra=LOG_ONCE)
        proxy = (proxy_host, proxy_port)
    else:
        proxy = None

    backend = backend_class(options.storage_url, backend_login, backend_passphrase,
                            ssl_context, proxy=proxy)
    try:
        # Do not use backend.lookup(), this would use a HEAD request and
        # not provide any useful error messages if something goes wrong
        # (e.g. wrong credentials)
        backend.fetch('s3ql_passphrase')

    except AuthenticationError:
        raise QuietError('Invalid credentials (or skewed system clock?).',
                         exitcode=14)

    except AuthorizationError:
        raise QuietError('No permission to access backend.',
                         exitcode=15)

    except DanglingStorageURLError as exc:
        raise QuietError(str(exc), exitcode=16)

    except NoSuchObject:
        encrypted = False

    else:
        encrypted = True

    finally:
        backend.close()

    if plain:
        return lambda: backend_class(options.storage_url, backend_login, backend_passphrase,
                                     ssl_context, proxy=proxy)

    if encrypted and not fs_passphrase:
        if sys.stdin.isatty():
            fs_passphrase = getpass("Enter file system encryption passphrase: ")
        else:
            fs_passphrase = sys.stdin.readline().rstrip()
    elif not encrypted:
        fs_passphrase = None

    if fs_passphrase is not None:
        fs_passphrase = fs_passphrase.encode('utf-8')

    if hasattr(options, 'compress'):
        compress = options.compress
    else:
        compress = ('lzma', 2)

    if not encrypted:
        return lambda: BetterBackend(None, compress,
                                    backend_class(options.storage_url, backend_login,
                                                  backend_passphrase, ssl_context=ssl_context,
                                                  proxy=proxy))

    tmp_backend = BetterBackend(fs_passphrase, compress, backend)

    try:
        data_pw = tmp_backend['s3ql_passphrase']
    except ChecksumError:
        raise QuietError('Wrong file system passphrase', exitcode=17)
    finally:
        tmp_backend.close()

    return lambda: BetterBackend(data_pw, compress,
                                 backend_class(options.storage_url, backend_login,
                                               backend_passphrase, ssl_context=ssl_context,
                                               proxy=proxy))
