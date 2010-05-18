'''
__init__.py - this file is part of S3QL (http://s3ql.googlecode.com)

Copyright (C) 2010 Nikolaus Rath <Nikolaus@rath.org>
Copyright (C) 2010 Ron Knapp <ron.siesta@gmail.com>

This program can be distributed under the terms of the GNU LGPL.
'''

from __future__ import division, print_function, absolute_import

from .common import AbstractConnection, AbstractBucket
import logging
import errno
import shutil
import cPickle as pickle
import os
import paramiko

log = logging.getLogger("backend.sftp")


class Connection(AbstractConnection):

    def __init__(self, host, port, login, password):
        super(Connection, self).__init__()

        self.port = port or 22
        self.host = host
        self.login = login
        self.password = password

        self._client = None
        self.sftp = None
        self.setup_ssh_connection()

    def setup_ssh_connection(self):

        self._client = paramiko.SSHClient()
        # Probably not a good idea to do this by default
        #self._client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        self._client.load_host_keys(os.path.join(os.environ['HOME'], '.ssh', 'known_hosts'))
        self._client.connect(self.host, port=self.port, username=self.login, password=self.password)
        self.sftp = self._client.open_sftp()

        # We don't want the connection to time out
        self._client.get_transport().set_keepalive(300)

    def __contains__(self, entry):
        try:
            self.sftp.stat(entry)
        except IOError as exc:
            if exc.errno == errno.ENOENT:
                return False
            else:
                raise
        else:
            return True

    def delete_bucket(self, name, recursive=False):
        """Delete bucket"""

        if name not in self:
            raise KeyError('Bucket directory does not exist on remote host')

        if recursive:
            for item in self.sftp.listdir(name):
                if not (item.endswith('.dat') or item.endswith('.meta')):
                    log.warn('Unknown file in bucket directory: %s', item)
                    continue
                self.sftp.remove(os.path.join(name, item))

        self.sftp.rmdir(name)

    def create_bucket(self, name, passphrase=None, compression='lzma'):
        """Create and return bucket"""

        self.sftp.mkdir(name)
        return self.get_bucket(name, passphrase, compression)

    def get_bucket(self, name, passphrase=None, compression='lzma'):
        """Return Bucket instance for the bucket `name`
        
        Raises `KeyError` if the bucket does not exist.
        """
        if name not in self:
            raise KeyError('Bucket %s does not exist' % name)

        return Bucket(self, name, passphrase, compression)

    def close(self):
        self._client.close()

    def prepare_fork(self):
        self._client.close()

    def finish_fork(self):
        self.setup_ssh_connection()

class Bucket(AbstractBucket):

    def __init__(self, conn, name, passphrase, compression):
        super(Bucket, self).__init__(passphrase, compression)
        self.conn = conn
        self.name = name

    def __str__(self):
        if self.passphrase:
            return '<encrypted sftp bucket, name=%r>' % self.name
        else:
            return '<sftp bucket, name=%r>' % self.name

    def clear(self):
        for name in self.conn.sftp.listdir(self.name):
            if not (name.endswith('.dat') or name.endswith('.meta')):
                log.warn('Unknown file in bucket directory: %s', name)
                continue
            self.conn.sftp.remove(os.path.join(self.name, name))

    def contains(self, key):
        return os.path.join(self.name, escape(key) + '.dat') in self.conn

    def raw_lookup(self, key):
        filename = os.path.join(self.name, escape(key))
        try:
            src = self.conn.sftp.open(filename + '.meta', 'rb')
            return pickle.load(src)
        except IOError as exc:
            if exc.errno == errno.ENOENT:
                raise KeyError('Key %r not in bucket' % key)
            else:
                raise

    def delete(self, key, force=False):
        filename = os.path.join(self.name, escape(key))

        try:
            self.conn.sftp.remove(filename + '.dat')
            self.conn.sftp.remove(filename + '.meta')
        except IOError as exc:
            if exc.errno == errno.ENOENT:
                if force:
                    pass
                else:
                    raise KeyError('Key %r not in bucket' % key)
            else:
                raise


    def list(self, prefix=''):
        for name in self.conn.sftp.listdir(self.name):
            if not name.endswith('.dat'):
                continue
            key = unescape(name[:-len('.dat')])
            if not key.startswith(prefix):
                continue
            yield key

    def get_size(self):
        size = 0
        for name in self.conn.sftp.listdir_attr(self.name):
            if not name.filename.endswith('.dat'):
                continue
            size += name.st_size
        return size

    def raw_fetch(self, key, fh):
        filename = os.path.join(self.name, escape(key))
        try:
            src = self.conn.sftp.open(filename + '.dat', 'r')
            fh.seek(0)
            shutil.copyfileobj(src, fh)

            src = self.conn.sftp.open(filename + '.meta', 'r')
            metadata = pickle.load(src)

        except IOError as exc:
            if exc.errno == errno.ENOENT:
                raise KeyError('Key %r not in bucket' % key)
            else:
                raise

        return metadata

    def raw_store(self, key, fh, metadata):
        filename = os.path.join(self.name, escape(key))

        destfilename = filename + '.dat'
        fh.seek(0)

        fh_dest = self.conn.sftp.open(destfilename, 'w')
        shutil.copyfileobj(fh, fh_dest)

        destfilename = filename + '.meta'
        fh.seek(0)
        fh_dest = self.conn.sftp.open(destfilename, 'w')
        pickle.dump(metadata, fh_dest, 2)

    def rename(self, src, dest):
        self.conn.sftp.rename(os.path.join(self.name, escape(src)),
                              os.path.join(self.name, escape(dest)))

def escape(s):
    '''Escape '/', '=' and '\0' in s'''

    s = s.replace('=', '=3D')
    s = s.replace('/', '=2F')
    s = s.replace('\0', '=00')

    return s

def unescape(s):
    '''Un-Escape '/', '=' and '\0' in s'''

    s = s.replace('=2F', '/')
    s = s.replace('=00', '\0')
    s = s.replace('=3D', '=')

    return s
