'''
$Id: __init__.py 832 2010-02-16 16:28:18Z nikratio $

Copyright (C) 2008-2009 Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU LGPL.
'''

from __future__ import division, print_function, absolute_import

from .common import AbstractConnection, AbstractBucket
from ..common import QuietError
import logging
import errno
import sys
import shutil
import cPickle as pickle
import os
import paramiko

log = logging.getLogger("backend.sftp")

class Connection(AbstractConnection):

    def __init__(self, host, port, login, password):
        super(Connection, self).__init__()
        raise QuietError('SFTP backend is not yet implemented.')

    def __init__(self, host_string, key_file=None):
        self.sftp = None
        self.private_key = key_file
        host_string = 'sftp://' + host_string
        obj = urlparse(host_string)
        self.host = obj.hostname
        self.username = obj.username
        self.password = obj.password
        self.port = obj.port
        if not self.port:
            self.port = 22
        self.path = obj.path
        self.setup_ssh_connection()



    def setup_ssh_connection(self):

        templog = tempfile.mkstemp('.log', 's3ql-sftp_connection-')[1]
        paramiko.util.log_to_file(templog, 20)
        self._client = paramiko.SSHClient()
        self._client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        self._client.connect(self.host, port=self.port, username=self.username, password=self.password,
                                key_filename=self.private_key)
        self._transport = self._client.get_transport()
        self._transport.set_keepalive(300)
        self.sftp = self._client.open_sftp()
        if self.path:
            try:
                self.sftp.chdir(self.path)
            except:
                raise KeyError('Path %s is not on server, or you do not have proper permissions' % self.path)

    def _dir_exists(self, dir):
        try:
            file_list = self.sftp.listdir(dir)

        except IOError as exc:
            if exc.errno == 2:
                return False
            else:
                print('dir_exists error %s' % exc[0])
                return False
        except:
            print('Exception = %s' % sys.exc_info()[0])
            return False

        return True


    def delete_bucket(self, name, recursive=False):
        """Delete bucket"""

        if not self._dir_exists(name):
            raise KeyError('Directory of local bucket does not exist')

        if recursive:
            file_list = self.sftp.listdir(name)
            for item in file_list:
                if not (item.endswith('.dat') or item.endswith('.meta')):
                    continue
                file = name + '/' + item
                self.sftp.remove(file)

        try:
            self.sftp.rmdir(name)
        except IOError:
           print('Failed to remove Bucket Directory, Possibly other files in directory %s' % name)
           return True


    def create_bucket(self, name, passphrase=None):
        """Create and return an S3 bucket"""
        self.sftp.mkdir(name)
        return self.get_bucket(name, passphrase)

    def get_bucket(self, name, passphrase=None):

        """Return a bucket instance for the bucket `name`
        Raises `KeyError` if the bucket does not exist.
        """
        if not self._dir_exists(name):
            raise KeyError('Bucket %s does not exist' % name)

        return Bucket(self, name, passphrase)

    def close(self):
        """Closes the connection and cleans up."""
        self._client.close()


class Bucket(AbstractBucket):
    '''A bucket that is stored on the local hard disk'''

    def __init__(self, conn, name, passphrase):
        super(Bucket, self).__init__()
        self.passphrase = passphrase
        self.conn = conn
        self.name = name

    def __str__(self):
        if self.passphrase:
            return '<encrypted local bucket, name=%r>' % self.name
        else:
            return '<local bucket, name=%r>' % self.name

    def clear(self):
        """Delete all objects in bucket"""

        for name in self.conn.sftp.listdir(self.name):
            if not (name.endswith('.dat') or name.endswith('.meta')):
                continue
            self.conn.sftp.remove(os.path.join(self.name, name))


    def prepare_fork(self):
        self.conn.close()

    def finish_fork(self):
        self.conn.setup_ssh_connection()

    def contains(self, key):
        filename = escape(key) + '.dat'
        return filename in self.conn.sftp.listdir(self.name)

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
        """Deletes the specified key
        ``bucket.delete(key)`` can also be written as ``del bucket[key]``.
        If `force` is true, do not return an error if the key does not exist.
        """

        filename = os.path.join(self.name, escape(key))

        try:
            delfilename = filename + '.dat'
            self.conn.sftp.remove(delfilename)
            delfilename = filename + '.meta'
            self.conn.sftp.remove(delfilename)
        except IOError as exc:
            if exc.errno == errno.ENOENT:
                if force:
                    pass
                else:
                    raise KeyError('Key %r not in bucket' % key)
            else:
                raise


    def list(self, prefix=''):
        """List keys in bucket
        Returns an iterator over all keys in the bucket.
        """

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


    def copy(self, src, dest):
        """Copy data stored under `src` to `dest`"""

        if not isinstance(src, str):
            raise TypeError('key must be of type str')

        if not isinstance(dest, str):
            raise TypeError('key must be of type str')

        filename_src = os.path.join(self.name, escape(src)) + '.dat'
        filename_dest = os.path.join(self.name, escape(dest)) + '.dat'
        cpy_cmd = 'cp ' + filename_src + ' ' + filename_dest
        self.conn._client.exec_command(cpy_cmd)

        filename_src = os.path.join(self.name, escape(src)) + '.meta'
        filename_dest = os.path.join(self.name, escape(dest)) + '.meta'
        cpy_cmd = 'cp ' + filename_src + ' ' + filename_dest
        self.conn._client.exec_command(cpy_cmd)



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
