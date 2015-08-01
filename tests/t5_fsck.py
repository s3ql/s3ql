#!/usr/bin/env python3
'''
t5_fsck.py - this file is part of S3QL.

Copyright © 2008 Nikolaus Rath <Nikolaus@rath.org>

This work can be distributed under the terms of the GNU GPLv3.
'''

if __name__ == '__main__':
    import pytest
    import sys
    sys.exit(pytest.main([__file__] + sys.argv[1:]))

from s3ql.common import get_backend_cachedir
from s3ql.database import Connection
from common import populate_dir, skip_without_rsync
import shutil
import subprocess
from subprocess import check_output, CalledProcessError
import t4_fuse
import tempfile

class TestFsck(t4_fuse.TestFuse):

    def test(self):
        skip_without_rsync()
        ref_dir = tempfile.mkdtemp(prefix='s3ql-ref-')
        try:
            populate_dir(ref_dir)

            # Make file system and fake high inode number
            self.mkfs()
            db = Connection(get_backend_cachedir(self.storage_url, self.cache_dir) + '.db')
            db.execute('UPDATE sqlite_sequence SET seq=? WHERE name=?',
                       (2 ** 31 + 10, 'inodes'))
            db.close()

            # Copy source data
            self.mount()
            subprocess.check_call(['rsync', '-aHAX', ref_dir + '/',
                                   self.mnt_dir + '/'])
            self.umount()

            # Check that inode watermark is high
            db = Connection(get_backend_cachedir(self.storage_url, self.cache_dir) + '.db')
            assert db.get_val('SELECT seq FROM sqlite_sequence WHERE name=?',
                              ('inodes',)) > 2 ** 31 + 10
            assert db.get_val('SELECT MAX(id) FROM inodes') > 2 ** 31 + 10
            db.close()

            # Renumber inodes
            self.fsck()

            # Check if renumbering was done
            db = Connection(get_backend_cachedir(self.storage_url, self.cache_dir) + '.db')
            assert db.get_val('SELECT seq FROM sqlite_sequence WHERE name=?',
                               ('inodes',)) < 2 ** 31
            assert db.get_val('SELECT MAX(id) FROM inodes') < 2 ** 31
            db.close()

            # Compare
            self.mount()
            try:
                out = check_output(['rsync', '-anciHAX', '--delete', '--exclude', '/lost+found',
                                   ref_dir + '/', self.mnt_dir + '/'], universal_newlines=True,
                                  stderr=subprocess.STDOUT)
            except CalledProcessError as exc:
                pytest.fail('rsync failed with ' + exc.output)
            if out:
                pytest.fail('Copy not equal to original, rsync says:\n' + out)

            self.umount()
        finally:
            shutil.rmtree(ref_dir)
