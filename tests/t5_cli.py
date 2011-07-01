'''
t5_cli.py - this file is part of S3QL (http://s3ql.googlecode.com)

Copyright (C) 2008-2009 Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU GPLv3.
'''

from __future__ import division, print_function
import os.path
import errno
import s3ql.cli.ctrl
import s3ql.cli.lock
import s3ql.cli.remove
import llfuse
import unittest2 as unittest
import t4_fuse

class cliTests(t4_fuse.fuse_tests):
    
    def runTest(self):
        self.mount()
        self.tst_lock_rm()
        self.tst_ctrl_flush()
        self.umount()

    def tst_ctrl_flush(self):

        try:
            s3ql.cli.ctrl.main(['flushcache', self.mnt_dir])
        except BaseException as exc:
            self.fail("s3qladm failed: %s" % exc)
            
    def tst_lock_rm(self):

        # Extract tar
        tempdir = os.path.join(self.mnt_dir, 'lock_dir')
        filename = os.path.join(tempdir, 'myfile')
        os.mkdir(tempdir)
        with open(filename, 'w') as fh:
            fh.write('Hello, world')

        # copy
        try:
            s3ql.cli.lock.main([tempdir])
        except BaseException as exc:
            self.fail("s3qllock failed: %s" % exc)

        # Try to delete
        with self.assertRaises(OSError) as cm:
            os.unlink(filename)
        self.assertEqual(cm.exception[0], errno.EPERM)

        # Try to write
        with self.assertRaises(IOError) as cm:
            open(filename, 'w+').write('Hello')
        self.assertEqual(cm.exception[0], errno.EPERM)

        # delete properly
        try:
            s3ql.cli.remove.main([tempdir])
        except BaseException as exc:
            self.fail("s3qlrm failed: %s" % exc)        

        self.assertTrue('lock_dir' not in llfuse.listdir(self.mnt_dir))

# Somehow important according to pyunit documentation
def suite():
    return unittest.makeSuite(cliTests)


# Allow calling from command line
if __name__ == "__main__":
    unittest.main()
