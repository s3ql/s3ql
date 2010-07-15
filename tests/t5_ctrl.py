'''
t5_ctrl.py - this file is part of S3QL (http://s3ql.googlecode.com)

Copyright (C) 2008-2009 Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU LGPL.
'''

from __future__ import division, print_function

import s3ql.cli.ctrl
import unittest2 as unittest
import t4_fuse

class ctrlTests(t4_fuse.fuse_tests):

    def runTest(self):

        self.mount()
        self.tst_flush()
        self.umount()

    def tst_flush(self):

        try:
            s3ql.cli.ctrl.main(['--flush-cache', self.mnt_dir])
        except BaseException as exc:
            self.fail("s3qladm failed: %s" % exc)
            

# Somehow important according to pyunit documentation
def suite():
    return unittest.makeSuite(ctrlTests)


# Allow calling from command line
if __name__ == "__main__":
    unittest.main()
