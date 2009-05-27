#!/usr/bin/env python
#
#    Copyright (C) 2008  Nikolaus Rath <Nikolaus@rath.org>
#
#    This program can be distributed under the terms of the GNU LGPL.
#

import unittest

class fsck_tests(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_something(self):
        """Docstring is displayed
        """
        pass

    def test_somethingElse(self):
        """Docstring is displayed
        """
        pass


# Somehow important according to pyunit documentation
def suite():
    return unittest.makeSuite(fsck_tests)


# Allow calling from command line
if __name__ == "__main__":
            unittest.main()
