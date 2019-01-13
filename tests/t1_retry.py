#!/usr/bin/env python3
'''
t1_retry.py - this file is part of S3QL.

Copyright © 2008 Nikolaus Rath <Nikolaus@rath.org>

This work can be distributed under the terms of the GNU GPLv3.
'''

if __name__ == '__main__':
    import pytest
    import sys
    sys.exit(pytest.main([__file__] + sys.argv[1:]))

from s3ql.backends.common import retry
from pytest_checklogs import assert_logs
import logging

class TemporaryProblem(Exception):
    pass

class NthAttempt:
    def __init__(self, succeed_on=3):
        self.count = 0
        self.succeed_on = succeed_on

    @staticmethod
    def is_temp_failure(exc):
        return isinstance(exc, TemporaryProblem)

    @retry
    def do_stuff(self):
        if self.count == self.succeed_on:
            return True
        self.count += 1
        raise TemporaryProblem()

    @retry
    def test_is_retry(self, is_retry=False):
        assert is_retry == (self.count != 0)
        if self.count == self.succeed_on:
            return True
        self.count += 1
        raise TemporaryProblem()

def test_retry():
    inst = NthAttempt(3)

    assert inst.do_stuff()

def test_is_retry():
    inst = NthAttempt(3)
    assert inst.test_is_retry()

def test_logging():
    inst = NthAttempt(6)
    with assert_logs(r'^Encountered %s \(%s\), retrying ',
                      count=2, level=logging.WARNING):
        inst.do_stuff()
