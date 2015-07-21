#!/usr/bin/env python3
'''
t1_retry.py - this file is part of S3QL (http://s3ql.googlecode.com)

Copyright © 2008 Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU GPLv3.
'''

if __name__ == '__main__':
    import pytest
    import sys
    sys.exit(pytest.main([__file__] + sys.argv[1:]))

from s3ql.backends.common import retry, retry_generator
from common import catch_logmsg
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

    @retry_generator
    def list_stuff(self, upto=10, start_after=-1):
        for i in range(upto):
            if i <= start_after:
                continue
            if i == 2 and self.count < 1:
                self.count += 1
                raise TemporaryProblem

            if i == 7 and self.count < 4:
                self.count += 1
                raise TemporaryProblem

            yield i

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

def test_retry_generator():
    inst = NthAttempt(3)
    assert list(inst.list_stuff(10)) == list(range(10))

def test_is_retry():
    inst = NthAttempt(3)
    assert inst.test_is_retry()

def test_logging():
    inst = NthAttempt(6)
    with catch_logmsg(r'^Encountered %s \(%s\), retrying ',
                      count=2, level=logging.WARNING):
        inst.do_stuff()
