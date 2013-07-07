'''
t1_retry.py - this file is part of S3QL (http://s3ql.googlecode.com)

Copyright (c) Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU GPLv3.
'''

from s3ql.backends.common import retry, retry_generator


class TemporaryProblem(Exception):
    pass

class ThirdAttempt:
    def __init__(self):
        self.count = 0

    @staticmethod
    def is_temp_failure(exc):
        return isinstance(exc, TemporaryProblem)

    @retry
    def do_stuff(self):
        if self.count == 3:
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
            
def test_retry():
    inst = ThirdAttempt()
    
    assert inst.do_stuff()
    

def test_retry_generator():
    inst = ThirdAttempt()
    assert list(inst.list_stuff(10)) == list(range(10))
    
    
