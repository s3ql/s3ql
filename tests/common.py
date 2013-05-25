'''
common.py - this file is part of S3QL (http://s3ql.googlecode.com)

Copyright (c) Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU GPLv3.


This module contains common functions used by multiple unit tests.
'''

from contextlib import contextmanager
from functools import wraps
import logging
import re

@contextmanager
def catch_logmsg(pattern, level=logging.WARNING, count=None):
    '''Catch (and ignore) log messages matching *pattern*

    *pattern* is matched against the *unformatted* log message, i.e. before any
    arguments are merged.

    If *count* is not None, raise an exception unless exactly *count* matching
    messages are caught.
    '''

    logger_class = logging.getLoggerClass()
    handle_orig = logger_class.handle
    caught = [0]
    
    @wraps(handle_orig)
    def handle_new(self, record):
        if (record.levelno != level
            or not re.search(pattern, record.msg)):
            return handle_orig(self, record)
        caught[0] += 1
        
    logger_class.handle = handle_new
    try:
        yield

    finally:
        logger_class.handle = handle_orig

        if count is not None and caught[0] != count:
            raise AssertionError('Expected to catch %d log messages, but got only %d'
                                 % (count, caught[0]))

