'''
logging.py - this file is part of S3QL (http://s3ql.googlecode.com)

Copyright (C) 2008-2009 Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU GPLv3.
'''

import logging

# Logging messages with severities larger or equal
# than this value will raise exceptions.
EXCEPTION_SEVERITY = logging.CRITICAL+1

class LoggingError(Exception):
    '''
    Raised when a `Logger` instance is used to log a message with
    a severity larger than its `exception_severity`.
    '''
    
    formatter = logging.Formatter('%(message)s')
                                
    def __init__(self, record):
        super().__init__()
        self.record = record
        
    def __str__(self):
        return 'Unexpected log message: ' + self.formatter.format(self.record)
        

class Logger(logging.getLoggerClass()):
    '''
    This class has the following features in addition to `logging.Logger`:
    
    * Loggers can automatically raise exceptions when a log message exceeds
      a specified severity. This is useful when running unit tests.
    '''
    
    def __init__(self, name):
        super().__init__(name)
        
    def handle(self, record):
        if record.levelno >= EXCEPTION_SEVERITY:
            raise LoggingError(record)
        
        return super().handle(record)

    
# Ensure that no handlers have been created yet
loggers = logging.Logger.manager.loggerDict 
if len(loggers) != 0:
    raise ImportError('%s must be imported before loggers are created! '
                      'Existing loggers: %s' % (__name__, loggers.keys()))
    
# Monkeypatch the root logger
def handle(self, record): 
    if record.levelno >= EXCEPTION_SEVERITY:
        raise LoggingError(record)
    self._handle_real(record)
root_logger_class = type(logging.getLogger())
root_logger_class._handle_real = root_logger_class.handle
root_logger_class.handle = handle 
    
logging.setLoggerClass(Logger)
    
