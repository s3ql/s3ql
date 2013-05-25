'''
logging.py - this file is part of S3QL (http://s3ql.googlecode.com)

Copyright (C) 2008-2009 Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU GPLv3.
'''

import logging
from cgitb import scanvars, __UNDEF__
import inspect
import linecache
import pydoc
import warnings
import sys

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


class QuietError(Exception):
    '''
    QuietError is the base class for exceptions that should not result
    in a stack trace being printed.

    It is typically used for exceptions that are the result of the user
    supplying invalid input data. The exception argument should be a
    string containing sufficient information about the problem.
    '''

    def __init__(self, msg=''):
        super().__init__()
        self.msg = msg

    def __str__(self):
        return self.msg


def setup_logging(options):
    root_logger = logging.getLogger()
    if root_logger.handlers:
        root_logger.debug("Logging already initialized.")
        return

    stdout_handler = add_stdout_logging(options.quiet)
    if hasattr(options, 'log') and options.log:
        root_logger.addHandler(options.log)
        debug_handler = options.log
    else:
        debug_handler = stdout_handler
    setup_excepthook()

    if options.debug:
        root_logger.setLevel(logging.DEBUG)
        debug_handler.setLevel(logging.NOTSET)
        if 'all' not in options.debug:
            # Adding the filter to the root logger has no effect.
            debug_handler.addFilter(LoggerFilter(options.debug, logging.INFO))
        logging.disable(logging.NOTSET)
    else:
        root_logger.setLevel(logging.INFO)
        logging.disable(logging.DEBUG)

    logging.captureWarnings(capture=True)

    return stdout_handler


# Adapted from cgitb.text, but less verbose
def format_tb(einfo):
    """Return a plain text document describing a given traceback."""

    etype, evalue, etb = einfo
    if type(etype) is type:
        etype = etype.__name__

    frames = [ 'Traceback (most recent call last):' ]
    records = inspect.getinnerframes(etb, context=7)
    for (frame, file_, lnum, func, lines, index) in records:
        (args, varargs, varkw, locals_) = inspect.getargvalues(frame)
        sig = inspect.formatargvalues(args, varargs, varkw, locals_,
                                      formatvalue=lambda value: '=' + pydoc.text.repr(value))

        rows = ['  File %r, line %d, in %s%s' % (file_, lnum, func, sig) ]

        # To print just current line
        if index is not None:
            rows.append('    %s' % lines[index].strip())

#        # To print with context:
#        if index is not None:
#            i = lnum - index
#            for line in lines:
#                num = '%5d ' % i
#                rows.append(num+line.rstrip())
#                i += 1

        def reader(lnum=[lnum]): #pylint: disable=W0102
            try:
                return linecache.getline(file_, lnum[0])
            finally:
                lnum[0] += 1

        printed = set()
        rows.append('  Current bindings:')
        for (name, where, value) in scanvars(reader, frame, locals_):
            if name in printed:
                continue
            printed.add(name)
            if value is not __UNDEF__:
                if where == 'global':
                    where = '(global)'
                elif where != 'local':
                    name = where + name.split('.')[-1]
                    where = '(local)'
                else:
                    where = ''
                rows.append('    %s = %s %s' % (name, pydoc.text.repr(value), where))
            else:
                rows.append(name + ' undefined')

        rows.append('')
        frames.extend(rows)

    exception = ['Exception: %s: %s' % (etype.__name__, evalue)]
    if isinstance(evalue, BaseException):

        # We may list deprecated attributes when iteracting, but obviously
        # we do not need any warnings about that.
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")

            for name in dir(evalue):
                if name.startswith('__'):
                    continue

                value = pydoc.text.repr(getattr(evalue, name))
                exception.append('  %s = %s' % (name, value))

    return '%s\n%s' % ('\n'.join(frames), '\n'.join(exception))


def setup_excepthook():
    '''Modify sys.excepthook to log exceptions

    Also makes sure that exceptions derived from `QuietException`
    do not result in stacktraces.
    '''

    def excepthook(type_, val, tb):
        root_logger = logging.getLogger()
        if isinstance(val, QuietError):
            root_logger.error(val.msg)
        else:
            # Customized exception handler has shown to just blow up the size
            # of error messages and potentially include confidential data
            # without providing any significant benefits
#            try:
#                msg = format_tb((type_, val, tb))
#            except:
#                root_logger.error('Uncaught top-level exception -- and tb handler failed!',
#                                  exc_info=(type_, val, tb))
#            else:
#                root_logger.error('Uncaught top-level exception. %s', msg)
            root_logger.error('Uncaught top-level exception:',
                              exc_info=(type_, val, tb))

    sys.excepthook = excepthook


class LoggerFilter(object):
    """
    For use with the logging module as a message filter.

    This filter accepts all messages which have at least the specified
    priority *or* come from a configured list of loggers.
    """

    def __init__(self, acceptnames, acceptlevel):
        """Initializes a Filter object"""

        self.acceptlevel = acceptlevel
        self.acceptnames = [ x.lower() for x in acceptnames ]

    def filter(self, record):
        '''Determine if the log message should be printed'''

        if record.levelno >= self.acceptlevel:
            return True

        if record.name.lower() in self.acceptnames:
            return True

        return False

    
def add_stdout_logging(quiet=False):
    '''Add stdout logging handler to root logger'''

    root_logger = logging.getLogger()
    formatter = logging.Formatter('%(message)s')
    handler = logging.StreamHandler()
    handler.setFormatter(formatter)
    if quiet:
        handler.setLevel(logging.WARN)
    else:
        handler.setLevel(logging.INFO)
    root_logger.addHandler(handler)
    return handler


# Ensure that no handlers have been created yet
loggers = logging.Logger.manager.loggerDict
if len(loggers) != 0:
    raise ImportError('%s must be imported before loggers are created! '
                      'Existing loggers: %s' % (__name__, loggers.keys()))

# Monkeypatch the root logger
#pylint: disable=W0212
def handle(self, record):
    if record.levelno >= EXCEPTION_SEVERITY:
        raise LoggingError(record)
    self._handle_real(record)
root_logger_class = type(logging.getLogger())
root_logger_class._handle_real = root_logger_class.handle
root_logger_class.handle = handle

logging.setLoggerClass(Logger)
