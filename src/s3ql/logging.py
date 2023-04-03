'''
logging.py - this file is part of S3QL.

Copyright © 2008 Nikolaus Rath <Nikolaus@rath.org>

This work can be distributed under the terms of the GNU GPLv3.
'''

import logging
import logging.handlers
import os.path
import sys
import warnings


class QuietError(Exception):
    '''
    QuietError is the base class for exceptions that should not result
    in a stack trace being printed.

    It is typically used for exceptions that are the result of the user
    supplying invalid input data. The exception argument should be a
    string containing sufficient information about the problem.
    '''

    def __init__(self, msg='', exitcode=1):
        super().__init__()
        self.msg = msg

        #: Exit code to use when terminating process
        self.exitcode = exitcode

    def __str__(self):
        return self.msg


SYSTEMD_LOG_LEVEL_MAP = {
    logging.CRITICAL: 0,
    logging.ERROR: 3,
    logging.WARNING: 4,
    logging.INFO: 6,
    logging.DEBUG: 7,
}


class SystemdFormatter(logging.Formatter):
    def format(self, record):
        s = super().format(record)
        prefix = SYSTEMD_LOG_LEVEL_MAP.get(record.levelno, None)
        if prefix:
            s = '<%d>%s' % (prefix, s)
        return s


class MyFormatter(logging.Formatter):
    '''Prepend severity to log message if it exceeds threshold'''

    def format(self, record):
        s = super().format(record)
        if record.levelno > logging.INFO:
            s = '%s: %s' % (record.levelname, s)
        return s


def create_handler(target):
    '''Create logging handler for given target'''

    if target.lower() == 'syslog':
        handler = logging.handlers.SysLogHandler('/dev/log')
        formatter = logging.Formatter(
            os.path.basename(sys.argv[0])
            + '[%(process)s:%(threadName)s] '
            + '%(name)s.%(funcName)s: %(message)s'
        )

    else:
        fullpath = os.path.expanduser(target)
        dirname = os.path.dirname(fullpath)
        if dirname and not os.path.exists(dirname):
            try:
                os.makedirs(dirname)
            except PermissionError:
                raise QuietError('No permission to create log file %s' % fullpath, exitcode=10)

        try:
            handler = logging.handlers.RotatingFileHandler(
                fullpath, maxBytes=10 * 1024**2, backupCount=5
            )
        except PermissionError:
            raise QuietError('No permission to write log file %s' % fullpath, exitcode=10)

        formatter = logging.Formatter(
            '%(asctime)s.%(msecs)03d %(process)s:%(threadName)s '
            '%(name)s.%(funcName)s: %(message)s',
            datefmt="%Y-%m-%d %H:%M:%S",
        )

    handler.setFormatter(formatter)
    return handler


def setup_logging(options):

    # We want to be able to detect warnings and higher severities
    # in the captured test output. 'critical' has too many potential
    # false positives, so we rename this level to "FATAL".
    logging.addLevelName(logging.CRITICAL, 'FATAL')

    root_logger = logging.getLogger()
    if root_logger.handlers:
        root_logger.debug("Logging already initialized.")
        return

    filter = LogFilter()

    stdout_handler = add_stdout_logging(options.quiet, getattr(options, 'systemd', False))
    stdout_handler.addFilter(filter)
    if hasattr(options, 'log') and options.log:
        handler = create_handler(options.log)
        handler.addFilter(filter)
        root_logger.addHandler(handler)
    elif options.debug and (not hasattr(options, 'log') or not options.log):
        # When we have debugging enabled but no separate log target,
        # make stdout logging more detailed.
        formatter = logging.Formatter(
            '%(asctime)s.%(msecs)03d %(process)s %(levelname)-8s '
            '%(threadName)s %(name)s.%(funcName)s: %(message)s',
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        stdout_handler.setFormatter(formatter)
        stdout_handler.setLevel(logging.NOTSET)

    setup_excepthook()

    if options.debug:
        logging.getLogger('__main__').setLevel(logging.DEBUG)
        if 'all' in options.debug:
            root_logger.setLevel(logging.DEBUG)
        else:
            root_logger.setLevel(logging.INFO)
            for module in options.debug:
                logging.getLogger(module).setLevel(logging.DEBUG)
        logging.disable(logging.NOTSET)
    else:
        root_logger.setLevel(logging.INFO)
        logging.disable(logging.DEBUG)

    return stdout_handler


def setup_warnings():
    # Always redirect warnings to logs so that they don't get lost
    logging.captureWarnings(capture=True)

    # If we're running under py.test, also narrow down warnings to actionable ones.
    # Otherwise, leave default configuration untouched.
    if sys.warnoptions != ['default', 'default:set_by_conftest']:
        return

    warnings.resetwarnings()
    for cat in (DeprecationWarning, PendingDeprecationWarning):
        warnings.filterwarnings(action='default', category=cat, module='^s3ql', append=True)
        warnings.filterwarnings(action='ignore', category=cat, append=True)
    warnings.filterwarnings(action='default', append=True)


def setup_excepthook():
    '''Modify sys.excepthook to log exceptions

    Also makes sure that exceptions derived from `QuietException`
    do not result in stacktraces.
    '''

    def excepthook(type_, val, tb):
        log = logging.getLogger('__main__')

        if isinstance(val, QuietError):
            log.debug('Uncaught top-level exception:', exc_info=(type_, val, tb))
            log.error(val.msg)
            sys.exit(val.exitcode)
        else:
            log.error('Uncaught top-level exception:', exc_info=(type_, val, tb))
            sys.exit(1)

    sys.excepthook = excepthook


def add_stdout_logging(quiet=False, systemd=False):
    '''Add stdout logging handler to root logger'''

    root_logger = logging.getLogger()
    if systemd:
        formatter = SystemdFormatter('%(message)s')
    else:
        formatter = MyFormatter('%(message)s')
    handler = logging.StreamHandler(sys.stderr)
    if not systemd and quiet:
        handler.setLevel(logging.WARNING)
    else:
        handler.setLevel(logging.INFO)
    handler.setFormatter(formatter)
    root_logger.addHandler(handler)
    return handler


class LogFilter:
    '''
    Log messages that are emitted with an *log_once* attribute in the
    *extra* parameter are only emitted once.
    '''

    def __init__(self):
        super().__init__()
        self.log_cache = set()

    def filter(self, record):
        if hasattr(record, 'log_once') and record.log_once:
            id_ = hash((record.name, record.levelno, record.msg, record.args, record.exc_info))
            if id_ in self.log_cache:
                return False
            self.log_cache.add(id_)
        return True


# Convenience object for use in logging calls, e.g.
# log.warning('This will be printed only once', extra=LOG_ONCE)
LOG_ONCE = {'log_once': True}
