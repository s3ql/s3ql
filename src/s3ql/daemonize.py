# -*- coding: utf-8 -*-
'''
daemonize.py - this file is part of S3QL.

Copyright © 2008 Nikolaus Rath <Nikolaus@rath.org>

This work can be distributed under the terms of the GNU GPLv3.

The functions in this file are based on the python-daemon module by Ben Finney
<ben+python@benfinney.id.au>.

The reason for not simply using the module instead is that it does a lot of
staff that we don't need (not a real problem) and some stuff that we must not do
(the real problem).

This main issue is that python-daemon unconditionally closes all open file
descriptors. We don't want this for S3QL, because we have already opened the
database and log files when we daemonize. I think this is good design, because
it allows us to give a meaningful error message to the user if these files
cannot be opened (if we open them after daemonizing, the user will only see a
vanishing daemon process without any indication what went wrong).

According to “Advanced Programming in the Unix Environment”, the point of
closing all open file descriptors is only to "prevent the daemon from holding
open any descriptors that it may have inherited from its parent (which could be
a shell or some other process)". In this case the user will have to live with
that.
'''

from .logging import logging # Ensure use of custom logger class
import os
import sys

log = logging.getLogger(__name__)

def daemonize(workdir='/'):
    '''Daemonize the process'''

    os.chdir(workdir)

    detach_process_context()

    redirect_stream(sys.stdin, None)
    redirect_stream(sys.stdout, None)
    redirect_stream(sys.stderr, None)


def detach_process_context():
    '''Detach the process context from parent and session.

    Detach from the parent process and session group, allowing the parent to
    exit while this process continues running.

    Reference: “Advanced Programming in the Unix Environment”, section 13.3, by
    W. Richard Stevens, published 1993 by Addison-Wesley.
    '''

    # Protected member
    #pylint: disable=W0212

    pid = os.fork()
    if pid > 0:
        os._exit(0)

    os.setsid()

    pid = os.fork()
    if pid > 0:
        log.info('Daemonizing, new PID is %d', pid)
        os._exit(0)


def redirect_stream(system_stream, target_stream):
    '''Redirect *system_stream* to *target_stream*

    `system_stream` is a standard system stream such as
    ``sys.stdout``. `target_stream` is an open file object that should replace
    the corresponding system stream object.

    If `target_stream` is ``None``, defaults to opening the operating
    system's null device and using its file descriptor.
    '''

    if target_stream is None:
        target_fd = os.open(os.devnull, os.O_RDWR)
    else:
        target_fd = target_stream.fileno()
    os.dup2(target_fd, system_stream.fileno())
