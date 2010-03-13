'''
$Id: __init__.py 832 2010-02-16 16:28:18Z nikratio $

Copyright (C) 2008-2009 Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU LGPL.
'''

from __future__ import division, print_function, absolute_import

from .common import AbstractConnection
from ..common import QuietError
import logging

log = logging.getLogger("backend.ftp")

class Connection(AbstractConnection):

    def __init__(self, host, port, login, password):
        super(Connection, self).__init__()
        raise QuietError('FTP backend is not yet implemented.')

class TLSConnection(Connection):

    def __init__(self, host, port, login, password):
        super(Connection, self).__init__()
        raise QuietError('FTP backend is not yet implemented.')
