#!/usr/bin/env python
#
#    Copyright (C) 2008  Nikolaus Rath <Nikolaus@rath.org>
#
#    This program can be distributed under the terms of the GNU LGPL.
#

from common import *
from file import *
from fs import *
import s3
import fsck
from mkfs import *

log_fn = sys.stderr.writelines
log_level = 1
