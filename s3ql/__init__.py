#!/usr/bin/env python
#
#    Copyright (C) 2008  Nikolaus Rath <Nikolaus@rath.org>
#
#    This program can be distributed under the terms of the GNU LGPL.
#

from common import *
from fs import *
from mkfs import *

import s3
import fsck

log_fn = sys.stderr.writelines
log_level = 1
