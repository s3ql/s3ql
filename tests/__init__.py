#
#    Copyright (C) 2008  Nikolaus Rath <Nikolaus@rath.org>
#
#    This program can be distributed under the terms of the GNU LGPL.
#


# Export all modules
from __future__ import unicode_literals
import os
testdir = os.path.dirname(__file__)
__all__  =  [ name[:-3] for name in os.listdir(testdir) if name.endswith(".py") ]
