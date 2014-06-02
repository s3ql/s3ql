#!/usr/bin/env python3

import re
import os
import subprocess
import sys

trailing_w_re = re.compile(r'\s+\n$')
only_w_re = re.compile(r'^\s+\n$')

found_problems = False
def checkfile(name):
    global found_problems
    with open(name, 'r') as fh:
        for (lineno, line) in enumerate(fh):
            if only_w_re.search(line):
                print('%s:%d: line consists only of whitespace' % (name, lineno+1))
                found_problems = True
            elif trailing_w_re.search(line):
                print('%s:%d: trailing whitespace' % (name, lineno+1))
                found_problems = True

os.chdir(os.path.dirname(__file__))
hg_out = subprocess.check_output(['hg', 'status', '--clean', '--modified', '--added',
                                  '--no-status', '--print0'])
for b_name in hg_out.split(b'\0'):
    if not b_name:
        continue
    name = b_name.decode('utf-8', errors='surrogatescape')
    checkfile(name)


if found_problems:
    sys.exit(1)
else:
    sys.exit(0)
