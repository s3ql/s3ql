#!/usr/bin/env python3

import re
import os
import subprocess
import sys
import shutil
from argparse import ArgumentParser

trailing_w_re = re.compile(r'\s+\n$')
only_w_re = re.compile(r'^\s+\n$')

def checkfile(name, correct=False):
    found_problems = False
    if correct:
        dst = open(name + '.checkpatch.tmp', 'w+')

    with open(name, 'r+') as fh:
        for (lineno, line) in enumerate(fh):
            if only_w_re.search(line):
                print('%s:%d: line consists only of whitespace' % (name, lineno+1))
                found_problems = True
            elif trailing_w_re.search(line):
                print('%s:%d: trailing whitespace' % (name, lineno+1))
                found_problems = True
            if correct:
                dst.write(line.rstrip() + '\n')

        if correct:
            fh.seek(0)
            dst.seek(0)
            shutil.copyfileobj(dst, fh)
            fh.truncate()
            os.unlink(dst.name)
            found_problems = False

    return found_problems

def parse_args():
    parser = ArgumentParser(
        description="Check tracked files for trailing whitespace")

    parser.add_argument("--fix", action="store_true", default=False,
                      help="Automatically correct problems")

    return parser.parse_args()

options = parse_args()

os.chdir(os.path.dirname(__file__))
hg_out = subprocess.check_output(['hg', 'status', '--clean', '--modified', '--added',
                                  '--no-status', '--print0'])
found_problems = False
for b_name in hg_out.split(b'\0'):
    if not b_name:
        continue
    name = b_name.decode('utf-8', errors='surrogatescape')
    if checkfile(name, correct=options.fix):
        found_problems = True

if found_problems:
    sys.exit(1)
else:
    sys.exit(0)
