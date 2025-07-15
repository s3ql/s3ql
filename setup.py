#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
setup.py - this file is part of S3QL.

Copyright © 2008 Nikolaus Rath <Nikolaus@rath.org>

This work can be distributed under the terms of the GNU GPLv3.
'''

import faulthandler
import os
import subprocess
import sys
from glob import glob

import setuptools
from Cython.Compiler import Options as Cython_options

faulthandler.enable()

basedir = os.path.abspath(os.path.dirname(sys.argv[0]))
DEVELOPER_MODE = os.path.exists(os.path.join(basedir, 'MANIFEST.in'))
if DEVELOPER_MODE:
    print('MANIFEST.in exists, running in developer mode')


Cython_options.language_level = "3"


def main():
    compile_args = ['-Wall', '-Wextra']

    # Value-changing conversions should always be explicit.
    compile_args.append('-Wconversion')

    # Note that (i > -1) is false if i is unsigned (-1 will be converted to
    # a large positive value). We certainly don't want to do this by
    # accident.
    compile_args.append('-Wsign-compare')

    # These warnings have always been harmless, and have always been due to
    # issues in Cython code rather than S3QL. Cython itself warns if there
    # are unused variables in .pyx code.
    compile_args.append('-Wno-unused-parameter')
    compile_args.append('-Wno-unused-function')

    setuptools.setup(
        name='s3ql',
        package_dir={'': 'src'},
        packages=setuptools.find_packages('src'),
        provides=['s3ql'],
        ext_modules=[
            setuptools.Extension(
                's3ql.sqlite3ext',
                ['src/s3ql/sqlite3ext.pyx'],
                extra_compile_args=compile_args,
                language='c++',
                depends=['src/s3ql/_sqlite3ext.cpp'],
            ),
        ],
        data_files=[
            (
                'share/man/man1',
                [
                    os.path.join('doc/manpages/', x)
                    for x in glob(os.path.join(basedir, 'doc', 'manpages', '*.1'))
                ],
            )
        ],
        cmdclass={'upload_docs': upload_docs},
    )


class upload_docs(setuptools.Command):
    user_options = []
    boolean_options = []
    description = "Upload documentation"

    def initialize_options(self):
        pass

    def finalize_options(self):
        pass

    def run(self):
        subprocess.check_call(
            [
                'rsync',
                '-aHv',
                '--del',
                os.path.join(basedir, 'doc', 'html') + '/',
                'ebox.rath.org:/srv/www.rath.org/s3ql-docs/',
            ]
        )
        subprocess.check_call(
            [
                'rsync',
                '-aHv',
                '--del',
                os.path.join(basedir, 'doc', 'manual.pdf'),
                'ebox.rath.org:/srv/www.rath.org/s3ql-docs/',
            ]
        )


if __name__ == '__main__':
    main()
