#!/usr/bin/env python
'''
setup.py - this file is part of S3QL (http://s3ql.googlecode.com)

Copyright (C) 2008-2009 Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU GPLv3.
'''

from __future__ import division, print_function

import sys
import os
import subprocess
import logging.handlers
from glob import glob

# Work around setuptools bug
# http://bitbucket.org/tarek/distribute/issue/152/
#pylint: disable=W0611
import multiprocessing

# Add S3QL sources
basedir = os.path.abspath(os.path.dirname(sys.argv[0]))
sys.path.insert(0, os.path.join(basedir, 'src'))
import s3ql

# Import distribute
sys.path.insert(0, os.path.join(basedir, 'util'))
from distribute_setup import use_setuptools
use_setuptools(version='0.6.14', download_delay=5)
import setuptools
import setuptools.command.test as setuptools_test
from setuptools import Extension
                       
class build_docs(setuptools.Command):
    description = 'Build Sphinx documentation'
    user_options = [
        ('fresh-env', 'E', 'discard saved environment'),
        ('all-files', 'a', 'build all files'),
    ]
    boolean_options = ['fresh-env', 'all-files']
    
    def initialize_options(self):
        self.fresh_env = False
        self.all_files = False

    def finalize_options(self):
        pass

    def run(self):
        try:
            from sphinx.application import Sphinx
            from docutils.utils import SystemMessage
        except ImportError:
            raise QuietError('This command requires Sphinx to be installed.')
            
        dest_dir = os.path.join(basedir, 'doc')
        src_dir = os.path.join(basedir, 'rst')
                    
        confoverrides = {}
        confoverrides['version'] = s3ql.VERSION
        confoverrides['release'] = s3ql.VERSION
        
        for builder in ('html', 'latex', 'man'):
            print('Running %s builder...' % builder)
            self.mkpath(os.path.join(dest_dir, builder))
            app = Sphinx(srcdir=src_dir, confdir=src_dir, 
                         outdir=os.path.join(dest_dir, builder),
                         doctreedir=os.path.join(dest_dir, 'doctrees'), 
                         buildername=builder, confoverrides=confoverrides,
                         freshenv=self.fresh_env)
            self.fresh_env = False
            self.all_files = False
        
            try:
                if self.all_files:
                    app.builder.build_all()
                else:
                    app.builder.build_update()
            except SystemMessage as err:
                print('reST markup error:',
                      err.args[0].encode('ascii', 'backslashreplace'),
                      file=sys.stderr)
                
        # These shouldn't be installed by default                    
        for name in ('expire_backups.1', 'pcp.1'):     
            os.rename(os.path.join(dest_dir, 'man', name),
                      os.path.join(basedir, 'contrib', name))

        print('Running pdflatex...')
        for _ in range(3):
            subprocess.check_call(['pdflatex', '-interaction',
                                   'batchmode', 'manual.tex'],
                                  cwd=os.path.join(dest_dir, 'latex'), 
                                  stdout=open('/dev/null', 'wb'))
        os.rename(os.path.join(dest_dir, 'latex', 'manual.pdf'),
                  os.path.join(dest_dir, 'manual.pdf'))
                  
    
def main():

    with open(os.path.join(basedir, 'rst', 'about.rst'), 'r') as fh:
        long_desc = fh.read()

    compile_args = ['-Wall', '-Wextra', '-Wno-unused-parameter' ]
    
    # http://trac.cython.org/cython_trac/ticket/704
    compile_args.append('-Wno-unused-but-set-variable')
    
    # http://bugs.python.org/issue969718
    if sys.version_info[0] == 2: 
        compile_args.append('-fno-strict-aliasing')

    # http://bugs.python.org/issue7576
    if sys.version_info[0] == 3 and sys.version_info[1] < 2:
        compile_args.append('-Wno-missing-field-initializers')
        
    setuptools.setup(
          name='s3ql',
          zip_safe=True,
          version=s3ql.VERSION,
          description='a full-featured file system for online data storage',
          long_description=long_desc,
          author='Nikolaus Rath',
          author_email='Nikolaus@rath.org',
          url='http://code.google.com/p/s3ql/',
          download_url='http://code.google.com/p/s3ql/downloads/list',
          license='GPLv3',
          classifiers=['Development Status :: 4 - Beta',
                       'Environment :: No Input/Output (Daemon)',
                       'Environment :: Console',
                       'License :: OSI Approved :: GNU Library or Lesser General Public License (GPLv3)',
                       'Topic :: Internet',
                       'Operating System :: POSIX',
                       'Topic :: System :: Archiving'],
          platforms=[ 'POSIX', 'UNIX', 'Linux' ],
          keywords=['FUSE', 'backup', 'archival', 'compression', 'encryption',
                    'deduplication', 'aws', 's3' ],
          package_dir={'': 'src'},
          packages=setuptools.find_packages('src'),
          provides=['s3ql'],
          ext_modules=[Extension('s3ql._deltadump', ['src/s3ql/_deltadump.c'], 
                                 extra_compile_args=compile_args,
                                 extra_link_args=[ '-lsqlite3'] )],          
          data_files = [ ('share/man/man1', 
                          [ os.path.join('doc/man/', x) for x
                            in glob(os.path.join(basedir, 'doc', 'man', '*.1')) ]) ],
          entry_points={ 'console_scripts':
                        [
                         'mkfs.s3ql = s3ql.mkfs:main',
                         'fsck.s3ql = s3ql.fsck:main',
                         'mount.s3ql = s3ql.mount:main',
                         'umount.s3ql = s3ql.umount:main',
                         's3qlcp = s3ql.cp:main',
                         's3qlstat = s3ql.statfs:main',
                         's3qladm = s3ql.adm:main',
                         's3qlctrl = s3ql.ctrl:main',
                         's3qllock = s3ql.lock:main',
                         's3qlrm = s3ql.remove:main',
                         ]
                          },
          install_requires=['apsw >= 3.7.0',
                            'pycryptopp',
                            'llfuse >= 0.37',
                            'argparse >= 1.1',
                            'pyliblzma >= 0.5.3' ],
          tests_require=['apsw >= 3.7.0', 'unittest2',
                         'pycryptopp',
                         'llfuse >= 0.37',
                         'argparse >= 1.1',
                         'pyliblzma >= 0.5.3' ],
          test_suite='tests',
          cmdclass={'test': test,
                    'upload_docs': upload_docs,
                    'build_cython': build_cython,
                    'build_sphinx': build_docs }, 
          command_options = { 'sdist': { 'formats': ('setup.py', 'bztar') } }, 
         )

class build_cython(setuptools.Command):
    user_options = []
    boolean_options = []
    description = "Compile .pyx to .c"

    def initialize_options(self):
        pass

    def finalize_options(self):
        # Attribute defined outside init
        #pylint: disable=W0201
        self.extensions = self.distribution.ext_modules

    def run(self):
        try:
            from Cython.Compiler.Main import compile as cython_compile
            from Cython.Compiler.Options import extra_warnings
        except ImportError:
            raise SystemExit('Cython needs to be installed for this command')

        directives = dict(extra_warnings)
        directives['embedsignature'] = True
        options = { 'recursive': False, 'verbose': True, 'timestamps': False, 
                   'compiler_directives': directives, 'warning_errors': True }
        
        for extension in self.extensions:
            for file_ in extension.sources:
                (file_, ext) = os.path.splitext(file_)
                path = os.path.join(basedir, file_)
                if ext != '.c':
                    continue 
                if os.path.exists(path + '.pyx'):
                    print('compiling %s to %s' % (file_ + '.pyx', file_ + ext))
                    res = cython_compile(path + '.pyx', full_module_name=extension.name,
                                         **options)
                    if res.num_errors != 0:
                        raise SystemExit('Cython encountered errors.')
                else:
                    print('%s is up to date' % (file_ + ext,))
                      
class test(setuptools_test.test):
    # Attributes defined outside init, required by setuptools.
    # pylint: disable=W0201
    description = "Run self-tests"
    user_options = (setuptools_test.test.user_options + 
                    [('debug=', None, 'Activate debugging for specified modules '
                                      '(separated by commas, specify "all" for all modules)')])


    def initialize_options(self):
        setuptools_test.test.initialize_options(self)
        self.debug = None

    def finalize_options(self):
        setuptools_test.test.finalize_options(self)
        self.test_loader = "ScanningLoader"
        if self.debug:
            self.debug = [ x.strip() for x  in self.debug.split(',') ]

    def run_tests(self):

        # Add test modules
        sys.path.insert(0, os.path.join(basedir, 'tests'))
        import unittest2 as unittest
        import _common
        from s3ql.common import (setup_excepthook, add_stdout_logging, LoggerFilter)

        # Initialize logging if not yet initialized
        root_logger = logging.getLogger()
        if not root_logger.handlers:
            add_stdout_logging(quiet=True)
            handler = logging.handlers.RotatingFileHandler("setup.log",
                                                           maxBytes=10*1024**2, backupCount=0)
            formatter = logging.Formatter('%(asctime)s.%(msecs)03d [%(process)s] %(threadName)s: '
                                          '[%(name)s] %(message)s', datefmt="%Y-%m-%d %H:%M:%S")
            handler.setFormatter(formatter)                                                
            root_logger.addHandler(handler)
            setup_excepthook()  
            if self.debug:
                root_logger.setLevel(logging.DEBUG)
                if 'all' not in self.debug:
                    root_logger.addFilter(LoggerFilter(self.debug, logging.INFO))
            else:
                root_logger.setLevel(logging.INFO) 
        else:
            root_logger.debug("Logging already initialized.")

        # Define our own test loader to order modules alphabetically
        from pkg_resources import resource_listdir, resource_exists
        class ScanningLoader(unittest.TestLoader):
            # Yes, this is a nasty hack
            # pylint: disable=W0232,W0221,W0622
            def loadTestsFromModule(self, module):
                """Return a suite of all tests cases contained in the given module"""
                tests = []
                if module.__name__!='setuptools.tests.doctest':  # ugh
                    tests.append(unittest.TestLoader.loadTestsFromModule(self,module))
                if hasattr(module, "additional_tests"):
                    tests.append(module.additional_tests())
                if hasattr(module, '__path__'):
                    for file in sorted(resource_listdir(module.__name__, '')):
                        if file.endswith('.py') and file!='__init__.py':
                            submodule = module.__name__+'.'+file[:-3]
                        else:
                            if resource_exists(
                                module.__name__, file+'/__init__.py'
                            ):
                                submodule = module.__name__+'.'+file
                            else:
                                continue
                        tests.append(self.loadTestsFromName(submodule))
                if len(tests)!=1:
                    return self.suiteClass(tests)
                else:
                    return tests[0] # don't create a nested suite for only one return
                
        unittest.main(
            None, None, [unittest.__file__]+self.test_args,
            testLoader = ScanningLoader())
        

class upload_docs(setuptools.Command):
    user_options = []
    boolean_options = []
    description = "Upload documentation"

    def initialize_options(self):
        pass

    def finalize_options(self):
        pass

    def run(self):
        subprocess.check_call(['rsync', '-aHv', '--del', os.path.join(basedir, 'doc', 'html') + '/',
                               'ebox.rath.org:/var/www/s3ql-docs/'])
        subprocess.check_call(['rsync', '-aHv', '--del', os.path.join(basedir, 'doc', 'manual.pdf'),
                               'ebox.rath.org:/var/www/s3ql-docs/'])

if __name__ == '__main__':
    main()
