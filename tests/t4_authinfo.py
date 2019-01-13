#!/usr/bin/env python3
'''
t4_authinfo.py - this file is part of S3QL.

Copyright © 2019 Nikolaus Rath <Nikolaus@rath.org>

This work can be distributed under the terms of the GNU GPLv3.
'''

if __name__ == '__main__':
    import pytest
    import sys
    sys.exit(pytest.main([__file__] + sys.argv[1:]))

from argparse import Namespace
import tempfile
import shutil
import subprocess
import pytest


def test_invalid_option(s3ql_cmd_argv, reg_output):
    with tempfile.NamedTemporaryFile('wt') as fh:
        print('[entry1]',
              'storage-url: local:///foo',
              'invalid-option: bla',
              '',
              file=fh, sep='\n')
        fh.flush()

        proc = subprocess.Popen(s3ql_cmd_argv('fsck.s3ql') +
                                [ '--quiet', '--authfile', fh.name,
                                  '--log', 'none', 'local:///foo/bar' ],
                                stdin=subprocess.PIPE, universal_newlines=True)
        proc.stdin.close()
        assert proc.wait() == 2

        reg_output(r"ERROR: '/com' does not exist", count=1)
        proc = subprocess.Popen(s3ql_cmd_argv('fsck.s3ql') +
                                [ '--quiet', '--authfile', fh.name,
                                  '--log', 'none', 'local:///com' ],
                                stdin=subprocess.PIPE, universal_newlines=True)
        proc.stdin.close()
        assert proc.wait() == 16


def test_invalid_backend_option(s3ql_cmd_argv):
    with tempfile.NamedTemporaryFile('wt') as fh:
        print('[entry1]',
              'storage-url: local:///foo',
              'backend-options: invalid-key',
              file=fh, sep='\n')
        fh.flush()

        proc = subprocess.Popen(s3ql_cmd_argv('fsck.s3ql') +
                                [ '--quiet', '--authfile', fh.name,
                                  '--log', 'none', 'local:///foo/bar' ],
                                stdin=subprocess.PIPE, universal_newlines=True)
        proc.stdin.close()
        assert proc.wait() == 3


def test_option_precedence(s3ql_cmd_argv, reg_output):
    with tempfile.NamedTemporaryFile('wt') as fh:
        print('[entry1]',
              'storage-url: s3://',
              'backend-options: invalid-key',
              '',
              '[entry2]',
              'storage-url: s3://foo',
              'backend-options: no-ssl',
              file=fh, sep='\n')
        fh.flush()

        reg_output(r"ERROR: Invalid storage URL", count=1)
        proc = subprocess.Popen(s3ql_cmd_argv('fsck.s3ql') +
                                [ '--quiet', '--authfile', fh.name,
                                  '--log', 'none', 's3://foo' ],
                                stdin=subprocess.PIPE, universal_newlines=True)
        proc.stdin.close()
        assert proc.wait() == 2


@pytest.fixture()
def context():
    ctx = Namespace()
    ctx.cache_dir = tempfile.mkdtemp(prefix='s3ql-cache-')
    ctx.backend_dir = tempfile.mkdtemp(prefix='s3ql-backend-')
    ctx.storage_url = 'local://' + ctx.backend_dir

    yield ctx

    shutil.rmtree(ctx.cache_dir)
    shutil.rmtree(ctx.backend_dir)


def test_passphrase(context, reg_output, s3ql_cmd_argv):
    passphrase = 'out3d'
    proc = subprocess.Popen(s3ql_cmd_argv('mkfs.s3ql') +
                            ['-L', 'test fs', '--max-obj-size', '500',
                             '--authfile', '/dev/null', '--cachedir', context.cache_dir,
                             '--quiet', context.storage_url ],
                            stdin=subprocess.PIPE, universal_newlines=True)

    print(passphrase, file=proc.stdin)
    print(passphrase, file=proc.stdin)
    proc.stdin.close()
    assert proc.wait() == 0
    reg_output(r'^WARNING: Maximum object sizes less than '
                    '1 MiB will degrade performance\.$', count=1)

    with tempfile.NamedTemporaryFile('wt') as fh:
        print('[entry1]',
              'storage-url: local://',
              'fs-passphrase: clearly wrong',
              '',
              '[entry2]',
              'storage-url: %s' % context.storage_url,
              'fs-passphrase: %s' % passphrase,
              file=fh, sep='\n')
        fh.flush()

        proc = subprocess.Popen(s3ql_cmd_argv('fsck.s3ql') +
                                [ '--quiet', '--authfile', fh.name,
                                  '--cachedir', context.cache_dir, '--log', 'none',
                                  context.storage_url ],
                                stdin=subprocess.PIPE, universal_newlines=True)
        proc.stdin.close()
        assert proc.wait() == 0
