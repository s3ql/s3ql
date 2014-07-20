#!/usr/bin/env python3

import re
import os
import subprocess
import sys
import shutil
import ast
from argparse import ArgumentParser

trailing_w_re = re.compile(r'\s+\n$')
only_w_re = re.compile(r'^\s+\n$')

def check_whitespace(name, correct=False):
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

def get_definitions(path):
    '''Yield all objects defined directly in *path*

    This does not include imported objects, or objects defined
    dynamically (e.g. by using eval, or modifying globals()).
    '''

    names = set()
    for node in ast.parse(open(path, 'rb').read()).body:
        for name in _iter_definitions(node):
            names.add(name)
    return names

def _iter_definitions(node):
    if isinstance(node, ast.Assign):
        for node in node.targets:
            while isinstance(node, ast.Attribute):
                node = node.value
            assert isinstance(node, ast.Name)
            yield node.id
    elif isinstance(node, (ast.FunctionDef, ast.ClassDef)):
        yield node.name
    elif isinstance(node, ast.If):
        for snode in node.body:
            yield from _iter_definitions(snode)
        for snode in node.orelse:
            yield from _iter_definitions(snode)
    elif isinstance(node, ast.Try):
        for snode in (node.body, node.finalbody, node.orelse):
            for ssnode in snode:
                yield from _iter_definitions(ssnode)
        for snode in node.handlers:
            assert isinstance(snode, ast.ExceptHandler)
            for ssnode in snode.body:
                yield from _iter_definitions(ssnode)

def iter_imports(path):
    '''Yield imports in *path*'''

    for node in ast.parse(open(path, 'rb').read()).body:
        if isinstance(node, ast.ImportFrom):
            if node.module is None:
                prefix = ()
            else:
                prefix = tuple(node.module.split('.'))
            for snode in node.names:
                yield (node.level, prefix + (snode.name,))
        elif isinstance(node, ast.Import):
            for node in node.names:
                yield (0, tuple(node.name.split('.')))

def yield_modules(path):
    '''Yield all Python modules underneath *path*'''

    for (dpath, dnames, fnames) in os.walk(path):
        module = tuple(dpath.split('/')[1:])
        for fname in fnames:
            if not fname.endswith('.py'):
                continue
            fpath = os.path.join(dpath, fname)
            if fname == '__init__.py':
                yield (fpath, module)
            else:
                yield (fpath, module + (fname[:-3],))

        dnames[:] = [ x for x in dnames
                      if os.path.exists(os.path.join(dpath, x, '__init__.py')) ]

def check_imports():
    '''Check if all imports are direct'''

    # Memorize where objects are defined
    definitions = dict()
    for (fpath, modname) in yield_modules('src'):
        definitions[modname] = get_definitions(fpath)

    # Special case, we always want to import these indirectly
    definitions['s3ql', 'logging'].add('logging')
    definitions['s3ql',].add('ROOT_INODE')

    # False positives
    definitions['s3ql',].add('deltadump')

    # Check if imports are direct
    found_problems = False
    for path in ('src', 'util', 'contrib', 'tests'):
        for (fpath, modname) in yield_modules(path):
            for (lvl, name) in iter_imports(fpath):
                if lvl:
                    if lvl and fpath.endswith('/__init__.py'):
                        lvl += 1
                    name = modname[:len(modname)-lvl] + name
                if name in definitions:
                    # Import of entire module
                    continue

                mod_idx = len(name)-1
                while mod_idx > 0:
                    if name[:mod_idx] in definitions:
                        break
                    mod_idx -= 1
                else:
                    # No definitions on record
                    continue

                if name[mod_idx] not in definitions[name[:mod_idx]]:
                    print('%s imports %s from %s, but is defined elsewhere'
                          % (fpath, name[mod_idx], '.'.join(name[:mod_idx])))
                    found_problems = True

    return found_problems

def parse_args():
    parser = ArgumentParser(
        description="Check if tracked files are ready for commit")

    parser.add_argument("--fix-whitespace", action="store_true", default=False,
                      help="Automatically correct whitespace problems")

    return parser.parse_args()

options = parse_args()

os.chdir(os.path.dirname(__file__))

found_problems = False
if not check_imports():
    found_problems = True

hg_out = subprocess.check_output(['hg', 'status', '--clean', '--modified', '--added',
                                  '--no-status', '--print0'])
for b_name in hg_out.split(b'\0'):
    if not b_name:
        continue
    name = b_name.decode('utf-8', errors='surrogatescape')
    if check_whitespace(name, correct=options.fix_whitespace):
        found_problems = True

if found_problems:
    sys.exit(1)
else:
    sys.exit(0)
