#!/usr/bin/env python

import os.path
import re
import sys
import subprocess

# Import pygccxml
basedir = os.path.abspath(os.path.dirname(sys.argv[0]))
sys.path.insert(0, os.path.join(basedir, 'src', 'pygccxml.zip'))

from pygccxml import parser
from pygccxml import declarations
from pyplusplus.module_builder import ctypes_module_builder_t, ctypes_decls_dependencies

# Work out compilation flags
proc = subprocess.Popen(['pkg-config', 'fuse', '--cflags'], stdout=subprocess.PIPE)
cflags = proc.stdout.readline().rstrip()
proc.stdout.close()
if proc.wait() != 0:
    sys.stderr.write('Failed to execute pkg-config. Exit code: %d.\n' 
                     % proc.returncode)
    sys.exit(1)
print 'Using cflags: %s' % cflags    
           
# Work out library location
proc = subprocess.Popen(['/sbin/ldconfig', '-p'], stdout=subprocess.PIPE)
shared_library_path = None
for line in proc.stdout:
    res = re.match('^\\s*libfuse\\.so\\.[0-9]+ \\(libc6\\) => (.+)$', line)
    if res is not None:
        shared_library_path = res.group(1)
        # Slurp rest of output
        for _ in proc.stdout:
            pass
        
proc.stdout.close()
if proc.wait() != 0:
    sys.stderr.write('Failed to execute /sbin/ldconfig. Exit code: %d.\n' 
                     % proc.returncode)
    sys.exit(1)

if shared_library_path is None:
    sys.stderr.write('Failed to locate fuse library libfuse.so.\n')
    sys.exit(1)
    
print 'Found fuse library in %s' % shared_library_path
    
header_file = os.path.join(basedir, 'src/fuse_ctypes.h')
gccxml_cfg = parser.gccxml_configuration_t(cflags=cflags)

mb = ctypes_module_builder_t( [header_file], shared_library_path, gccxml_config=gccxml_cfg )

# Work around bug in code generator
has_varargs = lambda f: f.arguments \
                        and isinstance( f.arguments[-1].type, declarations.ellipsis_t )
mb.calldefs( has_varargs ).exclude()
 
# Define names to generate
include_symbols = [
                   mb.global_ns.class_('fuse_lowlevel_ops'),
                   mb.global_ns.calldef('fuse_add_direntry'),
                   mb.global_ns.calldef('fuse_req_ctx'),
                   mb.global_ns.calldef('fuse_lowlevel_new')
                   ] 
# If we want to match several objects, we have to explicitly
# convert the result wrapper object into a list
include_symbols += list(mb.global_ns.calldefs(lambda f: f.name.startswith('fuse_reply_'))) 
                   
mb.global_ns.exclude()
for incl in include_symbols:
    incl.include()
for dep in ctypes_decls_dependencies.find_out_dependencies(include_symbols):
    dep.include()
            
mb.build_code_creator(shared_library_path)
mb.write_module(os.path.join(basedir, 'src', 's3ql', 'fuse_ctypes.py'))

os.unlink(os.path.join(basedir, 'src', 's3ql', 'exposed_decl.pypp.txt'))

print 'Code generation complete.'          
