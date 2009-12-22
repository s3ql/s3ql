#!/usr/bin/env python

import os.path
import re
import sys
import tempfile
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
include_functions = [ 'fuse_mount', 'fuse_lowlevel_new', 'fuse_add_direntry',
                     'fuse_set_signal_handlers', 'fuse_session_add_chan',
                     'fuse_session_loop_mt', 'fuse_session_remove_chan',
                     'fuse_remove_signal_handlers', 'fuse_session_destroy',
                     'fuse_unmount', 'fuse_req_ctx' ]
include_structs = [ 'fuse_lowlevel_ops' ]
include_symbols = [  mb.global_ns.calldef(x) for x in include_functions ] 
include_symbols += [  mb.global_ns.class_(x) for x in include_structs ]

# If we want to match several objects, we have to explicitly
# convert the result wrapper object into a list
include_symbols += list(mb.global_ns.calldefs(lambda f: f.name.startswith('fuse_reply_'))) 

# This function is only defined for FUSE 2.8 and above
include_symbols += list(mb.global_ns.calldefs(lambda f: f.name == 'fuse_req_getgroups'))


                   
mb.global_ns.exclude()
for incl in include_symbols:
    incl.include()
for dep in ctypes_decls_dependencies.find_out_dependencies(include_symbols):
    dep.include()
            
mb.build_code_creator(shared_library_path)
code_path = os.path.join(basedir, 'src', 's3ql')
mb.write_module(os.path.join(code_path, 'fuse_ctypes.py'))

# Superfluous output file
os.unlink(os.path.join(code_path, 'exposed_decl.pypp.txt'))

# Add comments
for file in ['fuse_ctypes.py', 'ctypes_utils.py']:
    tmp = tempfile.TemporaryFile()
    code = open(os.path.join(code_path, file), "r+")
    for line in code:
        tmp.write(line)
    tmp.seek(0)
    code.seek(0)
    code.truncate()
    code.write(tmp.readline())
    code.write('#pylint: disable-all\n')
    code.write('#@PydevCodeAnalysisIgnore\n')
    for line in tmp:
        code.write(line)
    code.close()
    tmp.close()    

print 'Code generation complete.'          
