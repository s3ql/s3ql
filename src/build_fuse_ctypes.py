import os.path
import sys
import subprocess
from pygccxml import parser
from pygccxml import declarations
from pyplusplus.module_builder import ctypes_module_builder_t, ctypes_decls_dependencies

pkgconf = subprocess.Popen(['pkg-config', 'fuse', '--cflags'], stdout=subprocess.PIPE)
(stdout, stderr) = pkgconf.communicate() # stderr will be None
if pkgconf.returncode != 0:
    sys.stderr.write('Failed to execute pkg-config. Exit code: %d.\n' 
                     % pkgconf.returncode)
    sys.exit(1)
    
# This confuses pylint
#pylint: disable-msg=E1103
cflags = stdout.lstrip().rstrip()
       
basedir = os.path.dirname(__file__)

header_file = os.path.join(basedir, 'fuse_ctypes.h')
symbols_file = '/usr/lib/libfuse.so'
shared_library_file = 'libfuse.so'

gccxml_cfg = parser.gccxml_configuration_t(cflags=cflags)

mb = ctypes_module_builder_t( [header_file], symbols_file, gccxml_config=gccxml_cfg )
#mb = ctypes_module_builder_t( [header_file], None, gccxml_config=gccxml_cfg )

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
            
mb.build_code_creator(shared_library_file)
mb.write_module(os.path.join(basedir, 's3ql', 'fuse_ctypes.py'))

