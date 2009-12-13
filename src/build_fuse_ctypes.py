from pygccxml import utils
import os.path
from pygccxml import parser
from pygccxml import declarations
from pyplusplus.module_builder import ctypes_module_builder_t

basedir = os.path.basename(__file__)

header_file = os.path.join(basedir, 'myheader.h')
symbols_file = '/usr/lib/libfuse.so'
shared_library_file = '/usr/lib/libfuse.so'

gccxml_cfg = parser.gccxml_configuration_t(cflags='-D_FILE_OFFSET_BITS=64 -I/usr/include/fuse')

mb = ctypes_module_builder_t( [header_file], symbols_file, gccxml_cfg )

# Work around bug in code generator
has_varargs = lambda f: f.arguments \
                        and isinstance( f.arguments[-1].type, declarations.ellipsis_t )
mb.calldefs( has_varargs ).exclude()

mb.build_code_creator(shared_library_file)
mb.write_module(os.path.join(basedir, 's3ql', 'fuse_ctypes.py'))
