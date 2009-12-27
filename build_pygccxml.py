#!/usr/bin/env python

import os.path
import re
import sys
import tempfile
import subprocess

# Define names to generate
include_prefixes = [ 'fuse_reply_' ]
include_symbols = [ 'fuse_mount', 'fuse_lowlevel_new', 'fuse_add_direntry',
                   'fuse_set_signal_handlers', 'fuse_session_add_chan',
                   'fuse_session_loop_mt', 'fuse_session_remove_chan',
                   'fuse_remove_signal_handlers', 'fuse_session_destroy',
                   'fuse_unmount', 'fuse_req_ctx', 'fuse_lowlevel_ops',
                   'fuse_session_loop' ]
    
# Import pygccxml
basedir = os.path.abspath(os.path.dirname(sys.argv[0]))
sys.path.insert(0, os.path.join(basedir, 'src', 'pygccxml.zip'))

from pygccxml import parser
from pygccxml import declarations
from pyplusplus.module_builder import ctypes_module_builder_t, ctypes_decls_dependencies

def main():
    '''Create ctypes API to local FUSE headers'''
    
    print 'Creating ctypes API from local fuse headers...'

    cflags = get_cflags()
    print 'Using cflags: %s' % ' '.join(cflags)  
    
    shared_library_path = get_library_path()
    print 'Found fuse library in %s' % shared_library_path    
    
    header_file = os.path.join(basedir, 'src', 'fuse_ctypes.h')
    gccxml_cfg = parser.gccxml_configuration_t(cflags=' '.join(cflags))
    mb = ctypes_module_builder_t( [header_file], shared_library_path, gccxml_config=gccxml_cfg )
    
    # Work around bug in code generator
    has_varargs = lambda f: f.arguments \
                            and isinstance( f.arguments[-1].type, declarations.ellipsis_t )
    mb.calldefs( has_varargs ).exclude()
     
    # If we want to match sev[eral objects, we have to explicitly
    # convert the result wrapper object into a list
    include = list()
    for prefix in include_prefixes:
        include += list(mb.global_ns.decls(lambda f: f.name.startswith(prefix))) 
    include += [  mb.global_ns.decl(x) for x in include_symbols ]
                   
    mb.global_ns.exclude()
    for incl in include:
        incl.include()
    for dep in ctypes_decls_dependencies.find_out_dependencies(include):
        dep.include()

    monkeypatch()              
    mb.build_code_creator(shared_library_path)
    code_path = os.path.join(basedir, 'src', 'llfuse')
    mb.write_module(os.path.join(code_path, 'ctypes_api.py'))
    
    # Add variables
    # This is horribly monkeypatched. We should really find a way
    # to have py++ export the declarations. 
    include_vars_prefixes = [ 'D_' ]
    fh = open(os.path.join(code_path, 'ctypes_api.py'), 'a')
    fh.write('\n')
    for prefix in include_vars_prefixes:
        for var in mb.global_ns.decls(lambda f: f.name.startswith(prefix)):
            fh.write('{0} = {1}\n'.format(var.name[2:], var._value))
    fh.close()        
               
    # Superfluous output file
    os.unlink(os.path.join(code_path, 'exposed_decl.pypp.txt'))
    
    # Add comments
    for file in ['ctypes_api.py', 'ctypes_utils.py']:
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

def get_cflags():
    '''Get cflags required to compile with fuse library''' 
    
    proc = subprocess.Popen(['pkg-config', 'fuse', '--cflags'], stdout=subprocess.PIPE)
    cflags = proc.stdout.readline().rstrip()
    proc.stdout.close()
    if proc.wait() != 0:
        sys.stderr.write('Failed to execute pkg-config. Exit code: %d.\n' 
                         % proc.returncode)
        sys.stderr.write('Check that the FUSE development package been installed properly.\n')
        sys.exit(1)
    return cflags.split()

  

def get_library_path():
    '''Find location of libfuse.so'''
    
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
        
    return shared_library_path

def monkeypatch():     
    '''Monkeypatch into py++
    
    Makes Py++ generate POINTER(c_char)
    instead of c_char_p (since c_char_p is assumed to be \0
    terminated and autoconverted to a Python string.
    '''
    
    from pygccxml import declarations   
    from pyplusplus.code_creators import ctypes_formatter
    class my_type_converter_t(ctypes_formatter.type_converter_t): 
        def visit_pointer( self ):
            no_ptr = declarations.remove_const( declarations.remove_pointer( self.user_type ) )
            #if declarations.is_same( declarations.char_t(), no_ptr ):
            #    return "ctypes.c_char_p"
            if declarations.is_same( declarations.wchar_t(), no_ptr ):
                return "ctypes.c_wchar_p"
            elif declarations.is_same( declarations.void_t(), no_ptr ):
                return "ctypes.c_void_p"
            else:
                base_visitor = my_type_converter_t( self.user_type.base, self.decl_formatter )
                internal_type_str = declarations.apply_visitor( base_visitor, base_visitor.user_type )
                return "ctypes.POINTER( %s )" % internal_type_str   
        def visit_reference( self ):
            no_ref = declarations.remove_const( declarations.remove_reference( self.user_type ) )
            #if declarations.is_same( declarations.char_t(), no_ref ):
            #    return "ctypes.c_char_p"
            if declarations.is_same( declarations.wchar_t(), no_ref ):
                return "ctypes.c_wchar_p"
            elif declarations.is_same( declarations.void_t(), no_ref ):
                return "ctypes.c_void_p"
            else:
                base_visitor = my_type_converter_t( self.user_type.base, self.decl_formatter )
                internal_type_str = declarations.apply_visitor( base_visitor, base_visitor.user_type )
                return "ctypes.POINTER( %s )" % internal_type_str         
    ctypes_formatter.type_converter_t = my_type_converter_t
              
if __name__ == '__main__':
    main()    
