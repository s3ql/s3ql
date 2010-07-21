'''
optparse.py - this file is part of S3QL (http://s3ql.googlecode.com)

Copyright (C) 2008-2009 Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU LGPL.


This module subclasses a few classes from the `optparse` module.

  - It sets a default for the --version option
  
  - It allows multiple paragraphs in the description
  
  - It adds a dummy option group `ArgumentGroup` that can be used
    to render help text for position arguments. The only `ArgumentGroup`
    method that should be called is `add_argument`. 
   
'''

from __future__ import division, print_function, absolute_import

import s3ql
import textwrap

# This is not a self import
#pylint: disable-msg=W0406
import optparse

__all__ = [ 'OptionParser', 'ArgumentGroup' ]

class HelpFormatter (optparse.IndentedHelpFormatter):
    # Looks like a pylint bug:
    #pylint: disable-msg=W0232
    def format_usage(self, usage):
        if '\n' in usage:
            variants = usage.split('\n')
            s =  'Usage: %s\n' % variants[0] 
            sp = '  or:  %s\n' 
            return s + '\n'.join(sp % x for x in variants[1:])
        else:
            return _("Usage: %s\n") % usage

    def _format_text(self, text):
        """
        Format one or more paragraphs of free-form text for inclusion in
        the help output at the current indentation level.
        """
        text_width = self.width - self.current_indent
        indent = " "*self.current_indent
        
        results = list()
        for prg in text.split('\n\n'):
            results.append(textwrap.fill(prg,
                             text_width,
                             initial_indent=indent,
                             subsequent_indent=indent))
        
        return '\n\n'.join(results)
              
    def format_argument(self, option):
        result = []
        #opts = self.option_strings[option]
        opts = option.name
        opt_width = self.help_position - self.current_indent - 2
        if len(opts) > opt_width:
            opts = "%*s%s\n" % (self.current_indent, "", opts)
            indent_first = self.help_position
        else:                       # start help on same line as opts
            opts = "%*s%-*s  " % (self.current_indent, "", opt_width, opts)
            indent_first = 0
        result.append(opts)
        
        help_text = option.help
        help_lines = textwrap.wrap(help_text, self.help_width)
        result.append("%*s%s\n" % (indent_first, "", help_lines[0]))
        result.extend(["%*s%s\n" % (self.help_position, "", line)
                       for line in help_lines[1:]])

        return "".join(result)
        
class OptionParser(optparse.OptionParser):
    
    def __init__(self, **kw):
        if 'version' not in kw:
            kw['version'] = 'S3QL %s' % s3ql.VERSION
        if 'formatter' not in kw:
            kw['formatter'] = HelpFormatter()
            
        optparse.OptionParser.__init__(self, **kw)
    
class Argument(object):
    
    def __init__(self, name, help_):
        self.name = name
        self.help = help_

                                           
class ArgumentGroup(optparse.OptionGroup):
    '''An option group that actually formats as a list of
    arguments'''
 
    def __init__(self, *a, **kw):
        optparse.OptionGroup.__init__(self, *a, **kw)
        self.arguments = dict()
        
    def format_argument_help(self, formatter):
        result = []
        for arg in self.arguments.itervalues():
            result.append(formatter.format_argument(arg))
        return "".join(result)

    def format_help(self, formatter):
        formatter.dedent()
        result = list()
        result.append(formatter.format_heading(self.title)[:-1])
        formatter.indent()
        
        if self.description:
            result.append(self.format_description(formatter))
            
        if self.arguments:
            result.append(self.format_argument_help(formatter))
            
        return "\n".join(result)
    
    def add_argument(self, name, help_):
        self.arguments[name] = Argument(name, help_)
