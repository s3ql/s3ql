'''
sphinx_pipe.py - this file is part of S3QL (http://s3ql.googlecode.com)

Implements a Sphinx extension that provides a `pipeinclude` directive
to include the output of a program.


Copyright (C) 2008-2011 Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU GPLv3.
'''

from docutils.parsers.rst.directives.misc import Include
import subprocess
import shlex
from docutils import io, nodes, statemachine
import os.path
import sys

class PipeInclude(Include):
    """
    Include program output as ReST source.
    """

    def run(self):
        source = self.state_machine.input_lines.source(
            self.lineno - self.state_machine.input_offset - 1)
        source_dir = os.path.dirname(os.path.abspath(source))

        command = self.arguments[0]
        command_list = shlex.split(command)

        if command_list[0] == 'python':
            command_list[0] = sys.executable

        encoding = self.options.get(
            'encoding', self.state.document.settings.input_encoding)
        tab_width = self.options.get(
            'tab-width', self.state.document.settings.tab_width)

        try:
            child = subprocess.Popen(command_list, stdout=subprocess.PIPE,
                                     cwd=source_dir, universal_newlines=True)
            include_file = io.FileInput(
                source=child.stdout, encoding=encoding,
                error_handler=(self.state.document.settings.\
                               input_encoding_error_handler),
                handle_io_errors=None)
        except IOError as error:
            raise self.severe('Problems with "%s" directive path:\n%s: %s.' %
                        (self.name, error.__class__.__name__, str(error)))
            # Hack: Since Python 2.6, the string interpolation returns a
            # unicode object if one of the supplied %s replacements is a
            # unicode object. IOError has no `__unicode__` method and the
            # fallback `__repr__` does not report the file name. Explicitely
            # converting to str fixes this for now::
            #   print '%s\n%s\n%s\n' %(error, str(error), repr(error))
        startline = self.options.get('start-line', None)
        endline = self.options.get('end-line', None)
        try:
            if startline or (endline is not None):
                include_lines = include_file.readlines()
                include_text = ''.join(include_lines[startline:endline])
            else:
                include_text = include_file.read()
        except UnicodeError as error:
            raise self.severe(
                'Problem with "%s" directive:\n%s: %s'
                % (self.name, error.__class__.__name__, error))
        # start-after/end-before: no restrictions on newlines in match-text,
        # and no restrictions on matching inside lines vs. line boundaries
        after_text = self.options.get('start-after', None)
        if after_text:
            # skip content in include_text before *and incl.* a matching text
            after_index = include_text.find(after_text)
            if after_index < 0:
                raise self.severe('Problem with "start-after" option of "%s" '
                                  'directive:\nText not found.' % self.name)
            include_text = include_text[after_index + len(after_text):]
        before_text = self.options.get('end-before', None)
        if before_text:
            # skip content in include_text after *and incl.* a matching text
            before_index = include_text.find(before_text)
            if before_index < 0:
                raise self.severe('Problem with "end-before" option of "%s" '
                                  'directive:\nText not found.' % self.name)
            include_text = include_text[:before_index]
        if 'literal' in self.options:
            # Convert tabs to spaces, if `tab_width` is positive.
            if tab_width >= 0:
                text = include_text.expandtabs(tab_width)
            else:
                text = include_text
            literal_block = nodes.literal_block(include_text, text,
                                                source=command)
            literal_block.line = 1
            return [literal_block]
        else:
            include_lines = statemachine.string2lines(
                include_text, tab_width, convert_whitespace=1)
            self.state_machine.insert_input(include_lines, command)
            return []


def setup(app):
    app.add_directive('pipeinclude', PipeInclude)
