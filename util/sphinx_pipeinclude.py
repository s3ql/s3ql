'''
sphinx_pipe.py - this file is part of S3QL (http://s3ql.googlecode.com)

Implements a Sphinx extension that provides a `pipeinclude` directive
to include the output of a program.


Copyright © 2008 Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU GPLv3.
'''

from docutils.parsers.rst.directives.misc import Include
import subprocess
import shlex
from docutils.utils.error_reporting import SafeString
import tempfile
import os.path
import sys

class PipeInclude(Include):
    """
    Include program output as ReST source.
    """

    def run(self):
        # To maximize code reuse, we just write the output in a temporary
        # file and call the base class. Otherwise we'd have to copy & paste
        # all the code to handle start-line, end-line etc options.

        source = self.state_machine.input_lines.source(
            self.lineno - self.state_machine.input_offset - 1)
        source_dir = os.path.dirname(os.path.abspath(source))

        command = self.arguments[0]
        command_list = shlex.split(command)

        if command_list[0] == 'python':
            command_list[0] = sys.executable

        with tempfile.NamedTemporaryFile() as fh:
            exitcode = subprocess.call(command_list, stdout=fh,
                                       cwd=source_dir)
            if exitcode != 0:
                raise self.severe('Problems with "%s" directive:\n'
                                  'Command %s returned with exit code %d' %
                                  (self.name, SafeString(command), exitcode))

            fh.flush()
            self.arguments[0] = fh.name
            return super().run()

def setup(app):
    app.add_directive('pipeinclude', PipeInclude)
