'''
cmdline_lexer.py - this file is part of S3QL (http://s3ql.googlecode.com)

Copyright © 2008 Nikolaus Rath <Nikolaus.org>

This program can be distributed under the terms of the GNU GPLv3.
'''

from pygments.token import Comment, Name, Generic, Literal
from pygments.lexer import RegexLexer

__all__ = [ 'CommandLineLexer' ]

class CommandLineLexer(RegexLexer):
    """
    A lexer that highlights a command line with variable parts
    """

    name = 'CommandLine'
    aliases = ['commandline']
    mimetypes = []

    tokens = {
        'root': [
            (r'#.*\n', Comment),
            (r'[^[<]+', Literal),
            (r'\[[^[\]]+\]', Generic.Emph),
            (r'<[^>]+>', Name.Variable),
            ],

    }
