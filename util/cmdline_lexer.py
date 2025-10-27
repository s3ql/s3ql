'''
cmdline_lexer.py - this file is part of S3QL.

Copyright © 2008 Nikolaus Rath <Nikolaus@rath.org>

This work can be distributed under the terms of the GNU GPLv3.
'''
# mypy: ignore-errors

from pygments.lexer import RegexLexer
from pygments.token import Comment, Generic, Literal, Name

__all__ = ['CommandLineLexer']


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
