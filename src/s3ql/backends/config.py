'''
backends/config.py - this file is part of S3QL.

Copyright © 2008 Nikolaus Rath <Nikolaus@rath.org>

This work can be distributed under the terms of the GNU GPLv3.
'''

from __future__ import annotations


def parse_suboptions(value: str) -> dict[str, str | bool]:
    '''Parse a comma-separated backend suboption string into a mapping.

    Each element is either `key=value` or a bare flag `key` (mapped to `True`), for example
    `ssl,timeout=42,sse`.
    '''
    opts: dict[str, str | bool] = {}
    for raw_opt in value.split(','):
        opt = raw_opt.strip()
        if not opt:
            continue
        if '=' in opt:
            (key, val) = opt.split('=', 1)
            opts[key.strip()] = val.strip()
        else:
            opts[opt] = True

    return opts
