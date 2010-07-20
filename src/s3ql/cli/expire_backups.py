#!/usr/bin/env python
'''
expire_backups.py - this file is part of S3QL (http://s3ql.googlecode.com)

Copyright (C) 2008-2009 Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU LGPL.
'''

from __future__ import division, print_function, absolute_import


import sys
from optparse import OptionParser
import os
from ..common import QuietError, add_stdout_logging, setup_excepthook
from datetime import datetime, timedelta
import logging
import re
import s3ql
import textwrap
import shutil

log = logging.getLogger()


def parse_args(args):
    '''Parse command line'''

    parser = OptionParser(
        usage="%prog  [options] <age> <age> ... \n"
              "       %prog --help",
        version='S3QL %s' % s3ql.VERSION,
        description=textwrap.dedent('''
        This program deletes backups that are no longer needed as defined by the
        specified backup strategy. It uses a sophisticated algorithm that
        ensures that there will always be at least one backup available in each
        the specified  "generations". A generation is specified by giving its
        relative age compared to the younger generation. Please refer to the
        S3QL dokumentation for details.
        '''))

    parser.add_option("--quiet", action="store_true", default=False,
                      help="Be really quiet")
    parser.add_option("-n", action="store_true", default=False,
                      help="Dry run. Just show which backups would be deleted.")
    parser.add_option("--debug", action="store_true",
                      help="Activate debugging output")
    
    (options, pps) = parser.parse_args(args)

    # Verify parameters
    if len(pps) < 2:
        parser.error("Incorrect number of arguments.")
    options.generations = pps

    return options

def main(args=None):

    if args is None:
        args = sys.argv[1:]

    options = parse_args(args)

    # Initialize logging if not yet initialized
    root_logger = logging.getLogger()
    if not root_logger.handlers:
        handler = add_stdout_logging(options.quiet)
        setup_excepthook()  
        if options.debug:
            root_logger.setLevel(logging.DEBUG)
            handler.setLevel(logging.DEBUG)
        else:
            root_logger.setLevel(logging.INFO)         
    else:
        log.info("Logging already initialized.")

    # Relative generation ages
    generations = list()
    for (i, txt_gen) in enumerate(options.generations):
        if txt_gen.endswith('h'):
            generations.append(timedelta(seconds=int(txt_gen[:-1]) * 3600))
        elif txt_gen.endswith('d'):
            generations.append(timedelta(days=int(txt_gen[:-1])))
        else:
            raise QuietError('Invalid backup age: %r' % txt_gen)
        if i == 0:
            log.info('Generation %d starts %s after most recent backup',
                     i+1, txt_gen)
        else:
            log.info('Generation %d starts %s after first backup of generation %d',
                     i+1, txt_gen, i)

    # Determine available backups
    available_txt = sorted(x for x in os.listdir('.')
                           if re.match(r'^\d{4}-\d\d-\d\d_\d\d:\d\d:\d\d$', x))

    # Get most recent backup
    now = datetime.strptime(available_txt.pop(), '%Y-%m-%d_%H:%M:%S')
    log.info('Most recent backup is from %s', now)

    # Backups that are available
    available = list()
    i = 0
    for name in reversed(available_txt):
        age = now - datetime.strptime(name, '%Y-%m-%d_%H:%M:%S')
        available.append((name, age))
        i += 1
        log.info('Backup %d is from %s, age: %s', i, name, age)

    step = min(generations)
    if not available:
        return

    if available[0][1] < step:
        log.warn('NOTE: Your most recent backup is %s old, but according to your backup\n'
                 'strategy, it should be at least %s old before creating a new backup.\n'
                 'Are you sure that you specified the backup generations correctly?',
                 available[0][1], step)

    # Backups that need to be kept
    keep = dict()

    # Go forward in time to see what backups need to be kept
    simulated = timedelta(0)
    warn_missing = True
    while True:
        log.debug('Considering situation on %s', now + simulated)

        # Go through generations
        cur_age = timedelta(0)
        done = True
        for (i, min_rel_age) in enumerate(generations):
            if cur_age < simulated: # Future backup
                cur_age = cur_age + min_rel_age
                continue
            done = False
            min_age = cur_age + min_rel_age
            log.debug('Minimum age for generation %d is %s', i+1, min_age)
            for (j, age) in enumerate((a[1] for a in available)):
                if age >= min_age:
                    if j not in keep:
                        if simulated:
                            log.info('Keeping backup %d, required for generation %d in %s hours',
                                     j+1, i + 1, simulated)
                        else:
                            log.info('Keeping backup %d for generation %d',
                                     j+1, i + 1)
                    cur_age = age
                    keep[j] = True
                    break
            else:
                if available:
                    keep[len(available) - 1] = True
                    if warn_missing:
                        if simulated:
                            log.warn('There will be no sufficiently old backup for generation %d in %s hours, '
                                     'keeping backup %d instead.\n'
                                     '(further warnings about missing backups will be suppressed)',
                                     i + 1, simulated, len(available))
                        else:
                            log.warn('Found no sufficiently old backup for generation %d, '
                                     'keeping backup %d instead.\n'
                                     '(further warnings about missing backups will be suppressed)',
                                     i + 1, len(available))

                        warn_missing = False
                else:
                    if warn_missing:
                        if simulated:
                            log.warn('There will be no backup for generation %d in %s hours\n'
                                     '(further warnings about missing backups will be suppressed)',
                                     simulated, i + 1)
                        else:
                            log.warn('Found no backup for generation %d\n'
                                     '(further warnings about missing backups will be suppressed)',
                                     simulated, i + 1)

                        warn_missing = False
                break

        # Update backup ages
        simulated += step
        available = [ (a[0], a[1] + step) for a in available ]

        if done:
            break

    # Remove what's left
    for name in [ available[i][0] for i in range(len(available)) if i not in keep ]:
        log.info('Backup %s is no longer needed, removing...', name)
        if not options.n:
            shutil.rmtree(name)


if __name__ == '__main__':
    main(sys.argv[1:])
