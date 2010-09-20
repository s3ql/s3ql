#!/usr/bin/env python
'''
expire_backups.py - this file is part of S3QL (http://s3ql.googlecode.com)

Copyright (C) 2008-2009 Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU LGPL.
'''

from __future__ import division, print_function, absolute_import


import sys
import os
from datetime import datetime, timedelta
import logging
import re
import textwrap
import shutil
import argparse

# We are running from the S3QL source directory, make sure
# that we use modules from this directory
basedir = os.path.abspath(os.path.join(os.path.dirname(sys.argv[0]), '..'))
if (os.path.exists(os.path.join(basedir, 'setup.py')) and
    os.path.exists(os.path.join(basedir, 'src', 's3ql', '__init__.py'))):
    sys.path = [os.path.join(basedir, 'src')] + sys.path
    
from s3ql.common import (setup_logging)
from s3ql.argparse import ArgumentParser
import s3ql.cli.remove
    
log = logging.getLogger('expire_backups')


def parse_args(args):
    '''Parse command line'''

    parser = ArgumentParser(
        description=textwrap.dedent('''\
        This program deletes backups that are no longer needed as defined by the
        specified backup strategy. It uses a sophisticated algorithm that
        ensures that there will always be at least one backup available in each
        of a number of age ranges. Age ranges are defined by giving the end
        of each range relative to the end of the previous range. The
        first range ends with the most recent backup. If there is no backup
        available for a age range, the ends of all previous ranges are
        shifted backwards in time until there is a backup available.
        Please refer to the S3QL documentation for details.
        '''))

    parser.add_quiet()
    parser.add_debug()
    parser.add_version()
    
    def age_type(s):
        if s.endswith('h'):
            return -timedelta(seconds=int(s[:-1]) * 3600)
        elif s.endswith('d'):
            return -timedelta(days=int(s[:-1]))
        else:
            raise argparse.ArgumentTypeError('%s is not a valid age.' % s)          
        
    parser.add_argument('ages', nargs='+', help='Relative beginnings of age ranges to keep',
                        type=age_type, metavar='<age>')

    parser.add_argument("-n", action="store_true", default=False,
                        help="Dry run. Just show which backups would be deleted.")

    parser.add_argument("--use-s3qlrm", action="store_true",
                      help="Use `s3qlrm` command to delete backups.")
        
    return parser.parse_args(args)

def main(args=None):

    if args is None:
        args = sys.argv[1:]

    options = parse_args(args)
    setup_logging(options)

    # Relative generation ages
    ages = options.ages
    log.info('Age range 0 ends with most recent backup')
    for (i, gen) in enumerate(ages):
        log.info('Age range %d ends %s hours before end of range %d',
                 i+1, -gen, i)

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
        age = datetime.strptime(name, '%Y-%m-%d_%H:%M:%S')
        available.append((name, age))
        i += 1
        log.info('Backup %d is from %s, age: %s hours', i, name, age)
 
    if not available:
        return
    step = -max(ages)
    log.debug('Stepsize is %s hours', step)

    if now - available[0][1] < step:
        log.warn('NOTE: Your most recent backup is %s hours old, but according to your backup\n'
                 'strategy, it should be at least %s hours old before creating a new backup.\n'
                 'Are you sure that you specified the age ranges correctly?',
                 now - available[0][1], step)

    # Backups that need to be kept
    keep = dict()

    # Go forward in time to see what backups need to be kept
    simulated = timedelta(0)
    warn_missing = True
    while True:
        log.debug('Considering situation on %s', now + simulated)

        # Determine age ranges
        ranges = list()
        beginning = available[0][1] + simulated # beginning of current range
        for (i, min_rel_age) in enumerate(ages):
            beginning += min_rel_age
            log.debug('Age range %d ends at %s', i+1, beginning)
            ranges.append(beginning)
            
        for i in range(len(ranges)-1):
            
            # If that range is actually in the future, we don't need a backup
            # for it
            if ranges[i] > now:
                continue
            
            for (j, age) in enumerate((a[1] for a in available[1:])):
                if age < ranges[i]:
                    if age <= ranges[i+1]:
                        dt = age - ranges[i+1]
                        log.debug('No backup for age range %d, moving subsequent '
                                  'ranges backward by %s hours', i+1, -dt)
                        for k in range(i+1, len(ranges)):
                            ranges[k] += dt
                            log.debug('Age range %d now ends at %s', k+1, ranges[k])
                            
                    if j not in keep:
                        log.info('Keeping backup %d, for age range %d%s',
                                 j+1, i + 1, format_future(simulated))
                    else:
                        log.debug('Keeping backup %d, for age range %d%s',
                                  j+1, i + 1, format_future(simulated))
                    keep[j] = True
                    break
            else:
                if available:
                    keep[len(available) - 1] = True
                    if warn_missing:
                        log.warn('There will be no sufficiently old backup for age range %d%s '
                                 'keeping backup %d instead.\n'
                                 '(further warnings about missing backups will be suppressed)',
                                 i + 1, format_future(simulated), len(available))
                        warn_missing = False
                else:
                    if warn_missing:
                        log.warn('There will be no backup for age range %d%s\n'
                                 '(further warnings about missing backups will be suppressed)',
                                 format_future(simulated), i + 1)
                        warn_missing = False
                break

        # If all ranges are in the future, we are done
        if ranges[-1] > now:
            break
        
        simulated += step


    # Remove what's left
    for name in [ available[i][0] for i in range(len(available)) if i not in keep ]:
        log.info('Backup %s is no longer needed, removing...', name)
        if not options.n:
            if options.use_s3qlrm:
                s3ql.cli.remove.main([name])
            else:
                shutil.rmtree(name)

def format_future(delta):
    if delta:
        return ' in %s hours' % delta
    else:
        return ''
    
if __name__ == '__main__':
    main(sys.argv[1:])
