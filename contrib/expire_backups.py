#!/usr/bin/env python3
'''
expire_backups.py - this file is part of S3QL.

Copyright © 2008 Nikolaus Rath <Nikolaus@rath.org>

This work can be distributed under the terms of the GNU GPLv3.
'''

import logging
import os
import re
import shutil
import sys
from collections import defaultdict
from collections.abc import Sequence
from datetime import datetime, timedelta
from typing import Annotated

import typer

from s3ql.common import freeze_basic_mapping, thaw_basic_mapping
from s3ql.logging import QuietError, setup_logging
from s3ql.parse_args import (
    DebugFlag,
    DebugModules,
    LogDest,
    QuietFlag,
    run_app,
    trio_command,
)
from s3ql.remove import main as s3qlrm

log = logging.getLogger(__name__)

app = typer.Typer(
    add_completion=False,
    rich_markup_mode=None,
    pretty_exceptions_enable=False,
    help='Intelligently remove old backups that are no longer needed. To define what backups you '
    'want to keep for how long, you define a number of age ranges in terms of backup cycles; '
    'expire_backups ensures that you have at least one backup in each age range at all times. '
    'Please refer to the S3QL documentation for details.',
)


@app.command()
@trio_command
async def expire_backups(
    cycles: Annotated[
        list[int],
        typer.Argument(metavar='<age>', help='Age range boundaries in terms of backup cycles'),
    ],
    state: Annotated[
        str, typer.Option(metavar='<file>', help='File to save state information in')
    ] = '.expire_backups.dat',
    dry_run: Annotated[
        bool, typer.Option('-n', help='Dry run. Just show which backups would be deleted.')
    ] = False,
    proportion_delete: Annotated[
        float,
        typer.Option(
            '-p',
            '--proportion-delete',
            metavar='<N>',
            help='Maximum proportion of backups to delete (between 0 and 1).',
        ),
    ] = 0.5,
    reconstruct_state: Annotated[
        bool,
        typer.Option(
            '--reconstruct-state', help='Try to reconstruct a missing state file from backup dates.'
        ),
    ] = False,
    use_s3qlrm: Annotated[
        bool, typer.Option('--use-s3qlrm', help='Use `s3qlrm` command to delete backups.')
    ] = False,
    log_target: LogDest = None,
    debug: DebugFlag = False,
    debug_modules: DebugModules = None,
    quiet: QuietFlag = False,
) -> None:
    '''Intelligently remove old backups that are no longer needed.'''
    setup_logging(quiet=quiet, log=log_target, debug=debug, debug_modules=debug_modules)

    if sorted(cycles) != cycles:
        raise typer.BadParameter('Age range boundaries must be in increasing order')
    if not (0 < proportion_delete <= 1):
        raise typer.BadParameter('Proportion of backups to delete must be between 0 and 1')

    # Determine available backups
    backup_list = set(
        x for x in os.listdir('.') if re.match(r'^\d{4}-\d\d-\d\d_\d\d:\d\d:\d\d$', x)
    )

    if not os.path.exists(state) and len(backup_list) > 1:
        if not reconstruct_state:
            raise QuietError('Found more than one backup but no state file! Aborting.')

        log.warning('Trying to reconstruct state file..')
        saved_state = upgrade_to_state(backup_list)
        if not dry_run:
            log.info('Saving reconstructed state..')
            with open(state, 'wb') as fh:
                fh.write(freeze_basic_mapping(saved_state))
    elif not os.path.exists(state):
        log.warning('Creating state file..')
        saved_state = dict()
    else:
        log.info('Reading state...')
        with open(state, 'rb') as fh:
            saved_state = thaw_basic_mapping(fh.read())

    to_delete = process_backups(backup_list, saved_state, cycles)

    if len(backup_list) and (len(to_delete) / len(backup_list) > proportion_delete):
        raise QuietError(
            'Would remove more than %d%% of backups, aborting' % (proportion_delete * 100)
        )

    for x in to_delete:
        log.info('Backup %s is no longer needed, removing...', x)
        if not dry_run:
            if use_s3qlrm:
                s3qlrm([x])
            else:
                shutil.rmtree(x)

    if dry_run:
        log.info('Dry run, not saving state.')
    else:
        log.info('Saving state..')
        with open(state + '.new', 'wb') as fh:
            fh.write(freeze_basic_mapping(saved_state))
        if os.path.exists(state):
            os.rename(state, state + '.bak')
        os.rename(state + '.new', state)


def main(args: Sequence[str] | None = None) -> None:
    run_app(app, args, prog_name='expire_backups.py')


def upgrade_to_state(backup_list):
    log.info('Several existing backups detected, trying to convert absolute ages to cycles')

    now = datetime.now()
    age = dict()
    for x in sorted(backup_list):
        age[x] = now - datetime.strptime(x, '%Y-%m-%d_%H:%M:%S')
        log.info('Backup %s is %s hours old', x, age[x])

    deltas = [abs(x - y) for x in age.values() for y in age.values() if x != y]
    step = min(deltas)
    log.info('Assuming backup interval of %s hours', step)

    state = dict()
    for x in sorted(age):
        state[x] = 0
        while age[x] > timedelta(0):
            state[x] += 1
            age[x] -= step
        log.info('Backup %s is %d cycles old', x, state[x])

    log.info('State construction complete.')
    return state


def process_backups(backup_list, state, cycles):
    # New backups
    new_backups = backup_list - set(state)
    for x in sorted(new_backups):
        log.info('Found new backup %s', x)
        for y in state:
            state[y] += 1
        state[x] = 0

    for x in state:
        log.debug('Backup %s has age %d', x, state[x])

    # Missing backups
    missing_backups = set(state) - backup_list
    for x in missing_backups:
        log.warning('backup %s is missing. Did you delete it manually?', x)
        del state[x]

    # Ranges
    ranges = [(0, cycles[0])]
    for i in range(1, len(cycles)):
        ranges.append((cycles[i - 1], cycles[i]))

    # Go forward in time to see what backups need to be kept
    simstate = dict()
    keep = set()
    missing = defaultdict(list)
    for step in range(max(cycles)):
        log.debug('Considering situation after %d more backups', step)
        for x in simstate:
            simstate[x] += 1
            log.debug('Backup x now has simulated age %d', simstate[x])

        # Add the hypothetical backup that has been made "just now"
        if step != 0:
            simstate[step] = 0

        for min_, max_ in ranges:
            log.debug('Looking for backup for age range %d to %d', min_, max_)

            # Look in simstate
            found = False
            for backup, age in simstate.items():  # noqa: B007
                if min_ <= age < max_:
                    found = True
                    break
            if found:
                # backup and age will be defined
                # pylint: disable=W0631
                log.debug('Using backup %s (age %d)', backup, age)
                continue

            # Look in state
            for backup, age in state.items():
                age += step
                if min_ <= age < max_:
                    log.info(
                        'Keeping backup %s (current age %d) for age range %d to %d%s',
                        backup,
                        state[backup],
                        min_,
                        max_,
                        (' in %d cycles' % step) if step else '',
                    )
                    simstate[backup] = age
                    keep.add(backup)
                    break

            else:
                if step == 0:
                    log.info(
                        'Note: there is currently no backup available for age range %d to %d',
                        min_,
                        max_,
                    )
                else:
                    missing['%d to %d' % (min_, max_)].append(step)

    for range_ in sorted(missing):
        log.info(
            'Note: there will be no backup for age range %s in (forthcoming) cycle(s): %s',
            range_,
            format_list(missing[range_]),
        )

    to_delete = set(state) - keep
    for x in to_delete:
        del state[x]

    return to_delete


def format_list(l):  # noqa: E741
    if not l:
        return ''
    l = l[:]  # copy  # noqa: E741

    # Append bogus end element
    l.append(l[-1] + 2)

    range_start = l.pop(0)
    cur = range_start
    res = list()
    for n in l:
        if n == cur + 1:
            pass
        elif range_start == cur:
            res.append('%d' % cur)
        elif range_start == cur - 1:
            res.append('%d' % range_start)
            res.append('%d' % cur)
        else:
            res.append('%d-%d' % (range_start, cur))

        if n != cur + 1:
            range_start = n
        cur = n

    if len(res) > 1:
        return '%s and %s' % (', '.join(res[:-1]), res[-1])
    else:
        return ', '.join(res)


if __name__ == '__main__':
    main(sys.argv[1:])
