#!/bin/bash
#
# Any arguments are passed through to sphinx-build
#

set -e

cd $(dirname $0)

sphinx-build -W --keep-going -b html -d doc/doctrees "$@" rst/ doc/html/

sphinx-build -W --keep-going -b man -d doc/doctrees "$@" rst/ doc/manpages/
for name in expire_backups.1 pcp.1; do
    mv -vf "doc/manpages/${name}" contrib/
done
