#!/bin/bash
#
# Any arguments are passed through to sphinx-build
#

set -e

cd $(dirname $0)

sphinx-build -b html -d doc/doctrees "$@" rst/ doc/html/

sphinx-build -b man -d doc/doctrees "$@" rst/ doc/manpages/
for name in expire_backups.1 pcp.1; do
    mv -vf "doc/manpages/${name}" contrib/
done

sphinx-build -b latex -d doc/doctrees "$@" rst/ doc/pdf/
