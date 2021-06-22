#!/bin/bash
#
# Any arguments are passed through to sphinx-build
#

set -e

cd $(dirname $0)

sphinx-build -b html -d doc/doctrees "$@" rst/ doc/html/

sphinx-build -b man -d doc/doctrees "$@" rst/ doc/man/
for name in expire_backups.1 pcp.1; do
    mv -vf "doc/man/${name}" contrib/
done


sphinx-build -b latex -d doc/doctrees "$@" rst/ doc/latex/
echo "Running pdflatex..."
for _ in seq 3; do
    (cd doc/latex &&
     pdflatex -interaction batchmode manual.tex)
done
mv -vf doc/latex/manual.pdf doc/manual.pdf

exit 0
