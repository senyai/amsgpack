#!/usr/bin/env bash
# tested on ubuntu 22.04
set -e
gcc --coverage -O0 -fPIC -shared $(python3-config --includes) amsgpack.c $(python3-config --ldflags) --coverage -o amsgpack.cpython-310-x86_64-linux-gnu.so
python -m unittest
mkdir -p coverage
gcovr --html-details -o coverage/coverage_report.html
