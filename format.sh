#!/usr/bin/env bash
set -e
clang-format -style=google -i *.h amsgpack.c
python3 -m black .