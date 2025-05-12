#!/usr/bin/env bash
set -e
clang-format --verbose -i *.h amsgpack.c
python3 -m black .