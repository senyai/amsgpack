#!/usr/bin/env bash
set -e
clang-format -i *.h amsgpack.c
python3 -m black .