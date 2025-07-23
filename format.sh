#!/usr/bin/env bash
set -e
clang-format --verbose -i src/*.h src/amsgpack.c
python3 -m black .