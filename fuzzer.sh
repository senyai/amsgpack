#!/usr/bin/env bash
# tested on ubuntu 22.04
set -e
clang -DAMSGPACK_FUZZER=1 -g -O1 -fsanitize=fuzzer -fPIE $(python3-config --includes) amsgpack.c $(python3-config --ldflags --embed) -o amsgpack_fuzzer
./amsgpack_fuzzer
