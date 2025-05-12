#!/usr/bin/env bash
# tested on ubuntu 22.04
set -e
if [ $# -eq 0 ]; then
# run fuzzer
clang -DAMSGPACK_FUZZER=1\
    -g3 -O0 \
    -fsanitize=fuzzer \
    -fsanitize=address \
    -fsanitize=undefined \
    -fPIE \
    $(python3-config --includes) \
    amsgpack.c $(python3-config --ldflags --embed) \
    -o amsgpack_fuzzer

    ASAN_OPTIONS=symbolize=1:print_stacktrace=1,detect_leaks=1,log_path=amsgpack_leak.log ./amsgpack_fuzzer corpus
else
gcc -DAMSGPACK_FUZZER=1 "-DAMSGPACK_FUZZER_MAIN=1" \
    -g3 -O0 \
    -fPIE \
    $(python3-config --includes) \
    amsgpack.c $(python3-config --ldflags --embed) \
    -o amsgpack_fuzzer_file

    ./amsgpack_fuzzer_file "$1"
fi
