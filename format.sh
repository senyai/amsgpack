#!/usr/bin/env bash
clang-format -style=google -i deque.h amsgpack.c
python3 -m black .