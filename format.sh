#!/usr/bin/env bash
clang-format -style=google -i ext.h deque.h amsgpack.c
python3 -m black .