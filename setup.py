import sys
from setuptools import setup, Extension

with open("src/amsgpack.c") as f:
    for line in f:
        if line.startswith("#define VERSION"):
            version = line.split('"')[-2]
            break
    else:
        raise ValueError("version not found")

# this check is not correct, as windows can use non msvc compiler, but okay
extra_compile_args: list[str] = []
if sys.platform != "win32":
    extra_compile_args.extend(
        [
            "-O3",
            "-Werror",
            "-Wall",
            "-Wextra",
            "-Wdouble-promotion",
            "-march=native",
        ]
    )
    if not hasattr(sys, "pypy_version_info"):
        # because PyPy/3.11.11/x64/include/pypy3.11/genericaliasobject.h
        extra_compile_args.append("-Wpedantic")

module = Extension(
    "amsgpack",
    sources=["src/amsgpack.c"],
    extra_compile_args=extra_compile_args,
)

setup(version=version, ext_modules=[module])
