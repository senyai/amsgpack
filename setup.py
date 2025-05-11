import sys
from setuptools import setup, Extension

with open("amsgpack.c") as f:
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

module = Extension(
    "amsgpack",
    sources=["amsgpack.c"],
    extra_compile_args=extra_compile_args,
)

setup(version=version, ext_modules=[module])
