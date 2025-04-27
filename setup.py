from setuptools import setup, Extension

with open("amsgpack.c") as f:
    for line in f:
        if line.startswith("#define VERSION"):
            version = line.split('"')[-2]
            break
    else:
        raise ValueError("version not found")

module = Extension(
    "amsgpack",
    sources=["amsgpack.c"],
    extra_compile_args=[
        "-O3",
        "-Werror",
        "-Wall",
        "-Wextra",
        "-Wdouble-promotion",
    ],
)

setup(version=version, ext_modules=[module])
