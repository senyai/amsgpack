from setuptools import setup, Extension

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

setup(ext_modules=[module])
