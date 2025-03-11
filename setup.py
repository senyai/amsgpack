from setuptools import setup, Extension

module = Extension("amsgpack", sources=["amsgpack.c"])

setup(
    name="amsgpack",
    version="0.0.0",
    description="MsgPack that can do it",
    ext_modules=[module],
)
