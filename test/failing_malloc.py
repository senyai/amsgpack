from __future__ import annotations
from typing import Literal

# import ctypes
try:
    from ctypes import pythonapi
except ImportError:  # pypy
    pythonapi = None
AVAILABLE = pythonapi is not None
from ctypes import (
    byref,
    c_size_t,
    c_void_p,
    cast as c_cast,
    CFUNCTYPE,
    POINTER,
    pointer as c_pointer,
    Structure as CStructure,
)
from contextlib import contextmanager

# PYMEM_DOMAIN_RAW = 0
# PYMEM_DOMAIN_MEM = 1
# PYMEM_DOMAIN_OBJ = 2


class Context(CStructure):
    _fields_ = [
        ("memory_limit_bytes", c_size_t),
        ("original_malloc", c_void_p),
        ("failures", c_size_t),  # counter to only fail once
    ]


class PyMemAllocatorEx(CStructure):
    _fields_ = [
        ("ctx", c_void_p),
        ("malloc", c_void_p),
        ("calloc", c_void_p),
        ("realloc", c_void_p),
        ("free", c_void_p),
    ]


MallocFunc = CFUNCTYPE(c_void_p, POINTER(Context), c_size_t)


def _py_failing_malloc_py(ctx: Context, size: int):
    if size > ctx.contents.memory_limit_bytes and ctx.contents.failures == 0:
        ctx.contents.failures += 1
        return None
    original_malloc = c_cast(ctx.contents.original_malloc, MallocFunc)
    return original_malloc(0, size)


failing_malloc_c = MallocFunc(_py_failing_malloc_py)
failing_malloc_ptr = c_cast(failing_malloc_c, c_void_p)


@contextmanager
def failing_malloc(
    memory_limit_bytes: int, domain: Literal["raw", "mem", "obj"]
):
    if domain == "obj":
        raise RuntimeError(
            "Makes little sense to limit obj domain "
            "because too many things are allocated in obj domain "
            "stack objects, for example"
        )
    domain_idx = ("raw", "mem", "obj").index(domain)
    original_allocator = PyMemAllocatorEx()
    pythonapi.PyMem_GetAllocator(domain_idx, byref(original_allocator))

    context = Context(
        memory_limit_bytes=memory_limit_bytes,
        original_malloc=original_allocator.malloc,
        failures=0,
    )
    failing_allocator = PyMemAllocatorEx(
        ctx=c_cast(c_pointer(context), c_void_p),
        malloc=failing_malloc_ptr,
        calloc=original_allocator.calloc,
        realloc=original_allocator.realloc,
        free=original_allocator.free,
    )
    try:
        pythonapi.PyMem_SetAllocator(domain_idx, byref(failing_allocator))
        yield
    finally:
        pythonapi.PyMem_SetAllocator(domain_idx, byref(original_allocator))
