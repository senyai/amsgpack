"""
A trick to load `amsgpack._amsgpack` shared library from custom directory
and then allow importing `amsgpack` from root directory
"""

import sys
import os.path as osp

try:
    import amsgpack._amsgpack as module  # pyright: ignore[reportMissingModuleSource, reportUnusedImport]
except ModuleNotFoundError:
    print(
        f"{sys.path} has no `amsgpack._amsgpack` module available. "
        "It might be that current python version don't support importing a module like this"
    )
    raise

del sys.modules["amsgpack"]
parent_dir = osp.abspath(osp.join(osp.dirname(__file__), osp.pardir))
sys.path.insert(0, parent_dir)
