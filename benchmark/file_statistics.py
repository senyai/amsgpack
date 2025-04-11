#!/usr/bin/env python3
from typing import Any
from collections import defaultdict
from functools import reduce


def merge(
    a: defaultdict[int, str], b: defaultdict[int, str]
) -> defaultdict[int, str]:
    out = a.copy()
    for key, value in b.items():
        out[key] += value
    return out


class FileStatistics:
    def __init__(self, obj: Any) -> None:
        self.stats: defaultdict[int, str] = defaultdict[str](int)
        self._gather(obj)

    def _gather(self, obj: Any) -> None:
        match obj:
            case str():
                length = len(obj)
                if length <= 31:
                    self.stats["fixstr"] += 1
                elif length <= 0xFF:
                    self.stats["str 8"] += 1
                elif length <= 0xFFFF:
                    self.stats["str 16"] += 1
                elif length <= 0xFFFFFFFF:
                    self.stats["str 32"] += 1
                else:
                    raise ValueError(length)
            case float():
                self.stats["float"] += 1
            case int():
                if -32 <= obj <= 0x7F:
                    self.stats["fixint"] += 1
                elif abs(obj) <= 0xFF:
                    self.stats["int 8"] += 1
                elif abs(obj) <= 0xFFFF:
                    self.stats["int 16"] += 1
                elif abs(obj) <= 0xFFFFFFFF:
                    self.stats["int 32"] += 1
                elif abs(obj) <= 0xFFFFFFFFFFFFFFFF:
                    self.stats["int 64"] += 1
                else:
                    raise ValueError(obj)
            case list():
                length = len(obj)
                if length <= 15:
                    self.stats["fixarray"] += 1
                elif length <= 0xFFFF:
                    self.stats["array 16"] += 1
                elif length <= 0xFFFFFFFF:
                    self.stats["array 32"] += 1
                else:
                    raise ValueError(length)
                for item in obj:
                    self._gather(item)
            case dict():
                length = len(obj)
                if length <= 15:
                    self.stats["fixmap"] += 1
                elif length <= 0xFFFF:
                    self.stats["map 16"] += 1
                elif length <= 0xFFFFFFFF:
                    self.stats["map 32"] += 1
                else:
                    raise ValueError(length)
                for key, value in obj.items():
                    self._gather(key)
                    self._gather(value)
            case None:
                self.stats["undefined"] += 1
            case _:
                raise ValueError(type(obj))


def main():
    import argparse
    from pathlib import Path

    parser = argparse.ArgumentParser()
    parser.add_argument("paths", nargs="+", type=Path)
    args = parser.parse_args()
    stats = []
    for path in args.paths:
        match path.suffix:
            case ".json":
                import json

                data = json.loads(path.read_text())
            case ".msgpack" | ".mpack":
                from amsgpack import unpackb

                data = unpackb(path.read_bytes())
            case _:
                raise ValueError(_)

        stats.append(FileStatistics(data).stats)

    res = reduce(merge, stats, defaultdict(int))
    for key, value in sorted(res.items(), key=lambda v: v[1], reverse=True):
        print(f"{key} -> {value}")


if __name__ == "__main__":
    main()
