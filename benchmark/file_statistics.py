#!/usr/bin/env python3
from __future__ import annotations
from typing import Any
from collections import defaultdict
from operator import itemgetter


class Stat(defaultdict[str, int]):
    def __init__(self, default_factory: type = int, *args: Any):
        super().__init__(default_factory, *args)

    def __add__(self, other: Stat) -> Stat:
        out = self.copy()
        for key, value in other.items():
            out[key] += value
        return out

    def repr(self):
        out: list[str] = []
        for key, value in sorted(
            self.items(), key=itemgetter(1), reverse=True
        ):
            out.append(f"{key} -> {value}")
        return "\n".join(out)


class FileStatistics:
    def __init__(self, obj: Any) -> None:
        self.stats = Stat()
        self.key_stats = Stat()
        self._gather(obj, self.stats)

    def __iadd__(self, other: FileStatistics):
        self.stats += other.stats
        self.key_stats += other.key_stats
        return self

    def _gather(self, obj: Any, stats: Stat) -> None:
        match obj:
            case str():
                length = len(obj)
                if length <= 31:
                    stats["fixstr"] += 1
                elif length <= 0xFF:
                    stats["str 8"] += 1
                elif length <= 0xFFFF:
                    stats["str 16"] += 1
                elif length <= 0xFFFFFFFF:
                    stats["str 32"] += 1
                else:
                    raise ValueError(length)
            case float():
                stats["float"] += 1
            case int():
                if -32 <= obj <= 0x7F:
                    stats["fixint"] += 1
                elif abs(obj) <= 0xFF:
                    stats["int 8"] += 1
                elif abs(obj) <= 0xFFFF:
                    stats["int 16"] += 1
                elif abs(obj) <= 0xFFFFFFFF:
                    stats["int 32"] += 1
                elif abs(obj) <= 0xFFFFFFFFFFFFFFFF:
                    stats["int 64"] += 1
                else:
                    raise ValueError(obj)
            case list():
                length = len(obj)
                if length <= 15:
                    stats["fixarray"] += 1
                elif length <= 0xFFFF:
                    stats["array 16"] += 1
                elif length <= 0xFFFFFFFF:
                    stats["array 32"] += 1
                else:
                    raise ValueError(length)
                for item in obj:
                    self._gather(item, stats)
                if len(set(type(item) for item in obj)) <= 1:
                    stats["array_same"] += 1
                else:
                    stats["array_different"] += 1
            case dict():
                length = len(obj)
                if length <= 15:
                    stats["fixmap"] += 1
                elif length <= 0xFFFF:
                    stats["map 16"] += 1
                elif length <= 0xFFFFFFFF:
                    stats["map 32"] += 1
                else:
                    raise ValueError(length)
                for key, value in obj.items():
                    self._gather(key, stats)
                    self._gather(key, self.key_stats)
                    self._gather(value, stats)
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
    file_statistics: FileStatistics | None = None
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

        stats = FileStatistics(data)
        print(f"\n{path}:")
        print(stats.stats.repr())
        if file_statistics is None:
            file_statistics = stats
        else:
            file_statistics += stats

    if file_statistics:
        print(f"\nall {len(args.paths)} files")
        print(file_statistics.stats.repr())
        print("\nkeys stats")
        print(file_statistics.key_stats.repr())
    else:
        print("no file statistics")


if __name__ == "__main__":
    main()
