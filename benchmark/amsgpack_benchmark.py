#!/usr/bin/env python3
from typing import Any, Callable
from amsgpack import packb as amsgpack_packb, unpackb as amsgpack_unpackb
from msgpack import packb as msgpack_packb, unpackb as msgpack_unpackb
from ormsgpack import packb as ormsgpack_packb, unpackb as ormsgpack_unpackb
from msgspec.msgpack import encode as msgspec_packb, decode as msgspec_unpackb

import google_benchmark as benchmark

# download these first
# https://github.com/aviramha/ormsgpack/raw/refs/heads/master/benchmarks/samples/canada.mpack
# https://github.com/aviramha/ormsgpack/raw/refs/heads/master/benchmarks/samples/citm_catalog.mpack
# https://github.com/aviramha/ormsgpack/raw/refs/heads/master/benchmarks/samples/github.mpack
# https://github.com/aviramha/ormsgpack/raw/refs/heads/master/benchmarks/samples/twitter.mpack


def validate(
    pack: Callable[[Any], bytes], unpack: Callable[[bytes], Any], dataset: Any
) -> None:
    data = pack(dataset)
    assert isinstance(data, (bytes, bytearray))
    result = unpack(data)
    assert result == dataset


files: list[tuple[str, bytes, Any]] = []
for filename in (
    "canada.mpack",
    "citm_catalog.mpack",
    "github.mpack",
    "twitter.mpack",
):
    with open(filename, "rb") as f:
        data = f.read()
        unpacked_data = ormsgpack_unpackb(data)
        files.append((filename, data, unpacked_data))
        validate(amsgpack_packb, amsgpack_unpackb, unpacked_data)
        validate(msgpack_packb, msgpack_unpackb, unpacked_data)
        validate(ormsgpack_packb, ormsgpack_unpackb, unpacked_data)
        validate(msgspec_packb, msgspec_unpackb, unpacked_data)


def pack_benchmark(state):
    func_label, packb_func = (
        ("amsgpack", amsgpack_packb),
        ("msgpack", msgpack_packb),
        ("ormsgpack", ormsgpack_packb),
        ("msgspec", msgspec_packb),
    )[state.range(0)]
    file_label, _, obj = files[state.range(1)]
    state.set_label(f"{func_label}({file_label})")
    size = 0
    while state:
        size = len(packb_func(obj))

    state.bytes_processed = state.iterations * size
    state.counters["size"] = size


def unpack_benchmark(state):
    func_label, unpackb_func = (
        ("amsgpack", amsgpack_unpackb),
        ("msgpack", msgpack_unpackb),
        ("ormsgpack", ormsgpack_unpackb),
        ("msgspec", msgspec_unpackb),
    )[state.range(0)]
    file_label, bytes, _ = files[state.range(1)]
    state.set_label(f"{func_label}({file_label})")
    while state:
        unpackb_func(bytes)

    state.bytes_processed = state.iterations * len(bytes)


for i in reversed(range(4)):
    for j in reversed(range(4)):
        pack_benchmark = benchmark.option.args((i, j))(pack_benchmark)
        unpack_benchmark = benchmark.option.args((i, j))(unpack_benchmark)

pack_benchmark = benchmark.register(pack_benchmark)
unpack_benchmark = benchmark.register(unpack_benchmark)


if __name__ == "__main__":
    import sys

    benchmark.main(
        [
            sys.argv[0],
            # "--benchmark_format=json",
            "--benchmark_out=msgpack_benchmark.json",
            "--benchmark_repetitions=5",
        ]
    )
