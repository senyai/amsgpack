#!/usr/bin/env python3
from typing import Any, NamedTuple
from collections.abc import Callable
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


class TestData(NamedTuple):
    name: str
    ref_bytes: bytes
    ref_object: object


class TestFunc(NamedTuple):
    name: str
    packb: Callable[[Any], bytes]
    unpackb: Callable[[bytes], Any]

    def validate(self, test: TestData) -> None:
        """
        Enure 'ref' -> `packb` -> `unpackb` outputs `ref`
        """
        data = self.packb(test.ref_object)
        assert isinstance(data, bytes)
        result = self.unpackb(data)
        assert result == test.ref_object, f"{self.name} for {test.name}"

    @classmethod
    def register(
        cls,
        name: str,
        packb: Callable[[Any], bytes],
        unpackb: Callable[[bytes], Any],
    ) -> None:
        test_func.append(cls(name=name, packb=packb, unpackb=unpackb))


test_data: list[TestData] = []
test_func: list[TestFunc] = []

TestFunc.register("amsgpack", amsgpack_packb, amsgpack_unpackb)
TestFunc.register(
    "msgpack", msgpack_packb, lambda bytes: msgpack_unpackb(bytes, raw=False)
)
TestFunc.register("ormsgpack", ormsgpack_packb, ormsgpack_unpackb)
TestFunc.register("msgspec", msgspec_packb, msgspec_unpackb)


def _register_file_test(filename: str) -> None:
    with open(filename, "rb") as f:
        data = f.read()
    test_data.append(
        TestData(
            name=filename.rsplit(".")[0],
            ref_bytes=data,
            ref_object=ormsgpack_unpackb(data),
        )
    )


def _register_double_256_test():
    ref = [float(i) for i in range(256)]
    ref_bytes = ormsgpack_packb(ref)
    assert len(ref_bytes) == 3 + 256 * 9, len(ref_bytes)
    test_data.append(
        TestData(name="doubles[256]", ref_bytes=ref_bytes, ref_object=ref)
    )


def _register_text_256_test(name: str, text: str) -> None:
    ref = [text] * 256
    ref_bytes = ormsgpack_packb(ref)
    test_data.append(TestData(name=name, ref_bytes=ref_bytes, ref_object=ref))


_register_file_test("canada.mpack")
_register_file_test("citm_catalog.mpack")
_register_file_test("github.mpack")
_register_file_test("twitter.mpack")
# _register_double_256_test()
# _register_text_256_test(
#     name="str[256]",
#     text="نظام الحكم سلطاني وراثي "
#     "في الذكور من ذرية السيد تركي بن سعيد بن سلطان ويشترط فيمن يختار لولاية"
#     " الحكم من بينهم ان يكون مسلما رشيدا عاقلا ًوابنا شرعيا لابوين عمانيين ",
# )
# _register_text_256_test(name="ascii[256]", text="Hello, world!")


def pack_benchmark(state: benchmark.State) -> None:
    func, test = test_func[state.range(0)], test_data[state.range(1)]
    state.set_label(f"{func.name}({test.name})")
    size = 0
    obj = test.ref_object
    packb_func = func.packb
    while state:
        size = len(packb_func(obj))

    state.bytes_processed = state.iterations * size
    state.counters["size"] = benchmark.Counter(size)


def unpack_benchmark(state: benchmark.State) -> None:
    func, test = test_func[state.range(0)], test_data[state.range(1)]
    state.set_label(f"{func.name}({test.name})")
    unpackb_func, bytes = func.unpackb, test.ref_bytes
    while state:
        unpackb_func(bytes)

    state.bytes_processed = state.iterations * len(bytes)


for func in test_func:
    for test in test_data:
        func.validate(test)


for i in reversed(range(len(test_func))):
    for j in reversed(range(len(test_data))):
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
