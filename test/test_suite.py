from unittest import TestCase
from amsgpack import Unpacker, Ext
from pathlib import Path
from datetime import datetime, timezone
from typing import Any


class SuiteTest(TestCase):
    def subtest(self, ref: Any, test_case: list[bytes]):
        for case_bytes in test_case:
            with self.subTest(case_bytes=case_bytes):
                exp = unpackb(case_bytes)
                self.assertEqual(ref, exp)

    def subtest_exception(self, ref: Any, test_case: list[bytes]):
        for case_bytes in test_case:
            with self.subTest(case_bytes=case_bytes):
                with self.assertRaises(ref):
                    unpackb(case_bytes)

    @classmethod
    def add_test(cls, name: str, ref: Any, test_case: list[bytes]):
        if ref is OverflowError:
            setattr(
                cls,
                f"test_{name}_exc".replace(".", "_").replace("-", "_"),
                lambda self: self.subtest_exception(ref, test_case),
            )
        else:
            setattr(
                cls,
                f"test_{name}".replace(".", "_").replace("-", "_"),
                lambda self: self.subtest(ref, test_case),
            )


def unpackb(data: bytes):
    u = Unpacker()
    u.feed(data)
    return next(u)


def _get_ref_value(data):
    match data:
        case {"nil": value}:
            return None
        case {"bool": value}:
            assert isinstance(value, bool)
            return value
        case {"binary": value}:
            assert isinstance(value, str)
            return bytes.fromhex(value.replace("-", " "))
        case {"number": value}:
            assert isinstance(value, (int, float)), value
            return value
        case {"bignum": value}:
            assert isinstance(value, str), value
            return int(value, 10)
        case {"string": value}:
            assert isinstance(value, str)
            return value
        case {"array": value}:
            assert isinstance(value, list)
            return value
        case {"map": value}:
            assert isinstance(value, dict)
            return value
        case {"timestamp": [tv_sec, tv_nsec]}:
            assert isinstance(tv_sec, int)
            assert isinstance(tv_nsec, int)
            if tv_sec < 0:
                return OverflowError
            try:
                return datetime.fromtimestamp(tv_sec, tz=timezone.utc).replace(
                    microsecond=int(tv_nsec / 1000)
                )
            except Exception as e:
                return OverflowError
        case {"ext": [code, data]}:
            assert isinstance(code, int)
            assert isinstance(data, str)
            return Ext(code, bytes.fromhex(data.replace("-", " ")))
        case _:
            raise ValueError(data)


def main():
    import json

    tests = json.loads(
        (Path(__file__).parent / "msgpack-test-suite.json").read_text()
    )
    for test_name, test_cases in tests.items():
        for idx, test_case in enumerate(test_cases):
            ref = _get_ref_value(test_case)
            test_case_ = [
                bytes.fromhex(case_str.replace("-", " "))
                for case_str in test_case["msgpack"]
            ]
            SuiteTest.add_test(f"{test_name}_{idx}", ref, test_case_)


main()
