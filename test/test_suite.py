from unittest import TestCase
from amsgpack import Ext, unpackb
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import Any
from sys import platform

is_windows = platform == "win32"


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
        if isinstance(ref, type):
            assert issubclass(ref, Exception), ref
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
            assert 0 <= tv_nsec <= 999999999
            microsecond = round(tv_nsec / 1000)
            if microsecond == 1000000:
                microsecond = 0
                tv_sec += 1
            if is_windows:
                epoch = datetime.fromtimestamp(0, tz=timezone.utc)
                try:
                    dt = epoch + timedelta(seconds=tv_sec)
                except OSError:  # windows only
                    return ValueError
                except Exception as e:
                    if isinstance(e, OverflowError) and e.args == (
                        "date value out of range",
                    ):
                        return ValueError
                    return type(e)
            else:
                try:
                    dt = datetime.fromtimestamp(tv_sec, tz=timezone.utc)
                except Exception as e:
                    return type(e)
            return dt.replace(microsecond=microsecond)
        case {"ext": [code, data]}:
            assert isinstance(code, int)
            assert isinstance(data, str)
            return Ext(code, bytes.fromhex(data.replace("-", " ")))
        case _:
            raise ValueError(data)


def main():
    import json

    tests = json.loads(
        (Path(__file__).parent / "msgpack-test-suite.json").read_text(
            encoding="utf-8"
        )
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
