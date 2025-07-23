from collections.abc import Sequence
from unittest import TestCase
from amsgpack import packb, Ext, Raw, Unpacker


Value = (
    dict[str, "Value"]
    | Sequence["Value"]
    | str
    | int
    | float
    | bool
    | None
    | Ext
)


class SequenceTestCase(TestCase):
    def safeSequenceEqual(
        self, unpacker: Unpacker, ref: tuple[Value, ...]
    ) -> None:
        it = iter(unpacker)
        self.assertIs(it, unpacker)
        for idx, item in enumerate(ref):
            self.assertEqual(next(it), item, f"{idx=}")
        with self.assertRaises(StopIteration):
            next(it)


class RawTest(TestCase):
    def test_unicode_exception(self):
        with self.assertRaises(TypeError) as context:
            Raw("123")
        self.assertEqual(
            str(context.exception), "Raw() argument 1 must be bytes, not str"
        )

    def test_arguments_exception(self):
        with self.assertRaises(TypeError) as context:
            Raw(code=1, data=b"123")
        self.assertEqual(
            str(context.exception),
            "Raw() takes at most 1 keyword argument (2 given)",
        )

    def test_single_raw(self):
        self.assertEqual(Raw(b"\xc2"), b"\xc2")

    def test_pack_in_array(self):
        self.assertEqual(packb([Raw(b"\xc2"), Raw(b"\xc3")]), b"\x92\xc2\xc3")

    def test_eq(self):
        self.assertEqual(Raw(b"\xc2"), Raw(b"\xc2"))

    def test_ne(self):
        self.assertNotEqual(Raw(b"\xc2"), Raw(b"\xc1"))

    def test_key(self):
        d = {
            Raw(b"\xc2"): 1,
            Raw(b"\xc2"): 2,
        }
        self.assertIn(Raw(b"\xc2"), d)
        self.assertEqual(d[Raw(b"\xc2")], 2)

    def test_repr(self):
        self.assertEqual(repr(Raw(b"Aa")), "Raw(data=b'Aa')")
