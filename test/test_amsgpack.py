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


class ExtClassTest(TestCase):
    def test_unicode_exception(self):
        with self.assertRaises(TypeError) as context:
            Ext(127, "123")
        self.assertEqual(
            str(context.exception), "a bytes object is required, not 'str'"
        )

    def test_arguments_exception(self):
        with self.assertRaises(TypeError) as context:
            Ext(code=127)
        self.assertEqual(
            str(context.exception),
            "function missing required argument 'data' (pos 2)",
        )

    def test_args_init(self):
        self.assertEqual(repr(Ext(127, b"123")), "Ext(code=127, data=b'123')")

    def test_kwargs_init(self):
        self.assertEqual(
            repr(Ext(code=127, data=b"123")), "Ext(code=127, data=b'123')"
        )

    def test_code_128_is_impossible(self):
        self.assertEqual(
            repr(Ext(code=128, data=b"1")), "Ext(code=-128, data=b'1')"
        )

    def test_hash_equal_when_code_and_data_equal(self):
        a = Ext(code=42, data=b"456")
        b = Ext(code=42, data=b"456")
        self.assertEqual(a, b)
        self.assertEqual(hash(a), hash(b))

    def test_hash_differ_when_code_is_different(self):
        a = Ext(code=127, data=b"123")
        b = Ext(code=128, data=b"123")
        self.assertNotEqual(a, b)
        self.assertNotEqual(hash(a), hash(b))
        self.assertFalse(a == b)

    def test_data_not_equal(self):
        a = Ext(code=42, data=b"456")
        b = Ext(code=42, data=b"457")
        self.assertNotEqual(a, b)
        self.assertNotEqual(hash(a), hash(b))

    def test_can_access_attributes(self):
        a = Ext(code=127, data=b"123")
        self.assertEqual(a.code, 127)
        self.assertEqual(a.data, b"123")

    def test_attributes_are_readonly(self):
        a = Ext(code=127, data=b"123")
        with self.assertRaises(AttributeError):
            a.code = 3
        with self.assertRaises(AttributeError):
            a.data = b"x"

    def test_compare_less_exception(self):
        a = Ext(code=127, data=b"123")
        b = Ext(code=128, data=b"123")
        with self.assertRaises(TypeError) as context:
            a < b
        self.assertIn(
            str(context.exception),
            (
                "'<' not supported between instances of 'amsgpack.Ext' and 'amsgpack.Ext'",
                "'<' not supported between instances of 'Ext' and 'Ext'",
            ),
        )

    def test_compare_types_exception(self):
        a = Ext(code=127, data=b"123")
        with self.assertRaises(TypeError) as context:
            a != b"b"
        self.assertEqual(
            str(context.exception),
            "other argument must be amsgpack.Ext instance",
        )

        with self.assertRaises(TypeError) as context:
            a == b"b"
        self.assertEqual(
            str(context.exception),
            "other argument must be amsgpack.Ext instance",
        )


class RawTest(TestCase):
    def test_unicode_exception(self):
        with self.assertRaises(TypeError) as context:
            Raw("123")
        self.assertEqual(
            str(context.exception), "a bytes object is required, not 'str'"
        )

    def test_arguments_exception(self):
        with self.assertRaises(TypeError) as context:
            Raw(code=1, data=b"123")
        self.assertEqual(
            str(context.exception),
            "function takes at most 1 keyword argument (2 given)",
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
