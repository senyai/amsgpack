from amsgpack import Unpacker, Ext, unpackb, packb, Timestamp
from unittest import TestCase
from .test_amsgpack import SequenceTestCase


class ExtTest(TestCase):
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
            "Ext() missing required argument 'data' (pos 2)",
        )

    def test_args_init(self):
        self.assertEqual(repr(Ext(127, b"123")), "Ext(code=127, data=b'123')")

    def test_kwargs_init(self):
        self.assertEqual(
            repr(Ext(code=127, data=b"123")), "Ext(code=127, data=b'123')"
        )

    def test_code_128_is_impossible(self):
        with self.assertRaises(ValueError) as context:
            Ext(code=128, data=b"1")
        self.assertEqual(
            str(context.exception), "`code` must be between -128 and 127"
        )

    def test_hash_equal_when_code_and_data_equal(self):
        a = Ext(code=42, data=b"456")
        b = Ext(code=42, data=b"456")
        self.assertEqual(a, b)
        self.assertEqual(hash(a), hash(b))

    def test_hash_differ_when_code_is_different(self):
        a = Ext(code=127, data=b"123")
        b = Ext(code=126, data=b"123")
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
        b = Ext(code=126, data=b"123")
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

    def test_ext_hook_invalid(self):
        with self.assertRaises(TypeError) as context:
            Unpacker(ext_hook=0)
        self.assertEqual(str(context.exception), "`ext_hook` must be callable")

    def test_can_be_in_set(self):
        self.assertEqual(len({Ext(0, b"1"), Ext(0, b"1")}), 1)
        self.assertEqual(len({Ext(0, b"1"), Ext(0, b"2")}), 2)


class PackExtTest(TestCase):
    def test_ext_size_1(self):
        value = Ext(0x43, b"1")
        self.assertEqual(packb(value), b"\xd4C1")

    def test_ext_size_2(self):
        value = Ext(0x43, b"11")
        self.assertEqual(packb(value), b"\xd5C11")

    def test_ext_size_3(self):
        value = Ext(0x42, b"123")
        self.assertEqual(packb(value), b"\xc7\x03B123")

    def test_ext_size_4(self):
        value = Ext(0x43, b"1111")
        self.assertEqual(packb(value), b"\xd6C1111")

    def test_ext_size_8(self):
        value = Ext(0x43, b"1" * 8)
        self.assertEqual(packb(value), b"\xd7C11111111")

    def test_ext_size_16(self):
        value = Ext(0x43, b"1" * 16)
        self.assertEqual(packb(value), b"\xd8C1111111111111111")

    def test_ext_size_1000(self):
        value = Ext(0x43, b"1" * 1000)
        self.assertEqual(packb(value), b"\xc8\x03\xe8C" + b"1" * 1000)

    def test_ext_size_67000(self):
        value = Ext(0x43, b"1" * 67000)
        self.assertEqual(packb(value), b"\xc9\x00\x01\x05\xb8C" + b"1" * 67000)

    def test_is_timestamp(self):
        self.assertTrue(
            Ext(code=-1, data=b"\x0f\x00\x00\x00" * 1).is_timestamp()
        )
        self.assertTrue(
            Ext(code=-1, data=b"\x0f\x00\x00\x00" * 2).is_timestamp()
        )
        self.assertTrue(
            Ext(code=-1, data=b"\x0f\x00\x00\x00" * 3).is_timestamp()
        )
        self.assertFalse(
            Ext(code=-1, data=b"\x0f\x00\x00\x00" * 4).is_timestamp()
        )
        self.assertFalse(
            Ext(code=1, data=b"\x0f\x00\x00\x00" * 1).is_timestamp()
        )

    def test_to_timestamp(self):
        self.assertEqual(
            Ext(code=-1, data=b"\x0f\x00\x00\x00").to_timestamp(),
            Timestamp(seconds=251658240, nanoseconds=0),
        )

    def test_to_timestamp_exception(self):
        with self.assertRaises(ValueError) as context:
            Ext(code=1, data=b"\x0f\x00\x00").to_timestamp()
        self.assertEqual(
            str(context.exception),
            "Invalid timestamp length 3, allowed values are "
            "4, 8 and 12 (see MessagePack specification)",
        )

    def test_to_datetime_exception(self):
        with self.assertRaises(ValueError) as context:
            Ext(code=1, data=b"\x0f\x00\x00").to_datetime()
        self.assertEqual(
            str(context.exception),
            "Invalid timestamp length 3, allowed values are "
            "4, 8 and 12 (see MessagePack specification)",
        )


class UnpackExtTest(SequenceTestCase):
    def test_huge_data(self):
        with self.assertRaises(ValueError) as context:
            unpackb(b"\xc9\x0f\xff\xff\xff\x00")
        self.assertEqual(
            str(context.exception),
            "ext size 268435455 is too big (>134217728)",
        )

    def test_ext_hook(self):
        from array import array

        def ext_hook(ext: Ext):
            if ext.code == 1:
                return array("I", ext.data)
            return ext.default()

        value = Unpacker(ext_hook=ext_hook).unpackb(
            b"\xd7\x01\x00\x00\x00\x00\x01\x00\x00\x00"
        )
        self.assertEqual(value, array("I", [0, 1]))

    def test_ext_hook_exc(self):
        def ext_hook(ext: Ext):
            raise ValueError(ext)

        with self.assertRaises(ValueError) as context:
            Unpacker(ext_hook=ext_hook).unpackb(
                b"\xd7\x01\x00\x00\x00\x00\x01\x00\x00\x00"
            )
        self.assertEqual(
            context.exception.args,
            (Ext(1, b"\x00\x00\x00\x00\x01\x00\x00\x00"),),
        )
