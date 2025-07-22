from typing import Any
from unittest import skipUnless
from math import pi
from amsgpack import packb, Ext, unpackb, Timestamp, Packer
from struct import pack
from .test_amsgpack import SequenceTestCase
from .failing_malloc import failing_malloc, AVAILABLE as FAILING_AVAILABLE


class PackbTest(SequenceTestCase):
    def test_true(self):
        self.assertEqual(packb(True), b"\xc3")

    def test_false(self):
        self.assertEqual(packb(False), b"\xc2")

    def test_none(self):
        self.assertEqual(packb(None), b"\xc0")

    def test_double(self):
        self.assertEqual(packb(pi), b"\xcb@\t!\xfbTD-\x18")

    def test_ascii(self):
        ascii = "".join(chr(i) for i in range(128))
        self.assertEqual(unpackb(packb(ascii)), ascii)

    def test_unicode(self):
        unicode = "Привет, ❤️"
        self.assertEqual(unpackb(packb(unicode)), unicode)

    def test_str8(self):
        unicode = "a" * 0
        self.assertEqual(unpackb(packb(unicode)), unicode)
        unicode = "a" * 0xFF
        self.assertEqual(unpackb(packb(unicode)), unicode)

    def test_str16(self):
        unicode = "a" * 0x100
        self.assertEqual(unpackb(packb(unicode)), unicode)
        unicode = "a" * 0xFFFF
        self.assertEqual(unpackb(packb(unicode)), unicode)

    def test_str32(self):
        unicode = "a" * 0x10002
        self.assertEqual(unpackb(packb(unicode)), unicode)

    def test_list_0_to_20_el(self):
        for n in range(21):
            value = [None] * n
            self.assertEqual(unpackb(packb(value)), value)

    def test_list_0x10000(self):
        value = [True] * 0x10000
        self.assertEqual(unpackb(packb(value)), value)

    def test_pack_empty_map(self):
        self.assertEqual(packb({}), b"\x80")
        self.assertEqual(unpackb(b"\x80"), {})

    def test_dict_0_to_20_el(self):
        value = {}
        for n in range(21):
            # value = dict.fromkeys([str(i) for i in range(n)])
            self.assertEqual(unpackb(packb(value)), value)
            value[str(n)] = n

    def test_dict_0x10000(self):
        value = dict.fromkeys([str(i) for i in range(0x10000)])
        self.assertEqual(unpackb(packb(value)), value)

    def test_bytes_0_to_20_el(self):
        for n in range(21):
            value = "A" * n
            self.assertEqual(unpackb(packb(value)), value)

    def test_bytes_other(self):
        value = b"A" * 0xFFFF
        self.assertEqual(unpackb(packb(value)), value)
        value = b"A" * 0x20000
        self.assertEqual(unpackb(packb(value)), value)

    def test_main_page_example(self):
        value = {"compact": True, "schema": 0}
        self.assertEqual(packb(value), b"\x82\xa7compact\xc3\xa6schema\x00")

    def test_can_pack_tuple(self):
        self.assertEqual(packb(()), b"\x90")
        self.assertEqual(packb((1, 2, 3)), b"\x93\x01\x02\x03")

    def test_can_pack_nested_dict(self):
        self.assertEqual(packb({"a": {"b": "c"}}), b"\x81\xa1a\x81\xa1b\xa1c")

    def test_pack_bytearray(self):
        self.assertEqual(packb(bytearray(b"world")), b"\xc4\x05world")

    def test_pack_datetime(self):
        from datetime import datetime, timezone

        dt = datetime(2025, 4, 27, 20, 57, 26, 763583, timezone.utc)
        bytes = packb(dt)
        self.assertEqual(bytes, b"\xd7\xff\xb6\rh`h\x0e\x9a6")
        self.assertEqual(unpackb(bytes), dt)

    def test_pack_timestamp_32(self):
        bytes = packb(Timestamp(seconds=1752955664))
        self.assertEqual(bytes, b"\xd6\xffh{\xfb\x10")

    def test_pack_timestamp_64(self):
        bytes = packb(Timestamp(seconds=1752955664, nanoseconds=1))
        self.assertEqual(bytes, b"\xd7\xff\x00\x00\x00\x04h{\xfb\x10")

    def test_pack_timestamp_96(self):
        bytes = packb(Timestamp(seconds=17529556640000, nanoseconds=1000))
        self.assertEqual(
            bytes, b"\xc7\x0c\xff\x00\x00\x03\xe8\x00\x00\x0f\xf1j\xff!\x00"
        )

    def test_unsupported_type_raises_exception(self):
        with self.assertRaises(TypeError) as context:
            packb(1j)
        self.assertEqual(
            str(context.exception), "Unserializable 'complex' object"
        )

    def test_deep_exception(self):
        outer = inner = []
        for i in range(31):
            inner.append([])
            inner = inner[0]
        self.assertEqual(packb(outer), b"\x91" * 31 + b"\x90")
        inner.append([])

        with self.assertRaises(ValueError) as context:
            packb(outer)
        self.assertEqual(str(context.exception), "Deeply nested object")
        inner.clear()
        inner.append({})
        with self.assertRaises(ValueError) as context:
            packb(outer)
        self.assertEqual(str(context.exception), "Deeply nested object")

    @skipUnless(FAILING_AVAILABLE, "not failing available")
    def test_initial_buffer_failure(self):
        with self.assertRaises(MemoryError), failing_malloc(1023, "raw"):
            packb(0)

    def test_default_simple(self):
        from array import array

        def default(value: Any) -> Ext:
            if isinstance(value, array):
                return Ext(1, value.tobytes())
            raise ValueError(f"Unserializable object: {value}")

        packb = Packer(default=default).packb
        res = packb(array("I", b"\x02" * 8))
        self.assertEqual(res, b"\xd7\x01" + b"\x02" * 8)

        with self.assertRaises(ValueError):
            packb(...)

    def test_default_recursive(self):
        packb = Packer(default=lambda a: a).packb
        with self.assertRaises(ValueError) as context:
            packb(...)
        self.assertRegex(str(context.exception), "Deeply nested object")


class PackbIntTest(SequenceTestCase):
    def test_out_of_range(self):
        with self.assertRaises(OverflowError) as context:
            packb(0x1_FFFF_FFFFF_FFFF_FFFF)
        self.assertIn(
            str(context.exception),
            ("int too big to convert", "integer too large"),
        )

    def test_i8(self):
        for ref in range(-0x80, -0x20):
            exp = packb(ref)
            self.assertEqual(exp, b"\xd0" + bytes((ref + 0x100,)))
            self.assertEqual(unpackb(exp), ref)

    def test_i16(self):
        ref = -0x8000
        exp = packb(ref)
        self.assertEqual(exp, b"\xd1\x80\x00")
        self.assertEqual(unpackb(exp), ref)

    def test_i32(self):
        ref = -0x8000_0000
        exp = packb(ref)
        self.assertEqual(exp, b"\xd2\x80\x00\x00\x00")
        self.assertEqual(unpackb(exp), ref)

    def test_i64(self):
        ref = -0x8000_0000_0000_0000
        exp = packb(ref)
        self.assertEqual(exp, b"\xd3\x80\x00\x00\x00\x00\x00\x00\x00")
        self.assertEqual(unpackb(exp), ref)

    def test_uint_8(self):
        for ref in range(0x80, 0x100):
            exp = packb(ref)
            self.assertEqual(exp, b"\xcc" + bytes((ref,)))

    def test_uint64(self):
        ref = 0x7FFF_FFFF_FFFF_FFFF
        exp = packb(ref)
        self.assertEqual(exp, b"\xcf\x7f\xff\xff\xff\xff\xff\xff\xff")
        self.assertEqual(unpackb(exp), ref)

    def test_uint_16(self):
        for val in (0xFFFF, 0x1000, 0xFF00, 0x0100):
            exp = packb(val)
            ref = pack(">BH", 0xCD, val)
            self.assertEqual(exp, ref)

    def test_uint_32(self):
        for val in (0xFFFFFFFF, 0x100000, 0xFF0000, 0x0100000):
            exp = packb(val)
            ref = pack(">BI", 0xCE, val)
            self.assertEqual(exp, ref)
