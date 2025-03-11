from unittest import TestCase
from amsgpack import packb
from math import pi
from msgpack import unpackb


class PackbTest(TestCase):
    def test_true(self):
        self.assertEqual(packb(True), b"\xc3")

    def test_false(self):
        self.assertEqual(packb(False), b"\xc2")

    def test_none(self):
        self.assertEqual(packb(None), b"\xc0")

    def test_double(self):
        self.assertEqual(packb(pi), b"\xcb@\t!\xfbTD-\x18")

    def test_i8(self):
        for i in range(-127, 128):
            self.assertEqual(unpackb(packb(i)), i)

    def test_i16(self):
        for i in (-0x7FFF, 0x1234, 0x8000):
            self.assertEqual(unpackb(packb(i)), i)

    def test_i32(self):
        for i in (-0x7FFF_0000, 0x1000_0000):
            self.assertEqual(unpackb(packb(i)), i)

    def test_i64(self):
        for i in (-0x7FFF_0000_0000_0000, 0x1000_0000_0000_0000):
            self.assertEqual(unpackb(packb(i)), i)

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

    def test_dict_0_to_20_el(self):
        for n in range(21):
            value = {}.fromkeys([str(i) for i in range(n)])
            self.assertEqual(unpackb(packb(value)), value)

    def test_bytes_0_to_20_el(self):
        for n in range(21):
            value = "A" * n
            self.assertEqual(unpackb(packb(value)), value)

    def test_bytes_other(self):
        value = "A" * 0xFFFF
        self.assertEqual(unpackb(packb(value)), value)
        value = "A" * 0x20000
        self.assertEqual(unpackb(packb(value)), value)
