from collections.abc import Sequence
from math import pi
from unittest import TestCase
from amsgpack import packb, Unpacker, Ext


def unpackb(data: bytes):
    u = Unpacker()
    u.feed(bytes(data))
    return next(u)


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

    def test_main_page_example(self):
        value = {"compact": True, "schema": 0}
        self.assertEqual(packb(value), b"\x82\xa7compact\xc3\xa6schema\x00")

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


class UnpackerTest(TestCase):
    def test_unpacker_gets_no_argumens(self):
        with self.assertRaises(TypeError):
            Unpacker("what", "is", "that")
        with self.assertRaises(TypeError):
            Unpacker(what="is that")

    def test_feed_nothing(self):
        self.safeSequenceEqual(Unpacker(), ())

    def test_feed_non_bytes(self):
        u = Unpacker()
        with self.assertRaises(TypeError) as context:
            u.feed("")
        self.assertEqual(
            str(context.exception), "a bytes object is required, not 'str'"
        )

    def test_unpack_none(self):
        u = Unpacker()
        u.feed(b"\xc0")
        self.safeSequenceEqual(u, (None,))

    def test_unpack_bool(self):
        u = Unpacker()
        u.feed(b"\xc2\xc3")
        self.safeSequenceEqual(u, (False, True))

    def test_double(self):
        u = Unpacker()
        u.feed(b"\xcb@\t!\xfbTD-\x11")
        self.safeSequenceEqual(u, (3.14159265358979,))

    def test_feed_2_bytes(self):
        u = Unpacker()
        u.feed(b"\xc2")
        u.feed(b"\xc3")
        self.safeSequenceEqual(u, (False, True))

    def test_feed_double_byte_by_byte(self):
        u = Unpacker()
        for byte in b"\xcb@\t!\xfbTD-\x11":
            u.feed(bytes((byte,)))
        self.safeSequenceEqual(u, (3.14159265358979,))

    def test_feed_double_byte_by_byte_and_iterate(self):
        u = Unpacker()
        for byte in b"\xcb@\t!\xfbTD-\x11":
            u.feed(bytes((byte,)))
            if byte == 0xCB:
                with self.assertRaises(StopIteration):
                    next(u)
        self.safeSequenceEqual(u, (3.14159265358979,))

    def test_feed_double_2_bytes_sequence(self):
        u = Unpacker()
        pi_bytes = b"\xcb@\t!\xfbTD-\x11"
        for idx in range(0, len(pi_bytes), 2):
            u.feed(bytes((*pi_bytes[idx : idx + 2],)))
        self.safeSequenceEqual(u, (3.14159265358979,))

    def test_list_len_0(self):
        u = Unpacker()
        u.feed(b"\x90")
        self.safeSequenceEqual(u, ([],))

    def test_list_len_1(self):
        u = Unpacker()
        u.feed(b"\x91\xc0")
        self.safeSequenceEqual(u, ([None],))

    def test_list_len_2(self):
        u = Unpacker()
        u.feed(b"\x92\xc0\xc0")
        self.safeSequenceEqual(u, ([None, None],))

    def test_list_inside_list(self):
        u = Unpacker()
        u.feed(b"\x92\x90\x90")
        self.safeSequenceEqual(u, ([[], []],))

    def test_main_page_example(self):
        u = Unpacker()
        u.feed(b"\x82\xa7compact\xc3\xa6schema\x00")
        self.safeSequenceEqual(u, ({"compact": True, "schema": 0},))

    def test_fixstr(self):
        u = Unpacker()
        for i in range(32):
            u.feed(bytes([0xA0 + i] + [0x41] * i))
        ref = tuple(["A" * i for i in range(32)])
        self.safeSequenceEqual(u, ref)

    def test_never_used(self):
        u = Unpacker()
        u.feed(b"\xc1")
        with self.assertRaises(ValueError) as context:
            next(u)
        self.assertEqual(
            str(context.exception), "amsgpack: 0xc1 byte must not be used"
        )

    def test_bin8(self):
        u = Unpacker()
        for i in range(2):
            u.feed(bytes([0xC4, i] + [0x41] * i))
        ref = tuple([b"A" * i for i in range(2)])
        self.safeSequenceEqual(u, ref)

    def test_bin16(self):
        u = Unpacker()
        for i in range(256, 258):
            u.feed(bytes([0xC5, 1, i - 256] + [0x41] * i))
        ref = tuple([b"A" * i for i in range(256, 258)])
        self.safeSequenceEqual(u, ref)

    def test_bin32(self):
        u = Unpacker()
        for i in range(65536, 65538):
            u.feed(bytes([0xC6, 0, 1, 0, i - 65536] + [0x41] * i))
        ref = tuple([b"A" * i for i in range(65536, 65538)])
        self.safeSequenceEqual(u, ref)

    def test_uint_8(self):
        u = Unpacker()
        for char in b"\xcc\x00\xcc\x7f\xcc\x80\xcc\xff":
            u.feed(bytes((char,)))
        self.safeSequenceEqual(u, (0, 127, 128, 255))

    def test_uint_16(self):
        u = Unpacker()
        for char in b"\xcd\x00\x00\xcd\x00\xff\xcd\x01\x00\xcd\xff\xff":
            u.feed(bytes((char,)))
        self.safeSequenceEqual(u, (0, 255, 256, 0xFFFF))

    def test_uint_32(self):
        u = Unpacker()
        for (
            char
        ) in b"\xce\x00\x00\x00\x00\xce\x00\x00\x00\xff\xce\x00\x00\x01\x00\xce\x00\x00\xff\xff\xce\xff\xff\xff\xff":
            u.feed(bytes((char,)))
        self.safeSequenceEqual(u, (0, 255, 256, 0xFFFF, 0xFFFFFFFF))

    def test_ext_4(self):
        ext = Ext(1, b"1234")
        ref_bytes = packb(ext)
        self.assertEqual(ref_bytes, b"\xd6\x011234")
        u = Unpacker()
        for char in ref_bytes:
            u.feed(bytes((char,)))
        self.safeSequenceEqual(u, (ext,))

    def safeSequenceEqual(
        self, unpacker: Unpacker, ref: tuple[Value, ...]
    ) -> None:
        it = iter(unpacker)
        self.assertIs(it, unpacker)
        for idx, item in enumerate(ref):
            self.assertEqual(next(it), item, f"{idx=}")
        with self.assertRaises(StopIteration):
            next(it)


class ExtTest(TestCase):
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
