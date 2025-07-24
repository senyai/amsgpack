from amsgpack import packb, Unpacker, Ext, unpackb
from typing import TypeAlias, cast
from .failing_malloc import failing_malloc, AVAILABLE as FAILING_AVAILABLE
from .test_amsgpack import SequenceTestCase
from unittest import skipUnless
from datetime import datetime, timezone

RecursiveDict: TypeAlias = "dict[int, RecursiveDict | None]"


class UnpackerTest(SequenceTestCase):
    def test_unpacker_gets_no_argumens(self):
        with self.assertRaises(TypeError) as context:
            Unpacker("what", "is", "that")  # pyright: ignore [reportCallIssue]
        self.assertEqual(
            str(context.exception),
            "Unpacker() takes at most 2 arguments (3 given)",
        )
        with self.assertRaises(TypeError) as context:
            Unpacker(what="is that")  # pyright: ignore [reportCallIssue]
        self.assertIn(
            str(context.exception),
            (
                "'what' is an invalid keyword argument for Unpacker()",
                "Unpacker() got an unexpected keyword argument 'what'",
            ),
        )

    def test_feed_nothing(self):
        self.safeSequenceEqual(Unpacker(), ())

    def test_feed_non_bytes(self):
        u = Unpacker()
        with self.assertRaises(TypeError) as context:
            u.feed("")  # pyright: ignore [reportArgumentType]
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

    def test_list_inside_list(self):
        u = Unpacker()
        u.feed(b"\x92\x90\x90")
        self.safeSequenceEqual(u, ([[], []],))

    def test_list_inside_list_as_tuple(self):
        u = Unpacker(tuple=True)
        u.feed(b"\x92\x90\x90")
        self.safeSequenceEqual(u, (((), ()),))

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

    def test_bin8_splitted(self):
        u = Unpacker()
        for char in b"\xc4\x02\x03\x04":
            u.feed(bytes((char,)))
        self.safeSequenceEqual(u, (b"\x03\x04",))

    @skipUnless(FAILING_AVAILABLE, "not failing available")
    def test_bin_malloc_failure(self):
        u = Unpacker()
        for char in b"\xc4\xffaaaaa":
            u.feed(bytes((char,)))
        u.feed(b"b" * 250)
        with self.assertRaises(MemoryError), failing_malloc(20, "mem"):
            self.safeSequenceEqual(u, (b"aaaaa" + b"b" * 250,))

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

    def test_ext_4(self):
        ext = Ext(1, b"1234")
        ref_bytes = packb(ext)
        self.assertEqual(ref_bytes, b"\xd6\x011234")
        u = Unpacker()
        for char in ref_bytes:
            u.feed(bytes((char,)))
        self.safeSequenceEqual(u, (ext,))

    def test_str8_split_in_1_byte(self):
        u = Unpacker()
        for char in b"\xd9\x01A":
            u.feed(bytes((char,)))
        self.assertEqual(list(u), ["A"])

    def test_bin8_split_in_1_byte(self):
        u = Unpacker()
        for char in b"\xc4\x01A":
            u.feed(bytes((char,)))
        self.assertEqual(list(u), [b"A"])

    def test_ext8_split_in_1_byte(self):
        u = Unpacker()
        for char in b"\xc7\x01\x02A":
            u.feed(bytes((char,)))
        self.assertEqual(list(u), [Ext(code=2, data=b"A")])

    def test_str8_zero_size_in_the_middle(self):
        u = Unpacker()
        for seq in (b"|\xd9", b"\x00\x00"):
            u.feed(seq)
        self.assertEqual(list(u), [124, "", 0])

    @skipUnless(FAILING_AVAILABLE, "not failing available")
    def test_bin_PyBytes_FromStringAndSize_malloc(self):
        u = Unpacker()
        u.feed(b"\xc6\x00\x01\x00\x00" + b"A" * 0x10000)
        with self.assertRaises(MemoryError), failing_malloc(0x10000, "raw"):
            next(u)

    def test_bin_header_only(self):
        u = Unpacker()
        u.feed(b"\xc6")
        with self.assertRaises(StopIteration):
            next(u)

    @skipUnless(FAILING_AVAILABLE, "not failing available")
    def test_feed_no_memory(self):
        u = Unpacker()
        with self.assertRaises(MemoryError), failing_malloc(9, "mem"):
            u.feed(b"\xc6")


class UnpackbTest(SequenceTestCase):
    def test_memoryview(self):
        one = unpackb(memoryview(b"\x01"))
        self.assertEqual(one, 1)

    def test_invalid_args(self):
        with self.assertRaises(TypeError) as context:
            unpackb(b"\xcc", 1)  # pyright: ignore [reportCallIssue]
        self.assertIn(
            str(context.exception),
            (
                "Unpacker.unpackb() takes exactly one argument (2 given)",
                "unpackb() takes exactly one argument (2 given)",
            ),
        )

    def test_invalid_type(self):
        with self.assertRaises(TypeError) as context:
            unpackb("\xcc")  # pyright: ignore [reportArgumentType]
        self.assertEqual(
            str(context.exception),
            "unpackb() argument 1 must be bytes, not str",
        )

    def test_extra_data(self):
        with self.assertRaises(ValueError) as context:
            unpackb(b"\x01\x02")
        self.assertEqual(str(context.exception), "Extra data")

    def test_incomplete_data(self):
        with self.assertRaises(ValueError) as context:
            unpackb(b"\xcc")
        self.assertEqual(
            str(context.exception), "Incomplete MessagePack format"
        )


class UnpackbIntTest(SequenceTestCase):
    def test_uint_8_only_ont_byte_is_available(self):
        u = Unpacker()
        u.feed(b"\xcc")
        self.safeSequenceEqual(u, ())

    def test_uint_16(self):
        u = Unpacker()
        for char in b"\xcd\x00\x00\xcd\x00\xff\xcd\x01\x00\xcd\xff\xff":
            u.feed(bytes((char,)))
        self.safeSequenceEqual(u, (0, 255, 256, 0xFFFF))

    def test_uint_16_partly(self):
        u = Unpacker()
        u.feed(b"\xcd")
        self.safeSequenceEqual(u, ())
        u.feed(b"\x00")
        self.safeSequenceEqual(u, ())
        u.feed(b"\x01")
        self.safeSequenceEqual(u, (1,))

    def test_uint_32(self):
        u = Unpacker()
        for (
            char
        ) in b"\xce\x00\x00\x00\x00\xce\x00\x00\x00\xff\xce\x00\x00\x01\x00\xce\x00\x00\xff\xff\xce\xff\xff\xff\xff":
            u.feed(bytes((char,)))
        self.safeSequenceEqual(u, (0, 255, 256, 0xFFFF, 0xFFFFFFFF))

    def test_uint_32_not_ready(self):
        u = Unpacker()
        u.feed(b"\xce")
        self.safeSequenceEqual(u, ())

    def test_uint_64_sliced(self):
        u = Unpacker()
        for char in b"\xcf\x01\x02\x03\x04\x05\x06\x07\x08":
            u.feed(bytes((char,)))
        self.safeSequenceEqual(u, (0x0102030405060708,))

    def test_uint_64_not_ready(self):
        u = Unpacker()
        u.feed(b"\xcf\x01\x02\x03\x04\x05\x06\x07")
        self.safeSequenceEqual(u, ())


class UnpackbFloatTest(SequenceTestCase):
    def test_float32(self):
        u = Unpacker()
        u.feed(b"\xca\x44\xf8\x20\x54")
        self.safeSequenceEqual(u, (1985.01025390625,))

    def test_float32_sliced(self):
        u = Unpacker()
        u.feed(b"\xca\x44")
        u.feed(b"\xf8\x20\x54")
        self.safeSequenceEqual(u, (1985.01025390625,))

    def test_float32_not_ready(self):
        u = Unpacker()
        u.feed(b"\xca\x44")
        u.feed(b"\xf8\x20")
        self.safeSequenceEqual(u, ())


class UnpackbMapTest(SequenceTestCase):
    def test_dict_with_array_value(self):
        self.assertEqual(unpackb(b"\x81\xa1b\x91\x01"), {"b": [1]})
        self.assertEqual(unpackb(b"\x81\xa1b\x91\x90"), {"b": [[]]})

    def test_unpack_incorrect_dict(self):
        with self.assertRaises(TypeError) as context:
            unpackb(b"\x81\x80\x02")
        self.assertEqual(str(context.exception), "unhashable type: 'dict'")

    def test_map_too_big(self):
        with self.assertRaises(ValueError) as context:
            unpackb(b"\xdf\xff\xff\xff\xff")
        self.assertEqual(
            str(context.exception), "dict size 4294967295 is too big (>100000)"
        )

    def test_deeply_nested_okay(self):
        res = cast(
            RecursiveDict,
            unpackb(b"".join(b"\x81\x00" for _ in range(32)) + b"\xc0"),
        )
        ref: RecursiveDict = eval("{0:" * 32 + "None" + "}" * 32)
        self.assertDictEqual(res, ref)

    def test_deeply_nested_exception(self):
        with self.assertRaises(ValueError) as context:
            unpackb(b"".join(b"\x81\x00" for _ in range(33)) + b"\xc0")

        self.assertEqual(str(context.exception), "Deeply nested object")


class UnpackbArrayTest(SequenceTestCase):
    def test_len_0(self):
        u = Unpacker()
        for l0_bin in (b"\x90", b"\xdc\x00\x00", b"\xdd\x00\x00\x00\x00"):
            u.feed(l0_bin)
            self.safeSequenceEqual(u, ([],))

    def test_len_1(self):
        u = Unpacker()
        for l1_bin in (b"\x91", b"\xdc\x00\x01", b"\xdd\x00\x00\x00\x01"):
            u.feed(l1_bin + b"\xc0")
            self.safeSequenceEqual(u, ([None],))

    def test_len_2(self):
        u = Unpacker()
        for l2_bin in (b"\x92", b"\xdc\x00\x02", b"\xdd\x00\x00\x00\x02"):
            u.feed(l2_bin + b"\xc0\xc0")
            self.safeSequenceEqual(u, ([None, None],))

    def test_array_too_big(self):
        with self.assertRaises(ValueError) as context:
            unpackb(b"\xdd\xff\xff\xff\xff")
        self.assertEqual(
            str(context.exception),
            "list size 4294967295 is too big (>10000000)",
        )

    def test_deeply_nested_okay(self):
        res = unpackb(b"".join(b"\x91" for _ in range(32)) + b"\xc0")
        ref = eval(f"[" * 32 + "None" + "]" * 32)
        self.assertListEqual(res, ref)  # pyright: ignore [reportArgumentType]

    def test_deeply_nested_exception(self):
        with self.assertRaises(ValueError) as context:
            unpackb(b"".join(b"\x91" for _ in range(33)) + b"\xc0")

        self.assertEqual(str(context.exception), "Deeply nested object")


class UnpackbStrTest(SequenceTestCase):
    def test_empty_str(self):
        u = Unpacker()
        u.feed(b"\xa0")
        u.feed(b"\xd9\x00")
        u.feed(b"\xda\x00\x00")
        u.feed(b"\xdb\x00\x00\x00\x00")
        self.safeSequenceEqual(u, ("",) * 4)

    def test_str_not_ready(self):
        u = Unpacker()
        u.feed(b"\xd9\x01")
        self.safeSequenceEqual(u, ())

    def test_str_in_parts(self):
        u = Unpacker()
        u.feed(b"\xd9\x05")
        u.feed(b"he")
        u.feed(b"llo")
        self.safeSequenceEqual(u, ("hello",))

    def test_str8_split_in_1_byte(self):
        u = Unpacker()
        for char in b"\xd9\x01A":
            u.feed(bytes((char,)))
        self.assertEqual(list(u), ["A"])

    def test_huge_string(self):
        with self.assertRaises(ValueError) as context:
            unpackb(b"\xdb\x0f\xff\xff\xff")
        self.assertEqual(
            str(context.exception),
            "string size 268435455 is too big (>134217728)",
        )


class UnpackbBinTest(SequenceTestCase):
    def test_huge_data(self):
        with self.assertRaises(ValueError) as context:
            unpackb(b"\xc6\x0f\xff\xff\xff")
        self.assertEqual(
            str(context.exception),
            "bytes size 268435455 is too big (>134217728)",
        )


class UnpackDateTimeTest(SequenceTestCase):
    def test_authors_birthday(self):
        self.assertEqual(
            unpackb(b"\xd7\xff\x00\x00\x00\x00\x1c9\xdfp"),
            datetime(1985, 1, 2, 23, 0, 0, tzinfo=timezone.utc),
        )
