from unittest import TestCase
import amsgpack
from io import BytesIO


class BadFileStr:
    def read(self):
        return "string"


class BadFileNotCallable:
    read = "string"


class InfiniteFile:
    size = object()

    def read(self, size: int | None = -1, /):
        self.size = size
        return b"\x91\xff"


class FileUnpackerTest(TestCase):
    def test_basic(self):
        buf = BytesIO()
        for i in range(100):
            buf.write(amsgpack.packb(i))

        buf.seek(0)

        unpacker = amsgpack.FileUnpacker(buf, 10)
        self.assertEqual(list(unpacker), list(range(100)))

    def test_file_is_none(self):
        with self.assertRaises(AttributeError) as context:
            amsgpack.FileUnpacker(None)
        self.assertEqual(
            str(context.exception),
            "'NoneType' object has no attribute 'read'",
        )

    def test_read_returns_str(self):
        unpacker = amsgpack.FileUnpacker(BadFileStr())
        with self.assertRaises(TypeError) as context:
            next(unpacker)
        self.assertEqual(
            str(context.exception),
            "a bytes object is required, not 'str'",
        )

    def test_read_not_callable(self):
        with self.assertRaises(TypeError) as context:
            amsgpack.FileUnpacker(BadFileNotCallable())
        self.assertEqual(
            str(context.exception),
            "`BadFileNotCallable.read` must be callable",
        )

    def test_pass_kwargs(self):
        with self.assertRaises(TypeError) as context:
            amsgpack.FileUnpacker(BadFileStr(), unicorn=True)
        self.assertIn(
            str(context.exception),
            (
                "'unicorn' is an invalid keyword argument for Unpacker()",
                "Unpacker() got an unexpected keyword argument 'unicorn'",
            ),
        )

    def test_no_arguments(self):
        with self.assertRaises(TypeError) as context:
            amsgpack.FileUnpacker()
        self.assertEqual(
            str(context.exception),
            "FileUnpacker() takes at least 1 argument (0 given)",
        )

    def test_read_without_arguments(self):
        infinite_file = InfiniteFile()
        unpacker = amsgpack.FileUnpacker(infinite_file)
        self.assertEqual(next(unpacker), [-1])
        self.assertEqual(infinite_file.size, -1)

    def test_read_with_argument(self):
        infinite_file = InfiniteFile()
        unpacker = amsgpack.FileUnpacker(infinite_file, 1024, tuple=True)
        self.assertEqual(next(unpacker), (-1,))
        self.assertEqual(infinite_file.size, 1024)
