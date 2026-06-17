from unittest import TestCase
import amsgpack
from io import BytesIO


class BadFileStr:
    def read(self, size: int = -1):
        assert size == -1
        return "string"


class BadFileReadMethod:
    def read(self):
        assert False, "programmer error"


class BadFileReadRaises:
    def read(self, size: int = -1):
        assert size == -1
        raise ValueError("Test Raise is handled")


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
            amsgpack.FileUnpacker(None)  # pyright: ignore [reportArgumentType]
        self.assertEqual(
            str(context.exception),
            "'NoneType' object has no attribute 'read'",
        )

    def test_read_returns_str(self):
        unpacker = amsgpack.FileUnpacker(
            BadFileStr()  # pyright: ignore [reportArgumentType]
        )
        with self.assertRaises(TypeError) as context:
            next(unpacker)
        self.assertEqual(
            str(context.exception),
            "a bytes object is required, not 'str'",
        )

    def test_read_bad_args(self):
        unpacker = amsgpack.FileUnpacker(
            BadFileReadMethod()  # pyright: ignore [reportArgumentType]
        )
        with self.assertRaises(TypeError) as context:
            next(unpacker)
        self.assertIn("takes 1 positional argument", str(context.exception))

    def test_read_raises(self):
        unpacker = amsgpack.FileUnpacker(BadFileReadRaises())
        with self.assertRaises(ValueError) as context:
            next(unpacker)
        self.assertEqual(str(context.exception), "Test Raise is handled")

    def test_read_not_callable(self):
        with self.assertRaises(TypeError) as context:
            amsgpack.FileUnpacker(
                BadFileNotCallable()  # pyright: ignore [reportArgumentType]
            )
        self.assertEqual(
            str(context.exception),
            "`BadFileNotCallable.read` must be callable",
        )

    def test_pass_kwargs(self):
        with self.assertRaises(TypeError) as context:
            amsgpack.FileUnpacker(
                BadFileStr(),
                unicorn=True,  # pyright: ignore [reportCallIssue]
            )
        self.assertIn(
            str(context.exception),
            (
                "'unicorn' is an invalid keyword argument for Unpacker()",
                "Unpacker() got an unexpected keyword argument 'unicorn'",
            ),
        )

    def test_no_arguments(self):
        with self.assertRaises(TypeError) as context:
            amsgpack.FileUnpacker()  # pyright: ignore [reportCallIssue]
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

    def test_read_raises_exception(self):
        class InvalidFile:
            def read(self, size: int | None = -1, /):
                raise ValueError("Oops")

        invalid_file = InvalidFile()
        unpacker = amsgpack.FileUnpacker(invalid_file, 1024)
        with self.assertRaises(ValueError) as context:
            next(unpacker)
        self.assertEqual(str(context.exception), "Oops")

    def test_incorrect_data(self):
        class IncorrectData:
            def read(self, size: int | None = -1, /):
                return b"\x00\xc1"

        invalid_file = IncorrectData()
        unpacker = amsgpack.FileUnpacker(invalid_file, 1024)
        next(unpacker)
        with self.assertRaises(ValueError) as context:
            next(unpacker)
        self.assertEqual(
            str(context.exception), "amsgpack: 0xc1 byte must not be used"
        )

    def test_multiple_reads(self):
        class SingleByteReader:
            def __init__(self, data: bytes) -> None:
                self._data = data
                self._idx = 0

            def read(self, size: int | None = -1, /) -> bytes:
                assert size == 1, size
                try:
                    result = self._data[self._idx : self._idx + 1]
                except IndexError:
                    return b""
                self._idx += 1
                return result

        data = SingleByteReader(amsgpack.packb([1, 2]))
        unpacker = amsgpack.FileUnpacker(data, 1)
        self.assertEqual(next(unpacker), [1, 2])
