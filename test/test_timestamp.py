from amsgpack import Timestamp
from unittest import TestCase


class TimestampTest(TestCase):
    def test_create_0(self):
        ts = Timestamp(seconds=0)
        self.assertEqual((ts.seconds, ts.nanoseconds), (0, 0))
        self.assertEqual(repr(ts), "Timestamp(seconds=0, nanoseconds=0)")

    def test_create_simple(self):
        ts = Timestamp(seconds=123456789, nanoseconds=9876543)
        self.assertEqual((ts.seconds, ts.nanoseconds), (123456789, 9876543))
        self.assertEqual(
            repr(ts), "Timestamp(seconds=123456789, nanoseconds=9876543)"
        )

    def test_create_minimum(self):
        ts = Timestamp(seconds=-0x8000000000000000)
        self.assertEqual(
            (ts.seconds, ts.nanoseconds), (-0x8000000000000000, 0)
        )
        self.assertEqual(eval(repr(ts)), ts)

    def test_create_maximum(self):
        ts = Timestamp(seconds=0x7FFFFFFFFFFFFFFF, nanoseconds=0xFFFFFFFF)
        self.assertEqual(
            (ts.seconds, ts.nanoseconds), (0x7FFFFFFFFFFFFFFF, 0xFFFFFFFF)
        )
        self.assertEqual(eval(repr(ts)), ts)

    def test_create_out_of_range_seconds(self):
        with self.assertRaises(OverflowError):
            Timestamp(seconds=0x8000000000000000)

    def test_compare(self):
        self.assertEqual(Timestamp(1), Timestamp(1))
        self.assertNotEqual(Timestamp(1), Timestamp(2))
        self.assertEqual(Timestamp(0, 1), Timestamp(0, 1))
        self.assertNotEqual(Timestamp(0, 1), Timestamp(0, 2))
        self.assertFalse(Timestamp(0, 1) != Timestamp(0, 1))
        self.assertFalse(Timestamp(0, 1) == Timestamp(0, 2))
        self.assertFalse(Timestamp(0, 1) == "lol")

        a, b = Timestamp(10, 500), Timestamp(10, 500)
        self.assertFalse(a < b)
        self.assertTrue(a <= b)
        self.assertFalse(a > b)
        self.assertTrue(a >= b)

        a, b = Timestamp(5, 300), Timestamp(5, 200)
        self.assertFalse(a < b)
        self.assertFalse(a <= b)
        self.assertTrue(a > b)
        self.assertTrue(a >= b)

        a, b = Timestamp(5, 100), Timestamp(5, 200)
        self.assertTrue(a < b)
        self.assertTrue(a <= b)
        self.assertFalse(a > b)
        self.assertFalse(a >= b)

        a, b = Timestamp(3, 200), Timestamp(2, 999)
        self.assertFalse(a < b)
        self.assertFalse(a <= b)
        self.assertTrue(a > b)
        self.assertTrue(a >= b)

        a, b = Timestamp(1, 500), Timestamp(2, 0)
        self.assertTrue(a < b)
        self.assertTrue(a <= b)
        self.assertFalse(a > b)
        self.assertFalse(a >= b)

    def test_can_not_subclass(self):
        with self.assertRaises(TypeError):

            class SuperTimestamp(  # pyright: ignore [reportUnusedClass]
                Timestamp  # pyright: ignore [reportGeneralTypeIssues]
            ):
                pass

    def test_can_be_in_set(self):
        self.assertEqual(len({Timestamp(0, 1), Timestamp(0, 1)}), 1)
        self.assertEqual(len({Timestamp(0, 1), Timestamp(0, 2)}), 2)
