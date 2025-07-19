#include <Python.h>
#include <datetime.h>
#include <inttypes.h>

#include "common.h"

#define DIV_ROUND_CLOSEST_POS(n, d) (((n) + (d) / 2) / (d))
/* 2000-03-01 (mod 400 year, immediately after feb29 */
#define LEAPOCH (946684800LL + 86400 * (31 + 29))
// 365 * 400 + 97
#define DAYS_PER_400Y 146097
#define DAYS_PER_100Y (365 * 100 + 24)
#define DAYS_PER_4Y (365 * 4 + 1)

// 60 * 60 * 24
#define SECONDS_PER_DAY 86400

static int64_t days_since_epoch(int year, int month, int day) {
  if (month <= 2) {
    year--;
    month += 12;
  }

  int era = year / 400;
  int yoe = year - era * 400;
  int doy = (153 * (month - 3) + 2) / 5 + day - 1;
  int doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;

  return era * DAYS_PER_400Y + doe - 719468;
}

// https://github.com/msgpack/msgpack/blob/master/spec.md#timestamp-extension-type
typedef struct {
  int64_t seconds;
  uint32_t nanosec;
} MsgPackTimestamp;

typedef struct {
  PyObject_HEAD
  MsgPackTimestamp timestamp;
} Timestamp;

static PyObject *timestamp_to_datetime(MsgPackTimestamp ts) {
  if A_UNLIKELY(ts.seconds < -62135596800 || ts.seconds > 253402300800) {
    PyErr_SetString(PyExc_ValueError, "timestamp out of range");
    return NULL;
  }
  int64_t years, days, secs;
  int micros = 0, months, remyears, remdays, remsecs;
  int qc_cycles, c_cycles, q_cycles;

  if (ts.nanosec != 0) {
    micros = DIV_ROUND_CLOSEST_POS(ts.nanosec, 1000);
    if (micros == 1000000) {
      micros = 0;
      ts.seconds++;
    }
  }

  static const char days_in_month[] = {31, 30, 31, 30, 31, 31,
                                       30, 31, 30, 31, 31, 29};

  secs = ts.seconds - LEAPOCH;
  days = secs / 86400;
  remsecs = secs % 86400;
  if (remsecs < 0) {
    remsecs += 86400;
    days--;
  }

  qc_cycles = days / DAYS_PER_400Y;
  remdays = days % DAYS_PER_400Y;
  if (remdays < 0) {
    remdays += DAYS_PER_400Y;
    qc_cycles--;
  }

  c_cycles = remdays / DAYS_PER_100Y;
  if (c_cycles == 4) {
    c_cycles--;
  }
  remdays -= c_cycles * DAYS_PER_100Y;

  q_cycles = remdays / DAYS_PER_4Y;
  if (q_cycles == 25) {
    q_cycles--;
  }
  remdays -= q_cycles * DAYS_PER_4Y;

  remyears = remdays / 365;
  if (remyears == 4) {
    remyears--;
  }
  remdays -= remyears * 365;

  years = remyears + 4 * q_cycles + 100 * c_cycles + 400LL * qc_cycles;

  for (months = 0; days_in_month[months] <= remdays; months++) {
    remdays -= days_in_month[months];
  }

  if (months >= 10) {
    months -= 12;
    years++;
  }

  return PyDateTimeAPI->DateTime_FromDateAndTime(
      years + 2000, months + 3, remdays + 1, remsecs / 3600, remsecs / 60 % 60,
      remsecs % 60, micros, PyDateTime_TimeZone_UTC,
      PyDateTimeAPI->DateTimeType);
}

static inline MsgPackTimestamp datetime_to_timestamp(PyObject *dt) {
  assert(PyDateTime_CheckExact(dt));
  int const year = PyDateTime_GET_YEAR(dt);
  int const month = PyDateTime_GET_MONTH(dt);
  int const day = PyDateTime_GET_DAY(dt);
  int const hour = PyDateTime_DATE_GET_HOUR(dt);
  int const minute = PyDateTime_DATE_GET_MINUTE(dt);
  int const second = PyDateTime_DATE_GET_SECOND(dt);
  int const microsecond = PyDateTime_DATE_GET_MICROSECOND(dt);
  int64_t const days = days_since_epoch(year, month, day);
  int64_t const total_seconds =
      days * SECONDS_PER_DAY + hour * 3600 + minute * 60 + second;
  uint64_t const nanoseconds = (uint32_t)microsecond * 1000;
  return (MsgPackTimestamp){.seconds = total_seconds, .nanosec = nanoseconds};
}

static PyObject *Timestamp_richcompare(Timestamp *self, PyObject *other,
                                       int op) {
  if (!Py_IS_TYPE(other, Py_TYPE(self))) {
    Py_RETURN_FALSE;
  }
  Timestamp const *other_ts = (Timestamp *)other;

  int const sec_equal = self->timestamp.seconds == other_ts->timestamp.seconds;
  switch (op) {
    case Py_LT:
      if (self->timestamp.seconds < other_ts->timestamp.seconds ||
          (sec_equal &&
           self->timestamp.nanosec < other_ts->timestamp.nanosec)) {
        Py_RETURN_TRUE;
      }
      Py_RETURN_FALSE;
    case Py_GT:
      if (self->timestamp.seconds > other_ts->timestamp.seconds ||
          (sec_equal &&
           self->timestamp.nanosec > other_ts->timestamp.nanosec)) {
        Py_RETURN_TRUE;
      }
      Py_RETURN_FALSE;

    case Py_EQ:
    case Py_NE: {
      int const is_equal =
          sec_equal && self->timestamp.nanosec == other_ts->timestamp.nanosec;
      if (op == Py_EQ ? is_equal : !is_equal) {
        Py_RETURN_TRUE;
      } else {
        Py_RETURN_FALSE;
      }
    }
    case Py_LE:
      if (self->timestamp.seconds < other_ts->timestamp.seconds ||
          (sec_equal &&
           self->timestamp.nanosec <= other_ts->timestamp.nanosec)) {
        Py_RETURN_TRUE;
      }
      Py_RETURN_FALSE;
    case Py_GE:
      if (self->timestamp.seconds > other_ts->timestamp.seconds ||
          (sec_equal &&
           self->timestamp.nanosec >= other_ts->timestamp.nanosec)) {
        Py_RETURN_TRUE;
      }
      Py_RETURN_FALSE;
    default:             // GCOVR_EXCL_LINE
      Py_UNREACHABLE();  // GCOVR_EXCL_LINE
  }
}

static PyMemberDef Timestamp_members[] = {
    {"seconds", T_LONGLONG, offsetof(Timestamp, timestamp.seconds), READONLY,
     "64-bit signed int"},
    {"nanoseconds", T_ULONG, offsetof(Timestamp, timestamp.nanosec), READONLY,
     "32-bit unsigned int"},
    {NULL, 0, 0, 0, NULL}  // Sentinel
};

static Py_hash_t Timestamp_hash(Timestamp *self) {
  Py_hash_t const sec_hash = (Py_hash_t)self->timestamp.seconds;
  Py_hash_t const nan_hash = (Py_hash_t)self->timestamp.nanosec;
  Py_hash_t const hash =
      sec_hash ^
      (nan_hash + 0x9e3779b9 + ((Py_uhash_t)sec_hash << 6) + (sec_hash >> 2));

  // Special case: if final hash is -1, return -2 instead
  // because -1 indicates error in Python's hash functions
  return hash == -1 ? -2 : hash;
}

static int Timestamp_init(Timestamp *self, PyObject *args, PyObject *kwargs) {
  static char *kwlist[] = {"seconds", "nanoseconds", NULL};
  if A_UNLIKELY(!PyArg_ParseTupleAndKeywords(args, kwargs, "L|I:Timestamp",
                                             kwlist, &self->timestamp.seconds,
                                             &self->timestamp.nanosec)) {
    return -1;
  }
  return 0;
}

static PyObject *Timestamp_repr(Timestamp *self) {
  return PyUnicode_FromFormat(
#ifndef PYPY_VERSION
      "Timestamp(seconds=%lli, nanoseconds=%u)",
#else
      "Timestamp(seconds=%ld, nanoseconds=%u)",
#endif
      self->timestamp.seconds, self->timestamp.nanosec);
}

PyDoc_STRVAR(Timestamp_doc,
             "Timestamp extension type from MessagePack specification.\n\n"
             ".. code-block::\n\n"
             "   >>> from amsgpack import Timestamp, packb\n"
             "   >>> ts = Timestamp(seconds=1752955664)\n"
             "   >>> ts\n"
             "   Timestamp(seconds=1752955664, nanoseconds=0)\n"
             "   >>> packb(ts)\n"
             "   b'\\xd6\\xffh{\\xfb\\x10'\n"
             "   >>> unpackb(packb(ts))\n"
             "   datetime.datetime(2025, 7, 19, 20, 7, 44, "
             "tzinfo=datetime.timezone.utc)\n\n"
             "See ``ext_hook`` on how to return :class:`Timestamp` instead of "
             "``datetime.datetime``");

BEGIN_NO_PEDANTIC
static PyType_Slot Timestamp_slots[] = {
    {Py_tp_doc, (char *)Timestamp_doc},
    {Py_tp_new, PyType_GenericNew},
    {Py_tp_init, Timestamp_init},
    {Py_tp_repr, Timestamp_repr},
    {Py_tp_members, Timestamp_members},
    {Py_tp_hash, Timestamp_hash},
    {Py_tp_richcompare, Timestamp_richcompare},
    {0, NULL}};
END_NO_PEDANTIC

static PyType_Spec Timestamp_spec = {
    .name = "amsgpack.Timestamp",
    .basicsize = sizeof(Timestamp),
    .flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_IMMUTABLETYPE,
    .slots = Timestamp_slots,
};

#undef DIV_ROUND_CLOSEST_POS
#undef LEAPOCH
#undef DAYS_PER_400Y
#undef DAYS_PER_100Y
#undef DAYS_PER_4Y
