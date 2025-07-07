#include <Python.h>

#include "deque.h"
#define MiB128 134217728

#ifndef PYPY_VERSION
#define ANEW_DICT _PyDict_NewPresized
#else
#define ANEW_DICT(N) PyDict_New()
#endif

typedef struct {
  enum UnpackAction { SEQUENCE_APPEND, DICT_KEY, DICT_VALUE } action;
  PyObject* sequence;
  Py_ssize_t size;
  Py_ssize_t pos;
  union {
    PyObject* key;
    PyObject** values;  // SEQUENCE_APPEND only
  };
} Stack;

typedef struct {
  Py_ssize_t await_bytes;  // number of bytes we are currently awaiting
  Py_ssize_t stack_length;
  Stack stack[A_STACK_SIZE];
} Parser;

static inline int can_not_append_stack(Parser const* parser) {
  return parser->stack_length >= A_STACK_SIZE;
}

typedef struct {
  PyObject_HEAD
  Deque deque;
  Parser parser;
  AMsgPackState* state;
  int use_tuple;
} Unpacker;

typedef union A_WORD {
  int16_t s;
  uint16_t us;
  char bytes[2];
} A_WORD;
typedef char check_word_size[sizeof(A_WORD) == 2 ? 1 : -1];

static inline A_WORD read_a_word(char const* data) {
  return (A_WORD){.bytes = {data[1], data[0]}};
}

typedef union A_DWORD {
  int32_t l;
  uint32_t ul;
  float f;
  char bytes[4];
} A_DWORD;

typedef char check_dword_size[sizeof(A_DWORD) == 4 ? 1 : -1];

static inline A_DWORD read_a_dword(char const* data) {
  return (A_DWORD){.bytes = {data[3], data[2], data[1], data[0]}};
}

typedef union A_QWORD {
  int64_t ll;
  uint64_t ull;
  double d;
  char bytes[8];
} A_QWORD;

static inline A_QWORD read_a_qword(char const* data) {
  return (A_QWORD){.bytes = {data[7], data[6], data[5], data[4], data[3],
                             data[2], data[1], data[0]}};
}

typedef char check_qword_size[sizeof(A_QWORD) == 8 ? 1 : -1];

static PyObject* size_error(char type[], Py_ssize_t length, Py_ssize_t limit) {
  return PyErr_Format(PyExc_ValueError, "%s size %zd is too big (>%zd)", type,
                      length, limit);
}

#define READ_A_DATA(length)                                       \
  char const* data = deque_read_bytes_fast(&self->deque, length); \
  char* allocated = NULL;                                         \
  if A_UNLIKELY(data == NULL) {                                   \
    data = allocated = deque_read_bytes(&self->deque, length);    \
    if A_UNLIKELY(allocated == NULL) {                            \
      return NULL;                                                \
    }                                                             \
  }

#define FREE_A_DATA(length)                            \
  do {                                                 \
    if A_LIKELY(allocated == NULL) {                   \
      deque_advance_first_bytes(&self->deque, length); \
    } else {                                           \
      PyMem_Free(allocated);                           \
    }                                                  \
  } while (0)

#define READ_A_WORD           \
  A_WORD word;                \
  do {                        \
    READ_A_DATA(2);           \
    word = read_a_word(data); \
    FREE_A_DATA(2);           \
  } while (0)

#define READ_A_DWORD            \
  A_DWORD dword;                \
  do {                          \
    READ_A_DATA(4);             \
    dword = read_a_dword(data); \
    FREE_A_DATA(4);             \
  } while (0)

#define READ_A_QWORD            \
  A_QWORD qword;                \
  do {                          \
    READ_A_DATA(8);             \
    qword = read_a_qword(data); \
    FREE_A_DATA(8);             \
  } while (0)

#pragma pack(push, 4)
typedef union {
  struct {
    int64_t seconds : 64;
    uint32_t nanosec : 32;
  };
  char bytes[12];
} TIMESTAMP96;
#pragma pack(pop)

typedef char _check_timestamp_96_size[sizeof(TIMESTAMP96) == 12 ? 1 : -1];

typedef struct {
  int64_t seconds;
  uint32_t nanosec;
} MsgPackTimestamp;

static inline MsgPackTimestamp parse_timestamp(char const* data,
                                               Py_ssize_t data_length) {
  MsgPackTimestamp timestamp;
  if (data_length == 8) {
    A_QWORD const timestamp64 = read_a_qword(data);
    timestamp.seconds = timestamp64.ull & 0x3ffffffff;  // 34 bits
    timestamp.nanosec = timestamp64.ull >> 34;          // 30 bits
  } else if (data_length == 4) {
    A_DWORD const timestamp32 = read_a_dword(data);
    timestamp.seconds = (int64_t)timestamp32.ul;
    timestamp.nanosec = 0;
  } else if (data_length == 12) {
    TIMESTAMP96 const timestamp96 = {
        .bytes = {data[11], data[10], data[9], data[8], data[7], data[6],
                  data[5], data[4], data[3], data[2], data[1], data[0]}};
    timestamp.seconds = timestamp96.seconds;
    timestamp.nanosec = timestamp96.nanosec;
  } else {             // GCOVR_EXCL_LINE
    Py_UNREACHABLE();  // GCOVR_EXCL_LINE
  }
  return timestamp;
}

#define DIV_ROUND_CLOSEST_POS(n, d) (((n) + (d) / 2) / (d))
/* 2000-03-01 (mod 400 year, immediately after feb29 */
#define LEAPOCH (946684800LL + 86400 * (31 + 29))
#define DAYS_PER_400Y (365 * 400 + 97)
#define DAYS_PER_100Y (365 * 100 + 24)
#define DAYS_PER_4Y (365 * 4 + 1)

static PyObject* ext_to_timestamp(char const* data, Py_ssize_t data_length) {
  MsgPackTimestamp ts = parse_timestamp(data, data_length);
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

static PyObject* Unpacker_iternext(Unpacker* self) {
parse_next:
  if (!deque_has_next_byte(&self->deque)) {
    return NULL;
  }
  char const next_byte = deque_peek_byte(&self->deque);
  PyObject* parsed_object = NULL;
  // to allow passing length between switch cases
  union State {
    Py_ssize_t str_length;
    Py_ssize_t bin_length;
    Py_ssize_t arr_length;
    Py_ssize_t map_length;
    Py_ssize_t ext_length;  // without code
  } state;
  switch (next_byte) {
    case '\x80':
      parsed_object = ANEW_DICT(0);
      if A_UNLIKELY(parsed_object == NULL) {
        return NULL;
      }
      deque_advance_first_bytes(&self->deque, 1);
      break;
    case '\x81':
    case '\x82':
    case '\x83':
    case '\x84':
    case '\x85':
    case '\x86':
    case '\x87':
    case '\x88':
    case '\x89':
    case '\x8a':
    case '\x8b':
    case '\x8c':
    case '\x8d':
    case '\x8e':
    case '\x8f':  // fixmap
      state.map_length = Py_CHARMASK(next_byte) & 0x0f;
      deque_advance_first_bytes(&self->deque, 1);
      goto map_length;
    map_length: {
      if A_UNLIKELY(can_not_append_stack(&self->parser)) {
        PyErr_SetString(PyExc_ValueError, "Deeply nested object");
        return NULL;
      }
      parsed_object = ANEW_DICT(state.map_length);
      if A_UNLIKELY(parsed_object == NULL) {
        return NULL;
      }
      if (state.map_length == 0) {
        break;
      }
      self->parser.stack[self->parser.stack_length++] =
          (Stack){.action = DICT_KEY,
                  .sequence = parsed_object,
                  .size = state.map_length,
                  .pos = 0};
      goto parse_next;
    }
    case '\x90':
    case '\x91':
    case '\x92':
    case '\x93':
    case '\x94':
    case '\x95':
    case '\x96':
    case '\x97':
    case '\x98':
    case '\x99':
    case '\x9a':
    case '\x9b':
    case '\x9c':
    case '\x9d':
    case '\x9e':
    case '\x9f': {  // fixarray
      state.arr_length = Py_CHARMASK(next_byte) & 0x0f;
      deque_advance_first_bytes(&self->deque, 1);
      goto arr_length;
    }
    arr_length: {
      if A_UNLIKELY(can_not_append_stack(&self->parser)) {
        PyErr_SetString(PyExc_ValueError, "Deeply nested object");
        return NULL;
      }
      parsed_object =
          (self->use_tuple == 0 ? PyList_New : PyTuple_New)(state.arr_length);
      if A_UNLIKELY(parsed_object == NULL) {
        return NULL;
      }
      if (state.arr_length == 0) {
        break;
      }
#ifndef PYPY_VERSION
      PyObject** values = self->use_tuple == 0
                              ? ((PyListObject*)parsed_object)->ob_item
                              : ((PyTupleObject*)parsed_object)->ob_item;
#else
      PyObject** values = PySequence_Fast_ITEMS(parsed_object);
#endif
      self->parser.stack[self->parser.stack_length++] =
          (Stack){.action = SEQUENCE_APPEND,
                  .sequence = parsed_object,
                  .size = state.arr_length,
                  .pos = 0,
                  .values = values};
      goto parse_next;
    }
    case '\xa1':
    case '\xa2':
    case '\xa3':
    case '\xa4':
    case '\xa5':
    case '\xa6':
    case '\xa7':
    case '\xa8':
    case '\xa9':
    case '\xaa':
    case '\xab':
    case '\xac':
    case '\xad':
    case '\xae':
    case '\xaf':
    case '\xb0':
    case '\xb1':
    case '\xb2':
    case '\xb3':
    case '\xb4':
    case '\xb5':
    case '\xb6':
    case '\xb7':
    case '\xb8':
    case '\xb9':
    case '\xba':
    case '\xbb':
    case '\xbc':
    case '\xbd':
    case '\xbe':
    case '\xbf':  // fixstr
      state.str_length = Py_CHARMASK(next_byte) & 0x1f;
      if A_LIKELY(deque_has_next_n_bytes(&self->deque, state.str_length + 1)) {
        deque_advance_first_bytes(&self->deque, 1);
        goto str_length;
      }
      return NULL;
    str_length: {
      READ_A_DATA(state.str_length);
      parsed_object = PyUnicode_DecodeUTF8(data, state.str_length, NULL);
      FREE_A_DATA(state.str_length);
      if A_UNLIKELY(parsed_object == NULL) {
        return NULL;
      }
      break;
    }
    case '\xc1':  // (never used)
      PyErr_SetString(PyExc_ValueError, "amsgpack: 0xc1 byte must not be used");
      return NULL;
    case '\xc4':  // bin 8
    case '\xc5':  // bin 16
    case '\xc6':  // bin 32
    {
      unsigned char const size_size = 1 << (next_byte - '\xc4');
      if A_LIKELY(deque_has_next_n_bytes(&self->deque, 1 + size_size)) {
        state.bin_length = deque_peek_size(&self->deque, size_size);
        if A_UNLIKELY(state.bin_length > MiB128) {
          return size_error("bytes", state.bin_length, MiB128);
        }
        if (deque_has_next_n_bytes(&self->deque,
                                   1 + size_size + state.bin_length)) {
          deque_skip_size(&self->deque, size_size);
          if A_UNLIKELY(state.bin_length == 0) {
            parsed_object = PyBytes_FromStringAndSize(NULL, state.bin_length);
          } else {
            READ_A_DATA(state.bin_length);
            parsed_object = PyBytes_FromStringAndSize(data, state.bin_length);
            FREE_A_DATA(state.bin_length);
          }
          if A_UNLIKELY(parsed_object == NULL) {
            return NULL;
          }
          break;
        }
      }
      return NULL;
    }
    case '\xc7':  // ext 8
    case '\xc8':  // ext 16
    case '\xc9':  // ext 32
    {
      unsigned char const size_size = 1 << (next_byte - '\xc7');
      if A_LIKELY(deque_has_next_n_bytes(&self->deque, 1 + size_size + 1)) {
        state.ext_length = deque_peek_size(&self->deque, size_size);
        if A_UNLIKELY(state.ext_length >= MiB128) {
          return size_error("ext", state.ext_length, MiB128);
        }
        if A_LIKELY(deque_has_next_n_bytes(
                        &self->deque, 1 + size_size + 1 + state.ext_length)) {
          deque_skip_size(&self->deque, size_size);
          goto ext_length;
        }
      }
      return NULL;
    }
    case '\xd0':  // int_8
    case '\xcc':  // uint_8
      if (deque_has_next_n_bytes(&self->deque, 2)) {
        deque_advance_first_bytes(&self->deque, 1);
        char const byte = deque_peek_byte(&self->deque);
        long const value =
            next_byte == '\xcc' ? (long)(unsigned char)byte : (long)byte;
        parsed_object = PyLong_FromLong(value);
        if A_UNLIKELY(parsed_object == NULL) {
          return NULL;
        }
        deque_advance_first_bytes(&self->deque, 1);
        break;
      }
      return NULL;
    case '\xd1':  // int_16
    case '\xcd':  // uint_16
      if A_LIKELY(deque_has_next_n_bytes(&self->deque, 3)) {
        deque_advance_first_bytes(&self->deque, 1);
        READ_A_WORD;
        parsed_object = next_byte == '\xcd' ? PyLong_FromLong((long)word.us)
                                            : PyLong_FromLong((long)word.s);
        if A_UNLIKELY(parsed_object == NULL) {
          return NULL;
        }
        break;
      }
      return NULL;
    case '\xd2':  // int_32
    case '\xce':  // uint_32
      if A_LIKELY(deque_has_next_n_bytes(&self->deque, 5)) {
        deque_advance_first_bytes(&self->deque, 1);
        READ_A_DWORD;
        parsed_object = next_byte == '\xce' ? PyLong_FromUnsignedLong(dword.ul)
                                            : PyLong_FromLong(dword.l);
        if A_UNLIKELY(parsed_object == NULL) {
          return NULL;
        }
        break;
      }
      return NULL;
    case '\xd3':  // int_64
    case '\xcf':  // uint_64
      if A_LIKELY(deque_has_next_n_bytes(&self->deque, 9)) {
        deque_advance_first_bytes(&self->deque, 1);
        READ_A_QWORD;
        parsed_object = next_byte == '\xcf'
                            ? PyLong_FromUnsignedLongLong(qword.ull)
                            : PyLong_FromLongLong(qword.ll);
        if A_UNLIKELY(parsed_object == NULL) {
          return NULL;
        }
        break;
      }
      return NULL;
    case '\xca':  // float (float_32)
      if A_LIKELY(deque_has_next_n_bytes(&self->deque, 5)) {
        deque_advance_first_bytes(&self->deque, 1);
        READ_A_DWORD;
        parsed_object = PyFloat_FromDouble((double)dword.f);
        if A_UNLIKELY(parsed_object == NULL) {
          return NULL;
        }
        break;
      }
      return NULL;
    case '\xcb':  // double (float_64)
      if (deque_has_next_n_bytes(&self->deque, 9)) {
        deque_advance_first_bytes(&self->deque, 1);
        READ_A_QWORD;
        parsed_object = PyFloat_FromDouble(qword.d);
        if (parsed_object == NULL) {
          return NULL;
        }
        break;
      }
      return NULL;
    case '\xd4':  // fixext 1
    case '\xd5':  // fixext 2
    case '\xd6':  // fixext 4
    case '\xd7':  // fixext 8
    case '\xd8':  // fixext 16
      state.ext_length = (Py_ssize_t)1 << (next_byte - '\xd4');
      if A_LIKELY(deque_has_next_n_bytes(&self->deque, 2 + state.ext_length)) {
        deque_advance_first_bytes(&self->deque, 1);
        goto ext_length;
      }
      return NULL;
    ext_length: {
      READ_A_DATA(state.ext_length + 1);
      char const code = data[0];
      if (code == -1 && (state.ext_length == 8 || state.ext_length == 4 ||
                         state.ext_length == 12)) {
        parsed_object = ext_to_timestamp(data + 1, state.ext_length);
        if A_UNLIKELY(parsed_object == NULL) {
          PyMem_Free(allocated);
          return NULL;  // likely overflow error
        }
      } else {
        Ext* ext = PyObject_New(Ext, self->state->ext_type);
        if A_UNLIKELY(ext == NULL) {
          PyMem_Free(allocated);
          return NULL;  // Allocation failed
        }
        ext->code = code;
        ext->data = PyBytes_FromStringAndSize(data + 1, state.ext_length);
        if A_UNLIKELY(ext->data == NULL) {
          Py_DECREF(ext);
          PyMem_Free(allocated);
          return NULL;
        }
        parsed_object = (PyObject*)ext;
      }
      FREE_A_DATA(state.ext_length + 1);
      break;
    }
    case '\xd9':  // str 8
    case '\xda':  // str 16
    case '\xdb':  // str 32
    {
      unsigned char const size_size = 1 << (next_byte - '\xd9');
      if A_LIKELY(deque_has_next_n_bytes(&self->deque, 1 + size_size)) {
        state.str_length = deque_peek_size(&self->deque, size_size);
        if A_UNLIKELY(state.str_length > MiB128) {
          return size_error("string", state.str_length, MiB128);
        }
        if A_LIKELY(deque_has_next_n_bytes(&self->deque,
                                           1 + size_size + state.str_length)) {
          deque_skip_size(&self->deque, size_size);
          if A_UNLIKELY(state.str_length == 0) {
            parsed_object = self->state->byte_object[EMPTY_STRING_IDX];
            Py_INCREF(parsed_object);
            break;
          }
          goto str_length;
        }
      }
      return NULL;
    }
    case '\xdc':  // array 16
      if A_LIKELY(deque_has_next_n_bytes(&self->deque, 3)) {
        deque_advance_first_bytes(&self->deque, 1);
        READ_A_WORD;
        state.arr_length = word.us;
        goto arr_length;
      }
      return NULL;
    case '\xdd':  // array 32
      if A_LIKELY(deque_has_next_n_bytes(&self->deque, 5)) {
        deque_advance_first_bytes(&self->deque, 1);
        READ_A_DWORD;
        state.arr_length = dword.ul;
        if A_UNLIKELY(state.arr_length > 10000000) {
          return size_error("list", state.arr_length, 10000000);
        }
        goto arr_length;
      }
      return NULL;
    case '\xde':  // map 16
      if A_LIKELY(deque_has_next_n_bytes(&self->deque, 3)) {
        deque_advance_first_bytes(&self->deque, 1);
        READ_A_WORD;
        state.map_length = word.us;
        goto map_length;
      }
      return NULL;
    case '\xdf':  // map 32
      if A_LIKELY(deque_has_next_n_bytes(&self->deque, 5)) {
        deque_advance_first_bytes(&self->deque, 1);
        READ_A_DWORD;
        state.map_length = dword.ul;
        if A_UNLIKELY(state.map_length > 100000) {
          return size_error("dict", state.map_length, 100000);
        }
        goto map_length;
      }
      return NULL;
    default:
      parsed_object = self->state->byte_object[(unsigned char)next_byte];
      assert(parsed_object != NULL);
      deque_advance_first_bytes(&self->deque, 1);
      Py_INCREF(parsed_object);
  }
  while (self->parser.stack_length > 0) {
    Stack* const item = &self->parser.stack[self->parser.stack_length - 1];
    switch (item->action) {
      case SEQUENCE_APPEND:
        assert(item->pos < item->size);
        item->values[item->pos] = parsed_object;
        item->pos += 1;
        if A_UNLIKELY(item->pos == item->size) {
          parsed_object = item->sequence;
          self->parser.stack_length -= 1;
          break;
        }
        goto parse_next;
      case DICT_KEY:
        assert(item->key == NULL);
        assert(parsed_object != NULL);
        item->action = DICT_VALUE;
        item->key = parsed_object;
        goto parse_next;
      case DICT_VALUE: {
        assert(item->pos < item->size);
        int const set_item_result =
            PyDict_SetItem(item->sequence, item->key, parsed_object);
        Py_DECREF(item->key);
        // The `key` must be set to NULL, because the unpacker can be deleted
        // after there's no more data
        item->key = NULL;
        Py_DECREF(parsed_object);
        if A_UNLIKELY(set_item_result != 0) {
          return NULL;
        }
        item->action = DICT_KEY;
        item->pos += 1;
        if A_UNLIKELY(item->pos == item->size) {
          parsed_object = item->sequence;
          self->parser.stack_length -= 1;
          break;
        }
        goto parse_next;
      }
      default:             // GCOVR_EXCL_LINE
        Py_UNREACHABLE();  // GCOVR_EXCL_LINE
    }
  }
  return parsed_object;
}

static PyObject* Unpacker_new(PyTypeObject* type, PyObject* args,
                              PyObject* kwargs) {
  static char* keywords[] = {"tuple", NULL};
  int use_tuple = 0;
  if (!PyArg_ParseTupleAndKeywords(args, kwargs, "|$p:Unpacker", keywords,
                                   &use_tuple)) {
    return NULL;
  }

  Unpacker* self = (Unpacker*)type->tp_alloc(type, 0);

  if A_LIKELY(self != NULL) {
    // zerofilled by python
    // init deque
    // memset(&self->deque, 0, sizeof(Deque));
    // init parser
    // memset(&self->parser, 0, sizeof(Parser));
    self->state = PyType_GetModuleState(type);
    if A_UNLIKELY(self->state == NULL) {
      return NULL;
    };
    self->use_tuple = use_tuple;
  }
  return (PyObject*)self;
}

static PyObject* unpacker_feed(Unpacker* self, PyObject* obj) {
  if A_UNLIKELY(PyBytes_CheckExact(obj) == 0) {
    PyErr_Format(PyExc_TypeError, "a bytes object is required, not '%.100s'",
                 Py_TYPE(obj)->tp_name);
    return NULL;
  }
  if A_UNLIKELY(deque_append(&self->deque, obj) < 0) {
    return PyErr_NoMemory();
  }
  Py_RETURN_NONE;
}

static void Unpacker_dealloc(Unpacker* self) {
  deque_clean(&self->deque);
  while (self->parser.stack_length) {
    Stack* item = self->parser.stack + (--self->parser.stack_length);
    Py_DECREF(item->sequence);
    if (item->action != SEQUENCE_APPEND) {
      Py_XDECREF(item->key);
    }
    memset(item, 0, sizeof(Stack));
  }
  Py_TYPE(self)->tp_free((PyObject*)self);
}

#undef MiB128
#undef ANEW_DICT
#undef READ_A_DATA
#undef FREE_A_DATA
#undef READ_A_WORD
#undef READ_A_DWORD
#undef READ_A_QWORD
#undef DIV_ROUND_CLOSEST_POS
#undef LEAPOCH
#undef DAYS_PER_400Y
#undef DAYS_PER_100Y
#undef DAYS_PER_4Y

static PyObject* unpackb(PyObject* restrict module, PyObject* restrict args,
                         PyObject* restrict kwargs) {
  PyObject* obj = NULL;
  if A_UNLIKELY(!PyArg_ParseTuple(args, "O:unpackb", &obj)) {
    return NULL;
  }

  if (PyBytes_CheckExact(obj) == 0) {
    PyObject* bytes_obj = PyBytes_FromObject(obj);
    if (bytes_obj == NULL) {
      PyErr_Format(PyExc_TypeError, "an obj object is required, not '%.100s'",
                   Py_TYPE(obj)->tp_name);
      return NULL;
    }
    obj = bytes_obj;
  } else {
    Py_INCREF(obj);  // so we can safely decref it later in this function
  }
  AMsgPackState* state = get_amsgpack_state(module);
  PyObject* no_args = state->byte_object[EMPTY_TUPLE_IDX];

  Unpacker* unpacker =
      (Unpacker*)Unpacker_new(state->unpacker_type, no_args, kwargs);
  if A_UNLIKELY(unpacker == NULL) {
    Py_DECREF(obj);
    return NULL;
  }
  int const append_result = deque_append(&unpacker->deque, obj);
  Py_DECREF(obj);
  if A_UNLIKELY(append_result < 0) {
    return NULL;
  }
  PyObject* ret = Unpacker_iternext(unpacker);
  if (ret == NULL) {
    if A_UNLIKELY(PyErr_Occurred() == NULL) {
      PyErr_SetString(PyExc_ValueError, "Incomplete Message Pack format");
    }
    goto error;
  }
  if A_UNLIKELY(unpacker->deque.deque_first != NULL) {
    PyErr_SetString(PyExc_ValueError, "Extra data");
    goto error;
  }
  Py_DECREF(unpacker);
  return ret;
error:
  Py_DECREF(unpacker);
  return NULL;
}

/*
  Unpacker class
*/
PyDoc_STRVAR(unpacker_feed_doc,
             "feed($self, bytes, /)\n--\n\n"
             "Append ``bytes`` to internal buffer.");

static PyMethodDef Unpacker_Methods[] = {
    {"feed", (PyCFunction)&unpacker_feed, METH_O, unpacker_feed_doc},
    {NULL, NULL, 0, NULL}  // Sentinel
};

BEGIN_NO_PEDANTIC
static PyType_Slot Unpacker_slots[] = {
    {Py_tp_doc, PyDoc_STR("Unpack bytes to python objects")},
    {Py_tp_new, Unpacker_new},
    {Py_tp_dealloc, (destructor)Unpacker_dealloc},
    {Py_tp_methods, Unpacker_Methods},
    {Py_tp_iter, AnyUnpacker_iter},
    {Py_tp_iternext, (iternextfunc)Unpacker_iternext},
    {0, NULL}};
END_NO_PEDANTIC

static PyType_Spec Unpacker_spec = {
    .name = "amsgpack.Unpacker",
    .basicsize = sizeof(Unpacker),
    .flags = Py_TPFLAGS_DEFAULT,
    .slots = Unpacker_slots,
};
