#include <Python.h>
#include <datetime.h>

#include "deque.h"
#include "ext.h"
#include "macros.h"

#define A_STACK_SIZE 32
#include "packb.h"
#define VERSION "0.1.3"
#define MiB128 134217728

static PyObject* unpackb(PyObject* restrict _module, PyObject* restrict args,
                         PyObject* restrict kwargs);

PyDoc_STRVAR(amsgpack_packb_doc,
             "packb($module, obj, /)\n--\n\n"
             "Serialize ``obj`` to a MessagePack formatted ``bytes``.");
PyDoc_STRVAR(amsgpack_unpackb_doc,
             "unpackb($module, data, /, *, tuple: bool = True)\n--\n\n"
             "Deserialize ``data`` (a ``bytes`` object) to a Python object.");

static PyMethodDef AMsgPackMethods[] = {
    {"packb", (PyCFunction)packb, METH_O, amsgpack_packb_doc},
    {"unpackb", (PyCFunction)(PyCFunction)(void (*)(void))unpackb,
     METH_VARARGS | METH_KEYWORDS, amsgpack_unpackb_doc},
    {NULL, NULL, 0, NULL}  // Sentinel
};

#ifndef PYPY_VERSION
#define ANEW_DICT _PyDict_NewPresized
#else
#define ANEW_DICT(N) PyDict_New()
#endif

static struct PyModuleDef amsgpack_module = {.m_base = PyModuleDef_HEAD_INIT,
                                             .m_name = "amsgpack",
                                             .m_doc = NULL,
                                             .m_size = -1,
                                             .m_methods = AMsgPackMethods};

static PyObject* msgpack_byte_object[256];
#define EMPTY_TUPLE_IDX 0xc4
#define EMPTY_STRING_IDX 0xa0

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

static inline int can_not_append_stack(Parser* parser) {
  return parser->stack_length >= A_STACK_SIZE;
}

typedef struct {
  PyObject_HEAD
  Deque deque;
  Parser parser;
  int use_tuple;
} Unpacker;

typedef struct {
  Unpacker unpacker;
  PyObject* read_callback;
  PyObject* read_size;
} FileUnpacker;

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

PyDoc_STRVAR(unpacker_feed_doc,
             "feed($self, bytes, /)\n--\n\n"
             "Append ``bytes`` to internal buffer.");

static PyMethodDef Unpacker_Methods[] = {
    {"feed", (PyCFunction)&unpacker_feed, METH_O, unpacker_feed_doc},
    {NULL, NULL, 0, NULL}  // Sentinel
};

static PyObject* Unpacker_new(PyTypeObject* type, PyObject* args,
                              PyObject* kwargs) {
  static char* keywords[] = {"tuple", NULL};
  int use_tuple = 0;
  if (!PyArg_ParseTupleAndKeywords(args, kwargs, "|$p:Unpacker", keywords,
                                   &use_tuple)) {
    return NULL;
  }

  Unpacker* self = (Unpacker*)type->tp_alloc(type, 0);

  if (self != NULL) {
    // init deque
    memset(&self->deque, 0, sizeof(Deque));
    // init parser
    memset(&self->parser, 0, sizeof(Parser));
    self->use_tuple = use_tuple;
  }
  return (PyObject*)self;
}

static PyObject* FileUnpacker_new(PyTypeObject* type, PyObject* args,
                                  PyObject* kwargs) {
  PyObject* file = NULL;
  PyObject* read_size = NULL;
  if (!PyArg_ParseTuple(args, "O|O:FileUnpacker", &file, &read_size)) {
    return NULL;
  }

  PyObject* read_callback = PyObject_GetAttrString(file, "read");
  if A_UNLIKELY(read_callback == NULL) {
    return NULL;
  }

  if A_UNLIKELY(Py_TYPE(read_callback)->tp_call == NULL) {
    Py_DECREF(read_callback);
    PyObject* errorMessage = PyUnicode_FromFormat("`%s.read` must be callable",
                                                  Py_TYPE(file)->tp_name);
    PyErr_SetObject(PyExc_TypeError, errorMessage);
    Py_XDECREF(errorMessage);
    return NULL;
  }

  PyObject* no_args = msgpack_byte_object[EMPTY_TUPLE_IDX];
  FileUnpacker* self = (FileUnpacker*)Unpacker_new(type, no_args, kwargs);
  if A_UNLIKELY(self == NULL) {
    return NULL;
  }
  self->read_callback = read_callback;
  Py_XINCREF(read_size);
  self->read_size = read_size;
  return (PyObject*)self;
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

static void FileUnpacker_dealloc(FileUnpacker* self) {
  Py_XDECREF(self->read_callback);
  Py_XDECREF(self->read_size);
  Unpacker_dealloc(&self->unpacker);
}

static PyObject* AnyUnpacker_iter(PyObject* self) {
  Py_INCREF(self);
  return self;
}

typedef union A_WORD {
  int16_t s;
  uint16_t us;
  char bytes[2];
} A_WORD;
typedef char check_word_size[sizeof(A_WORD) == 2 ? 1 : -1];

typedef union A_DWORD {
  int32_t l;
  uint32_t ul;
  float f;
  char bytes[4];
} A_DWORD;

typedef char check_dword_size[sizeof(A_DWORD) == 4 ? 1 : -1];

typedef union A_QWORD {
  int64_t ll;
  uint64_t ull;
  double d;
  char bytes[8];
} A_QWORD;

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

#define READ_A_WORD          \
  A_WORD word;               \
  do {                       \
    READ_A_DATA(2);          \
    word.bytes[0] = data[1]; \
    word.bytes[1] = data[0]; \
    FREE_A_DATA(2);          \
  } while (0)

#define READ_A_DWORD          \
  A_DWORD dword;              \
  do {                        \
    READ_A_DATA(4);           \
    dword.bytes[0] = data[3]; \
    dword.bytes[1] = data[2]; \
    dword.bytes[2] = data[1]; \
    dword.bytes[3] = data[0]; \
    FREE_A_DATA(4);           \
  } while (0)

#define READ_A_QWORD          \
  A_QWORD qword;              \
  do {                        \
    READ_A_DATA(8);           \
    qword.bytes[0] = data[7]; \
    qword.bytes[1] = data[6]; \
    qword.bytes[2] = data[5]; \
    qword.bytes[3] = data[4]; \
    qword.bytes[4] = data[3]; \
    qword.bytes[5] = data[2]; \
    qword.bytes[6] = data[1]; \
    qword.bytes[7] = data[0]; \
    FREE_A_DATA(8);           \
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
    A_QWORD timestamp64;
    timestamp64.bytes[0] = data[7];
    timestamp64.bytes[1] = data[6];
    timestamp64.bytes[2] = data[5];
    timestamp64.bytes[3] = data[4];
    timestamp64.bytes[4] = data[3];
    timestamp64.bytes[5] = data[2];
    timestamp64.bytes[6] = data[1];
    timestamp64.bytes[7] = data[0];
    timestamp.seconds = timestamp64.ull & 0x3ffffffff;  // 34 bits
    timestamp.nanosec = timestamp64.ull >> 34;          // 30 bits
  } else if (data_length == 4) {
    A_DWORD timestamp32;
    timestamp32.bytes[0] = data[3];
    timestamp32.bytes[1] = data[2];
    timestamp32.bytes[2] = data[1];
    timestamp32.bytes[3] = data[0];
    timestamp.seconds = (int64_t)timestamp32.ul;
    timestamp.nanosec = 0;
  } else if (data_length == 12) {
    TIMESTAMP96 timestamp96;
    timestamp96.bytes[0] = data[11];
    timestamp96.bytes[1] = data[10];
    timestamp96.bytes[2] = data[9];
    timestamp96.bytes[3] = data[8];
    timestamp96.bytes[4] = data[7];
    timestamp96.bytes[5] = data[6];
    timestamp96.bytes[6] = data[5];
    timestamp96.bytes[7] = data[4];
    timestamp96.bytes[8] = data[3];
    timestamp96.bytes[9] = data[2];
    timestamp96.bytes[10] = data[1];
    timestamp96.bytes[11] = data[0];
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
    case '\x80': {
      parsed_object = ANEW_DICT(0);
      if A_UNLIKELY(parsed_object == NULL) {
        return NULL;
      }
      deque_advance_first_bytes(&self->deque, 1);
      break;
    }
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
    case '\xbf': {  // fixstr
      state.str_length = Py_CHARMASK(next_byte) & 0x1f;
      if A_LIKELY(deque_has_next_n_bytes(&self->deque, state.str_length + 1)) {
        deque_advance_first_bytes(&self->deque, 1);
        goto str_length;
      }
      return NULL;
    }
    str_length: {
      READ_A_DATA(state.str_length);
      parsed_object = PyUnicode_DecodeUTF8(data, state.str_length, NULL);
      FREE_A_DATA(state.str_length);
      if A_UNLIKELY(parsed_object == NULL) {
        return NULL;
      }
      break;
    }
    case '\xc1': {  // (never used)
      PyErr_SetString(PyExc_ValueError, "amsgpack: 0xc1 byte must not be used");
      return NULL;
    }
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
            Ext* ext = (Ext*)Ext_Type.tp_alloc(&Ext_Type, 0);
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
      }
      return NULL;
    }
    case '\xd0':    // int_8
    case '\xcc': {  // uint_8
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
    }
    case '\xd1':    // int_16
    case '\xcd': {  // uint_16
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
    }
    case '\xd2':    // int_32
    case '\xce': {  // uint_32
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
    }
    case '\xd3':    // int_64
    case '\xcf': {  // uint_64
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
    }
    case '\xca': {  // float (float_32)
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
    }
    case '\xcb': {  // double (float_64)
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
    }
    case '\xd4':  // fixext 1
    case '\xd5':  // fixext 2
    case '\xd6':  // fixext 4
    case '\xd7':  // fixext 8
    case '\xd8':  // fixext 16
    {
      state.ext_length = (Py_ssize_t)1 << (next_byte - '\xd4');
      if A_LIKELY(deque_has_next_n_bytes(&self->deque, 2 + state.ext_length)) {
        deque_advance_first_bytes(&self->deque, 1);
        READ_A_DATA(state.ext_length + 1);
        char const code = data[0];
        if (code == -1 && (state.ext_length == 8 || state.ext_length == 4)) {
          parsed_object = ext_to_timestamp(data + 1, state.ext_length);
          if (parsed_object == NULL) {
            PyMem_Free(allocated);
            return NULL;  // likely overflow error
          }
        } else {
          Ext* ext = (Ext*)Ext_Type.tp_alloc(&Ext_Type, 0);
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
      return NULL;
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
            parsed_object = msgpack_byte_object[EMPTY_STRING_IDX];
            Py_INCREF(parsed_object);
            break;
          }
          goto str_length;
        }
      }
      return NULL;
    }
    case '\xdc': {  // array 16
      if A_LIKELY(deque_has_next_n_bytes(&self->deque, 3)) {
        deque_advance_first_bytes(&self->deque, 1);
        READ_A_WORD;
        state.arr_length = word.us;
        goto arr_length;
      }
      return NULL;
    }
    case '\xdd': {  // array 32
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
    }
    case '\xde': {  // map 16
      if A_LIKELY(deque_has_next_n_bytes(&self->deque, 3)) {
        deque_advance_first_bytes(&self->deque, 1);
        READ_A_WORD;
        state.map_length = word.us;
        goto map_length;
      }
      return NULL;
    }
    case '\xdf': {  // map 32
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
    }
    default: {
      parsed_object = msgpack_byte_object[(unsigned char)next_byte];
      assert(parsed_object != NULL);
      deque_advance_first_bytes(&self->deque, 1);
      Py_INCREF(parsed_object);
    }
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

// returns: -1 - failure
//           0 - success
static int init_msgpack_byte_object() {
  int i = -32;
  for (; i != 128; ++i) {
    PyObject* number = PyLong_FromLong(i);
    if A_UNLIKELY(number == NULL) {
      return -1;
    }
    msgpack_byte_object[(unsigned char)i] = number;
  }
  // fixstr of length 0
  msgpack_byte_object[EMPTY_STRING_IDX] = PyUnicode_FromStringAndSize(NULL, 0);
  if A_UNLIKELY(msgpack_byte_object[EMPTY_STRING_IDX] == NULL) {
    return -1;
  }
  msgpack_byte_object[0xc0] = Py_None;
  Py_INCREF(Py_None);
  msgpack_byte_object[0xc2] = Py_False;
  Py_INCREF(Py_False);
  msgpack_byte_object[0xc3] = Py_True;
  Py_INCREF(Py_True);
  msgpack_byte_object[EMPTY_TUPLE_IDX] = PyTuple_New(0);  // amsgpack specific
  if A_UNLIKELY(msgpack_byte_object[EMPTY_TUPLE_IDX] == NULL) {
    return -1;
  }
  return 0;
}

static PyObject* FileUnpacker_iternext(FileUnpacker* self) {
  // 1. Try to unpack current data
  {
    PyObject* current = Unpacker_iternext(&self->unpacker);
    if A_UNLIKELY(PyErr_Occurred() != NULL) {
      return NULL;
    }
    if (current != NULL) {
      return current;
    }
  }

  // 2. Read some bytes
  PyObject* bytes = self->read_size ? PyObject_CallOneArg(self->read_callback,
                                                          self->read_size)
                                    : PyObject_CallNoArgs(self->read_callback);
  if A_UNLIKELY(bytes == NULL) {
    return NULL;
  }
  if A_UNLIKELY(PyBytes_CheckExact(bytes) == 0) {
    PyErr_Format(PyExc_TypeError, "a bytes object is required, not '%.100s'",
                 Py_TYPE(bytes)->tp_name);
    return NULL;
  }
  // 3. Push bytes to the deque
  int const append_result = deque_append(&self->unpacker.deque, bytes);
  Py_DECREF(bytes);
  if A_UNLIKELY(append_result != 0) {
    return NULL;
  }

  // 4. Try to iterate
  return Unpacker_iternext(&self->unpacker);
}

static PyTypeObject Unpacker_Type = {
    PyVarObject_HEAD_INIT(NULL, 0).tp_name = "amsgpack.Unpacker",
    .tp_basicsize = sizeof(Unpacker),
    .tp_doc = PyDoc_STR("Unpack bytes to python objects"),
    .tp_new = Unpacker_new,
    .tp_dealloc = (destructor)Unpacker_dealloc,
    .tp_flags = Py_TPFLAGS_DEFAULT,
    .tp_methods = Unpacker_Methods,
    .tp_iter = AnyUnpacker_iter,
    .tp_iternext = (iternextfunc)Unpacker_iternext,
};

static PyTypeObject FileUnpacker_Type = {
    PyVarObject_HEAD_INIT(NULL, 0).tp_name = "amsgpack.FileUnpacker",
    .tp_basicsize = sizeof(FileUnpacker),
    .tp_doc = PyDoc_STR("Iteratively unpack binary stream to python objects"),
    .tp_new = FileUnpacker_new,
    .tp_dealloc = (destructor)FileUnpacker_dealloc,
    .tp_flags = Py_TPFLAGS_DEFAULT,
    .tp_iter = AnyUnpacker_iter,
    .tp_iternext = (iternextfunc)FileUnpacker_iternext,
};

PyMODINIT_FUNC PyInit_amsgpack(void) {
  PyObject* module = PyModule_Create(&amsgpack_module);
  if (module == NULL) {
    return NULL;
  }
  if (PyModule_AddStringConstant(module, "__version__", VERSION) != 0) {
    goto error;
  }
  if (init_msgpack_byte_object() != 0) {
    goto error;
  }
  if (PyType_Ready(&Ext_Type) < 0) {
    goto error;
  }
  if (PyModule_AddType(module, &Ext_Type) < 0) {
    goto error;
  }
  if (PyType_Ready(&Raw_Type) < 0) {
    goto error;
  }
  if (PyModule_AddType(module, &Raw_Type) < 0) {
    goto error;
  }
  if (PyType_Ready(&Unpacker_Type) < 0) {
    goto error;
  }
  if (PyModule_AddType(module, &Unpacker_Type) < 0) {
    goto error;
  }
  if (PyType_Ready(&FileUnpacker_Type) < 0) {
    goto error;
  }
  if (PyModule_AddType(module, &FileUnpacker_Type) < 0) {
    goto error;
  }
  PyDateTime_IMPORT;

  return module;
error:
  Py_DECREF(module);
  return NULL;
}

static PyObject* unpackb(PyObject* restrict Py_UNUSED(module),
                         PyObject* restrict args, PyObject* restrict kwargs) {
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
  PyObject* no_args = msgpack_byte_object[EMPTY_TUPLE_IDX];
  Unpacker* unpacker = (Unpacker*)Unpacker_new(&Unpacker_Type, no_args, kwargs);
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

#ifdef AMSGPACK_FUZZER
static PyObject* python_initialization() {
  Py_InitializeEx(0);
  return PyInit_amsgpack();
}

// fuzzer, forgive me
int LLVMFuzzerTestOneInput(uint8_t const* data, size_t size) {
  PyObject* no_args = PyTuple_New(0);
  PyObject* no_kwargs = PyDict_New();
  Unpacker* unpacker =
      (Unpacker*)Unpacker_new(&Unpacker_Type, no_args, no_kwargs);
  Py_DECREF(no_args);
  Py_DECREF(no_kwargs);
  assert(unpacker != NULL);
  {
    for (size_t data_idx = 0; data_idx < size; data_idx += 3) {
      PyObject* bytes = PyBytes_FromStringAndSize((char const*)data + data_idx,
                                                  Py_MIN(2, size - data_idx));
      assert(bytes != NULL);
      PyObject* feed_res = unpacker_feed(unpacker, bytes);
      Py_DECREF(bytes);
      if (feed_res == NULL) {
        Py_DECREF(unpacker);
        return -1;  // failed to feed, don't add to the corpus.
      } else {
        Py_DECREF(feed_res);
      }
    }
  }
  for (;;) {
    PyObject* unpacked_object = Unpacker_iternext(unpacker);
    if (unpacked_object == NULL) {
      break;
    } else {
      Py_DECREF(unpacked_object);
    }
  };
  Py_DECREF(unpacker);
  return 0;
}

int LLVMFuzzerInitialize(int* Py_UNUSED(argc), char*** Py_UNUSED(argv)) {
  python_initialization();
  return 0;
}
#endif  // AMSGPACK_FUZZER

#ifdef AMSGPACK_FUZZER_MAIN
#include <stdio.h>

int main(int argc, char** argv) {
  if (argc != 2) {
    fprintf(stderr,
            "convenient way to run amsgpack on files, generated by fuzzer\n\n"
            "usage: %s [FILENAME]\n",
            argc ? argv[0] : "?");
    return 1;
  }
  char const* path = argv[1];
  PyObject* module = python_initialization();
  FILE* infile = fopen(path, "rb");
  if (!infile) {
    fprintf(stderr, "failed to open file '%s'", path);
    return 1;
  }
  fseek(infile, 0L, SEEK_END);
  long const size = ftell(infile);
  if (size < 0) {
    return 1;
  }
  fseek(infile, 0L, SEEK_SET);
  uint8_t* const buffer = (uint8_t*)calloc(size, sizeof(uint8_t));
  if (!buffer) {
    fprintf(stderr, "can't allocate %ld bytes\n", size);
    return 1;
  }
  size_t const read_ok = fread(buffer, sizeof(uint8_t), size, infile);
  if (read_ok != size) {
    fprintf(stderr, "failed to read file (%zu read %ld expected)\n", read_ok,
            size);
    free(buffer);
    return 1;
  }
  fclose(infile);
  int const ret = LLVMFuzzerTestOneInput(buffer, (size_t)size);
  free(buffer);

  if (ret != 0) {
    fprintf(stderr, "LLVMFuzzerTestOneInput returned %d\n", ret);
  }
  for (int i = 0;
       i < sizeof(msgpack_byte_object) / sizeof(*msgpack_byte_object); ++i) {
    Py_XDECREF(msgpack_byte_object + i);
  }

  int const finalize_ret = Py_FinalizeEx();
  if (finalize_ret != 0) {
    fprintf(stderr, "Py_FinalizeEx returned %d\n", ret);
  }
  return ret;
}
#endif  // AMSGPACK_FUZZER_MAIN