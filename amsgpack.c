#include <Python.h>
#include <datetime.h>

#include "deque.h"
#include "ext.h"
#include "macros.h"

#define A_STACK_SIZE 32
#include "packb.h"
#define VERSION "0.1.1"
#define MiB128 134217728

static PyObject* unpackb(PyObject* restrict _module, PyObject* restrict args,
                         PyObject* restrict kwargs);

PyDoc_STRVAR(amsgpack_packb_doc,
             "packb($module, obj, /)\n--\n\n"
             "Serialize ``obj`` to a MessagePack formatted ``bytearray``.");
PyDoc_STRVAR(amsgpack_unpackb_doc,
             "unpackb($module, data, /, *, tuple: bool = True)\n--\n\n"
             "Deserialize ``data`` (a ``bytes`` object) to a Python object.");

static PyMethodDef AMsgPackMethods[] = {
    {"packb", (PyCFunction)packb, METH_O, amsgpack_packb_doc},
    {"unpackb", (PyCFunction)(PyCFunction)(void (*)(void))unpackb,
     METH_VARARGS | METH_KEYWORDS, amsgpack_unpackb_doc},
    {NULL, NULL, 0, NULL}  // Sentinel
};

static struct PyModuleDef amsgpack_module = {.m_base = PyModuleDef_HEAD_INIT,
                                             .m_name = "amsgpack",
                                             .m_doc = NULL,
                                             .m_size = -1,
                                             .m_methods = AMsgPackMethods};

static PyObject* msgpack_byte_object[256];
#define EMPTY_TUPLE_IDX 0xc4
static PyObject* epoch = NULL;

typedef struct {
  enum UnpackAction { SEQUENCE_APPEND, DICT_KEY, DICT_VALUE } action;
  PyObject* sequence;
  Py_ssize_t size;
  Py_ssize_t pos;
  PyObject* key;
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
  PyObject_HEAD;
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
    return NULL;
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

PyObject* Unpacker_new(PyTypeObject* type, PyObject* args, PyObject* kwargs) {
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

PyObject* FileUnpacker_new(PyTypeObject* type, PyObject* args,
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
    Py_ssize_t idx = self->parser.stack_length - 1;
    Py_DECREF(self->parser.stack[idx].sequence);
    Py_XDECREF(self->parser.stack[idx].key);
    memset(&self->parser.stack[idx], 0, sizeof(Stack));
    self->parser.stack_length = idx;
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

typedef union A_TIMESTAMP_96 {
#pragma pack(4)
  struct {
    uint64_t seconds : 64;
    uint32_t nanosec : 32;
  };
  char bytes[12];
} A_TIMESTAMP_96;
typedef char _check_timestamp_96_size[sizeof(A_TIMESTAMP_96) == 12 ? 1 : -1];

static PyObject* size_error(char type[], Py_ssize_t length, Py_ssize_t limit) {
  PyObject* errorMessage =
      PyUnicode_FromFormat("%s size %z is too big (>%z)", type, length, limit);
  PyErr_SetObject(PyExc_ValueError, errorMessage);
  Py_XDECREF(errorMessage);
  return NULL;
}

static PyObject* ext_to_timestamp(char const* data, Py_ssize_t data_length) {
  // timestamp case
  if (epoch == NULL) {  // initialize epoch
    PyObject* args = PyTuple_New(2);
    if A_UNLIKELY(args == NULL) {
      return NULL;
    }
    PyTuple_SET_ITEM(args, 0, msgpack_byte_object[0]);
    PyTuple_SET_ITEM(args, 1, PyDateTime_TimeZone_UTC);
    epoch = PyDateTime_FromTimestamp(args);
    if A_UNLIKELY(epoch == NULL) {
      return NULL;
    }
  }
  int days = 0;
  int seconds = 0;
  int microseconds = 0;
  if (data_length == 8) {
    unsigned char const* udata = (unsigned char const*)data;
    uint32_t const msb =
        (udata[0] << 24) | (udata[1] << 16) | (udata[2] << 8) | udata[3];
    uint32_t const lsb =
        (udata[4] << 24) | (udata[5] << 16) | (udata[6] << 8) | udata[7];
    uint32_t const nanosec_30_bit = msb >> 2;
    uint64_t const seconds_34_bit =
        (((uint64_t)msb & 0x00000003) << 32) | (uint64_t)lsb;
    days = seconds_34_bit / (60 * 60 * 24);
    seconds = seconds_34_bit % (60 * 60 * 24);
    microseconds = nanosec_30_bit / 1000;
  } else if (data_length == 4) {
    A_DWORD timestamp32;
    timestamp32.bytes[0] = data[3];
    timestamp32.bytes[1] = data[2];
    timestamp32.bytes[2] = data[1];
    timestamp32.bytes[3] = data[0];
    days = timestamp32.ul / (60 * 60 * 24);
    seconds = timestamp32.ul % (60 * 60 * 24);
  } else if (data_length == 12) {
    A_TIMESTAMP_96 timestamp96;
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
    days = timestamp96.seconds / (60 * 60 * 24);
    seconds = timestamp96.seconds % (60 * 60 * 24);
    microseconds = timestamp96.nanosec / 1000;
  } else {
    Py_UNREACHABLE();  // GCOVR_EXCL_LINE
  }
  PyObject* delta = PyDelta_FromDSU(days, seconds, microseconds);

  if A_UNLIKELY(delta == NULL) {
    return NULL;
  }
  PyObject* datetime_obj = PyNumber_Add(epoch, delta);
  Py_XDECREF(delta);
  return datetime_obj;
}

static PyObject* Unpacker_iternext(Unpacker* self) {
parse_next:
  if (!deque_has_next_byte(&self->deque)) {
    return NULL;
  }
  char const next_byte = deque_peek_byte(&self->deque);
  PyObject* parsed_object = NULL;
  switch (next_byte) {
    case '\x80': {
      parsed_object = _PyDict_NewPresized(0);
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
    case '\x8f': {  // fixmap
      if A_UNLIKELY(can_not_append_stack(&self->parser)) {
        PyErr_SetString(PyExc_ValueError, "Deeply nested object");
        return NULL;
      }
      Py_ssize_t const length = Py_CHARMASK(next_byte) & 0x0f;
      parsed_object = _PyDict_NewPresized(length);
      if A_UNLIKELY(parsed_object == NULL) {
        return NULL;
      }
      deque_advance_first_bytes(&self->deque, 1);
      Stack const item = {.action = DICT_KEY,
                          .sequence = parsed_object,
                          .size = length,
                          .pos = 0};
      self->parser.stack[self->parser.stack_length++] = item;
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
      if A_UNLIKELY(can_not_append_stack(&self->parser)) {
        PyErr_SetString(PyExc_ValueError, "Deeply nested object");
        return NULL;
      }
      Py_ssize_t const length = Py_CHARMASK(next_byte) & 0x0f;
      parsed_object = (self->use_tuple == 0 ? PyList_New : PyTuple_New)(length);
      if A_UNLIKELY(parsed_object == NULL) {
        return NULL;
      }
      deque_advance_first_bytes(&self->deque, 1);
      if (length == 0) {
        break;
      }
      Stack const item = {.action = SEQUENCE_APPEND,
                          .sequence = parsed_object,
                          .size = length,
                          .pos = 0};
      self->parser.stack[self->parser.stack_length++] = item;
      goto parse_next;
    }

    case '\xa0': {
      parsed_object = PyUnicode_FromStringAndSize(NULL, 0);
      if (parsed_object == NULL) {
        return NULL;
      }
      deque_advance_first_bytes(&self->deque, 1);
      break;
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
      Py_ssize_t const length = Py_CHARMASK(next_byte) & 0x1f;
      if (deque_has_n_next_byte(&self->deque, length + 1)) {
        deque_advance_first_bytes(&self->deque, 1);
        char* data = deque_read_bytes_fast(&self->deque, length);
        char* allocated = NULL;
        if A_UNLIKELY(data == NULL) {
          data = allocated = deque_read_bytes(&self->deque, length);
          if A_UNLIKELY(allocated == NULL) {
            return NULL;
          }
        }
        parsed_object = PyUnicode_DecodeUTF8(data, length, NULL);
        if A_LIKELY(allocated == NULL) {
          deque_advance_first_bytes(&self->deque, length);
        } else {
          PyMem_Free(allocated);
        }
        if (parsed_object == NULL) {
          return NULL;
        }
        break;
      }
      return NULL;
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
      if (deque_has_n_next_byte(&self->deque, 1 + size_size)) {
        Py_ssize_t length = deque_peek_size(&self->deque, size_size);
        if (deque_has_n_next_byte(&self->deque, 1 + size_size + length)) {
          deque_skip_size(&self->deque, size_size);
          if (length == 0) {
            parsed_object = PyBytes_FromStringAndSize(NULL, length);
          } else {
            if A_UNLIKELY(length > MiB128) {
              return size_error("bytes", length, MiB128);
            }
            char* data = deque_read_bytes_fast(&self->deque, length);
            char* allocated = NULL;
            if A_UNLIKELY(data == NULL) {
              data = allocated = deque_read_bytes(&self->deque, length);
              if A_UNLIKELY(allocated == NULL) {
                return NULL;
              }
            }

            parsed_object = PyBytes_FromStringAndSize(data, length);
            if A_LIKELY(allocated == NULL) {
              deque_advance_first_bytes(&self->deque, length);
            } else {
              PyMem_Free(allocated);
            }
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
      if A_LIKELY(deque_has_n_next_byte(&self->deque, 1 + size_size + 1)) {
        Py_ssize_t const data_length = deque_peek_size(&self->deque, size_size);
        if A_UNLIKELY(data_length >= MiB128) {
          return NULL;
        }
        if A_LIKELY(deque_has_n_next_byte(&self->deque,
                                          1 + size_size + 1 + data_length)) {
          deque_skip_size(&self->deque, size_size);
          char* data = deque_read_bytes_fast(&self->deque, data_length + 1);
          char* allocated = NULL;
          if A_UNLIKELY(data == NULL) {
            data = allocated = deque_read_bytes(&self->deque, data_length + 1);
            if A_UNLIKELY(allocated == NULL) {
              return NULL;
            }
          }

          char const code = data[0];
          if (code == -1 &&
              (data_length == 8 || data_length == 4 || data_length == 12)) {
            parsed_object = ext_to_timestamp(data + 1, data_length);
            if (parsed_object == NULL) {
              return NULL;  // likely overflow error
            }
          } else {
            Ext* ext = (Ext*)Ext_Type.tp_alloc(&Ext_Type, 0);
            if (ext == NULL) {
              return NULL;  // Allocation failed
            }
            ext->code = code;
            ext->data = PyBytes_FromStringAndSize(data + 1, data_length);
            if (ext->data == NULL) {
              return NULL;
            }
            parsed_object = (PyObject*)ext;
          }
          if A_LIKELY(allocated == NULL) {
            deque_advance_first_bytes(&self->deque, data_length + 1);
          } else {
            PyMem_Free(allocated);
          }
          break;
        }
      }
      return NULL;
    }
    case '\xd0':    // int_8
    case '\xcc': {  // uint_8
      if (deque_has_n_next_byte(&self->deque, 2)) {
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
      if A_LIKELY(deque_has_n_next_byte(&self->deque, 3)) {
        deque_advance_first_bytes(&self->deque, 1);
        char* data = deque_read_bytes_fast(&self->deque, 2);
        char* allocated = NULL;
        if A_UNLIKELY(data == NULL) {
          data = allocated = deque_read_bytes(&self->deque, 2);
          if A_UNLIKELY(allocated == NULL) {
            return NULL;
          }
        }

        A_WORD word;
        word.bytes[0] = data[1];
        word.bytes[1] = data[0];

        parsed_object = next_byte == '\xcd' ? PyLong_FromLong((long)word.us)
                                            : PyLong_FromLong((long)word.s);
        if (parsed_object == NULL) {
          return NULL;
        }
        if A_LIKELY(allocated == NULL) {
          deque_advance_first_bytes(&self->deque, 2);
        } else {
          PyMem_Free(allocated);
        }
        break;
      }
      return NULL;
    }
    case '\xd2':    // int_32
    case '\xce': {  // uint_32
      if A_LIKELY(deque_has_n_next_byte(&self->deque, 5)) {
        deque_advance_first_bytes(&self->deque, 1);
        char* data = deque_read_bytes_fast(&self->deque, 4);
        char* allocated = NULL;
        if A_UNLIKELY(data == NULL) {
          data = allocated = deque_read_bytes(&self->deque, 4);
          if A_UNLIKELY(allocated == NULL) {
            return NULL;
          }
        }
        A_DWORD dword;
        dword.bytes[0] = data[3];
        dword.bytes[1] = data[2];
        dword.bytes[2] = data[1];
        dword.bytes[3] = data[0];

        parsed_object = next_byte == '\xce' ? PyLong_FromUnsignedLong(dword.ul)
                                            : PyLong_FromLong(dword.l);
        if A_UNLIKELY(parsed_object == NULL) {
          return NULL;
        }
        if A_LIKELY(allocated == NULL) {
          deque_advance_first_bytes(&self->deque, 4);
        } else {
          PyMem_Free(allocated);
        }
        break;
      }
      return NULL;
    }
    case '\xd3':    // int_64
    case '\xcf': {  // uint_64
      if A_LIKELY(deque_has_n_next_byte(&self->deque, 9)) {
        deque_advance_first_bytes(&self->deque, 1);
        char* data = deque_read_bytes_fast(&self->deque, 8);
        char* allocated = NULL;
        if A_UNLIKELY(data == NULL) {
          data = allocated = deque_read_bytes(&self->deque, 8);
          if A_UNLIKELY(allocated == NULL) {
            return NULL;
          }
        }
        A_QWORD qword;
        qword.bytes[0] = data[7];
        qword.bytes[1] = data[6];
        qword.bytes[2] = data[5];
        qword.bytes[3] = data[4];
        qword.bytes[4] = data[3];
        qword.bytes[5] = data[2];
        qword.bytes[6] = data[1];
        qword.bytes[7] = data[0];

        parsed_object = next_byte == '\xcf'
                            ? PyLong_FromUnsignedLongLong(qword.ull)
                            : PyLong_FromLongLong(qword.ll);
        if A_UNLIKELY(parsed_object == NULL) {
          return NULL;
        }
        if A_LIKELY(allocated == NULL) {
          deque_advance_first_bytes(&self->deque, 8);
        } else {
          PyMem_Free(allocated);
        }
        break;
      }
      return NULL;
    }
    case '\xca': {  // float (float_32)
      if A_LIKELY(deque_has_n_next_byte(&self->deque, 5)) {
        deque_advance_first_bytes(&self->deque, 1);
        char* data = deque_read_bytes_fast(&self->deque, 4);
        char* allocated = NULL;
        if A_UNLIKELY(data == NULL) {
          data = allocated = deque_read_bytes(&self->deque, 4);
          if A_UNLIKELY(allocated == NULL) {
            return NULL;
          }
        }
        A_DWORD dword;
        dword.bytes[0] = data[3];
        dword.bytes[1] = data[2];
        dword.bytes[2] = data[1];
        dword.bytes[3] = data[0];

        parsed_object = PyFloat_FromDouble((double)dword.f);
        if A_UNLIKELY(parsed_object == NULL) {
          return NULL;
        }
        if A_UNLIKELY(allocated == NULL) {
          deque_advance_first_bytes(&self->deque, 4);
        } else {
          PyMem_Free(allocated);
        }
        break;
      }
      return NULL;
    }
    case '\xcb': {  // double (float_64)
      if (deque_has_n_next_byte(&self->deque, 9)) {
        deque_advance_first_bytes(&self->deque, 1);
        char* data = deque_read_bytes_fast(&self->deque, 8);
        char* allocated = NULL;
        if A_UNLIKELY(data == NULL) {
          data = allocated = deque_read_bytes(&self->deque, 8);
          if A_UNLIKELY(allocated == NULL) {
            return NULL;
          }
        }
        A_QWORD qword;
        qword.bytes[0] = data[7];
        qword.bytes[1] = data[6];
        qword.bytes[2] = data[5];
        qword.bytes[3] = data[4];
        qword.bytes[4] = data[3];
        qword.bytes[5] = data[2];
        qword.bytes[6] = data[1];
        qword.bytes[7] = data[0];
        if A_LIKELY(allocated == NULL) {
          deque_advance_first_bytes(&self->deque, 8);
        } else {
          PyMem_Free(allocated);
        }
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
      Py_ssize_t const data_length = 1 << (next_byte - '\xd4');
      if A_LIKELY(deque_has_n_next_byte(&self->deque, 2 + data_length)) {
        deque_advance_first_bytes(&self->deque, 1);
        char* data = deque_read_bytes_fast(&self->deque, data_length + 1);
        char* allocated = NULL;
        if A_UNLIKELY(data == NULL) {
          data = allocated = deque_read_bytes(&self->deque, data_length + 1);
          if A_UNLIKELY(allocated == NULL) {
            return NULL;
          }
        }
        char const code = data[0];
        if (code == -1 && (data_length == 8 || data_length == 4)) {
          parsed_object = ext_to_timestamp(data + 1, data_length);
          if (parsed_object == NULL) {
            return NULL;  // likely overflow error
          }
        } else {
          Ext* ext = (Ext*)Ext_Type.tp_alloc(&Ext_Type, 0);
          if (ext == NULL) {
            return NULL;  // Allocation failed
          }
          ext->code = code;
          ext->data = PyBytes_FromStringAndSize(data + 1, data_length);
          parsed_object = (PyObject*)ext;
        }
        if A_LIKELY(allocated == NULL) {
          deque_advance_first_bytes(&self->deque, data_length + 1);
        } else {
          PyMem_Free(allocated);
        }
        break;
      }
      return NULL;
    }
    case '\xd9':  // str 8
    case '\xda':  // str 16
    case '\xdb':  // str 32
    {
      unsigned char const size_size = 1 << (next_byte - '\xd9');
      if A_LIKELY(deque_has_n_next_byte(&self->deque, 1 + size_size)) {
        Py_ssize_t const length = deque_peek_size(&self->deque, size_size);
        if A_LIKELY(deque_has_n_next_byte(&self->deque,
                                          1 + size_size + length)) {
          deque_skip_size(&self->deque, size_size);
          if (length == 0) {
            parsed_object = PyUnicode_FromStringAndSize(NULL, length);
          } else {
            if A_UNLIKELY(length > MiB128) {
              return size_error("string", length, MiB128);
            }

            char* data = deque_read_bytes_fast(&self->deque, length);
            char* allocated = NULL;
            if A_UNLIKELY(data == NULL) {
              data = allocated = deque_read_bytes(&self->deque, length);
              if A_UNLIKELY(allocated == NULL) {
                return NULL;
              }
            }
            parsed_object = PyUnicode_DecodeUTF8(data, length, NULL);
            if A_LIKELY(allocated == NULL) {
              deque_advance_first_bytes(&self->deque, length);
            } else {
              PyMem_Free(allocated);
            }
          }
          if A_UNLIKELY(parsed_object == NULL) {
            return NULL;
          }
          break;
        }
      }
      return NULL;
    }
    case '\xdc':    // array 16
    case '\xdd': {  // array 32
      Py_ssize_t length;
      if (next_byte == '\xdc') {  // array 16
        if A_LIKELY(deque_has_n_next_byte(&self->deque, 3)) {
          deque_advance_first_bytes(&self->deque, 1);
          char* data = deque_read_bytes_fast(&self->deque, 2);
          char* allocated = NULL;
          if A_UNLIKELY(data == NULL) {
            data = allocated = deque_read_bytes(&self->deque, 2);
            if A_UNLIKELY(allocated == NULL) {
              return NULL;
            }
          }
          A_WORD word;
          word.bytes[0] = data[1];
          word.bytes[1] = data[0];
          length = word.us;

          if A_LIKELY(allocated == NULL) {
            deque_advance_first_bytes(&self->deque, 2);
          } else {
            PyMem_Free(allocated);
          }
        } else {
          return NULL;
        }
      } else {  // array 32
        if A_LIKELY(deque_has_n_next_byte(&self->deque, 5)) {
          deque_advance_first_bytes(&self->deque, 1);
          char* data = deque_read_bytes_fast(&self->deque, 4);
          char* allocated = NULL;
          if A_UNLIKELY(data == NULL) {
            data = allocated = deque_read_bytes(&self->deque, 4);
            if A_UNLIKELY(allocated == NULL) {
              return NULL;
            }
          }
          A_DWORD word;
          word.bytes[0] = data[3];
          word.bytes[1] = data[2];
          word.bytes[2] = data[1];
          word.bytes[3] = data[0];
          length = word.ul;

          if A_LIKELY(allocated == NULL) {
            deque_advance_first_bytes(&self->deque, 4);
          } else {
            PyMem_Free(allocated);
          }
        } else {
          return NULL;
        }
      }
      if A_UNLIKELY(length > 10000000) {
        return size_error("list", length, 10000000);
      }
      if A_UNLIKELY(can_not_append_stack(&self->parser)) {
        PyErr_SetString(PyExc_ValueError, "Deeply nested object");
        return NULL;
      }
      parsed_object = (self->use_tuple == 0 ? PyList_New : PyTuple_New)(length);
      if A_UNLIKELY(parsed_object == NULL) {
        return NULL;
      }
      if (length == 0) {
        break;
      }
      Stack const item = {.action = SEQUENCE_APPEND,
                          .sequence = parsed_object,
                          .size = length,
                          .pos = 0};
      self->parser.stack[self->parser.stack_length++] = item;
      goto parse_next;
    }
    case '\xde':    // map 16
    case '\xdf': {  // map 32
      Py_ssize_t length;
      if A_LIKELY(next_byte == '\xde') {
        if A_LIKELY(deque_has_n_next_byte(&self->deque, 3)) {
          deque_advance_first_bytes(&self->deque, 1);
          char* data = deque_read_bytes_fast(&self->deque, 2);
          char* allocated = NULL;
          if A_UNLIKELY(data == NULL) {
            data = allocated = deque_read_bytes(&self->deque, 2);
            if A_UNLIKELY(allocated == NULL) {
              return NULL;
            }
          }
          A_WORD word;
          word.bytes[0] = data[1];
          word.bytes[1] = data[0];
          length = word.us;

          if A_LIKELY(allocated == NULL) {
            deque_advance_first_bytes(&self->deque, 2);
          } else {
            PyMem_Free(allocated);
          }
        } else {
          return NULL;
        }
      } else {
        if A_LIKELY(deque_has_n_next_byte(&self->deque, 5)) {
          deque_advance_first_bytes(&self->deque, 1);
          char* data = deque_read_bytes_fast(&self->deque, 4);
          char* allocated = NULL;
          if A_UNLIKELY(data == NULL) {
            data = allocated = deque_read_bytes(&self->deque, 4);
            if A_UNLIKELY(allocated == NULL) {
              return NULL;
            }
          }
          A_DWORD word;
          word.bytes[0] = data[3];
          word.bytes[1] = data[2];
          word.bytes[2] = data[1];
          word.bytes[3] = data[0];
          length = word.ul;

          if (allocated) {
            PyMem_Free(allocated);
          } else {
            deque_advance_first_bytes(&self->deque, 4);
          }
        } else {
          return NULL;
        }
      }
      if A_UNLIKELY(length > 100000) {
        return size_error("dict", length, 100000);
      }
      if A_UNLIKELY(can_not_append_stack(&self->parser)) {
        PyErr_SetString(PyExc_ValueError, "Deeply nested object");
        return NULL;
      }
      parsed_object = _PyDict_NewPresized(length);
      if (length == 0) {
        break;
      }
      Stack const item = {.action = DICT_KEY,
                          .sequence = parsed_object,
                          .size = length,
                          .pos = 0};
      self->parser.stack[self->parser.stack_length++] = item;
      goto parse_next;
    }
    default: {
      parsed_object = msgpack_byte_object[(unsigned char)next_byte];
      assert(parsed_object != NULL);
      deque_advance_first_bytes(&self->deque, 1);
      Py_INCREF(parsed_object);
    }
  }
  while (self->parser.stack_length > 0) {
    Stack* item = &self->parser.stack[self->parser.stack_length - 1];
    switch (item->action) {
      case SEQUENCE_APPEND:
        assert(item->pos < item->size);
        self->use_tuple == 0
            ? PyList_SET_ITEM(item->sequence, item->pos, parsed_object)
            : PyTuple_SET_ITEM(item->sequence, item->pos, parsed_object);
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
      default:
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
    if (number == NULL) {
      return -1;
    }
    msgpack_byte_object[(unsigned char)i] = number;
  }
  msgpack_byte_object[0xc0] = Py_None;
  Py_INCREF(Py_None);
  msgpack_byte_object[0xc2] = Py_False;
  Py_INCREF(Py_False);
  msgpack_byte_object[0xc3] = Py_True;
  Py_INCREF(Py_True);
  msgpack_byte_object[EMPTY_TUPLE_IDX] = PyTuple_New(0);  // amsgpack specific
  Py_INCREF(msgpack_byte_object[EMPTY_TUPLE_IDX]);
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
// fuzzer, forgive me
int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
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
        return -1;
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
  Py_InitializeEx(0);
  PyInit_amsgpack();
  return 0;
}
#endif  // AMSGPACK_FUZZER
