#include <Python.h>
#include <datetime.h>

#include "deque.h"
#include "ext.h"

#define VERSION "0.0.2"

static inline void put2(char* dst, char header, char value) {
  dst[0] = header;
  dst[1] = value;
}

static inline void put3(char* dst, char header, unsigned short value) {
  dst[0] = header;
  dst[1] = ((char*)&value)[1];
  dst[2] = ((char*)&value)[0];
}

static inline void put5(char* dst, char header, unsigned int value) {
  dst[0] = header;
  dst[1] = ((char*)&value)[3];
  dst[2] = ((char*)&value)[2];
  dst[3] = ((char*)&value)[1];
  dst[4] = ((char*)&value)[0];
}

static inline void put9(char* dst, char header, unsigned long long value) {
  dst[0] = header;
  dst[1] = ((char*)&value)[7];
  dst[2] = ((char*)&value)[6];
  dst[3] = ((char*)&value)[5];
  dst[4] = ((char*)&value)[4];
  dst[5] = ((char*)&value)[3];
  dst[6] = ((char*)&value)[2];
  dst[7] = ((char*)&value)[1];
  dst[8] = ((char*)&value)[0];
}

static inline void put9_dbl(char* dst, char header, double value) {
  dst[0] = header;
  dst[1] = ((char*)&value)[7];
  dst[2] = ((char*)&value)[6];
  dst[3] = ((char*)&value)[5];
  dst[4] = ((char*)&value)[4];
  dst[5] = ((char*)&value)[3];
  dst[6] = ((char*)&value)[2];
  dst[7] = ((char*)&value)[1];
  dst[8] = ((char*)&value)[0];
}

#define AMSGPACK_RESIZE(n)                                \
  do {                                                    \
    if (PyByteArray_Resize(byte_array, pos + (n)) != 0) { \
      return -1;                                          \
    }                                                     \
    data = PyByteArray_AS_STRING(byte_array) + pos;       \
  } while (0)

// returns: -1 - failure
//           0 - success
static int packb_(PyObject* obj, PyObject* byte_array, int level) {
  Py_ssize_t pos = PyByteArray_GET_SIZE(byte_array);
  char* data;
  if (level > 16) {
    PyErr_SetString(PyExc_ValueError, "Object is too deep");
    return -1;
  }
  if (PyBool_Check(obj)) {
    AMSGPACK_RESIZE(1);
    if (obj == Py_True) {
      data[0] = '\xc3';
    } else {
      data[0] = '\xc2';
    }
  } else if (PyFloat_CheckExact(obj)) {
    // https://docs.python.org/3/c-api/float.html
    AMSGPACK_RESIZE(9);
    double const value = PyFloat_AS_DOUBLE(obj);
    put9_dbl(data, '\xcb', value);
  } else if (PyLong_CheckExact(obj)) {
    // https://docs.python.org/3/c-api/long.html
    long const value = PyLong_AsLong(obj);
    if (-0x20 <= value && value < 0x80) {
      AMSGPACK_RESIZE(1);
      data[0] = (char)value;
    } else if (0x80 <= value && value <= UCHAR_MAX) {
      AMSGPACK_RESIZE(2);
      put2(data, '\xcc', (char)value);
    } else if (SCHAR_MIN <= value && value < 0) {
      AMSGPACK_RESIZE(2);
      put2(data, '\xd0', (char)value);
    } else if (UCHAR_MAX < value && value <= 0xffff) {
      AMSGPACK_RESIZE(3);
      put3(data, '\xcd', value);
    } else if (SHRT_MIN <= value && value < -0x80) {
      AMSGPACK_RESIZE(3);
      put3(data, '\xd1', (unsigned short)value);
    } else if (0xffff < value && value <= 0xffffffff) {
      AMSGPACK_RESIZE(5);

      put5(data, '\xce', value);
    } else if (-0x7fffffff <= value && value < -0x8000) {
      AMSGPACK_RESIZE(5);
      put5(data, '\xd2', (unsigned int)value);
    } else if (0xffffffff < value /*&& value <= 0xffffffffffffffff*/) {
      AMSGPACK_RESIZE(9);
      put9(data, '\xcf', value);
    } else if (/*-0x8000000000000000 <= value &&*/ value < -0x80000000LL) {
      AMSGPACK_RESIZE(9);
      put9(data, '\xd3', value);
    } else {
      PyErr_SetString(PyExc_ValueError,
                      "Integral value is out of MessagePack range");
      return -1;
    }
  } else if (obj == Py_None) {
    AMSGPACK_RESIZE(1);
    data[0] = '\xc0';
  } else if (PyUnicode_CheckExact(obj)) {
    // https://docs.python.org/3.11/c-api/unicode.html
    Py_ssize_t u8size = 0;
    const char* u8string = PyUnicode_AsUTF8AndSize(obj, &u8size);
    if (u8size <= 0xf) {
      AMSGPACK_RESIZE(1 + u8size);
      data[0] = 0xa0 + u8size;
      memcpy(data + 1, u8string, u8size);
      return 0;
    }
    if (u8size <= 0xff) {
      AMSGPACK_RESIZE(2 + u8size);
      put2(data, '\xd9', (char)u8size);
      memcpy(data + 2, u8string, u8size);
      return 0;
    }
    if (u8size <= 0xffff) {
      AMSGPACK_RESIZE(3 + u8size);
      put3(data, '\xda', (unsigned short)u8size);
      memcpy(data + 3, u8string, u8size);
      return 0;
    }
    if (u8size <= 0xffffffff) {
      AMSGPACK_RESIZE(5 + u8size);
      put5(data, '\xdb', u8size);
      memcpy(data + 5, u8string, u8size);
      return 0;
    }
    PyErr_SetString(PyExc_ValueError,
                    "String length is out of MessagePack range");
    return -1;
  } else if (PyList_CheckExact(obj)) {
    // https://docs.python.org/3.11/c-api/list.html
    Py_ssize_t const list_size = PyList_Size(obj);
    if (list_size <= 15) {
      AMSGPACK_RESIZE(1);
      data[0] = '\x90' + (char)list_size;
    } else if (list_size <= 0xffff) {
      AMSGPACK_RESIZE(3);
      put3(data, '\xdc', (unsigned short)list_size);
    } else if (list_size <= 0xffffffff) {
      AMSGPACK_RESIZE(5);
      put5(data, '\xdd', (unsigned int)list_size);
    } else {
      PyErr_SetString(PyExc_ValueError,
                      "List length is out of MessagePack range");
      return -1;
    }
    for (Py_ssize_t i = 0; i < list_size; i++) {
      PyObject* item = PyList_GET_ITEM(obj, i);
      if (packb_(item, byte_array, level + 1) != 0) {
        return -1;
      }
    }
  } else if (PyDict_CheckExact(obj)) {
    // https://docs.python.org/3.11/c-api/dict.html
    Py_ssize_t const dict_size = PyDict_Size(obj);

    if (dict_size <= 15) {
      AMSGPACK_RESIZE(1);
      data[0] = 0x80 + dict_size;
    } else if (dict_size <= 0xffff) {
      AMSGPACK_RESIZE(3);
      put3(data, '\xde', (unsigned short)dict_size);
    } else if (dict_size <= 0xffffffff) {
      AMSGPACK_RESIZE(5);
      put5(data, '\xdf', (unsigned int)dict_size);
    } else {
      PyErr_SetString(PyExc_ValueError,
                      "Dict length is out of MessagePack range");
      return -1;
    }
    PyObject *key, *value;
    while (PyDict_Next(obj, &pos, &key, &value)) {
      if (packb_(key, byte_array, level + 1) != 0) {
        return -1;
      }
      if (packb_(value, byte_array, level + 1) != 0) {
        return -1;
      }
    }
  } else if (PyBytes_CheckExact(obj)) {
    // https://docs.python.org/3.11/c-api/bytes.html
    char* buffer;
    Py_ssize_t bytes_size;
    if (PyBytes_AsStringAndSize(obj, &buffer, &bytes_size) < 0) {
      return -1;
    }
    if (bytes_size <= 0xff) {
      AMSGPACK_RESIZE(2 + bytes_size);
      put2(data, '\xc4', bytes_size);
      pos = 2;
    } else if (bytes_size <= 0xffff) {
      AMSGPACK_RESIZE(3 + bytes_size);
      put3(data, '\xc5', bytes_size);
      pos = 3;
    } else if (bytes_size <= 0xffffffff) {
      AMSGPACK_RESIZE(5 + bytes_size);
      put5(data, '\xc6', (unsigned int)bytes_size);
      pos = 5;
    } else {
      PyErr_SetString(PyExc_ValueError,
                      "Bytes length is out of MessagePack range");
      return -1;
    }
    memcpy(data + pos, buffer, bytes_size);
  } else if (Py_IS_TYPE(obj, &Ext_Type)) {
    Ext* ext = (Ext*)obj;
    Py_ssize_t const data_length = PyBytes_GET_SIZE(ext->data);
    char const* data_bytes = PyBytes_AS_STRING(ext->data);
    char header = '\0';
    switch (data_length) {
      case 1:
        header = '\xd4';
        goto non_default;
      case 2:
        header = '\xd5';
        goto non_default;
      case 4:
        header = '\xd6';
        goto non_default;
      case 8:
        header = '\xd7';
        goto non_default;
      case 16:
        header = '\xd8';
        goto non_default;
      default:
        if (data_length <= 0xff) {
          AMSGPACK_RESIZE(2 + 1 + data_length);
          put2(data, '\xc7', data_length);
          data[2] = ext->code;
          memcpy(data + 3, data_bytes, data_length);
        } else if (data_length <= 0xffff) {
          AMSGPACK_RESIZE(3 + 1 + data_length);
          put3(data, '\xc8', data_length);
          data[3] = ext->code;
          memcpy(data + 4, data_bytes, data_length);
        } else if (data_length <= 0xffffffff) {
          AMSGPACK_RESIZE(5 + 1 + data_length);
          put5(data, '\xc9', data_length);
          data[5] = ext->code;
          memcpy(data + 6, data_bytes, data_length);
        } else {
          PyErr_SetString(PyExc_TypeError, "Ext() length is too large");
          return -1;
        }
        break;
      non_default:
        AMSGPACK_RESIZE(2 + data_length);
        put2(data, header, ext->code);
        memcpy(data + 2, data_bytes, data_length);
    }
  } else {
    PyTypeObject const* cls = Py_TYPE(obj);
    PyObject* errorMessage = PyUnicode_FromFormat("unsupported type: %S", cls);
    PyErr_SetObject(PyExc_TypeError, errorMessage);
    Py_XDECREF(errorMessage);
    return -1;
  }
  return 0;
}

static PyObject* packb(PyObject*, PyObject* obj) {
  PyObject* byte_array = PyByteArray_FromStringAndSize(NULL, 0);
  if (byte_array == NULL) {
    return NULL;
  }

  if (packb_(obj, byte_array, 0) != 0) {
    Py_XDECREF(byte_array);
    return NULL;
  }
  return byte_array;
}

PyDoc_STRVAR(amsgpack_packb_doc,
             "packb($module, objq, /)\n--\n\n"
             "Serialize ``obj`` to a MessagePack formatted ``bytearray``.");

static PyMethodDef AMsgPackMethods[] = {
    {"packb", (PyCFunction)packb, METH_O, amsgpack_packb_doc},
    {NULL, NULL, 0, NULL}  // Sentinel
};

static struct PyModuleDef amsgpack_module = {.m_base = PyModuleDef_HEAD_INIT,
                                             .m_name = "amsgpack",
                                             .m_doc = NULL,
                                             .m_size = -1,
                                             .m_methods = AMsgPackMethods};

static PyObject* msgpack_byte_object[256];
static PyObject* epoch = NULL;

typedef struct {
  enum Action { LIST_APPEND, DICT_KEY, DICT_VALUE } action;
  PyObject* iterable;
  Py_ssize_t size;
  Py_ssize_t pos;
  PyObject* key;
} Stack;

typedef struct {
  Py_ssize_t await_bytes;  // number of bytes we are currently awaiting
  Py_ssize_t stack_length;
  Stack stack[32];
} Parser;

typedef struct {
  PyObject_HEAD;
  Deque deque;
  Parser parser;
} Unpacker;

static PyObject* unpacker_feed(Unpacker* self, PyObject* obj) {
  if (PyBytes_CheckExact(obj) == 0) {
    PyErr_Format(PyExc_TypeError, "a bytes object is required, not '%.100s'",
                 Py_TYPE(obj)->tp_name);
    return NULL;
  }
  if (deque_append(&self->deque, obj) == NULL) {
    return NULL;
  }
  Py_RETURN_NONE;
}

PyDoc_STRVAR(unpacker_feed_doc,
             "feed($self, bytes, /)\n--\n\n"
             "Append ``bytes`` to internal buffer.");

static PyMethodDef Unpacker_Methods[] = {
    {"feed", (PyCFunction)&unpacker_feed, METH_O, unpacker_feed_doc},
    {NULL},
};

PyObject* Unpacker_new(PyTypeObject* type, PyObject* args, PyObject* kwds) {
  // Check that no arguments were provided
  if (!PyArg_ParseTuple(args, ":Ext")) {
    return NULL;
  }

  if (kwds != NULL && PyDict_Size(kwds) > 0) {
    PyErr_SetString(PyExc_TypeError, "Ext() takes no keyword arguments");
    return NULL;
  }

  Unpacker* self = (Unpacker*)type->tp_alloc(type, 0);

  if (self != NULL) {
    // init deque
    memset(&self->deque, 0, sizeof(Deque));
    // init parser
    memset(&self->parser, 0, sizeof(Parser));
  }
  return (PyObject*)self;
}

static void Unpacker_dealloc(Unpacker* self) {
  deque_clean(&self->deque);
  while (self->parser.stack_length) {
    Py_ssize_t idx = self->parser.stack_length - 1;
    Py_DECREF(self->parser.stack[idx].iterable);
    Py_XDECREF(self->parser.stack[idx].key);
    memset(&self->parser.stack[idx], 0, sizeof(Stack));
    self->parser.stack_length = idx;
  }
  Py_TYPE(self)->tp_free((PyObject*)self);
}

static PyObject* Unpacker_iter(PyObject* self) {
  Py_INCREF(self);
  return self;
}

typedef union A_WORD {
  int16_t s;
  uint16_t us;
  char bytes[2];
} A_WORD;
typedef char _check_dword_size[sizeof(A_WORD) == 2 ? 1 : -1];

typedef union A_DWORD {
  int32_t l;
  uint32_t ul;
  char bytes[4];
} A_DWORD;

typedef char _check_dword_size[sizeof(A_DWORD) == 4 ? 1 : -1];

typedef union A_QWORD {
  int64_t ll;
  uint64_t ull;
  char bytes[8];
} A_QWORD;

typedef char _check_dword_size[sizeof(A_QWORD) == 8 ? 1 : -1];

typedef union A_TIMESTAMP_96 {
#pragma pack(4)
  struct {
    uint64_t seconds : 64;
    uint32_t nanosec : 32;
  };
  char bytes[12];
} A_TIMESTAMP_96;
typedef char _check_timestamp_96_size[sizeof(A_TIMESTAMP_96) == 12 ? 1 : -1];

static PyObject* ext_to_timestamp(char const* data, Py_ssize_t data_length) {
  // timestamp case
  if (epoch == NULL) {  // initialize epoch
    PyDateTime_IMPORT;
    PyObject* args = PyTuple_New(2);
    if (args == NULL) {
      return NULL;
    }
    PyTuple_SET_ITEM(args, 0, msgpack_byte_object[0]);
    PyTuple_SET_ITEM(args, 1, PyDateTime_TimeZone_UTC);
    epoch = PyDateTime_FromTimestamp(args);
    if (epoch == NULL) {
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
    assert(0);  // programmer error
  }
  PyObject* delta = PyDelta_FromDSU(days, seconds, microseconds);

  if (delta == NULL) {
    return NULL;
  }
  PyObject* datetime_obj = PyNumber_Add(epoch, delta);
  Py_DECREF(delta);
  return datetime_obj;
}

static PyObject* Unpacker_iternext(PyObject* arg0) {
  Unpacker* self = (Unpacker*)arg0;
parse_next:
  if (!deque_has_next_byte(&self->deque)) {
    return NULL;
  }
  char next_byte = deque_peek_byte(&self->deque);
  PyObject* parsed_object = msgpack_byte_object[(unsigned char)next_byte];
  if (parsed_object == NULL) {
    switch (next_byte) {
      case '\x80': {
        parsed_object = PyDict_New();
        if (parsed_object == NULL) {
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
        Py_ssize_t const length = next_byte & 0x0f;
        parsed_object = PyDict_New();
        deque_advance_first_bytes(&self->deque, 1);
        Stack const item = {.action = DICT_KEY,
                            .iterable = parsed_object,
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
        Py_ssize_t const length = next_byte & 0x0f;
        parsed_object = PyList_New(length);
        deque_advance_first_bytes(&self->deque, 1);
        if (length == 0) {
          break;
        }
        Stack const item = {.action = LIST_APPEND,
                            .iterable = parsed_object,
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
        Py_ssize_t const length = next_byte & 0x1f;
        if (deque_has_n_next_byte(&self->deque, length + 1)) {
          deque_advance_first_bytes(&self->deque, 1);
          char const* data = 0;
          char* allocated = deque_read_bytes(&data, &self->deque, length);
          if (data == NULL) {
            return NULL;
          }
          parsed_object = PyUnicode_FromStringAndSize(data, length);
          if (allocated) {
            PyMem_Free(allocated);
          } else {
            deque_advance_first_bytes(&self->deque, length);
          }
          if (parsed_object == NULL) {
            return NULL;
          }
          break;
        }
        return NULL;
      }
      case '\xc1': {  // (never used)
        PyErr_SetString(PyExc_ValueError,
                        "amsgpack: 0xc1 byte must not be used");
        return NULL;
      }
      case '\xc4':  // bin 8
      case '\xc5':  // bin 16
      case '\xc6':  // bin 32
      {
        unsigned char const size_size = 1 << (next_byte - '\xc4');
        if (deque_has_n_next_byte(&self->deque, 1 + size_size)) {
          Py_ssize_t length = deque_peek_size(&self->deque, size_size);
          if (deque_has_n_next_byte(&self->deque, length + 2)) {
            deque_advance_first_bytes(&self->deque, 1 + size_size);
            if (length == 0) {
              parsed_object = PyBytes_FromStringAndSize(NULL, length);
            } else {
              char const* data = 0;
              char* allocated = deque_read_bytes(&data, &self->deque, length);
              if (data == NULL) {
                return NULL;
              }
              parsed_object = PyBytes_FromStringAndSize(data, length);
              if (allocated) {
                PyMem_Free(allocated);
              } else {
                deque_advance_first_bytes(&self->deque, length);
              }
            }
            if (parsed_object == NULL) {
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
        if (deque_has_n_next_byte(&self->deque, 2 + size_size)) {
          Py_ssize_t data_length = deque_peek_size(&self->deque, size_size);
          if (deque_has_n_next_byte(&self->deque, data_length + 3)) {
            deque_advance_first_bytes(&self->deque, 1 + size_size);
            char const* data = 0;
            char* allocated =
                deque_read_bytes(&data, &self->deque, data_length + 1);
            if (data == NULL) {
              return NULL;
            }
            char const code = data[0];
            if (code == -1 &&
                (data_length == 8 || data_length == 4 || data_length == 12)) {
              parsed_object = ext_to_timestamp(data + 1, data_length);
              break;
            }
            Ext* ext = (Ext*)Ext_Type.tp_alloc(&Ext_Type, 0);
            if (ext == NULL) {
              return NULL;  // Allocation failed
            }

            ext->code = code;
            ext->data = PyBytes_FromStringAndSize(data + 1, data_length);
            if (allocated) {
              PyMem_Free(allocated);
            } else {
              deque_advance_first_bytes(&self->deque, data_length + 1);
            }
            if (ext->data == NULL) {
              return NULL;
            }
            parsed_object = (PyObject*)ext;
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
          parsed_object = next_byte == '\xcc'
                              ? PyLong_FromLong((long)(unsigned char)byte)
                              : PyLong_FromLong((long)byte);
          if (parsed_object == NULL) {
            return NULL;
          }
          deque_advance_first_bytes(&self->deque, 1);
          break;
        }
        return NULL;
      }
      case '\xd1':    // int_16
      case '\xcd': {  // uint_16
        if (deque_has_n_next_byte(&self->deque, 3)) {
          deque_advance_first_bytes(&self->deque, 1);
          char const* data = 0;
          char* allocated = deque_read_bytes(&data, &self->deque, 2);
          if (data == NULL) {
            return NULL;
          }
          int16_t word;
          ((char*)&word)[0] = data[1];
          ((char*)&word)[1] = data[0];

          parsed_object = next_byte == '\xcd'
                              ? PyLong_FromLong((long)(uint16_t)word)
                              : PyLong_FromLong((long)word);
          if (parsed_object == NULL) {
            return NULL;
          }
          if (allocated) {
            PyMem_Free(allocated);
          } else {
            deque_advance_first_bytes(&self->deque, 2);
          }
          break;
        }
        return NULL;
      }
      case '\xd2':    // int_32
      case '\xce': {  // uint_32
        if (deque_has_n_next_byte(&self->deque, 5)) {
          deque_advance_first_bytes(&self->deque, 1);
          char const* data = 0;
          char* allocated = deque_read_bytes(&data, &self->deque, 4);
          if (data == NULL) {
            return NULL;
          }
          A_DWORD dword;
          dword.bytes[0] = data[3];
          dword.bytes[1] = data[2];
          dword.bytes[2] = data[1];
          dword.bytes[3] = data[0];

          parsed_object = next_byte == '\xce'
                              ? PyLong_FromUnsignedLong(dword.ul)
                              : PyLong_FromLong(dword.l);
          if (parsed_object == NULL) {
            return NULL;
          }
          if (allocated) {
            PyMem_Free(allocated);
          } else {
            deque_advance_first_bytes(&self->deque, 4);
          }
          break;
        }
        return NULL;
      }
      case '\xd3':    // int_64
      case '\xcf': {  // uint_64
        if (deque_has_n_next_byte(&self->deque, 9)) {
          deque_advance_first_bytes(&self->deque, 1);
          char const* data = 0;
          char* allocated = deque_read_bytes(&data, &self->deque, 8);
          if (data == NULL) {
            return NULL;
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
          if (parsed_object == NULL) {
            return NULL;
          }
          if (allocated) {
            PyMem_Free(allocated);
          } else {
            deque_advance_first_bytes(&self->deque, 8);
          }
          break;
        }
        return NULL;
      }
      case '\xca': {  // float (float_32)

        if (deque_has_n_next_byte(&self->deque, 5)) {
          deque_advance_first_bytes(&self->deque, 1);
          char const* data = 0;
          char* allocated = deque_read_bytes(&data, &self->deque, 4);
          if (data == NULL) {
            return NULL;
          }
          float dword;
          ((char*)&dword)[0] = data[3];
          ((char*)&dword)[1] = data[2];
          ((char*)&dword)[2] = data[1];
          ((char*)&dword)[3] = data[0];

          parsed_object = PyFloat_FromDouble((double)dword);
          if (parsed_object == NULL) {
            return NULL;
          }
          if (allocated) {
            PyMem_Free(allocated);
          } else {
            deque_advance_first_bytes(&self->deque, 4);
          }
          break;
        }
        return NULL;
      }
      case '\xcb': {  // double (float_64)
        if (deque_has_n_next_byte(&self->deque, 9)) {
          deque_advance_first_bytes(&self->deque, 1);
          char const* data = 0;
          char* allocated = deque_read_bytes(&data, &self->deque, 8);
          if (data == NULL) {
            return NULL;
          }
          double value;
          char* value_bytes = (char*)&value;
          value_bytes[0] = data[7];
          value_bytes[1] = data[6];
          value_bytes[2] = data[5];
          value_bytes[3] = data[4];
          value_bytes[4] = data[3];
          value_bytes[5] = data[2];
          value_bytes[6] = data[1];
          value_bytes[7] = data[0];
          if (allocated) {
            PyMem_Free(allocated);
          } else {
            deque_advance_first_bytes(&self->deque, 8);
          }
          parsed_object = PyFloat_FromDouble(value);
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
        if (deque_has_n_next_byte(&self->deque, 2 + data_length)) {
          deque_advance_first_bytes(&self->deque, 1);
          char const* data = 0;
          char* allocated =
              deque_read_bytes(&data, &self->deque, data_length + 1);
          if (data == NULL) {
            return NULL;
          }
          char const code = data[0];
          if (code == -1 && (data_length == 8 || data_length == 4)) {
            parsed_object = ext_to_timestamp(data + 1, data_length);
            break;
          }

          Ext* ext = (Ext*)Ext_Type.tp_alloc(&Ext_Type, 0);
          if (ext == NULL) {
            return NULL;  // Allocation failed
          }
          ext->code = code;
          ext->data = PyBytes_FromStringAndSize(data + 1, data_length);
          if (allocated) {
            PyMem_Free(allocated);
          } else {
            deque_advance_first_bytes(&self->deque, data_length + 1);
          }
          parsed_object = (PyObject*)ext;
          break;
        }
        return NULL;
      }
      case '\xd9':  // str 8
      case '\xda':  // str 16
      case '\xdb':  // str 32
      {
        unsigned char const size_size = 1 << (next_byte - '\xd9');
        if (deque_has_n_next_byte(&self->deque, 1 + size_size)) {
          Py_ssize_t length = deque_peek_size(&self->deque, size_size);
          if (deque_has_n_next_byte(&self->deque, length + 2)) {
            deque_advance_first_bytes(&self->deque, 1 + size_size);
            if (length == 0) {
              parsed_object = PyUnicode_FromStringAndSize(NULL, length);
            } else {
              char const* data = 0;
              char* allocated = deque_read_bytes(&data, &self->deque, length);
              if (data == NULL) {
                return NULL;
              }
              parsed_object = PyUnicode_FromStringAndSize(data, length);
              if (allocated) {
                PyMem_Free(allocated);
              } else {
                deque_advance_first_bytes(&self->deque, length);
              }
            }
            if (parsed_object == NULL) {
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
        if (next_byte == '\xdc') {
          if (deque_has_n_next_byte(&self->deque, 3)) {
            deque_advance_first_bytes(&self->deque, 1);
            char const* data = 0;
            char* allocated = deque_read_bytes(&data, &self->deque, 2);
            if (data == NULL) {
              return NULL;
            }
            A_WORD word;
            word.bytes[0] = data[1];
            word.bytes[1] = data[0];
            length = word.us;

            if (allocated) {
              PyMem_Free(allocated);
            } else {
              deque_advance_first_bytes(&self->deque, 2);
            }
          } else {
            return NULL;
          }
        } else {
          if (deque_has_n_next_byte(&self->deque, 5)) {
            deque_advance_first_bytes(&self->deque, 1);
            char const* data = 0;
            char* allocated = deque_read_bytes(&data, &self->deque, 4);
            if (data == NULL) {
              return NULL;
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
        parsed_object = PyList_New(length);
        if (length == 0) {
          break;
        }
        Stack const item = {.action = LIST_APPEND,
                            .iterable = parsed_object,
                            .size = length,
                            .pos = 0};
        self->parser.stack[self->parser.stack_length++] = item;
        goto parse_next;
      }
      case '\xde':    // map 16
      case '\xdf': {  // map 32
        Py_ssize_t length;
        if (next_byte == '\xde') {
          if (deque_has_n_next_byte(&self->deque, 3)) {
            deque_advance_first_bytes(&self->deque, 1);
            char const* data = 0;
            char* allocated = deque_read_bytes(&data, &self->deque, 2);
            if (data == NULL) {
              return NULL;
            }
            A_WORD word;
            word.bytes[0] = data[1];
            word.bytes[1] = data[0];
            length = word.us;

            if (allocated) {
              PyMem_Free(allocated);
            } else {
              deque_advance_first_bytes(&self->deque, 2);
            }
          } else {
            return NULL;
          }
        } else {
          if (deque_has_n_next_byte(&self->deque, 5)) {
            deque_advance_first_bytes(&self->deque, 1);
            char const* data = 0;
            char* allocated = deque_read_bytes(&data, &self->deque, 4);
            if (data == NULL) {
              return NULL;
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
        parsed_object = PyDict_New();
        if (length == 0) {
          break;
        }
        Stack const item = {.action = DICT_KEY,
                            .iterable = parsed_object,
                            .size = length,
                            .pos = 0};
        self->parser.stack[self->parser.stack_length++] = item;
        goto parse_next;
      }
      default: {
        // temporary not implemented error
        PyObject* errorMessage = PyUnicode_FromFormat(
            "NOT IMPLEMENTED BYTE: %d", (unsigned char)next_byte);
        PyErr_SetObject(PyExc_TypeError, errorMessage);
        Py_XDECREF(errorMessage);
        return NULL;
      }
    }
  } else {
    deque_advance_first_bytes(&self->deque, 1);
    Py_INCREF(parsed_object);
  }
  if (self->parser.stack_length > 0) {
    Stack* item = &self->parser.stack[self->parser.stack_length - 1];
    switch (item->action) {
      case LIST_APPEND:
        if (item->pos < item->size) {
          PyList_SET_ITEM(item->iterable, item->pos++, parsed_object);
        }
        if (item->pos == item->size) {
          parsed_object = item->iterable;
          memset(item, 0, sizeof(Stack));
          self->parser.stack_length -= 1;
        } else {
          goto parse_next;
        }
        break;
      case DICT_KEY:
        item->action = DICT_VALUE;
        assert(item->key == NULL);
        item->key = parsed_object;
        goto parse_next;
      case DICT_VALUE: {
        if (item->pos < item->size) {
          if (PyDict_SetItem(item->iterable, item->key, parsed_object) != 0) {
            return NULL;
          }
          Py_DECREF(item->key);
          item->key = NULL;
          item->pos += 1;
        }
        if (item->pos == item->size) {
          parsed_object = item->iterable;
          memset(item, 0, sizeof(Stack));
          self->parser.stack_length -= 1;
        } else {
          item->action = DICT_KEY;
          item->key = NULL;
          goto parse_next;
        }
        break;
      }
      default:
        assert(0);
    }
  }
  return parsed_object;
}

// returns: -1 - failure
//           0 - success
int init_msgpack_byte_object() {
  int i = 0;
  for (; i != 128; ++i) {
    PyObject* number = PyLong_FromLong(i);
    if (number == NULL) {
      return -1;
    }
    msgpack_byte_object[i] = number;
  }
  for (; i != 128 + 16 + 16 + 32; ++i) {
    msgpack_byte_object[i] = NULL;  // it must already be null
  }
  msgpack_byte_object[i++] = Py_None;
  Py_INCREF(Py_None);

  msgpack_byte_object[i++] = NULL;

  msgpack_byte_object[i++] = Py_False;  // False
  Py_INCREF(Py_False);
  msgpack_byte_object[i++] = Py_True;  // True
  Py_INCREF(Py_True);

  for (; i != 0xe0; ++i) {
    msgpack_byte_object[i] = NULL;  // it must already be null
  }

  for (; i != 256; ++i) {
    PyObject* number = PyLong_FromLong((char)i);
    if (number == NULL) {
      return -1;
    }
    msgpack_byte_object[i] = number;
  }
  return 0;
}

static PyTypeObject Unpacker_Type = {
    PyVarObject_HEAD_INIT(NULL, 0).tp_name = "amsgpack.Unpacker",
    .tp_basicsize = sizeof(Unpacker),
    .tp_doc = PyDoc_STR("Unpack bytes to python objects"),
    .tp_new = Unpacker_new,
    .tp_dealloc = (destructor)Unpacker_dealloc,
    .tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE,
    .tp_methods = Unpacker_Methods,
    .tp_iter = Unpacker_iter,
    .tp_iternext = Unpacker_iternext,
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
  if (PyType_Ready(&Unpacker_Type) < 0) {
    goto error;
  }
  if (PyModule_AddType(module, &Unpacker_Type) < 0) {
    goto error;
  }
  return module;
error:
  Py_DECREF(module);
  return NULL;
}
