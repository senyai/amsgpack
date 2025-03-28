#include <Python.h>

#include "deque.h"

#define VERSION "0.0.1"

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
      data[0] = 0x90 + list_size;
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
  } else {
    PyTypeObject const* cls = Py_TYPE(obj);
    PyObject* typeStr = PyObject_Str((PyObject*)cls);

    const char* typeCString = PyUnicode_AsUTF8(typeStr);

    PyObject* errorMessage =
        PyUnicode_FromFormat("unsupported type: %s", typeCString);
    PyErr_SetObject(PyExc_TypeError, errorMessage);
    Py_XDECREF(typeStr);
    Py_XDECREF(errorMessage);
    return -1;
  }
  return 0;
}

static PyObject* packb(PyObject* module, PyObject* obj) {
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

typedef struct {
  PyObject* iterable;
  Py_ssize_t size;
  Py_ssize_t pos;
} Stack;

typedef struct {
  Py_ssize_t await_bytes;  // number of bytes we are currently awaiting
  Py_ssize_t stack_length;
  Stack stack[32];
  PyObject** actions;  // just a reference
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
  Unpacker* self = (Unpacker*)type->tp_alloc(type, 0);

  if (self != NULL) {
    PyObject* actions = PyObject_GetAttrString((PyObject*)type, "_actions");
    if (actions == NULL) {
      goto error;
    }

    // init deque
    memset(&self->deque, 0, sizeof(Deque));
    // init parser
    memset(&self->parser, 0, sizeof(Parser));
    self->parser.actions = &PyTuple_GET_ITEM(actions, 0);
  }
  return (PyObject*)self;
error:
  Py_DECREF(self);
  return NULL;
}

static PyObject* Unpacker_iter(PyObject* self) {
  Py_INCREF(self);
  return self;
}

static PyObject* Unpacker_iternext(PyObject* arg0) {
  Unpacker* self = (Unpacker*)arg0;
  // repeat:
  if (!deque_has_next_byte(&self->deque)) {
    return NULL;
  }
  if (self->parser.stack_length) {
    //...
  }
  char next_byte = deque_peek_byte(&self->deque);
  PyObject* action = self->parser.actions[(unsigned char)next_byte];
  if (PyBytes_CheckExact(action)) {
    switch (next_byte) {
      case '\xcb': {  // double
        if (deque_has_n_next_byte(&self->deque, 8)) {
          deque_advance_one_byte(&self->deque);
          char* data = 0;
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
          }
          return PyFloat_FromDouble(value);
        }
        return NULL;
      }
    }
    PyObject* errorMessage =
        PyUnicode_FromFormat("IS BYTE: %d", (unsigned char)next_byte);
    PyErr_SetObject(PyExc_TypeError, errorMessage);
    Py_XDECREF(errorMessage);
    return NULL;
  }
  deque_advance_one_byte(&self->deque);
  Py_INCREF(action);
  return action;
}

PyObject* create_unpacker_actions() {
  // todo: add error checks
  PyObject* actions = PyTuple_New(256);
  if (actions == NULL) {
    return NULL;
  }
  int i = 0;
  for (; i != 128; ++i) {
    PyObject* number = PyLong_FromLong(i);
    if (number == NULL) {
      return NULL;
    }
    PyTuple_SET_ITEM(actions, i, number);
  }
  PyObject* fixmap = PyBytes_FromString("m");
  for (; i != 128 + 16; ++i) {
    PyTuple_SET_ITEM(actions, i, fixmap);
  }
  PyObject* fixarray = PyBytes_FromString("F");
  for (; i != 128 + 16 + 16; ++i) {
    PyTuple_SET_ITEM(actions, i, fixarray);
  }

  PyObject* fixstr = PyBytes_FromString("S");
  for (; i != 128 + 16 + 16 + 32; ++i) {
    PyTuple_SET_ITEM(actions, i, fixstr);
  }
  PyTuple_SET_ITEM(actions, i++, Py_None);
  Py_INCREF(Py_None);

  PyObject* not_implemented = PyBytes_FromString("!");
  PyTuple_SET_ITEM(actions, i++, not_implemented);

  PyTuple_SET_ITEM(actions, i++, Py_False);  // False
  Py_INCREF(Py_False);
  PyTuple_SET_ITEM(actions, i++, Py_True);  // True
  Py_INCREF(Py_True);

  PyObject* bin = PyBytes_FromString("b");
  for (; i != 0xc7; ++i) {  // bin 8, 16, 32
    PyTuple_SET_ITEM(actions, i, bin);
  }
  PyObject* ext = PyBytes_FromString("e");
  for (; i != 0xc7; ++i) {  // ext 8, 16, 32
    PyTuple_SET_ITEM(actions, i, ext);
  }

  PyObject* float_ = PyBytes_FromString("f");
  PyTuple_SET_ITEM(actions, i++, float_);
  PyObject* double_ = PyBytes_FromString("d");
  PyTuple_SET_ITEM(actions, i++, double_);
  PyObject* uint8_ = PyBytes_FromString("B");
  PyTuple_SET_ITEM(actions, i++, uint8_);
  PyObject* uint16_ = PyBytes_FromString("H");
  PyTuple_SET_ITEM(actions, i++, uint16_);
  PyObject* uint32_ = PyBytes_FromString("I");
  PyTuple_SET_ITEM(actions, i++, uint32_);
  PyObject* uint64_ = PyBytes_FromString("Q");
  PyTuple_SET_ITEM(actions, i++, uint64_);
  PyObject* int8_ = PyBytes_FromString("b");
  PyTuple_SET_ITEM(actions, i++, int8_);
  PyObject* int16_ = PyBytes_FromString("h");
  PyTuple_SET_ITEM(actions, i++, int16_);
  PyObject* int32_ = PyBytes_FromString("i");
  PyTuple_SET_ITEM(actions, i++, int32_);
  PyObject* int64_ = PyBytes_FromString("q");
  PyTuple_SET_ITEM(actions, i++, int64_);
  for (; i != 0xd9; ++i) {  // fixext 1, 2, 4, 8, 16
    PyTuple_SET_ITEM(actions, i, not_implemented);
  }

  PyObject* str8_ = PyBytes_FromString("u");
  PyTuple_SET_ITEM(actions, i++, str8_);
  PyObject* str16 = PyBytes_FromString("U");
  PyTuple_SET_ITEM(actions, i++, str16);
  PyObject* str32 = PyBytes_FromString("y");
  PyTuple_SET_ITEM(actions, i++, str32);
  PyObject* arr16 = PyBytes_FromString("a");
  PyTuple_SET_ITEM(actions, i++, arr16);
  PyObject* arr32 = PyBytes_FromString("A");
  PyTuple_SET_ITEM(actions, i++, arr32);
  PyObject* map16 = PyBytes_FromString("m");
  PyTuple_SET_ITEM(actions, i++, map16);
  PyObject* map32 = PyBytes_FromString("m");
  PyTuple_SET_ITEM(actions, i++, map32);

  for (; i != 256; ++i) {
    PyObject* number = PyLong_FromLong((char)i);
    if (number == NULL) {
      return NULL;
    }
    PyTuple_SET_ITEM(actions, i, number);
  }
  return actions;
}

static PyTypeObject Unpacker_Type = {
    PyVarObject_HEAD_INIT(NULL, 0).tp_name = "amsgpack.Unpacker",
    .tp_basicsize = sizeof(Unpacker),
    .tp_doc = PyDoc_STR("Unpack bytes to python objects"),
    .tp_new = Unpacker_new,
    // .tp_dealloc = ...,
    .tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE,
    .tp_methods = Unpacker_Methods,
    .tp_iter = Unpacker_iter,
    .tp_iternext = Unpacker_iternext,
};

PyMODINIT_FUNC PyInit_amsgpack(void) {
  PyObject* actions = NULL;
  PyObject* module = PyModule_Create(&amsgpack_module);
  if (module == NULL) {
    return NULL;
  }
  if (PyModule_AddStringConstant(module, "__version__", VERSION) != 0) {
    goto error;
  }
  if (PyType_Ready(&Unpacker_Type) < 0) {
    goto error;
  }
  actions = create_unpacker_actions();
  if (!actions) {
    goto error;
  }
  if (PyDict_SetItemString(Unpacker_Type.tp_dict, "_actions", actions) < 0) {
    Py_DECREF(actions);
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
