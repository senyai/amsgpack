#include <Python.h>

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
    if (PyByteArray_Resize(byte_array, pos + 1) != 0) {
      return -1;
    }
    data = PyByteArray_AS_STRING(byte_array) + pos;
    if (obj == Py_True) {
      data[0] = '\xc3';
    } else {
      data[0] = '\xc2';
    }
  } else if (PyFloat_CheckExact(obj)) {
    // https://docs.python.org/3/c-api/float.html
    if (PyByteArray_Resize(byte_array, pos + 9) != 0) {
      return -1;
    }
    data = PyByteArray_AS_STRING(byte_array) + pos;

    double const value = PyFloat_AS_DOUBLE(obj);
    data[0] = '\xcb';
    PyFloat_Pack8(value, data + 1, 0);
  } else if (PyLong_CheckExact(obj)) {
    // https://docs.python.org/3/c-api/long.html
    PyObject* part;
    long const value = PyLong_AsLong(obj);
    if (-0x20 <= value && value < 0x80) {
      if (PyByteArray_Resize(byte_array, pos + 1) != 0) {
        return -1;
      }
      data = PyByteArray_AS_STRING(byte_array) + pos;
      data[0] = (char)value;
      // part = PyBytes_FromStringAndSize(&i8, 1);
    } else if (0x80 <= value && value <= UCHAR_MAX) {
      if (PyByteArray_Resize(byte_array, pos + 2) != 0) {
        return -1;
      }
      data = PyByteArray_AS_STRING(byte_array) + pos;
      data[0] = '\xcc';
      data[1] = (char)value;
    } else if (SCHAR_MIN <= value && value < 0) {
      if (PyByteArray_Resize(byte_array, pos + 2) != 0) {
        return -1;
      }
      data = PyByteArray_AS_STRING(byte_array) + pos;
      data[0] = '\xd0';
      data[1] = (char)value;
    } else if (UCHAR_MAX < value && value <= 0xffff) {
      if (PyByteArray_Resize(byte_array, pos + 3) != 0) {
        return -1;
      }
      data = PyByteArray_AS_STRING(byte_array) + pos;
      data[0] = '\xcd';
      PyLong_AsNativeBytes(obj, data + 1, 2, 0);
    } else if (SHRT_MIN <= value && value < -0x80) {
      if (PyByteArray_Resize(byte_array, pos + 3) != 0) {
        return -1;
      }
      data = PyByteArray_AS_STRING(byte_array) + pos;
      data[0] = '\xd1';

      PyLong_AsNativeBytes(obj, data + 1, 2, 0);
    } else if (0xffff < value && value <= 0xffffffff) {
      if (PyByteArray_Resize(byte_array, pos + 5) != 0) {
        return -1;
      }
      data = PyByteArray_AS_STRING(byte_array) + pos;

      data[0] = '\xce';
      PyLong_AsNativeBytes(obj, data + 1, 4, 0);
    } else if (-0x7fffffff <= value && value < -0x8000) {
      if (PyByteArray_Resize(byte_array, pos + 5) != 0) {
        return -1;
      }
      data = PyByteArray_AS_STRING(byte_array) + pos;
      data[0] = '\xd2';
      PyLong_AsNativeBytes(obj, data + 1, 4, 0);
    } else if (0xffffffff < value /*&& value <= 0xffffffffffffffff*/) {
      if (PyByteArray_Resize(byte_array, pos + 9) != 0) {
        return -1;
      }
      data = PyByteArray_AS_STRING(byte_array) + pos;
      data[0] = '\xcf';

      PyLong_AsNativeBytes(obj, data + 1, 8, 0);
    } else if (/*-0x8000000000000000 <= value &&*/ value < -0x80000000LL) {
      if (PyByteArray_Resize(byte_array, pos + 9) != 0) {
        return -1;
      }
      data = PyByteArray_AS_STRING(byte_array) + pos;
      data[0] = '\xd3';

      PyLong_AsNativeBytes(obj, data + 1, 8, 0);
    } else {
      PyErr_SetString(PyExc_ValueError,
                      "Integral value is out of MessagePack range");
      return -1;
    }
  } else if (obj == Py_None) {
    if (PyByteArray_Resize(byte_array, pos + 1) != 0) {
      return -1;
    }
    data = PyByteArray_AS_STRING(byte_array) + pos;
    data[0] = '\xc0';
  } else if (PyUnicode_CheckExact(obj)) {
    // https://docs.python.org/3.11/c-api/unicode.html
    Py_ssize_t u8size = 0;
    const char* u8string = PyUnicode_AsUTF8AndSize(obj, &u8size);
    if (u8size <= 0xff) {
      if (PyByteArray_Resize(byte_array, pos + 2 + u8size) != 0) {
        return -1;
      }
      data = PyByteArray_AS_STRING(byte_array) + pos;

      data[0] = 0xd9;
      data[1] = (char)u8size;
      memcpy(data + 2, u8string, u8size);
      return 0;
    }
    if (u8size <= 0xffff) {
      if (PyByteArray_Resize(byte_array, pos + 3 + u8size) != 0) {
        return -1;
      }
      data = PyByteArray_AS_STRING(byte_array) + pos;

      data[0] = 0xda;
      data[1] = ((char*)&u8size)[1];
      data[2] = ((char*)&u8size)[0];
      memcpy(data + 3, u8string, u8size);
      return 0;
    }
    if (u8size <= 0xffffffff) {
      if (PyByteArray_Resize(byte_array, pos + 5 + u8size) != 0) {
        return -1;
      }
      data = PyByteArray_AS_STRING(byte_array) + pos;
      data[0] = 0xdb;
      data[1] = ((char*)&u8size)[3];
      data[2] = ((char*)&u8size)[2];
      data[3] = ((char*)&u8size)[1];
      data[4] = ((char*)&u8size)[0];
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
      if (PyByteArray_Resize(byte_array, pos + 1) != 0) {
        return -1;
      }
      data = PyByteArray_AS_STRING(byte_array) + pos;
      data[0] = 0x90 + list_size;
    } else if (list_size <= 0xffff) {
      if (PyByteArray_Resize(byte_array, pos + 3) != 0) {
        return -1;
      }
      data = PyByteArray_AS_STRING(byte_array) + pos;
      unsigned short size2 = list_size;
      data[0] = '\xdc';
      data[1] = ((char*)&size2)[1];
      data[2] = ((char*)&size2)[0];
    } else if (list_size <= 0xffffffff) {
      if (PyByteArray_Resize(byte_array, pos + 5) != 0) {
        return -1;
      }
      data = PyByteArray_AS_STRING(byte_array) + pos;
      unsigned int size4 = list_size;
      data[0] = '\xdd';
      data[1] = ((char*)&size4)[3];
      data[2] = ((char*)&size4)[2];
      data[3] = ((char*)&size4)[1];
      data[4] = ((char*)&size4)[0];
    } else {
      PyErr_SetString(PyExc_ValueError,
                      "List length is out of MessagePack range");
      return -1;
    }
    for (Py_ssize_t i = 0; i < list_size; i++) {
      PyObject* item = PyList_GetItem(obj, i);
      if (packb_(item, byte_array, level + 1) != 0) {
        return -1;
      }
    }
  } else if (PyDict_CheckExact(obj)) {
    // https://docs.python.org/3.11/c-api/dict.html
    Py_ssize_t const dict_size = PyDict_Size(obj);

    if (dict_size <= 15) {
      if (PyByteArray_Resize(byte_array, pos + 1) != 0) {
        return -1;
      }
      data = PyByteArray_AS_STRING(byte_array) + pos;
      data[0] = 0x80 + dict_size;
    } else if (dict_size <= 0xffff) {
      if (PyByteArray_Resize(byte_array, pos + 3) != 0) {
        return -1;
      }
      data = PyByteArray_AS_STRING(byte_array) + pos;
      unsigned short size2 = dict_size;
      data[0] = '\xde';
      data[1] = ((char*)&size2)[1];
      data[2] = ((char*)&size2)[0];
    } else if (dict_size <= 0xffffffff) {
      if (PyByteArray_Resize(byte_array, pos + 5) != 0) {
        return -1;
      }
      data = PyByteArray_AS_STRING(byte_array) + pos;
      unsigned int size4 = dict_size;
      data[0] = '\xdf';
      data[1] = ((char*)&size4)[3];
      data[2] = ((char*)&size4)[2];
      data[3] = ((char*)&size4)[1];
      data[4] = ((char*)&size4)[0];
    } else {
      PyErr_SetString(PyExc_ValueError,
                      "Dict length is out of MessagePack range");
      return -1;
    }

    PyObject *key, *value;

    Py_ssize_t dict_pos = 0;

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
      if (PyByteArray_Resize(byte_array, pos + 2 + bytes_size) != 0) {
        return -1;
      }
      data = PyByteArray_AS_STRING(byte_array) + pos;
      data[0] = 0xc4;
      data[1] = bytes_size;
      pos = 2;
    } else if (bytes_size <= 0xffff) {
      if (PyByteArray_Resize(byte_array, pos + 3 + bytes_size) != 0) {
        return -1;
      }
      data = PyByteArray_AS_STRING(byte_array) + pos;
      unsigned short size2 = bytes_size;
      data[0] = '\xc5';
      data[1] = ((char*)&size2)[1];
      data[2] = ((char*)&size2)[0];
      pos = 3;
    } else if (bytes_size <= 0xffffffff) {
      if (PyByteArray_Resize(byte_array, pos + 5 + bytes_size) != 0) {
        return -1;
      }
      data = PyByteArray_AS_STRING(byte_array) + pos;
      unsigned int size4 = bytes_size;
      data[0] = '\xc6';
      data[1] = ((char*)&size4)[3];
      data[2] = ((char*)&size4)[2];
      data[3] = ((char*)&size4)[1];
      data[4] = ((char*)&size4)[0];
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

static PyObject* packb(PyObject* module, PyObject* const* args,
                       Py_ssize_t nargs) {
  if (nargs != 1) {
    PyErr_SetString(PyExc_TypeError,
                    "packb() requires 1 positional argument: 'obj'");
    return NULL;
  }

  PyObject* byte_array = PyByteArray_FromStringAndSize(NULL, 0);
  if (byte_array == NULL) {
    return NULL;
  }
  PyObject* obj = args[0];

  if (packb_(obj, byte_array, 0) != 0) {
    Py_XDECREF(byte_array);
    return NULL;
  }
  return byte_array;
}

static PyMethodDef MyMethods[] = {
    {"packb", (PyCFunction)packb, METH_FASTCALL,
     "Serialize ``obj`` to a MessagePack formatted ``bytes``."},
    {NULL, NULL, 0, NULL}  // Sentinel
};

static struct PyModuleDef mymodule = {
    PyModuleDef_HEAD_INIT,
    "amsgpack",  // Name of the module
    NULL,        // Module documentation
    -1,          // Size of per-interpreter state of the module
    MyMethods};

PyMODINIT_FUNC PyInit_amsgpack(void) { return PyModule_Create(&mymodule); }
