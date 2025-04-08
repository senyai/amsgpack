#define AMSGPACK_RESIZE(n)                                \
  do {                                                    \
    if (PyByteArray_Resize(byte_array, pos + (n)) != 0) { \
      return -1;                                          \
    }                                                     \
    data = PyByteArray_AS_STRING(byte_array) + pos;       \
  } while (0)

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

// returns: -1 - failure
//           0 - success
static int packb_(PyObject* obj, PyObject* byte_array, int level) {
  Py_ssize_t pos = PyByteArray_GET_SIZE(byte_array);
  char* data;
  unsigned int is_list = 1;
  if (level >= A_STACK_SIZE) {
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
  } else if (PyList_CheckExact(obj) ||
             (PyTuple_CheckExact(obj) && (is_list = 2))) {
    // https://docs.python.org/3.11/c-api/list.html
    Py_ssize_t const list_size =
        (is_list == 1) ? PyList_GET_SIZE(obj) : PyTuple_GET_SIZE(obj);
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
    if (is_list == 1)
      for (Py_ssize_t i = 0; i < list_size; i++) {
        PyObject* item = PyList_GET_ITEM(obj, i);
        if (packb_(item, byte_array, level + 1) != 0) {
          return -1;
        }
      }
    else {
      for (Py_ssize_t i = 0; i < list_size; i++) {
        PyObject* item = PyTuple_GET_ITEM(obj, i);
        if (packb_(item, byte_array, level + 1) != 0) {
          return -1;
        }
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
    Py_ssize_t dict_pos = 0;
    while (PyDict_Next(obj, &dict_pos, &key, &value)) {
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
    PyObject* errorMessage = PyUnicode_FromFormat("Unserializable '%s' object",
                                                  Py_TYPE(obj)->tp_name);
    PyErr_SetObject(PyExc_TypeError, errorMessage);
    Py_XDECREF(errorMessage);
    return -1;
  }
  return 0;
}

static PyObject* packb(PyObject* _module, PyObject* obj) {
  (void)_module;
  PyObject* byte_array = PyByteArray_FromStringAndSize(NULL, 0);
  if (byte_array == NULL) {
    return NULL;
  }

  if (packb_(obj, byte_array, 0) != 0) {
    Py_DECREF(byte_array);
    return NULL;
  }
  return byte_array;
}

#undef AMSGPACK_RESIZE
