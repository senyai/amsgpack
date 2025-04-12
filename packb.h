#define AMSGPACK_RESIZE(n)                                \
  do {                                                    \
    if (PyByteArray_Resize(byte_array, pos + (n)) != 0) { \
      goto error;                                         \
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

typedef struct {
  enum PackAction { LIST_NEXT, TUPLE_NEXT, KEY_NEXT, VALUE_NEXT } action;
  PyObject* sequence;
  Py_ssize_t size;
  Py_ssize_t pos;
  PyObject* value;
} PackbStack;

// returns: -1 - failure
//           0 - success
static PyObject* packb(PyObject* _module, PyObject* obj) {
  (void)_module;
  PyObject* byte_array = PyByteArray_FromStringAndSize(NULL, 0);
  if (byte_array == NULL) {
    return NULL;
  }
  PackbStack stack[A_STACK_SIZE];
  unsigned int stack_length = 0;
pack_next:
  Py_ssize_t pos = PyByteArray_GET_SIZE(byte_array);
  char* data;
  PyTypeObject const* obj_type = Py_TYPE(obj);
  if (obj_type == &PyFloat_Type) {
    // https://docs.python.org/3/c-api/float.html
    AMSGPACK_RESIZE(9);
    double const value = PyFloat_AS_DOUBLE(obj);
    put9_dbl(data, '\xcb', value);
  } else if (obj_type == &PyLong_Type) {
    // https://docs.python.org/3/c-api/long.html
    long const value = PyLong_AsLong(obj);
    if (value == -1 && PyErr_Occurred() != NULL) {
      goto error;
    }
    if (value >= -0x20) {
      if (value < 0x80) {
        // fixint
        AMSGPACK_RESIZE(1);
        data[0] = (char)value;
      } else if (value <= 0xff) {
        // uint 8
        AMSGPACK_RESIZE(2);
        put2(data, '\xcc', (char)value);
      } else if (value <= 0xffff) {
        // unit 16
        AMSGPACK_RESIZE(3);
        put3(data, '\xcd', value);
      } else if (value <= 0xffffffff) {
        // unit 32
        AMSGPACK_RESIZE(5);
        put5(data, '\xce', value);
      } else {
        // uint 64
        AMSGPACK_RESIZE(9);
        put9(data, '\xcf', value);
      }
    } else {
      if (value >= -0x80) {
        // int 8
        AMSGPACK_RESIZE(2);
        put2(data, '\xd0', (char)value);
      } else if (value >= -0x8000) {
        // int 16
        AMSGPACK_RESIZE(3);
        put3(data, '\xd1', (unsigned short)value);
      } else if (value >= -0x80000000LL) {
        // int 32
        AMSGPACK_RESIZE(5);
        put5(data, '\xd2', (unsigned int)value);
      } else {
        // int 64
        AMSGPACK_RESIZE(9);
        put9(data, '\xd3', value);
      }
    }
  } else if (obj_type == &PyUnicode_Type) {
    // https://docs.python.org/3.11/c-api/unicode.html
    Py_ssize_t u8size = 0;
    const char* u8string = PyUnicode_AsUTF8AndSize(obj, &u8size);
    // ToDo: add error checking
    if (u8size <= 0xf) {
      AMSGPACK_RESIZE(1 + u8size);
      data[0] = 0xa0 + u8size;
      memcpy(data + 1, u8string, u8size);
    } else if (u8size <= 0xff) {
      AMSGPACK_RESIZE(2 + u8size);
      put2(data, '\xd9', (char)u8size);
      memcpy(data + 2, u8string, u8size);
    } else if (u8size <= 0xffff) {
      AMSGPACK_RESIZE(3 + u8size);
      put3(data, '\xda', (unsigned short)u8size);
      memcpy(data + 3, u8string, u8size);
    } else if (u8size <= 0xffffffff) {
      AMSGPACK_RESIZE(5 + u8size);
      put5(data, '\xdb', u8size);
      memcpy(data + 5, u8string, u8size);
    } else {
      PyErr_SetString(PyExc_ValueError,
                      "String length is out of MessagePack range");
      goto error;
    }
  } else if (obj_type == &PyList_Type || obj_type == &PyTuple_Type) {
    // https://docs.python.org/3.11/c-api/list.html
    if (stack_length >= A_STACK_SIZE) {
      PyErr_SetString(PyExc_ValueError, "Deeply nested object");
      goto error;
    }
    Py_ssize_t const length = (obj_type == &PyList_Type)
                                  ? PyList_GET_SIZE(obj)
                                  : PyTuple_GET_SIZE(obj);
    if (length <= 0x0f) {
      AMSGPACK_RESIZE(1);
      data[0] = '\x90' + (char)length;
    } else if (length <= 0xffff) {
      AMSGPACK_RESIZE(3);
      put3(data, '\xdc', (unsigned short)length);
    } else if (length <= 0xffffffff) {
      AMSGPACK_RESIZE(5);
      put5(data, '\xdd', (unsigned int)length);
    } else {
      PyErr_SetString(PyExc_ValueError,
                      "List length is out of MessagePack range");
      goto error;
    }
    PackbStack const item = {
        .action = (obj_type == &PyList_Type ? LIST_NEXT : TUPLE_NEXT),
        .sequence = obj,
        .size = length,
        .pos = 0};
    stack[stack_length++] = item;
  } else if (obj_type == &PyDict_Type) {
    if (stack_length >= A_STACK_SIZE) {
      PyErr_SetString(PyExc_ValueError, "Deeply nested object");
      goto error;
    }
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
      goto error;
    }
    PackbStack const item = {
        .action = KEY_NEXT, .sequence = obj, .size = dict_size, .pos = 0};
    stack[stack_length++] = item;
  } else if (obj_type == &PyBytes_Type) {
    // https://docs.python.org/3.11/c-api/bytes.html
    char* buffer;
    Py_ssize_t bytes_size;
    if (PyBytes_AsStringAndSize(obj, &buffer, &bytes_size) < 0) {
      goto error;
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
      goto error;
    }
    memcpy(data + pos, buffer, bytes_size);
  } else if (obj == Py_None) {
    AMSGPACK_RESIZE(1);
    data[0] = '\xc0';
  } else if (obj_type == &PyBool_Type) {
    AMSGPACK_RESIZE(1);
    data[0] = obj == Py_True ? '\xc3' : '\xc2';
  } else if (obj_type == &Ext_Type) {
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
          goto error;
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
    goto error;
  }

  while (stack_length) {
    PackbStack* item = &stack[stack_length - 1];
    switch (item->action) {
      case LIST_NEXT:
        if (item->pos == item->size) {
          stack_length -= 1;
          break;
        }
        obj = PyList_GET_ITEM(item->sequence, item->pos);
        item->pos += 1;
        goto pack_next;
      case TUPLE_NEXT:
        if (item->pos == item->size) {
          stack_length -= 1;
          break;
        }
        obj = PyTuple_GET_ITEM(item->sequence, item->pos);
        item->pos += 1;
        goto pack_next;
      case KEY_NEXT:
        if (item->pos == item->size) {
          stack_length -= 1;
          break;
        }
        PyDict_Next(item->sequence, &item->pos, &obj, &item->value);
        item->action = VALUE_NEXT;
        goto pack_next;
      case VALUE_NEXT:
        item->action = KEY_NEXT;
        obj = item->value;
        goto pack_next;
      default:
        assert(0);
    }
  }

  return byte_array;
error:
  Py_DECREF(byte_array);
  return NULL;
}

#undef AMSGPACK_RESIZE
