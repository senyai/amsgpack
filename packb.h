#include "raw.h"

#define AMSGPACK_RESIZE(n)                                        \
  do {                                                            \
    if A_UNLIKELY(capacity < size + n) {                          \
      capacity += Py_MAX(capacity, n);                            \
      if A_UNLIKELY(_PyBytes_Resize(&buffer_py, capacity) != 0) { \
        goto error;                                               \
      }                                                           \
      data = PyBytes_AS_STRING(buffer_py);                        \
    }                                                             \
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

#define SECONDS_PER_DAY 86400
#define DAYS_PER_400_YEARS 146097

static int64_t days_since_epoch(int year, int month, int day) {
  if (month <= 2) {
    year--;
    month += 12;
  }

  int era = year / 400;
  int yoe = year - era * 400;
  int doy = (153 * (month - 3) + 2) / 5 + day - 1;
  int doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;

  return era * DAYS_PER_400_YEARS + doe - 719468;
}

typedef struct {
  enum PackAction { LIST_NEXT, TUPLE_NEXT, KEY_NEXT, VALUE_NEXT } action;
  PyObject* sequence;
  Py_ssize_t size;
  Py_ssize_t pos;
  PyObject* value;
} PackbStack;

static PyObject* packb(PyObject* Py_UNUSED(module), PyObject* obj) {
  Py_ssize_t capacity = 1024;
  Py_ssize_t size = 0;
  PyObject* buffer_py = PyBytes_FromStringAndSize(NULL, capacity);
  if A_UNLIKELY(buffer_py == NULL) {
    return NULL;
  }
  char* data = PyBytes_AS_STRING(buffer_py);

  PackbStack stack[A_STACK_SIZE];
  unsigned int stack_length = 0;
  void* obj_type;
pack_next:
  obj_type = Py_TYPE(obj);
  if A_UNLIKELY(obj_type == &PyFloat_Type) {
    // https://docs.python.org/3/c-api/float.html
    AMSGPACK_RESIZE(9);
    put9_dbl(data + size, '\xcb', PyFloat_AS_DOUBLE(obj));
    size += 9;
  } else if A_UNLIKELY(obj_type == &PyLong_Type) {
    // https://docs.python.org/3/c-api/long.html
    long const value = PyLong_AsLong(obj);
    if A_UNLIKELY(value == -1 && PyErr_Occurred() != NULL) {
      goto error;
    }
    if (value >= -0x20) {
      if (value < 0x80) {
        // fixint
        AMSGPACK_RESIZE(1);
        data[size] = (char)value;
        size += 1;
      } else if (value <= 0xff) {
        // uint 8
        AMSGPACK_RESIZE(2);
        put2(data + size, '\xcc', (char)value);
        size += 2;
      } else if (value <= 0xffff) {
        // unit 16
        AMSGPACK_RESIZE(3);
        put3(data + size, '\xcd', value);
        size += 3;
      } else if (value <= 0xffffffff) {
        // unit 32
        AMSGPACK_RESIZE(5);
        put5(data + size, '\xce', value);
        size += 5;
      } else {
        // uint 64
        AMSGPACK_RESIZE(9);
        put9(data + size, '\xcf', value);
        size += 9;
      }
    } else {
      if (value >= -0x80) {
        // int 8
        AMSGPACK_RESIZE(2);
        put2(data + size, '\xd0', (char)value);
        size += 2;
      } else if (value >= -0x8000) {
        // int 16
        AMSGPACK_RESIZE(3);
        put3(data + size, '\xd1', (unsigned short)value);
        size += 3;
      } else if (value >= -0x80000000LL) {
        // int 32
        AMSGPACK_RESIZE(5);
        put5(data + size, '\xd2', (unsigned int)value);
        size += 5;
      } else {
        // int 64
        AMSGPACK_RESIZE(9);
        put9(data + size, '\xd3', value);
        size += 9;
      }
    }
  } else if A_UNLIKELY(obj_type == &PyUnicode_Type) {
    // https://docs.python.org/3.11/c-api/unicode.html
    Py_ssize_t u8size = 0;
    const char* u8string = PyUnicode_AsUTF8AndSize(obj, &u8size);
    // ToDo: add error checking
    if (u8size <= 0xf) {
      AMSGPACK_RESIZE(1 + u8size);
      data[size] = 0xa0 + u8size;
      size += 1;
    } else if (u8size <= 0xff) {
      AMSGPACK_RESIZE(2 + u8size);
      put2(data + size, '\xd9', (char)u8size);
      size += 2;
    } else if (u8size <= 0xffff) {
      AMSGPACK_RESIZE(3 + u8size);
      put3(data + size, '\xda', (unsigned short)u8size);
      size += 3;
    } else if (u8size <= 0xffffffff) {
      AMSGPACK_RESIZE(5 + u8size);
      put5(data + size, '\xdb', u8size);
      size += 5;
    } else {
      PyErr_SetString(PyExc_ValueError,
                      "String length is out of MessagePack range");
      goto error;
    }
    memcpy(data + size, u8string, u8size);
    size += u8size;
  } else if A_UNLIKELY(obj_type == &PyList_Type || obj_type == &PyTuple_Type) {
    // https://docs.python.org/3.11/c-api/list.html
    if A_UNLIKELY(stack_length >= A_STACK_SIZE) {
      PyErr_SetString(PyExc_ValueError, "Deeply nested object");
      goto error;
    }
    Py_ssize_t const length = (obj_type == &PyList_Type)
                                  ? PyList_GET_SIZE(obj)
                                  : PyTuple_GET_SIZE(obj);
    if (length <= 0x0f) {
      AMSGPACK_RESIZE(1);
      data[size] = '\x90' + (char)length;
      size += 1;
    } else if (length <= 0xffff) {
      AMSGPACK_RESIZE(3);
      put3(data + size, '\xdc', (unsigned short)length);
      size += 3;
    } else if (length <= 0xffffffff) {
      AMSGPACK_RESIZE(5);
      put5(data + size, '\xdd', (unsigned int)length);
      size += 5;
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
  } else if A_UNLIKELY(obj_type == &PyDict_Type) {
    if A_UNLIKELY(stack_length >= A_STACK_SIZE) {
      PyErr_SetString(PyExc_ValueError, "Deeply nested object");
      goto error;
    }
    // https://docs.python.org/3.11/c-api/dict.html
    Py_ssize_t const dict_size = PyDict_Size(obj);

    if (dict_size <= 15) {
      AMSGPACK_RESIZE(1);
      data[size] = 0x80 + dict_size;
      size += 1;
    } else if (dict_size <= 0xffff) {
      AMSGPACK_RESIZE(3);
      put3(data + size, '\xde', (unsigned short)dict_size);
      size += 3;
    } else if (dict_size <= 0xffffffff) {
      AMSGPACK_RESIZE(5);
      put5(data + size, '\xdf', (unsigned int)dict_size);
      size += 5;
    } else {
      PyErr_SetString(PyExc_ValueError,
                      "Dict length is out of MessagePack range");
      goto error;
    }
    PackbStack const item = {
        .action = KEY_NEXT, .sequence = obj, .size = dict_size, .pos = 0};
    stack[stack_length++] = item;
  } else if A_UNLIKELY(obj_type == &PyBytes_Type ||
                       obj_type == &PyByteArray_Type) {
    // https://docs.python.org/3.11/c-api/bytes.html
    // https://docs.python.org/3.11/c-api/bytearray.html
    char* bytes_buffer;
    Py_ssize_t bytes_size;
    if A_LIKELY(obj_type == &PyBytes_Type) {
      bytes_size = PyBytes_GET_SIZE(obj);
      bytes_buffer = PyBytes_AS_STRING(obj);
    } else {
      bytes_size = PyByteArray_GET_SIZE(obj);
      bytes_buffer = PyByteArray_AS_STRING(obj);
    }
    if (bytes_size <= 0xff) {
      AMSGPACK_RESIZE(2 + bytes_size);
      put2(data + size, '\xc4', bytes_size);
      size += 2;
    } else if (bytes_size <= 0xffff) {
      AMSGPACK_RESIZE(3 + bytes_size);
      put3(data + size, '\xc5', bytes_size);
      size += 3;
    } else if (bytes_size <= 0xffffffff) {
      AMSGPACK_RESIZE(5 + bytes_size);
      put5(data + size, '\xc6', (unsigned int)bytes_size);
      size += 5;
    } else {
      PyErr_SetString(PyExc_ValueError,
                      "Bytes length is out of MessagePack range");
      goto error;
    }
    memcpy(data + size, bytes_buffer, bytes_size);
    size += bytes_size;
  } else if A_UNLIKELY(obj == Py_None) {
    AMSGPACK_RESIZE(1);
    data[size] = '\xc0';
    size += 1;
  } else if A_UNLIKELY(obj_type == &PyBool_Type) {
    AMSGPACK_RESIZE(1);
    data[size] = obj == Py_True ? '\xc3' : '\xc2';
    size += 1;
  } else if A_UNLIKELY(obj_type == &Ext_Type) {
    Ext* ext = (Ext*)obj;
    Py_ssize_t const ext_data_length = PyBytes_GET_SIZE(ext->data);
    char const* data_bytes = PyBytes_AS_STRING(ext->data);
    char header = '\0';
    switch (ext_data_length) {
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
        if (ext_data_length <= 0xff) {
          AMSGPACK_RESIZE(2 + 1 + ext_data_length);
          put2(data + size, '\xc7', ext_data_length);
          size += 2;
        } else if (ext_data_length <= 0xffff) {
          AMSGPACK_RESIZE(3 + 1 + ext_data_length);
          put3(data + size, '\xc8', ext_data_length);
          size += 3;
        } else if (ext_data_length <= 0xffffffff) {
          AMSGPACK_RESIZE(5 + 1 + ext_data_length);
          put5(data + size, '\xc9', ext_data_length);
          size += 5;
        } else {
          PyErr_SetString(PyExc_TypeError, "Ext() length is too large");
          goto error;
        }
        data[size] = ext->code;
        memcpy(data + size + 1, data_bytes, ext_data_length);
        size += 1 + ext_data_length;
        break;
      non_default:
        AMSGPACK_RESIZE(2 + ext_data_length);
        put2(data, header, ext->code);
        memcpy(data + 2, data_bytes, ext_data_length);
        size += 2 + ext_data_length;
    }
  } else if A_UNLIKELY(obj_type == &Raw_Type) {
    Py_ssize_t const raw_length = PyBytes_GET_SIZE(((Raw*)obj)->data);
    AMSGPACK_RESIZE(raw_length);
    memcpy(data + size, PyBytes_AS_STRING(((Raw*)obj)->data), raw_length);
    size += raw_length;
  } else if A_UNLIKELY(PyDateTime_CheckExact(obj)) {
    int const year = PyDateTime_GET_YEAR(obj);
    int const month = PyDateTime_GET_MONTH(obj);
    int const day = PyDateTime_GET_DAY(obj);
    int const hour = PyDateTime_DATE_GET_HOUR(obj);
    int const minute = PyDateTime_DATE_GET_MINUTE(obj);
    int const second = PyDateTime_DATE_GET_SECOND(obj);
    int const microsecond = PyDateTime_DATE_GET_MICROSECOND(obj);
    int64_t const days = days_since_epoch(year, month, day);
    int64_t const total_seconds =
        days * SECONDS_PER_DAY + hour * 3600 + minute * 60 + second;
    uint64_t const nanoseconds = (uint64_t)microsecond * 1000;
    uint64_t const timestamp64 =
        ((nanoseconds & 0x3FFFFFFF) << 34) | (total_seconds & 0x3FFFFFFFF);

    AMSGPACK_RESIZE(10);
    // packing fixext 8
    data[size] = '\xd7';
    data[size + 1] = '\xff';
    data[size + 2 + 0] = (timestamp64 >> 070) & 0xff;
    data[size + 2 + 1] = (timestamp64 >> 060) & 0xff;
    data[size + 2 + 2] = (timestamp64 >> 050) & 0xff;
    data[size + 2 + 3] = (timestamp64 >> 040) & 0xff;
    data[size + 2 + 4] = (timestamp64 >> 030) & 0xff;
    data[size + 2 + 5] = (timestamp64 >> 020) & 0xff;
    data[size + 2 + 6] = (timestamp64 >> 010) & 0xff;
    data[size + 2 + 7] = (timestamp64 >> 000) & 0xff;
    size += 10;
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
        if A_UNLIKELY(item->pos == item->size) {
          stack_length -= 1;
          break;
        }
        obj = PyList_GET_ITEM(item->sequence, item->pos);
        item->pos += 1;
        goto pack_next;
      case TUPLE_NEXT:
        if A_UNLIKELY(item->pos == item->size) {
          stack_length -= 1;
          break;
        }
        obj = PyTuple_GET_ITEM(item->sequence, item->pos);
        item->pos += 1;
        goto pack_next;
      case KEY_NEXT:
        if A_UNLIKELY(item->pos == item->size) {
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
        Py_UNREACHABLE();  // GCOVR_EXCL_LINE
    }
  }

  Py_SET_SIZE(buffer_py, size);
  data[size] = 0;  // warning! is it safe?
  return buffer_py;
error:
  Py_XDECREF(buffer_py);
  return NULL;
}

#undef AMSGPACK_RESIZE
