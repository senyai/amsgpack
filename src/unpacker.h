#include <Python.h>

#include "deque.h"
#define MiB128 134217728

#ifndef PYPY_VERSION
#define ANEW_DICT _PyDict_NewPresized
#else
#define ANEW_DICT(N) PyDict_New()
#endif

static inline uint32_t xxhash32(uint8_t const* data, uint32_t len,
                                uint32_t seed) {
  if (len > MAX_CACHE_LEN) {
    Py_UNREACHABLE();  // GCOVR_EXCL_LINE
  }
  uint32_t const prime = 0x9E3779B1;
  uint32_t hash = seed + prime;
  size_t idx = 0;

  for (; idx != (len & ~0x03U); idx += 4) {
    uint32_t block;
    // Avoids strict-aliasing issues, as we know the data is not aligned
    memcpy(&block, data + idx, 4);
    hash ^= block;
    hash *= prime;
    hash = (hash << 13) | (hash >> 19);  // Rotate left 13
  }

  for (; idx < len; ++idx) {
    hash ^= data[idx];
    hash *= prime;
    hash = (hash << 13) | (hash >> 19);
  }

  hash ^= len;
  hash *= 0x85EBCA77;
  hash ^= (hash >> 16);
  return hash;
}

static inline PyObject* as_string(AMsgPackState* state, char const* str,
                                  Py_ssize_t length) {
  if A_LIKELY(length <= MAX_CACHE_LEN) {
    // let's not use the seed, as there's no actual denial of service
    uint32_t const hash =
        xxhash32((uint8_t const*)str, length, 0 /*_Py_HashSecret.siphash.k0*/);
    CacheEntry* cache_entry =
        &state->unicode_cache[hash & (CACHE_TABLE_SIZE - 1)];
    if (cache_entry->hash == hash && cache_entry->len == length &&
        memcmp(str, cache_entry->data, length) == 0) {
      Py_INCREF(cache_entry->obj);
      return cache_entry->obj;
    }
    PyObject* parsed_object = PyUnicode_DecodeUTF8(str, length, NULL);
    if A_UNLIKELY(parsed_object == NULL) {
      return parsed_object;
    }
    Py_XDECREF(cache_entry->obj);
    cache_entry->hash = hash;
    Py_INCREF(parsed_object);
    cache_entry->obj = parsed_object;
    cache_entry->len = length;
    memcpy(cache_entry->data, str, length);
    return parsed_object;
  }
  return PyUnicode_DecodeUTF8(str, length, NULL);
}

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

#include "ext.h"

static inline int can_not_append_stack(Parser const* parser) {
  return parser->stack_length >= A_STACK_SIZE;
}

typedef struct {
  unsigned char readonly : 1;
} UnpackerFlags;

typedef struct {
  PyObject_HEAD
  Deque deque;
  Parser parser;
  AMsgPackState* state;
  PyObject* ext_hook;
  UnpackerFlags flags;
} Unpacker;

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

static PyObject* Unpacker_iternext(Unpacker* self) {
  int parse_a_key = 0;
  // to allow passing length between switch cases
  union {
    Py_ssize_t str;
    Py_ssize_t bin;
    Py_ssize_t arr;
    Py_ssize_t map;
    Py_ssize_t ext;  // without code
  } length;
  PyObject* parsed_object;
  char next_byte;
parse_next:
  if (!deque_has_next_byte(&self->deque)) {
    if A_UNLIKELY(self->deque.size == 0 && self->deque.deque_first != NULL) {
      PyErr_SetString(PyExc_RuntimeError, "Must not re-enter unpacking");
    }
    goto exception;
  }
  next_byte = deque_peek_byte(&self->deque);
parse_next_with_next_byte_set:
  switch (next_byte) {
    case '\x80':
      parsed_object = ANEW_DICT(0);
      if A_UNLIKELY(parsed_object == NULL) {
        goto exception;
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
      length.map = Py_CHARMASK(next_byte) & 0x0f;
      deque_advance_first_bytes(&self->deque, 1);
      goto length_map;
    length_map: {
      if A_UNLIKELY(can_not_append_stack(&self->parser)) {
        PyErr_SetString(PyExc_ValueError, "Deeply nested object");
        goto exception;
      }
      parsed_object = ANEW_DICT(length.map);
      if A_UNLIKELY(parsed_object == NULL) {
        goto exception;
      }
      if (length.map == 0) {
        break;
      }
      self->parser.stack[self->parser.stack_length++] =
          (Stack){.action = DICT_KEY,
                  .sequence = parsed_object,
                  .size = length.map,
                  .pos = 0};
      if (!deque_has_next_byte(&self->deque)) {
        goto exception;
      }
      parse_a_key = 1;
      next_byte = deque_peek_byte(&self->deque);
      if (next_byte >= '\xa1' && next_byte <= '\xbf') {
        goto fixstr;
      }
      goto parse_next_with_next_byte_set;
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
      length.arr = Py_CHARMASK(next_byte) & 0x0f;
      deque_advance_first_bytes(&self->deque, 1);
      goto length_arr;
    }
    length_arr: {
      if A_UNLIKELY(can_not_append_stack(&self->parser)) {
        PyErr_SetString(PyExc_ValueError, "Deeply nested object");
        goto exception;
      }
      parsed_object =
          (self->flags.readonly == 0 ? PyList_New : PyTuple_New)(length.arr);
      if A_UNLIKELY(parsed_object == NULL) {
        goto exception;
      }
      if (length.arr == 0) {
        break;
      }
#ifndef PYPY_VERSION
      PyObject** values = self->flags.readonly == 0
                              ? ((PyListObject*)parsed_object)->ob_item
                              : ((PyTupleObject*)parsed_object)->ob_item;
#else
      PyObject** values = PySequence_Fast_ITEMS(parsed_object);
#endif
      self->parser.stack[self->parser.stack_length++] =
          (Stack){.action = SEQUENCE_APPEND,
                  .sequence = parsed_object,
                  .size = length.arr,
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
    case '\xbf':
    fixstr:
      length.str = Py_CHARMASK(next_byte) & 0x1f;
      if A_LIKELY(deque_has_next_n_bytes(&self->deque, length.str + 1)) {
        deque_advance_first_bytes(&self->deque, 1);
        goto length_str;
      }
      goto exception;
    length_str: {
      READ_A_DATA(length.str);
      if (parse_a_key != 0) {
        parsed_object = as_string(self->state, data, length.str);
        parse_a_key = 0;
      } else {
        parsed_object = PyUnicode_DecodeUTF8(data, length.str, NULL);
      }

      FREE_A_DATA(length.str);
      if A_UNLIKELY(parsed_object == NULL) {
        goto exception;
      }
      break;
    }
    case '\xc1':  // (never used)
      PyErr_SetString(PyExc_ValueError, "amsgpack: 0xc1 byte must not be used");
      goto exception;
    case '\xc4':  // bin 8
    case '\xc5':  // bin 16
    case '\xc6':  // bin 32
    {
      unsigned char const size_size = 1 << (next_byte - '\xc4');
      if A_LIKELY(deque_has_next_n_bytes(&self->deque, 1 + size_size)) {
        length.bin = deque_peek_size(&self->deque, size_size);
        if A_UNLIKELY(length.bin > MiB128) {
          return size_error("bytes", length.bin, MiB128);
        }
        if (deque_has_next_n_bytes(&self->deque, 1 + size_size + length.bin)) {
          deque_skip_size(&self->deque, size_size);
          if A_UNLIKELY(length.bin == 0) {
            parsed_object = PyBytes_FromStringAndSize(NULL, length.bin);
          } else {
            READ_A_DATA(length.bin);
            parsed_object = PyBytes_FromStringAndSize(data, length.bin);
            FREE_A_DATA(length.bin);
          }
          if A_UNLIKELY(parsed_object == NULL) {
            goto exception;
          }
          break;
        }
      }
      goto exception;
    }
    case '\xc7':  // ext 8
    case '\xc8':  // ext 16
    case '\xc9':  // ext 32
    {
      unsigned char const size_size = 1 << (next_byte - '\xc7');
      if A_LIKELY(deque_has_next_n_bytes(&self->deque, 1 + size_size + 1)) {
        length.ext = deque_peek_size(&self->deque, size_size);
        if A_UNLIKELY(length.ext >= MiB128) {
          return size_error("ext", length.ext, MiB128);
        }
        if A_LIKELY(deque_has_next_n_bytes(&self->deque,
                                           1 + size_size + 1 + length.ext)) {
          deque_skip_size(&self->deque, size_size);
          goto length_ext;
        }
      }
      goto exception;
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
          goto exception;
        }
        deque_advance_first_bytes(&self->deque, 1);
        break;
      }
      goto exception;
    case '\xd1':  // int_16
    case '\xcd':  // uint_16
      if A_LIKELY(deque_has_next_n_bytes(&self->deque, 3)) {
        deque_advance_first_bytes(&self->deque, 1);
        READ_A_WORD;
        parsed_object = next_byte == '\xcd' ? PyLong_FromLong((long)word.us)
                                            : PyLong_FromLong((long)word.s);
        if A_UNLIKELY(parsed_object == NULL) {
          goto exception;
        }
        break;
      }
      goto exception;
    case '\xd2':  // int_32
    case '\xce':  // uint_32
      if A_LIKELY(deque_has_next_n_bytes(&self->deque, 5)) {
        deque_advance_first_bytes(&self->deque, 1);
        READ_A_DWORD;
        parsed_object = next_byte == '\xce' ? PyLong_FromUnsignedLong(dword.ul)
                                            : PyLong_FromLong(dword.l);
        if A_UNLIKELY(parsed_object == NULL) {
          goto exception;
        }
        break;
      }
      goto exception;
    case '\xd3':  // int_64
    case '\xcf':  // uint_64
      if A_LIKELY(deque_has_next_n_bytes(&self->deque, 9)) {
        deque_advance_first_bytes(&self->deque, 1);
        READ_A_QWORD;
        parsed_object = next_byte == '\xcf'
                            ? PyLong_FromUnsignedLongLong(qword.ull)
                            : PyLong_FromLongLong(qword.ll);
        if A_UNLIKELY(parsed_object == NULL) {
          goto exception;
        }
        break;
      }
      goto exception;
    case '\xca':  // float (float_32)
      if A_LIKELY(deque_has_next_n_bytes(&self->deque, 5)) {
        deque_advance_first_bytes(&self->deque, 1);
        READ_A_DWORD;
        parsed_object = PyFloat_FromDouble((double)dword.f);
        if A_UNLIKELY(parsed_object == NULL) {
          goto exception;
        }
        break;
      }
      goto exception;
    case '\xcb':  // double (float_64)
      if (deque_has_next_n_bytes(&self->deque, 9)) {
        deque_advance_first_bytes(&self->deque, 1);
        READ_A_QWORD;
        parsed_object = PyFloat_FromDouble(qword.d);
        if (parsed_object == NULL) {
          goto exception;
        }
        break;
      }
      goto exception;
    case '\xd4':  // fixext 1
    case '\xd5':  // fixext 2
    case '\xd6':  // fixext 4
    case '\xd7':  // fixext 8
    case '\xd8':  // fixext 16
      length.ext = (Py_ssize_t)1 << (next_byte - '\xd4');
      if A_LIKELY(deque_has_next_n_bytes(&self->deque, 2 + length.ext)) {
        deque_advance_first_bytes(&self->deque, 1);
        goto length_ext;
      }
      goto exception;
    length_ext: {
      READ_A_DATA(length.ext + 1);
      char const code = data[0];
      Ext* ext = PyObject_New(Ext, self->state->ext_type);
      if A_UNLIKELY(ext == NULL) {
        PyMem_Free(allocated);
        goto exception;  // Allocation failed, likely
      }
      ext->code = code;
      ext->data = PyBytes_FromStringAndSize(data + 1, length.ext);
      if A_UNLIKELY(ext->data == NULL) {
        Py_DECREF(ext);
        PyMem_Free(allocated);
        goto exception;
      }
      PyObject* new_ext;
      if A_LIKELY(self->ext_hook == NULL) {
        new_ext = Ext_default(ext, NULL);
      } else {
        Py_ssize_t const original_length = self->deque.size;
        // move `deque` to `in ext_hook` mode
        self->deque.size = 0;
        new_ext = PyObject_CallOneArg(self->ext_hook, (PyObject*)ext);
        self->deque.size = original_length;
      }
      Py_DECREF(ext);
      parsed_object = (PyObject*)new_ext;
      FREE_A_DATA(length.ext + 1);
      if A_UNLIKELY(parsed_object == NULL) {
        goto exception;  // likely exception in user supplied code
      }
      break;
    }
    case '\xd9':  // str 8
    case '\xda':  // str 16
    case '\xdb':  // str 32
    {
      unsigned char const size_size = 1 << (next_byte - '\xd9');
      if A_LIKELY(deque_has_next_n_bytes(&self->deque, 1 + size_size)) {
        length.str = deque_peek_size(&self->deque, size_size);
        if A_UNLIKELY(length.str > MiB128) {
          return size_error("string", length.str, MiB128);
        }
        if A_LIKELY(deque_has_next_n_bytes(&self->deque,
                                           1 + size_size + length.str)) {
          deque_skip_size(&self->deque, size_size);
          if A_UNLIKELY(length.str == 0) {
            parsed_object = self->state->byte_object[EMPTY_STRING_IDX];
            Py_INCREF(parsed_object);
            break;
          }
          goto length_str;
        }
      }
      goto exception;
    }
    case '\xdc':  // array 16
      if A_LIKELY(deque_has_next_n_bytes(&self->deque, 3)) {
        deque_advance_first_bytes(&self->deque, 1);
        READ_A_WORD;
        length.arr = word.us;
        goto length_arr;
      }
      goto exception;
    case '\xdd':  // array 32
      if A_LIKELY(deque_has_next_n_bytes(&self->deque, 5)) {
        deque_advance_first_bytes(&self->deque, 1);
        READ_A_DWORD;
        length.arr = dword.ul;
        if A_UNLIKELY(length.arr > 10000000) {
          return size_error("list", length.arr, 10000000);
        }
        goto length_arr;
      }
      goto exception;
    case '\xde':  // map 16
      if A_LIKELY(deque_has_next_n_bytes(&self->deque, 3)) {
        deque_advance_first_bytes(&self->deque, 1);
        READ_A_WORD;
        length.map = word.us;
        goto length_map;
      }
      goto exception;
    case '\xdf':  // map 32
      if A_LIKELY(deque_has_next_n_bytes(&self->deque, 5)) {
        deque_advance_first_bytes(&self->deque, 1);
        READ_A_DWORD;
        length.map = dword.ul;
        if A_UNLIKELY(length.map > 100000) {
          return size_error("dict", length.map, 100000);
        }
        goto length_map;
      }
      goto exception;
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
          goto exception;
        }
        item->action = DICT_KEY;
        item->pos += 1;
        if A_UNLIKELY(item->pos == item->size) {
#if HAVE_FROZENDICT
          if A_UNLIKELY(self->flags.readonly) {
            parsed_object = PyFrozenDict_New(item->sequence);
            Py_DECREF(item->sequence);
          } else {
            parsed_object = item->sequence;
          }
#else
          parsed_object = item->sequence;
#endif
          self->parser.stack_length -= 1;
          break;
        }
        if (!deque_has_next_byte(&self->deque)) {
          goto exception;
        }
        parse_a_key = 1;
        next_byte = deque_peek_byte(&self->deque);
        if (next_byte >= '\xa1' && next_byte <= '\xbf') {
          goto fixstr;
        }
        goto parse_next_with_next_byte_set;
      }
      default:             // GCOVR_EXCL_LINE
        Py_UNREACHABLE();  // GCOVR_EXCL_LINE
    }
  }
  return parsed_object;
exception:
  return NULL;
}

static Unpacker* Unpacker_new(PyTypeObject* type, PyObject* args,
                              PyObject* kwargs) {
  static char* keywords[] = {"readonly", "ext_hook", NULL};
  int readonly = 0;
  PyObject* ext_hook = NULL;
  if (!PyArg_ParseTupleAndKeywords(args, kwargs, "|$pO:Unpacker", keywords,
                                   &readonly, &ext_hook)) {
    return NULL;
  }
  if A_UNLIKELY(ext_hook != NULL && Py_TYPE(ext_hook)->tp_call == NULL) {
    PyErr_SetString(PyExc_TypeError, "`ext_hook` must be callable");
    return NULL;
  }

  AMsgPackState* state =
      get_amsgpack_state(((PyHeapTypeObject*)type)->ht_module);
  if A_UNLIKELY(state == NULL) {
    return NULL;
  };

  Unpacker* self = (Unpacker*)type->tp_alloc(type, 0);
  if A_UNLIKELY(self == NULL) {
    return NULL;
  }
  self->state = state;
  if (ext_hook != NULL) {
    self->ext_hook = Py_NewRef(ext_hook);
  }
  self->flags.readonly = readonly;

  return self;
}

static PyObject* unpacker_feed(Unpacker* self, PyObject* obj) {
  if A_UNLIKELY(PyBytes_CheckExact(obj) == 0) {
    PyErr_Format(PyExc_TypeError, "a bytes object is required, not '%.100s'",
                 Py_TYPE(obj)->tp_name);
    return NULL;
  }
  if A_UNLIKELY(self->deque.size == 0 && self->deque.deque_first != NULL) {
    PyErr_SetString(PyExc_RuntimeError, "Must not call `feed` in `ext_hook`");
    return NULL;
  }
  if A_UNLIKELY(deque_append(&self->deque, obj) < 0) {
    return PyErr_NoMemory();
  }
  Py_RETURN_NONE;
}

// Make unpacker ready for the next input.
static void unpacker_reset_(Unpacker* self) {
  deque_clean(&self->deque);
  while (self->parser.stack_length) {
    Stack* item = self->parser.stack + (--self->parser.stack_length);
    Py_DECREF(item->sequence);
    if (item->action != SEQUENCE_APPEND) {
      Py_XDECREF(item->key);
    }
    memset(item, 0, sizeof(Stack));
  }
}

// Always returns None. Accessible with `Unpacker.reset`
static PyObject* unpacker_reset(Unpacker* self, PyObject* Py_UNUSED(unused)) {
  if A_UNLIKELY(self->deque.size == 0 && self->deque.deque_first != NULL) {
    PyErr_SetString(PyExc_RuntimeError,
                    "Can't call `reset` on `Unpacker` while in `ext_hook`");
    return NULL;
  }

  unpacker_reset_(self);
  Py_RETURN_NONE;
}

// `Unpacker.unpackb`
// Expects empty `deque`
static PyObject* unpacker_unpackb(Unpacker* self, PyObject* obj) {
  if A_UNLIKELY(self->deque.deque_first != NULL) {
    PyErr_SetString(
        PyExc_RuntimeError,
        "Can't call `unpackb` on `Unpacker` that has non empty buffer");
    return NULL;
  }
  if A_UNLIKELY(PyBytes_CheckExact(obj) == 0) {
    PyObject* bytes_obj = PyBytes_FromObject(obj);
    if (bytes_obj == NULL) {
      PyErr_Format(PyExc_TypeError,
                   "unpackb() argument 1 must be bytes, not %s",
                   Py_TYPE(obj)->tp_name);
      return NULL;
    }
    obj = bytes_obj;
  } else {
    Py_INCREF(obj);  // so we can safely decref it later in this function
  }
  int const append_result = deque_append(&self->deque, obj);
  Py_DECREF(obj);
  if A_UNLIKELY(append_result < 0) {
    return NULL;
  }
  PyObject* ret = Unpacker_iternext(self);
  if (ret == NULL) {
    if A_UNLIKELY(PyErr_Occurred() == NULL) {
      PyErr_SetString(PyExc_ValueError, "Incomplete MessagePack format");
    }
    goto error;
  }
  if A_UNLIKELY(self->deque.deque_first != NULL) {
    PyErr_SetString(PyExc_ValueError, "Extra data");
    goto error;
  }
  unpacker_reset_(self);
  return ret;
error:
  unpacker_reset_(self);
  return NULL;
}

static void Unpacker_dealloc(Unpacker* self) {
  Py_CLEAR(self->ext_hook);
  unpacker_reset_(self);
  Py_TYPE(self)->tp_free((PyObject*)self);
}

#undef MiB128
#undef ANEW_DICT
#undef READ_A_DATA
#undef FREE_A_DATA
#undef READ_A_WORD
#undef READ_A_DWORD
#undef READ_A_QWORD

/*
  Unpacker class
*/
PyDoc_STRVAR(unpacker_feed_doc,
             "feed($self, bytes, /)\n--\n\n"
             "Append ``bytes`` to internal queue.");
PyDoc_STRVAR(unpacker_unpackb_doc,
             "unpackb($self, data, /)\n--\n\n"
             "Deserialize ``data`` (a ``bytes`` object) to a Python object by "
             "calling '__next__' one time and ensuring there's no more data");
PyDoc_STRVAR(
    unpacker_reset_doc,
    "reset($self, /)\n--\n\n"
    "Cleans up internal queue, that was filled by :meth:`feed` method and "
    "cleans up stack, that might've been filled by :meth:`__next__`");

static PyMethodDef Unpacker_Methods[] = {
    {"feed", (PyCFunction)&unpacker_feed, METH_O, unpacker_feed_doc},
    {"unpackb", (PyCFunction)&unpacker_unpackb, METH_O, unpacker_unpackb_doc},
    {"reset", (PyCFunction)&unpacker_reset, METH_NOARGS, unpacker_reset_doc},
    {NULL, NULL, 0, NULL}  // Sentinel
};

PyDoc_STRVAR(Unpacker_doc,
             "Unpacker(readonly = False, ext_hook = None)\n"
             "--\n\n"
             "Unpack bytes to python objects.\n"
             "\n"
             ":param readonly: tells the :class:`Unpacker` to "
             "output sequences as ``tuple`` instead of ``list``, and if "
             " ``frozendict`` is available use it.\n"
             ":param ext_hook: A callable that takes a single :class:`Ext` "
             "argument and returns the Python object to use in its place. "
             "Default implementation converts `Ext` with code -1 to "
             "`datetime.datetime`` \n"
             "\n"
             "The ``amsgpack.unpackb`` function is created using::\n\n"
             "  unpackb = Unpacker().unpackb\n\n"
             "\n"
             "`ext_hook` example:\n"
             "\n"
             ""
             ">>> from amsgpack import Ext, Unpacker\n"
             ">>> from array import array\n"
             ">>>\n"
             ">>> def ext_hook(ext: Ext):\n"
             "...     if ext.code == 1:\n"
             "...         return array(\"I\", ext.data)\n"
             "...     return ext.default()\n"
             "...\n"
             ">>> Unpacker(ext_hook=ext_hook).unpackb(\n"
             "...     b\"\\xd7\\x01\\xba\\x00\\x00\\x00\\xde\\x00\\x00\\x00\"\n"
             "... )\n"
             "array('I', [186, 222])\n");

BEGIN_NO_PEDANTIC
static PyType_Slot Unpacker_slots[] = {
    {Py_tp_doc, (char*)Unpacker_doc},
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
