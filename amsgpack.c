#include <Python.h>

#include "macros.h"

#define A_STACK_SIZE 32  // common for packer and unpacker
#define EMPTY_TUPLE_IDX 0xc4
#define EMPTY_STRING_IDX 0xa0

// currently we only store fixstr keys in cache, so the size is 15 + 1
#define MAX_CACHE_LEN 16
#define CACHE_TABLE_SIZE (1 << 9)

typedef struct {
  uint32_t hash;
  PyObject* obj;
  uint16_t len;
  uint8_t data[MAX_CACHE_LEN];
} CacheEntry;

typedef struct {
  PyObject* byte_object[256];
  PyTypeObject* ext_type;
  PyTypeObject* raw_type;
  PyTypeObject* unpacker_type;
  PyTypeObject* file_unpacker_type;
  int_fast8_t gc_cycle;
  CacheEntry unicode_cache[CACHE_TABLE_SIZE];
} AMsgPackState;

static inline AMsgPackState* get_amsgpack_state(PyObject* module) {
  void* state = PyModule_GetState(module);
  assert(state != NULL);
  return (AMsgPackState*)state;
}

#include "packb.h"

// used in `Unpacker` and `FileUnpacker`
static PyObject* AnyUnpacker_iter(PyObject* self) {
  Py_INCREF(self);
  return self;
}

#include "unpacker.h"
// include unpacker before file_unpacker
#include "file_unpacker.h"
#define VERSION "0.1.4"

/*
  amsgpack module
*/

PyDoc_STRVAR(amsgpack_packb_doc,
             "packb($module, obj, /)\n--\n\n"
             "Serialize ``obj`` to a MessagePack formatted ``bytes``.");
PyDoc_STRVAR(amsgpack_unpackb_doc,
             "unpackb($module, data, /, *, tuple: bool = True)\n--\n\n"
             "Deserialize ``data`` (a ``bytes`` object) to a Python object.");

static PyMethodDef amsgpack_methods[] = {
    {"packb", (PyCFunction)packb, METH_O, amsgpack_packb_doc},
    {"unpackb", (PyCFunction)(PyCFunction)(void (*)(void))unpackb,
     METH_VARARGS | METH_KEYWORDS, amsgpack_unpackb_doc},
    {NULL, NULL, 0, NULL}  // Sentinel
};

// returns: -1 - failure
//           0 - success
static inline int amsgpack_init_state(AMsgPackState* state) {
  int i = -32;
  for (; i != 128; ++i) {
    PyObject* number = PyLong_FromLong(i);
    if A_UNLIKELY(number == NULL) {
      return -1;
    }
    state->byte_object[(unsigned char)i] = number;
  }
  // fixstr of length 0
  state->byte_object[EMPTY_STRING_IDX] = PyUnicode_FromStringAndSize(NULL, 0);
  if A_UNLIKELY(state->byte_object[EMPTY_STRING_IDX] == NULL) {
    return -1;
  }
  state->byte_object[0xc0] = Py_None;
  Py_INCREF(Py_None);
  state->byte_object[0xc2] = Py_False;
  Py_INCREF(Py_False);
  state->byte_object[0xc3] = Py_True;
  Py_INCREF(Py_True);
  state->byte_object[EMPTY_TUPLE_IDX] = PyTuple_New(0);  // amsgpack specific
  if A_UNLIKELY(state->byte_object[EMPTY_TUPLE_IDX] == NULL) {
    return -1;
  }
  return 0;
}

static int amsgpack_exec(PyObject* module) {
  PyDateTime_IMPORT;
  if (PyDateTimeAPI == NULL) {
    return -1;
  }
  if (PyModule_AddStringConstant(module, "__version__", VERSION) != 0) {
    return -1;
  }
  AMsgPackState* state = get_amsgpack_state(module);
  if (amsgpack_init_state(state) != 0) {
    return -1;
  }
#define ADD_TYPE(TypeName, type_name)                                          \
  state->type_name##_type =                                                    \
      (PyTypeObject*)PyType_FromModuleAndSpec(module, &TypeName##_spec, NULL); \
  if (state->type_name##_type == NULL) {                                       \
    assert(0);                                                                 \
    return -1;                                                                 \
  }                                                                            \
  if (PyModule_AddObjectRef(module, #TypeName,                                 \
                            (PyObject*)state->type_name##_type) < 0) {         \
    return -1;                                                                 \
  }

  ADD_TYPE(Ext, ext);
  ADD_TYPE(Raw, raw);
  ADD_TYPE(Unpacker, unpacker);
  ADD_TYPE(FileUnpacker, file_unpacker);
#undef ADD_TYPE
  return 0;
}

BEGIN_NO_PEDANTIC
static PyModuleDef_Slot amsgpack_slots[] = {
    {Py_mod_exec, (void*)amsgpack_exec},
#ifdef Py_mod_multiple_interpreters
    {Py_mod_multiple_interpreters, Py_MOD_PER_INTERPRETER_GIL_SUPPORTED},
#endif
#ifdef Py_mod_gil
    {Py_mod_gil, Py_MOD_GIL_NOT_USED},
#endif
    {0, NULL}};
END_NO_PEDANTIC

static int amsgpack_traverse(PyObject* module, visitproc Py_UNUSED(visit),
                             void* Py_UNUSED(arg)) {
  AMsgPackState* state = get_amsgpack_state(module);
  state->gc_cycle++;
  // amsgpack_traverse is usually called two times in a row, so:
  if ((state->gc_cycle & 1) == 1) {
    int_fast8_t clear_part = state->gc_cycle / 2;
    if (clear_part > 7) {
      clear_part = state->gc_cycle = 0;
    }
    int const stride_el = CACHE_TABLE_SIZE / 8;
    for (int i = clear_part * stride_el; i < clear_part * stride_el + stride_el;
         ++i) {
      PyObject* obj = state->unicode_cache[i].obj;
      // Technically, another module can hold strings in its cache
      // and we will never clear memory. Do not know what to do about it.
      if (obj != NULL && Py_REFCNT(obj) == 1) {
        Py_DECREF(obj);
        state->unicode_cache[i].obj = NULL;
      }
    }
  }
  return 0;
}

static void amsgpack_free(void* module) {
  AMsgPackState* state = get_amsgpack_state((PyObject*)module);
  for (unsigned int i = 0; i < sizeof((*state).byte_object) / sizeof(PyObject*);
       ++i) {
    Py_XDECREF(state->byte_object[i]);
  }
  Py_XDECREF(state->ext_type);
  Py_XDECREF(state->raw_type);
  Py_XDECREF(state->unpacker_type);
  Py_XDECREF(state->file_unpacker_type);
  for (unsigned int i = 0; i < CACHE_TABLE_SIZE; ++i) {
    Py_XDECREF(state->unicode_cache[i].obj);
  }
}

static struct PyModuleDef amsgpack_module = {.m_base = PyModuleDef_HEAD_INIT,
                                             .m_name = "amsgpack",
                                             .m_doc = NULL,
                                             .m_size = sizeof(AMsgPackState),
                                             .m_methods = amsgpack_methods,
                                             .m_slots = amsgpack_slots,
                                             .m_traverse = amsgpack_traverse,
                                             .m_free = amsgpack_free};

PyMODINIT_FUNC PyInit_amsgpack(void) {
  return PyModuleDef_Init(&amsgpack_module);
}

#ifdef AMSGPACK_FUZZER
static PyObject* import_amsgpack(void) {
  Py_InitializeEx(0);
  PyObject* module = PyImport_ImportModule("amsgpack");
  assert(module != NULL);
  PyModule_ExecDef(module, &amsgpack_module);
  return module;
}

// fuzzer, forgive me
int LLVMFuzzerTestOneInput(uint8_t const* data, size_t size) {
  static PyObject* module = NULL;
  if (module == NULL) {
    module = import_amsgpack();
  }
  PyObject* no_args = PyTuple_New(0);
  PyObject* no_kwargs = PyDict_New();
  AMsgPackState* state = get_amsgpack_state(module);
  Unpacker* unpacker =
      (Unpacker*)Unpacker_new(state->unpacker_type, no_args, no_kwargs);
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
  PyObject* module = import_amsgpack();
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
  Py_DECREF(module);
  int const finalize_ret = Py_FinalizeEx();
  if (finalize_ret != 0) {
    fprintf(stderr, "Py_FinalizeEx returned %d\n", ret);
  }
  return ret;
}
#endif  // AMSGPACK_FUZZER_MAIN
