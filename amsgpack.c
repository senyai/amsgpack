#include <Python.h>

#include "macros.h"

#define A_STACK_SIZE 32
#define EMPTY_TUPLE_IDX 0xc4
#define EMPTY_STRING_IDX 0xa0
static PyObject* msgpack_byte_object[256];

#include "packb.h"
#include "unpacker.h"
#define VERSION "0.1.4"

static PyObject* AnyUnpacker_iter(PyObject* self) {
  Py_INCREF(self);
  return self;
}

/*
  Unpacker class
*/
PyDoc_STRVAR(unpacker_feed_doc,
             "feed($self, bytes, /)\n--\n\n"
             "Append ``bytes`` to internal buffer.");

static PyMethodDef Unpacker_Methods[] = {
    {"feed", (PyCFunction)&unpacker_feed, METH_O, unpacker_feed_doc},
    {NULL, NULL, 0, NULL}  // Sentinel
};

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

/*
  FileUnpacker class
*/

#include "file_unpacker.h"

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

/*
  amsgpack module
*/

/* this is here, because it requires Unpacker_Type */
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

static struct PyModuleDef amsgpack_module = {.m_base = PyModuleDef_HEAD_INIT,
                                             .m_name = "amsgpack",
                                             .m_doc = NULL,
                                             .m_size = -1,
                                             .m_methods = AMsgPackMethods};

/*
  amsgpack initialization
*/

// returns: -1 - failure
//           0 - success
static inline int init_msgpack_byte_object(void) {
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

#ifdef AMSGPACK_FUZZER
static PyObject* python_initialization(void) {
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
