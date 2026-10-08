#include <Python.h>

typedef struct {
  Unpacker unpacker;
  PyObject* read_callback;
  PyObject* read_size;
} FileUnpacker;

static FileUnpacker* FileUnpacker_new(PyTypeObject* type, PyObject* args,
                                      PyObject* kwargs) {
  PyObject* file = NULL;
  PyObject* read_size = NULL;  // default argument to call read with
  if_error (!PyArg_ParseTuple(args, "O|O:FileUnpacker", &file, &read_size)) {
    return NULL;
  }

  PyObject* read_callback = PyObject_GetAttrString(file, "read");
  if_user A_UNLIKELY(read_callback == NULL) {
    return NULL;  // PyObject_GetAttrString already set the exception
  }

  if_user A_UNLIKELY(Py_TYPE(read_callback)->tp_call == NULL) {
    PyErr_Format(PyExc_TypeError, "`%s.read` must be callable",
                 Py_TYPE(file)->tp_name);
    goto error;
  }

  if_user (read_size == NULL) {
    // use default value of -1
    read_size = PyLong_FromLong(-1);
    if_error A_UNLIKELY(read_size == NULL) {
      goto error;
    }
  } else {
    Py_INCREF(read_size);
  }

  PyObject* no_args = PyTuple_New(0);
  if_error A_UNLIKELY(no_args == NULL) {
    goto error;  // GCOVR_EXCL_LINE
  }
  FileUnpacker* self = (FileUnpacker*)Unpacker_new(type, no_args, kwargs);
  Py_DECREF(no_args);
  if_error A_UNLIKELY(self == NULL) {
    goto error;
  }
  self->read_callback = read_callback;
  self->read_size = read_size;
  return self;
error:
  Py_DECREF(read_callback);
  Py_XDECREF(read_size);
  return NULL;
}

static PyObject* FileUnpacker_iternext(FileUnpacker* self) {
  // 1. Try to unpack current data
  {
    PyObject* current = Unpacker_iternext(&self->unpacker);
    if_error A_UNLIKELY(PyErr_Occurred() != NULL) {
      return NULL;
    }
    if_algo (current != NULL) {
      return current;
    }
  }

  PyObject* result = NULL;
  do {
    // 2. Read some bytes
    PyObject* bytes = PyObject_CallOneArg(self->read_callback, self->read_size);
    if_error A_UNLIKELY(bytes == NULL) {
      return NULL;
    }
    if_user A_UNLIKELY(PyBytes_CheckExact(bytes) == 0) {
      PyErr_Format(PyExc_TypeError, "a bytes object is required, not '%.100s'",
                   Py_TYPE(bytes)->tp_name);
      return NULL;
    }
    // 3. Push bytes to the deque
    int const append_result = deque_append(&self->unpacker.deque, bytes);
    Py_DECREF(bytes);
    if_error A_UNLIKELY(append_result != 0) {
      return NULL;
    }

    // 4. Try to iterate
    result = Unpacker_iternext(&self->unpacker);
  } while (result == NULL);

  return result;
}

static void FileUnpacker_dealloc(FileUnpacker* self) {
  Py_XDECREF(self->read_callback);
  Py_XDECREF(self->read_size);
  Unpacker_dealloc(&self->unpacker);
}
PyDoc_STRVAR(
    FileUnpacker_doc,
    "FileUnpacker(file, read_size = -1, readonly = False, ext_hook = None)\n"
    "--\n\n"
    ":param file: `BinaryStream` that has read method\n"
    ":param read_size: argument that is passed to `read_size.read` method\n"
    ":param readonly: see :class:`Unpacker`\n"
    ":param ext_hook: see :class:`Unpacker`\n\n"
    "Iteratively unpack binary stream to python objects:\n\n"
    ">>> from amsgpack import FileUnpacker\n"
    ">>> from io import BytesIO\n"
    ">>> for data in FileUnpacker(BytesIO(b'\\x00\\x01\\x02')):\n"
    "...     print(data)\n"
    "...\n"
    "0\n"
    "1\n"
    "2\n"

);

BEGIN_NO_PEDANTIC
static PyType_Slot FileUnpacker_slots[] = {
    {Py_tp_doc, (char*)FileUnpacker_doc},
    {Py_tp_new, FileUnpacker_new},
    {Py_tp_dealloc, (destructor)FileUnpacker_dealloc},
    {Py_tp_iter, AnyUnpacker_iter},
    {Py_tp_iternext, (iternextfunc)FileUnpacker_iternext},
    {0, NULL}};
END_NO_PEDANTIC

static PyType_Spec FileUnpacker_spec = {
    .name = "amsgpack.FileUnpacker",
    .basicsize = sizeof(FileUnpacker),
    .flags = Py_TPFLAGS_DEFAULT,
    .slots = FileUnpacker_slots,
};
