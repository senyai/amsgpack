#include <Python.h>

typedef struct {
  Unpacker unpacker;
  PyObject* read_callback;
  PyObject* read_size;
} FileUnpacker;

static PyObject* FileUnpacker_new(PyTypeObject* type, PyObject* args,
                                  PyObject* kwargs) {
  PyObject* file = NULL;
  PyObject* read_size = NULL;
  if (!PyArg_ParseTuple(args, "O|O:FileUnpacker", &file, &read_size)) {
    return NULL;
  }

  PyObject* read_callback = PyObject_GetAttrString(file, "read");
  if A_UNLIKELY(read_callback == NULL) {
    return NULL;
  }

  if A_UNLIKELY(Py_TYPE(read_callback)->tp_call == NULL) {
    Py_DECREF(read_callback);
    PyObject* errorMessage = PyUnicode_FromFormat("`%s.read` must be callable",
                                                  Py_TYPE(file)->tp_name);
    PyErr_SetObject(PyExc_TypeError, errorMessage);
    Py_XDECREF(errorMessage);
    return NULL;
  }

  PyObject* no_args = msgpack_byte_object[EMPTY_TUPLE_IDX];
  FileUnpacker* self = (FileUnpacker*)Unpacker_new(type, no_args, kwargs);
  if A_UNLIKELY(self == NULL) {
    return NULL;
  }
  self->read_callback = read_callback;
  Py_XINCREF(read_size);
  self->read_size = read_size;
  return (PyObject*)self;
}

static PyObject* FileUnpacker_iternext(FileUnpacker* self) {
  // 1. Try to unpack current data
  {
    PyObject* current = Unpacker_iternext(&self->unpacker);
    if A_UNLIKELY(PyErr_Occurred() != NULL) {
      return NULL;
    }
    if (current != NULL) {
      return current;
    }
  }

  // 2. Read some bytes
  PyObject* bytes = self->read_size ? PyObject_CallOneArg(self->read_callback,
                                                          self->read_size)
                                    : PyObject_CallNoArgs(self->read_callback);
  if A_UNLIKELY(bytes == NULL) {
    return NULL;
  }
  if A_UNLIKELY(PyBytes_CheckExact(bytes) == 0) {
    PyErr_Format(PyExc_TypeError, "a bytes object is required, not '%.100s'",
                 Py_TYPE(bytes)->tp_name);
    return NULL;
  }
  // 3. Push bytes to the deque
  int const append_result = deque_append(&self->unpacker.deque, bytes);
  Py_DECREF(bytes);
  if A_UNLIKELY(append_result != 0) {
    return NULL;
  }

  // 4. Try to iterate
  return Unpacker_iternext(&self->unpacker);
}

static void FileUnpacker_dealloc(FileUnpacker* self) {
  Py_XDECREF(self->read_callback);
  Py_XDECREF(self->read_size);
  Unpacker_dealloc(&self->unpacker);
}
