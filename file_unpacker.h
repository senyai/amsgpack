#include <Python.h>

typedef struct {
  Unpacker unpacker;
  PyObject* read_callback;
  PyObject* read_size;
} FileUnpacker;

static int FileUnpacker_init(FileUnpacker* self, PyObject* args,
                             PyObject* kwargs) {
  PyObject* no_args = PyTuple_New(0);
  PyObject* file = NULL;
  PyObject* read_size = NULL;
  if (!PyArg_ParseTuple(args, "O|O:FileUnpacker", &file, &read_size)) {
    return -1;
  }

  PyObject* read_callback = PyObject_GetAttrString(file, "read");
  if A_UNLIKELY(read_callback == NULL) {
    return -1;
  }

  if A_UNLIKELY(Py_TYPE(read_callback)->tp_call == NULL) {
    Py_DECREF(read_callback);
    PyErr_Format(PyExc_TypeError, "`%s.read` must be callable",
                 Py_TYPE(file)->tp_name);
    return -1;
  }

  if A_UNLIKELY(no_args == NULL) {
    return -1;  // GCOVR_EXCL_LINE
  }
  if (Unpacker_init(&self->unpacker, no_args, kwargs) != 0) {
    return -1;
  }
  Py_DECREF(no_args);
  self->read_callback = read_callback;
  Py_XINCREF(read_size);
  self->read_size = read_size;
  return 0;
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

BEGIN_NO_PEDANTIC
static PyType_Slot FileUnpacker_slots[] = {
    {Py_tp_doc,
     PyDoc_STR("Iteratively unpack binary stream to python objects")},
    {Py_tp_new, PyType_GenericNew},
    {Py_tp_init, FileUnpacker_init},
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
