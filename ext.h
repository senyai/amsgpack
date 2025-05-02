#include <Python.h>
#include <structmember.h>

typedef struct {
  PyObject_HEAD;
  char code;
  PyObject *data;
} Ext;

static PyMemberDef Ext_members[] = {
    {"code", T_BYTE, offsetof(Ext, code), READONLY, "Ext code"},
    {"data", T_OBJECT_EX, offsetof(Ext, data), READONLY, "Ext data"},
    {NULL, 0, 0, 0, NULL}  // Sentinel
};

static int Ext_init(Ext *self, PyObject *args, PyObject *kwargs) {
  char code = 0;
  PyObject *bytes = NULL;
  static char *kwlist[] = {"code", "data", NULL};
  if A_UNLIKELY(!PyArg_ParseTupleAndKeywords(args, kwargs, "bO", kwlist, &code,
                                             &bytes)) {
    return -1;
  }
  if A_UNLIKELY(PyBytes_CheckExact(bytes) == 0) {
    PyErr_Format(PyExc_TypeError, "a bytes object is required, not '%.100s'",
                 Py_TYPE(bytes)->tp_name);
    return -1;
  }
  self->code = code;
  Py_INCREF(bytes);
  self->data = bytes;
  return 0;
}

static Py_hash_t Ext_hash(Ext *self) {
  Py_hash_t const code_hash = (Py_hash_t)self->code;
  Py_hash_t const data_hash = PyObject_Hash(self->data);
  Py_hash_t const hash = code_hash ^ (data_hash + 0x9e3779b9 +
                                      (code_hash << 6) + (code_hash >> 2));

  // Special case: if final hash is -1, return -2 instead
  // because -1 indicates error in Python's hash functions
  return hash == -1 ? -2 : hash;
}

static PyObject *Ext_repr(Ext *self) {
  return PyUnicode_FromFormat("Ext(code=%d, data=%S)", self->code,
                              PyBytes_Type.tp_repr(self->data));
}

static PyObject *Ext_richcompare(Ext *self, PyObject *other, int op);

static PyTypeObject Ext_Type = {
    PyVarObject_HEAD_INIT(NULL, 0).tp_name = "amsgpack.Ext",
    .tp_basicsize = sizeof(Ext),
    .tp_doc = PyDoc_STR("ext type from msgpack specification"),
    .tp_new = PyType_GenericNew,
    .tp_init = (initproc)Ext_init,
    .tp_flags = Py_TPFLAGS_DEFAULT,
    .tp_repr = (reprfunc)Ext_repr,
    .tp_members = Ext_members,
    .tp_hash = (hashfunc)Ext_hash,
    .tp_richcompare = (richcmpfunc)Ext_richcompare,
};

static PyObject *Ext_richcompare(Ext *self, PyObject *other, int op) {
  PyObject *result = NULL;

  // Only implement equality comparison
  if (op != Py_EQ && op != Py_NE) {
    Py_RETURN_NOTIMPLEMENTED;
  }

  // Check if 'other' is the same type
  if (!Py_IS_TYPE(other, &Ext_Type)) {
    if (op == Py_EQ) {
      Py_RETURN_FALSE;
    } else {
      Py_RETURN_TRUE;
    }
  }

  Ext *other_ext = (Ext *)other;

  // Compare the 'code' fields
  if (self->code != other_ext->code) {
    if (op == Py_EQ) {
      Py_RETURN_FALSE;
    } else {
      Py_RETURN_TRUE;
    }
  }

  // Compare the 'data' fields using Python's equality
  int data_cmp = PyObject_RichCompareBool(self->data, other_ext->data, Py_EQ);
  if A_UNLIKELY(data_cmp == -1) {
    // Exception occurred. It's not possible to reach this code, as
    // `self->data` and `other_ext->data` are of the same type
    return NULL;  // GCOVR_EXCL_LINE
  }

  // Determine result based on comparison operator
  if (op == Py_EQ) {
    result = data_cmp ? Py_True : Py_False;
  } else {  // Py_NE
    result = data_cmp ? Py_False : Py_True;
  }

  Py_INCREF(result);
  return result;
}
