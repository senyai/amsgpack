#include <Python.h>
#include <structmember.h>

typedef struct {
  PyObject_HEAD
  PyObject *data;  // store PyBytes only
} Raw;

static PyMemberDef Raw_members[] = {
    {"data", T_OBJECT_EX, offsetof(Raw, data), READONLY, "Raw data"},
    {NULL, 0, 0, 0, NULL}  // Sentinel
};

static int Raw_init(Raw *self, PyObject *args, PyObject *kwargs) {
  PyObject *bytes = NULL;
  static char *kwlist[] = {"data", NULL};
  if (!PyArg_ParseTupleAndKeywords(args, kwargs, "O", kwlist, &bytes)) {
    return -1;
  }
  if A_UNLIKELY(PyBytes_CheckExact(bytes) == 0) {
    PyErr_Format(PyExc_TypeError, "a bytes object is required, not '%.100s'",
                 Py_TYPE(bytes)->tp_name);
    return -1;
  }
  Py_INCREF(bytes);
  self->data = bytes;
  return 0;
}

static void Raw_dealloc(Raw *self) {
  Py_XDECREF(self->data);
  Py_TYPE(self)->tp_free((PyObject *)self);
}

static Py_hash_t Raw_hash(Raw *self) { return PyObject_Hash(self->data); }

static PyObject *Raw_repr(Raw *self) {
  return PyUnicode_FromFormat("Raw(data=%S)", PyBytes_Type.tp_repr(self->data));
}

static PyObject *Raw_richcompare(Raw *self, PyObject *other, int op) {
  return PyObject_RichCompare(self->data, other, op);
}

BEGIN_NO_PEDANTIC
static PyType_Slot Raw_slots[] = {
    {Py_tp_doc, PyDoc_STR("Raw type for packb")},
    {Py_tp_new, PyType_GenericNew},
    {Py_tp_dealloc, (destructor)Raw_dealloc},
    {Py_tp_init, (initproc)Raw_init},
    {Py_tp_repr, (reprfunc)Raw_repr},
    {Py_tp_members, Raw_members},
    {Py_tp_hash, (hashfunc)Raw_hash},
    {Py_tp_richcompare, (richcmpfunc)Raw_richcompare},

    {0, NULL}};
END_NO_PEDANTIC

static PyType_Spec Raw_spec = {
    .name = "amsgpack.Raw",
    .basicsize = sizeof(Raw),
    .flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_IMMUTABLETYPE,
    .slots = Raw_slots,
};
