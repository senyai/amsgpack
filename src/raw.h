#include <Python.h>
#include <structmember.h>

typedef struct {
  PyObject_HEAD
  PyObject *data;  // store PyBytes only
} Raw;

static PyMemberDef Raw_members[] = {
    {"data", T_OBJECT_EX, offsetof(Raw, data), READONLY, "Raw data (bytes)"},
    {NULL, 0, 0, 0, NULL}  // Sentinel
};

static int Raw_init(Raw *self, PyObject *args, PyObject *kwargs) {
  static char *kwlist[] = {"data", NULL};
  if (!PyArg_ParseTupleAndKeywords(args, kwargs, "O!:Raw", kwlist,
                                   &PyBytes_Type, &self->data)) {
    return -1;
  }
  Py_INCREF(self->data);
  return 0;
}

static void Raw_dealloc(Raw *self) {
  Py_XDECREF(self->data);
  Py_TYPE(self)->tp_free((PyObject *)self);
}

static Py_hash_t Raw_hash(Raw *self) { return PyObject_Hash(self->data); }

static PyObject *Raw_repr(Raw *self) {
  return PyUnicode_FromFormat("Raw(data=%R)", self->data);
}

static PyObject *Raw_richcompare(Raw *self, PyObject *other, int op) {
  return PyObject_RichCompare(self->data, other, op);
}

PyDoc_STRVAR(
    Raw_doc,
    "Raw(data)\n"
    "--\n\n"
    "Raw type for :func:`packb`. When packer sees :class:`Raw` type, it "
    "inserts its :attr:`data` as is.\n"
    "\n"
    ">>> from amsgpack import Raw, packb\n"
    ">>> packb(Raw(b'Hello'))\n"
    "b'Hello'\n");

BEGIN_NO_PEDANTIC
static PyType_Slot Raw_slots[] = {
    {Py_tp_doc, (char *)Raw_doc},
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
