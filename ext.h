#pragma once
#include <Python.h>
#include <structmember.h>  // PyMemberDef

#include "timestamp.h"

typedef struct {
  PyObject_HEAD
  char code;
  PyObject *data;
} Ext;

static PyMemberDef Ext_members[] = {
    {"code", T_BYTE, offsetof(Ext, code), READONLY, "Ext code (int)"},
    {"data", T_OBJECT_EX, offsetof(Ext, data), READONLY, "Ext data (bytes)"},
    {NULL, 0, 0, 0, NULL}  // Sentinel
};

static int Ext_init(Ext *self, PyObject *args, PyObject *kwargs) {
  int code = 0;
  PyObject *bytes = NULL;
  static char *kwlist[] = {"code", "data", NULL};
  if A_UNLIKELY(!PyArg_ParseTupleAndKeywords(args, kwargs, "iO:Ext", kwlist,
                                             &code, &bytes)) {
    return -1;
  }
  if A_UNLIKELY(PyBytes_CheckExact(bytes) == 0) {
    PyErr_Format(PyExc_TypeError, "a bytes object is required, not '%.100s'",
                 Py_TYPE(bytes)->tp_name);
    return -1;
  }
  if A_UNLIKELY(code < -128 || code > 127) {
    PyErr_SetString(PyExc_ValueError, "`code` must be between -128 and 127");
    return -1;
  }
  self->code = (char)code;
  Py_INCREF(bytes);
  self->data = bytes;
  return 0;
}

static void Ext_dealloc(Ext *self) {
  Py_XDECREF(self->data);
  Py_TYPE(self)->tp_free((PyObject *)self);
}

static Py_hash_t Ext_hash(Ext *self) {
  Py_hash_t const code_hash = (Py_hash_t)self->code;
  Py_hash_t const data_hash = PyObject_Hash(self->data);
  Py_hash_t const hash =
      code_hash ^ (data_hash + 0x9e3779b9 + ((Py_uhash_t)code_hash << 6) +
                   (code_hash >> 2));

  // Special case: if final hash is -1, return -2 instead
  // because -1 indicates error in Python's hash functions
  return hash == -1 ? -2 : hash;
}

static PyObject *Ext_repr(Ext *self) {
  return PyUnicode_FromFormat("Ext(code=%d, data=%R)", self->code, self->data);
}

static inline int parse_timestamp(MsgPackTimestamp *timestamp, char const *data,
                                  Py_ssize_t data_length) {
  if (data_length == 8) {
    A_QWORD const timestamp64 = read_a_qword(data);
    timestamp->seconds = timestamp64.ull & 0x3ffffffff;  // 34 bits
    timestamp->nanosec = timestamp64.ull >> 34;          // 30 bits
  } else if (data_length == 4) {
    A_DWORD const timestamp32 = read_a_dword(data);
    timestamp->seconds = (int64_t)timestamp32.ul;
    timestamp->nanosec = 0;
  } else if (data_length == 12) {
    TIMESTAMP96 const timestamp96 = {
        .bytes = {data[11], data[10], data[9], data[8], data[7], data[6],
                  data[5], data[4], data[3], data[2], data[1], data[0]}};
    timestamp->seconds = timestamp96.seconds;
    timestamp->nanosec = timestamp96.nanosec;
  } else {
    PyErr_Format(PyExc_ValueError,
                 "Invalid timestamp length %zd, allowed values are 4, 8 and 12 "
                 "(see MessagePack specification)",
                 data_length);
    return -1;
  }
  return 0;
}

/*
  ``Ext.to_timestamp`` method, should raise exception,
  when the size is incorrect
*/
static PyObject *Ext_to_timestamp(Ext *self, PyObject *Py_UNUSED(unused)) {
  PyObject *module = ((PyHeapTypeObject *)Py_TYPE(self))->ht_module;
  assert(module != NULL);
  AMsgPackState *state = get_amsgpack_state(module);
  Timestamp *timestamp = PyObject_New(Timestamp, state->timestamp_type);
  if A_UNLIKELY(timestamp == NULL) {
    return NULL;  // GCOVR_EXCL_LINE
  }
  if A_UNLIKELY(parse_timestamp(&timestamp->timestamp,
                                PyBytes_AS_STRING(self->data),
                                PyBytes_GET_SIZE(self->data)) != 0) {
    Py_DECREF(timestamp);
    return NULL;  // exception is set in `parse_timestamp`
  }
  return (PyObject *)timestamp;
}

static PyObject *Ext_to_datetime(Ext const *self, PyObject *Py_UNUSED(unused)) {
  MsgPackTimestamp ts;
  if (parse_timestamp(&ts, PyBytes_AS_STRING(self->data),
                      PyBytes_GET_SIZE(self->data)) != 0) {
    return NULL;
  }
  return timestamp_to_datetime(ts);
}

/*
  Default behaviour is to return `Ext`, unless the type is timestamp,
  then return `datetime.datetime`
*/
static PyObject *Ext_default(Ext *self, PyObject *Py_UNUSED(obj)) {
  if (self->code == -1) {
    Py_ssize_t const length = PyBytes_GET_SIZE(self->data);
    if A_LIKELY(length == 8 || length == 4 || length == 12) {
      return Ext_to_datetime(self, NULL);
    }
  }
  Py_INCREF(self);
  return (PyObject *)self;
}

static PyObject *Ext_is_timestamp(Ext *self, PyObject *Py_UNUSED(obj)) {
  if (self->code != -1) {
    Py_RETURN_FALSE;
  }
  Py_ssize_t const length = PyBytes_GET_SIZE(self->data);
  if (length == 8 || length == 4 || length == 12) {
    Py_RETURN_TRUE;
  }
  Py_RETURN_FALSE;
}

static PyObject *Ext_richcompare(Ext *self, PyObject *other, int op) {
  // Only implement equality comparison
  if (op != Py_EQ && op != Py_NE) {
    Py_RETURN_NOTIMPLEMENTED;
  }

  if (!Py_IS_TYPE(other, Py_TYPE(self))) {
    PyErr_Format(PyExc_TypeError,
                 "other argument must be amsgpack.Ext instance");
    return NULL;
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
  PyObject *result = NULL;

  // Determine result based on comparison operator
  if (op == Py_EQ) {
    result = data_cmp ? Py_True : Py_False;
  } else {  // Py_NE
    result = data_cmp ? Py_False : Py_True;
  }

  Py_INCREF(result);
  return result;
}

PyDoc_STRVAR(Ext_to_timestamp_doc,
             "to_timestamp($self, /)\n--\n\n"
             "Converts ``data`` to ``amsgpack.Timestamp``");
PyDoc_STRVAR(Ext_to_datetime_doc,
             "to_datetime($self, /)\n--\n\n"
             "Converts ``data`` to ``datetime.datetime``");
PyDoc_STRVAR(
    Ext_default_doc,
    "default($self, /)\n--\n\n"
    "Returns ``datetime.datetime`` object, when :attr:`code` equals -1 and "
    ":attr:`data` size is equal 4, 8 or 12");
PyDoc_STRVAR(Ext_is_timestamp_doc,
             "is_timestamp($self, /)\n--\n\n"
             "Returns ``True`` when :attr:`code` equals -1 and "
             ":attr:`data`'s size is 4, 8 or 12");

static PyMethodDef Ext_methods[] = {
    {"to_timestamp", (PyCFunction)Ext_to_timestamp, METH_NOARGS,
     Ext_to_timestamp_doc},
    {"to_datetime", (PyCFunction)Ext_to_datetime, METH_NOARGS,
     Ext_to_datetime_doc},
    {"default", (PyCFunction)Ext_default, METH_NOARGS, Ext_default_doc},
    {"is_timestamp", (PyCFunction)Ext_is_timestamp, METH_NOARGS,
     Ext_is_timestamp_doc},
    {NULL, NULL, 0, NULL}  // Sentinel
};

BEGIN_NO_PEDANTIC
static PyType_Slot Ext_slots[] = {
    {Py_tp_doc, PyDoc_STR("Ext type from MessagePack specification")},
    {Py_tp_new, PyType_GenericNew},
    {Py_tp_dealloc, Ext_dealloc},
    {Py_tp_init, Ext_init},
    {Py_tp_repr, Ext_repr},
    {Py_tp_members, Ext_members},
    {Py_tp_methods, Ext_methods},
    {Py_tp_hash, Ext_hash},
    {Py_tp_richcompare, Ext_richcompare},
    {Py_tp_new, PyType_GenericNew},
    {0, NULL}};
END_NO_PEDANTIC

static PyType_Spec Ext_spec = {
    .name = "amsgpack.Ext",
    .basicsize = sizeof(Ext),
    .flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_IMMUTABLETYPE,
    .slots = Ext_slots,
};
