#include <Python.h>

static PyObject* my_add(PyObject* self, PyObject* args) {
    int a, b;
    if (!PyArg_ParseTuple(args, "ii", &a, &b)) {
        return NULL; // Error parsing arguments
    }
    return PyLong_FromLong(a + b);
}

static PyMethodDef MyMethods[] = {
    {"add", my_add, METH_VARARGS, "Add two numbers"},
    {NULL, NULL, 0, NULL} // Sentinel
};

static struct PyModuleDef mymodule = {
    PyModuleDef_HEAD_INIT,
    "my_module", // Name of the module
    NULL, // Module documentation
    -1, // Size of per-interpreter state of the module
    MyMethods
};

PyMODINIT_FUNC PyInit_my_module(void) {
    return PyModule_Create(&mymodule);
}
