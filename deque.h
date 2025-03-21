#include <Python.h>

/*
 double-ended queue
*/

typedef struct BytesNode {
  PyObject *bytes;
  struct BytesNode *next;
} BytesNode;

typedef struct {
  BytesNode *deque_first;  // head
  BytesNode *deque_last;   // tail
  Py_ssize_t size;
  Py_ssize_t pos;
} Deque;

static inline void deque_pop_first(Deque *deque) {
  assert(deque->deque_first);
  assert(deque->deque_last);
  BytesNode *next = deque->deque_first->next;
  PyMem_Free(deque->deque_first);
  if (deque->deque_first == deque->deque_last) {
    PyMem_Free(deque->deque_first);
    assert(next == NULL);
    deque->deque_first = deque->deque_last = next;
  } else {
    assert(next != NULL);
    deque->deque_first = next;
  }
}

static inline BytesNode *deque_append(Deque *deque, PyObject *obj) {
  BytesNode *new_node = (BytesNode *)PyMem_Malloc(sizeof(BytesNode));
  if (new_node == NULL) {
    return NULL;
  }
  new_node->bytes = obj;
  new_node->next = NULL;
  if (deque->deque_first == NULL) {
    // deque init
    assert(deque->deque_last == NULL);
    deque->deque_first = deque->deque_last = new_node;
  } else {
    // deque append
    deque->deque_last->next = new_node;
    deque->deque_last = new_node;
  }
  deque->size += PyBytes_GET_SIZE(obj);
  return new_node;
}

static inline int deque_has_next_byte(Deque *deque) {
  return deque->pos < deque->size;
}

static inline int deque_has_n_next_byte(Deque *deque, Py_ssize_t size) {
  return deque->pos + size <= deque->size;
}

// deque_read_bytes must
// return NULL, when nothing needs to be freed
// bytes are NULL when memory error occured
static char *deque_read_bytes(char **bytes, Deque *deque, Py_ssize_t size) {
  assert(deque->pos + size <= deque->size);
  PyObject *obj = deque->deque_first->bytes;
  Py_ssize_t size_first = PyBytes_GET_SIZE(obj);
  if (size_first <= (deque->pos + size)) {
    // can get a view
    *bytes = PyBytes_AS_STRING(obj) + deque->pos;
    deque->pos += size;
    if (deque->pos == size) {
      deque_pop_first(deque);
    }
    return NULL;
  }
  return NULL;
}

static inline char deque_read_byte(Deque *deque) {
  PyObject *obj = deque->deque_first->bytes;
  return PyBytes_AS_STRING(obj)[deque->pos++];
}

static inline void deque_advance(Deque *deque, Py_ssize_t size) {
  deque->pos += size;
}
