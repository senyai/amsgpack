#include <Python.h>

#define MIN(x, y) ((x) < (y) ? (x) : (y))
#define MAX(x, y) ((x) > (y) ? (x) : (y))

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
  Py_ssize_t size;         // number of bytes left
  Py_ssize_t pos;          // position in the 'head'
} Deque;

// safe to call, no memory is allocated, and `PyMem_Free` returns void,
// so nothing to check
static inline void deque_pop_first(Deque *deque, Py_ssize_t size_first) {
  assert(deque->deque_first);
  assert(deque->deque_last);
  BytesNode *next = deque->deque_first->next;
  Py_DECREF(deque->deque_first->bytes);
  PyMem_Free(deque->deque_first);
  if (next == NULL) {
    deque->deque_first = deque->deque_last = next;
  } else {
    deque->deque_first = next;
  }
  deque->pos = 0;
  deque->size -= size_first;
}

static inline void deque_clean(Deque *deque) {
  while (deque->deque_first) {
    deque_pop_first(deque, 0);
  }
  deque->size = 0;
}

// returns: -1 - failure
//           0 - success
//           1 - no op, when bytes size is 0
static inline int deque_append(Deque *deque, PyObject *bytes) {
  Py_ssize_t const bytes_size = PyBytes_GET_SIZE(bytes);
  if (bytes_size == 0) {
    return 1;
  }
  BytesNode *new_node = (BytesNode *)PyMem_Malloc(sizeof(BytesNode));
  if (new_node == NULL) {
    return -1;
  }
  Py_INCREF(bytes);
  new_node->bytes = bytes;
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
  deque->size += bytes_size;
  return 0;
}

static inline int deque_has_next_byte(Deque *deque) {
  return deque->pos < deque->size;
}

static inline int deque_has_n_next_byte(Deque *deque, Py_ssize_t size) {
  return deque->pos + size <= deque->size;
}

// deque_read_bytes must
// return NULL, when nothing needs to be freed (PyMem_Free)
// bytes are NULL when memory error occurred
// advances the deque for `requested_size` when non NULL is returned
static char *deque_read_bytes(char const **bytes, Deque *deque,
                              Py_ssize_t const requested_size) {
  assert(*bytes == NULL);
  assert(deque->pos + requested_size <= deque->size);
  assert(requested_size > 0);
  assert(deque->deque_first);
  PyObject *const obj = deque->deque_first->bytes;
  Py_ssize_t size_first = PyBytes_GET_SIZE(obj);
  char const *start = PyBytes_AS_STRING(obj) + deque->pos;
  if ((deque->pos + requested_size) <= size_first) {
    // can get a view
    *bytes = start;
    // when view is returned, the used is responsible for advancing the deque
    return NULL;
  }
  // must return copy
  char *new_mem = (char *)PyMem_Malloc(requested_size);
  if (new_mem == NULL) {
    return NULL;
  }
  Py_ssize_t copy_size = size_first - deque->pos;
  memcpy(new_mem, start, copy_size);
  deque_pop_first(deque, size_first);
  Py_ssize_t left_to_copy = requested_size - copy_size;
  // copy_size = 0;
  assert(left_to_copy > 0);
  for (Py_ssize_t char_idx = copy_size; char_idx < requested_size;) {
    Py_ssize_t iter_size = PyBytes_GET_SIZE(deque->deque_first->bytes);
    char const *iter_data = PyBytes_AS_STRING(deque->deque_first->bytes);
    Py_ssize_t copy_size = MIN(iter_size, left_to_copy);
    memcpy(new_mem + char_idx, iter_data, copy_size);
    left_to_copy -= copy_size;
    if (copy_size == iter_size) {
      deque_pop_first(deque, iter_size);
    } else {
      deque->pos = copy_size;
    }
    char_idx += iter_size;
  }
  *bytes = new_mem;
  return new_mem;
}

static inline char deque_peek_byte(Deque *deque) {
  assert(deque->deque_first);
  PyObject *obj = deque->deque_first->bytes;
  char byte = PyBytes_AS_STRING(obj)[deque->pos];
  return byte;
}

static inline char deque_read_byte(Deque *deque) {
  assert(deque->deque_first);
  assert(deque->deque_last);
  assert(deque->pos < deque->size);
  PyObject *obj = deque->deque_first->bytes;
  char byte = PyBytes_AS_STRING(obj)[deque->pos++];
  Py_ssize_t size_first = PyBytes_GET_SIZE(obj);
  if (size_first == deque->pos) {
    deque_pop_first(deque, size_first);
  }
  return byte;
}

// advance deque, but not more, than the size of the first item
static inline void deque_advance_first_bytes(Deque *deque, Py_ssize_t size) {
  Py_ssize_t size_first = PyBytes_GET_SIZE(deque->deque_first->bytes);
  assert(deque->pos + size <= size_first);
  deque->pos += size;
  if (size_first == deque->pos) {
    deque_pop_first(deque, size_first);
  }
}

static inline Py_ssize_t deque_peek_size(Deque *deque,
                                         Py_ssize_t requested_size) {
  assert(requested_size == 1 || requested_size == 2 || requested_size == 4);
  assert(deque->pos + requested_size <= deque->size);
  assert(requested_size > 0);
  assert(deque->deque_first);
  Py_ssize_t const pos = deque->pos + 1;  // read the size after current byte
  PyObject *const obj = deque->deque_first->bytes;
  Py_ssize_t size_first = PyBytes_GET_SIZE(obj);
  char const *start = PyBytes_AS_STRING(obj) + pos;
  Py_ssize_t ret = 0;
  if ((pos + requested_size) <= size_first) {
    memcpy(&ret, start, requested_size);
  } else {
    Py_ssize_t copy_size = size_first - pos;
    memcpy(&ret, start, copy_size);
    // deque_pop_first(deque, size_first);
    BytesNode *cur = deque->deque_first->next;
    Py_ssize_t left_to_copy = requested_size - copy_size;
    // copy_size = 0;
    assert(left_to_copy > 0);
    for (Py_ssize_t char_idx = copy_size; char_idx < requested_size;) {
      Py_ssize_t iter_size = PyBytes_GET_SIZE(cur);
      char const *iter_data = PyBytes_AS_STRING(cur);
      Py_ssize_t copy_size = MIN(iter_size, left_to_copy);
      memcpy((char *)&ret + char_idx, iter_data, copy_size);
      left_to_copy -= copy_size;
      char_idx += iter_size;
      cur = cur->next;
    }
  }

  Py_ssize_t ret_fixed = ret;
  if (requested_size == 2) {
    ((char *)&ret_fixed)[0] = ((char *)&ret)[1];
    ((char *)&ret_fixed)[1] = ((char *)&ret)[0];
  } else if (requested_size == 4) {
    ((char *)&ret_fixed)[0] = ((char *)&ret)[3];
    ((char *)&ret_fixed)[1] = ((char *)&ret)[2];
    ((char *)&ret_fixed)[2] = ((char *)&ret)[1];
    ((char *)&ret_fixed)[3] = ((char *)&ret)[0];
  }

  return ret_fixed;
}
