#include <Python.h>

#include "macros.h"

/*
 double-ended queue
*/

typedef struct BytesNode {
  PyObject *bytes;
  struct BytesNode *next;
} BytesNode;

typedef struct {
  BytesNode *deque_first;  // head
  char *deque_bytes;       // head's bytes
  Py_ssize_t size_first;   // head's size
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
    // deque->deque_bytes = NULL;
    // deque->size_first = 0;
  } else {
    deque->deque_first = next;
    deque->deque_bytes = PyBytes_AS_STRING(next->bytes);
    deque->size_first = PyBytes_GET_SIZE(next->bytes);
  }
  deque->pos = 0;
  deque->size -= size_first;
}

static inline void deque_clean(Deque *deque) {
  while (deque->deque_first) {
    deque_pop_first(deque, 0);
  }
  deque->size = 0;
  deque->deque_bytes = NULL;
  deque->size_first = 0;
}

// returns: -1 - failure
//           0 - success
//           1 - no op, when bytes size is 0
static inline int deque_append(Deque *deque, PyObject *bytes) {
  Py_ssize_t const bytes_size = PyBytes_GET_SIZE(bytes);
  if A_UNLIKELY(bytes_size == 0) {
    return 1;
  }
  BytesNode *new_node = (BytesNode *)PyMem_Malloc(sizeof(BytesNode));
  if A_UNLIKELY(new_node == NULL) {
    return -1;
  }
  Py_INCREF(bytes);
  new_node->bytes = bytes;
  new_node->next = NULL;
  if (deque->deque_first == NULL) {
    // deque init
    assert(deque->deque_last == NULL);
    deque->deque_first = deque->deque_last = new_node;
    deque->deque_bytes = PyBytes_AS_STRING(bytes);
    deque->size_first = bytes_size;
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

static inline int deque_has_n_next_byte(Deque const *deque, Py_ssize_t size) {
  return deque->pos + size <= deque->size;
}

// returns non null pointer to data when data is available in de  deque's head
static inline char *deque_read_bytes_fast(Deque *deque,
                                          Py_ssize_t const requested_size) {
  if ((deque->pos + requested_size) <= deque->size_first) {
    return deque->deque_bytes + deque->pos;
  }
  return NULL;
}

// deque_read_bytes must
// returned needs to be freed (PyMem_Free)
// advances the deque
// only needs to be called when data is not in deque head
static char *deque_read_bytes(Deque *deque, Py_ssize_t const requested_size) {
  assert(deque->pos + requested_size <= deque->size);
  assert(requested_size > 0);
  assert(deque->deque_first);
  char *new_mem = (char *)PyMem_Malloc(requested_size);
  if A_UNLIKELY(new_mem == NULL) {
    return NULL;
  }
  char const *start = deque->deque_bytes + deque->pos;
  Py_ssize_t const size_first = deque->size_first;
  Py_ssize_t const copy_size = size_first - deque->pos;
  memcpy(new_mem, start, copy_size);
  deque_pop_first(deque, size_first);
  Py_ssize_t left_to_copy = requested_size - copy_size;
  assert(left_to_copy > 0);
  for (Py_ssize_t char_idx = copy_size; char_idx < requested_size;) {
    Py_ssize_t const iter_size = PyBytes_GET_SIZE(deque->deque_first->bytes);
    char const *iter_data = PyBytes_AS_STRING(deque->deque_first->bytes);
    Py_ssize_t copy_size = Py_MIN(iter_size, left_to_copy);
    memcpy(new_mem + char_idx, iter_data, copy_size);
    left_to_copy -= copy_size;
    if (copy_size == iter_size) {
      deque_pop_first(deque, iter_size);
    } else {
      deque->pos = copy_size;
    }
    char_idx += iter_size;
  }
  return new_mem;
}

static inline char deque_peek_byte(Deque const *deque) {
  assert(deque->deque_bytes);
  return deque->deque_bytes[deque->pos];
}

static inline char deque_read_byte(Deque *deque) {
  assert(deque->deque_first);
  assert(deque->deque_last);
  assert(deque->pos < deque->size);
  char const byte = deque->deque_bytes[deque->pos++];
  Py_ssize_t const size_first = deque->size_first;
  if A_UNLIKELY(size_first == deque->pos) {
    deque_pop_first(deque, size_first);
  }
  return byte;
}

// advance deque, but not more, than the size of the first item
// should only be used for when `size` was obtained with `deque_read_bytes_fast`
static inline void deque_advance_first_bytes(Deque *deque, Py_ssize_t size) {
  Py_ssize_t const size_first = deque->size_first;
  assert(deque->pos + size <= size_first);
  deque->pos += size;
  if A_UNLIKELY(size_first == deque->pos) {
    deque_pop_first(deque, size_first);
  }
}

static inline Py_ssize_t deque_peek_size(Deque const *deque,
                                         Py_ssize_t requested_size) {
  assert(requested_size == 1 || requested_size == 2 || requested_size == 4);
  assert(deque->pos + requested_size <= deque->size);
  assert(requested_size > 0);
  assert(deque->deque_first);
  Py_ssize_t const pos = deque->pos + 1;  // read the size after current byte
  Py_ssize_t const size_first = deque->size_first;
  char const *start;
  char ret[4] = {0, 0, 0, 0};
  if A_LIKELY((pos + requested_size) <= size_first) {
    start = deque->deque_bytes + pos;
  } else {
    Py_ssize_t copy_size = size_first - pos;
    start = ret;
    memcpy(&ret, start, copy_size);
    BytesNode *cur = deque->deque_first->next;
    Py_ssize_t left_to_copy = requested_size - copy_size;
    assert(left_to_copy > 0);
    for (Py_ssize_t char_idx = copy_size; char_idx < requested_size;) {
      Py_ssize_t const iter_size = PyBytes_GET_SIZE(cur->bytes);
      char const *iter_data = PyBytes_AS_STRING(cur->bytes);
      copy_size = Py_MIN(iter_size, left_to_copy);
      memcpy(ret + char_idx, iter_data, copy_size);
      left_to_copy -= copy_size;
      char_idx += iter_size;
      cur = cur->next;
    }
  }

  Py_ssize_t ret_fixed = 0;
  switch (requested_size) {
    case 1:
      ((char *)&ret_fixed)[0] = start[0];
      break;
    case 2:
      ((char *)&ret_fixed)[0] = start[1];
      ((char *)&ret_fixed)[1] = start[0];
      break;
    case 4:
      ((char *)&ret_fixed)[0] = start[3];
      ((char *)&ret_fixed)[1] = start[2];
      ((char *)&ret_fixed)[2] = start[1];
      ((char *)&ret_fixed)[3] = start[0];
      break;
    default:
      Py_UNREACHABLE();  // GCOVR_EXCL_LINE
  }
  return ret_fixed;
}

// should only be used after `deque_peek_size`
// skips size + 1
static inline void deque_skip_size(Deque *deque, Py_ssize_t size) {
  assert(size == 1 || size == 2 || size == 4);
  assert(deque->pos + size <= deque->size);
  assert(size > 0);
  assert(deque->deque_first);

  Py_ssize_t const pos = deque->pos + 1;  // read the size after current byte
  Py_ssize_t const size_first = deque->size_first;
  if A_LIKELY((pos + size + 1) <= size_first) {
    deque->pos += size + 1;
    if A_UNLIKELY(size_first == deque->pos) {
      deque_pop_first(deque, size_first);
    }
    return;
  }
  Py_ssize_t skip_size = size_first - deque->pos;
  deque_pop_first(deque, size_first);
  Py_ssize_t left_to_skip = size + 1 - skip_size;
  for (Py_ssize_t char_idx = skip_size; char_idx <= size;) {
    Py_ssize_t const iter_size = PyBytes_GET_SIZE(deque->deque_first->bytes);
    skip_size = Py_MIN(iter_size, left_to_skip);
    left_to_skip -= skip_size;
    if (skip_size == iter_size) {
      deque_pop_first(deque, iter_size);
    } else {
      deque->pos = skip_size;
    }
    char_idx += iter_size;
  }
}
