#ifndef A_INCLUDE_COMMON_H
#define A_INCLUDE_COMMON_H
#include <stdint.h>

/*
  A_WORD
*/
typedef union A_WORD {
  int16_t s;
  uint16_t us;
  char bytes[2];
} A_WORD;
typedef char check_word_size[sizeof(A_WORD) == 2 ? 1 : -1];

static inline A_WORD read_a_word(char const* data) {
  return (A_WORD){.bytes = {data[1], data[0]}};
}

#define READ_A_WORD           \
  A_WORD word;                \
  do {                        \
    READ_A_DATA(2);           \
    word = read_a_word(data); \
    FREE_A_DATA(2);           \
  } while (0)

/*
  A_DWORD
*/

typedef union A_DWORD {
  int32_t l;
  uint32_t ul;
  float f;
  char bytes[4];
} A_DWORD;

typedef char check_dword_size[sizeof(A_DWORD) == 4 ? 1 : -1];

static inline A_DWORD read_a_dword(char const* data) {
  return (A_DWORD){.bytes = {data[3], data[2], data[1], data[0]}};
}

#define READ_A_DWORD            \
  A_DWORD dword;                \
  do {                          \
    READ_A_DATA(4);             \
    dword = read_a_dword(data); \
    FREE_A_DATA(4);             \
  } while (0)

/*
  A_QWORD
*/
typedef union A_QWORD {
  int64_t ll;
  uint64_t ull;
  double d;
  char bytes[8];
} A_QWORD;

static inline A_QWORD read_a_qword(char const* data) {
  return (A_QWORD){.bytes = {data[7], data[6], data[5], data[4], data[3],
                             data[2], data[1], data[0]}};
}

typedef char check_qword_size[sizeof(A_QWORD) == 8 ? 1 : -1];

#define READ_A_QWORD            \
  A_QWORD qword;                \
  do {                          \
    READ_A_DATA(8);             \
    qword = read_a_qword(data); \
    FREE_A_DATA(8);             \
  } while (0)

/*
  TIMESTAMP96
*/
#pragma pack(push, 4)
typedef union {
  struct {
    int64_t seconds : 64;
    uint32_t nanosec : 32;
  };
  char bytes[12];
} TIMESTAMP96;
#pragma pack(pop)

typedef char _check_timestamp_96_size[sizeof(TIMESTAMP96) == 12 ? 1 : -1];

#endif  // ifndef A_INCLUDE_COMMON_H
