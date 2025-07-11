#ifndef A_INCLUDE_MACROS_H
#define A_INCLUDE_MACROS_H
#ifdef __GNUC__
#define A_LIKELY(cond) (__builtin_expect(!!(cond), 1))
#define A_UNLIKELY(cond) (__builtin_expect(!!(cond), 0))
#else
#define A_LIKELY(cond) (cond)
#define A_UNLIKELY(cond) (cond)
#endif

#ifdef __GNUC__
#define A_FORCE_INLINE __attribute__((always_inline)) inline
#define A_NOINLINE __attribute__((noinline))
#elif defined(_MSC_VER)
#define A_FORCE_INLINE __forceinline
#define A_NOINLINE __declspec(noinline)
#else
#define A_FORCE_INLINE inline
#define A_NOINLINE
#endif

#ifdef __GNUC__
#define BEGIN_NO_PEDANTIC        \
  _Pragma("GCC diagnostic push") \
      _Pragma("GCC diagnostic ignored \"-Wpedantic\"")
#define END_NO_PEDANTIC _Pragma("GCC diagnostic pop")
#else
#define BEGIN_NO_PEDANTIC
#define END_NO_PEDANTIC
#endif

#endif  // end A_INCLUDE_MACROS_H
