#ifndef MY_BYTEORDER_INCLUDED
#define MY_BYTEORDER_INCLUDED




#include <string.h>
#include <sys/types.h>
#include "arpa/inet.h"

#ifdef HAVE_ARPA_INET_H
#include <arpa/inet.h>
#endif

#if defined(_MSC_VER)
#include <stdlib.h>
#endif

#define mi_uint5korr(A) ((ulonglong)(((uint32) (((uchar*) (A))[4])) +\
                                    (((uint32) (((uchar*) (A))[3])) << 8) +\
                                    (((uint32) (((uchar*) (A))[2])) << 16) +\
                                    (((uint32) (((uchar*) (A))[1])) << 24)) +\
                                    (((ulonglong) (((uchar*) (A))[0])) << 32))


#define mi_sint2korr(A) ((int16)(((int16) (((uchar*) (A))[1])) +\
                                    (((int16) (((uchar*) (A))[0])) << 8)))

#define mi_sint3korr(A) ((int32) (((((uchar*) (A))[0]) & 128) ? \
            (((uint32) 255L << 24) | \
            (((uint32) ((uchar*) (A))[0]) << 16) |\
            (((uint32) ((uchar*) (A))[1]) << 8) | \
            ((uint32) ((uchar*) (A))[2])) : \
            (((uint32) ((uchar*) (A))[0]) << 16) |\
            (((uint32) ((uchar*) (A))[1]) << 8) | \
            ((uint32) ((uchar*) (A))[2])))

#include "my_inttypes.h"
#include "little_endian.h"

//static inline float flofloat4getat4get(const uchar *M) {
//    float V;
//    memcpy(&V, (M), sizeof(float));
//    return V;
//}

static inline int32 sint3korr(const uchar *A) {
  return ((int32)(((A[2]) & 128)
                      ? (((uint32)255L << 24) | (((uint32)A[2]) << 16) |
                         (((uint32)A[1]) << 8) | ((uint32)A[0]))
                      : (((uint32)A[2]) << 16) | (((uint32)A[1]) << 8) |
                            ((uint32)A[0])));
}

static inline uint32 uint3korr(const uchar *A) {
  return (uint32)(((uint32)(A[0])) + (((uint32)(A[1])) << 8) +
                  (((uint32)(A[2])) << 16));
}

static inline ulonglong uint5korr(const uchar *A) {
  return ((ulonglong)(((uint32)(A[0])) + (((uint32)(A[1])) << 8) +
                      (((uint32)(A[2])) << 16) + (((uint32)(A[3])) << 24)) +
          (((ulonglong)(A[4])) << 32));
}

static inline ulonglong uint6korr(const uchar *A) {
  return ((ulonglong)(((uint32)(A[0])) + (((uint32)(A[1])) << 8) +
                      (((uint32)(A[2])) << 16) + (((uint32)(A[3])) << 24)) +
          (((ulonglong)(A[4])) << 32) + (((ulonglong)(A[5])) << 40));
}

/**
  int3store

  Stores an unsinged integer in a platform independent way

  @param T  The destination buffer. Must be at least 3 bytes long
  @param A  The integer to store.

  _Example:_
  A @ref a_protocol_type_int3 "int \<3\>" with the value 1 is stored as:
  ~~~~~~~~~~~~~~~~~~~~~
  01 00 00
  ~~~~~~~~~~~~~~~~~~~~~
*/
static inline void int3store(uchar *T, uint A) {
  *(T) = (uchar)(A);
  *(T + 1) = (uchar)(A >> 8);
  *(T + 2) = (uchar)(A >> 16);
}

static inline void int5store(uchar *T, ulonglong A) {
  *(T) = (uchar)(A);
  *(T + 1) = (uchar)(A >> 8);
  *(T + 2) = (uchar)(A >> 16);
  *(T + 3) = (uchar)(A >> 24);
  *(T + 4) = (uchar)(A >> 32);
}

static inline void int6store(uchar *T, ulonglong A) {
  *(T) = (uchar)(A);
  *(T + 1) = (uchar)(A >> 8);
  *(T + 2) = (uchar)(A >> 16);
  *(T + 3) = (uchar)(A >> 24);
  *(T + 4) = (uchar)(A >> 32);
  *(T + 5) = (uchar)(A >> 40);
}

#ifdef __cplusplus

inline int16 sint2korr(const char *pT) {
  return sint2korr(static_cast<const uchar *>(static_cast<const void *>(pT)));
}

inline uint16 uint2korr(const char *pT) {
  return uint2korr(static_cast<const uchar *>(static_cast<const void *>(pT)));
}

inline uint32 uint3korr(const char *pT) {
  return uint3korr(static_cast<const uchar *>(static_cast<const void *>(pT)));
}

inline int32 sint3korr(const char *pT) {
  return sint3korr(static_cast<const uchar *>(static_cast<const void *>(pT)));
}

inline uint32 uint4korr(const char *pT) {
  return uint4korr(static_cast<const uchar *>(static_cast<const void *>(pT)));
}

inline int32 sint4korr(const char *pT) {
  return sint4korr(static_cast<const uchar *>(static_cast<const void *>(pT)));
}

inline ulonglong uint6korr(const char *pT) {
  return uint6korr(static_cast<const uchar *>(static_cast<const void *>(pT)));
}

inline ulonglong uint8korr(const char *pT) {
  return uint8korr(static_cast<const uchar *>(static_cast<const void *>(pT)));
}

inline longlong sint8korr(const char *pT) {
  return sint8korr(static_cast<const uchar *>(static_cast<const void *>(pT)));
}

inline void int2store(char *pT, uint16 A) {
  int2store(static_cast<uchar *>(static_cast<void *>(pT)), A);
}

inline void int3store(char *pT, uint A) {
  int3store(static_cast<uchar *>(static_cast<void *>(pT)), A);
}

inline void int4store(char *pT, uint32 A) {
  int4store(static_cast<uchar *>(static_cast<void *>(pT)), A);
}

inline void int5store(char *pT, ulonglong A) {
  int5store(static_cast<uchar *>(static_cast<void *>(pT)), A);
}

inline void int6store(char *pT, ulonglong A) {
  int6store(static_cast<uchar *>(static_cast<void *>(pT)), A);
}

inline void int8store(char *pT, ulonglong A) {
  int8store(static_cast<uchar *>(static_cast<void *>(pT)), A);
}

/*
  Functions for reading and storing in machine format from/to
  short/long to/from some place in memory V should be a variable
  and M a pointer to byte.
*/

inline void float4store(char *V, float M) {
  float4store(static_cast<uchar *>(static_cast<void *>(V)), M);
}

inline double float8get(const char *M) {
  return float8get(static_cast<const uchar *>(static_cast<const void *>(M)));
}

inline void float8store(char *V, double M) {
  float8store(static_cast<uchar *>(static_cast<void *>(V)), M);
}

/*
 Functions that have the same behavior on little- and big-endian.
*/

inline float floatget(const uchar *ptr) {
  float val;
  memcpy(&val, ptr, sizeof(val));
  return val;
}

inline void floatstore(uchar *ptr, float val) {
  memcpy(ptr, &val, sizeof(val));
}

inline double doubleget(const uchar *ptr) {
  double val;
  memcpy(&val, ptr, sizeof(val));
  return val;
}

inline void doublestore(uchar *ptr, double val) {
  memcpy(ptr, &val, sizeof(val));
}

inline uint16 ushortget(const uchar *ptr) {
  uint16 val;
  memcpy(&val, ptr, sizeof(val));
  return val;
}

inline int16 shortget(const uchar *ptr) {
  int16 val;
  memcpy(&val, ptr, sizeof(val));
  return val;
}

inline void shortstore(uchar *ptr, int16 val) {
  memcpy(ptr, &val, sizeof(val));
}

inline int32 longget(const uchar *ptr) {
  int32 val;
  memcpy(&val, ptr, sizeof(val));
  return val;
}

inline void longstore(uchar *ptr, int32 val) { memcpy(ptr, &val, sizeof(val)); }

inline uint32 ulongget(const uchar *ptr) {
  uint32 val;
  memcpy(&val, ptr, sizeof(val));
  return val;
}

inline longlong longlongget(const uchar *ptr) {
  longlong val;
  memcpy(&val, ptr, sizeof(val));
  return val;
}

inline void longlongstore(uchar *ptr, longlong val) {
  memcpy(ptr, &val, sizeof(val));
}

/*
 Functions for big-endian loads and stores. These are safe to use
 no matter what the compiler, CPU or alignment, and also with -fstrict-aliasing.

 The stores return a pointer just past the value that was written.
*/

inline uint16 load16be(const char *ptr) {
  uint16 val;
  memcpy(&val, ptr, sizeof(val));
  return ntohs(val);
}

inline uint32 load32be(const char *ptr) {
  uint32 val;
  memcpy(&val, ptr, sizeof(val));
  return ntohl(val);
}

inline char *store16be(char *ptr, uint16 val) {
#if defined(_MSC_VER)
  // _byteswap_ushort is an intrinsic on MSVC, but htons is not.
  val = _byteswap_ushort(val);
#else
  val = htons(val);
#endif
  memcpy(ptr, &val, sizeof(val));
  return ptr + sizeof(val);
}

inline char *store32be(char *ptr, uint32 val) {
  val = htonl(val);
  memcpy(ptr, &val, sizeof(val));
  return ptr + sizeof(val);
}

// Adapters for using uchar * instead of char *.

inline uint16 load16be(const uchar *ptr) {
  return load16be((ptr));
}

inline uint32 load32be(const uchar *ptr) {
  return load32be((ptr));
}

inline uchar *store16be(uchar *ptr, uint16 val) {
  return static_cast<uchar *>(store16be((ptr), val));
}

inline uchar *store32be(uchar *ptr, uint32 val) {
  return static_cast<uchar *>(store32be((ptr), val));
}

#endif /* __cplusplus */

#endif /* MY_BYTEORDER_INCLUDED */
