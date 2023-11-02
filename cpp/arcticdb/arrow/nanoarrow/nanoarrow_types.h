// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#ifndef NANOARROW_NANOARROW_TYPES_H_INCLUDED
#define NANOARROW_NANOARROW_TYPES_H_INCLUDED

#include <stdint.h>
#include <string.h>

#if defined(NANOARROW_DEBUG) && !defined(NANOARROW_PRINT_AND_DIE)
#include <stdio.h>
#include <stdlib.h>
#endif

#ifdef __cplusplus
extern "C" {
#endif


/// \defgroup nanoarrow-arrow-cdata Arrow C Data interface
///
/// The Arrow C Data (https://arrow.apache.org/docs/format/CDataInterface.html)
/// and Arrow C Stream (https://arrow.apache.org/docs/format/CStreamInterface.html)
/// interfaces are part of the
/// Arrow Columnar Format specification
/// (https://arrow.apache.org/docs/format/Columnar.html). See the Arrow documentation for
/// documentation of these structures.
///
/// @{

#ifndef ARROW_C_DATA_INTERFACE
#define ARROW_C_DATA_INTERFACE

#define ARROW_FLAG_DICTIONARY_ORDERED 1
#define ARROW_FLAG_NULLABLE 2
#define ARROW_FLAG_MAP_KEYS_SORTED 4

struct ArrowSchema {
  // Array type description
  const char* format;
  const char* name;
  const char* metadata;
  int64_t flags;
  int64_t n_children;
  struct ArrowSchema** children;
  struct ArrowSchema* dictionary;

  // Release callback
  void (*release)(struct ArrowSchema*);
  // Opaque producer-specific data
  void* private_data;
};

struct ArrowArray {
  // Array data description
  int64_t length;
  int64_t null_count;
  int64_t offset;
  int64_t n_buffers;
  int64_t n_children;
  const void** buffers;
  struct ArrowArray** children;
  struct ArrowArray* dictionary;

  // Release callback
  void (*release)(struct ArrowArray*);
  // Opaque producer-specific data
  void* private_data;
};

#endif  // ARROW_C_DATA_INTERFACE

#ifndef ARROW_C_STREAM_INTERFACE
#define ARROW_C_STREAM_INTERFACE

struct ArrowArrayStream {
  // Callback to get the stream type
  // (will be the same for all arrays in the stream).
  //
  // Return value: 0 if successful, an `errno`-compatible error code otherwise.
  //
  // If successful, the ArrowSchema must be released independently from the stream.
  int (*get_schema)(struct ArrowArrayStream*, struct ArrowSchema* out);

  // Callback to get the next array
  // (if no error and the array is released, the stream has ended)
  //
  // Return value: 0 if successful, an `errno`-compatible error code otherwise.
  //
  // If successful, the ArrowArray must be released independently from the stream.
  int (*get_next)(struct ArrowArrayStream*, struct ArrowArray* out);

  // Callback to get optional detailed error information.
  // This must only be called if the last stream operation failed
  // with a non-0 return code.
  //
  // Return value: pointer to a null-terminated character array describing
  // the last error, or NULL if no description is available.
  //
  // The returned pointer is only valid until the next operation on this stream
  // (including release).
  const char* (*get_last_error)(struct ArrowArrayStream*);

  // Release callback: release the stream's own resources.
  // Note that arrays returned by `get_next` must be individually released.
  void (*release)(struct ArrowArrayStream*);

  // Opaque producer-specific data
  void* private_data;
};

#endif  // ARROW_C_STREAM_INTERFACE

/// \brief Move the contents of src into dst and set src->release to NULL
static inline void ArrowSchemaMove(struct ArrowSchema* src, struct ArrowSchema* dst) {
  memcpy(dst, src, sizeof(struct ArrowSchema));
  src->release = NULL;
}

/// \brief Move the contents of src into dst and set src->release to NULL
static inline void ArrowArrayMove(struct ArrowArray* src, struct ArrowArray* dst) {
  memcpy(dst, src, sizeof(struct ArrowArray));
  src->release = NULL;
}

/// \brief Move the contents of src into dst and set src->release to NULL
static inline void ArrowArrayStreamMove(struct ArrowArrayStream* src,
                                        struct ArrowArrayStream* dst) {
  memcpy(dst, src, sizeof(struct ArrowArrayStream));
  src->release = NULL;
}

/// @}

// Utility macros
#define _NANOARROW_CONCAT(x, y) x##y
#define _NANOARROW_MAKE_NAME(x, y) _NANOARROW_CONCAT(x, y)

#define _NANOARROW_RETURN_NOT_OK_IMPL(NAME, EXPR) \
  do {                                            \
    const int NAME = (EXPR);                      \
    if (NAME) return NAME;                        \
  } while (0)

#define _NANOARROW_CHECK_RANGE(x_, min_, max_) \
  NANOARROW_RETURN_NOT_OK((x_ >= min_ && x_ <= max_) ? NANOARROW_OK : EINVAL)

#define _NANOARROW_CHECK_UPPER_LIMIT(x_, max_) \
  NANOARROW_RETURN_NOT_OK((x_ <= max_) ? NANOARROW_OK : EINVAL)

#if defined(NANOARROW_DEBUG)
#define _NANOARROW_RETURN_NOT_OK_WITH_ERROR_IMPL(NAME, EXPR, ERROR_PTR_EXPR, EXPR_STR) \
  do {                                                                                 \
    const int NAME = (EXPR);                                                           \
    if (NAME) {                                                                        \
      ArrowErrorSet((ERROR_PTR_EXPR), "%s failed with errno %d\n* %s:%d", EXPR_STR,    \
                    NAME, __FILE__, __LINE__);                                         \
      return NAME;                                                                     \
    }                                                                                  \
  } while (0)
#else
#define _NANOARROW_RETURN_NOT_OK_WITH_ERROR_IMPL(NAME, EXPR, ERROR_PTR_EXPR, EXPR_STR) \
  do {                                                                                 \
    const int NAME = (EXPR);                                                           \
    if (NAME) {                                                                        \
      ArrowErrorSet((ERROR_PTR_EXPR), "%s failed with errno %d", EXPR_STR, NAME);      \
      return NAME;                                                                     \
    }                                                                                  \
  } while (0)
#endif

/// \brief Return code for success.
/// \ingroup nanoarrow-errors
#define NANOARROW_OK 0

/// \brief Represents an errno-compatible error code
/// \ingroup nanoarrow-errors
typedef int ArrowErrorCode;

/// \brief Check the result of an expression and return it if not NANOARROW_OK
/// \ingroup nanoarrow-errors
#define NANOARROW_RETURN_NOT_OK(EXPR) \
  _NANOARROW_RETURN_NOT_OK_IMPL(_NANOARROW_MAKE_NAME(errno_status_, __COUNTER__), EXPR)

/// \brief Check the result of an expression and return it if not NANOARROW_OK,
/// adding an auto-generated message to an ArrowError.
/// \ingroup nanoarrow-errors
///
/// This macro is used to ensure that functions that accept an ArrowError
/// as input always set its message when returning an error code (e.g., when calling
/// a nanoarrow function that does *not* accept ArrowError).
#define NANOARROW_RETURN_NOT_OK_WITH_ERROR(EXPR, ERROR_EXPR) \
  _NANOARROW_RETURN_NOT_OK_WITH_ERROR_IMPL(                  \
      _NANOARROW_MAKE_NAME(errno_status_, __COUNTER__), EXPR, ERROR_EXPR, #EXPR)

#if defined(NANOARROW_DEBUG) && !defined(NANOARROW_PRINT_AND_DIE)
#define NANOARROW_PRINT_AND_DIE(VALUE, EXPR_STR)                                  \
  do {                                                                            \
    fprintf(stderr, "%s failed with errno %d\n* %s:%d\n", EXPR_STR, (int)(VALUE), \
            __FILE__, (int)__LINE__);                                             \
    abort();                                                                      \
  } while (0)
#endif

#if defined(NANOARROW_DEBUG)
#define _NANOARROW_ASSERT_OK_IMPL(NAME, EXPR, EXPR_STR) \
  do {                                                  \
    const int NAME = (EXPR);                            \
    if (NAME) NANOARROW_PRINT_AND_DIE(NAME, EXPR_STR);  \
  } while (0)

/// \brief Assert that an expression's value is NANOARROW_OK
/// \ingroup nanoarrow-errors
///
/// If nanoarrow was built in debug mode (i.e., defined(NANOARROW_DEBUG) is true),
/// print a message to stderr and abort. If nanoarrow was built in release mode,
/// this statement has no effect. You can customize fatal error behaviour
/// be defining the NANOARROW_PRINT_AND_DIE macro before including nanoarrow.h
/// This macro is provided as a convenience for users and is not used internally.
#define NANOARROW_ASSERT_OK(EXPR) \
  _NANOARROW_ASSERT_OK_IMPL(_NANOARROW_MAKE_NAME(errno_status_, __COUNTER__), EXPR, #EXPR)
#else
#define NANOARROW_ASSERT_OK(EXPR) EXPR
#endif

static char _ArrowIsLittleEndian(void) {
  uint32_t check = 1;
  char first_byte;
  memcpy(&first_byte, &check, sizeof(char));
  return first_byte;
}

/// \brief Arrow type enumerator
/// \ingroup nanoarrow-utils
///
/// These names are intended to map to the corresponding arrow::Type::type
/// enumerator; however, the numeric values are specifically not equal
/// (i.e., do not rely on numeric comparison).
enum ArrowType {
  NANOARROW_TYPE_UNINITIALIZED = 0,
  NANOARROW_TYPE_NA = 1,
  NANOARROW_TYPE_BOOL,
  NANOARROW_TYPE_UINT8,
  NANOARROW_TYPE_INT8,
  NANOARROW_TYPE_UINT16,
  NANOARROW_TYPE_INT16,
  NANOARROW_TYPE_UINT32,
  NANOARROW_TYPE_INT32,
  NANOARROW_TYPE_UINT64,
  NANOARROW_TYPE_INT64,
  NANOARROW_TYPE_HALF_FLOAT,
  NANOARROW_TYPE_FLOAT,
  NANOARROW_TYPE_DOUBLE,
  NANOARROW_TYPE_STRING,
  NANOARROW_TYPE_BINARY,
  NANOARROW_TYPE_FIXED_SIZE_BINARY,
  NANOARROW_TYPE_DATE32,
  NANOARROW_TYPE_DATE64,
  NANOARROW_TYPE_TIMESTAMP,
  NANOARROW_TYPE_TIME32,
  NANOARROW_TYPE_TIME64,
  NANOARROW_TYPE_INTERVAL_MONTHS,
  NANOARROW_TYPE_INTERVAL_DAY_TIME,
  NANOARROW_TYPE_DECIMAL128,
  NANOARROW_TYPE_DECIMAL256,
  NANOARROW_TYPE_LIST,
  NANOARROW_TYPE_STRUCT,
  NANOARROW_TYPE_SPARSE_UNION,
  NANOARROW_TYPE_DENSE_UNION,
  NANOARROW_TYPE_DICTIONARY,
  NANOARROW_TYPE_MAP,
  NANOARROW_TYPE_EXTENSION,
  NANOARROW_TYPE_FIXED_SIZE_LIST,
  NANOARROW_TYPE_DURATION,
  NANOARROW_TYPE_LARGE_STRING,
  NANOARROW_TYPE_LARGE_BINARY,
  NANOARROW_TYPE_LARGE_LIST,
  NANOARROW_TYPE_INTERVAL_MONTH_DAY_NANO
};

/// \brief Get a string value of an enum ArrowType value
/// \ingroup nanoarrow-utils
///
/// Returns NULL for invalid values for type
static inline const char* ArrowTypeString(enum ArrowType type);

static inline const char* ArrowTypeString(enum ArrowType type) {
  switch (type) {
    case NANOARROW_TYPE_NA:
      return "na";
    case NANOARROW_TYPE_BOOL:
      return "bool";
    case NANOARROW_TYPE_UINT8:
      return "uint8";
    case NANOARROW_TYPE_INT8:
      return "int8";
    case NANOARROW_TYPE_UINT16:
      return "uint16";
    case NANOARROW_TYPE_INT16:
      return "int16";
    case NANOARROW_TYPE_UINT32:
      return "uint32";
    case NANOARROW_TYPE_INT32:
      return "int32";
    case NANOARROW_TYPE_UINT64:
      return "uint64";
    case NANOARROW_TYPE_INT64:
      return "int64";
    case NANOARROW_TYPE_HALF_FLOAT:
      return "half_float";
    case NANOARROW_TYPE_FLOAT:
      return "float";
    case NANOARROW_TYPE_DOUBLE:
      return "double";
    case NANOARROW_TYPE_STRING:
      return "string";
    case NANOARROW_TYPE_BINARY:
      return "binary";
    case NANOARROW_TYPE_FIXED_SIZE_BINARY:
      return "fixed_size_binary";
    case NANOARROW_TYPE_DATE32:
      return "date32";
    case NANOARROW_TYPE_DATE64:
      return "date64";
    case NANOARROW_TYPE_TIMESTAMP:
      return "timestamp";
    case NANOARROW_TYPE_TIME32:
      return "time32";
    case NANOARROW_TYPE_TIME64:
      return "time64";
    case NANOARROW_TYPE_INTERVAL_MONTHS:
      return "interval_months";
    case NANOARROW_TYPE_INTERVAL_DAY_TIME:
      return "interval_day_time";
    case NANOARROW_TYPE_DECIMAL128:
      return "decimal128";
    case NANOARROW_TYPE_DECIMAL256:
      return "decimal256";
    case NANOARROW_TYPE_LIST:
      return "list";
    case NANOARROW_TYPE_STRUCT:
      return "struct";
    case NANOARROW_TYPE_SPARSE_UNION:
      return "sparse_union";
    case NANOARROW_TYPE_DENSE_UNION:
      return "dense_union";
    case NANOARROW_TYPE_DICTIONARY:
      return "dictionary";
    case NANOARROW_TYPE_MAP:
      return "map";
    case NANOARROW_TYPE_EXTENSION:
      return "extension";
    case NANOARROW_TYPE_FIXED_SIZE_LIST:
      return "fixed_size_list";
    case NANOARROW_TYPE_DURATION:
      return "duration";
    case NANOARROW_TYPE_LARGE_STRING:
      return "large_string";
    case NANOARROW_TYPE_LARGE_BINARY:
      return "large_binary";
    case NANOARROW_TYPE_LARGE_LIST:
      return "large_list";
    case NANOARROW_TYPE_INTERVAL_MONTH_DAY_NANO:
      return "interval_month_day_nano";
    default:
      return NULL;
  }
}

/// \brief Arrow time unit enumerator
/// \ingroup nanoarrow-utils
///
/// These names and values map to the corresponding arrow::TimeUnit::type
/// enumerator.
enum ArrowTimeUnit {
  NANOARROW_TIME_UNIT_SECOND = 0,
  NANOARROW_TIME_UNIT_MILLI = 1,
  NANOARROW_TIME_UNIT_MICRO = 2,
  NANOARROW_TIME_UNIT_NANO = 3
};

/// \brief Validation level enumerator
/// \ingroup nanoarrow-array
enum ArrowValidationLevel {
  /// \brief Do not validate buffer sizes or content.
  NANOARROW_VALIDATION_LEVEL_NONE = 0,

  /// \brief Validate buffer sizes that depend on array length but do not validate buffer
  /// sizes that depend on buffer data access.
  NANOARROW_VALIDATION_LEVEL_MINIMAL = 1,

  /// \brief Validate all buffer sizes, including those that require buffer data access,
  /// but do not perform any checks that are O(1) along the length of the buffers.
  NANOARROW_VALIDATION_LEVEL_DEFAULT = 2,

  /// \brief Validate all buffer sizes and all buffer content. This is useful in the
  /// context of untrusted input or input that may have been corrupted in transit.
  NANOARROW_VALIDATION_LEVEL_FULL = 3
};

/// \brief Get a string value of an enum ArrowTimeUnit value
/// \ingroup nanoarrow-utils
///
/// Returns NULL for invalid values for time_unit
static inline const char* ArrowTimeUnitString(enum ArrowTimeUnit time_unit);

static inline const char* ArrowTimeUnitString(enum ArrowTimeUnit time_unit) {
  switch (time_unit) {
    case NANOARROW_TIME_UNIT_SECOND:
      return "s";
    case NANOARROW_TIME_UNIT_MILLI:
      return "ms";
    case NANOARROW_TIME_UNIT_MICRO:
      return "us";
    case NANOARROW_TIME_UNIT_NANO:
      return "ns";
    default:
      return NULL;
  }
}

/// \brief Functional types of buffers as described in the Arrow Columnar Specification
/// \ingroup nanoarrow-array-view
enum ArrowBufferType {
  NANOARROW_BUFFER_TYPE_NONE,
  NANOARROW_BUFFER_TYPE_VALIDITY,
  NANOARROW_BUFFER_TYPE_TYPE_ID,
  NANOARROW_BUFFER_TYPE_UNION_OFFSET,
  NANOARROW_BUFFER_TYPE_DATA_OFFSET,
  NANOARROW_BUFFER_TYPE_DATA
};

/// \brief An non-owning view of a string
/// \ingroup nanoarrow-utils
struct ArrowStringView {
  /// \brief A pointer to the start of the string
  ///
  /// If size_bytes is 0, this value may be NULL.
  const char* data;

  /// \brief The size of the string in bytes,
  ///
  /// (Not including the null terminator.)
  int64_t size_bytes;
};

/// \brief Return a view of a const C string
/// \ingroup nanoarrow-utils
static inline struct ArrowStringView ArrowCharView(const char* value);

static inline struct ArrowStringView ArrowCharView(const char* value) {
  struct ArrowStringView out;

  out.data = value;
  if (value) {
    out.size_bytes = (int64_t)strlen(value);
  } else {
    out.size_bytes = 0;
  }

  return out;
}

union ArrowBufferViewData {
  const void* data;
  const int8_t* as_int8;
  const uint8_t* as_uint8;
  const int16_t* as_int16;
  const uint16_t* as_uint16;
  const int32_t* as_int32;
  const uint32_t* as_uint32;
  const int64_t* as_int64;
  const uint64_t* as_uint64;
  const double* as_double;
  const float* as_float;
  const char* as_char;
};

/// \brief An non-owning view of a buffer
/// \ingroup nanoarrow-utils
struct ArrowBufferView {
  /// \brief A pointer to the start of the buffer
  ///
  /// If size_bytes is 0, this value may be NULL.
  union ArrowBufferViewData data;

  /// \brief The size of the buffer in bytes
  int64_t size_bytes;
};

/// \brief Array buffer allocation and deallocation
/// \ingroup nanoarrow-buffer
///
/// Container for allocate, reallocate, and free methods that can be used
/// to customize allocation and deallocation of buffers when constructing
/// an ArrowArray.
struct ArrowBufferAllocator {
  /// \brief Reallocate a buffer or return NULL if it cannot be reallocated
  uint8_t* (*reallocate)(struct ArrowBufferAllocator* allocator, uint8_t* ptr,
                         int64_t old_size, int64_t new_size);

  /// \brief Deallocate a buffer allocated by this allocator
  void (*free)(struct ArrowBufferAllocator* allocator, uint8_t* ptr, int64_t size);

  /// \brief Opaque data specific to the allocator
  void* private_data;
};

/// \brief An owning mutable view of a buffer
/// \ingroup nanoarrow-buffer
struct ArrowBuffer {
  /// \brief A pointer to the start of the buffer
  ///
  /// If capacity_bytes is 0, this value may be NULL.
  uint8_t* data;

  /// \brief The size of the buffer in bytes
  int64_t size_bytes;

  /// \brief The capacity of the buffer in bytes
  int64_t capacity_bytes;

  /// \brief The allocator that will be used to reallocate and/or free the buffer
  struct ArrowBufferAllocator allocator;
};

/// \brief An owning mutable view of a bitmap
/// \ingroup nanoarrow-bitmap
struct ArrowBitmap {
  /// \brief An ArrowBuffer to hold the allocated memory
  struct ArrowBuffer buffer;

  /// \brief The number of bits that have been appended to the bitmap
  int64_t size_bits;
};

/// \brief A description of an arrangement of buffers
/// \ingroup nanoarrow-utils
///
/// Contains the minimum amount of information required to
/// calculate the size of each buffer in an ArrowArray knowing only
/// the length and offset of the array.
struct ArrowLayout {
  /// \brief The function of each buffer
  enum ArrowBufferType buffer_type[3];

  /// \brief The data type of each buffer
  enum ArrowType buffer_data_type[3];

  /// \brief The size of an element each buffer or 0 if this size is variable or unknown
  int64_t element_size_bits[3];

  /// \brief The number of elements in the child array per element in this array for a
  /// fixed-size list
  int64_t child_size_elements;
};

/// \brief A non-owning view of an ArrowArray
/// \ingroup nanoarrow-array-view
///
/// This data structure provides access to the values contained within
/// an ArrowArray with fields provided in a more readily-extractible
/// form. You can re-use an ArrowArrayView for multiple ArrowArrays
/// with the same storage type, use it to represent a hypothetical
/// ArrowArray that does not exist yet, or use it to validate the buffers
/// of a future ArrowArray.
struct ArrowArrayView {
  /// \brief The underlying ArrowArray or NULL if it has not been set or
  /// if the buffers in this ArrowArrayView are not backed by an ArrowArray.
  struct ArrowArray* array;

  /// \brief The number of elements from the physical start of the buffers.
  int64_t offset;

  /// \brief The number of elements in this view.
  int64_t length;

  /// \brief A cached null count or -1 to indicate that this value is unknown.
  int64_t null_count;

  /// \brief The type used to store values in this array
  ///
  /// This type represents only the minimum required information to
  /// extract values from the array buffers (e.g., for a Date32 array,
  /// this value will be NANOARROW_TYPE_INT32). For dictionary-encoded
  /// arrays, this will be the index type.
  enum ArrowType storage_type;

  /// \brief The buffer types, strides, and sizes of this Array's buffers
  struct ArrowLayout layout;

  /// \brief This Array's buffers as ArrowBufferView objects
  struct ArrowBufferView buffer_views[3];

  /// \brief The number of children of this view
  int64_t n_children;

  /// \brief Pointers to views of this array's children
  struct ArrowArrayView** children;

  /// \brief Pointer to a view of this array's dictionary
  struct ArrowArrayView* dictionary;

  /// \brief Union type id to child index mapping
  ///
  /// If storage_type is a union type, a 256-byte ArrowMalloc()ed buffer
  /// such that child_index == union_type_id_map[type_id] and
  /// type_id == union_type_id_map[128 + child_index]. This value may be
  /// NULL in the case where child_id == type_id.
  int8_t* union_type_id_map;
};

// Used as the private data member for ArrowArrays allocated here and accessed
// internally within inline ArrowArray* helpers.
struct ArrowArrayPrivateData {
  // Holder for the validity buffer (or first buffer for union types, which are
  // the only type whose first buffer is not a valdiity buffer)
  struct ArrowBitmap bitmap;

  // Holder for additional buffers as required
  struct ArrowBuffer buffers[2];

  // The array of pointers to buffers. This must be updated after a sequence
  // of appends to synchronize its values with the actual buffer addresses
  // (which may have ben reallocated uring that time)
  const void* buffer_data[3];

  // The storage data type, or NANOARROW_TYPE_UNINITIALIZED if unknown
  enum ArrowType storage_type;

  // The buffer arrangement for the storage type
  struct ArrowLayout layout;

  // Flag to indicate if there are non-sequence union type ids.
  // In the future this could be replaced with a type id<->child mapping
  // to support constructing unions in append mode where type_id != child_index
  int8_t union_type_id_is_child_index;
};

/// \brief A representation of an interval.
/// \ingroup nanoarrow-utils
struct ArrowInterval {
  /// \brief The type of interval being used
  enum ArrowType type;
  /// \brief The number of months represented by the interval
  int32_t months;
  /// \brief The number of days represented by the interval
  int32_t days;
  /// \brief The number of ms represented by the interval
  int32_t ms;
  /// \brief The number of ns represented by the interval
  int64_t ns;
};

/// \brief Zero initialize an Interval with a given unit
/// \ingroup nanoarrow-utils
static inline void ArrowIntervalInit(struct ArrowInterval* interval,
                                     enum ArrowType type) {
  memset(interval, 0, sizeof(struct ArrowInterval));
  interval->type = type;
}

/// \brief A representation of a fixed-precision decimal number
/// \ingroup nanoarrow-utils
///
/// This structure should be initialized with ArrowDecimalInit() once and
/// values set using ArrowDecimalSetInt(), ArrowDecimalSetBytes128(),
/// or ArrowDecimalSetBytes256().
struct ArrowDecimal {
  /// \brief An array of 64-bit integers of n_words length defined in native-endian order
  uint64_t words[4];

  /// \brief The number of significant digits this decimal number can represent
  int32_t precision;

  /// \brief The number of digits after the decimal point. This can be negative.
  int32_t scale;

  /// \brief The number of words in the words array
  int n_words;

  /// \brief Cached value used by the implementation
  int high_word_index;

  /// \brief Cached value used by the implementation
  int low_word_index;
};

/// \brief Initialize a decimal with a given set of type parameters
/// \ingroup nanoarrow-utils
static inline void ArrowDecimalInit(struct ArrowDecimal* decimal, int32_t bitwidth,
                                    int32_t precision, int32_t scale) {
  memset(decimal->words, 0, sizeof(decimal->words));
  decimal->precision = precision;
  decimal->scale = scale;
  decimal->n_words = bitwidth / 8 / sizeof(uint64_t);

  if (_ArrowIsLittleEndian()) {
    decimal->low_word_index = 0;
    decimal->high_word_index = decimal->n_words - 1;
  } else {
    decimal->low_word_index = decimal->n_words - 1;
    decimal->high_word_index = 0;
  }
}

/// \brief Get a signed integer value of a sufficiently small ArrowDecimal
///
/// This does not check if the decimal's precision sufficiently small to fit
/// within the signed 64-bit integer range (A precision less than or equal
/// to 18 is sufficiently small).
static inline int64_t ArrowDecimalGetIntUnsafe(struct ArrowDecimal* decimal) {
  return (int64_t)decimal->words[decimal->low_word_index];
}

/// \brief Copy the bytes of this decimal into a sufficiently large buffer
/// \ingroup nanoarrow-utils
static inline void ArrowDecimalGetBytes(struct ArrowDecimal* decimal, uint8_t* out) {
  memcpy(out, decimal->words, decimal->n_words * sizeof(uint64_t));
}

/// \brief Returns 1 if the value represented by decimal is >= 0 or -1 otherwise
/// \ingroup nanoarrow-utils
static inline int64_t ArrowDecimalSign(struct ArrowDecimal* decimal) {
  return 1 | ((int64_t)(decimal->words[decimal->high_word_index]) >> 63);
}

/// \brief Sets the integer value of this decimal
/// \ingroup nanoarrow-utils
static inline void ArrowDecimalSetInt(struct ArrowDecimal* decimal, int64_t value) {
  if (value < 0) {
    memset(decimal->words, 0xff, decimal->n_words * sizeof(uint64_t));
  } else {
    memset(decimal->words, 0, decimal->n_words * sizeof(uint64_t));
  }

  decimal->words[decimal->low_word_index] = value;
}

/// \brief Copy bytes from a buffer into this decimal
/// \ingroup nanoarrow-utils
static inline void ArrowDecimalSetBytes(struct ArrowDecimal* decimal,
                                        const uint8_t* value) {
  memcpy(decimal->words, value, decimal->n_words * sizeof(uint64_t));
}

#ifdef __cplusplus
}
#endif

#endif
