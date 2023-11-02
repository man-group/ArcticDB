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

#ifndef NANOARROW_H_INCLUDED
#define NANOARROW_H_INCLUDED

#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>

#include "nanoarrow_types.h"

// If using CMake, optionally pass -DNANOARROW_NAMESPACE=MyNamespace which will set this
// define in nanoarrow_config.h. If not, you can optionally #define NANOARROW_NAMESPACE
// MyNamespace here.
#define NANOARROW_NAMESPACE ArcticDBNanoArrow
// This section remaps the non-prefixed symbols to the prefixed symbols so that
// code written against this build can be used independent of the value of
// NANOARROW_NAMESPACE.
#ifdef NANOARROW_NAMESPACE
#define NANOARROW_CAT(A, B) A##B
#define NANOARROW_SYMBOL(A, B) NANOARROW_CAT(A, B)

#define ArrowNanoarrowVersion NANOARROW_SYMBOL(NANOARROW_NAMESPACE, ArrowNanoarrowVersion)
#define ArrowNanoarrowVersionInt \
  NANOARROW_SYMBOL(NANOARROW_NAMESPACE, ArrowNanoarrowVersionInt)
#define ArrowErrorMessage NANOARROW_SYMBOL(NANOARROW_NAMESPACE, ArrowErrorMessage)
#define ArrowMalloc NANOARROW_SYMBOL(NANOARROW_NAMESPACE, ArrowMalloc)
#define ArrowRealloc NANOARROW_SYMBOL(NANOARROW_NAMESPACE, ArrowRealloc)
#define ArrowFree NANOARROW_SYMBOL(NANOARROW_NAMESPACE, ArrowFree)
#define ArrowBufferAllocatorDefault \
  NANOARROW_SYMBOL(NANOARROW_NAMESPACE, ArrowBufferAllocatorDefault)
#define ArrowBufferDeallocator \
  NANOARROW_SYMBOL(NANOARROW_NAMESPACE, ArrowBufferDeallocator)
#define ArrowErrorSet NANOARROW_SYMBOL(NANOARROW_NAMESPACE, ArrowErrorSet)
#define ArrowLayoutInit NANOARROW_SYMBOL(NANOARROW_NAMESPACE, ArrowLayoutInit)
#define ArrowSchemaInit NANOARROW_SYMBOL(NANOARROW_NAMESPACE, ArrowSchemaInit)
#define ArrowSchemaInitFromType \
  NANOARROW_SYMBOL(NANOARROW_NAMESPACE, ArrowSchemaInitFromType)
#define ArrowSchemaSetType NANOARROW_SYMBOL(NANOARROW_NAMESPACE, ArrowSchemaSetType)
#define ArrowSchemaSetTypeStruct \
  NANOARROW_SYMBOL(NANOARROW_NAMESPACE, ArrowSchemaSetTypeStruct)
#define ArrowSchemaSetTypeFixedSize \
  NANOARROW_SYMBOL(NANOARROW_NAMESPACE, ArrowSchemaSetTypeFixedSize)
#define ArrowSchemaSetTypeDecimal \
  NANOARROW_SYMBOL(NANOARROW_NAMESPACE, ArrowSchemaSetTypeDecimal)
#define ArrowSchemaSetTypeDateTime \
  NANOARROW_SYMBOL(NANOARROW_NAMESPACE, ArrowSchemaSetTypeDateTime)
#define ArrowSchemaSetTypeUnion \
  NANOARROW_SYMBOL(NANOARROW_NAMESPACE, ArrowSchemaSetTypeUnion)
#define ArrowSchemaDeepCopy NANOARROW_SYMBOL(NANOARROW_NAMESPACE, ArrowSchemaDeepCopy)
#define ArrowSchemaSetFormat NANOARROW_SYMBOL(NANOARROW_NAMESPACE, ArrowSchemaSetFormat)
#define ArrowSchemaSetName NANOARROW_SYMBOL(NANOARROW_NAMESPACE, ArrowSchemaSetName)
#define ArrowSchemaSetMetadata \
  NANOARROW_SYMBOL(NANOARROW_NAMESPACE, ArrowSchemaSetMetadata)
#define ArrowSchemaAllocateChildren \
  NANOARROW_SYMBOL(NANOARROW_NAMESPACE, ArrowSchemaAllocateChildren)
#define ArrowSchemaAllocateDictionary \
  NANOARROW_SYMBOL(NANOARROW_NAMESPACE, ArrowSchemaAllocateDictionary)
#define ArrowMetadataReaderInit \
  NANOARROW_SYMBOL(NANOARROW_NAMESPACE, ArrowMetadataReaderInit)
#define ArrowMetadataReaderRead \
  NANOARROW_SYMBOL(NANOARROW_NAMESPACE, ArrowMetadataReaderRead)
#define ArrowMetadataSizeOf NANOARROW_SYMBOL(NANOARROW_NAMESPACE, ArrowMetadataSizeOf)
#define ArrowMetadataHasKey NANOARROW_SYMBOL(NANOARROW_NAMESPACE, ArrowMetadataHasKey)
#define ArrowMetadataGetValue NANOARROW_SYMBOL(NANOARROW_NAMESPACE, ArrowMetadataGetValue)
#define ArrowMetadataBuilderInit \
  NANOARROW_SYMBOL(NANOARROW_NAMESPACE, ArrowMetadataBuilderInit)
#define ArrowMetadataBuilderAppend \
  NANOARROW_SYMBOL(NANOARROW_NAMESPACE, ArrowMetadataBuilderAppend)
#define ArrowMetadataBuilderSet \
  NANOARROW_SYMBOL(NANOARROW_NAMESPACE, ArrowMetadataBuilderSet)
#define ArrowMetadataBuilderRemove \
  NANOARROW_SYMBOL(NANOARROW_NAMESPACE, ArrowMetadataBuilderRemove)
#define ArrowSchemaViewInit NANOARROW_SYMBOL(NANOARROW_NAMESPACE, ArrowSchemaViewInit)
#define ArrowSchemaToString NANOARROW_SYMBOL(NANOARROW_NAMESPACE, ArrowSchemaToString)
#define ArrowArrayInitFromType \
  NANOARROW_SYMBOL(NANOARROW_NAMESPACE, ArrowArrayInitFromType)
#define ArrowArrayInitFromSchema \
  NANOARROW_SYMBOL(NANOARROW_NAMESPACE, ArrowArrayInitFromSchema)
#define ArrowArrayInitFromArrayView \
  NANOARROW_SYMBOL(NANOARROW_NAMESPACE, ArrowArrayInitFromArrayView)
#define ArrowArrayInitFromArrayView \
  NANOARROW_SYMBOL(NANOARROW_NAMESPACE, ArrowArrayInitFromArrayView)
#define ArrowArrayAllocateChildren \
  NANOARROW_SYMBOL(NANOARROW_NAMESPACE, ArrowArrayAllocateChildren)
#define ArrowArrayAllocateDictionary \
  NANOARROW_SYMBOL(NANOARROW_NAMESPACE, ArrowArrayAllocateDictionary)
#define ArrowArraySetValidityBitmap \
  NANOARROW_SYMBOL(NANOARROW_NAMESPACE, ArrowArraySetValidityBitmap)
#define ArrowArraySetBuffer NANOARROW_SYMBOL(NANOARROW_NAMESPACE, ArrowArraySetBuffer)
#define ArrowArrayReserve NANOARROW_SYMBOL(NANOARROW_NAMESPACE, ArrowArrayReserve)
#define ArrowArrayFinishBuilding \
  NANOARROW_SYMBOL(NANOARROW_NAMESPACE, ArrowArrayFinishBuilding)
#define ArrowArrayFinishBuildingDefault \
  NANOARROW_SYMBOL(NANOARROW_NAMESPACE, ArrowArrayFinishBuildingDefault)
#define ArrowArrayViewInitFromType \
  NANOARROW_SYMBOL(NANOARROW_NAMESPACE, ArrowArrayViewInitFromType)
#define ArrowArrayViewInitFromSchema \
  NANOARROW_SYMBOL(NANOARROW_NAMESPACE, ArrowArrayViewInitFromSchema)
#define ArrowArrayViewInitFromArray \
  NANOARROW_SYMBOL(NANOARROW_NAMESPACE, ArrowArrayViewInitFromArray)
#define ArrowArrayViewAllocateChildren \
  NANOARROW_SYMBOL(NANOARROW_NAMESPACE, ArrowArrayViewAllocateChildren)
#define ArrowArrayViewAllocateDictionary \
  NANOARROW_SYMBOL(NANOARROW_NAMESPACE, ArrowArrayViewAllocateDictionary)
#define ArrowArrayViewSetLength \
  NANOARROW_SYMBOL(NANOARROW_NAMESPACE, ArrowArrayViewSetLength)
#define ArrowArrayViewSetArray \
  NANOARROW_SYMBOL(NANOARROW_NAMESPACE, ArrowArrayViewSetArray)
#define ArrowArrayViewSetArrayMinimal \
  NANOARROW_SYMBOL(NANOARROW_NAMESPACE, ArrowArrayViewSetArrayMinimal)
#define ArrowArrayViewValidate \
  NANOARROW_SYMBOL(NANOARROW_NAMESPACE, ArrowArrayViewValidate)
#define ArrowArrayViewReset NANOARROW_SYMBOL(NANOARROW_NAMESPACE, ArrowArrayViewReset)
#define ArrowBasicArrayStreamInit \
  NANOARROW_SYMBOL(NANOARROW_NAMESPACE, ArrowBasicArrayStreamInit)
#define ArrowBasicArrayStreamSetArray \
  NANOARROW_SYMBOL(NANOARROW_NAMESPACE, ArrowBasicArrayStreamSetArray)
#define ArrowBasicArrayStreamValidate \
  NANOARROW_SYMBOL(NANOARROW_NAMESPACE, ArrowBasicArrayStreamValidate)

#endif

#ifdef __cplusplus
extern "C" {
#endif

/// \defgroup nanoarrow Nanoarrow C library
///
/// Except where noted, objects are not thread-safe and clients should
/// take care to serialize accesses to methods.
///
/// Because this library is intended to be vendored, it provides full type
/// definitions and encourages clients to stack or statically allocate
/// where convenient.

/// \defgroup nanoarrow-malloc Memory management
///
/// Non-buffer members of a struct ArrowSchema and struct ArrowArray
/// must be allocated using ArrowMalloc() or ArrowRealloc() and freed
/// using ArrowFree() for schemas and arrays allocated here. Buffer members
/// are allocated using an ArrowBufferAllocator.
///
/// @{

/// \brief Allocate like malloc()
void* ArrowMalloc(int64_t size);

/// \brief Reallocate like realloc()
void* ArrowRealloc(void* ptr, int64_t size);

/// \brief Free a pointer allocated using ArrowMalloc() or ArrowRealloc().
void ArrowFree(void* ptr);

/// \brief Return the default allocator
///
/// The default allocator uses ArrowMalloc(), ArrowRealloc(), and
/// ArrowFree().
struct ArrowBufferAllocator ArrowBufferAllocatorDefault(void);

/// \brief Create a custom deallocator
///
/// Creates a buffer allocator with only a free method that can be used to
/// attach a custom deallocator to an ArrowBuffer. This may be used to
/// avoid copying an existing buffer that was not allocated using the
/// infrastructure provided here (e.g., by an R or Python object).
struct ArrowBufferAllocator ArrowBufferDeallocator(
    void (*custom_free)(struct ArrowBufferAllocator* allocator, uint8_t* ptr,
                        int64_t size),
    void* private_data);

/// @}

/// \defgroup nanoarrow-errors Error handling
///
/// Functions generally return an errno-compatible error code; functions that
/// need to communicate more verbose error information accept a pointer
/// to an ArrowError. This can be stack or statically allocated. The
/// content of the message is undefined unless an error code has been
/// returned. If a nanoarrow function is passed a non-null ArrowError pointer, the
/// ArrowError pointed to by the argument will be propagated with a
/// null-terminated error message. It is safe to pass a NULL ArrowError anywhere
/// in the nanoarrow API.
///
/// Except where documented, it is generally not safe to continue after a
/// function has returned a non-zero ArrowErrorCode. The NANOARROW_RETURN_NOT_OK and
/// NANOARROW_ASSERT_OK macros are provided to help propagate errors. C++ clients can use
/// the helpers provided in the nanoarrow.hpp header to facilitate using C++ idioms
/// for memory management and error propgagtion.
///
/// @{

/// \brief Error type containing a UTF-8 encoded message.
struct ArrowError {
  /// \brief A character buffer with space for an error message.
  char message[1024];
};

/// \brief Ensure an ArrowError is null-terminated by zeroing the first character.
///
/// If error is NULL, this function does nothing.
static inline void ArrowErrorInit(struct ArrowError* error) {
  if (error) {
    error->message[0] = '\0';
  }
}

/// \brief Set the contents of an error using printf syntax.
///
/// If error is NULL, this function does nothing and returns NANOARROW_OK.
ArrowErrorCode ArrowErrorSet(struct ArrowError* error, const char* fmt, ...);

/// \brief Get the contents of an error
///
/// If error is NULL, returns "", or returns the contents of the error message
/// otherwise.
const char* ArrowErrorMessage(struct ArrowError* error);

/// @}

/// \defgroup nanoarrow-utils Utility data structures
///
/// @{

/// \brief Return a version string in the form "major.minor.patch"
const char* ArrowNanoarrowVersion(void);

/// \brief Return an integer that can be used to compare versions sequentially
int ArrowNanoarrowVersionInt(void);

/// \brief Initialize a description of buffer arrangements from a storage type
void ArrowLayoutInit(struct ArrowLayout* layout, enum ArrowType storage_type);

/// \brief Create a string view from a null-terminated string
static inline struct ArrowStringView ArrowCharView(const char* value);

/// @}

/// \defgroup nanoarrow-schema Creating schemas
///
/// These functions allocate, copy, and destroy ArrowSchema structures
///
/// @{

/// \brief Initialize an ArrowSchema
///
/// Initializes the fields and release callback of schema_out. Caller
/// is responsible for calling the schema->release callback if
/// NANOARROW_OK is returned.
void ArrowSchemaInit(struct ArrowSchema* schema);

/// \brief Initialize an ArrowSchema from an ArrowType
///
/// A convenience constructor for that calls ArrowSchemaInit() and
/// ArrowSchemaSetType() for the common case of constructing an
/// unparameterized type. The caller is responsible for calling the schema->release
/// callback if NANOARROW_OK is returned.
ArrowErrorCode ArrowSchemaInitFromType(struct ArrowSchema* schema, enum ArrowType type);

/// \brief Get a human-readable summary of a Schema
///
/// Writes a summary of an ArrowSchema to out (up to n - 1 characters)
/// and returns the number of characters required for the output if
/// n were sufficiently large. If recursive is non-zero, the result will
/// also include children.
int64_t ArrowSchemaToString(struct ArrowSchema* schema, char* out, int64_t n,
                            char recursive);

/// \brief Set the format field of a schema from an ArrowType
///
/// Initializes the fields and release callback of schema_out. For
/// NANOARROW_TYPE_LIST, NANOARROW_TYPE_LARGE_LIST, and
/// NANOARROW_TYPE_MAP, the appropriate number of children are
/// allocated, initialized, and named; however, the caller must
/// ArrowSchemaSetType() on the preinitialized children. Schema must have been initialized
/// using ArrowSchemaInit() or ArrowSchemaDeepCopy().
ArrowErrorCode ArrowSchemaSetType(struct ArrowSchema* schema, enum ArrowType type);

/// \brief Set the format field and initialize children of a struct schema
///
/// The specified number of children are initialized; however, the caller is responsible
/// for calling ArrowSchemaSetType() and ArrowSchemaSetName() on each child.
/// Schema must have been initialized using ArrowSchemaInit() or ArrowSchemaDeepCopy().
ArrowErrorCode ArrowSchemaSetTypeStruct(struct ArrowSchema* schema, int64_t n_children);

/// \brief Set the format field of a fixed-size schema
///
/// Returns EINVAL for fixed_size <= 0 or for type that is not
/// NANOARROW_TYPE_FIXED_SIZE_BINARY or NANOARROW_TYPE_FIXED_SIZE_LIST.
/// For NANOARROW_TYPE_FIXED_SIZE_LIST, the appropriate number of children are
/// allocated, initialized, and named; however, the caller must
/// ArrowSchemaSetType() the first child. Schema must have been initialized using
/// ArrowSchemaInit() or ArrowSchemaDeepCopy().
ArrowErrorCode ArrowSchemaSetTypeFixedSize(struct ArrowSchema* schema,
                                           enum ArrowType type, int32_t fixed_size);

/// \brief Set the format field of a decimal schema
///
/// Returns EINVAL for scale <= 0 or for type that is not
/// NANOARROW_TYPE_DECIMAL128 or NANOARROW_TYPE_DECIMAL256. Schema must have been
/// initialized using ArrowSchemaInit() or ArrowSchemaDeepCopy().
ArrowErrorCode ArrowSchemaSetTypeDecimal(struct ArrowSchema* schema, enum ArrowType type,
                                         int32_t decimal_precision,
                                         int32_t decimal_scale);

/// \brief Set the format field of a time, timestamp, or duration schema
///
/// Returns EINVAL for type that is not
/// NANOARROW_TYPE_TIME32, NANOARROW_TYPE_TIME64,
/// NANOARROW_TYPE_TIMESTAMP, or NANOARROW_TYPE_DURATION. The
/// timezone parameter must be NULL for a non-timestamp type. Schema must have been
/// initialized using ArrowSchemaInit() or ArrowSchemaDeepCopy().
ArrowErrorCode ArrowSchemaSetTypeDateTime(struct ArrowSchema* schema, enum ArrowType type,
                                          enum ArrowTimeUnit time_unit,
                                          const char* timezone);

/// \brief Seet the format field of a union schema
///
/// Returns EINVAL for a type that is not NANOARROW_TYPE_DENSE_UNION
/// or NANOARROW_TYPE_SPARSE_UNION. The specified number of children are
/// allocated, and initialized.
ArrowErrorCode ArrowSchemaSetTypeUnion(struct ArrowSchema* schema, enum ArrowType type,
                                       int64_t n_children);

/// \brief Make a (recursive) copy of a schema
///
/// Allocates and copies fields of schema into schema_out.
ArrowErrorCode ArrowSchemaDeepCopy(struct ArrowSchema* schema,
                                   struct ArrowSchema* schema_out);

/// \brief Copy format into schema->format
///
/// schema must have been allocated using ArrowSchemaInitFromType() or
/// ArrowSchemaDeepCopy().
ArrowErrorCode ArrowSchemaSetFormat(struct ArrowSchema* schema, const char* format);

/// \brief Copy name into schema->name
///
/// schema must have been allocated using ArrowSchemaInitFromType() or
/// ArrowSchemaDeepCopy().
ArrowErrorCode ArrowSchemaSetName(struct ArrowSchema* schema, const char* name);

/// \brief Copy metadata into schema->metadata
///
/// schema must have been allocated using ArrowSchemaInitFromType() or
/// ArrowSchemaDeepCopy.
ArrowErrorCode ArrowSchemaSetMetadata(struct ArrowSchema* schema, const char* metadata);

/// \brief Allocate the schema->children array
///
/// Includes the memory for each child struct ArrowSchema.
/// schema must have been allocated using ArrowSchemaInitFromType() or
/// ArrowSchemaDeepCopy().
ArrowErrorCode ArrowSchemaAllocateChildren(struct ArrowSchema* schema,
                                           int64_t n_children);

/// \brief Allocate the schema->dictionary member
///
/// schema must have been allocated using ArrowSchemaInitFromType() or
/// ArrowSchemaDeepCopy().
ArrowErrorCode ArrowSchemaAllocateDictionary(struct ArrowSchema* schema);

/// @}

/// \defgroup nanoarrow-metadata Create, read, and modify schema metadata
///
/// @{

/// \brief Reader for key/value pairs in schema metadata
///
/// The ArrowMetadataReader does not own any data and is only valid
/// for the lifetime of the underlying metadata pointer.
struct ArrowMetadataReader {
  /// \brief A metadata string from a schema->metadata field.
  const char* metadata;

  /// \brief The current offset into the metadata string
  int64_t offset;

  /// \brief The number of remaining keys
  int32_t remaining_keys;
};

/// \brief Initialize an ArrowMetadataReader
ArrowErrorCode ArrowMetadataReaderInit(struct ArrowMetadataReader* reader,
                                       const char* metadata);

/// \brief Read the next key/value pair from an ArrowMetadataReader
ArrowErrorCode ArrowMetadataReaderRead(struct ArrowMetadataReader* reader,
                                       struct ArrowStringView* key_out,
                                       struct ArrowStringView* value_out);

/// \brief The number of bytes in in a key/value metadata string
int64_t ArrowMetadataSizeOf(const char* metadata);

/// \brief Check for a key in schema metadata
char ArrowMetadataHasKey(const char* metadata, struct ArrowStringView key);

/// \brief Extract a value from schema metadata
///
/// If key does not exist in metadata, value_out is unmodified
ArrowErrorCode ArrowMetadataGetValue(const char* metadata, struct ArrowStringView key,
                                     struct ArrowStringView* value_out);

/// \brief Initialize a builder for schema metadata from key/value pairs
///
/// metadata can be an existing metadata string or NULL to initialize
/// an empty metadata string.
ArrowErrorCode ArrowMetadataBuilderInit(struct ArrowBuffer* buffer, const char* metadata);

/// \brief Append a key/value pair to a buffer containing serialized metadata
ArrowErrorCode ArrowMetadataBuilderAppend(struct ArrowBuffer* buffer,
                                          struct ArrowStringView key,
                                          struct ArrowStringView value);

/// \brief Set a key/value pair to a buffer containing serialized metadata
///
/// Ensures that the only entry for key in the metadata is set to value.
/// This function maintains the existing position of (the first instance of)
/// key if present in the data.
ArrowErrorCode ArrowMetadataBuilderSet(struct ArrowBuffer* buffer,
                                       struct ArrowStringView key,
                                       struct ArrowStringView value);

/// \brief Remove a key from a buffer containing serialized metadata
ArrowErrorCode ArrowMetadataBuilderRemove(struct ArrowBuffer* buffer,
                                          struct ArrowStringView key);

/// @}

/// \defgroup nanoarrow-schema-view Reading schemas
///
/// @{

/// \brief A non-owning view of a parsed ArrowSchema
///
/// Contains more readily extractable values than a raw ArrowSchema.
/// Clients can stack or statically allocate this structure but are
/// encouraged to use the provided getters to ensure forward
/// compatibility.
struct ArrowSchemaView {
  /// \brief A pointer to the schema represented by this view
  struct ArrowSchema* schema;

  /// \brief The data type represented by the schema
  ///
  /// This value may be NANOARROW_TYPE_DICTIONARY if the schema has a
  /// non-null dictionary member; datetime types are valid values.
  /// This value will never be NANOARROW_TYPE_EXTENSION (see
  /// extension_name and/or extension_metadata to check for
  /// an extension type).
  enum ArrowType type;

  /// \brief The storage data type represented by the schema
  ///
  /// This value will never be NANOARROW_TYPE_DICTIONARY, NANOARROW_TYPE_EXTENSION
  /// or any datetime type. This value represents only the type required to
  /// interpret the buffers in the array.
  enum ArrowType storage_type;

  /// \brief The storage layout represented by the schema
  struct ArrowLayout layout;

  /// \brief The extension type name if it exists
  ///
  /// If the ARROW:extension:name key is present in schema.metadata,
  /// extension_name.data will be non-NULL.
  struct ArrowStringView extension_name;

  /// \brief The extension type metadata if it exists
  ///
  /// If the ARROW:extension:metadata key is present in schema.metadata,
  /// extension_metadata.data will be non-NULL.
  struct ArrowStringView extension_metadata;

  /// \brief Format fixed size parameter
  ///
  /// This value is set when parsing a fixed-size binary or fixed-size
  /// list schema; this value is undefined for other types. For a
  /// fixed-size binary schema this value is in bytes; for a fixed-size
  /// list schema this value refers to the number of child elements for
  /// each element of the parent.
  int32_t fixed_size;

  /// \brief Decimal bitwidth
  ///
  /// This value is set when parsing a decimal type schema;
  /// this value is undefined for other types.
  int32_t decimal_bitwidth;

  /// \brief Decimal precision
  ///
  /// This value is set when parsing a decimal type schema;
  /// this value is undefined for other types.
  int32_t decimal_precision;

  /// \brief Decimal scale
  ///
  /// This value is set when parsing a decimal type schema;
  /// this value is undefined for other types.
  int32_t decimal_scale;

  /// \brief Format time unit parameter
  ///
  /// This value is set when parsing a date/time type. The value is
  /// undefined for other types.
  enum ArrowTimeUnit time_unit;

  /// \brief Format timezone parameter
  ///
  /// This value is set when parsing a timestamp type and represents
  /// the timezone format parameter. This value points to
  /// data within the schema and is undefined for other types.
  const char* timezone;

  /// \brief Union type ids parameter
  ///
  /// This value is set when parsing a union type and represents
  /// type ids parameter. This value points to
  /// data within the schema and is undefined for other types.
  const char* union_type_ids;
};

/// \brief Initialize an ArrowSchemaView
ArrowErrorCode ArrowSchemaViewInit(struct ArrowSchemaView* schema_view,
                                   struct ArrowSchema* schema, struct ArrowError* error);

/// @}

/// \defgroup nanoarrow-buffer Owning, growable buffers
///
/// @{

/// \brief Initialize an ArrowBuffer
///
/// Initialize a buffer with a NULL, zero-size buffer using the default
/// buffer allocator.
static inline void ArrowBufferInit(struct ArrowBuffer* buffer);

/// \brief Set a newly-initialized buffer's allocator
///
/// Returns EINVAL if the buffer has already been allocated.
static inline ArrowErrorCode ArrowBufferSetAllocator(
    struct ArrowBuffer* buffer, struct ArrowBufferAllocator allocator);

/// \brief Reset an ArrowBuffer
///
/// Releases the buffer using the allocator's free method if
/// the buffer's data member is non-null, sets the data member
/// to NULL, and sets the buffer's size and capacity to 0.
static inline void ArrowBufferReset(struct ArrowBuffer* buffer);

/// \brief Move an ArrowBuffer
///
/// Transfers the buffer data and lifecycle management to another
/// address and resets buffer.
static inline void ArrowBufferMove(struct ArrowBuffer* src, struct ArrowBuffer* dst);

/// \brief Grow or shrink a buffer to a given capacity
///
/// When shrinking the capacity of the buffer, the buffer is only reallocated
/// if shrink_to_fit is non-zero. Calling ArrowBufferResize() does not
/// adjust the buffer's size member except to ensure that the invariant
/// capacity >= size remains true.
static inline ArrowErrorCode ArrowBufferResize(struct ArrowBuffer* buffer,
                                               int64_t new_capacity_bytes,
                                               char shrink_to_fit);

/// \brief Ensure a buffer has at least a given additional capacity
///
/// Ensures that the buffer has space to append at least
/// additional_size_bytes, overallocating when required.
static inline ArrowErrorCode ArrowBufferReserve(struct ArrowBuffer* buffer,
                                                int64_t additional_size_bytes);

/// \brief Write data to buffer and increment the buffer size
///
/// This function does not check that buffer has the required capacity
static inline void ArrowBufferAppendUnsafe(struct ArrowBuffer* buffer, const void* data,
                                           int64_t size_bytes);

/// \brief Write data to buffer and increment the buffer size
///
/// This function writes and ensures that the buffer has the required capacity,
/// possibly by reallocating the buffer. Like ArrowBufferReserve, this will
/// overallocate when reallocation is required.
static inline ArrowErrorCode ArrowBufferAppend(struct ArrowBuffer* buffer,
                                               const void* data, int64_t size_bytes);

/// \brief Write fill to buffer and increment the buffer size
///
/// This function writes the specified number of fill bytes and
/// ensures that the buffer has the required capacity,
static inline ArrowErrorCode ArrowBufferAppendFill(struct ArrowBuffer* buffer,
                                                   uint8_t value, int64_t size_bytes);

/// \brief Write an 8-bit integer to a buffer
static inline ArrowErrorCode ArrowBufferAppendInt8(struct ArrowBuffer* buffer,
                                                   int8_t value);

/// \brief Write an unsigned 8-bit integer to a buffer
static inline ArrowErrorCode ArrowBufferAppendUInt8(struct ArrowBuffer* buffer,
                                                    uint8_t value);

/// \brief Write a 16-bit integer to a buffer
static inline ArrowErrorCode ArrowBufferAppendInt16(struct ArrowBuffer* buffer,
                                                    int16_t value);

/// \brief Write an unsigned 16-bit integer to a buffer
static inline ArrowErrorCode ArrowBufferAppendUInt16(struct ArrowBuffer* buffer,
                                                     uint16_t value);

/// \brief Write a 32-bit integer to a buffer
static inline ArrowErrorCode ArrowBufferAppendInt32(struct ArrowBuffer* buffer,
                                                    int32_t value);

/// \brief Write an unsigned 32-bit integer to a buffer
static inline ArrowErrorCode ArrowBufferAppendUInt32(struct ArrowBuffer* buffer,
                                                     uint32_t value);

/// \brief Write a 64-bit integer to a buffer
static inline ArrowErrorCode ArrowBufferAppendInt64(struct ArrowBuffer* buffer,
                                                    int64_t value);

/// \brief Write an unsigned 64-bit integer to a buffer
static inline ArrowErrorCode ArrowBufferAppendUInt64(struct ArrowBuffer* buffer,
                                                     uint64_t value);

/// \brief Write a double to a buffer
static inline ArrowErrorCode ArrowBufferAppendDouble(struct ArrowBuffer* buffer,
                                                     double value);

/// \brief Write a float to a buffer
static inline ArrowErrorCode ArrowBufferAppendFloat(struct ArrowBuffer* buffer,
                                                    float value);

/// \brief Write an ArrowStringView to a buffer
static inline ArrowErrorCode ArrowBufferAppendStringView(struct ArrowBuffer* buffer,
                                                         struct ArrowStringView value);

/// \brief Write an ArrowBufferView to a buffer
static inline ArrowErrorCode ArrowBufferAppendBufferView(struct ArrowBuffer* buffer,
                                                         struct ArrowBufferView value);

/// @}

/// \defgroup nanoarrow-bitmap Bitmap utilities
///
/// @{

/// \brief Extract a boolean value from a bitmap
static inline int8_t ArrowBitGet(const uint8_t* bits, int64_t i);

/// \brief Set a boolean value to a bitmap to true
static inline void ArrowBitSet(uint8_t* bits, int64_t i);

/// \brief Set a boolean value to a bitmap to false
static inline void ArrowBitClear(uint8_t* bits, int64_t i);

/// \brief Set a boolean value to a bitmap
static inline void ArrowBitSetTo(uint8_t* bits, int64_t i, uint8_t value);

/// \brief Set a boolean value to a range in a bitmap
static inline void ArrowBitsSetTo(uint8_t* bits, int64_t start_offset, int64_t length,
                                  uint8_t bits_are_set);

/// \brief Count true values in a bitmap
static inline int64_t ArrowBitCountSet(const uint8_t* bits, int64_t i_from, int64_t i_to);

/// \brief Extract int8 boolean values from a range in a bitmap
static inline void ArrowBitsUnpackInt8(const uint8_t* bits, int64_t start_offset,
                                       int64_t length, int8_t* out);

/// \brief Extract int32 boolean values from a range in a bitmap
static inline void ArrowBitsUnpackInt32(const uint8_t* bits, int64_t start_offset,
                                        int64_t length, int32_t* out);

/// \brief Initialize an ArrowBitmap
///
/// Initialize the builder's buffer, empty its cache, and reset the size to zero
static inline void ArrowBitmapInit(struct ArrowBitmap* bitmap);

/// \brief Move an ArrowBitmap
///
/// Transfers the underlying buffer data and lifecycle management to another
/// address and resets the bitmap.
static inline void ArrowBitmapMove(struct ArrowBitmap* src, struct ArrowBitmap* dst);

/// \brief Ensure a bitmap builder has at least a given additional capacity
///
/// Ensures that the buffer has space to append at least
/// additional_size_bits, overallocating when required.
static inline ArrowErrorCode ArrowBitmapReserve(struct ArrowBitmap* bitmap,
                                                int64_t additional_size_bits);

/// \brief Grow or shrink a bitmap to a given capacity
///
/// When shrinking the capacity of the bitmap, the bitmap is only reallocated
/// if shrink_to_fit is non-zero. Calling ArrowBitmapResize() does not
/// adjust the buffer's size member except when shrinking new_capacity_bits
/// to a value less than the current number of bits in the bitmap.
static inline ArrowErrorCode ArrowBitmapResize(struct ArrowBitmap* bitmap,
                                               int64_t new_capacity_bits,
                                               char shrink_to_fit);

/// \brief Reserve space for and append zero or more of the same boolean value to a bitmap
static inline ArrowErrorCode ArrowBitmapAppend(struct ArrowBitmap* bitmap,
                                               uint8_t bits_are_set, int64_t length);

/// \brief Append zero or more of the same boolean value to a bitmap
static inline void ArrowBitmapAppendUnsafe(struct ArrowBitmap* bitmap,
                                           uint8_t bits_are_set, int64_t length);

/// \brief Append boolean values encoded as int8_t to a bitmap
///
/// The values must all be 0 or 1.
static inline void ArrowBitmapAppendInt8Unsafe(struct ArrowBitmap* bitmap,
                                               const int8_t* values, int64_t n_values);

/// \brief Append boolean values encoded as int32_t to a bitmap
///
/// The values must all be 0 or 1.
static inline void ArrowBitmapAppendInt32Unsafe(struct ArrowBitmap* bitmap,
                                                const int32_t* values, int64_t n_values);

/// \brief Reset a bitmap builder
///
/// Releases any memory held by buffer, empties the cache, and resets the size to zero
static inline void ArrowBitmapReset(struct ArrowBitmap* bitmap);

/// @}

/// \defgroup nanoarrow-array Creating arrays
///
/// These functions allocate, copy, and destroy ArrowArray structures.
/// Once an ArrowArray has been initialized via ArrowArrayInitFromType()
/// or ArrowArrayInitFromSchema(), the caller is responsible for releasing
/// it using the embedded release callback.
///
/// @{

/// \brief Initialize the fields of an array
///
/// Initializes the fields and release callback of array. Caller
/// is responsible for calling the array->release callback if
/// NANOARROW_OK is returned.
ArrowErrorCode ArrowArrayInitFromType(struct ArrowArray* array,
                                      enum ArrowType storage_type);

/// \brief Initialize the contents of an ArrowArray from an ArrowSchema
///
/// Caller is responsible for calling the array->release callback if
/// NANOARROW_OK is returned.
ArrowErrorCode ArrowArrayInitFromSchema(struct ArrowArray* array,
                                        struct ArrowSchema* schema,
                                        struct ArrowError* error);

/// \brief Initialize the contents of an ArrowArray from an ArrowArrayView
///
/// Caller is responsible for calling the array->release callback if
/// NANOARROW_OK is returned.
ArrowErrorCode ArrowArrayInitFromArrayView(struct ArrowArray* array,
                                           struct ArrowArrayView* array_view,
                                           struct ArrowError* error);

/// \brief Allocate the array->children array
///
/// Includes the memory for each child struct ArrowArray,
/// whose members are marked as released and may be subsequently initialized
/// with ArrowArrayInitFromType() or moved from an existing ArrowArray.
/// schema must have been allocated using ArrowArrayInitFromType().
ArrowErrorCode ArrowArrayAllocateChildren(struct ArrowArray* array, int64_t n_children);

/// \brief Allocate the array->dictionary member
///
/// Includes the memory for the struct ArrowArray, whose contents
/// is marked as released and may be subsequently initialized
/// with ArrowArrayInitFromType() or moved from an existing ArrowArray.
/// array must have been allocated using ArrowArrayInitFromType()
ArrowErrorCode ArrowArrayAllocateDictionary(struct ArrowArray* array);

/// \brief Set the validity bitmap of an ArrowArray
///
/// array must have been allocated using ArrowArrayInitFromType()
void ArrowArraySetValidityBitmap(struct ArrowArray* array, struct ArrowBitmap* bitmap);

/// \brief Set a buffer of an ArrowArray
///
/// array must have been allocated using ArrowArrayInitFromType()
ArrowErrorCode ArrowArraySetBuffer(struct ArrowArray* array, int64_t i,
                                   struct ArrowBuffer* buffer);

/// \brief Get the validity bitmap of an ArrowArray
///
/// array must have been allocated using ArrowArrayInitFromType()
static inline struct ArrowBitmap* ArrowArrayValidityBitmap(struct ArrowArray* array);

/// \brief Get a buffer of an ArrowArray
///
/// array must have been allocated using ArrowArrayInitFromType()
static inline struct ArrowBuffer* ArrowArrayBuffer(struct ArrowArray* array, int64_t i);

/// \brief Start element-wise appending to an ArrowArray
///
/// Initializes any values needed to use ArrowArrayAppend*() functions.
/// All element-wise appenders append by value and return EINVAL if the exact value
/// cannot be represented by the underlying storage type.
/// array must have been allocated using ArrowArrayInitFromType()
static inline ArrowErrorCode ArrowArrayStartAppending(struct ArrowArray* array);

/// \brief Reserve space for future appends
///
/// For buffer sizes that can be calculated (i.e., not string data buffers or
/// child array sizes for non-fixed-size arrays), recursively reserve space for
/// additional elements. This is useful for reducing the number of reallocations
/// that occur using the item-wise appenders.
ArrowErrorCode ArrowArrayReserve(struct ArrowArray* array,
                                 int64_t additional_size_elements);

/// \brief Append a null value to an array
static inline ArrowErrorCode ArrowArrayAppendNull(struct ArrowArray* array, int64_t n);

/// \brief Append an empty, non-null value to an array
static inline ArrowErrorCode ArrowArrayAppendEmpty(struct ArrowArray* array, int64_t n);

/// \brief Append a signed integer value to an array
///
/// Returns NANOARROW_OK if value can be exactly represented by
/// the underlying storage type or EINVAL otherwise (e.g., value
/// is outside the valid array range).
static inline ArrowErrorCode ArrowArrayAppendInt(struct ArrowArray* array, int64_t value);

/// \brief Append an unsigned integer value to an array
///
/// Returns NANOARROW_OK if value can be exactly represented by
/// the underlying storage type or EINVAL otherwise (e.g., value
/// is outside the valid array range).
static inline ArrowErrorCode ArrowArrayAppendUInt(struct ArrowArray* array,
                                                  uint64_t value);

/// \brief Append a double value to an array
///
/// Returns NANOARROW_OK if value can be exactly represented by
/// the underlying storage type or EINVAL otherwise (e.g., value
/// is outside the valid array range or there is an attempt to append
/// a non-integer to an array with an integer storage type).
static inline ArrowErrorCode ArrowArrayAppendDouble(struct ArrowArray* array,
                                                    double value);

/// \brief Append a string of bytes to an array
///
/// Returns NANOARROW_OK if value can be exactly represented by
/// the underlying storage type, EOVERFLOW if appending value would overflow
/// the offset type (e.g., if the data buffer would be larger than 2 GB for a
/// non-large string type), or EINVAL otherwise (e.g., the underlying array is not a
/// binary, string, large binary, large string, or fixed-size binary array, or value is
/// the wrong size for a fixed-size binary array).
static inline ArrowErrorCode ArrowArrayAppendBytes(struct ArrowArray* array,
                                                   struct ArrowBufferView value);

/// \brief Append a string value to an array
///
/// Returns NANOARROW_OK if value can be exactly represented by
/// the underlying storage type, EOVERFLOW if appending value would overflow
/// the offset type (e.g., if the data buffer would be larger than 2 GB for a
/// non-large string type), or EINVAL otherwise (e.g., the underlying array is not a
/// string or large string array).
static inline ArrowErrorCode ArrowArrayAppendString(struct ArrowArray* array,
                                                    struct ArrowStringView value);

/// \brief Append a Interval to an array
///
/// Returns NANOARROW_OK if value can be exactly represented by
/// the underlying storage type or EINVAL otherwise.
static inline ArrowErrorCode ArrowArrayAppendInterval(struct ArrowArray* array,
                                                      struct ArrowInterval* value);

/// \brief Append a decimal value to an array
///
/// Returns NANOARROW_OK if array is a decimal array with the appropriate
/// bitwidth or EINVAL otherwise.
static inline ArrowErrorCode ArrowArrayAppendDecimal(struct ArrowArray* array,
                                                     struct ArrowDecimal* value);

/// \brief Finish a nested array element
///
/// Appends a non-null element to the array based on the first child's current
/// length. Returns NANOARROW_OK if the item was successfully added, EOVERFLOW
/// if the child of a list or map array would exceed INT_MAX elements, or EINVAL
/// if the underlying storage type is not a struct, list, large list, or fixed-size
/// list, or if there was an attempt to add a struct or fixed-size list element where the
/// length of the child array(s) did not match the expected length.
static inline ArrowErrorCode ArrowArrayFinishElement(struct ArrowArray* array);

/// \brief Finish a union array element
///
/// Appends an element to the union type ids buffer and increments array->length.
/// For sparse unions, up to one element is added to non type-id children. Returns
/// EINVAL if the underlying storage type is not a union, if type_id is not valid,
/// or if child sizes after appending are inconsistent.
static inline ArrowErrorCode ArrowArrayFinishUnionElement(struct ArrowArray* array,
                                                          int8_t type_id);

/// \brief Shrink buffer capacity to the size required
///
/// Also applies shrinking to any child arrays. array must have been allocated using
/// ArrowArrayInitFromType
static inline ArrowErrorCode ArrowArrayShrinkToFit(struct ArrowArray* array);

/// \brief Finish building an ArrowArray
///
/// Flushes any pointers from internal buffers that may have been reallocated
/// into array->buffers and checks the actual size of the buffers
/// against the expected size based on the final length.
/// array must have been allocated using ArrowArrayInitFromType()
ArrowErrorCode ArrowArrayFinishBuildingDefault(struct ArrowArray* array,
                                               struct ArrowError* error);

/// \brief Finish building an ArrowArray with explicit validation
///
/// Finish building with an explicit validation level. This could perform less validation
/// (i.e. NANOARROW_VALIDATION_LEVEL_NONE or NANOARROW_VALIDATION_LEVEL_MINIMAL) if CPU
/// buffer data access is not possible or more validation (i.e.,
/// NANOARROW_VALIDATION_LEVEL_FULL) if buffer content was obtained from an untrusted or
/// corruptible source.
ArrowErrorCode ArrowArrayFinishBuilding(struct ArrowArray* array,
                                        enum ArrowValidationLevel validation_level,
                                        struct ArrowError* error);

/// @}

/// \defgroup nanoarrow-array-view Reading arrays
///
/// These functions read and validate the contents ArrowArray structures.
///
/// @{

/// \brief Initialize the contents of an ArrowArrayView
void ArrowArrayViewInitFromType(struct ArrowArrayView* array_view,
                                enum ArrowType storage_type);

/// \brief Move an ArrowArrayView
///
/// Transfers the ArrowArrayView data and lifecycle management to another
/// address and resets the contents of src.
static inline void ArrowArrayViewMove(struct ArrowArrayView* src,
                                      struct ArrowArrayView* dst);

/// \brief Initialize the contents of an ArrowArrayView from an ArrowSchema
ArrowErrorCode ArrowArrayViewInitFromSchema(struct ArrowArrayView* array_view,
                                            struct ArrowSchema* schema,
                                            struct ArrowError* error);


//ArrowErrorCode ArrowArrayViewInitFromArray(struct ArrowArrayView* array_view,
//                                           struct ArrowArray* array);

/// \brief Allocate the array_view->children array
///
/// Includes the memory for each child struct ArrowArrayView
ArrowErrorCode ArrowArrayViewAllocateChildren(struct ArrowArrayView* array_view,
                                              int64_t n_children);

/// \brief Allocate array_view->dictionary
ArrowErrorCode ArrowArrayViewAllocateDictionary(struct ArrowArrayView* array_view);

/// \brief Set data-independent buffer sizes from length
void ArrowArrayViewSetLength(struct ArrowArrayView* array_view, int64_t length);

/// \brief Set buffer sizes and data pointers from an ArrowArray
ArrowErrorCode ArrowArrayViewSetArray(struct ArrowArrayView* array_view,
                                      struct ArrowArray* array, struct ArrowError* error);

/// \brief Set buffer sizes and data pointers from an ArrowArray except for those
/// that require dereferencing buffer content.
ArrowErrorCode ArrowArrayViewSetArrayMinimal(struct ArrowArrayView* array_view,
                                             struct ArrowArray* array,
                                             struct ArrowError* error);

/// \brief Performs checks on the content of an ArrowArrayView
///
/// If using ArrowArrayViewSetArray() to back array_view with an ArrowArray,
/// the buffer sizes and some content (fist and last offset) have already
/// been validated at the "default" level. If setting the buffer pointers
/// and sizes otherwise, you may wish to perform checks at a different level. See
/// documentation for ArrowValidationLevel for the details of checks performed
/// at each level.
ArrowErrorCode ArrowArrayViewValidate(struct ArrowArrayView* array_view,
                                      enum ArrowValidationLevel validation_level,
                                      struct ArrowError* error);

/// \brief Reset the contents of an ArrowArrayView and frees resources
void ArrowArrayViewReset(struct ArrowArrayView* array_view);

/// \brief Check for a null element in an ArrowArrayView
static inline int8_t ArrowArrayViewIsNull(struct ArrowArrayView* array_view, int64_t i);

/// \brief Get the type id of a union array element
static inline int8_t ArrowArrayViewUnionTypeId(struct ArrowArrayView* array_view,
                                               int64_t i);

/// \brief Get the child index of a union array element
static inline int8_t ArrowArrayViewUnionChildIndex(struct ArrowArrayView* array_view,
                                                   int64_t i);

/// \brief Get the index to use into the relevant union child array
static inline int64_t ArrowArrayViewUnionChildOffset(struct ArrowArrayView* array_view,
                                                     int64_t i);

/// \brief Get an element in an ArrowArrayView as an integer
///
/// This function does not check for null values, that values are actually integers, or
/// that values are within a valid range for an int64.
static inline int64_t ArrowArrayViewGetIntUnsafe(struct ArrowArrayView* array_view,
                                                 int64_t i);

/// \brief Get an element in an ArrowArrayView as an unsigned integer
///
/// This function does not check for null values, that values are actually integers, or
/// that values are within a valid range for a uint64.
static inline uint64_t ArrowArrayViewGetUIntUnsafe(struct ArrowArrayView* array_view,
                                                   int64_t i);

/// \brief Get an element in an ArrowArrayView as a double
///
/// This function does not check for null values, or
/// that values are within a valid range for a double.
static inline double ArrowArrayViewGetDoubleUnsafe(struct ArrowArrayView* array_view,
                                                   int64_t i);

/// \brief Get an element in an ArrowArrayView as an ArrowStringView
///
/// This function does not check for null values.
static inline struct ArrowStringView ArrowArrayViewGetStringUnsafe(
    struct ArrowArrayView* array_view, int64_t i);

/// \brief Get an element in an ArrowArrayView as an ArrowBufferView
///
/// This function does not check for null values.
static inline struct ArrowBufferView ArrowArrayViewGetBytesUnsafe(
    struct ArrowArrayView* array_view, int64_t i);

/// \brief Get an element in an ArrowArrayView as an ArrowDecimal
///
/// This function does not check for null values. The out parameter must
/// be initialized with ArrowDecimalInit() with the proper parameters for this
/// type before calling this for the first time.
static inline void ArrowArrayViewGetDecimalUnsafe(struct ArrowArrayView* array_view,
                                                  int64_t i, struct ArrowDecimal* out);

/// @}

/// \defgroup nanoarrow-basic-array-stream Basic ArrowArrayStream implementation
///
/// An implementation of an ArrowArrayStream based on a collection of
/// zero or more previously-existing ArrowArray objects. Users should
/// initialize and/or validate the contents before transferring the
/// responsibility of the ArrowArrayStream elsewhere.
///
/// @{

/// \brief Initialize an ArrowArrayStream backed by this implementation
///
/// This function moves the ownership of schema to the array_stream. If
/// this function returns NANOARROW_OK, the caller is responsible for
/// releasing the ArrowArrayStream.
ArrowErrorCode ArrowBasicArrayStreamInit(struct ArrowArrayStream* array_stream,
                                         struct ArrowSchema* schema, int64_t n_arrays);

/// \brief Set the ith ArrowArray in this ArrowArrayStream.
///
/// array_stream must have been initialized with ArrowBasicArrayStreamInit().
/// This function move the ownership of array to the array_stream. i must
/// be greater than zero and less than the value of n_arrays passed in
/// ArrowBasicArrayStreamInit(). Callers are not required to fill all
/// n_arrays members (i.e., n_arrays is a maximum bound).
void ArrowBasicArrayStreamSetArray(struct ArrowArrayStream* array_stream, int64_t i,
                                   struct ArrowArray* array);

/// \brief Validate the contents of this ArrowArrayStream
///
/// array_stream must have been initialized with ArrowBasicArrayStreamInit().
/// This function uses ArrowArrayStreamInitFromSchema() and ArrowArrayStreamSetArray()
/// to validate the contents of the arrays.
ArrowErrorCode ArrowBasicArrayStreamValidate(struct ArrowArrayStream* array_stream,
                                             struct ArrowError* error);

/// @}

// Inline function definitions
#include "array_inline.h"
#include "buffer_inline.h"

#ifdef __cplusplus
}
#endif

#endif
