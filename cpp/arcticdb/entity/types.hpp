/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/util/preconditions.hpp>
#include <arcticdb/util/constructors.hpp>
#include <arcticdb/util/variant.hpp>
#include <arcticdb/log/log.hpp>
#include <arcticdb/entity/protobufs.hpp>
#include <google/protobuf/util/message_differencer.h>

#include <folly/Range.h>
#include <fmt/format.h>
#include <folly/Likely.h>
#include <pybind11/pybind11.h>
#include <pybind11/numpy.h>

#include <algorithm>
#include <cstdint>
#include <vector>
#include <string>
#include <exception>
#include <type_traits>
#include <iostream>
#include <optional>
#include <variant>

namespace arcticdb::entity {

 enum class SortedValue : uint8_t {
    UNKNOWN = 0,
    UNSORTED = 1,
    ASCENDING = 2,
    DESCENDING = 3,
};

inline arcticc::pb2::descriptors_pb2::SortedValue sorted_value_to_proto(SortedValue sorted) {
    switch (sorted) {
    case SortedValue::UNSORTED:
        return arcticc::pb2::descriptors_pb2::SortedValue::UNSORTED;
    case SortedValue::DESCENDING:
        return arcticc::pb2::descriptors_pb2::SortedValue::DESCENDING;
    case SortedValue::ASCENDING:
        return arcticc::pb2::descriptors_pb2::SortedValue::ASCENDING;
    default:
        return arcticc::pb2::descriptors_pb2::SortedValue::UNKNOWN;
    }
}

inline SortedValue sorted_value_from_proto(const arcticc::pb2::descriptors_pb2::SortedValue &sorted_proto) {
    switch (sorted_proto) {
    case arcticc::pb2::descriptors_pb2::SortedValue::UNSORTED:
        return SortedValue::UNSORTED;
    case arcticc::pb2::descriptors_pb2::SortedValue::DESCENDING:
        return SortedValue::DESCENDING;
    case arcticc::pb2::descriptors_pb2::SortedValue::ASCENDING:
        return SortedValue::ASCENDING;
    default:
        return SortedValue::UNKNOWN;
    }
}

using NumericId = int64_t;
using StringId = std::string;
using VariantId = std::variant<NumericId, StringId>;
using StreamId = VariantId;
using SnapshotId = VariantId;
using VersionId = uint64_t;
using SignedVersionId = int64_t;
using GenerationId = VersionId;
using timestamp = int64_t;
using shape_t = ssize_t;
using stride_t = ssize_t;
using position_t = ssize_t;

/** The VariantId holds int64 (NumericId) but is also used to store sizes up to uint64, so needs safe conversion */
inline NumericId safe_convert_to_numeric_id(uint64_t input, const char* input_name) {
    util::check(input <= static_cast<uint64_t>(std::numeric_limits<NumericId>::max()),
        "{} greater than 2^63 is not supported.", input_name);
    return static_cast<NumericId>(input);
}

namespace py = pybind11;


// See https://sourceforge.net/p/numpy/mailman/numpy-discussion/thread/1139250278.7538.52.camel%40localhost.localdomain/#msg11998404
constexpr size_t UNICODE_WIDTH = sizeof(Py_UNICODE);
constexpr size_t ASCII_WIDTH = 1;
//TODO: Fix unicode width for windows
#ifndef _WIN32
    static_assert(UNICODE_WIDTH == 4, "Only support python platforms where unicode width is 4");
#endif

// Beware, all the enum values of the field must match exactly the values
// descriptors.proto::TypeDescriptor::ValueType
enum class ValueType : uint8_t {
    UNKNOWN_VALUE_TYPE = 0,
    UINT = 1,
    INT = 2,
    FLOAT = 3,

    BOOL = 4,
    NANOSECONDS_UTC = 5,

//    SYMBOL = 6, // categorical string of low cardinality suitable for dictionary encoding
    ASCII_FIXED = 7, // fixed size string when dim > 1, inputs of type uint8_t, no encoding
    UTF8_FIXED = 8, // fixed size string when dim > 1, inputs of type uint8_t, utf8 encoding
    BYTES = 9, // implies fixed size bytes array when dim > 1, opaque
    // PICKLE = 12, // BYTES + pickle specific encoding

    UTF_DYNAMIC = 11,
    ASCII_DYNAMIC = 12,
    /// Used to represent null types. Each type can be converted to Empty and Empty can be converted to each type.
    EMPTY = 13,
    COUNT // Not a real value type, should not be added to proto descriptor. Used to count the number of items in the enum
};

// Sequence types are composed of more than one element
constexpr bool is_sequence_type(ValueType v){
    return uint8_t(v) >= uint8_t(ValueType::ASCII_FIXED) &&
    uint8_t(v) <= uint8_t(ValueType::ASCII_DYNAMIC);
}

constexpr bool is_numeric_type(ValueType v){
    return v == ValueType::NANOSECONDS_UTC ||
    (uint8_t(v) >= uint8_t(ValueType::UINT) &&
    uint8_t(v) <= uint8_t(ValueType::FLOAT));
}

constexpr bool is_floating_point_type(ValueType v){
    return uint8_t(v) == uint8_t(ValueType::FLOAT);
}

constexpr bool is_time_type(ValueType v){
    return uint8_t(v) == uint8_t(ValueType::NANOSECONDS_UTC);
}

constexpr bool is_integer_type(ValueType v){
    return uint8_t(v) == uint8_t(ValueType::INT) || uint8_t(v) == uint8_t(ValueType::UINT);
}

constexpr bool is_fixed_string_type(ValueType v){
    return v == ValueType::ASCII_FIXED || v == ValueType::UTF8_FIXED;
}

constexpr bool is_dynamic_string_type(ValueType v){
    return is_sequence_type(v) && !is_fixed_string_type(v);
}
constexpr bool is_utf_type(ValueType v) {
    return v == ValueType::UTF8_FIXED || v == ValueType::UTF_DYNAMIC;
}

constexpr bool is_empty_type(ValueType v) {
    return v == ValueType::EMPTY;
}

enum class SizeBits : uint8_t {
    UNKNOWN_SIZE_BITS = 0,
    S8 = 1,
    S16 = 2,
    S32 = 3,
    S64 = 4,
    COUNT
};

constexpr SizeBits get_size_bits(uint8_t size) {
    switch (size) {
        case 2:return SizeBits::S16;
        case 4:return SizeBits::S32;
        case 8:return SizeBits::S64;
        default:return SizeBits::S8;
    }
}

namespace detail{

constexpr uint8_t combine_val_bits(ValueType v, SizeBits b = SizeBits::UNKNOWN_SIZE_BITS) {
    return (static_cast<uint8_t>(v) << 3u) | static_cast<uint8_t>(b);
}

} // namespace anonymous

enum class DataType : uint8_t {
#define DT_COMBINE(TYPE, SIZE) TYPE = combine_val_bits(ValueType::TYPE, SizeBits::SIZE)
    UINT8 = detail::combine_val_bits(ValueType::UINT, SizeBits::S8),
    UINT16 = detail::combine_val_bits(ValueType::UINT, SizeBits::S16),
    UINT32 = detail::combine_val_bits(ValueType::UINT, SizeBits::S32),
    UINT64 = detail::combine_val_bits(ValueType::UINT, SizeBits::S64),
    INT8 = detail::combine_val_bits(ValueType::INT, SizeBits::S8),
    INT16 = detail::combine_val_bits(ValueType::INT, SizeBits::S16),
    INT32 = detail::combine_val_bits(ValueType::INT, SizeBits::S32),
    INT64 = detail::combine_val_bits(ValueType::INT, SizeBits::S64),
    FLOAT32 = detail::combine_val_bits(ValueType::FLOAT, SizeBits::S32),
    FLOAT64 = detail::combine_val_bits(ValueType::FLOAT, SizeBits::S64),
    BOOL8 = detail::combine_val_bits(ValueType::BOOL, SizeBits::S8),
    NANOSECONDS_UTC64 = detail::combine_val_bits(ValueType::NANOSECONDS_UTC, SizeBits::S64),
    ASCII_FIXED64 = detail::combine_val_bits(ValueType::ASCII_FIXED, SizeBits::S64),
    ASCII_DYNAMIC64 = detail::combine_val_bits(ValueType::ASCII_DYNAMIC, SizeBits::S64),
    UTF_FIXED64 = detail::combine_val_bits(ValueType::UTF8_FIXED, SizeBits::S64),
    UTF_DYNAMIC64 = detail::combine_val_bits(ValueType::UTF_DYNAMIC, SizeBits::S64),
    EMPTYVAL = detail::combine_val_bits(ValueType::EMPTY, SizeBits::S64),
#undef DT_COMBINE
    UNKNOWN = 0,
};

std::string_view datatype_to_str(DataType dt);

constexpr DataType combine_data_type(ValueType v, SizeBits b = SizeBits::UNKNOWN_SIZE_BITS) {
    return static_cast<DataType>(detail::combine_val_bits(v, b));
}

// Constructs the corresponding DataType from a given primitive arithmetic type (u/int8_t, float, or double)
template <typename T>
constexpr DataType data_type_from_raw_type() {
    static_assert(std::is_arithmetic_v<T>);
    if constexpr (std::is_floating_point_v<T>) {
        return combine_data_type(ValueType::FLOAT, get_size_bits(sizeof(T)));
    }
    if constexpr(std::is_signed_v<T>) {
        return combine_data_type(ValueType::INT, get_size_bits(sizeof(T)));
    }
    return combine_data_type(ValueType::UINT, get_size_bits(sizeof(T)));
}

constexpr ValueType get_value_type(char specifier) noexcept;

constexpr DataType get_data_type(char specifier, SizeBits size) noexcept {
    return combine_data_type(get_value_type(specifier), size);
}

constexpr ValueType slice_value_type(DataType dt) noexcept {
    return static_cast<ValueType>(static_cast<uint8_t>(dt) >> 3u);
}

constexpr SizeBits slice_bit_size(DataType dt) noexcept {
    return static_cast<SizeBits>(static_cast<uint8_t>(dt) & 0x7u);
}

constexpr size_t get_type_size(DataType dt) noexcept {
    auto s = slice_bit_size(dt);
    return size_t(1) << (size_t(s) - 1);
}

constexpr bool is_sequence_type(DataType v){
    return is_sequence_type(slice_value_type(v));
}

constexpr bool is_numeric_type(DataType v){
    return is_numeric_type(slice_value_type(v));
}

constexpr bool is_bool_type(DataType dt) {
    return slice_value_type(dt) == ValueType::BOOL;
}

constexpr bool is_unsigned_type(DataType dt) {
    return slice_value_type(dt) == ValueType::UINT;
}

constexpr bool is_signed_type(DataType dt) {
    return slice_value_type(dt) == ValueType::INT;
}

constexpr bool is_floating_point_type(DataType v){
    return is_floating_point_type(slice_value_type(v));
}

constexpr bool is_time_type(DataType v){
    return is_time_type(slice_value_type(v));
}

constexpr bool is_integer_type(DataType v){
    return is_integer_type(slice_value_type(v));
}

constexpr bool is_fixed_string_type(DataType v){
    return is_fixed_string_type(slice_value_type(v));
}

constexpr bool is_dynamic_string_type(DataType v){
    return is_dynamic_string_type(slice_value_type(v));
}

constexpr bool is_utf_type(DataType v){
    return is_utf_type(slice_value_type(v));
}

constexpr bool is_empty_type(DataType v){
    return is_empty_type(slice_value_type(v));
}

static_assert(slice_value_type((DataType::UINT16)) == ValueType(1));
static_assert(get_type_size(DataType::UINT32) == 4);
static_assert(get_type_size(DataType::UINT64) == 8);

constexpr  ValueType get_value_type(char specifier) noexcept {
    switch(specifier){
        case 'u': return ValueType::UINT; //  unsigned integer
        case 'i': return ValueType::INT; //  signed integer
        case 'f': return ValueType::FLOAT; //  floating-point
        case 'b': return ValueType::BOOL; //  boolean
        // NOTE: this is safe as of Pandas < 2.0 because `datetime64` _always_ has been using nanosecond resolution,
        // i.e. Pandas < 2.0 _always_ provides `datetime64[ns]` and ignores any other resolution.
        // Yet, this has changed in Pandas 2.0 and other resolution can be used,
        // i.e. Pandas >= 2.0 will also provides `datetime64[us]`, `datetime64[ms]` and `datetime64[s]`.
        // See: https://pandas.pydata.org/docs/dev/whatsnew/v2.0.0.html#construction-with-datetime64-or-timedelta64-dtype-with-unsupported-resolution
        // TODO: for the support of Pandas>=2.0, convert any `datetime` to `datetime64[ns]` before-hand and do not
        // rely uniquely on the resolution-less 'M' specifier if it this doable.
        case 'M': return ValueType::NANOSECONDS_UTC; //  datetime // numpy doesn't support the buffer protocol for datetime64
        case 'U': return ValueType::UTF8_FIXED; //  Unicode
        case 'S': return ValueType::ASCII_FIXED; //  (byte-)string
        case 'O': return ValueType::BYTES; // Fishy, an actual type might be better
        default:
            return ValueType::UNKNOWN_VALUE_TYPE;    // Unknown
    }
}

constexpr char get_dtype_specifier(ValueType vt){
    switch(vt){
        case ValueType::UINT: return 'u';
        case ValueType::INT:  return 'i';
        case ValueType::FLOAT: return 'f';
        case ValueType::BOOL: return 'b';
        // NOTE: this is safe as of Pandas < 2.0 because `datetime64` _always_ has been using nanosecond resolution,
        // i.e. Pandas < 2.0 _always_ provides `datetime64[ns]` and ignores any other resolution.
        // Yet, this has changed in Pandas 2.0 and other resolution can be used,
        // i.e. Pandas >= 2.0 will also provides `datetime64[us]`, `datetime64[ms]` and `datetime64[s]`.
        // See: https://pandas.pydata.org/docs/dev/whatsnew/v2.0.0.html#construction-with-datetime64-or-timedelta64-dtype-with-unsupported-resolution
        // TODO: for the support of Pandas>=2.0, convert any `datetime` to `datetime64[ns]` before-hand and do not
        // rely uniquely on the resolution-less 'M' specifier if it this doable.
        case ValueType::NANOSECONDS_UTC: return 'M';
        case ValueType::UTF8_FIXED: return 'U';
        case ValueType::ASCII_FIXED: return 'S';
        case ValueType::BYTES: return 'O';
        case ValueType::EMPTY: return 'O';
        default:
            return 'x';
    }
}

constexpr char get_dtype_specifier(DataType dt){
    return get_dtype_specifier(slice_value_type(dt));
}

static_assert(get_value_type('u') == ValueType::UINT);

struct DataTypeTagBase {};

template<DataType DT>
struct DataTypeTag {
};

#define DATA_TYPE_TAG(__DT__, __T__)  \
template<> \
struct DataTypeTag<DataType::__DT__> : public DataTypeTagBase { \
    static constexpr DataType data_type = DataType::__DT__; \
    static constexpr ValueType value_type = slice_value_type(DataType::__DT__); \
    static constexpr SizeBits size_bits = slice_bit_size(DataType::__DT__); \
    using raw_type = __T__; \
}; \
using TAG_##__DT__ = DataTypeTag<DataType::__DT__>;

using timestamp = int64_t;


DATA_TYPE_TAG(UINT8, std::uint8_t)
DATA_TYPE_TAG(UINT16, std::uint16_t)
DATA_TYPE_TAG(UINT32, std::uint32_t)
DATA_TYPE_TAG(UINT64, std::uint64_t)
DATA_TYPE_TAG(INT8, std::int8_t)
DATA_TYPE_TAG(INT16, std::int16_t)
DATA_TYPE_TAG(INT32, std::int32_t)
DATA_TYPE_TAG(INT64, std::int64_t)
DATA_TYPE_TAG(FLOAT32, float)
DATA_TYPE_TAG(FLOAT64, double)
DATA_TYPE_TAG(BOOL8, bool)
DATA_TYPE_TAG(NANOSECONDS_UTC64, timestamp)
DATA_TYPE_TAG(ASCII_FIXED64, std::uint64_t)
DATA_TYPE_TAG(ASCII_DYNAMIC64, std::uint64_t)
DATA_TYPE_TAG(UTF_FIXED64, std::uint64_t)
DATA_TYPE_TAG(UTF_DYNAMIC64, std::uint64_t)
DATA_TYPE_TAG(EMPTYVAL, std::uint64_t)
#undef DATA_TYPE_TAG

enum class Dimension : uint8_t {
    Dim0 = 0,
    Dim1 = 1,
    Dim2 = 2,
};

struct DimensionTagBase {
};

template<Dimension dim>
struct DimensionTag {
};

#define DIMENSION(__D__) template<> \
struct DimensionTag<Dimension::Dim##__D__> : public DimensionTagBase { \
    static constexpr Dimension value = Dimension::Dim##__D__; \
}
DIMENSION(0);
DIMENSION(1);
DIMENSION(2);
#undef DIMENSION

Dimension as_dim_checked(uint8_t d);

struct TypeDescriptor;

inline void set_data_type(DataType data_type, arcticdb::proto::descriptors::TypeDescriptor& type_desc) {
    type_desc.set_size_bits(
        static_cast<arcticdb::proto::descriptors::TypeDescriptor_SizeBits>(
            static_cast<std::uint8_t>(slice_bit_size(data_type))));
    type_desc.set_value_type(
        static_cast<arcticdb::proto::descriptors::TypeDescriptor_ValueType>(
            static_cast<std::uint8_t>(slice_value_type(data_type))));
}

inline void set_data_type(DataType data_type, TypeDescriptor& type_desc);

struct TypeDescriptor {
    DataType data_type_;
    Dimension dimension_;

    using Proto = arcticdb::proto::descriptors::TypeDescriptor;

    TypeDescriptor(DataType dt, uint8_t dim) : data_type_(dt), dimension_(as_dim_checked(dim)) {}
    constexpr TypeDescriptor(DataType dt, Dimension dim) : data_type_(dt), dimension_(dim) {}
    constexpr TypeDescriptor(ValueType v, SizeBits b, Dimension dim) :
        data_type_(combine_data_type(v, b)), dimension_(dim) {}

    TypeDescriptor() : data_type_(DataType::UINT8), dimension_(Dimension::Dim0) {}

    ARCTICDB_MOVE_COPY_DEFAULT(TypeDescriptor)

    template<typename Callable>
    auto visit_tag(Callable &&callable) const;

    bool operator==(const TypeDescriptor &o) const {
        return data_type_ == o.data_type_ && dimension_ == o.dimension_;
    }

    bool operator!=(const TypeDescriptor &o) const {
        return !(*this == o);
    }

    [[nodiscard]] DataType data_type() const {
        return data_type_;
    }

    [[nodiscard]] Dimension dimension() const {
        return dimension_;
    }

    explicit operator Proto() const {
        return proto();
    }

    [[nodiscard]] Proto proto() const {
        arcticdb::proto::descriptors::TypeDescriptor output;
        output.set_dimension(static_cast<std::uint32_t>(dimension_));
        set_data_type(data_type_, output);

        return output;
    }
};

inline void set_data_type(DataType data_type, TypeDescriptor& type_desc) {
    type_desc.data_type_ = data_type;
}

inline TypeDescriptor make_scalar_type(DataType dt) {
    return TypeDescriptor{dt, Dimension::Dim0};
}

template<typename DT, typename D>
struct TypeDescriptorTag {
    static_assert(std::is_base_of_v<DataTypeTagBase, DT>);
    static_assert(std::is_base_of_v<DimensionTagBase, D>);
    using DataTypeTag = DT;
    using DimensionTag = D;
    explicit constexpr operator TypeDescriptor() const {
        return TypeDescriptor{DataTypeTag::data_type, DimensionTag::value};
    }
};

inline DataType get_data_type(const arcticdb::proto::descriptors::TypeDescriptor& type_desc) {
    return combine_data_type(
        static_cast<ValueType>(static_cast<uint8_t >(type_desc.value_type())),
        static_cast<SizeBits>(static_cast<uint8_t >(type_desc.size_bits()))
        );
}

inline TypeDescriptor type_desc_from_proto(const arcticdb::proto::descriptors::TypeDescriptor& type_desc) {
    return {
        combine_data_type(
            static_cast<ValueType>(static_cast<uint8_t >(type_desc.value_type())),
            static_cast<SizeBits>(static_cast<uint8_t >(type_desc.size_bits()))
        ),
        static_cast<Dimension>(static_cast<uint8_t>(type_desc.dimension()))
    };
}

inline DataType data_type_from_proto(const arcticdb::proto::descriptors::TypeDescriptor type_desc) {
    return type_desc_from_proto((type_desc)).data_type();
}

template <typename DTT>
using ScalarTagType = TypeDescriptorTag<DTT, DimensionTag<Dimension::Dim0>>;

inline arcticdb::proto::descriptors::StreamDescriptor_FieldDescriptor field_proto(DataType dt, Dimension dim, std::string_view name) {
    arcticdb::proto::descriptors::StreamDescriptor_FieldDescriptor output;
    if(!name.empty())
        output.set_name(name.data(), name.size());

    output.mutable_type_desc()->set_dimension(static_cast<uint32_t>(dim));
    output.mutable_type_desc()->set_size_bits(static_cast<arcticdb::proto::descriptors::TypeDescriptor_SizeBits>(
                                                  static_cast<std::uint8_t>(slice_bit_size(dt))));

    output.mutable_type_desc()->set_value_type(
        static_cast<arcticdb::proto::descriptors::TypeDescriptor_ValueType>(
            static_cast<std::uint8_t>(slice_value_type(dt))));

    return output;
}
/*
inline auto scalar_field (DataType dt, std::string_view name) {
    return field_proto<Dimension::Dim0>(dt, name);
}

inline auto scalar_field(TypeDescriptor::Proto&& proto, std::string_view name = std::string_view()) {
    using namespace pb2::descriptors_pb2;
    StreamDescriptor_FieldDescriptor output;
    if(!name.empty())
        output.set_name(name.data(), name.size());

    *output.mutable_type_desc() = std::move(proto);
    return output;
}

inline auto scalar_field_proto(const TypeDescriptor::Proto& proto, std::string_view name = std::string_view()) {
    using namespace pb2::descriptors_pb2;
    StreamDescriptor_FieldDescriptor output;
    if(!name.empty())
        output.set_name(name.data(), name.size());

    output.mutable_type_desc()->CopyFrom(proto);
    return output;
}
*/
struct IndexDescriptor {
        using Proto = arcticdb::proto::descriptors::IndexDescriptor;

    Proto data_;
    using Type = arcticdb::proto::descriptors::IndexDescriptor::Type;

    static const Type UNKNOWN = arcticdb::proto::descriptors::IndexDescriptor_Type_UNKNOWN;
    static const Type ROWCOUNT = arcticdb::proto::descriptors::IndexDescriptor_Type_ROWCOUNT;
    static const Type STRING = arcticdb::proto::descriptors::IndexDescriptor_Type_STRING;
    static const Type TIMESTAMP = arcticdb::proto::descriptors::IndexDescriptor_Type_TIMESTAMP;

    using TypeChar = char;

    IndexDescriptor() = default;
    IndexDescriptor(size_t field_count, Type type) {
        data_.set_kind(type);
        data_.set_field_count(static_cast<uint32_t>(field_count));
    }

    explicit IndexDescriptor(const arcticdb::proto::descriptors::IndexDescriptor& data)
        : data_(data) {
    }

    bool uninitialized() const {
        return data_.field_count() == 0 && data_.kind() == Type::IndexDescriptor_Type_UNKNOWN;
    }

    const Proto& proto() const  {
        return data_;
    }

    size_t field_count() const {
        return static_cast<size_t>(data_.field_count());
    }

    Type type() const {
        return data_.kind();
    }

    void set_type(Type type) {
        data_.set_kind(type);
    }

    ARCTICDB_MOVE_COPY_DEFAULT(IndexDescriptor)

    friend bool operator==(const IndexDescriptor& left, const IndexDescriptor& right) {
        return left.type() == right.type();
    }
};

constexpr IndexDescriptor::TypeChar to_type_char(IndexDescriptor::Type type) {
    switch (type) {
    case IndexDescriptor::TIMESTAMP:return 'T';
    case IndexDescriptor::ROWCOUNT:return 'R';
    case IndexDescriptor::STRING:return 'S';
    case IndexDescriptor::UNKNOWN:return 'U';
    default:util::raise_rte("Unknown index type: {}", type);
    }
}

constexpr IndexDescriptor::Type from_type_char(IndexDescriptor::TypeChar type) {
    switch (type) {
    case 'T': return IndexDescriptor::TIMESTAMP;
    case 'R': return IndexDescriptor::ROWCOUNT;
    case 'S': return IndexDescriptor::STRING;
    case 'U': return IndexDescriptor::UNKNOWN;
    default:util::raise_rte("Unknown index type: {}", type);
    }
}

struct FieldRef {
    TypeDescriptor type_;
    std::string_view name_;

    TypeDescriptor type() const {
        return type_;
    }

    std::string_view name() const {
        return name_;
    }

    friend bool operator==(const FieldRef& left, const FieldRef& right) {
        return left.type_ == right.type_ && left.name_ == right.name_;
    }
};

inline void set_id(arcticdb::proto::descriptors::StreamDescriptor& pb_desc, StreamId id) {
    std::visit([&pb_desc](auto &&arg) {
        using IdType = std::decay_t<decltype(arg)>;
        if constexpr (std::is_same_v<IdType, NumericId>)
            pb_desc.set_num_id(arg);
        else if constexpr (std::is_same_v<IdType, StringId>)
            pb_desc.set_str_id(arg);
        else
            util::raise_rte("Encoding unknown descriptor type");
    }, id);
}

struct Field {
    uint32_t size_ = 0;
    TypeDescriptor type_;
    static constexpr size_t NameSize = 2u;
    char name_[NameSize] = {};

    ARCTICDB_NO_MOVE_OR_COPY(Field)

    using Proto = arcticdb::proto::descriptors::StreamDescriptor_FieldDescriptor;

private:
    Field(const FieldRef& ref) {
        set(ref.type_, ref.name_);
    }

    Field(TypeDescriptor type, std::string_view name) {
        set(type, name);
    }
public:
    static void emplace(TypeDescriptor type, std::string_view name, void* ptr) {
        new (ptr) Field(type, name);
    }

    static Field from_ref(const FieldRef& ref) {
        return Field(ref);
    }

    static size_t calc_size(std::string_view name) {
      return sizeof(type_) + sizeof(size_) + std::max(NameSize, name.size());
    }

    std::string_view name() const {
        return {name_, size_};
    }

    const TypeDescriptor& type() const {
        return type_;
    }

    TypeDescriptor& mutable_type() {
        return type_;
    }

    FieldRef ref() const {
        return {type_, name()};
    }

    void set(TypeDescriptor type, std::string_view name) {
        type_ = type;
        size_ = name.size();
        memcpy(name_, name.data(), size_);
    }

    friend bool operator<(const Field &l, const Field &r) {
        const auto l_data_type = l.type().data_type();
        const auto r_data_type = r.type().data_type();
        const auto l_dim = l.type().dimension();
        const auto r_dim = r.type().dimension();
        const auto l_name = l.name();
        const auto r_name = r.name();
        auto lt = std::tie(l_name, l_data_type, l_dim);
        auto rt = std::tie(r_name, r_data_type, r_dim);
        return lt < rt;
    }
};

struct FieldWrapper {
    std::vector<uint8_t> data_;
    FieldWrapper(TypeDescriptor type, std::string_view name) :
    data_(Field::calc_size(name)) {
        mutable_field().set(type, name);
    }

    const Field& field() const {
        return *reinterpret_cast<const Field*>(data_.data());
    }

private:
    Field& mutable_field() {
        return *reinterpret_cast<Field*>(data_.data());
    }
};

inline FieldRef scalar_field(DataType type, std::string_view name) {
    return {TypeDescriptor{type, Dimension::Dim0}, name};
}

template <typename Callable>
auto visit_field(const Field& field, Callable&& c) {
    return field.type().visit_tag(std::forward<Callable>(c));
}

inline bool operator==(const Field& l, const Field& r) {
    return l.type() == r.type() && l.name() == r.name();
}

inline bool operator!=(const Field& l, const Field& r) {
    return !(l == r);
}

using UnicodeType = Py_UNICODE;

} // namespace arcticdb

// StreamId ordering - numbers before strings
namespace std {
template<>
struct less<arcticdb::entity::StreamId> {

    bool operator()(const arcticdb::entity::StreamId &left, const arcticdb::entity::StreamId &right) const {
        using namespace arcticdb::entity;
        if (std::holds_alternative<NumericId>(left)) {
            if (std::holds_alternative<NumericId>(right))
                return left < right;
            else
                return true;
        } else {
            if (std::holds_alternative<StringId>(right))
                return left < right;
            else
                return false;
        }
    }
};
}



namespace fmt {

using namespace arcticdb::entity;

template<>
struct formatter<FieldRef> {

    template<typename ParseContext>
    constexpr auto parse(ParseContext &ctx) { return ctx.begin(); }

    template<typename FormatContext>
    auto format(const FieldRef f, FormatContext &ctx) const {
        auto out = ctx.out();
        return format_to(ctx.out(), "{}: {}", f.type_, f.name_);
        return out;
    }
};

}

#define ARCTICDB_TYPES_H_
#include "types-inl.hpp"
