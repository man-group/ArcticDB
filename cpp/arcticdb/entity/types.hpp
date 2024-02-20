/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/util/preconditions.hpp>
#include <arcticdb/util/constructors.hpp>

#include <cstdint>
#include <vector>
#include <string>
#include <type_traits>
#include <variant>


#ifdef _WIN32
// `ssize_t` is defined in `sys/types.h` but it is not ISO C (it simply is POSIX), hence its is not defined natively by MSVC.
// See: https://learn.microsoft.com/en-us/windows/win32/winprog/windows-data-types
#include <BaseTsd.h>
using ssize_t = SSIZE_T;
#endif

namespace arcticdb::entity {

enum class SortedValue : uint8_t {
    UNKNOWN = 0,
    UNSORTED = 1,
    ASCENDING = 2,
    DESCENDING = 3,
};

using NumericId = int64_t;
using StringId = std::string;
using VariantId = std::variant<NumericId, StringId>;
using StreamId = VariantId;
using SnapshotId = VariantId;
using VersionId = uint64_t;
using SignedVersionId = int64_t;
using GenerationId = VersionId;
using timestamp = int64_t;
using shape_t = int64_t;
using stride_t = int64_t;
using position_t = int64_t;

/** The VariantId holds int64 (NumericId) but is also used to store sizes up to uint64, so needs safe conversion */
inline NumericId safe_convert_to_numeric_id(uint64_t input) {
    util::check(input <= static_cast<uint64_t>(std::numeric_limits<NumericId>::max()), "Numeric symbol greater than 2^63 is not supported.");
    return static_cast<NumericId>(input);
}


// See: https://github.com/python/cpython/issues/105156
// See: https://peps.python.org/pep-0393/
using UnicodeType = wchar_t;
constexpr size_t UNICODE_WIDTH = sizeof(UnicodeType);
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
    // EMPTY is used by:
    //  - empty columns whose type has not been explicitly specified
    //  - columns full of placeholder values such as `None.`
    //  (e.g. in Pandas, `pd.DataFrame({"c": [None, None, None]})`, would be mapped
    //  within ArcticDB to a column of type `EMPTY`)
    // EMPTY can be promoted to any type.
    // For instance, Pandas empty series whose types has not been specified is mapping to EMPTY.
    // When data is appended, the column type is inferred from the data and the column is promoted to the inferred type.
    EMPTY = 13,
    /// Nullable booleans
    PYBOOL = 14,
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
    COUNT = 5
};

constexpr SizeBits get_size_bits(uint8_t size) {
    switch (size) {
        case 2:return SizeBits::S16;
        case 4:return SizeBits::S32;
        case 8:return SizeBits::S64;
        default:return SizeBits::S8;
    }
}

[[nodiscard]] constexpr int get_byte_count(SizeBits size_bits) {
    switch(size_bits) {
        case SizeBits::S8: return 1;
        case SizeBits::S16: return 2;
        case SizeBits::S32: return 4;
        case SizeBits::S64: return 8;
        default: util::raise_rte("Unknown size bits");
    }
}

namespace detail{

constexpr uint8_t combine_val_bits(ValueType v, SizeBits b = SizeBits::UNKNOWN_SIZE_BITS) {
    return (static_cast<uint8_t>(v) << 3u) | static_cast<uint8_t>(b);
}

} // namespace anonymous

enum class DataType : uint8_t {
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
    PYBOOL8 = detail::combine_val_bits(ValueType::PYBOOL, SizeBits::S8),
    PYBOOL64 = detail::combine_val_bits(ValueType::PYBOOL, SizeBits::S64),
    UNKNOWN = 0,
};

std::string_view datatype_to_str(const DataType dt);

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

constexpr bool is_py_bool_type(DataType dt) {
    return slice_value_type(dt) == ValueType::PYBOOL;
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

static_assert(slice_value_type(DataType::UINT16) == ValueType(1));
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
DATA_TYPE_TAG(PYBOOL8, uint8_t)
DATA_TYPE_TAG(PYBOOL64, std::uint64_t)
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

inline void set_data_type(DataType data_type, TypeDescriptor& type_desc);

struct TypeDescriptor {
    DataType data_type_;
    Dimension dimension_;

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

    [[nodiscard]] constexpr DataType data_type() const {
        return data_type_;
    }

    [[nodiscard]] constexpr Dimension dimension() const {
        return dimension_;
    }

    void set_size_bits(SizeBits new_size_bits) {
        data_type_ = combine_data_type(slice_value_type(data_type_), new_size_bits);
    }

    [[nodiscard]] constexpr int get_type_byte_size() const {
        return get_byte_count(slice_bit_size(data_type_));
    }
};


/// @brief Check if the type must contain data
/// Some types are allowed not to have any data, e.g. empty arrays or the empty type (which by design denotes the
/// lack of data).
/// @return true if the type must contain data, false it it's allowed for the type to have 0 bytes of data
constexpr bool must_contain_data(TypeDescriptor td) {
    return !(is_empty_type(td.data_type()) || td.dimension() > Dimension::Dim0);
}

/// @biref Check if type descriptor corresponds to numpy array type
/// @important Be sure to match this with the type handler registry in: cpp/arcticdb/python/python_module.cpp#register_type_handlers
constexpr bool is_numpy_array(TypeDescriptor td) {
    return (is_numeric_type(td.data_type()) || is_bool_type(td.data_type()) || is_empty_type(td.data_type())) &&
           (td.dimension() == Dimension::Dim1);
}

constexpr bool is_pyobject_type(TypeDescriptor td) {
    return is_dynamic_string_type(slice_value_type(td.data_type())) ||
           slice_value_type(td.data_type()) == ValueType::PYBOOL ||
           is_numpy_array(td);
}


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

    [[nodiscard]] constexpr Dimension dimension() const {
        return DimensionTag::value;
    }

    [[nodiscard]] constexpr DataType data_type() const {
        return DataTypeTag::data_type;
    }
};

template <typename DTT>
using ScalarTagType = TypeDescriptorTag<DTT, DimensionTag<Dimension::Dim0>>;

template<typename T>
struct ScalarTypeInfo {
    using TDT = ScalarTagType<T>;
    static constexpr auto data_type = TDT::DataTypeTag::data_type;
    using RawType = typename TDT::DataTypeTag::raw_type;
};

struct FieldRef {
    TypeDescriptor type_;
    std::string_view name_;

    [[nodiscard]] TypeDescriptor type() const {
        return type_;
    }

    [[nodiscard]] std::string_view name() const {
        return name_;
    }

    friend bool operator==(const FieldRef& left, const FieldRef& right) {
        return left.type_ == right.type_ && left.name_ == right.name_;
    }
};


struct Field {
    uint32_t size_ = 0;
    TypeDescriptor type_;
    static constexpr size_t NameSize = 2u;
    char name_[NameSize] = {};

    ARCTICDB_NO_MOVE_OR_COPY(Field)



private:
    explicit Field(const FieldRef& ref) {
        set(ref.type_, ref.name_);
    }

    Field(TypeDescriptor type, std::string_view name) {
        set(type, name);
    }
public:
    static void emplace(TypeDescriptor type, std::string_view name, void* ptr) {
        new (ptr) Field(type, name);
    }

    static size_t calc_size(std::string_view name) {
      return sizeof(type_) + sizeof(size_) + std::max(NameSize, name.size());
    }

    [[nodiscard]] std::string_view name() const {
        return {name_, size_};
    }

    [[nodiscard]] const TypeDescriptor& type() const {
        return type_;
    }

    [[nodiscard]] TypeDescriptor* mutable_type_desc() {
        return &type_;
    }

    TypeDescriptor& mutable_type() {
        return type_;
    }

    [[nodiscard]] FieldRef ref() const {
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

} // namespace arcticdb::entity

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
    auto format(const FieldRef& f, FormatContext &ctx) const {
        return fmt::format_to(ctx.out(), "{}: {}", f.type_, f.name_);
    }
};

}

#define ARCTICDB_TYPES_H_
#include "types-inl.hpp"
