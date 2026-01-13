/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <string>
#include <arcticdb/util/preconditions.hpp>
#include <limits>
#include <arcticdb/entity/types.hpp>
#include <arcticdb/entity/atom_key.hpp>
#include <arcticdb/entity/ref_key.hpp>
#include <arcticdb/entity/variant_key.hpp>
#include <arcticdb/stream/index.hpp>
#include <arcticdb/util/buffer.hpp>
#include <arcticdb/util/cursored_buffer.hpp>
#include <arcticdb/util/string_utils.hpp>

namespace arcticdb::entity {

enum class OldKeyField {
    id,
    version_id,
    creation_ts,
    content_hash,
    index_type,
    start_index,
    end_index,
    num_key_fields
};

enum class NewKeyField { id, version_id, creation_ts, content_hash, start_index, end_index, num_key_fields };

constexpr char OldKeyDelimiter = '|';
constexpr char NewKeyDelimiter = '*';
constexpr size_t NumOldKeyFields = size_t(OldKeyField::num_key_fields);
constexpr size_t NumNewKeyFields = size_t(NewKeyField::num_key_fields);

inline VariantId variant_id_from_token(std::string_view strv, VariantType variant_type) {
    switch (variant_type) {
    case VariantType::STRING_TYPE:
        return VariantId(std::string(strv));
    case VariantType::NUMERIC_TYPE:
        return VariantId(util::num_from_strv(strv));
    default: // This may occur here because generation segments don't set an index type
        return VariantId();
    }
}

inline VariantType variant_type_from_index_type(IndexDescriptorImpl::Type index_type) {
    switch (index_type) {
    case IndexDescriptorImpl::Type::TIMESTAMP:
    case IndexDescriptorImpl::Type::ROWCOUNT:
        return VariantType::NUMERIC_TYPE;
    case IndexDescriptorImpl::Type::STRING:
        return VariantType::STRING_TYPE;
    default:
        return VariantType::UNKNOWN_TYPE;
    }
}

template<typename FieldIndex>
AtomKey atom_key_from_tokens(
        std::array<std::string_view, size_t(FieldIndex::num_key_fields)> arr, VariantType id_type,
        VariantType index_type, KeyType key_type
) {
    auto start_index = variant_id_from_token(arr[int(FieldIndex::start_index)], index_type);
    auto end_index = variant_id_from_token(arr[int(FieldIndex::end_index)], index_type);
    auto id = variant_id_from_token(arr[int(FieldIndex::id)], id_type);
    return AtomKeyBuilder()
            .version_id(util::num_from_strv(arr[int(FieldIndex::version_id)]))
            .creation_ts(util::num_from_strv(arr[int(FieldIndex::creation_ts)]))
            .content_hash(util::num_from_strv(arr[int(FieldIndex::content_hash)]))
            .start_index(start_index)
            .end_index(end_index)
            .build(id, key_type);
}

// This was the original way of doing it; we need to be able to read this kind but we don't want to
// write them any more
inline AtomKey key_from_old_style_bytes(const uint8_t* data, size_t size, KeyType key_type) {
    auto cursor = std::string_view(reinterpret_cast<const char*>(data), size);
    auto arr = util::split_to_array<NumOldKeyFields>(cursor, OldKeyDelimiter);
    auto id_variant_type = variant_type_from_key_type(key_type);
    auto index_type = IndexDescriptorImpl::Type(util::num_from_strv(arr[int(OldKeyField::index_type)]));
    auto index_variant_type = variant_type_from_index_type(index_type);
    return atom_key_from_tokens<OldKeyField>(arr, id_variant_type, index_variant_type, key_type);
}

constexpr inline size_t max_string_size() { return std::numeric_limits<uint8_t>::max(); }

template<typename T>
inline void serialize_string(const std::string& str, CursoredBuffer<T>& output) {
    util::check_arg(str.size() < max_string_size(), "String too long for serialization type");
    output.ensure_bytes(str.size() + sizeof(uint8_t));
    auto data = output.cursor();
    *data++ = static_cast<uint8_t>(str.size());
    memcpy(data, str.data(), str.size());
    output.commit();
}

inline std::string unserialize_string(const uint8_t*& data) {
    uint8_t size = *data++;
    auto output = std::string{reinterpret_cast<const char*>(data), size};
    data += size;
    return output;
}

template<typename T>
inline void serialize_number(uint64_t n, CursoredBuffer<T>& output) {
    output.template ensure<uint64_t>();
    *reinterpret_cast<uint64_t*>(output.cursor()) = n;
    output.commit();
}

template<typename T>
inline T unserialize_number(const uint8_t*& data) {
    auto n = reinterpret_cast<const T*>(data);
    data += sizeof(T);
    return *n;
}

static constexpr char SerializedKeyIdentifier = 42;

inline bool is_serialized_key(const uint8_t* data) { return *data == SerializedKeyIdentifier; }

inline VariantType variant_type_from_id(const VariantId& id) {
    return std::holds_alternative<NumericId>(id) ? VariantType::NUMERIC_TYPE : VariantType::STRING_TYPE;
}

template<typename T>
inline void serialize_variant_type(VariantId id, CursoredBuffer<T>& output) {
    if (std::holds_alternative<NumericId>(id))
        serialize_number(std::get<NumericId>(id), output);
    else
        serialize_string(std::get<std::string>(id), output);
}

inline VariantId unserialize_variant_type(VariantType type, const uint8_t*& data) {
    if (type == VariantType::NUMERIC_TYPE)
        return VariantId(unserialize_number<timestamp>(data));
    else
        return VariantId(unserialize_string(data));
}

enum class FormatType : char {
    OPAQUE =
            'o', // This is an efficient type (i.e. numbers as numbers), for things that don't required string-like keys
    TOKENIZED = 't', // This is a readable type, with delimiters, for things that require keys to consist of printable
                     // characters
};

inline size_t max_id_size(const VariantId& id) {
    return util::variant_match(
            id, [](const StringId&) { return max_string_size(); }, [](const auto&) { return sizeof(uint64_t); }
    );
}

inline size_t max_index_size(const IndexDescriptor& index) {
    switch (index.type_) {
    case IndexDescriptor::Type::STRING:
        return max_string_size();
    default:
        return sizeof(uint64_t);
    }
}

struct KeyDescriptor {
    explicit KeyDescriptor(const AtomKey& key, FormatType format_type) :
        identifier(SerializedKeyIdentifier),
        id_type(variant_type_from_id(key.id())),
        index_type(to_type_char(stream::get_index_value_type(key))),
        format_type(format_type) {}

    KeyDescriptor(const StringId& id, IndexDescriptorImpl::Type index_type, FormatType format_type) :
        identifier(SerializedKeyIdentifier),
        id_type(variant_type_from_id(id)),
        index_type(to_type_char(index_type)),
        format_type(format_type) {}

    KeyDescriptor(const RefKey& key, FormatType format_type) :
        identifier(SerializedKeyIdentifier),
        id_type(variant_type_from_id(key.id())),
        index_type(to_type_char(IndexDescriptorImpl::Type::UNKNOWN)),
        format_type(format_type) {}

    char identifier;
    VariantType id_type;
    IndexDescriptorImpl::TypeChar index_type;
    FormatType format_type;
};

inline size_t max_key_size(const StreamId& id, const IndexDescriptor& idx) {
    return 3 * sizeof(uint64_t) + max_id_size(id) + (2 * max_index_size(idx)) + sizeof(KeyDescriptor);
}

} // namespace arcticdb::entity

namespace fmt {
template<>
struct formatter<KeyDescriptor> {
    template<typename ParseContext>
    constexpr auto parse(ParseContext& ctx) {
        return ctx.begin();
    }

    template<typename FormatContext>
    auto format(const KeyDescriptor& kd, FormatContext& ctx) const {
        return fmt::format_to(
                ctx.out(),
                FMT_COMPILE("{}{}{}{}"),
                kd.identifier,
                char(kd.id_type),
                char(kd.index_type),
                char(kd.format_type)
        );
    }
};

} // namespace fmt

namespace arcticdb::entity {

inline std::string to_serialized_key(const AtomKey& key) {
    CursoredBuffer<Buffer> output;
    output.ensure<KeyDescriptor>();
    (void)new (output.cursor()) KeyDescriptor(key, FormatType::OPAQUE);
    output.commit();

    serialize_variant_type(key.id(), output);
    serialize_number(key.version_id(), output);
    serialize_number(key.creation_ts(), output);
    serialize_number(key.content_hash(), output);
    serialize_variant_type(key.start_index(), output);
    serialize_variant_type(key.end_index(), output);
    return std::string(reinterpret_cast<const char*>(output.data()), output.bytes());
}

inline std::string to_serialized_key(const RefKey& key) {
    CursoredBuffer<Buffer> output;
    output.ensure<KeyDescriptor>();
    (void)new (output.cursor()) KeyDescriptor(key, FormatType::OPAQUE);
    output.commit();

    serialize_variant_type(key.id(), output);
    return std::string(reinterpret_cast<const char*>(output.data()), output.bytes());
}

inline std::string to_serialized_key(const entity::VariantKey& key) {
    return std::visit([&](const auto& key) { return to_serialized_key(key); }, key);
}

inline AtomKey from_serialized_atom_key(const uint8_t* data, KeyType key_type) {
    const auto* descr = reinterpret_cast<const KeyDescriptor*>(data);
    util::check(
            descr->identifier == SerializedKeyIdentifier,
            "Read invalid serialized key {} in from_serialized_atom_key",
            descr->identifier
    );
    data += sizeof(KeyDescriptor);
    VariantId stream_id = unserialize_variant_type(descr->id_type, data);
    auto variant_type = variant_type_from_index_type(from_type_char(descr->index_type));
    return atom_key_builder()
            .version_id(unserialize_number<uint64_t>(data))
            .creation_ts(unserialize_number<uint64_t>(data))
            .content_hash(unserialize_number<uint64_t>(data))
            .start_index(unserialize_variant_type(variant_type, data))
            .end_index(unserialize_variant_type(variant_type, data))
            .build(stream_id, key_type);
}

inline RefKey from_serialized_ref_key(const uint8_t* data, KeyType key_type) {
    const auto* descr = reinterpret_cast<const KeyDescriptor*>(data);
    util::check(
            descr->identifier == SerializedKeyIdentifier,
            "Read invalid serialized key {} in from_serialized_ref_key",
            descr->identifier
    );
    data += sizeof(KeyDescriptor);
    VariantId stream_id = unserialize_variant_type(descr->id_type, data);
    return RefKey{stream_id, key_type};
}

inline std::string to_tokenized_key(const AtomKey& key) {
    KeyDescriptor kd{key, FormatType::TOKENIZED};
    return fmt::format(
            FMT_COMPILE("{}*{}*{}*{}*{}*{}*{}"),
            kd,
            key.id(),
            key.version_id(),
            key.creation_ts(),
            key.content_hash(),
            tokenized_index(key.start_index()),
            tokenized_index(key.end_index())
    );
}

inline std::string to_tokenized_key(const RefKey& key) {
    KeyDescriptor kd{key, FormatType::TOKENIZED};
    return fmt::format(FMT_COMPILE("{}*{}"), kd, key.id());
}

inline std::string to_tokenized_key(const entity::VariantKey& key) {
    return std::visit([&](const auto& key) { return to_tokenized_key(key); }, key);
}

inline AtomKey from_tokenized_atom_key(const uint8_t* data, size_t size, KeyType key_type) {
    const auto* descr = reinterpret_cast<const KeyDescriptor*>(data);
    util::check(
            descr->identifier == SerializedKeyIdentifier,
            "Read invalid tokenized key {} in from_tokenized_atom_key",
            descr->identifier
    );
    std::string_view cursor(reinterpret_cast<const char*>(data) + sizeof(KeyDescriptor), size - sizeof(KeyDescriptor));
    auto tokens = std::count(std::begin(cursor), std::end(cursor), NewKeyDelimiter);
    auto index_variant_type = variant_type_from_index_type(from_type_char(descr->index_type));
    if (tokens == NumNewKeyFields) {
        auto arr = util::split_to_array<NumNewKeyFields>(cursor, NewKeyDelimiter);
        return atom_key_from_tokens<NewKeyField>(arr, descr->id_type, index_variant_type, key_type);
    } else {
        auto vec = util::split_to_vector(cursor, NewKeyDelimiter);
        util::check(
                vec.size() > NumNewKeyFields,
                "Expected number of key fields {} to be greater than expected: {}",
                vec.size(),
                NumNewKeyFields
        );
        auto extra = vec.size() - NumNewKeyFields;
        std::array<std::string_view, NumNewKeyFields> fixup;

        auto it = vec.begin();
        std::advance(it, size_t(NewKeyField::id));
        std::ostringstream strm;
        strm << *it++;

        for (auto j = 0ULL; j < extra; ++j) {
            strm << NewKeyDelimiter << *it;
            it = vec.erase(it);
        }

        auto replace_it = vec.begin();
        std::advance(replace_it, size_t(NewKeyField::id));
        auto merged = strm.str();
        *replace_it = merged;

        std::copy_n(vec.begin(), NumNewKeyFields, fixup.begin());
        return atom_key_from_tokens<NewKeyField>(fixup, descr->id_type, index_variant_type, key_type);
    }
}

inline RefKey from_tokenized_ref_key(const uint8_t* data, size_t size, KeyType key_type) {
    const auto* descr = reinterpret_cast<const KeyDescriptor*>(data);
    util::check(
            descr->identifier == SerializedKeyIdentifier,
            "Read invalid tokenized key {} in from_tokenized_ref_key",
            descr->identifier
    );
    // data looks like: "*sUt*snapshot" and the descr for ref key is "*"
    auto prefix = sizeof(KeyDescriptor) + 1;
    std::string_view cursor(reinterpret_cast<const char*>(data) + prefix, size - prefix);
    auto id = variant_id_from_token(cursor, descr->id_type);
    return RefKey{id, key_type};
}

inline VariantKey from_tokenized_variant_key(const uint8_t* data, size_t size, KeyType key_type) {
    if (is_ref_key_class(key_type))
        return from_tokenized_ref_key(data, size, key_type);
    else
        return from_tokenized_atom_key(data, size, key_type);
}

inline AtomKey atom_key_from_bytes(const uint8_t* data, size_t size, KeyType key_type) {
    if (!is_serialized_key(data))
        return key_from_old_style_bytes(data, size, key_type);

    const auto* descr = reinterpret_cast<const KeyDescriptor*>(data);
    if (descr->format_type == FormatType::OPAQUE)
        return from_serialized_atom_key(data, key_type);
    else if (descr->format_type == FormatType::TOKENIZED)
        return from_tokenized_atom_key(data, size, key_type);

    util::raise_rte("Unrecognized key format '{}", char(descr->format_type));
}

inline RefKey ref_key_from_bytes(const uint8_t* data, size_t size, KeyType key_type) {
    const auto* descr = reinterpret_cast<const KeyDescriptor*>(data);
    if (descr->format_type == FormatType::OPAQUE)
        return from_serialized_ref_key(data, key_type);
    else if (descr->format_type == FormatType::TOKENIZED)
        return from_tokenized_ref_key(data, size, key_type);

    util::raise_rte("Unrecognized key format '{}", char(descr->format_type));
}

inline VariantKey variant_key_from_bytes(const uint8_t* data, size_t size, KeyType key_type) {
    if (is_ref_key_class(key_type))
        return ref_key_from_bytes(data, size, key_type);
    else
        return atom_key_from_bytes(data, size, key_type);
}

} // namespace arcticdb::entity
