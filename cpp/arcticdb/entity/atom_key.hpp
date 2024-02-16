/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/entity/key.hpp>
#include <arcticdb/entity/types.hpp>
#include <arcticdb/entity/index_range.hpp>
#include <arcticdb/util/string_utils.hpp>
#include <variant>
#include <string_view>

namespace arcticdb::entity {

class AtomKeyImpl {
  public:

    template<class IndexValueType>
    AtomKeyImpl(
        StreamId id,
        VersionId version_id,
        timestamp creation_ts,
        ContentHash content_hash,
        IndexValueType start_index,
        IndexValueType end_index,
        KeyType key_type);

    AtomKeyImpl() = default;
    AtomKeyImpl(const AtomKeyImpl &other) = default;
    AtomKeyImpl &operator=(const AtomKeyImpl &other) = default;
    AtomKeyImpl(AtomKeyImpl &&other) = default;
    AtomKeyImpl &operator=(AtomKeyImpl &&other) = default;

    const StreamId& id() const;
    const VersionId& version_id() const;
    const VersionId& gen_id() const;
    const timestamp& creation_ts() const;
    TimestampRange time_range() const;
    timestamp start_time() const;
    timestamp end_time() const;
    const ContentHash& content_hash() const;
    const KeyType& type() const;
    KeyType& type();
    const IndexValue &start_index() const;
    const IndexValue &end_index() const;
    IndexRange index_range() const;

    auto change_type(KeyType new_type);

    /**
     * Useful for caching/replacing the ID with an existing shared instance.
     * @param id Will be moved.
     * @return The old id moved out.
     */
    StreamId change_id(StreamId id);

    friend bool operator==(const AtomKeyImpl &l, const AtomKeyImpl &r);

    friend bool operator!=(const AtomKeyImpl &l, const AtomKeyImpl &r);

    friend bool operator<(const AtomKeyImpl &l, const AtomKeyImpl &r);

    friend bool operator>(const AtomKeyImpl &l, const AtomKeyImpl &r);

    size_t get_cached_hash() const;

    void set_string() const;

    std::string_view view() const;

private:
    StreamId id_;
    VersionId version_id_ = 0;
    timestamp creation_ts_ = 0;
    ContentHash content_hash_ = 0;
    KeyType key_type_ = KeyType::UNDEFINED;
    IndexValue index_start_;
    IndexValue index_end_;
    mutable std::string str_; //TODO internalized string
    mutable std::optional<size_t> hash_;

    void reset_cached();
};

/**
 * Builder introduced since I feel having a ctor for the key with 4 fields with the same type next
 * to each other is going to result in inverted fields making it difficult at call site
 * to see what's happening.
 * It might be avoided in perf critical situations.
 * @tparam StringViewable
 */
class AtomKeyBuilder {
  public:
    AtomKeyBuilder &version_id(VersionId v);

    AtomKeyBuilder &gen_id(VersionId v);

    AtomKeyBuilder &creation_ts(timestamp v);

    AtomKeyBuilder &string_index(const std::string &s);

    AtomKeyBuilder &start_index(const IndexValue &iv);

    AtomKeyBuilder &end_index(const IndexValue &iv);

    AtomKeyBuilder &content_hash(ContentHash v);

    template<KeyType KT>
    AtomKeyImpl build(StreamId id);

    AtomKeyImpl build(StreamId id, KeyType key_type);

  private:
    VersionId version_id_ = 0;
    arcticdb::entity::timestamp creation_ts_ = 0;
    ContentHash content_hash_ = 0;
    IndexValue index_start_;
    IndexValue index_end_;
};

using AtomKey = AtomKeyImpl;

// Aliases to aid implicit documentation of functions. To be made fully type-safe later:
/**
 * AtomKey that matches the is_index_key_type() check (i.e. TABLE_INDEX and equivalent).
 */
using IndexTypeKey = AtomKey;

inline AtomKeyBuilder atom_key_builder();

inline AtomKey null_key();

} // namespace arcticdb::entity
