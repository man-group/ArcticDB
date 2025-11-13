/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/entity/types.hpp>
#include <arcticdb/util/preconditions.hpp>
#include <algorithm>
#include <variant>
#include <array>

namespace rng = std::ranges;

namespace arcticdb::entity {

struct DefaultAtomKeyFormat {
    static constexpr char format[] = "Key(type={}, id='{}', version_id={:d}, content_hash=0x{:x}, creation_ts={:d}, "
                                     "start_index={}, end_index={})";
};

template<class T, class FormattingTag = DefaultAtomKeyFormat>
struct FormattableRef {
    const T& ref;

    explicit FormattableRef(const T& k) : ref(k) {}

    FormattableRef(const FormattableRef&) = delete;

    FormattableRef(FormattableRef&&) = delete;
};

template<class T, class FormattingTag = DefaultAtomKeyFormat>
auto formattable(const T& ref) {
    return FormattableRef<T, FormattingTag>(ref);
}

using ContentHash = std::uint64_t;

enum class KeyClass : int {
    /*
     * ATOM_KEY is a KeyType containing a segment with either data or other keys in it and the client needs to
     * have the entire key in memory with all fields populated to read it out.
     */
    ATOM_KEY,
    /*
     * REF_KEY is the KeyType containing other keys in it, and can be easily read by the client using just the
     * id() field in the key as it is unique for a given (id, KeyType) combination
     */
    REF_KEY,
    UNKNOWN_CLASS
};

enum class KeyType : int {
    /*
     * TODO: For the Kestrel streaming stuff, not in general use atm.
     */
    STREAM_GROUP = 0,
    /*
     * TODO: For Kestrel streaming stuff, not in general use atm.
     */
    GENERATION = 1,
    // The following keys are string-id-based
    /*
     * TABLE_DATA are the leaf nodes of our tree and contain the actual data in the segment.
     */
    TABLE_DATA = 2,
    /*
     * TABLE_INDEX contains multiple TABLE_DATA keys in the segment of the key, and sits between the VERSION
     * and TABLE_DATA keys. *_REF, MULTI_KEY and VERSION keys point to the index key type.
     */
    TABLE_INDEX = 3,
    /*
     * At the moment, VERSION keys point to a single TABLE_INDEX key. In the segment of the key, it can contain
     * other VERSION keys (for faster traversal of versions, via a linked list on storage) , TABLE_INDEX key (just
     * one atm, but no reason why we can't have multiple) and TOMBSTONE Keys
     */
    VERSION = 4,
    /*
     * VERSION_JOURNAL is not used anymore, just exist for backwards compatibility reasons
     */
    VERSION_JOURNAL = 5,
    /*
     * METRICS is not longer used, was initially used to log timing and other performance metrics.
     */
    METRICS = 6,
    /*
     * Is not used anymore, was used before SNAPSHOT_REF was introduced and might still be present in older libraries.
     * It points to TABLE_INDEX keys in the segment
     */
    SNAPSHOT = 7,
    /*
     * SYMBOL_LIST is used for creating a symbol linked list in storage to avoid iterating all symbols in operations
     * like list symbols.
     */
    SYMBOL_LIST = 8,
    /*
     * VERSION_REF sits at the top of the tree and points to the latest index and version for each stream_id,
     * this helps in speeding up latest_version operations and by pointing to the VERSION key type it forms the head
     * of the linked list in storage
     */
    VERSION_REF = 9,
    /*
     * Used for storing storage informations (like last sync time)
     */
    STORAGE_INFO = 10,
    /*
     * The head of the list of incomplete appended segments
     */
    APPEND_REF = 11,
    /*
     * Is used as a index of indexes, specially for cases with recursive normalizers where a single write
     * might lead to multiple reads, and on a read being issued by the client we want a faster way to reconstruct
     * the original structure
     */
    MULTI_KEY = 12,
    /*
     * When present for a symbol, this helps us create a locking mechanism in the storage where the presence of
     * this key will block other concurrent write operations from running at the same time.
     */
    LOCK = 13,
    /*
     * Is used to reference a single snapshot, and points to multiple index keys. replaces the old SNAPSHOT key type
     */
    SNAPSHOT_REF = 14,
    /*
     * TOMBSTONE KeyType WILL NEVER EXIST in the storage as a standalone key , and only lives inside the segment of
     * VERSION KeyType to indicate a version that was pruned or explicitly deleted
     */
    TOMBSTONE = 15,
    /*
     * An incomplete appended segment
     */
    APPEND_DATA = 16,
    /*
     * Used for logging write, tombstone, and tombstone all events (to be used by high-priority replication job)
     * See also LOG_COMPACTED
     */
    LOG = 17,
    /*
     * This is used to store references to partition values for a symbol, eg. for values a, b, c for a
     * symbol 'sym' we will have a partition key per write for that symbol with subkeys for the dataframes
     * containing individual values.
     */
    PARTITION = 18,
    /*
     * Used for storing offsets of input streams
     */
    OFFSET = 19,
    /*
     * Temporary - remove_me
     */
    BACKUP_SNAPSHOT_REF = 20,
    /*
     * Used for indicating that all keys lower than this version id are deleted
     */
    TOMBSTONE_ALL = 21,
    /*
     * Used for library configs
     */
    LIBRARY_CONFIG = 22,
    /*
     * Marks a SNAPSHOT_REF scheduled for removal. Unlike the TOMBSTONE[_ALL] keys for versioning, this is written to
     * storage as a root key as opposed being stored in the segment. The segment contents of this key is the same as the
     * SNAPSHOT_REF removed.
     */
    SNAPSHOT_TOMBSTONE = 23,
    /*
     * Contains multiple LOG keys in its segment (to be used by low-priority replication job)
     */
    LOG_COMPACTED = 24,
    /*
     * Contains column stats about the index key with the same stream ID and version number
     */
    COLUMN_STATS = 25,
    /*
     * Used for storing the ids of storages that failed to sync
     */
    REPLICATION_FAIL_INFO = 26,
    /*
     * A reference key storing many versions, used to track state within some of our background jobs.
     */
    BLOCK_VERSION_REF = 27,
    /*
     * Used for a list based reliable storage lock
     */
    ATOMIC_LOCK = 28,
    UNDEFINED
};

consteval auto key_types_write_precedence() {
    // TOMBSTONE[_ALL] keys are not included because they're not written to the storage,
    // they just exist inside version keys
    return std::array{
            KeyType::LIBRARY_CONFIG,
            KeyType::TABLE_DATA,
            KeyType::TABLE_INDEX,
            KeyType::MULTI_KEY,
            KeyType::VERSION,
            KeyType::VERSION_JOURNAL,
            KeyType::VERSION_REF,
            KeyType::SYMBOL_LIST,
            KeyType::SNAPSHOT,
            KeyType::SNAPSHOT_REF,
            KeyType::SNAPSHOT_TOMBSTONE,
            KeyType::APPEND_REF,
            KeyType::APPEND_DATA,
            KeyType::PARTITION,
            KeyType::OFFSET
    };
}

consteval auto key_types_read_precedence() {
    auto output = key_types_write_precedence();
    rng::reverse(output);
    return output;
}

} // namespace arcticdb::entity

namespace std {
template<>
struct hash<arcticdb::entity::KeyType> {
    size_t operator()(arcticdb::entity::KeyType kt) const { return std::hash<int>{}(static_cast<int>(kt)); }
};
} // namespace std

namespace arcticdb::entity {

// N.B. some storages are case-insensitive, so these need to be
// prefix-unique disregarding case
const char* key_type_long_name(KeyType key_type);

char key_type_short_name(KeyType key_type);

enum class VariantType : char { STRING_TYPE = 's', NUMERIC_TYPE = 'd', UNKNOWN_TYPE = 'u' };

VariantType variant_type_from_key_type(KeyType key_type);

constexpr bool is_index_key_type(KeyType key_type) {
    // TODO: Change name probably.
    return (key_type == KeyType::TABLE_INDEX) || (key_type == KeyType::MULTI_KEY);
}

bool is_string_key_type(KeyType k);

bool is_ref_key_class(KeyType k);

bool is_block_ref_key_class(KeyType k);

constexpr KeyType get_key_type_for_data_stream(const StreamId&) { return KeyType::TABLE_DATA; }

constexpr KeyType get_key_type_for_index_stream(const StreamId&) { return KeyType::TABLE_INDEX; }

const char* get_key_description(KeyType type);

template<typename Function>
constexpr auto foreach_key_type_read_precedence(Function&& func) {
    rng::for_each(key_types_read_precedence(), func);
}

template<typename Function>
constexpr auto foreach_key_type_write_precedence(Function&& func) {
    rng::for_each(key_types_write_precedence(), func);
}

inline KeyType key_type_from_int(int type_num) {
    util::check(type_num > 0 && type_num < int(KeyType::UNDEFINED), "Unrecognized key type number {}", type_num);
    return KeyType(type_num);
}

template<typename Function>
auto foreach_key_type(Function&& func) {
    for (int k = 1; k < int(KeyType::UNDEFINED); ++k) {
        func(key_type_from_int(k));
    }
}

} // namespace arcticdb::entity

template<>
struct fmt::formatter<arcticdb::entity::KeyType> {

    template<typename ParseContext>
    constexpr auto parse(ParseContext& ctx) {
        return ctx.begin();
    }

    template<typename FormatContext>
    auto format(const arcticdb::entity::KeyType k, FormatContext& ctx) const {
        using namespace arcticdb::entity;
        return fmt::format_to(ctx.out(), "{}", key_type_short_name(k));
    }
};
