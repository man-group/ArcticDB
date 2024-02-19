/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/entity/atom_key.hpp>

namespace arcticdb::entity {

template<class IndexValueType>
AtomKeyImpl::AtomKeyImpl(
    StreamId id,
    VersionId version_id,
    timestamp creation_ts,
    ContentHash content_hash,
    IndexValueType start_index,
    IndexValueType end_index,
    KeyType key_type)
    :
    id_(std::move(id)),
    version_id_(version_id),
    creation_ts_(creation_ts),
    content_hash_(content_hash),
    key_type_(key_type),
    index_start_(std::move(start_index)),
    index_end_(std::move(end_index)),
    str_() { }

const StreamId& AtomKeyImpl::id() const { return id_; }
const VersionId& AtomKeyImpl::version_id() const { return version_id_; }
const VersionId& AtomKeyImpl::gen_id() const { return version_id_; }
const timestamp& AtomKeyImpl::creation_ts() const { return creation_ts_; }
TimestampRange AtomKeyImpl::time_range() const { return {start_time(), end_time()}; }
timestamp AtomKeyImpl::start_time() const { if (std::holds_alternative<timestamp>(index_start_)) return std::get<timestamp>(index_start_); else return 0LL; }
timestamp AtomKeyImpl::end_time() const { if (std::holds_alternative<timestamp>(index_end_)) return std::get<timestamp>(index_end_); else return 0LL; }
const ContentHash& AtomKeyImpl::content_hash() const { return content_hash_; }
const KeyType& AtomKeyImpl::type() const { return key_type_; }
KeyType& AtomKeyImpl::type() { return key_type_; }
const IndexValue &AtomKeyImpl::start_index() const { return index_start_; }
const IndexValue &AtomKeyImpl::end_index() const { return index_end_; }
IndexRange AtomKeyImpl::index_range() const { IndexRange ir = {index_start_, index_end_}; ir.end_closed_ = false; return ir;}

auto AtomKeyImpl::change_type(KeyType new_type) {
    key_type_ = new_type;
    reset_cached();
}

/**
 * Useful for caching/replacing the ID with an existing shared instance.
 * @param id Will be moved.
 * @return The old id moved out.
 */
StreamId AtomKeyImpl::change_id(StreamId id) {
    auto out = std::move(id_);
    id_ = std::move(id);
    reset_cached();
    return out;
}

bool operator==(const AtomKeyImpl &l, const AtomKeyImpl &r) {
    return l.version_id() == r.version_id()
        && l.creation_ts() == r.creation_ts()
        && l.content_hash() == r.content_hash()
        && l.start_index() == r.start_index()
        && l.end_index() == r.end_index()
        && l.type() == r.type()
        && l.id() == r.id();
}

bool operator!=(const AtomKeyImpl &l, const AtomKeyImpl &r) {
    return !(l == r);
}

bool operator<(const AtomKeyImpl &l, const AtomKeyImpl &r) {
    auto lt = std::tie(l.id_, l.version_id_, l.index_start_, l.index_end_, l.creation_ts_);
    auto rt = std::tie(r.id_, r.version_id_, r.index_start_, r.index_end_, r.creation_ts_);
    return lt < rt;
}

bool operator>(const AtomKeyImpl &l, const AtomKeyImpl &r) {
    return !(l < r) && (l != r);
}

size_t AtomKeyImpl::get_cached_hash() const {
    if (!hash_) {
        // arcticdb::commutative_hash_combine needs extra template specialisations for our variant types, folly's
        // built-in variant forwards to std::hash which should be good enough for these simple types
        hash_ = folly::hash::hash_combine(id_, version_id_, creation_ts_, content_hash_, key_type_, index_start_,
                index_end_);
    }
    return *hash_;
}

void AtomKeyImpl::set_string() const {
    str_ = fmt::format("{}", *this);
}

std::string_view AtomKeyImpl::view() const { if(str_.empty()) set_string(); return {str_}; }

void AtomKeyImpl::reset_cached() {
    str_.clear();
    hash_.reset();
}

AtomKeyBuilder &AtomKeyBuilder::version_id(VersionId v) {
    version_id_ = v;
    return *this;
}

AtomKeyBuilder &AtomKeyBuilder::gen_id(VersionId v) {
    util::check_arg(version_id_ == 0, "Should not set both version_id and version id on a key");
    version_id_ = v;
    return *this;
}

AtomKeyBuilder &AtomKeyBuilder::creation_ts(timestamp v) {
    creation_ts_ = v;
    return *this;
}

AtomKeyBuilder &AtomKeyBuilder::string_index(const std::string &s) {
    index_start_ = s;
    return *this;
}
AtomKeyBuilder &AtomKeyBuilder::start_index(const IndexValue &iv) {
    index_start_ = iv;
    return *this;
}

AtomKeyBuilder &AtomKeyBuilder::end_index(const IndexValue &iv) {
    index_end_ = iv;
    return *this;
}

AtomKeyBuilder &AtomKeyBuilder::content_hash(ContentHash v) {
    content_hash_ = v;
    return *this;
}

AtomKeyImpl AtomKeyBuilder::build(StreamId id, KeyType key_type) {
    return {
        std::move(id), version_id_, creation_ts_, content_hash_, index_start_, index_end_, key_type
    };
}

} // namespace arcticdb::entity

