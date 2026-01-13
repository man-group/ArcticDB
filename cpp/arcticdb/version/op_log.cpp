/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/version/op_log.hpp>

namespace arcticdb {
OpLog::OpLog(AtomKey&& key) :
    id_(std::get<StringIndex>(key.start_index())),
    version_id_(key.version_id()),
    action_(std::get<StringId>(key.id())),
    creation_ts_(key.creation_ts()),
    content_hash_(key.content_hash()) {}

OpLog::OpLog(StringId id, VersionId version_id, const std::string& action, timestamp creation_ts) :
    id_(id),
    version_id_(version_id),
    action_(action),
    creation_ts_(creation_ts) {}

const StringId& OpLog::id() const { return id_; }

VersionId OpLog::version_id() const { return version_id_; }

const std::string& OpLog::action() const { return action_; }

timestamp OpLog::creation_ts() const { return creation_ts_; }

AtomKey OpLog::extract_key() {
    util::check(content_hash_.has_value(), "Cannot extract Atomkey from OpLog without content hash");
    // Contents need to be compatible with version_log.hpp#log_event
    return AtomKeyBuilder()
            .version_id(version_id_)
            .creation_ts(creation_ts_)
            .content_hash(content_hash_.value())
            .start_index(id_)
            .end_index(id_)
            .build<KeyType::LOG>(std::move(action_));
}
} // namespace arcticdb