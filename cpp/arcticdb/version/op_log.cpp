/*
* Copyright 2023 Man Group Operations Ltd.
* NO WARRANTY, EXPRESSED OR IMPLIED
*/

#include <arcticdb/version/op_log.hpp>

namespace arcticdb {
    OpLog::OpLog(AtomKey&& key) {
        key_ = std::move(key);
    }

    OpLog::OpLog(StringId id, VersionId version_id, const std::string& action, timestamp creation_ts):
    id_(id),
    version_id_(version_id),
    action_(action),
    creation_ts_(creation_ts)
    {}

    const StringId& OpLog::id() const {
        if (key_.has_value()) {
            return std::get<StringId>((*key_).start_index());
        } else {
            return id_;
        }
    }

    VersionId OpLog::version_id() const {
        if (key_.has_value()) {
            return (*key_).version_id();
        } else {
            return version_id_;
        }
    }

    const std::string& OpLog::action() const {
        if (key_.has_value()) {
            return std::get<std::string>((*key_).id());
        } else {
            return action_;
        }
    }

    timestamp OpLog::creation_ts() const {
        if (key_.has_value()) {
            return (*key_).creation_ts();
        } else {
            return creation_ts_;
        }
    }

    AtomKey&& OpLog::extract_key() {
        util::check(key_.has_value(), "Cannot extract Atomkey from OpLog");
        return std::move(*key_);
    }
}