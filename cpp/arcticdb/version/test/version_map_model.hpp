/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/version/version_functions.hpp>
#include <arcticdb/util/test/test_utils.hpp>
#include <arcticdb/util/preprocess.hpp>
#include <arcticdb/stream/test/stream_test_common.hpp>
#include <arcticdb/storage/test/in_memory_store.hpp>
#include <arcticdb/version/test/version_backwards_compat.hpp>
#include <arcticdb/entity/types.hpp>

namespace arcticdb {

AtomKey make_test_index_key(std::string id,
                           VersionId version_id,
                           KeyType key_type,
                           IndexValue index_start = 0,
                           IndexValue index_end = 0,
                           timestamp creation_ts = PilotedClock::nanos_since_epoch()) {
    return atom_key_builder().version_id(version_id).start_index(index_start).end_index(index_end).creation_ts(
            creation_ts)
        .content_hash(0).build(id, key_type);
}

struct MapStorePair {
    const bool tombstones_;

    MapStorePair(bool tombstones) :
    tombstones_(tombstones),
    store_(std::make_shared<InMemoryStore>()) {
    }

    void write_version(const std::string &id) {
        log::version().info("MapStorePair, write version {}", id);
        auto prev = get_latest_version(store_, map_, id, pipelines::VersionQuery{}, ReadOptions{});
        auto version_id = prev ? prev.value().version_id() + 1 : 0;
        map_->write_version(store_, make_test_index_key(id, version_id, KeyType::TABLE_INDEX));
    }

    void delete_all_versions(const std::string &id) {
        log::version().info("MapStorePair, delete_all_versions {}", id);
        if(tombstones_)
            map_->delete_all_versions(store_, id);
        else
            backwards_compat_delete_all_versions(store_, map_, id);
    }

    void write_and_prune_previous(const std::string &id) {
        log::version().info("MapStorePair, write_and_prune_previous version {}", id);
        auto prev = get_latest_version(store_, map_, id, pipelines::VersionQuery{}, ReadOptions{});
        auto version_id = prev ? prev.value().version_id() + 1 : 0;

        if(tombstones_)
            map_->write_and_prune_previous(store_,  make_test_index_key(id, version_id, KeyType::TABLE_INDEX), prev);
        else
            backwards_compat_write_and_prune_previous(store_, map_, make_test_index_key(id, version_id, KeyType::TABLE_INDEX));
    }

    std::shared_ptr<VersionMap> map_ = std::make_shared<VersionMap>();
    std::shared_ptr<Store> store_;
};

struct VersionMapModel {
    std::unordered_map<StringId, std::set<VersionId, std::greater<VersionId>>> data_;
    std::vector<std::string> symbols_;

    std::optional<VersionId> get_latest_version(const std::string &id) const {
        auto it = data_.find(id);
        return it == data_.end() || it->second.empty() ? std::nullopt
                                                       : std::make_optional<VersionId>(*it->second.begin());
    }

    std::vector<VersionId> get_all_versions(const std::string &id) const {
        std::vector<VersionId> output;
        auto it = data_.find(id);
        if (it != data_.end()) {
            output.assign(std::begin(it->second), std::end(it->second));
        }
        return output;
    }

    void write_version(const std::string &id) {
        auto prev = get_latest_version(id);
        auto version_id = prev ? prev.value() + 1 : 0;
        data_[id].insert(version_id);
    }

    void delete_all_versions(const std::string &id) {
        data_[id].clear();
    }

    void write_and_prune_previous(const std::string &id) {
        auto prev = get_latest_version(id);
        VersionId version_id{0};
        if (prev) {
            version_id = prev.value() + 1;
            data_[id].clear();
        }
        data_[id].insert(version_id);
    }
};

struct VersionMapTombstonesModel {
    std::unordered_map<StringId, std::set<VersionId, std::greater<VersionId>>> data_;
    std::unordered_map<StringId, std::unordered_set<VersionId>> tombstones_;
    std::vector<std::string> symbols_;

    VersionMapTombstonesModel() {
    };

    std::optional<VersionId> get_latest_version(const std::string &id) const {
        log::version().info("VersionMapTombstonesModel, get_latest_version {}", id);
        auto it = data_.find(id);
        return it == data_.end() || it->second.empty() ? std::nullopt
                                                       : std::make_optional<VersionId>(*it->second.begin());
    }

    std::optional<VersionId> get_latest_undeleted_version(const std::string &id) const {
        log::version().info("VersionMapTombstonesModel, get_latest_undeleted_version {}", id);
        auto it = data_.find(id);
        if(it == data_.end()) return std::nullopt;

        auto tombstones = tombstones_.find(id);
        for(auto v : it->second) {
            if(tombstones == tombstones_.end() || tombstones->second.find(v) == tombstones->second.end())
                return v;
        }
        return  std::nullopt;
    }

    std::vector<VersionId> get_all_versions(const std::string &id) const {
        log::version().info("VersionMapTombstonesModel, get_all_versions", id);
        std::vector<VersionId> output;
        auto it = data_.find(id);
        if (it != data_.end()) {
            auto tombstones = tombstones_.find(id);
            std::copy_if(std::begin(it->second), std::end(it->second), std::back_inserter(output), [&] (auto v) {
                return tombstones == tombstones_.end() || tombstones->second.find(v) == tombstones->second.end();
            });
        }
        return output;
    }

    void write_version(const std::string &id) {
        log::version().info("VersionMapTombstonesModel, write version {}", id);
        auto prev = get_latest_version(id);
        auto version_id = prev ? prev.value() + 1 : 0;
        data_[id].insert(version_id);
    }

    void delete_versions(const std::vector<VersionId> versions, const std::string& id) {
        log::version().info("VersionMapTombstonesModel, delete_versions {}", id);
        auto& tombstones = tombstones_[id];
        for(auto v : versions)
            tombstones.insert(v);
    }

    void delete_all_versions(const std::string &id) {
        delete_versions(get_all_versions(id), id);
    }

    void write_and_prune_previous(const std::string &id) {
        log::version().info("VersionMapTombstonesModel, write_and_prune_previous version {}", id);
        auto prev = get_latest_version(id);
        VersionId version_id{0};
        if (prev) {
            version_id = prev.value() + 1;
            delete_all_versions(id);
        }
        data_[id].insert(version_id);
    }
};
} // namespace arcticdb