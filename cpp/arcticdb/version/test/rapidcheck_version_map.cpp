/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <cstdint>
#include <limits>

#include <arcticdb/version/version_map.hpp>
#include <arcticdb/util/test/test_utils.hpp>
#include <arcticdb/util/preprocess.hpp>
#include <arcticdb/stream/test/stream_test_common.hpp>
#include <arcticdb/storage/test/in_memory_store.hpp>

#include <arcticdb/entity/types.hpp>
#include <gtest/gtest.h>
#include <arcticdb/util/test/rapidcheck.hpp>
#include <arcticdb/version/test/version_map_model.hpp>
#include <arcticdb/version/version_functions.hpp>

template <typename Model>
void check_latest_versions(const Model&  s0, MapStorePair &sut, std::string symbol) {
    using namespace arcticdb;
    auto prev = get_latest_version(sut.store_,sut.map_, symbol, pipelines::VersionQuery{}, ReadOptions{});
    auto sut_version_id = prev ? prev.value().version_id() : 0;
    auto model_prev = s0.get_latest_version(symbol);
    auto model_version_id = model_prev ? model_prev.value() : 0;
    RC_ASSERT(sut_version_id == model_version_id);
}

template <typename Model>
void check_latest_undeleted_versions(const Model&  s0, MapStorePair &sut, std::string symbol) {
    using namespace arcticdb;
    pipelines::VersionQuery version_query;
    version_query.set_skip_compat(true),
    version_query.set_iterate_on_failure(true);
    auto prev = get_latest_undeleted_version(sut.store_, sut.map_, symbol, version_query, ReadOptions{});
    auto sut_version_id = prev ? prev.value().version_id() : 0;
    auto model_prev = s0.get_latest_undeleted_version(symbol);
    auto model_version_id = model_prev ? model_prev.value() : 0;
    RC_ASSERT(sut_version_id == model_version_id);
}

template <typename Model>
struct WriteVersion : rc::state::Command<Model, MapStorePair> {
    std::string symbol_;

    explicit WriteVersion(const Model& s0) ARCTICDB_UNUSED:
        symbol_(*rc::gen::elementOf(s0.symbols_)) {}

    void apply(Model &s0) const override {
        s0.write_version(symbol_);
    }

    void run(const Model& s0, MapStorePair &sut) const override {
        check_latest_versions(s0, sut, symbol_);
        sut.write_version(symbol_);
    }

    void show(std::ostream &os) const override {
        os << "WriteVersion(" << symbol_ << ")";
    }
};

template <typename Model>
struct DeleteAllVersions : rc::state::Command<Model, MapStorePair> {
    std::string symbol_;

    explicit DeleteAllVersions(const Model& s0) :
        symbol_(*rc::gen::elementOf(s0.symbols_)) {}

    void apply(Model &s0) const override {
        s0.delete_all_versions(symbol_);
    }

    void run(const Model&, MapStorePair &sut) const override {
        sut.delete_all_versions(symbol_);
    }

    void show(std::ostream &os) const override {
        os << "WriteVersion(" << symbol_ << ")";
    }
};

template <typename Model>
struct WriteAndPrunePreviousVersion : rc::state::Command<Model, MapStorePair> {
    std::string symbol_;

    explicit WriteAndPrunePreviousVersion(const Model& s0) ARCTICDB_UNUSED :
        symbol_(*rc::gen::elementOf(s0.symbols_)) {}

    void apply(Model &s0) const override {
        s0.write_and_prune_previous(symbol_);
    }

    void run(const Model& s0, MapStorePair &sut) const override {
        check_latest_versions(s0, sut, symbol_);
        sut.write_and_prune_previous(symbol_);
    }

    void show(std::ostream &os) const override {
        os << "WriteAndPrunePreviousVersion(" << symbol_ << ")";
    }
};

template <typename Model>
struct GetLatestVersion : rc::state::Command<Model, MapStorePair> {
    std::string symbol_;

    explicit GetLatestVersion(const Model& s0) ARCTICDB_UNUSED :
        symbol_(*rc::gen::elementOf(s0.symbols_)) {}

    void apply(Model &) const override {
    }

    void run(const Model&  s0, MapStorePair &sut) const override {
        ARCTICDB_DEBUG(log::version(), "MapStorePair: get_latest_version");
        check_latest_versions(s0, sut, symbol_);
    }

    void show(std::ostream &os) const override {
        os << "GetLatestVersion(" << symbol_ << ")";
    }
};

template <typename Model>
struct GetLatestUndeletedVersion : rc::state::Command<Model, MapStorePair> {
    std::string symbol_;

    explicit GetLatestUndeletedVersion(const Model& s0) ARCTICDB_UNUSED :
        symbol_(*rc::gen::elementOf(s0.symbols_)) {}

    void apply(Model &) const override {
    }

    void run(const Model&  s0, MapStorePair &sut) const override {
        check_latest_versions(s0, sut, symbol_);
        ARCTICDB_DEBUG(log::version(), "MapStorePair: get latest undeleted");
    }

    void show(std::ostream &os) const override {
        os << "GetLatestVersion(" << symbol_ << ")";
    }
};

template <typename Model>
struct Compact : rc::state::Command<Model, MapStorePair> {
    std::string symbol_;

    explicit Compact(const Model& s0) :
        symbol_(*rc::gen::elementOf(s0.symbols_)) {}

    void apply(Model &) const override {
    }

    void run(const Model& s0, MapStorePair &sut) const override {
        ARCTICDB_DEBUG(log::version(), "MapStorePair: compact");
        sut.map_->compact(sut.store_, symbol_);
        check_latest_versions(s0, sut, symbol_);
    }

    void show(std::ostream &os) const override {
        os << "Compact(" << symbol_ << ")";
    }
};

template <typename Model>
struct DeleteRefKey : rc::state::Command<Model, MapStorePair> {
    std::string symbol_;

    explicit DeleteRefKey(const Model& s0) :
        symbol_(*rc::gen::elementOf(s0.symbols_)) {}

    void apply(Model &) const override {
    }

    void run(const Model& s0, MapStorePair &sut) const override {
        RefKey ref_key{symbol_, KeyType::VERSION_REF};
        try {
            sut.store_->remove_key_sync(ref_key, storage::RemoveOpts{});
        } catch (const std::invalid_argument& ) {
            // Don't care
        }
        check_latest_versions(s0, sut, symbol_);
    }

    void show(std::ostream &os) const override {
        os << "Compact(" << symbol_ << ")";
    }
};

template<typename Model>
struct CompactAndRemoveDeleted : rc::state::Command<Model, MapStorePair> {
    std::string symbol_;

    explicit CompactAndRemoveDeleted(const Model& s0)  ARCTICDB_UNUSED:
        symbol_(*rc::gen::elementOf(s0.symbols_)) {}

    void apply(Model &) const override {
    }

    void run(const Model& s0, MapStorePair &sut) const override {
        ARCTICDB_DEBUG(log::version(), "MapStorePair: compact and remove deleted");
        sut.map_->compact_and_remove_deleted_indexes(sut.store_, symbol_);
        check_latest_versions(s0, sut, symbol_);
    }

    void show(std::ostream &os) const override {
    os << "Compact(" << symbol_ << ")";
    }
};

template <typename Model>
struct GetAllVersions : rc::state::Command<Model, MapStorePair> {
    std::string symbol_;

    explicit GetAllVersions(const Model& s0) ARCTICDB_UNUSED :
        symbol_(*rc::gen::elementOf(s0.symbols_)) {}

    void apply(Model &) const override {
    }

    void run(const Model& s0, MapStorePair &sut) const override {
        auto model_versions = s0.get_all_versions(symbol_);
        using namespace arcticdb;
        auto sut_version = get_all_versions(sut.store_, sut.map_, symbol_, pipelines::VersionQuery{}, ReadOptions{});
        RC_ASSERT(model_versions.size() == sut_version.size());

        for(auto i = size_t{0}; i < model_versions.size(); ++i)
            RC_ASSERT(model_versions[i] == sut_version[i].version_id());
    }

    void show(std::ostream &os) const override {
        os << "GetAllVersions(" << symbol_ << ")";
    }
};

RC_GTEST_PROP(VersionMap, Rapidcheck, ()) {
    VersionMapModel initial_state;
    ScopedConfig max_blocks("VersionMap.MaxVersionBlocks", 1);
    ScopedConfig reload_interval("VersionMap.ReloadInterval", 0);
    auto num_symbols = *rc::gen::inRange(size_t{1}, size_t{5});
    initial_state.symbols_ = *rc::gen::container<std::vector<std::string>>(num_symbols, rc::gen::nonEmpty(rc::gen::string<std::string>()));
    MapStorePair sut(false);
    rc::state::check(initial_state,
        sut,
        rc::state::gen::execOneOfWithArgs<
            WriteVersion<VersionMapModel>,
            WriteAndPrunePreviousVersion<VersionMapModel>,
            GetLatestVersion<VersionMapModel>,
            GetAllVersions<VersionMapModel>,
            DeleteAllVersions<VersionMapModel>,
          //  DeleteRefKey<VersionMapModel>,
            Compact<VersionMapModel>>()
    );
}

RC_GTEST_PROP(VersionMap, RapidcheckTombstones, ()) {
    VersionMapTombstonesModel initial_state;
    auto num_symbols = *rc::gen::inRange(size_t{1}, size_t{5});
    initial_state.symbols_ = *rc::gen::container<std::vector<std::string>>(num_symbols, rc::gen::nonEmpty(rc::gen::string<std::string>()));
    ScopedConfig max_blocks("VersionMap.MaxVersionBlocks", 1);
    ScopedConfig reload_interval("VersionMap.ReloadInterval", 0);
    MapStorePair sut(true);
    sut.map_->set_validate(true);
    rc::state::check(initial_state,
                     sut,
                     rc::state::gen::execOneOfWithArgs<
                         WriteVersion<VersionMapTombstonesModel>,
                         WriteAndPrunePreviousVersion<VersionMapTombstonesModel>,
                         GetLatestVersion<VersionMapTombstonesModel>,
                         GetAllVersions<VersionMapTombstonesModel>,
                         DeleteAllVersions<VersionMapTombstonesModel>,
                         Compact<VersionMapTombstonesModel>>()
    );
}