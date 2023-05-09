#include <gtest/gtest.h>

#include <arcticdb/version/version_map_batch_methods.hpp>
#include <arcticdb/stream/test/stream_test_common.hpp>

struct VersionMapBatchStore : arcticdb::TestStore {
protected:
    std::string get_name() override {
        return "version_map_batch";
    }
};

namespace arcticdb {

AtomKey test_index_key(StreamId id, VersionId version_id) {
    return atom_key_builder().version_id(version_id).creation_ts(PilotedClock::nanos_since_epoch()).content_hash(3)
        .start_index(4).end_index(5).build(id, KeyType::TABLE_INDEX);
}

void add_versions_for_stream(
    std::shared_ptr<VersionMap> version_map,
    std::shared_ptr<Store> store,
    StreamId stream_id,
    size_t num_versions,
    size_t start = 0u) {
    for(auto i = start; i < start + num_versions; ++i) {
        version_map->write_version(store, test_index_key(stream_id, i));
    }
};
}

TEST_F(VersionMapBatchStore, Simple) {
    using namespace arcticdb;
    using namespace arcticdb::pipelines;

    auto store = test_store_->_test_get_store();
    auto version_map = std::make_shared<VersionMap>();

    for(auto i = 0u; i < 5; ++i) {
        auto stream = fmt::format("stream_{}", i);
        add_versions_for_stream(version_map, store, stream, 5);
    }

    for(auto i = 5u; i < 10u; ++i) {
        auto stream = fmt::format("stream_{}", i);
        add_versions_for_stream(version_map, store, stream, 5);
    }

    for(auto i = 0u; i < 5; ++i) {
        auto stream = fmt::format("stream_{}", i);
        add_versions_for_stream(version_map, store, stream, 5, 5);
    }

    std::vector<StreamId> stream_ids;
    std::vector<VersionQuery> version_queries;

    stream_ids.push_back(StreamId{"stream_1"});
    version_queries.push_back(VersionQuery{SpecificVersionQuery{4}, false, false});
    stream_ids.push_back(StreamId{"stream_4"});
    version_queries.push_back(VersionQuery{SpecificVersionQuery{4}, false, false});
    stream_ids.push_back(StreamId{"stream_1"});
    version_queries.push_back(VersionQuery{SpecificVersionQuery{1}, false, false});
    stream_ids.push_back(StreamId{"stream_3"});
    version_queries.push_back(VersionQuery{SpecificVersionQuery{7}, false, false});
    stream_ids.push_back(StreamId{"stream_8"});
    version_queries.push_back(VersionQuery{SpecificVersionQuery{4}, false, false});


    auto versions = folly::collect(batch_get_versions(store, version_map, stream_ids, version_queries)).get();
    ASSERT_EQ(versions[0]->id(), StreamId{"stream_1"});
    ASSERT_EQ(versions[0]->version_id(), 4);

    ASSERT_EQ(versions[1]->id(), StreamId{"stream_4"});
    ASSERT_EQ(versions[1]->version_id(), 4);

    ASSERT_EQ(versions[2]->id(), StreamId{"stream_1"});
    ASSERT_EQ(versions[2]->version_id(), 1);

    ASSERT_EQ(versions[3]->id(), StreamId{"stream_3"});
    ASSERT_EQ(versions[3]->version_id(), 7);

    ASSERT_EQ(versions[4]->id(), StreamId{"stream_8"});
    ASSERT_EQ(versions[4]->version_id(), 4);
}