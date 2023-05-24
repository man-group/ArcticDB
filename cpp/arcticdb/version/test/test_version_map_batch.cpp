#include <gtest/gtest.h>

#include <arcticdb/version/version_map_batch_methods.hpp>
#include <arcticdb/stream/test/stream_test_common.hpp>

using namespace arcticdb;
using namespace arcticdb::pipelines;

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

TEST_F(VersionMapBatchStore, SimpleVersionIdQueries) {
    auto store = test_store_->_test_get_store();
    auto version_map = std::make_shared<VersionMap>();

    for(auto i = 0u; i < 10; ++i) {
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

TEST_F(VersionMapBatchStore, SimpleTimestampQueries) {
    auto store = test_store_->_test_get_store();
    auto version_map = std::make_shared<VersionMap>();

    for(auto i = 0u; i < 10; ++i) {
        auto stream = fmt::format("stream_{}", i);
        add_versions_for_stream(version_map, store, stream, 5);
    }

    for(auto i = 0u; i < 5; ++i) {
        auto stream = fmt::format("stream_{}", i);
        add_versions_for_stream(version_map, store, stream, 5, 5);
    }

    std::vector<StreamId> stream_ids;
    std::vector<VersionQuery> version_queries;

    // First, for test purposes information, we retrieve the information of

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


    //Secondly, once we have the timestamps in hand, we are going to query them

    version_queries.clear();
    version_queries.push_back(VersionQuery{TimestampVersionQuery{timestamp(versions[0]->creation_ts())}, false, false});
    version_queries.push_back(VersionQuery{TimestampVersionQuery{timestamp(versions[1]->creation_ts())}, false, false});
    version_queries.push_back(VersionQuery{TimestampVersionQuery{timestamp(versions[2]->creation_ts())}, false, false});
    version_queries.push_back(VersionQuery{TimestampVersionQuery{timestamp(versions[3]->creation_ts())}, false, false});
    version_queries.push_back(VersionQuery{TimestampVersionQuery{timestamp(versions[4]->creation_ts())}, false, false});


    auto versions_querying_with_timestamp = folly::collect(batch_get_versions(store, version_map, stream_ids, version_queries)).get();

    ASSERT_EQ(versions_querying_with_timestamp[0]->id(), StreamId{"stream_1"});
    ASSERT_EQ(versions_querying_with_timestamp[0]->version_id(), versions[0]->version_id());

    ASSERT_EQ(versions_querying_with_timestamp[1]->id(), StreamId{"stream_4"});
    ASSERT_EQ(versions_querying_with_timestamp[1]->version_id(), versions[1]->version_id());

    ASSERT_EQ(versions_querying_with_timestamp[2]->id(), StreamId{"stream_1"});
    ASSERT_EQ(versions_querying_with_timestamp[2]->version_id(), versions[2]->version_id());

    ASSERT_EQ(versions_querying_with_timestamp[3]->id(), StreamId{"stream_3"});
    ASSERT_EQ(versions_querying_with_timestamp[3]->version_id(), versions[3]->version_id());

    ASSERT_EQ(versions_querying_with_timestamp[4]->id(), StreamId{"stream_8"});
    ASSERT_EQ(versions_querying_with_timestamp[4]->version_id(), versions[4]->version_id());
}


TEST_F(VersionMapBatchStore, MultipleVersionsSameSymbolVersionIdQueries) {
    auto store = test_store_->_test_get_store();
    auto version_map = std::make_shared<VersionMap>();

    auto stream = fmt::format("stream_{}", 0);
    add_versions_for_stream(version_map, store, stream, 50);

    std::vector<StreamId> stream_ids;
    std::vector<VersionQuery> version_queries;

    stream_ids.push_back(StreamId{"stream_0"});
    version_queries.push_back(VersionQuery{SpecificVersionQuery{4}, false, false});
    stream_ids.push_back(StreamId{"stream_0"});
    version_queries.push_back(VersionQuery{SpecificVersionQuery{44}, false, false});
    stream_ids.push_back(StreamId{"stream_0"});
    version_queries.push_back(VersionQuery{SpecificVersionQuery{8}, false, false});
    stream_ids.push_back(StreamId{"stream_0"});
    version_queries.push_back(VersionQuery{SpecificVersionQuery{7}, false, false});
    stream_ids.push_back(StreamId{"stream_0"});
    version_queries.push_back(VersionQuery{SpecificVersionQuery{6}, false, false});
    stream_ids.push_back(StreamId{"stream_0"});
    version_queries.push_back(VersionQuery{SpecificVersionQuery{9}, false, false});
    stream_ids.push_back(StreamId{"stream_0"});
    version_queries.push_back(VersionQuery{SpecificVersionQuery{30}, false, false});
    stream_ids.push_back(StreamId{"stream_0"});
    version_queries.push_back(VersionQuery{SpecificVersionQuery{1}, false, false});
    stream_ids.push_back(StreamId{"stream_0"});
    version_queries.push_back(VersionQuery{SpecificVersionQuery{33}, false, false});
    stream_ids.push_back(StreamId{"stream_0"});
    version_queries.push_back(VersionQuery{SpecificVersionQuery{29}, false, false});
    stream_ids.push_back(StreamId{"stream_0"});
    version_queries.push_back(VersionQuery{SpecificVersionQuery{45}, false, false});

    auto versions = folly::collect(batch_get_versions(store, version_map, stream_ids, version_queries)).get();
    ASSERT_EQ(versions[0]->id(), StreamId{"stream_0"});
    ASSERT_EQ(versions[0]->version_id(), 4);

    ASSERT_EQ(versions[1]->id(), StreamId{"stream_0"});
    ASSERT_EQ(versions[1]->version_id(), 44);

    ASSERT_EQ(versions[2]->id(), StreamId{"stream_0"});
    ASSERT_EQ(versions[2]->version_id(), 8);

    ASSERT_EQ(versions[3]->id(), StreamId{"stream_0"});
    ASSERT_EQ(versions[3]->version_id(), 7);

    ASSERT_EQ(versions[4]->id(), StreamId{"stream_0"});
    ASSERT_EQ(versions[4]->version_id(), 6);

    ASSERT_EQ(versions[5]->id(), StreamId{"stream_0"});
    ASSERT_EQ(versions[5]->version_id(), 9);

    ASSERT_EQ(versions[6]->id(), StreamId{"stream_0"});
    ASSERT_EQ(versions[6]->version_id(), 30);

    ASSERT_EQ(versions[7]->id(), StreamId{"stream_0"});
    ASSERT_EQ(versions[7]->version_id(), 1);

    ASSERT_EQ(versions[8]->id(), StreamId{"stream_0"});
    ASSERT_EQ(versions[8]->version_id(),33);

    ASSERT_EQ(versions[9]->id(), StreamId{"stream_0"});
    ASSERT_EQ(versions[9]->version_id(), 29);

    ASSERT_EQ(versions[10]->id(), StreamId{"stream_0"});
    ASSERT_EQ(versions[10]->version_id(), 45);
}


TEST_F(VersionMapBatchStore, MultipleVersionsSameSymbolTimestampQueries) {
    auto store = test_store_->_test_get_store();
    auto version_map = std::make_shared<VersionMap>();

    auto stream = fmt::format("stream_{}", 0);
    add_versions_for_stream(version_map, store, stream, 50);

    std::vector<StreamId> stream_ids;
    std::vector<VersionQuery> version_queries;

    stream_ids.push_back(StreamId{"stream_0"});
    version_queries.push_back(VersionQuery{SpecificVersionQuery{4}, false, false});
    stream_ids.push_back(StreamId{"stream_0"});
    version_queries.push_back(VersionQuery{SpecificVersionQuery{44}, false, false});
    stream_ids.push_back(StreamId{"stream_0"});
    version_queries.push_back(VersionQuery{SpecificVersionQuery{8}, false, false});
    stream_ids.push_back(StreamId{"stream_0"});
    version_queries.push_back(VersionQuery{SpecificVersionQuery{7}, false, false});
    stream_ids.push_back(StreamId{"stream_0"});
    version_queries.push_back(VersionQuery{SpecificVersionQuery{6}, false, false});
    stream_ids.push_back(StreamId{"stream_0"});
    version_queries.push_back(VersionQuery{SpecificVersionQuery{9}, false, false});
    stream_ids.push_back(StreamId{"stream_0"});
    version_queries.push_back(VersionQuery{SpecificVersionQuery{30}, false, false});
    stream_ids.push_back(StreamId{"stream_0"});
    version_queries.push_back(VersionQuery{SpecificVersionQuery{1}, false, false});
    stream_ids.push_back(StreamId{"stream_0"});
    version_queries.push_back(VersionQuery{SpecificVersionQuery{33}, false, false});
    stream_ids.push_back(StreamId{"stream_0"});
    version_queries.push_back(VersionQuery{SpecificVersionQuery{29}, false, false});
    stream_ids.push_back(StreamId{"stream_0"});
    version_queries.push_back(VersionQuery{SpecificVersionQuery{45}, false, false});


    auto versions = folly::collect(batch_get_versions(store, version_map, stream_ids, version_queries)).get();


    //Secondly, once we have the timestamps in hand, we are going to query them

    version_queries.clear();
    version_queries.push_back(VersionQuery{TimestampVersionQuery{timestamp(versions[0]->creation_ts())}, false, false});
    version_queries.push_back(VersionQuery{TimestampVersionQuery{timestamp(versions[1]->creation_ts())}, false, false});
    version_queries.push_back(VersionQuery{TimestampVersionQuery{timestamp(versions[2]->creation_ts())}, false, false});
    version_queries.push_back(VersionQuery{TimestampVersionQuery{timestamp(versions[3]->creation_ts())}, false, false});
    version_queries.push_back(VersionQuery{TimestampVersionQuery{timestamp(versions[4]->creation_ts())}, false, false});
    version_queries.push_back(VersionQuery{TimestampVersionQuery{timestamp(versions[5]->creation_ts())}, false, false});
    version_queries.push_back(VersionQuery{TimestampVersionQuery{timestamp(versions[6]->creation_ts())}, false, false});
    version_queries.push_back(VersionQuery{TimestampVersionQuery{timestamp(versions[7]->creation_ts())}, false, false});
    version_queries.push_back(VersionQuery{TimestampVersionQuery{timestamp(versions[8]->creation_ts())}, false, false});
    version_queries.push_back(VersionQuery{TimestampVersionQuery{timestamp(versions[9]->creation_ts())}, false, false});
    version_queries.push_back(VersionQuery{TimestampVersionQuery{timestamp(versions[10]->creation_ts())}, false, false});

    auto versions_querying_with_timestamp = folly::collect(batch_get_versions(store, version_map, stream_ids, version_queries)).get();

    ASSERT_EQ(versions_querying_with_timestamp[0]->id(), StreamId{"stream_0"});
    ASSERT_EQ(versions_querying_with_timestamp[0]->version_id(), versions[0]->version_id());

    ASSERT_EQ(versions_querying_with_timestamp[1]->id(), StreamId{"stream_0"});
    ASSERT_EQ(versions_querying_with_timestamp[1]->version_id(), versions[1]->version_id());

    ASSERT_EQ(versions_querying_with_timestamp[2]->id(), StreamId{"stream_0"});
    ASSERT_EQ(versions_querying_with_timestamp[2]->version_id(), versions[2]->version_id());

    ASSERT_EQ(versions_querying_with_timestamp[3]->id(), StreamId{"stream_0"});
    ASSERT_EQ(versions_querying_with_timestamp[3]->version_id(), versions[3]->version_id());

    ASSERT_EQ(versions_querying_with_timestamp[4]->id(), StreamId{"stream_0"});
    ASSERT_EQ(versions_querying_with_timestamp[4]->version_id(), versions[4]->version_id());

    ASSERT_EQ(versions_querying_with_timestamp[5]->id(), StreamId{"stream_0"});
    ASSERT_EQ(versions_querying_with_timestamp[5]->version_id(), versions[5]->version_id());

    ASSERT_EQ(versions_querying_with_timestamp[6]->id(), StreamId{"stream_0"});
    ASSERT_EQ(versions_querying_with_timestamp[6]->version_id(), versions[6]->version_id());

    ASSERT_EQ(versions_querying_with_timestamp[7]->id(), StreamId{"stream_0"});
    ASSERT_EQ(versions_querying_with_timestamp[7]->version_id(), versions[7]->version_id());

    ASSERT_EQ(versions_querying_with_timestamp[8]->id(), StreamId{"stream_0"});
    ASSERT_EQ(versions_querying_with_timestamp[8]->version_id(), versions[8]->version_id());

    ASSERT_EQ(versions_querying_with_timestamp[9]->id(), StreamId{"stream_0"});
    ASSERT_EQ(versions_querying_with_timestamp[9]->version_id(), versions[9]->version_id());

    ASSERT_EQ(versions_querying_with_timestamp[10]->id(), StreamId{"stream_0"});
    ASSERT_EQ(versions_querying_with_timestamp[10]->version_id(), versions[10]->version_id());
}

TEST_F(VersionMapBatchStore, CombinedQueries) {
    auto store = test_store_->_test_get_store();
    auto version_map = std::make_shared<VersionMap>();

    auto stream = fmt::format("stream_{}", 0);
    add_versions_for_stream(version_map, store, stream, 50);

    for(auto i = 1u; i < 5; ++i) {
        auto stream = fmt::format("stream_{}", i);
        add_versions_for_stream(version_map, store, stream, 10);
    }

    std::vector<StreamId> stream_ids;
    std::vector<VersionQuery> version_queries;

    //First pass we do an initial set of mixed requests but only for version if queries, to retrieve the timestamps
    stream_ids.push_back(StreamId{"stream_0"});
    version_queries.push_back(VersionQuery{SpecificVersionQuery{4}, false, false});
    stream_ids.push_back(StreamId{"stream_0"});
    version_queries.push_back(VersionQuery{SpecificVersionQuery{44}, false, false});
    stream_ids.push_back(StreamId{"stream_0"});
    version_queries.push_back(VersionQuery{SpecificVersionQuery{8}, false, false});
    stream_ids.push_back(StreamId{"stream_1"});
    version_queries.push_back(VersionQuery{std::monostate{}, false, false});
    stream_ids.push_back(StreamId{"stream_0"});
    version_queries.push_back(VersionQuery{SpecificVersionQuery{6}, false, false});
    stream_ids.push_back(StreamId{"stream_2"});
    version_queries.push_back(VersionQuery{SpecificVersionQuery{9}, false, false});
    stream_ids.push_back(StreamId{"stream_0"});
    version_queries.push_back(VersionQuery{std::monostate{}, false, false});
    stream_ids.push_back(StreamId{"stream_4"});
    version_queries.push_back(VersionQuery{SpecificVersionQuery{1}, false, false});
    stream_ids.push_back(StreamId{"stream_0"});
    version_queries.push_back(VersionQuery{SpecificVersionQuery{33}, false, false});
    stream_ids.push_back(StreamId{"stream_3"});
    version_queries.push_back(VersionQuery{SpecificVersionQuery{8}, false, false});
    stream_ids.push_back(StreamId{"stream_0"});
    version_queries.push_back(VersionQuery{SpecificVersionQuery{49}, false, false});
    stream_ids.push_back(StreamId{"stream_0"});
    version_queries.push_back(VersionQuery{SpecificVersionQuery{48}, false, false});


    auto versions = folly::collect(batch_get_versions(store, version_map, stream_ids, version_queries)).get();
    ASSERT_EQ(versions[0]->id(), StreamId{"stream_0"});
    ASSERT_EQ(versions[0]->version_id(), 4);

    ASSERT_EQ(versions[1]->id(), StreamId{"stream_0"});
    ASSERT_EQ(versions[1]->version_id(), 44);

    ASSERT_EQ(versions[2]->id(), StreamId{"stream_0"});
    ASSERT_EQ(versions[2]->version_id(), 8);

    ASSERT_EQ(versions[3]->id(), StreamId{"stream_1"});
    ASSERT_EQ(versions[3]->version_id(), 9);

    ASSERT_EQ(versions[4]->id(), StreamId{"stream_0"});
    ASSERT_EQ(versions[4]->version_id(), 6);

    ASSERT_EQ(versions[5]->id(), StreamId{"stream_2"});
    ASSERT_EQ(versions[5]->version_id(), 9);

    ASSERT_EQ(versions[6]->id(), StreamId{"stream_0"});
    ASSERT_EQ(versions[6]->version_id(), 49);

    ASSERT_EQ(versions[7]->id(), StreamId{"stream_4"});
    ASSERT_EQ(versions[7]->version_id(), 1);

    ASSERT_EQ(versions[8]->id(), StreamId{"stream_0"});
    ASSERT_EQ(versions[8]->version_id(),33);

    ASSERT_EQ(versions[9]->id(), StreamId{"stream_3"});
    ASSERT_EQ(versions[9]->version_id(), 8);

    ASSERT_EQ(versions[10]->id(), StreamId{"stream_0"});
    ASSERT_EQ(versions[10]->version_id(), 49);

    ASSERT_EQ(versions[11]->id(), StreamId{"stream_0"});
    ASSERT_EQ(versions[11]->version_id(), 48);

    //Second pass we combine all the type of requests, using both version id and timestamp requests

    version_queries.clear();

    version_queries.push_back(VersionQuery{SpecificVersionQuery{4}, false, false});
    version_queries.push_back(VersionQuery{SpecificVersionQuery{44}, false, false});
    version_queries.push_back(VersionQuery{SpecificVersionQuery{8}, false, false});
    version_queries.push_back(VersionQuery{std::monostate{}, false, false});
    version_queries.push_back(VersionQuery{TimestampVersionQuery{timestamp(versions[4]->creation_ts())}, false, false});
    version_queries.push_back(VersionQuery{SpecificVersionQuery{9}, false, false});
    version_queries.push_back(VersionQuery{std::monostate{}, false, false});
    version_queries.push_back(VersionQuery{SpecificVersionQuery{1}, false, false});
    version_queries.push_back(VersionQuery{TimestampVersionQuery{timestamp(versions[8]->creation_ts())}, false, false});
    version_queries.push_back(VersionQuery{SpecificVersionQuery{8}, false, false});
    version_queries.push_back(VersionQuery{SpecificVersionQuery{49}, false, false});
    version_queries.push_back(VersionQuery{TimestampVersionQuery{timestamp(versions[11]->creation_ts())}, false, false});

    auto versions_querying_with_mix_types = folly::collect(batch_get_versions(store, version_map, stream_ids, version_queries)).get();
    ASSERT_EQ(versions_querying_with_mix_types[0]->id(), StreamId{"stream_0"});
    ASSERT_EQ(versions_querying_with_mix_types[0]->version_id(), 4);

    ASSERT_EQ(versions_querying_with_mix_types[1]->id(), StreamId{"stream_0"});
    ASSERT_EQ(versions_querying_with_mix_types[1]->version_id(), 44);

    ASSERT_EQ(versions_querying_with_mix_types[2]->id(), StreamId{"stream_0"});
    ASSERT_EQ(versions_querying_with_mix_types[2]->version_id(), 8);

    ASSERT_EQ(versions_querying_with_mix_types[3]->id(), StreamId{"stream_1"});
    ASSERT_EQ(versions_querying_with_mix_types[3]->version_id(), 9);

    ASSERT_EQ(versions_querying_with_mix_types[4]->id(), StreamId{"stream_0"});
    ASSERT_EQ(versions_querying_with_mix_types[4]->version_id(), versions[4]->version_id());

    ASSERT_EQ(versions_querying_with_mix_types[5]->id(), StreamId{"stream_2"});
    ASSERT_EQ(versions_querying_with_mix_types[5]->version_id(), 9);

    ASSERT_EQ(versions_querying_with_mix_types[6]->id(), StreamId{"stream_0"});
    ASSERT_EQ(versions_querying_with_mix_types[6]->version_id(), 49);

    ASSERT_EQ(versions_querying_with_mix_types[7]->id(), StreamId{"stream_4"});
    ASSERT_EQ(versions_querying_with_mix_types[7]->version_id(), 1);

    ASSERT_EQ(versions_querying_with_mix_types[8]->id(), StreamId{"stream_0"});
    ASSERT_EQ(versions_querying_with_mix_types[8]->version_id(), versions[8]->version_id());

    ASSERT_EQ(versions_querying_with_mix_types[9]->id(), StreamId{"stream_3"});
    ASSERT_EQ(versions_querying_with_mix_types[9]->version_id(), 8);

    ASSERT_EQ(versions_querying_with_mix_types[10]->id(), StreamId{"stream_0"});
    ASSERT_EQ(versions_querying_with_mix_types[10]->version_id(), 49);

    ASSERT_EQ(versions_querying_with_mix_types[11]->id(), StreamId{"stream_0"});
    ASSERT_EQ(versions_querying_with_mix_types[11]->version_id(), versions[11]->version_id());
}
