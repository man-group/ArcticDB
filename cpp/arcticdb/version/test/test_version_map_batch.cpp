#include <gtest/gtest.h>

#include <arcticdb/version/version_map_batch_methods.hpp>
#include <arcticdb/stream/test/stream_test_common.hpp>
#include <arcticdb/util/test/gtest_utils.hpp>

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
    SKIP_WIN("Exceeds LMDB map size");
    auto store = test_store_->_test_get_store();
    auto version_map = std::make_shared<VersionMap>();

    uint64_t num_streams = 10;
    uint64_t num_versions_per_stream = 5;

    for(uint64_t i = 0; i < num_streams; ++i) {
        auto stream = fmt::format("stream_{}", i);
        add_versions_for_stream(version_map, store, stream, num_versions_per_stream);
    }

    std::vector<StreamId> stream_ids;
    std::vector<VersionQuery> version_queries;

    // Add queries
    for(uint64_t i = 0; i < num_streams; i++){
        auto stream = fmt::format("stream_{}", i);
        for(uint64_t j = 0; j < num_versions_per_stream; j++){
            stream_ids.push_back(StreamId{stream});
            version_queries.push_back(VersionQuery{SpecificVersionQuery{static_cast<SignedVersionId>(j)}, false, false});
        }
    }

    // do batch versions read
    auto versions = folly::collect(batch_get_versions_async(store, version_map, stream_ids, version_queries, false)).get();
    
    // Do the checks
    for(uint64_t i = 0; i < num_streams; i++){
        auto stream = fmt::format("stream_{}", i);
        for(uint64_t j = 0; j < num_versions_per_stream; j++){
            uint64_t idx = i * num_versions_per_stream + j;
            ASSERT_EQ(versions[idx]->id(), StreamId{stream});
            ASSERT_EQ(versions[idx]->version_id(), j);
        }
    }
}

TEST_F(VersionMapBatchStore, SimpleTimestampQueries) {
    SKIP_WIN("Exceeds LMDB map size");
    auto store = test_store_->_test_get_store();
    auto version_map = std::make_shared<VersionMap>();

    uint64_t num_streams = 25;
    uint64_t num_versions_per_stream = 50;

    for(uint64_t i = 0; i < num_streams; ++i) {
        auto stream = fmt::format("stream_{}", i);
        add_versions_for_stream(version_map, store, stream, num_versions_per_stream);
    }

    std::vector<StreamId> stream_ids;
    std::vector<VersionQuery> version_queries;

    // First, for test purposes information, we retrieve the full information the stored data
    // in order to know the timestamps

    // Add queries
    for(uint64_t i = 0; i < num_streams; i++){
        auto stream = fmt::format("stream_{}", i);
        for(uint64_t j = 0; j < num_versions_per_stream; j++){
            stream_ids.push_back(StreamId{stream});
            version_queries.push_back(VersionQuery{SpecificVersionQuery{static_cast<SignedVersionId>(j)}, false, false});
        }
    }

    // do batch versions read
    auto versions = folly::collect(batch_get_versions_async(store, version_map, stream_ids, version_queries, false)).get();

    //Secondly, once we have the timestamps in hand, we are going to query them
    version_queries.clear();
    for(uint64_t i = 0; i < num_streams; i++){
        for(uint64_t j = 0; j < num_versions_per_stream; j++){
            uint64_t idx = i * num_versions_per_stream + j;
            version_queries.push_back(VersionQuery{TimestampVersionQuery{timestamp(versions[idx]->creation_ts())}, false, false});
        }
    }

    // Now we can perform the actual batch query per timestamps
    auto versions_querying_with_timestamp = folly::collect(batch_get_versions_async(store, version_map, stream_ids, version_queries, false)).get();

    // Do the checks
    for(uint64_t i = 0; i < num_streams; i++){
        auto stream = fmt::format("stream_{}", i);
        for(uint64_t j = 0; j < num_versions_per_stream; j++){
            uint64_t idx = i * num_versions_per_stream + j;
            ASSERT_EQ(versions_querying_with_timestamp[idx]->id(), StreamId{stream});
            ASSERT_EQ(versions_querying_with_timestamp[idx]->version_id(), versions[idx]->version_id());
            ASSERT_EQ(versions_querying_with_timestamp[idx]->creation_ts(), versions[idx]->creation_ts());
        }
    }
}


TEST_F(VersionMapBatchStore, MultipleVersionsSameSymbolVersionIdQueries) {
    SKIP_WIN("Exceeds LMDB map size");
    auto store = test_store_->_test_get_store();
    auto version_map = std::make_shared<VersionMap>();

    auto stream = fmt::format("stream_{}", 0);
    uint64_t num_versions = 50;

    // Add versions
    add_versions_for_stream(version_map, store, stream, num_versions);

    std::vector<StreamId> stream_ids;
    std::vector<VersionQuery> version_queries;


    // Add queries
    for(uint64_t i = 0; i < num_versions; i++){
        stream_ids.push_back(StreamId{"stream_0"});
        version_queries.push_back(VersionQuery{SpecificVersionQuery{static_cast<SignedVersionId>(i)}, false, false});
    }

    // Do query
    auto versions = folly::collect(batch_get_versions_async(store, version_map, stream_ids, version_queries, false)).get();
    
    // Check results
    for(uint64_t i = 0; i < num_versions; i++){
        ASSERT_EQ(versions[i]->id(), StreamId{stream});
        ASSERT_EQ(versions[i]->version_id(), i);
    }
}


TEST_F(VersionMapBatchStore, MultipleVersionsSameSymbolTimestampQueries) {
    SKIP_WIN("Exceeds LMDB map size");
    auto store = test_store_->_test_get_store();
    auto version_map = std::make_shared<VersionMap>();

    uint64_t num_versions = 50;
    
    // Add versions
    auto stream = fmt::format("stream_{}", 0);
    std::vector<StreamId> stream_ids;
    std::vector<VersionQuery> version_queries;
    add_versions_for_stream(version_map, store, stream, num_versions);

    // First, for test purposes information, we retrieve the full information the stored data
    // in order to know the timestamps

    // Add queries
    for(uint64_t i = 0; i < num_versions; i++){
        stream_ids.push_back(StreamId{"stream_0"});
        version_queries.push_back(VersionQuery{SpecificVersionQuery{static_cast<SignedVersionId>(i)}, false, false});
    }

    // Do query
    auto versions = folly::collect(batch_get_versions_async(store, version_map, stream_ids, version_queries, false)).get();

    //Secondly, once we have the timestamps in hand, we are going to query them
    version_queries.clear();
    for(uint64_t i = 0; i < num_versions; i++){
        version_queries.push_back(VersionQuery{TimestampVersionQuery{timestamp(versions[i]->creation_ts())}, false, false});
    }

    // Now we can perform the actual batch query per timestamps
    auto versions_querying_with_timestamp = folly::collect(batch_get_versions_async(store, version_map, stream_ids, version_queries, false)).get();

    // Do the checks
    for(uint64_t i = 0; i < num_versions; i++){
        ASSERT_EQ(versions_querying_with_timestamp[i]->id(), StreamId{stream});
        ASSERT_EQ(versions_querying_with_timestamp[i]->version_id(), versions[i]->version_id());
        ASSERT_EQ(versions_querying_with_timestamp[i]->creation_ts(), versions[i]->creation_ts());
    }
}

TEST_F(VersionMapBatchStore, CombinedQueries) {
    SKIP_WIN("Exceeds LMDB map size");
    auto store = test_store_->_test_get_store();
    auto version_map = std::make_shared<VersionMap>();

    uint64_t num_streams = 10;
    uint64_t num_versions_per_stream = 5;

    for(uint64_t i = 0; i < num_streams; ++i) {
        auto stream = fmt::format("stream_{}", i);
        add_versions_for_stream(version_map, store, stream, num_versions_per_stream);
    }

    std::vector<StreamId> stream_ids;
    std::vector<VersionQuery> version_queries;

    // First, for test purposes information, we retrieve the full information the stored data
    // in order to know the timestamps

    // Add queries
    for(uint64_t i = 0; i < num_streams; i++){
        auto stream = fmt::format("stream_{}", i);
        for(uint64_t j = 0; j < num_versions_per_stream; j++){
            stream_ids.push_back(StreamId{stream});
            version_queries.push_back(VersionQuery{SpecificVersionQuery{static_cast<SignedVersionId>(j)}, false, false});
        }
    }

    // do batch versions read
    auto versions = folly::collect(batch_get_versions_async(store, version_map, stream_ids, version_queries, false)).get();

    //Secondly, once we have the timestamps in hand, we are going to query them
    version_queries.clear();
    stream_ids.clear();
    for(uint64_t i = 0; i < num_streams; i++){
        auto stream = fmt::format("stream_{}", i);
        for(uint64_t j = 0; j < num_versions_per_stream; j++){
            uint64_t idx = i * num_versions_per_stream + j;
            stream_ids.push_back(StreamId{stream});
            version_queries.push_back(VersionQuery{SpecificVersionQuery{static_cast<SignedVersionId>(j)}, false, false});
            stream_ids.push_back(StreamId{stream});
            version_queries.push_back(VersionQuery{TimestampVersionQuery{timestamp(versions[idx]->creation_ts())}, false, false});
            stream_ids.push_back(StreamId{stream});
            version_queries.push_back(VersionQuery{std::monostate{}, false, false});
        }
    }

    auto versions_querying_with_mix_types = folly::collect(batch_get_versions_async(store, version_map, stream_ids, version_queries, false)).get();

    // Do the checks
    for(uint64_t i = 0; i < num_streams; i++){
        auto stream = fmt::format("stream_{}", i);
        for(uint64_t j = 0; j < num_versions_per_stream; j++){
            uint64_t idx_versions = i * num_versions_per_stream + j;
            uint64_t idx = idx_versions * 3;
            ASSERT_EQ(versions_querying_with_mix_types[idx]->id(), StreamId{stream});
            ASSERT_EQ(versions_querying_with_mix_types[idx + 1]->id(), StreamId{stream});
            ASSERT_EQ(versions_querying_with_mix_types[idx + 2]->id(), StreamId{stream});
            ASSERT_EQ(versions_querying_with_mix_types[idx]->version_id(), j);
            ASSERT_EQ(versions_querying_with_mix_types[idx + 1]->version_id(), versions[idx_versions]->version_id());
            ASSERT_EQ(versions_querying_with_mix_types[idx + 1]->creation_ts(), versions[idx_versions]->creation_ts());
            ASSERT_EQ(versions_querying_with_mix_types[idx + 2]->version_id(), num_versions_per_stream - 1);
        }
    }
}
