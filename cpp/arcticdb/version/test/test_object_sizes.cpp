/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <gtest/gtest.h>

#include <arcticdb/storage/test/in_memory_store.hpp>
#include <arcticdb/util/test/generators.hpp>
#include <arcticdb/version/local_versioned_engine.hpp>

namespace arcticdb {

namespace {

/// Sizes one key type cannot be listed. Real storages fail this way per key type - a bucket policy denying a
/// prefix, or a listing that times out - and one such key type must not cost the caller the whole library.
class OneKeyTypeFailsStore : public InMemoryStore {
  public:
    explicit OneKeyTypeFailsStore(KeyType failing) : failing_(failing) {}

    folly::Future<std::shared_ptr<storage::ObjectSizes>> get_object_sizes(
            KeyType key_type, const std::optional<StreamId>&
    ) override {
        if (key_type == failing_) {
            return folly::makeFuture<std::shared_ptr<storage::ObjectSizes>>(
                    std::runtime_error(fmt::format("cannot list {}", key_type))
            );
        }
        return folly::makeFuture(std::make_shared<storage::ObjectSizes>(key_type, 3, 30, 100));
    }

  private:
    KeyType failing_;
};

version_store::LocalVersionedEngine engine_with_failing_key_type(KeyType failing) {
    auto engine = get_test_engine();
    engine._test_set_store(std::make_shared<OneKeyTypeFailsStore>(failing));
    return engine;
}

} // namespace

TEST(ScanObjectSizes, RaisesOnFailureByDefault) {
    auto engine = engine_with_failing_key_type(KeyType::TABLE_INDEX);
    const std::vector<KeyType> key_types{KeyType::TABLE_DATA, KeyType::TABLE_INDEX};

    EXPECT_ANY_THROW(engine.scan_object_sizes(key_types));
}

TEST(ScanObjectSizes, OmitsFailedKeyTypesWhenNotRaising) {
    auto engine = engine_with_failing_key_type(KeyType::TABLE_INDEX);
    const std::vector<KeyType> key_types{KeyType::TABLE_DATA, KeyType::TABLE_INDEX, KeyType::VERSION};

    const auto sizes = engine.scan_object_sizes(key_types, false);

    // The readable key types are still reported, in full, and in the order asked for
    ASSERT_EQ(sizes.size(), 2);
    EXPECT_EQ(sizes[0].key_type_, KeyType::TABLE_DATA);
    EXPECT_EQ(sizes[1].key_type_, KeyType::VERSION);
    EXPECT_EQ(sizes[0].count_, 3);
    EXPECT_EQ(sizes[0].compressed_size_, 30);
    EXPECT_EQ(sizes[0].scan_duration_ns_, 100);
}

TEST(ScanObjectSizes, EveryKeyTypeFailingIsAnEmptyResultNotAnError) {
    auto engine = engine_with_failing_key_type(KeyType::TABLE_DATA);

    EXPECT_TRUE(engine.scan_object_sizes(std::vector<KeyType>{KeyType::TABLE_DATA}, false).empty());
}

} // namespace arcticdb
