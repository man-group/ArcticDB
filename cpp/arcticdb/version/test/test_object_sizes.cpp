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

/// A store where the chosen key types cannot be listed. Real storages fail this way per key type - a bucket
/// policy denying a prefix, or a listing that times out - and one such key type must not cost the caller the
/// whole library.
class ScanFailsStore : public InMemoryStore {
  public:
    explicit ScanFailsStore(std::function<bool(KeyType)> fails) : fails_(std::move(fails)) {}

    folly::Future<std::shared_ptr<storage::ObjectSizes>>
    get_object_sizes(KeyType key_type, const std::optional<StreamId>&) override {
        if (fails_(key_type)) {
            return folly::makeFuture<std::shared_ptr<storage::ObjectSizes>>(
                    std::runtime_error(fmt::format("cannot list {}", key_type))
            );
        }
        return folly::makeFuture(std::make_shared<storage::ObjectSizes>(key_type, 3, 30, 100));
    }

  private:
    std::function<bool(KeyType)> fails_;
};

version_store::LocalVersionedEngine engine_where_scan_fails(std::function<bool(KeyType)> fails) {
    auto engine = get_test_engine();
    engine._test_set_store(std::make_shared<ScanFailsStore>(std::move(fails)));
    return engine;
}

bool scanned(const std::vector<storage::ObjectSizes>& sizes, KeyType key_type) {
    return std::ranges::any_of(sizes, [key_type](const auto& s) { return s.key_type_ == key_type; });
}

// Only call for a key type that was scanned
const storage::ObjectSizes& find_sizes(const std::vector<storage::ObjectSizes>& sizes, KeyType key_type) {
    return *std::ranges::find(sizes, key_type, &storage::ObjectSizes::key_type_);
}

} // namespace

TEST(ScanObjectSizes, RaisesOnFailureByDefault) {
    auto engine = engine_where_scan_fails([](KeyType key_type) { return key_type == KeyType::TABLE_INDEX; });

    EXPECT_ANY_THROW(engine.scan_object_sizes());
}

TEST(ScanObjectSizes, OmitsFailedKeyTypesWhenSkipping) {
    auto engine = engine_where_scan_fails([](KeyType key_type) { return key_type == KeyType::TABLE_INDEX; });

    const auto sizes = engine.scan_object_sizes(version_store::OnScanFailure::Skip);

    // The readable key types are still reported, in full, and with the duration of their own scan
    EXPECT_FALSE(scanned(sizes, KeyType::TABLE_INDEX));
    ASSERT_TRUE(scanned(sizes, KeyType::TABLE_DATA));
    const auto& table_data = find_sizes(sizes, KeyType::TABLE_DATA);
    EXPECT_EQ(table_data.count_, 3);
    EXPECT_EQ(table_data.compressed_size_, 30);
    EXPECT_EQ(table_data.scan_duration_ns_, 100);
}

TEST(ScanObjectSizes, EveryKeyTypeFailingIsAnEmptyResultNotAnError) {
    auto engine = engine_where_scan_fails([](KeyType) { return true; });

    EXPECT_TRUE(engine.scan_object_sizes(version_store::OnScanFailure::Skip).empty());
}

} // namespace arcticdb
