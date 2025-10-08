/* Copyright 2025 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <gtest/gtest.h>
#include <pipeline/write_frame.hpp>

#include <util/test/generators.hpp>

using namespace arcticdb;

static StorageFailureSimulator::ParamActionSequence make_fault_sequence(
        std::initializer_list<double> fault_probabilities
) {
    StorageFailureSimulator::ParamActionSequence seq;
    seq.reserve(fault_probabilities.size());
    for (auto prob : fault_probabilities) {
        seq.push_back(action_factories::fault<QuotaExceededException>(prob));
    }
    return seq;
}

struct TestScenario {
    std::string name;
    StorageFailureSimulator::ParamActionSequence failures;
    size_t expected_ref_keys{};
    size_t expected_version_keys{};
    size_t expected_index_keys{};
    size_t expected_data_keys{};
    size_t num_writes{};
    std::vector<bool> write_should_throw;
};

class RollbackOnQuotaExceeded : public ::testing::TestWithParam<TestScenario> {
  protected:
    void SetUp() override {
        const auto& scenario = GetParam();
        StorageFailureSimulator::instance()->configure({{FailureType::WRITE, scenario.failures}});

        proto::storage::VersionStoreConfig version_store_cfg;
        version_store_cfg.mutable_write_options()->set_column_group_size(100);
        version_store_cfg.mutable_write_options()->set_segment_row_size(10);

        auto [version_store, _] = python_version_store_in_memory(version_store_cfg);
        version_store_ = std::make_unique<version_store::PythonVersionStore>(std::move(version_store));
    }

    void TearDown() override { StorageFailureSimulator::instance()->reset(); }

    void write_version_frame_with_three_segments() {
        using namespace arcticdb;
        using namespace arcticdb::storage;
        using namespace arcticdb::stream;
        using namespace arcticdb::pipelines;

        auto frame = get_test_timeseries_frame(stream_id_, 30, 0).frame_; // 30 rows -> 3 segments
        version_store_->write_versioned_dataframe_internal(stream_id_, std::move(frame), false, false, false);
    }

    auto get_keys() {
        auto& mock_store = *version_store_->_test_get_store();
        std::vector<RefKey> version_ref_keys;
        mock_store.iterate_type(KeyType::VERSION_REF, [&](auto&& vk) {
            version_ref_keys.emplace_back(std::get<RefKey>(std::move(vk)));
        });

        std::vector<AtomKeyImpl> version_keys;
        mock_store.iterate_type(KeyType::VERSION, [&](auto&& vk) {
            version_keys.emplace_back(std::get<AtomKeyImpl>(std::move(vk)));
        });

        std::vector<AtomKeyImpl> index_keys;
        mock_store.iterate_type(KeyType::TABLE_INDEX, [&](auto&& vk) {
            index_keys.emplace_back(std::get<AtomKeyImpl>(std::move(vk)));
        });

        std::vector<AtomKeyImpl> data_keys;
        mock_store.iterate_type(KeyType::TABLE_DATA, [&](auto&& vk) {
            data_keys.emplace_back(std::get<AtomKeyImpl>(std::move(vk)));
        });

        return std::make_tuple(version_ref_keys, version_keys, index_keys, data_keys);
    }

    std::unique_ptr<version_store::PythonVersionStore> version_store_;
    StreamId stream_id_{"sym"};
};

class RollbackOnQuotaExceededUpdate : public RollbackOnQuotaExceeded {
  protected:
    void SetUp() override {
        // Write some data (successfully) before the updates which may fail.
        proto::storage::VersionStoreConfig version_store_cfg;
        version_store_cfg.mutable_write_options()->set_column_group_size(100);
        version_store_cfg.mutable_write_options()->set_segment_row_size(10);
        auto [version_store, _] = python_version_store_in_memory(version_store_cfg);
        version_store_ = std::make_unique<version_store::PythonVersionStore>(std::move(version_store));
        write_version_frame_with_three_segments();

        const auto& scenario = GetParam();
        StorageFailureSimulator::instance()->configure({{FailureType::WRITE, scenario.failures}});

        auto [version_ref_keys, version_keys, index_keys, data_keys] = get_keys();

        ASSERT_EQ(version_ref_keys.size(), 1);
        ASSERT_EQ(version_keys.size(), 1);
        ASSERT_EQ(index_keys.size(), 1);
        ASSERT_EQ(data_keys.size(), 3);
    }

    void update_with_three_segments() {
        auto frame = get_test_timeseries_frame(stream_id_, 30, 0).frame_; // 30 rows -> 3 segments
        version_store_->update_internal(stream_id_, UpdateQuery{}, std::move(frame), false, false, false);
    }
};

const auto TEST_DATA = ::testing::Values(
        TestScenario{
                .name = "Every_second_write_fails",
                .failures = make_fault_sequence({0, 1}),
                .expected_ref_keys = 0,
                .expected_version_keys = 0,
                .expected_index_keys = 0,
                .expected_data_keys = 0,
                .num_writes = 2,
                .write_should_throw = {true, true},
        },
        TestScenario{
                .name = "All_writes_fail",
                .failures = make_fault_sequence({1}),
                .expected_ref_keys = 0,
                .expected_version_keys = 0,
                .expected_index_keys = 0,
                .expected_data_keys = 0,
                .num_writes = 2,
                .write_should_throw = {true, true},
        },
        TestScenario{
                .name = "Only_third_segment_write_fails",
                .failures = make_fault_sequence({0, 0, 1, 0, 0, 0}),
                .expected_ref_keys = 1,
                .expected_version_keys = 1,
                .expected_index_keys = 1,
                .expected_data_keys = 3,
                .num_writes = 2,
                .write_should_throw = {true, false},
        },
        TestScenario{
                .name = "All_succeed",
                .failures = make_fault_sequence({0}),
                .expected_ref_keys = 1,
                .expected_version_keys = 2,
                .expected_index_keys = 2,
                .expected_data_keys = 6,
                .num_writes = 2,
                .write_should_throw = {false, false},
        }
);

const auto TEST_DATA_UPDATE_ONLY = ::testing::Values(
        TestScenario{
                .name = "Update_succeeds_initial_write_then_fails_on_rewrite",
                .failures = make_fault_sequence({0, 0, 0, 1}),
                .expected_ref_keys = 0,
                .expected_version_keys = 0,
                .expected_index_keys = 0,
                .expected_data_keys = 0,
                .num_writes = 1,
                .write_should_throw = {true}
        }
);

INSTANTIATE_TEST_SUITE_P(, RollbackOnQuotaExceeded, TEST_DATA, [](const testing::TestParamInfo<TestScenario>& info) {
    return info.param.name;
});

TEST_P(RollbackOnQuotaExceeded, BasicWrite) {
    const auto& scenario = GetParam();

    for (size_t i = 0; i < scenario.num_writes; ++i) {
        if (scenario.write_should_throw[i]) {
            EXPECT_THROW(write_version_frame_with_three_segments(), QuotaExceededException);
        } else {
            EXPECT_NO_THROW(write_version_frame_with_three_segments());
        }
    }

    auto [version_ref_keys, version_keys, index_keys, data_keys] = get_keys();

    ASSERT_EQ(version_ref_keys.size(), scenario.expected_ref_keys);
    ASSERT_EQ(version_keys.size(), scenario.expected_version_keys);
    ASSERT_EQ(index_keys.size(), scenario.expected_index_keys);
    ASSERT_EQ(data_keys.size(), scenario.expected_data_keys);
}

TEST_P(RollbackOnQuotaExceededUpdate, BasicUpdate) {
    const auto& scenario = GetParam();

    auto initial_keys = get_keys();

    for (size_t i = 0; i < scenario.num_writes; ++i) {
        if (scenario.write_should_throw[i]) {
            EXPECT_THROW(update_with_three_segments(), QuotaExceededException);
        } else {
            EXPECT_NO_THROW(update_with_three_segments());
        }
    }

    auto keys = get_keys();
    auto [version_ref_keys, version_keys, index_keys, data_keys] = keys;
    if (scenario.expected_ref_keys == 0) {
        // Nothing should have changed
        ASSERT_EQ(keys, initial_keys);
    }

    // Excluding the initial write
    ASSERT_EQ(version_ref_keys.size(), 1);
    ASSERT_EQ(version_keys.size(), scenario.expected_version_keys + 1);
    ASSERT_EQ(index_keys.size(), scenario.expected_index_keys + 1);
    ASSERT_EQ(data_keys.size(), scenario.expected_data_keys + 3);
}

INSTANTIATE_TEST_SUITE_P(
        RollbackUpdateCommon, RollbackOnQuotaExceededUpdate, TEST_DATA,
        [](const testing::TestParamInfo<TestScenario>& info) { return info.param.name; }
);

INSTANTIATE_TEST_SUITE_P(
        RollbackUpdate, RollbackOnQuotaExceededUpdate, TEST_DATA_UPDATE_ONLY,
        [](const testing::TestParamInfo<TestScenario>& info) { return info.param.name; }
);
