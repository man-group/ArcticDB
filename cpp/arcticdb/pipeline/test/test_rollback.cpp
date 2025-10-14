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

namespace {

TestTensorFrame write_version_frame_with_three_segments(
        version_store::PythonVersionStore& store, const StreamId& stream_id
) {
    auto frame = get_test_timeseries_frame(stream_id, 30, 0); // 30 rows -> 3 segments
    store.write_versioned_dataframe_internal(stream_id, frame.frame_, false, false, false);
    return frame;
}

auto get_keys(version_store::PythonVersionStore& store) {
    auto mock_store = store._test_get_store();
    std::vector<RefKey> version_ref_keys;
    mock_store->iterate_type(KeyType::VERSION_REF, [&](VariantKey&& vk) {
        version_ref_keys.emplace_back(std::get<RefKey>(std::move(vk)));
    });

    std::vector<AtomKeyImpl> version_keys;
    mock_store->iterate_type(KeyType::VERSION, [&](VariantKey&& vk) {
        version_keys.emplace_back(std::get<AtomKeyImpl>(std::move(vk)));
    });

    std::vector<AtomKeyImpl> index_keys;
    mock_store->iterate_type(KeyType::TABLE_INDEX, [&](VariantKey&& vk) {
        index_keys.emplace_back(std::get<AtomKeyImpl>(std::move(vk)));
    });

    std::vector<AtomKeyImpl> data_keys;
    mock_store->iterate_type(KeyType::TABLE_DATA, [&](VariantKey&& vk) {
        data_keys.emplace_back(std::get<AtomKeyImpl>(std::move(vk)));
    });

    return std::make_tuple(version_ref_keys, version_keys, index_keys, data_keys);
}

TestTensorFrame update_with_three_segments(version_store::PythonVersionStore& store, const StreamId& stream_id) {
    constexpr RowRange update_range{5, 28};
    constexpr size_t update_val{1};
    auto update_frame = get_test_frame<TimeseriesIndex>(
            stream_id, get_test_timeseries_fields(), update_range.diff(), update_range.first, update_val
    );
    store.update_internal(stream_id, UpdateQuery{}, update_frame.frame_, false, false, false);
    return update_frame;
}

} // namespace

#include <optional>
#include <type_traits>

template<typename... Ts>
static StorageFailureSimulator::ParamActionSequence make_fault_sequence() {
    StorageFailureSimulator::ParamActionSequence seq;
    seq.reserve(sizeof...(Ts));

    auto append = []<typename T>(StorageFailureSimulator::ParamActionSequence& s) {
        if constexpr (std::is_same_v<T, std::nullopt_t>) {
            s.emplace_back(action_factories::no_op);
        } else {
            s.emplace_back(action_factories::fault<T>(1));
        }
    };

    (append.template operator()<Ts>(seq), ...);
    return seq;
}

struct TestScenario {
    std::string name;
    StorageFailureSimulator::ParamActionSequence failures;
    size_t expected_written_ref_keys{};
    size_t expected_written_version_keys{};
    size_t expected_written_index_keys{};
    bool check_data_keys{true};
    size_t expected_written_data_keys{};
    size_t num_writes{};
    std::vector<bool> write_should_throw_quota_exception;
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
        initial_frame_ =
                std::make_unique<TestTensorFrame>(write_version_frame_with_three_segments(*version_store_, stream_id_));

        const auto& scenario = GetParam();
        StorageFailureSimulator::instance()->configure({{FailureType::WRITE, scenario.failures}});

        auto [version_ref_keys, version_keys, index_keys, data_keys] = get_keys(*version_store_);

        ASSERT_EQ(version_ref_keys.size(), 1);
        ASSERT_EQ(version_keys.size(), 1);
        ASSERT_EQ(index_keys.size(), 1);
        ASSERT_EQ(data_keys.size(), 3);
    }
    std::unique_ptr<TestTensorFrame> initial_frame_;
};

using QUOTA = QuotaExceededException;
using OTHER = StorageException;
using NONE = std::nullopt_t;
const auto TEST_DATA_WRITE = ::testing::Values(
        TestScenario{
                .name = "Every_second_write_fails",
                .failures = make_fault_sequence<NONE, QUOTA, NONE, QUOTA, NONE, QUOTA>(),
                .expected_written_ref_keys = 0,
                .expected_written_version_keys = 0,
                .expected_written_index_keys = 0,
                .expected_written_data_keys = 0,
                .num_writes = 2,
                .write_should_throw_quota_exception = {true, true},
        },
        TestScenario{
                .name = "All_writes_fail",
                .failures = make_fault_sequence<QUOTA>(),
                .expected_written_ref_keys = 0,
                .expected_written_version_keys = 0,
                .expected_written_index_keys = 0,
                .expected_written_data_keys = 0,
                .num_writes = 1,
                .write_should_throw_quota_exception = {true, true},
        },
        TestScenario{
                .name = "Only_third_segment_write_fails",
                .failures = make_fault_sequence<NONE, NONE, QUOTA, NONE, NONE, NONE>(),
                .expected_written_ref_keys = 1,
                .expected_written_version_keys = 1,
                .expected_written_index_keys = 1,
                .expected_written_data_keys = 3,
                .num_writes = 2,
                .write_should_throw_quota_exception = {true, false},
        },
        TestScenario{
                .name = "All_succeed",
                .failures = make_fault_sequence<NONE>(),
                .expected_written_ref_keys = 1,
                .expected_written_version_keys = 2,
                .expected_written_index_keys = 2,
                .expected_written_data_keys = 6,
                .num_writes = 2,
                .write_should_throw_quota_exception = {false, false},
        }
);

const auto TEST_DATA_UPDATE = ::testing::Values(
        TestScenario{
                .name = "All_succeed",
                .failures = make_fault_sequence<NONE>(),
                .expected_written_ref_keys = 1,
                .expected_written_version_keys = 1,
                .expected_written_index_keys = 1,
                .expected_written_data_keys = 5, // Three for the update segments, two rewritten before and after
                .num_writes = 1,
                .write_should_throw_quota_exception = {false},
        },
        TestScenario{
                .name = "Update_succeeds_initial_write_then_fails_on_only_one_rewrite",
                .failures = make_fault_sequence<NONE, NONE, NONE, QUOTA, NONE>(),
                .expected_written_ref_keys = 0,
                .expected_written_version_keys = 0,
                .expected_written_index_keys = 0,
                .expected_written_data_keys = 0,
                .num_writes = 1,
                .write_should_throw_quota_exception = {true}
        },
        TestScenario{
                .name = "Update_succeeds_initial_write_then_fails_on_every_rewrite",
                .failures = make_fault_sequence<NONE, NONE, NONE, QUOTA, QUOTA>(),
                .expected_written_ref_keys = 0,
                .expected_written_version_keys = 0,
                .expected_written_index_keys = 0,
                .expected_written_data_keys = 0,
                .num_writes = 1,
                .write_should_throw_quota_exception = {true}
        },
        TestScenario{
                .name = "Update_succeeds_initial_write_then_fails_with_different_exceptions_on_rewrite",
                .failures = make_fault_sequence<NONE, NONE, NONE, OTHER, QUOTA>(),
                .expected_written_ref_keys = 0,
                .expected_written_version_keys = 0,
                .expected_written_index_keys = 0,
                .expected_written_data_keys = 0,
                .num_writes = 1,
                .write_should_throw_quota_exception = {true}
        },
        TestScenario{
                .name = "Update_fails_initial_write_then_no_rewrite",
                .failures = make_fault_sequence<NONE, QUOTA, NONE, NONE, NONE>(),
                .expected_written_ref_keys = 0,
                .expected_written_version_keys = 0,
                .expected_written_index_keys = 0,
                .expected_written_data_keys = 0,
                .num_writes = 1,
                .write_should_throw_quota_exception = {true}
        },
        TestScenario{
                .name = "Update_fails_with_another_exception_initially",
                .failures = make_fault_sequence<NONE, OTHER, NONE, NONE, NONE>(),
                .expected_written_ref_keys = 0,
                .expected_written_version_keys = 0,
                .expected_written_index_keys = 0,
                .check_data_keys = false, // Some writes may have succeeded and data is left orphaned.
                .expected_written_data_keys = 0,
                .num_writes = 1,
                .write_should_throw_quota_exception = {false}
        },
        TestScenario{
                .name = "Update_fails_with_another_exception_on_rewrite",
                .failures = make_fault_sequence<NONE, NONE, NONE, OTHER, NONE>(),
                .expected_written_ref_keys = 0,
                .expected_written_version_keys = 0,
                .expected_written_index_keys = 0,
                .expected_written_data_keys = 3,
                .num_writes = 1,
                .write_should_throw_quota_exception = {false}
        },
        TestScenario{
                .name = "Update_fails_with_different_exceptions_on_initial",
                .failures = make_fault_sequence<NONE, QUOTA, OTHER, NONE, NONE>(),
                .expected_written_ref_keys = 0,
                .expected_written_version_keys = 0,
                .expected_written_index_keys = 0,
                .expected_written_data_keys = 0, // Quota exception should prevail and delete the keys
                .num_writes = 1,
                .write_should_throw_quota_exception = {true}
        } // TODO: Add test that throws exception on rollback
);

INSTANTIATE_TEST_SUITE_P(
        RollbackWrite, RollbackOnQuotaExceeded, TEST_DATA_WRITE,
        [](const testing::TestParamInfo<TestScenario>& info) { return info.param.name; }
);

TEST_P(RollbackOnQuotaExceeded, BasicWrite) {
    const auto& scenario = GetParam();

    for (size_t i = 0; i < scenario.num_writes; ++i) {
        if (scenario.write_should_throw_quota_exception[i]) {
            EXPECT_THROW(write_version_frame_with_three_segments(*version_store_, stream_id_), QuotaExceededException);
        } else {
            EXPECT_NO_THROW(write_version_frame_with_three_segments(*version_store_, stream_id_));
        }
    }

    auto [version_ref_keys, version_keys, index_keys, data_keys] = get_keys(*version_store_);

    ASSERT_EQ(version_ref_keys.size(), scenario.expected_written_ref_keys);
    ASSERT_EQ(version_keys.size(), scenario.expected_written_version_keys);
    ASSERT_EQ(index_keys.size(), scenario.expected_written_index_keys);
    ASSERT_EQ(data_keys.size(), scenario.expected_written_data_keys);
}

TEST_P(RollbackOnQuotaExceededUpdate, BasicUpdate) {
    const auto& scenario = GetParam();

    auto initial_keys = get_keys(*version_store_);

    for (size_t i = 0; i < scenario.num_writes; ++i) {
        if (scenario.write_should_throw_quota_exception[i]) {
            EXPECT_THROW(update_with_three_segments(*version_store_, stream_id_), QuotaExceededException);
        } else {
            try {
                update_with_three_segments(*version_store_, stream_id_);
            } catch (...) {
            }
        }
    }

    auto [version_ref_keys, version_keys, index_keys, data_keys] = get_keys(*version_store_);
    if (scenario.expected_written_ref_keys == 0) {
        ASSERT_EQ(version_ref_keys, std::get<0>(initial_keys));
    }
    if (scenario.expected_written_version_keys == 0) {
        ASSERT_EQ(version_keys, std::get<1>(initial_keys));
    }
    if (scenario.expected_written_index_keys == 0) {
        ASSERT_EQ(index_keys, std::get<2>(initial_keys));
    }
    if (scenario.expected_written_data_keys == 0 && scenario.check_data_keys) {
        ASSERT_EQ(data_keys, std::get<3>(initial_keys));
    }

    // Excluding the initial write
    ASSERT_EQ(version_ref_keys.size(), 1);
    ASSERT_EQ(version_keys.size(), scenario.expected_written_version_keys + 1);
    ASSERT_EQ(index_keys.size(), scenario.expected_written_index_keys + 1);
    if (scenario.check_data_keys) {
        ASSERT_EQ(data_keys.size(), scenario.expected_written_data_keys + 3);
    }
}

INSTANTIATE_TEST_SUITE_P(
        RollbackUpdate, RollbackOnQuotaExceededUpdate, TEST_DATA_UPDATE,
        [](const testing::TestParamInfo<TestScenario>& info) { return info.param.name; }
);