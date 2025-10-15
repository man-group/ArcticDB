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
    std::unordered_set<RefKey> version_ref_keys;
    mock_store->iterate_type(KeyType::VERSION_REF, [&](VariantKey&& vk) {
        version_ref_keys.emplace(std::get<RefKey>(std::move(vk)));
    });

    std::unordered_set<AtomKeyImpl> version_keys;
    mock_store->iterate_type(KeyType::VERSION, [&](VariantKey&& vk) {
        version_keys.emplace(std::get<AtomKeyImpl>(std::move(vk)));
    });

    std::unordered_set<AtomKeyImpl> index_keys;
    mock_store->iterate_type(KeyType::TABLE_INDEX, [&](VariantKey&& vk) {
        index_keys.emplace(std::get<AtomKeyImpl>(std::move(vk)));
    });

    std::unordered_set<AtomKeyImpl> data_keys;
    mock_store->iterate_type(KeyType::TABLE_DATA, [&](VariantKey&& vk) {
        data_keys.emplace(std::get<AtomKeyImpl>(std::move(vk)));
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

enum class Outcome { QUOTA, OTHER, NONE, UNKNOWN_EXCEPTION };

static StorageFailureSimulator::ParamActionSequence make_fault_sequence(const std::vector<Outcome>& types) {
    StorageFailureSimulator::ParamActionSequence seq;
    seq.reserve(types.size());

    for (auto type : types) {
        switch (type) {
        case Outcome::NONE:
            seq.emplace_back(action_factories::no_op);
            break;
        case Outcome::QUOTA:
            seq.emplace_back(action_factories::fault<QuotaExceededException>(1));
            break;
        case Outcome::OTHER:
            seq.emplace_back(action_factories::fault<StorageException>(1));
            break;
        }
    }
    return seq;
}

struct TestScenario {
    std::string name;
    StorageFailureSimulator::ParamActionSequence write_failures;
    StorageFailureSimulator::ParamActionSequence delete_failures;
    size_t expected_written_ref_keys{};
    size_t expected_written_version_keys{};
    size_t expected_written_index_keys{};
    bool check_data_keys{true};
    size_t expected_written_data_keys{};
    size_t num_writes{};
    std::vector<Outcome> write_expected_outcome;
};

class RollbackOnQuotaExceeded : public ::testing::TestWithParam<TestScenario> {
  protected:
    void SetUp() override {
        const auto& scenario = GetParam();
        StorageFailureSimulator::instance()->configure({{FailureType::WRITE, scenario.write_failures}});
        StorageFailureSimulator::instance()->configure({{FailureType::DELETE, scenario.delete_failures}});

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
        StorageFailureSimulator::instance()->configure({{FailureType::WRITE, scenario.write_failures}});

        auto [version_ref_keys, version_keys, index_keys, data_keys] = get_keys(*version_store_);

        ASSERT_EQ(version_ref_keys.size(), 1);
        ASSERT_EQ(version_keys.size(), 1);
        ASSERT_EQ(index_keys.size(), 1);
        ASSERT_EQ(data_keys.size(), 3);
    }
    std::unique_ptr<TestTensorFrame> initial_frame_;
};

constexpr auto NONE = Outcome::NONE;
constexpr auto QUOTA = Outcome::QUOTA;
constexpr auto OTHER = Outcome::OTHER;
constexpr auto UNKNOWN_EXCEPTION = Outcome::UNKNOWN_EXCEPTION;
const auto TEST_DATA_WRITE = ::testing::Values(
        TestScenario{
                .name = "Every_second_write_fails",
                .write_failures = make_fault_sequence({NONE, QUOTA, NONE, QUOTA, NONE, QUOTA}),
                .expected_written_ref_keys = 0,
                .expected_written_version_keys = 0,
                .expected_written_index_keys = 0,
                .expected_written_data_keys = 0,
                .num_writes = 2,
                .write_expected_outcome = {QUOTA, QUOTA},
        },
        TestScenario{
                .name = "All_writes_fail",
                .write_failures = make_fault_sequence({QUOTA}),
                .expected_written_ref_keys = 0,
                .expected_written_version_keys = 0,
                .expected_written_index_keys = 0,
                .expected_written_data_keys = 0,
                .num_writes = 1,
                .write_expected_outcome = {QUOTA, QUOTA},
        },
        TestScenario{
                .name = "Only_third_segment_write_fails",
                .write_failures = make_fault_sequence({NONE, NONE, QUOTA, NONE, NONE, NONE}),
                .expected_written_ref_keys = 1,
                .expected_written_version_keys = 1,
                .expected_written_index_keys = 1,
                .expected_written_data_keys = 3,
                .num_writes = 2,
                .write_expected_outcome = {QUOTA, NONE},
        },
        TestScenario{
                .name = "All_succeed",
                .write_failures = make_fault_sequence({NONE}),
                .expected_written_ref_keys = 1,
                .expected_written_version_keys = 2,
                .expected_written_index_keys = 2,
                .expected_written_data_keys = 6,
                .num_writes = 2,
                .write_expected_outcome = {NONE, NONE},
        },
        TestScenario{
                .name = "Write_triggers_rollback_but_then_delete_fails",
                .write_failures = make_fault_sequence({NONE, QUOTA, NONE, NONE, NONE, NONE}),
                .delete_failures = make_fault_sequence({OTHER}),
                .expected_written_ref_keys = 1,
                .expected_written_version_keys = 1,
                .expected_written_index_keys = 1,
                .check_data_keys = false,
                .expected_written_data_keys = 0,
                .num_writes = 2,
                .write_expected_outcome = {OTHER, NONE}
        }
);

const auto TEST_DATA_UPDATE = ::testing::Values(
        TestScenario{
                .name = "All_succeed",
                .write_failures = make_fault_sequence({NONE}),
                .expected_written_ref_keys = 1,
                .expected_written_version_keys = 1,
                .expected_written_index_keys = 1,
                .expected_written_data_keys = 5, // Three for the update segments, two rewritten before and after
                .num_writes = 1,
                .write_expected_outcome = {NONE},
        },
        TestScenario{
                .name = "Update_succeeds_initial_write_then_fails_on_only_one_rewrite",
                .write_failures = make_fault_sequence({NONE, NONE, NONE, QUOTA, NONE}),
                .expected_written_ref_keys = 0,
                .expected_written_version_keys = 0,
                .expected_written_index_keys = 0,
                .expected_written_data_keys = 0,
                .num_writes = 1,
                .write_expected_outcome = {QUOTA}
        },
        TestScenario{
                // TODO: Flakes
                .name = "Update_succeeds_initial_write_then_fails_on_only_one_rewrite_other",
                .write_failures = make_fault_sequence({NONE, NONE, NONE, NONE, QUOTA}),
                .expected_written_ref_keys = 0,
                .expected_written_version_keys = 0,
                .expected_written_index_keys = 0,
                .expected_written_data_keys = 0,
                .num_writes = 1,
                .write_expected_outcome = {QUOTA}
        },
        TestScenario{
                .name = "Update_succeeds_initial_write_then_fails_on_every_rewrite",
                .write_failures = make_fault_sequence({NONE, NONE, NONE, QUOTA, QUOTA}),
                .expected_written_ref_keys = 0,
                .expected_written_version_keys = 0,
                .expected_written_index_keys = 0,
                .expected_written_data_keys = 0,
                .num_writes = 1,
                .write_expected_outcome = {QUOTA},
        },
        TestScenario{
                .name = "Update_succeeds_initial_write_then_fails_with_different_exceptions_on_rewrite",
                .write_failures = make_fault_sequence({NONE, NONE, NONE, OTHER, QUOTA}),
                .expected_written_ref_keys = 0,
                .expected_written_version_keys = 0,
                .expected_written_index_keys = 0,
                .check_data_keys = false,
                .expected_written_data_keys = 0,
                .num_writes = 1,
                // If either of the rewrites (before or after) throws non-quota exception while the other throws quota,
                // it is undefined which exception is propagated
                .write_expected_outcome = {UNKNOWN_EXCEPTION}
        },
        TestScenario{
                .name = "Update_fails_initial_write_then_no_rewrite",
                .write_failures = make_fault_sequence({NONE, QUOTA, NONE, NONE, NONE}),
                .expected_written_ref_keys = 0,
                .expected_written_version_keys = 0,
                .expected_written_index_keys = 0,
                .expected_written_data_keys = 0,
                .num_writes = 1,
                .write_expected_outcome = {QUOTA}
        },
        TestScenario{
                .name = "Update_fails_with_another_exception_initially",
                .write_failures = make_fault_sequence({NONE, OTHER, NONE, NONE, NONE}),
                .expected_written_ref_keys = 0,
                .expected_written_version_keys = 0,
                .expected_written_index_keys = 0,
                .check_data_keys = false, // Some writes may have succeeded and data is left orphaned.
                .expected_written_data_keys = 0,
                .num_writes = 1,
                .write_expected_outcome = {OTHER}
        },
        TestScenario{
                .name = "Update_fails_with_another_exception_on_rewrite",
                .write_failures = make_fault_sequence({NONE, NONE, NONE, OTHER, NONE}),
                .expected_written_ref_keys = 0,
                .expected_written_version_keys = 0,
                .expected_written_index_keys = 0,
                .check_data_keys = false,
                .expected_written_data_keys = 3,
                .num_writes = 1,
                .write_expected_outcome = {OTHER}
        },
        TestScenario{
                .name = "Update_fails_with_different_exceptions_on_initial",
                .write_failures = make_fault_sequence({NONE, QUOTA, OTHER, NONE, NONE}),
                .expected_written_ref_keys = 0,
                .expected_written_version_keys = 0,
                .expected_written_index_keys = 0,
                .expected_written_data_keys = 0, // Quota exception should prevail and delete the keys
                .num_writes = 1,
                .write_expected_outcome = {QUOTA}
        },
        TestScenario{
                .name = "Update_triggers_rollback_but_then_delete_fails",
                .write_failures = make_fault_sequence({NONE, QUOTA, NONE, NONE, NONE}),
                .delete_failures = make_fault_sequence({OTHER}),
                .expected_written_ref_keys = 0,
                .expected_written_version_keys = 0,
                .expected_written_index_keys = 0,
                .check_data_keys = false,
                .expected_written_data_keys = 0,
                .num_writes = 1,
                .write_expected_outcome = {OTHER}
        }
);

INSTANTIATE_TEST_SUITE_P(
        , RollbackOnQuotaExceeded, TEST_DATA_WRITE,
        [](const testing::TestParamInfo<TestScenario>& info) { return info.param.name; }
);

TEST_P(RollbackOnQuotaExceeded, BasicWrite) {
    const auto& scenario = GetParam();

    for (size_t i = 0; i < scenario.num_writes; ++i) {
        switch (scenario.write_expected_outcome[i]) {
        case Outcome::NONE:
            EXPECT_NO_THROW(write_version_frame_with_three_segments(*version_store_, stream_id_));
            break;
        case Outcome::OTHER:
            EXPECT_THROW(write_version_frame_with_three_segments(*version_store_, stream_id_), StorageException);
            break;
        case Outcome::QUOTA:
            EXPECT_THROW(write_version_frame_with_three_segments(*version_store_, stream_id_), QuotaExceededException);
        case Outcome::UNKNOWN_EXCEPTION:
            EXPECT_ANY_THROW(update_with_three_segments(*version_store_, stream_id_));
            break;
        }
    }

    auto [version_ref_keys, version_keys, index_keys, data_keys] = get_keys(*version_store_);

    ASSERT_EQ(version_ref_keys.size(), scenario.expected_written_ref_keys);
    ASSERT_EQ(version_keys.size(), scenario.expected_written_version_keys);
    ASSERT_EQ(index_keys.size(), scenario.expected_written_index_keys);
    if (scenario.check_data_keys) {
        ASSERT_EQ(data_keys.size(), scenario.expected_written_data_keys);
    }
}

TEST_P(RollbackOnQuotaExceededUpdate, BasicUpdate) {
    const auto& scenario = GetParam();

    auto initial_keys = get_keys(*version_store_);

    for (size_t i = 0; i < scenario.num_writes; ++i) {
        switch (scenario.write_expected_outcome[i]) {
        case Outcome::NONE:
            EXPECT_NO_THROW(update_with_three_segments(*version_store_, stream_id_));
            break;
        case Outcome::OTHER:
            EXPECT_THROW(update_with_three_segments(*version_store_, stream_id_), StorageException);
            break;
        case Outcome::QUOTA:
            EXPECT_THROW(update_with_three_segments(*version_store_, stream_id_), QuotaExceededException);
            break;
        case Outcome::UNKNOWN_EXCEPTION:
            EXPECT_ANY_THROW(update_with_three_segments(*version_store_, stream_id_));
            break;
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
        , RollbackOnQuotaExceededUpdate, TEST_DATA_UPDATE,
        [](const testing::TestParamInfo<TestScenario>& info) { return info.param.name; }
);