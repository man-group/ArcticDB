/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <google/protobuf/util/message_differencer.h>

#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <arcticdb/processing/schema_combine.hpp>

using namespace arcticdb;
using namespace arcticdb::entity;
using namespace google::protobuf::util;
using ::testing::ElementsAre;
using ::testing::ElementsAreArray;
using ::testing::IsEmpty;
using NormalizationMetadata = arcticdb::proto::descriptors::NormalizationMetadata;

namespace {

// A descriptor field as the tests describe one: its name and its type.
using ColumnSpec = std::pair<std::string, DataType>;

OutputSchema written_empty(OutputSchema schema) {
    schema.set_inferred_from_empty_frame(true);
    return schema;
}

OutputSchema timeseries_df(
        const std::string& index_name, const std::vector<ColumnSpec>& columns, const std::string& tz = ""
) {
    StreamDescriptor desc{StreamId{}, IndexDescriptorImpl{IndexDescriptor::Type::TIMESTAMP, 1}};
    desc.add_scalar_field(DataType::NANOSECONDS_UTC64, index_name);
    for (const auto& [name, type] : columns) {
        desc.add_scalar_field(type, name);
    }
    NormalizationMetadata norm;
    auto* index = norm.mutable_df()->mutable_common()->mutable_index();
    index->set_is_physically_stored(true);
    index->set_name(index_name);
    index->set_tz(tz);
    return {std::move(desc), std::move(norm)};
}

// Levels are named and typed the same way as columns. The norm metadata's field count is one less than the number
// of levels, as _normalization.py records it.
OutputSchema multiindex_df(
        const std::vector<ColumnSpec>& levels, const std::vector<ColumnSpec>& columns,
        const std::vector<uint32_t>& unnamed_levels = {}
) {
    const auto num_levels = static_cast<uint32_t>(levels.size());
    StreamDescriptor desc{StreamId{}, IndexDescriptorImpl{IndexDescriptor::Type::TIMESTAMP, num_levels}};
    for (const auto& [name, type] : levels) {
        desc.add_scalar_field(type, name);
    }
    for (const auto& [name, type] : columns) {
        desc.add_scalar_field(type, name);
    }
    NormalizationMetadata norm;
    auto* multi_index = norm.mutable_df()->mutable_common()->mutable_multi_index();
    multi_index->set_field_count(num_levels - 1);
    multi_index->set_name(levels.front().first);
    for (const auto position : unnamed_levels) {
        multi_index->add_fake_field_pos(position);
    }
    return {std::move(desc), std::move(norm)};
}

OutputSchema timeseries_series(const std::string& index_name, const std::string& series_name, DataType value_type) {
    StreamDescriptor desc{StreamId{}, IndexDescriptorImpl{IndexDescriptor::Type::TIMESTAMP, 1}};
    desc.add_scalar_field(DataType::NANOSECONDS_UTC64, index_name);
    desc.add_scalar_field(value_type, series_name);
    NormalizationMetadata norm;
    auto* common = norm.mutable_series()->mutable_common();
    common->mutable_index()->set_is_physically_stored(true);
    common->mutable_index()->set_name(index_name);
    common->set_name(series_name);
    common->set_has_name(true);
    return {std::move(desc), std::move(norm)};
}

OutputSchema rowcount_series(const std::string& index_name, const std::string& series_name, DataType value_type) {
    StreamDescriptor desc{StreamId{}, IndexDescriptorImpl{IndexDescriptor::Type::ROWCOUNT, 0}};
    desc.add_scalar_field(value_type, series_name);
    NormalizationMetadata norm;
    auto* common = norm.mutable_series()->mutable_common();
    common->mutable_index()->set_is_physically_stored(false);
    common->mutable_index()->set_name(index_name);
    // A real RangeIndex always has a non-zero step; a step of zero is how an empty index is recognised.
    common->mutable_index()->set_step(1);
    common->set_name(series_name);
    common->set_has_name(true);
    return {std::move(desc), std::move(norm)};
}

// Positions of the multiindex levels the output treats as unnamed.
std::vector<uint32_t> fake_field_pos_of(const OutputSchema& schema) {
    const auto& common = schema.norm_metadata_.has_series() ? schema.norm_metadata_.series().common()
                                                            : schema.norm_metadata_.df().common();
    std::vector<uint32_t> positions{
            common.multi_index().fake_field_pos().begin(), common.multi_index().fake_field_pos().end()
    };
    EXPECT_TRUE(std::ranges::is_sorted(positions));
    return positions;
}

OutputSchema empty_index_df(const std::vector<ColumnSpec>& columns) {
    StreamDescriptor desc{StreamId{}, IndexDescriptorImpl{IndexDescriptor::Type::EMPTY, 0}};
    for (const auto& [name, type] : columns) {
        desc.add_scalar_field(type, name);
    }
    NormalizationMetadata norm;
    norm.mutable_df()->mutable_common()->mutable_index()->set_is_physically_stored(false);
    // An empty index is only ever written by an empty frame.
    return written_empty({std::move(desc), std::move(norm)});
}

OutputSchema rowcount_df(const std::vector<ColumnSpec>& columns, const std::string& index_name = "") {
    StreamDescriptor desc{StreamId{}, IndexDescriptorImpl{IndexDescriptor::Type::ROWCOUNT, 0}};
    for (const auto& [name, type] : columns) {
        desc.add_scalar_field(type, name);
    }
    NormalizationMetadata norm;
    auto* index = norm.mutable_df()->mutable_common()->mutable_index();
    index->set_is_physically_stored(false);
    index->set_name(index_name);
    // A real RangeIndex always has a non-zero step; a step of zero is how an empty index is recognised.
    index->set_step(1);
    return {std::move(desc), std::move(norm)};
}

// The shape pandas 2 gives a zero-row frame: the descriptor is rewritten to a DatetimeIndex whatever index the user
// had, while the normalization metadata still says the index is not physically stored. Neither claim is worth anything,
// which is why such a schema needs handling of its own.
OutputSchema written_empty_df(const std::vector<ColumnSpec>& columns, const std::string& index_name = "index") {
    auto schema = timeseries_df(index_name, columns);
    auto* index = schema.norm_metadata_.mutable_df()->mutable_common()->mutable_index();
    index->set_is_physically_stored(false);
    index->set_fake_name(index_name == "index");
    return written_empty(std::move(schema));
}

// Helper to take an `std::vector` so we can pass in an initializer_list like `combine({a, b}, options)`
OutputSchema combine(std::vector<OutputSchema> schemas, const SchemaCombineOptions& options) {
    return combine_schema(schemas, options);
}

std::vector<ColumnSpec> columns_of(const OutputSchema& schema) {
    std::vector<ColumnSpec> out;
    for (const auto& field : schema.stream_descriptor().fields()) {
        out.emplace_back(std::string(field.name()), field.type().data_type());
    }
    return out;
}

} // namespace

TEST(CombineSchema, ConcatOuterUnionOfColumns) {
    auto base = timeseries_df("ts", {{"a", DataType::FLOAT64}, {"b", DataType::FLOAT64}});
    auto other = timeseries_df("ts", {{"b", DataType::FLOAT64}, {"c", DataType::FLOAT64}});
    auto combined = combine({base, other}, concat_options(JoinType::OUTER));
    const std::array expected{
            ColumnSpec{"ts", DataType::NANOSECONDS_UTC64},
            ColumnSpec{"a", DataType::FLOAT64},
            ColumnSpec{"b", DataType::FLOAT64},
            ColumnSpec{"c", DataType::FLOAT64}
    };
    ASSERT_THAT(columns_of(combined), ElementsAreArray(expected));
}

TEST(CombineSchema, ConcatInnerIntersectionOfColumns) {
    auto base = timeseries_df("ts", {{"a", DataType::FLOAT64}, {"b", DataType::FLOAT64}});
    auto other = timeseries_df("ts", {{"b", DataType::FLOAT64}, {"c", DataType::FLOAT64}});
    auto combined = combine({base, other}, concat_options(JoinType::INNER));
    const std::array expected{ColumnSpec{"ts", DataType::NANOSECONDS_UTC64}, ColumnSpec{"b", DataType::FLOAT64}};
    ASSERT_THAT(columns_of(combined), ElementsAreArray(expected));
}

TEST(CombineSchema, ConcatTypePromotion) {
    auto base = timeseries_df("ts", {{"a", DataType::INT32}});
    auto other = timeseries_df("ts", {{"a", DataType::INT64}});
    for (auto join_type : {JoinType::OUTER, JoinType::INNER}) {
        auto combined = combine({base, other}, concat_options(join_type));
        ASSERT_EQ(combined.stream_descriptor().field(1).type().data_type(), DataType::INT64);
    }
}

TEST(CombineSchema, ConcatMismatchedIndexNameReconciledToFake) {
    auto base = timeseries_df("ts1", {{"a", DataType::FLOAT64}});
    auto other = timeseries_df("ts2", {{"a", DataType::FLOAT64}});
    auto combined = combine({base, other}, concat_options(JoinType::OUTER));
    ASSERT_EQ(combined.stream_descriptor().field(0).name(), "index");
}

TEST(CombineSchema, ConcatRenamedMultiIndexLevelsReconciledToFake) {
    const std::vector<ColumnSpec> columns{{"a", DataType::FLOAT64}};
    const ColumnSpec lvl{"lvl", DataType::INT32};
    const ColumnSpec level2{"level2", DataType::INT32};
    const ColumnSpec dt{"dt", DataType::NANOSECONDS_UTC64};
    const ColumnSpec ts{"ts", DataType::NANOSECONDS_UTC64};
    const auto both_named = multiindex_df({dt, lvl}, columns);
    const auto first_renamed = multiindex_df({ts, lvl}, columns);
    const auto second_renamed = multiindex_df({dt, level2}, columns);
    const auto both_renamed = multiindex_df({ts, level2}, columns);

    const auto combine_both_ways = [](const OutputSchema& lhs, const OutputSchema& rhs, auto&& assertions) {
        assertions(combine({lhs, rhs}, concat_options(JoinType::OUTER)));
        assertions(combine({rhs, lhs}, concat_options(JoinType::OUTER)));
    };

    // Only level 0 differs: it takes the name "index", level 1 keeps its own.
    combine_both_ways(both_named, first_renamed, [](const OutputSchema& combined) {
        ASSERT_EQ(combined.stream_descriptor().field(0).name(), "index");
        ASSERT_EQ(combined.stream_descriptor().field(1).name(), "lvl");
        ASSERT_THAT(fake_field_pos_of(combined), ElementsAre(0));
        ASSERT_EQ(combined.norm_metadata_.df().common().multi_index().name(), "index");
    });

    // Only level 1 differs: level 0 keeps its name and level 1 takes the __fkidx__ scheme.
    combine_both_ways(both_named, second_renamed, [](const OutputSchema& combined) {
        ASSERT_EQ(combined.stream_descriptor().field(0).name(), "dt");
        ASSERT_EQ(combined.stream_descriptor().field(1).name(), "__fkidx__1");
        ASSERT_THAT(fake_field_pos_of(combined), ElementsAre(1));
    });

    combine_both_ways(both_named, both_renamed, [](const OutputSchema& combined) {
        ASSERT_EQ(combined.stream_descriptor().field(0).name(), "index");
        ASSERT_EQ(combined.stream_descriptor().field(1).name(), "__fkidx__1");
        ASSERT_THAT(fake_field_pos_of(combined), ElementsAre(0, 1));
    });

    combine_both_ways(both_named, both_named, [](const OutputSchema& combined) {
        ASSERT_EQ(combined.stream_descriptor().field(0).name(), "dt");
        ASSERT_EQ(combined.stream_descriptor().field(1).name(), "lvl");
        ASSERT_THAT(fake_field_pos_of(combined), IsEmpty());
    });
}

// Disagreeing Series names leave the result unnamed, named the way a write of an unnamed Series names it rather than
// with the __fkidx__ scheme the index levels use.
TEST(CombineSchema, ConcatRenamedSeriesValueColumnDropsTheName) {
    const auto series_a = timeseries_series("ts", "a", DataType::FLOAT64);
    const auto series_b = timeseries_series("ts", "b", DataType::FLOAT64);

    for (auto schemas :
         {std::vector<OutputSchema>{series_a, series_b}, std::vector<OutputSchema>{series_b, series_a}}) {
        auto combined = combine(schemas, concat_options(JoinType::OUTER));
        ASSERT_EQ(combined.stream_descriptor().field(0).name(), "ts");
        ASSERT_EQ(combined.stream_descriptor().field(1).name(), "0");
        ASSERT_FALSE(combined.norm_metadata_.series().common().has_name());
    }

    auto combined = combine({series_a, series_a}, concat_options(JoinType::OUTER));
    ASSERT_EQ(combined.stream_descriptor().field(1).name(), "a");
    ASSERT_TRUE(combined.norm_metadata_.series().common().has_name());
    ASSERT_EQ(combined.norm_metadata_.series().common().name(), "a");
}

// The read side decides a level has no name from fake_field_pos, so a level every schema agrees is unnamed -
// and which therefore records no mismatch - must still come out in it.
TEST(CombineSchema, AlreadyUnnamedMultiIndexLevelsStayUnnamed) {
    const std::vector<ColumnSpec> columns{{"a", DataType::FLOAT64}};
    const ColumnSpec fake_1{"__fkidx__1", DataType::INT32};
    const auto unnamed_level_1 = multiindex_df({{"dt", DataType::NANOSECONDS_UTC64}, fake_1}, columns, {1});

    auto combined = combine({unnamed_level_1, unnamed_level_1}, concat_options(JoinType::OUTER));
    ASSERT_THAT(fake_field_pos_of(combined), ElementsAre(1));
    ASSERT_EQ(combined.stream_descriptor().field(1).name(), "__fkidx__1");

    // Level 0 disagrees, so there is a mismatch, and level 1 must not be lost while it is applied.
    const auto renamed_level_0 = multiindex_df({{"ts", DataType::NANOSECONDS_UTC64}, fake_1}, columns, {1});
    combined = combine({unnamed_level_1, renamed_level_0}, concat_options(JoinType::OUTER));
    ASSERT_THAT(fake_field_pos_of(combined), ElementsAre(0, 1));

    // And appending them is fine under either schema, since nothing about the names disagrees.
    for (const auto& options : {append_options(true), append_options(false)}) {
        combined = combine({unnamed_level_1, unnamed_level_1}, options);
        ASSERT_THAT(fake_field_pos_of(combined), ElementsAre(1)) << "for " << options.name();
    }
}

TEST(CombineSchema, AgreedUnnamedLevelSurvivesAlongsideADisagreeingOne) {
    const ColumnSpec dt{"dt", DataType::NANOSECONDS_UTC64};
    const ColumnSpec fake_1{"__fkidx__1", DataType::INT32};
    const auto both = multiindex_df({dt, fake_1, {"__fkidx__2", DataType::INT32}}, {{"a", DataType::FLOAT64}}, {1, 2});
    const auto only_level_1 = multiindex_df({dt, fake_1, {"lvl2", DataType::INT32}}, {{"a", DataType::FLOAT64}}, {1});

    auto combined = combine({both, only_level_1}, concat_options(JoinType::OUTER));
    ASSERT_THAT(fake_field_pos_of(combined), ElementsAre(1, 2));

    // Independent of the ordering of the inputs.
    combined = combine({only_level_1, both}, concat_options(JoinType::OUTER));
    ASSERT_THAT(fake_field_pos_of(combined), ElementsAre(1, 2));
}

// A RangeIndexed Series has no index field, so its value column is required field 0 - the position a scalar
// index would occupy. The two must not be confused.
TEST(CombineSchema, RowCountSeriesNameMismatchLeavesTheIndexAlone) {
    const auto series_a = rowcount_series("idx", "a", DataType::FLOAT64);
    const auto series_b = rowcount_series("idx", "b", DataType::FLOAT64);

    auto combined = combine({series_a, series_b}, concat_options(JoinType::OUTER));
    const auto& common = combined.norm_metadata_.series().common();
    ASSERT_FALSE(common.has_name());
    ASSERT_EQ(common.index().name(), "idx");
    ASSERT_FALSE(common.index().fake_name());
}

TEST(CombineSchema, IncompatibleRequiredFieldShapesRaise) {
    const std::vector<ColumnSpec> columns{{"a", DataType::FLOAT64}};
    const ColumnSpec dt{"dt", DataType::NANOSECONDS_UTC64};
    const ColumnSpec lvl{"lvl", DataType::INT32};
    const auto two_levels = multiindex_df({dt, lvl}, columns);
    const auto three_levels = multiindex_df({dt, lvl, {"lvl2", DataType::INT32}}, columns);
    const auto scalar_index = timeseries_df("dt", columns);
    const auto series = timeseries_series("dt", "v", DataType::FLOAT64);

    // Every shape disagrees with every other, in either order.
    const std::vector<std::pair<std::string, OutputSchema>> shapes{
            {"two_levels", two_levels},
            {"three_levels", three_levels},
            {"scalar_index", scalar_index},
            {"series", series}
    };
    for (const auto& options : {concat_options(JoinType::OUTER), append_options(true)}) {
        for (const auto& [base_name, base] : shapes) {
            for (const auto& [other_name, other] : shapes) {
                if (&base == &other) {
                    continue;
                }
                ASSERT_THROW(combine({base, other}, options), NormalizationException)
                        << base_name << " combined with " << other_name << " for " << options.name();
            }
        }
    }
}

// Arrow metadata records neither the Series/DataFrame distinction nor multi-index levels, so it has no shape
// to disagree about.
TEST(CombineSchema, ArrowSchemaHasNoShapeToDisagreeAbout) {
    const auto series = timeseries_series("ts", "col", DataType::INT64);
    auto arrow = timeseries_df("ts", {{"col", DataType::INT64}});
    arrow.norm_metadata_.mutable_experimental_arrow()->set_has_index(true);

    const std::array expected{ColumnSpec{"ts", DataType::NANOSECONDS_UTC64}, ColumnSpec{"col", DataType::INT64}};
    for (auto schemas : {std::vector<OutputSchema>{series, arrow}, std::vector<OutputSchema>{arrow, series}}) {
        auto combined = combine(schemas, concat_options(JoinType::OUTER));
        ASSERT_THAT(columns_of(combined), ElementsAreArray(expected));
    }
}

TEST(CombineSchema, ErrorMessagesNameTheOperation) {
    const auto base = timeseries_df("ts", {{"a", DataType::UTF_DYNAMIC64}});
    const auto other = timeseries_df("ts", {{"a", DataType::INT64}});
    const auto message_for = [&](const SchemaCombineOptions& options) {
        try {
            combine({base, other}, options);
        } catch (const SchemaException& exception) {
            return std::string{exception.what()};
        }
        return std::string{};
    };
    ASSERT_NE(message_for(append_options(true)).find("append"), std::string::npos);
    ASSERT_NE(message_for(update_options(true)).find("update"), std::string::npos);
    ASSERT_NE(message_for(concat_options(JoinType::OUTER)).find("concat"), std::string::npos);
}

// A name disagreement is a descriptor mismatch wherever it occurs. Only the shape of the required fields - their
// count, or Series versus DataFrame - is an index incompatibility. See IncompatibleRequiredFieldShapesRaise.
TEST(CombineSchema, AppendRejectsRenamedRequiredFields) {
    const std::vector<ColumnSpec> columns{{"a", DataType::FLOAT64}};
    const ColumnSpec dt{"dt", DataType::NANOSECONDS_UTC64};
    const ColumnSpec ts{"ts", DataType::NANOSECONDS_UTC64};
    const ColumnSpec lvl{"lvl", DataType::INT32};
    const auto options = append_options(true);
    ASSERT_THROW(
            combine({multiindex_df({dt, lvl}, columns), multiindex_df({ts, lvl}, columns)}, options), SchemaException
    );
    ASSERT_THROW(
            combine({multiindex_df({dt, lvl}, columns), multiindex_df({dt, {"level2", DataType::INT32}}, columns)},
                    options),
            SchemaException
    );
    ASSERT_THROW(
            combine({timeseries_series("ts", "a", DataType::FLOAT64), timeseries_series("ts", "b", DataType::FLOAT64)},
                    options),
            SchemaException
    );
}

TEST(CombineSchema, AppendStaticSameColumnsSucceeds) {
    auto base = timeseries_df("ts", {{"a", DataType::FLOAT64}, {"b", DataType::INT64}});
    auto other = timeseries_df("ts", {{"a", DataType::FLOAT64}, {"b", DataType::INT64}});
    auto combined = combine({base, other}, append_options(false));
    ASSERT_EQ(columns_of(combined), columns_of(base));
}

TEST(CombineSchema, AppendStaticMissingColumnRaises) {
    auto base = timeseries_df("ts", {{"a", DataType::FLOAT64}, {"b", DataType::INT64}});
    auto other = timeseries_df("ts", {{"a", DataType::FLOAT64}});
    ASSERT_THROW(combine({base, other}, append_options(false)), SchemaException);
}

// Both promotions static schema allows are symmetric: whichever side is the base, the wider type wins.
TEST(CombineSchema, AppendStaticEmptyToConcretePromotion) {
    const auto empty = timeseries_df("ts", {{"a", DataType::EMPTYVAL}});
    const auto concrete = timeseries_df("ts", {{"a", DataType::FLOAT64}});
    ASSERT_EQ(
            combine({empty, concrete}, append_options(false)).stream_descriptor().field(1).type().data_type(),
            DataType::FLOAT64
    );
    ASSERT_EQ(
            combine({concrete, empty}, append_options(false)).stream_descriptor().field(1).type().data_type(),
            DataType::FLOAT64
    );
}

TEST(CombineSchema, AppendStaticFixedToDynamicStringPromotion) {
    const auto fixed = timeseries_df("ts", {{"a", DataType::UTF_FIXED64}});
    const auto dynamic = timeseries_df("ts", {{"a", DataType::UTF_DYNAMIC64}});
    ASSERT_EQ(
            combine({fixed, dynamic}, append_options(false)).stream_descriptor().field(1).type().data_type(),
            DataType::UTF_DYNAMIC64
    );
    ASSERT_EQ(
            combine({dynamic, fixed}, append_options(false)).stream_descriptor().field(1).type().data_type(),
            DataType::UTF_DYNAMIC64
    );
}

TEST(CombineSchema, AppendMismatchedIndexNameRaises) {
    auto base = timeseries_df("ts1", {{"a", DataType::FLOAT64}});
    auto other = timeseries_df("ts2", {{"a", DataType::FLOAT64}});
    ASSERT_THROW(combine({base, other}, append_options(false)), SchemaException);
}

TEST(CombineSchema, AppendDynamicKeepsUnionAndPromotes) {
    auto base = timeseries_df("ts", {{"a", DataType::INT32}});
    auto other = timeseries_df("ts", {{"a", DataType::INT64}, {"b", DataType::FLOAT64}});
    auto combined = combine({base, other}, append_options(true));
    const std::array expected{
            ColumnSpec{"ts", DataType::NANOSECONDS_UTC64},
            ColumnSpec{"a", DataType::INT64},
            ColumnSpec{"b", DataType::FLOAT64}
    };
    ASSERT_THAT(columns_of(combined), ElementsAreArray(expected));
}

TEST(CombineSchema, ConcatPromotesMixedSignednessDataColumnToFloat64) {
    auto base = timeseries_df("ts", {{"a", DataType::UINT64}});
    auto other = timeseries_df("ts", {{"a", DataType::INT64}});
    for (auto join_type : {JoinType::OUTER, JoinType::INNER}) {
        auto combined = combine({base, other}, concat_options(join_type));
        ASSERT_EQ(combined.stream_descriptor().field(1).type().data_type(), DataType::FLOAT64);
    }
}

TEST(CombineSchema, AppendRejectsMixedSignednessDataColumn) {
    auto base = timeseries_df("ts", {{"a", DataType::UINT64}});
    auto other = timeseries_df("ts", {{"a", DataType::INT64}});
    ASSERT_THROW(combine({base, other}, append_options(true)), SchemaException);
}

TEST(CombineSchema, RequiredFieldsNeverTakeTheFloat64Fallback) {
    const std::vector<ColumnSpec> columns{{"a", DataType::FLOAT64}};
    const ColumnSpec dt{"dt", DataType::NANOSECONDS_UTC64};
    auto base = multiindex_df({dt, {"lvl", DataType::UINT64}}, columns);
    auto other = multiindex_df({dt, {"lvl", DataType::INT64}}, columns);
    ASSERT_THROW(combine({base, other}, concat_options(JoinType::OUTER)), SchemaException);
    ASSERT_THROW(combine({base, other}, append_options(true)), SchemaException);
    // A level pair that does have an exact common type still promotes.
    auto promotable = multiindex_df({dt, {"lvl", DataType::INT32}}, columns);
    auto combined = combine({promotable, other}, concat_options(JoinType::OUTER));
    ASSERT_EQ(combined.stream_descriptor().field(1).type().data_type(), DataType::INT64);
}

TEST(CombineSchema, AllEmptyIndicesCombineToAnEmptyIndex) {
    auto empty_a = empty_index_df({{"a", DataType::EMPTYVAL}});
    auto empty_b = empty_index_df({{"b", DataType::EMPTYVAL}});

    auto combined = combine({empty_a, empty_b}, concat_options(JoinType::OUTER));
    ASSERT_EQ(combined.stream_descriptor().index().type(), IndexDescriptor::Type::EMPTY);
    const std::array expected{ColumnSpec{"a", DataType::EMPTYVAL}, ColumnSpec{"b", DataType::EMPTYVAL}};
    ASSERT_THAT(columns_of(combined), ElementsAreArray(expected));

    ASSERT_THAT(columns_of(combine({empty_a, empty_b}, concat_options(JoinType::INNER))), IsEmpty());
}

// Append used to let the last write's index name silently win (Monday 9797097831).
TEST(CombineSchema, RowCountIndexNameMismatchIsReconciledForConcatAndRaisesForAppend) {
    auto named = rowcount_df({{"a", DataType::FLOAT64}}, "index_name_1");
    auto renamed = rowcount_df({{"a", DataType::FLOAT64}}, "index_name_2");

    auto combined = combine({named, renamed}, concat_options(JoinType::OUTER));
    const auto& index = combined.norm_metadata_.df().common().index();
    ASSERT_EQ(index.name(), "index");
    ASSERT_TRUE(index.fake_name());

    ASSERT_THROW(combine({named, renamed}, append_options(true)), SchemaException);
    // Matching names are left alone.
    ASSERT_EQ(
            combine({named, named}, append_options(true)).norm_metadata_.df().common().index().name(), "index_name_1"
    );
}

// Which placeholder an unnamed index is stored under changed between client versions: 1.6.2 left the name empty
// where we write "index". Both record fake_name, so the name itself must not be compared, or data written by an old
// client can no longer be appended to. See test_compatibility.py::test_compat_update_old_updated_data.
TEST(CombineSchema, UnnamedIndexPlaceholderNamesFromDifferentClientVersionsAgree) {
    const std::vector<ColumnSpec> columns{{"a", DataType::FLOAT64}};
    auto old_client = timeseries_df("index", columns);
    old_client.norm_metadata_.mutable_df()->mutable_common()->mutable_index()->set_name("");
    old_client.norm_metadata_.mutable_df()->mutable_common()->mutable_index()->set_fake_name(true);
    auto new_client = timeseries_df("index", columns);
    new_client.norm_metadata_.mutable_df()->mutable_common()->mutable_index()->set_fake_name(true);

    // Whichever placeholder the base carries is the one that survives, and the result stays marked unnamed - an
    // old-client symbol must not come back named "index", nor a new-client one un-named.
    for (const auto& options : {append_options(false), append_options(true), update_options(false)}) {
        for (const auto& [base, other, expected_name] :
             {std::tuple{old_client, new_client, ""}, std::tuple{new_client, old_client, "index"}}) {
            const auto combined = combine({base, other}, options);
            const auto& index = combined.norm_metadata_.df().common().index();
            ASSERT_EQ(index.name(), expected_name) << "for " << options.name();
            ASSERT_TRUE(index.fake_name()) << "for " << options.name();
        }
    }

    // An index one side names and the other does not is still a disagreement.
    auto really_named = timeseries_df("index", columns);
    ASSERT_THROW(combine({old_client, really_named}, append_options(true)), SchemaException);
}

TEST(CombineSchema, UnnamedMultiIndexLevel0PlaceholderNamesAgree) {
    const std::vector<ColumnSpec> levels{{"index", DataType::NANOSECONDS_UTC64}, {"lvl", DataType::INT32}};
    auto old_client = multiindex_df(levels, {{"a", DataType::FLOAT64}}, {0});
    old_client.norm_metadata_.mutable_df()->mutable_common()->mutable_multi_index()->set_name("");
    auto new_client = multiindex_df(levels, {{"a", DataType::FLOAT64}}, {0});

    for (const auto& [base, other, expected_name] :
         {std::tuple{old_client, new_client, ""}, std::tuple{new_client, old_client, "index"}}) {
        const auto combined = combine({base, other}, append_options(true));
        const auto& multi_index = combined.norm_metadata_.df().common().multi_index();
        ASSERT_EQ(multi_index.name(), expected_name);
        ASSERT_THAT(fake_field_pos_of(combined), ElementsAre(0));
    }
}

// Append used to let the new frame's timezone overwrite the existing one (Monday 12029540807).
TEST(CombineSchema, MismatchedTimezoneIsClearedUnderDynamicSchemaAndRaisesUnderStatic) {
    auto london = timeseries_df("ts", {{"a", DataType::FLOAT64}}, "Europe/London");
    auto new_york = timeseries_df("ts", {{"a", DataType::FLOAT64}}, "America/New_York");
    for (const auto& options : {concat_options(JoinType::OUTER), append_options(true), update_options(true)}) {
        auto combined = combine({london, new_york}, options);
        ASSERT_EQ(combined.norm_metadata_.df().common().index().tz(), "");
    }
    for (const auto& options : {append_options(false), update_options(false)}) {
        ASSERT_THROW(combine({london, new_york}, options), SchemaException);
    }
    // A shared timezone survives under either schema.
    for (const auto& options : {append_options(true), append_options(false)}) {
        ASSERT_EQ(combine({london, london}, options).norm_metadata_.df().common().index().tz(), "Europe/London");
    }
}

TEST(CombineSchema, MismatchedMultiIndexLevelTimezoneFollowsTheSameRule) {
    const std::vector<ColumnSpec> levels{{"dt", DataType::NANOSECONDS_UTC64}, {"lvl", DataType::INT32}};
    auto london = multiindex_df(levels, {{"a", DataType::FLOAT64}});
    london.norm_metadata_.mutable_df()->mutable_common()->mutable_multi_index()->set_tz("Europe/London");
    auto new_york = multiindex_df(levels, {{"a", DataType::FLOAT64}});
    new_york.norm_metadata_.mutable_df()->mutable_common()->mutable_multi_index()->set_tz("America/New_York");

    ASSERT_EQ(combine({london, new_york}, append_options(true)).norm_metadata_.df().common().multi_index().tz(), "");
    ASSERT_THROW(combine({london, new_york}, append_options(false)), SchemaException);
}

TEST(CombineSchema, IncompatibleIndexTypesRaise) {
    auto timeseries = timeseries_df("ts", {{"a", DataType::FLOAT64}});
    auto rowcount = rowcount_df({{"a", DataType::FLOAT64}});
    ASSERT_THROW(combine({timeseries, rowcount}, concat_options(JoinType::OUTER)), NormalizationException);
    ASSERT_THROW(combine({rowcount, timeseries}, append_options(true)), NormalizationException);
}

// A type clash on a column a third schema lacks does not matter, as the inner join drops it either way.
TEST(CombineSchema, InnerJoinIgnoresIncompatibleTypesOnDroppedColumns) {
    auto schema_0 = timeseries_df("ts", {{"common", DataType::FLOAT64}, {"a", DataType::UTF_DYNAMIC64}});
    auto schema_1 = timeseries_df("ts", {{"common", DataType::FLOAT64}, {"a", DataType::INT64}});
    auto schema_2 = timeseries_df("ts", {{"common", DataType::FLOAT64}});

    auto combined = combine({schema_0, schema_1, schema_2}, concat_options(JoinType::INNER));
    const std::array expected{ColumnSpec{"ts", DataType::NANOSECONDS_UTC64}, ColumnSpec{"common", DataType::FLOAT64}};
    ASSERT_THAT(columns_of(combined), ElementsAreArray(expected));

    // An outer join keeps "a", so there the clash does matter.
    ASSERT_THROW(combine({schema_0, schema_1, schema_2}, concat_options(JoinType::OUTER)), SchemaException);
}

TEST(CombineSchema, ThreeSchemasKeepFirstSeenColumnOrder) {
    auto schema_0 = timeseries_df("ts", {{"a", DataType::FLOAT64}});
    auto schema_1 = timeseries_df("ts", {{"c", DataType::FLOAT64}, {"b", DataType::FLOAT64}});
    auto schema_2 = timeseries_df("ts", {{"b", DataType::FLOAT64}, {"d", DataType::FLOAT64}});
    auto combined = combine({schema_0, schema_1, schema_2}, concat_options(JoinType::OUTER));
    const std::array expected{
            ColumnSpec{"ts", DataType::NANOSECONDS_UTC64},
            ColumnSpec{"a", DataType::FLOAT64},
            ColumnSpec{"c", DataType::FLOAT64},
            ColumnSpec{"b", DataType::FLOAT64},
            ColumnSpec{"d", DataType::FLOAT64}
    };
    ASSERT_THAT(columns_of(combined), ElementsAreArray(expected));
}

TEST(CombineSchema, EmptyFrameSchemaImposesNoIndexConstraint) {
    const std::vector<ColumnSpec> columns{{"a", DataType::FLOAT64}};
    auto empty = written_empty_df(columns);
    auto rowcount = rowcount_df(columns);
    auto timeseries = timeseries_df("ts", columns);

    // The same pairs raise when the empty side has rows - see IncompatibleIndexTypesRaise.
    for (const auto& options : {append_options(true), append_options(false), concat_options(JoinType::OUTER)}) {
        ASSERT_EQ(
                combine({empty, rowcount}, options).stream_descriptor().index().type(), IndexDescriptor::Type::ROWCOUNT
        );
        ASSERT_EQ(
                combine({rowcount, empty}, options).stream_descriptor().index().type(), IndexDescriptor::Type::ROWCOUNT
        );
        ASSERT_EQ(
                combine({empty, timeseries}, options).stream_descriptor().index().type(),
                IndexDescriptor::Type::TIMESTAMP
        );
        ASSERT_EQ(
                combine({timeseries, empty}, options).stream_descriptor().index().type(),
                IndexDescriptor::Type::TIMESTAMP
        );
    }
}

// An empty frame's index name is not the name its user gave it, so it takes no part in the reconciliation.
TEST(CombineSchema, EmptyFrameSchemaImposesNoIndexName) {
    auto empty = written_empty_df({{"a", DataType::FLOAT64}}, "differing_index_name");
    auto timeseries = timeseries_df("ts", {{"a", DataType::FLOAT64}});
    const std::array expected{ColumnSpec{"ts", DataType::NANOSECONDS_UTC64}, ColumnSpec{"a", DataType::FLOAT64}};
    for (const auto& options : {append_options(true), concat_options(JoinType::OUTER)}) {
        auto combined = combine({empty, timeseries}, options);
        ASSERT_THAT(columns_of(combined), ElementsAreArray(expected));
        ASSERT_FALSE(combined.norm_metadata_.df().common().index().fake_name());
    }
}

// Declaring a column is deliberate with or without rows to put in it, so every policy treats an empty frame's columns
// as it would any other schema's.
TEST(CombineSchema, EmptyFrameSchemaContributesItsColumnsUnderEveryPolicy) {
    auto empty = written_empty_df({{"a", DataType::FLOAT64}, {"only_in_empty", DataType::FLOAT64}});
    auto timeseries = timeseries_df("ts", {{"a", DataType::FLOAT64}, {"only_in_rows", DataType::FLOAT64}});
    const ColumnSpec index{"ts", DataType::NANOSECONDS_UTC64};
    const ColumnSpec a{"a", DataType::FLOAT64};
    const ColumnSpec only_in_empty{"only_in_empty", DataType::FLOAT64};
    const ColumnSpec only_in_rows{"only_in_rows", DataType::FLOAT64};

    const std::array united{index, a, only_in_empty, only_in_rows};
    ASSERT_THAT(columns_of(combine({empty, timeseries}, concat_options(JoinType::OUTER))), ElementsAreArray(united));
    const std::array united_other_way{index, a, only_in_rows, only_in_empty};
    ASSERT_THAT(
            columns_of(combine({timeseries, empty}, concat_options(JoinType::OUTER))),
            ElementsAreArray(united_other_way)
    );

    // An intersection drops the column the empty frame does not have, as it would for any other schema.
    const std::array intersected{index, a};
    ASSERT_THAT(
            columns_of(combine({empty, timeseries}, concat_options(JoinType::INNER))), ElementsAreArray(intersected)
    );

    // And a strict policy rejects the pair outright.
    ASSERT_THROW(combine({empty, timeseries}, append_options(false)), SchemaException);
    ASSERT_THROW(combine({timeseries, empty}, append_options(false)), SchemaException);
}

TEST(CombineSchema, EmptyFrameSchemaCombineDifferentSizedIndices) {
    const std::vector<ColumnSpec> columns{{"a", DataType::FLOAT64}};
    const std::vector<ColumnSpec> levels{{"dt", DataType::NANOSECONDS_UTC64}, {"lvl", DataType::INT64}};
    auto multi_index = multiindex_df(levels, columns);
    auto empty_datetime = written_empty_df(columns);           // one index field, which the metadata denies
    auto empty_rowcount = written_empty(rowcount_df(columns)); // none at all
    const std::array expected{
            ColumnSpec{"dt", DataType::NANOSECONDS_UTC64},
            ColumnSpec{"lvl", DataType::INT64},
            ColumnSpec{"a", DataType::FLOAT64}
    };
    ASSERT_THAT(
            columns_of(combine({multi_index, empty_datetime, empty_rowcount}, append_options(true))),
            ElementsAreArray(expected)
    );
    ASSERT_THAT(
            columns_of(combine({empty_rowcount, empty_datetime, multi_index}, append_options(true))),
            ElementsAreArray(expected)
    );
}

// An empty frame says nothing about its index, but it does say what kind of object it is.
TEST(CombineSchema, EmptyFrameSchemaStillDecidesSeriesOrDataFrame) {
    auto empty_series = written_empty(timeseries_series("ts", "s", DataType::FLOAT64));
    auto empty_frame = written_empty_df({{"a", DataType::FLOAT64}});
    auto series = timeseries_series("ts", "s", DataType::FLOAT64);
    auto frame = timeseries_df("ts", {{"a", DataType::FLOAT64}});

    ASSERT_TRUE(combine({empty_series, series}, append_options(true)).norm_metadata_.has_series());
    ASSERT_TRUE(combine({empty_frame, frame}, append_options(true)).norm_metadata_.has_df());
    ASSERT_THROW(combine({empty_series, frame}, append_options(true)), NormalizationException);
    ASSERT_THROW(combine({empty_frame, series}, append_options(true)), NormalizationException);
}

// A rowcount Series' value column is a required field, so it is combined even though its index is not.
TEST(CombineSchema, EmptyFrameSchemaContributesItsSeriesValueColumn) {
    auto empty = written_empty(rowcount_series("index", "s", DataType::FLOAT64));
    auto series = timeseries_series("ts", "s", DataType::FLOAT64);
    const std::array expected{ColumnSpec{"ts", DataType::NANOSECONDS_UTC64}, ColumnSpec{"s", DataType::FLOAT64}};
    ASSERT_THAT(columns_of(combine({empty, series}, append_options(true))), ElementsAreArray(expected));
    ASSERT_THAT(columns_of(combine({series, empty}, append_options(true))), ElementsAreArray(expected));
}

// An empty frame can have more index fields than the result has required fields altogether, so the two ranges are lined
// up by their ends and the offset between them goes negative.
TEST(CombineSchema, EmptyFrameSchemaWithMoreIndexFieldsThanTheResult) {
    StreamDescriptor desc{StreamId{}, IndexDescriptorImpl{IndexDescriptor::Type::TIMESTAMP, 2}};
    desc.add_scalar_field(DataType::NANOSECONDS_UTC64, "dt");
    desc.add_scalar_field(DataType::INT64, "lvl");
    desc.add_scalar_field(DataType::FLOAT64, "s");
    NormalizationMetadata norm;
    auto* common = norm.mutable_series()->mutable_common();
    common->mutable_multi_index()->set_field_count(1);
    common->set_name("s");
    common->set_has_name(true);
    auto empty_multi_index_series = written_empty(OutputSchema{std::move(desc), std::move(norm)});
    auto series = rowcount_series("index", "s", DataType::FLOAT64);

    const std::array expected{ColumnSpec{"s", DataType::FLOAT64}};
    ASSERT_THAT(
            columns_of(combine({empty_multi_index_series, series}, append_options(true))), ElementsAreArray(expected)
    );
    ASSERT_THAT(
            columns_of(combine({series, empty_multi_index_series}, append_options(true))), ElementsAreArray(expected)
    );
}

TEST(CombineSchema, CombinedSchemaIsInferredOnlyIfEveryInputIs) {
    const std::vector<ColumnSpec> columns{{"a", DataType::FLOAT64}};
    auto empty = written_empty_df(columns);
    auto timeseries = timeseries_df("ts", columns);
    ASSERT_FALSE(combine({empty, timeseries}, append_options(true)).inferred_from_empty_frame());
    ASSERT_FALSE(combine({timeseries, timeseries}, append_options(true)).inferred_from_empty_frame());
    ASSERT_TRUE(combine({empty, empty}, append_options(true)).inferred_from_empty_frame());
}

// Only the arrow side knows whether there is an index, so its answer is written into the pandas metadata the result is
// denormalized from, and nothing the empty frame recorded about its own index survives.
TEST(CombineSchema, ArrowWithEmptyPandasDescribesTheIndexArrowHas) {
    auto empty = written_empty_df({{"a", DataType::FLOAT64}}, "index_name_from_the_empty_frame");
    empty.norm_metadata_.mutable_df()->mutable_common()->mutable_index()->set_tz("America/New_York");

    for (const bool arrow_has_index : {true, false}) {
        auto arrow = arrow_has_index ? timeseries_df("ts", {{"a", DataType::FLOAT64}})
                                     : rowcount_df({{"a", DataType::FLOAT64}});
        arrow.norm_metadata_.mutable_experimental_arrow()->set_has_index(arrow_has_index);

        for (auto schemas : {std::vector<OutputSchema>{empty, arrow}, std::vector<OutputSchema>{arrow, empty}}) {
            auto combined = combine(schemas, append_options(true));
            const auto& index = combined.norm_metadata_.df().common().index();
            ASSERT_EQ(index.is_physically_stored(), arrow_has_index);
            ASSERT_EQ(index.name(), "");
            ASSERT_EQ(index.tz(), "");
            if (!arrow_has_index) {
                // A RangeIndex needs a step for the read to rebuild it with.
                ASSERT_EQ(index.step(), 1);
            }
        }
    }
}
