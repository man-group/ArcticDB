/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <google/protobuf/util/message_differencer.h>

#include <gtest/gtest.h>
#include <arcticdb/processing/schema_combine.hpp>

using namespace arcticdb;
using namespace arcticdb::entity;
using namespace google::protobuf::util;
using NormalizationMetadata = arcticdb::proto::descriptors::NormalizationMetadata;

namespace {

OutputSchema timeseries_df(
        const std::string& index_name, const std::vector<std::pair<std::string, DataType>>& columns,
        const std::string& tz = ""
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

// Level 0 is a timestamp; the rest default to INT32 unless level_types says otherwise. The norm metadata's field
// count is one less than the number of levels, as _normalization.py records it.
OutputSchema multiindex_df(
        const std::vector<std::string>& level_names, const std::vector<std::pair<std::string, DataType>>& columns,
        const std::vector<uint32_t>& unnamed_levels = {}, const std::vector<DataType>& level_types = {}
) {
    const auto num_levels = static_cast<uint32_t>(level_names.size());
    StreamDescriptor desc{StreamId{}, IndexDescriptorImpl{IndexDescriptor::Type::TIMESTAMP, num_levels}};
    desc.add_scalar_field(DataType::NANOSECONDS_UTC64, level_names.front());
    for (size_t idx = 1; idx < level_names.size(); ++idx) {
        desc.add_scalar_field(idx - 1 < level_types.size() ? level_types[idx - 1] : DataType::INT32, level_names[idx]);
    }
    for (const auto& [name, type] : columns) {
        desc.add_scalar_field(type, name);
    }
    NormalizationMetadata norm;
    auto* multi_index = norm.mutable_df()->mutable_common()->mutable_multi_index();
    multi_index->set_field_count(num_levels - 1);
    multi_index->set_name(level_names.front());
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

OutputSchema empty_index_df(const std::vector<std::pair<std::string, DataType>>& columns) {
    StreamDescriptor desc{StreamId{}, IndexDescriptorImpl{IndexDescriptor::Type::EMPTY, 0}};
    for (const auto& [name, type] : columns) {
        desc.add_scalar_field(type, name);
    }
    NormalizationMetadata norm;
    norm.mutable_df()->mutable_common()->mutable_index()->set_is_physically_stored(false);
    return {std::move(desc), std::move(norm)};
}

OutputSchema rowcount_df(
        const std::vector<std::pair<std::string, DataType>>& columns, const std::string& index_name = ""
) {
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

OutputSchema combine(std::vector<OutputSchema> schemas, const SchemaCombineOptions& options) {
    return combine_schema(schemas, options);
}

std::vector<std::pair<std::string, DataType>> columns_of(const OutputSchema& schema) {
    std::vector<std::pair<std::string, DataType>> out;
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
    std::vector<std::pair<std::string, DataType>> expected{
            {"ts", DataType::NANOSECONDS_UTC64},
            {"a", DataType::FLOAT64},
            {"b", DataType::FLOAT64},
            {"c", DataType::FLOAT64}
    };
    ASSERT_EQ(columns_of(combined), expected);
}

TEST(CombineSchema, ConcatInnerIntersectionOfColumns) {
    auto base = timeseries_df("ts", {{"a", DataType::FLOAT64}, {"b", DataType::FLOAT64}});
    auto other = timeseries_df("ts", {{"b", DataType::FLOAT64}, {"c", DataType::FLOAT64}});
    auto combined = combine({base, other}, concat_options(JoinType::INNER));
    std::vector<std::pair<std::string, DataType>> expected{
            {"ts", DataType::NANOSECONDS_UTC64}, {"b", DataType::FLOAT64}
    };
    ASSERT_EQ(columns_of(combined), expected);
}

TEST(CombineSchema, ConcatOuterTypePromotion) {
    auto base = timeseries_df("ts", {{"a", DataType::INT32}});
    auto other = timeseries_df("ts", {{"a", DataType::INT64}});
    auto combined = combine({base, other}, concat_options(JoinType::OUTER));
    ASSERT_EQ(combined.stream_descriptor().field(1).type().data_type(), DataType::INT64);
}

TEST(CombineSchema, ConcatMismatchedIndexNameReconciledToFake) {
    auto base = timeseries_df("ts1", {{"a", DataType::FLOAT64}});
    auto other = timeseries_df("ts2", {{"a", DataType::FLOAT64}});
    auto combined = combine({base, other}, concat_options(JoinType::OUTER));
    ASSERT_EQ(combined.stream_descriptor().field(0).name(), "index");
}

TEST(CombineSchema, ConcatRenamedMultiIndexLevelsReconciledToFake) {
    const std::vector<std::pair<std::string, DataType>> columns{{"a", DataType::FLOAT64}};
    const auto both_named = multiindex_df({"dt", "lvl"}, columns);
    const auto first_renamed = multiindex_df({"ts", "lvl"}, columns);
    const auto second_renamed = multiindex_df({"dt", "level2"}, columns);
    const auto both_renamed = multiindex_df({"ts", "level2"}, columns);

    const auto combine_both_ways = [&](const OutputSchema& lhs, const OutputSchema& rhs, auto&& assertions) {
        assertions(combine({lhs, rhs}, concat_options(JoinType::OUTER)));
        assertions(combine({rhs, lhs}, concat_options(JoinType::OUTER)));
    };

    // Only level 0 differs: it takes the name "index", level 1 keeps its own.
    combine_both_ways(both_named, first_renamed, [](const OutputSchema& combined) {
        ASSERT_EQ(combined.stream_descriptor().field(0).name(), "index");
        ASSERT_EQ(combined.stream_descriptor().field(1).name(), "lvl");
        ASSERT_EQ(fake_field_pos_of(combined), std::vector<uint32_t>{0});
        ASSERT_EQ(combined.norm_metadata_.df().common().multi_index().name(), "index");
    });

    // Only level 1 differs: level 0 keeps its name and level 1 takes the __fkidx__ scheme.
    combine_both_ways(both_named, second_renamed, [](const OutputSchema& combined) {
        ASSERT_EQ(combined.stream_descriptor().field(0).name(), "dt");
        ASSERT_EQ(combined.stream_descriptor().field(1).name(), "__fkidx__1");
        ASSERT_EQ(fake_field_pos_of(combined), std::vector<uint32_t>{1});
    });

    combine_both_ways(both_named, both_renamed, [](const OutputSchema& combined) {
        ASSERT_EQ(combined.stream_descriptor().field(0).name(), "index");
        ASSERT_EQ(combined.stream_descriptor().field(1).name(), "__fkidx__1");
        ASSERT_EQ(fake_field_pos_of(combined), (std::vector<uint32_t>{0, 1}));
    });

    combine_both_ways(both_named, both_named, [](const OutputSchema& combined) {
        ASSERT_EQ(combined.stream_descriptor().field(0).name(), "dt");
        ASSERT_EQ(combined.stream_descriptor().field(1).name(), "lvl");
        ASSERT_TRUE(fake_field_pos_of(combined).empty());
    });
}

TEST(CombineSchema, ConcatRenamedSeriesValueColumnDropsTheName) {
    const auto series_a = timeseries_series("ts", "a", DataType::FLOAT64);
    const auto series_b = timeseries_series("ts", "b", DataType::FLOAT64);

    for (auto schemas :
         {std::vector<OutputSchema>{series_a, series_b}, std::vector<OutputSchema>{series_b, series_a}}) {
        auto combined = combine(schemas, concat_options(JoinType::OUTER));
        ASSERT_EQ(combined.stream_descriptor().field(0).name(), "ts");
        ASSERT_EQ(combined.stream_descriptor().field(1).name(), "__fkidx__1");
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
    const std::vector<std::pair<std::string, DataType>> columns{{"a", DataType::FLOAT64}};
    const auto unnamed_level_1 = multiindex_df({"dt", "__fkidx__1"}, columns, {1});

    auto combined = combine({unnamed_level_1, unnamed_level_1}, concat_options(JoinType::OUTER));
    ASSERT_EQ(fake_field_pos_of(combined), std::vector<uint32_t>{1});
    ASSERT_EQ(combined.stream_descriptor().field(1).name(), "__fkidx__1");

    // Level 0 disagrees, so there is a mismatch, and level 1 must not be lost while it is applied.
    const auto renamed_level_0 = multiindex_df({"ts", "__fkidx__1"}, columns, {1});
    combined = combine({unnamed_level_1, renamed_level_0}, concat_options(JoinType::OUTER));
    ASSERT_EQ(fake_field_pos_of(combined), (std::vector<uint32_t>{0, 1}));

    // And appending them is fine, since nothing about the names disagrees.
    combined = combine({unnamed_level_1, unnamed_level_1}, append_options(true));
    ASSERT_EQ(fake_field_pos_of(combined), std::vector<uint32_t>{1});
}

TEST(CombineSchema, AgreedUnnamedLevelSurvivesAlongsideADisagreeingOne) {
    const auto both = multiindex_df({"dt", "__fkidx__1", "__fkidx__2"}, {{"a", DataType::FLOAT64}}, {1, 2});
    const auto only_level_1 = multiindex_df({"dt", "__fkidx__1", "lvl2"}, {{"a", DataType::FLOAT64}}, {1});

    auto combined = combine({both, only_level_1}, concat_options(JoinType::OUTER));
    ASSERT_EQ(fake_field_pos_of(combined), (std::vector<uint32_t>{1, 2}));

    // Independent of the ordering of the inputs.
    combined = combine({only_level_1, both}, concat_options(JoinType::OUTER));
    ASSERT_EQ(fake_field_pos_of(combined), (std::vector<uint32_t>{1, 2}));
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
    const std::vector<std::pair<std::string, DataType>> columns{{"a", DataType::FLOAT64}};
    const auto two_levels = multiindex_df({"dt", "lvl"}, {{"a", DataType::FLOAT64}});
    const auto three_levels = multiindex_df({"dt", "lvl", "lvl2"}, {{"a", DataType::FLOAT64}});
    const auto scalar_index = timeseries_df("dt", columns);
    const auto series = timeseries_series("dt", "v", DataType::FLOAT64);

    for (const auto& options : {concat_options(JoinType::OUTER), append_options(true)}) {
        ASSERT_THROW(combine({two_levels, three_levels}, options), NormalizationException);
        ASSERT_THROW(combine({three_levels, two_levels}, options), NormalizationException);
        ASSERT_THROW(combine({two_levels, scalar_index}, options), NormalizationException);
        ASSERT_THROW(combine({scalar_index, two_levels}, options), NormalizationException);
        ASSERT_THROW(combine({scalar_index, series}, options), NormalizationException);
        ASSERT_THROW(combine({series, scalar_index}, options), NormalizationException);
        ASSERT_THROW(combine({two_levels, series}, options), NormalizationException);
    }
}

// Arrow metadata records neither the Series/DataFrame distinction nor multi-index levels, so it has no shape
// to disagree about.
TEST(CombineSchema, ArrowSchemaHasNoShapeToDisagreeAbout) {
    const auto series = timeseries_series("ts", "col", DataType::INT64);
    auto arrow = timeseries_df("ts", {{"col", DataType::INT64}});
    arrow.norm_metadata_.mutable_experimental_arrow()->set_has_index(true);

    for (auto schemas : {std::vector<OutputSchema>{series, arrow}, std::vector<OutputSchema>{arrow, series}}) {
        auto combined = combine(schemas, concat_options(JoinType::OUTER));
        ASSERT_EQ(
                columns_of(combined),
                (std::vector<std::pair<std::string, DataType>>{
                        {"ts", DataType::NANOSECONDS_UTC64}, {"col", DataType::INT64}
                })
        );
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

TEST(CombineSchema, AppendRejectsRenamedRequiredFields) {
    const std::vector<std::pair<std::string, DataType>> columns{{"a", DataType::FLOAT64}};
    const auto options = append_options(true);
    // A multi-index level name is what keeps the normalization metadata in step with the data, so a disagreement
    // is an index incompatibility rather than a descriptor mismatch.
    ASSERT_THROW(
            combine({multiindex_df({"dt", "lvl"}, columns), multiindex_df({"ts", "lvl"}, columns)}, options),
            NormalizationException
    );
    ASSERT_THROW(
            combine({multiindex_df({"dt", "lvl"}, columns), multiindex_df({"dt", "level2"}, columns)}, options),
            NormalizationException
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

TEST(CombineSchema, AppendStaticEmptyToConcretePromotion) {
    auto base = timeseries_df("ts", {{"a", DataType::EMPTYVAL}});
    auto other = timeseries_df("ts", {{"a", DataType::FLOAT64}});
    auto combined = combine({base, other}, append_options(false));
    ASSERT_EQ(combined.stream_descriptor().field(1).type().data_type(), DataType::FLOAT64);
}

TEST(CombineSchema, AppendStaticFixedToDynamicStringPromotion) {
    auto base = timeseries_df("ts", {{"a", DataType::UTF_FIXED64}});
    auto other = timeseries_df("ts", {{"a", DataType::UTF_DYNAMIC64}});
    auto combined = combine({base, other}, append_options(false));
    ASSERT_EQ(combined.stream_descriptor().field(1).type().data_type(), DataType::UTF_DYNAMIC64);
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
    std::vector<std::pair<std::string, DataType>> expected{
            {"ts", DataType::NANOSECONDS_UTC64}, {"a", DataType::INT64}, {"b", DataType::FLOAT64}
    };
    ASSERT_EQ(columns_of(combined), expected);
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
    auto base = multiindex_df({"dt", "lvl"}, {{"a", DataType::FLOAT64}}, {}, {DataType::UINT64});
    auto other = multiindex_df({"dt", "lvl"}, {{"a", DataType::FLOAT64}}, {}, {DataType::INT64});
    ASSERT_THROW(combine({base, other}, concat_options(JoinType::OUTER)), SchemaException);
    ASSERT_THROW(combine({base, other}, append_options(true)), SchemaException);
    // A level pair that does have an exact common type still promotes.
    auto promotable = multiindex_df({"dt", "lvl"}, {{"a", DataType::FLOAT64}});
    auto combined = combine({promotable, other}, concat_options(JoinType::OUTER));
    ASSERT_EQ(combined.stream_descriptor().field(1).type().data_type(), DataType::INT64);
}

TEST(CombineSchema, EmptyIndexTakesOnTheConcreteIndexItIsCombinedWith) {
    auto empty = empty_index_df({{"a", DataType::EMPTYVAL}});
    auto timeseries = timeseries_df("ts", {{"a", DataType::FLOAT64}});
    for (const auto& options :
         {concat_options(JoinType::OUTER), concat_options(JoinType::INNER), append_options(true)}) {
        for (auto schemas :
             {std::vector<OutputSchema>{empty, timeseries}, std::vector<OutputSchema>{timeseries, empty}}) {
            auto combined = combine(schemas, options);
            ASSERT_EQ(combined.stream_descriptor().index().type(), IndexDescriptor::Type::TIMESTAMP);
            ASSERT_EQ(
                    columns_of(combined),
                    (std::vector<std::pair<std::string, DataType>>{
                            {"ts", DataType::NANOSECONDS_UTC64}, {"a", DataType::FLOAT64}
                    })
            );
            ASSERT_TRUE(combined.norm_metadata_.df().common().index().is_physically_stored());
        }
    }
}

// Callers that want a zero-row frame to contribute nothing drop it before combining, rather than relying on
// the combine to ignore it.
TEST(CombineSchema, EmptyIndexSchemaContributesItsDataColumns) {
    auto empty = empty_index_df({{"a", DataType::EMPTYVAL}, {"only_empty", DataType::EMPTYVAL}});
    auto timeseries = timeseries_df("ts", {{"a", DataType::FLOAT64}});

    auto outer = combine({timeseries, empty}, concat_options(JoinType::OUTER));
    ASSERT_EQ(
            columns_of(outer),
            (std::vector<std::pair<std::string, DataType>>{
                    {"ts", DataType::NANOSECONDS_UTC64}, {"a", DataType::FLOAT64}, {"only_empty", DataType::EMPTYVAL}
            })
    );

    auto inner = combine({timeseries, empty}, concat_options(JoinType::INNER));
    ASSERT_EQ(
            columns_of(inner),
            (std::vector<std::pair<std::string, DataType>>{
                    {"ts", DataType::NANOSECONDS_UTC64}, {"a", DataType::FLOAT64}
            })
    );
}

TEST(CombineSchema, AllEmptyIndicesCombineToAnEmptyIndex) {
    auto empty_a = empty_index_df({{"a", DataType::EMPTYVAL}});
    auto empty_b = empty_index_df({{"b", DataType::EMPTYVAL}});

    auto combined = combine({empty_a, empty_b}, concat_options(JoinType::OUTER));
    ASSERT_EQ(combined.stream_descriptor().index().type(), IndexDescriptor::Type::EMPTY);
    ASSERT_EQ(
            columns_of(combined),
            (std::vector<std::pair<std::string, DataType>>{{"a", DataType::EMPTYVAL}, {"b", DataType::EMPTYVAL}})
    );

    ASSERT_TRUE(columns_of(combine({empty_a, empty_b}, concat_options(JoinType::INNER))).empty());
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
    const std::vector<std::pair<std::string, DataType>> columns{{"a", DataType::FLOAT64}};
    auto old_client = timeseries_df("index", columns);
    old_client.norm_metadata_.mutable_df()->mutable_common()->mutable_index()->set_name("");
    old_client.norm_metadata_.mutable_df()->mutable_common()->mutable_index()->set_fake_name(true);
    auto new_client = timeseries_df("index", columns);
    new_client.norm_metadata_.mutable_df()->mutable_common()->mutable_index()->set_fake_name(true);

    for (const auto& options : {append_options(false), append_options(true), update_options(false)}) {
        ASSERT_NO_THROW(combine({old_client, new_client}, options));
        ASSERT_NO_THROW(combine({new_client, old_client}, options));
    }

    // An index one side names and the other does not is still a disagreement.
    auto really_named = timeseries_df("index", columns);
    ASSERT_THROW(combine({old_client, really_named}, append_options(true)), SchemaException);
}

TEST(CombineSchema, UnnamedMultiIndexLevel0PlaceholderNamesAgree) {
    auto old_client = multiindex_df({"index", "lvl"}, {{"a", DataType::FLOAT64}}, {0});
    old_client.norm_metadata_.mutable_df()->mutable_common()->mutable_multi_index()->set_name("");
    auto new_client = multiindex_df({"index", "lvl"}, {{"a", DataType::FLOAT64}}, {0});

    ASSERT_NO_THROW(combine({old_client, new_client}, append_options(true)));
    ASSERT_NO_THROW(combine({new_client, old_client}, append_options(true)));
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
    auto london = multiindex_df({"dt", "lvl"}, {{"a", DataType::FLOAT64}});
    london.norm_metadata_.mutable_df()->mutable_common()->mutable_multi_index()->set_tz("Europe/London");
    auto new_york = multiindex_df({"dt", "lvl"}, {{"a", DataType::FLOAT64}});
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
    std::vector<std::pair<std::string, DataType>> expected{
            {"ts", DataType::NANOSECONDS_UTC64}, {"common", DataType::FLOAT64}
    };
    ASSERT_EQ(columns_of(combined), expected);

    // An outer join keeps "a", so there the clash does matter.
    ASSERT_THROW(combine({schema_0, schema_1, schema_2}, concat_options(JoinType::OUTER)), SchemaException);
}

TEST(CombineSchema, ThreeSchemasKeepFirstSeenColumnOrder) {
    auto schema_0 = timeseries_df("ts", {{"a", DataType::FLOAT64}});
    auto schema_1 = timeseries_df("ts", {{"c", DataType::FLOAT64}, {"b", DataType::FLOAT64}});
    auto schema_2 = timeseries_df("ts", {{"b", DataType::FLOAT64}, {"d", DataType::FLOAT64}});
    auto combined = combine({schema_0, schema_1, schema_2}, concat_options(JoinType::OUTER));
    std::vector<std::pair<std::string, DataType>> expected{
            {"ts", DataType::NANOSECONDS_UTC64},
            {"a", DataType::FLOAT64},
            {"c", DataType::FLOAT64},
            {"b", DataType::FLOAT64},
            {"d", DataType::FLOAT64}
    };
    ASSERT_EQ(columns_of(combined), expected);
}
