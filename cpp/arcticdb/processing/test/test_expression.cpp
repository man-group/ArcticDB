/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <gtest/gtest.h>
#include <arcticdb/processing/expression_context.hpp>
#include <arcticdb/processing/expression_node.hpp>
#include <arcticdb/processing/processing_unit.hpp>
#include <arcticdb/processing/test/ast_test_helpers.hpp>
#include <arcticdb/pipeline/value_set.hpp>
#include <arcticdb/column_store/string_pool.hpp>
#include <arcticdb/util/string_utils.hpp>
#include <arcticdb/util/test/generators.hpp>
#include <arcticdb/util/test/segment_generation_utils.hpp>

#include <algorithm>
#include <stdexcept>
#include <unordered_set>

TEST(ExpressionNode, AddBasic) {
    using namespace arcticdb;
    StreamId symbol("test_add");
    SegmentInMemory seg = create_dense_segment(
            stream_descriptor(
                    symbol,
                    stream::RowCountIndex{},
                    {scalar_field(DataType::UINT64, "thing1"), scalar_field(DataType::UINT64, "thing2")}
            ),
            std::views::iota(uint64_t{0}, uint64_t{20}),
            std::views::iota(uint64_t{1}, uint64_t{21})
    );
    ProcessingUnit proc(std::move(seg));
    auto root = node(col("thing1"), col("thing2"), OperationType::ADD);
    auto expression_context = std::make_shared<ExpressionContext>();
    expression_context->root_ = root;
    proc.set_expression_context(expression_context);
    auto ret = root->compute(proc);
    const auto& result_col = std::get<ColumnWithStrings>(ret).column_;

    for (auto j = 0; j < 20; ++j) {
        auto v1 = proc.segments_->at(0)->scalar_at<uint64_t>(j, 0);
        ASSERT_EQ(v1.value(), j);
        auto v2 = proc.segments_->at(0)->scalar_at<uint64_t>(j, 1);
        ASSERT_EQ(v2.value(), j + 1);
        ASSERT_EQ(result_col->scalar_at<uint64_t>(j), v1.value() + v2.value());
    }
}

TEST(ExpressionNode, SubexpressionMemoized) {
    using namespace arcticdb;
    StreamId symbol("test_longhand");
    SegmentInMemory seg = create_dense_segment(
            stream_descriptor(
                    symbol,
                    stream::RowCountIndex{},
                    {scalar_field(DataType::UINT64, "thing1"), scalar_field(DataType::UINT64, "thing2")}
            ),
            std::views::iota(uint64_t{0}, uint64_t{20}),
            std::views::iota(uint64_t{1}, uint64_t{21})
    );
    ProcessingUnit proc(std::move(seg));

    auto add1 = node(col("thing1"), col("thing2"), OperationType::ADD);
    auto add2 = node(col("thing1"), col("thing2"), OperationType::ADD);
    auto root = node(add1, add2, OperationType::MUL);

    auto expression_context = std::make_shared<ExpressionContext>();
    expression_context->root_ = root;
    proc.set_expression_context(expression_context);

    auto ret = root->compute(proc);
    const auto& result_col = std::get<ColumnWithStrings>(ret).column_;

    for (auto j = 0; j < 20; ++j) {
        auto v1 = proc.segments_->at(0)->scalar_at<uint64_t>(j, 0).value();
        auto v2 = proc.segments_->at(0)->scalar_at<uint64_t>(j, 1).value();
        ASSERT_EQ(result_col->scalar_at<uint64_t>(j), (v1 + v2) * (v1 + v2));
    }

    const std::string add_label = R"((Column["thing1"] ADD Column["thing2"]))";
    ASSERT_EQ(add1->label_, add_label);
    ASSERT_EQ(add2->label_, add_label);
    ASSERT_TRUE(proc.computed_data_.contains(add_label));
    const auto* cached_node = proc.computed_data_.at(add_label).first;
    ASSERT_TRUE(cached_node == add1.get() || cached_node == add2.get());
    ASSERT_EQ(proc.computed_data_.size(), 2);
}

TEST(ExpressionNode, NoFalseReuseOnLabelClash) {
    using namespace arcticdb;
    StreamId symbol("test_label_clash");
    SegmentInMemory seg = create_dense_segment(
            stream_descriptor(symbol, stream::RowCountIndex{}, {scalar_field(DataType::INT64, "thing1")}),
            std::views::iota(int64_t{0}, int64_t{20})
    );
    ProcessingUnit proc(std::move(seg));

    auto set_a = std::make_shared<ValueSet>(
            std::make_shared<std::unordered_set<int64_t>>(std::unordered_set<int64_t>{0, 1, 2})
    );
    auto set_b = std::make_shared<ValueSet>(
            std::make_shared<std::unordered_set<int64_t>>(std::unordered_set<int64_t>{3, 4, 5})
    );

    auto isin_a = node(col("thing1"), vset(set_a), OperationType::ISIN);
    auto isin_b = node(col("thing1"), vset(set_b), OperationType::ISIN);

    auto expression_context = std::make_shared<ExpressionContext>();
    expression_context->root_ = isin_a;
    proc.set_expression_context(expression_context);

    // The coarse value-set label keys only on dtype and size, so these two operations collide.
    ASSERT_EQ(isin_a->label_, isin_b->label_);

    auto bitset_a = std::get<util::BitSet>(isin_a->compute(proc));
    auto bitset_b = std::get<util::BitSet>(isin_b->compute(proc));

    for (size_t idx = 0; idx < 20; ++idx) {
        ASSERT_EQ(set_a->get_set<int64_t>()->contains(static_cast<int64_t>(idx)), bitset_a.get_bit(idx));
        ASSERT_EQ(set_b->get_set<int64_t>()->contains(static_cast<int64_t>(idx)), bitset_b.get_bit(idx));
    }
}

namespace {
using namespace arcticdb;

// A fixed-width string pool entry holds the numpy array's bytes verbatim, null padded to the column
// width: UCS-4 per character for `<U`, one byte per character for `<S`.
ColumnWithStrings build_fixed_width_column(DataType data_type, std::string_view padded_bytes) {
    auto string_pool = std::make_shared<StringPool>();
    const auto offset = string_pool->get(padded_bytes, false).offset();
    Column col(make_scalar_type(data_type), 1, AllocationType::PRESIZED, Sparsity::NOT_PERMITTED);
    col.reference_at<entity::position_t>(0) = offset;
    col.set_row_data(0);
    return {std::move(col), string_pool, "strings"};
}

std::string padded_utf32(std::string_view utf8, size_t width) {
    auto utf32 = util::utf8_to_u32(utf8);
    utf32.resize(width, char32_t{0});
    return {reinterpret_cast<const char*>(utf32.data()), utf32.size() * sizeof(char32_t)};
}

std::optional<std::string_view> stripped_string_at_row_zero(const ColumnWithStrings& column) {
    const auto offset = column.column_->scalar_at<entity::position_t>(0);
    return column.string_at_offset(*offset, true);
}
} // namespace

TEST(ColumnWithStringsFixedWidth, Utf32PaddingComesOffAWholeCodepointAtATime) {
    const auto column = build_fixed_width_column(DataType::UTF_FIXED64, padded_utf32("ab", 8));
    const auto stripped = stripped_string_at_row_zero(column);

    // Stripping in units of UNICODE_WIDTH, which is sizeof(wchar_t) and so two bytes on Windows,
    // leaves six bytes here: the two trailing zero bytes of 'b' are counted as padding, and anything
    // reading the view back as UCS-4 then silently loses the 'b'.
    ASSERT_TRUE(stripped.has_value());
    ASSERT_EQ(stripped->size(), 2 * UTF32_WIDTH);
    ASSERT_EQ(util::utf32_to_u8(*stripped), "ab");
}

TEST(ColumnWithStringsFixedWidth, Utf32PaddingStripKeepsACodepointWhoseHighBytesAreZero) {
    // U+00E9 is 'e9 00 00 00' little-endian, so three of its four bytes are zero: a narrower strip
    // width eats the whole codepoint and leaves an empty view.
    const auto column = build_fixed_width_column(DataType::UTF_FIXED64, padded_utf32("\xC3\xA9", 4));
    const auto stripped = stripped_string_at_row_zero(column);

    ASSERT_TRUE(stripped.has_value());
    ASSERT_EQ(stripped->size(), UTF32_WIDTH);
    ASSERT_EQ(util::utf32_to_u8(*stripped), "\xC3\xA9");
}

TEST(ColumnWithStringsFixedWidth, Utf32InteriorNullCodepointIsNotPadding) {
    const auto column = build_fixed_width_column(DataType::UTF_FIXED64, padded_utf32(std::string{"a\0b", 3}, 8));
    const auto stripped = stripped_string_at_row_zero(column);

    // Only the trailing codepoints go. The interior null is data, and the engine's equality path
    // matches on it.
    ASSERT_TRUE(stripped.has_value());
    ASSERT_EQ(stripped->size(), 3 * UTF32_WIDTH);
}

TEST(ColumnWithStringsFixedWidth, Utf32EntryOfPurePaddingStripsToNothing) {
    const auto column = build_fixed_width_column(DataType::UTF_FIXED64, padded_utf32("", 4));
    const auto stripped = stripped_string_at_row_zero(column);

    ASSERT_TRUE(stripped.has_value());
    ASSERT_TRUE(stripped->empty());
}

TEST(ColumnWithStringsFixedWidth, AsciiPaddingComesOffAByteAtATime) {
    std::string padded{"ab"};
    padded.resize(8, '\0');
    const auto column = build_fixed_width_column(DataType::ASCII_FIXED64, padded);
    const auto stripped = stripped_string_at_row_zero(column);

    ASSERT_TRUE(stripped.has_value());
    ASSERT_EQ(*stripped, "ab");
}

namespace {
using namespace arcticdb;

ProcessingUnit int_column_processing_unit(const std::shared_ptr<ExpressionNode>& root) {
    SegmentInMemory seg = create_dense_segment(
            stream_descriptor(StreamId{"deep"}, stream::RowCountIndex{}, {scalar_field(DataType::INT64, "thing1")}),
            std::views::iota(int64_t{0}, int64_t{20})
    );
    ProcessingUnit proc(std::move(seg));
    auto expression_context = std::make_shared<ExpressionContext>();
    expression_context->root_ = root;
    proc.set_expression_context(expression_context);
    return proc;
}

} // namespace

TEST(ExpressionNode, DeepNotChainDoesNotOverflowStack) {
    using namespace arcticdb;
    // Deep enough to overflow an 8 MB stack when recursing, but not so deep that quadratic labels use much memory
    constexpr size_t depth = 10'000;
    auto root = node(col("thing1"), val(int64_t{9}, DataType::INT64), OperationType::GT);
    for (size_t i = 0; i < depth; ++i) {
        root = node(std::move(root), OperationType::NOT);
    }

    const ankerl::unordered_dense::map<std::string, DataType> column_types{{"thing1", DataType::INT64}};
    ASSERT_TRUE(std::holds_alternative<BitSetTag>(root->compute(column_types)));

    auto proc = int_column_processing_unit(root);
    auto bitset = std::get<util::BitSet>(root->compute(proc));
    for (size_t idx = 0; idx < 20; ++idx) {
        ASSERT_EQ(bitset.get_bit(idx), idx > 9);
    }
    // Destroying the chain must not recurse once per level either
    proc.computed_data_.clear();
    proc.expression_context_.reset();
    root.reset();
}

TEST(ExpressionNode, VeryDeepNotChainUsesBoundedMemory) {
    using namespace arcticdb;
    constexpr size_t depth = 200'000;
    auto root = node(col("thing1"), val(int64_t{9}, DataType::INT64), OperationType::GT);
    for (size_t i = 0; i < depth; ++i) {
        root = node(std::move(root), OperationType::NOT);
        // Fail at once rather than build quadratically many bytes of labels
        ASSERT_LE(root->label_.size(), 4096);
    }

    auto proc = int_column_processing_unit(root);
    auto bitset = std::get<util::BitSet>(root->compute(proc));
    for (size_t idx = 0; idx < 20; ++idx) {
        ASSERT_EQ(bitset.get_bit(idx), idx > 9);
    }
    ASSERT_LE(proc.computed_data_.size(), 128);
    proc.computed_data_.clear();
    proc.expression_context_.reset();
    root.reset();
}

TEST(ExpressionNode, SharedSubexpressionsEvaluatedOnce) {
    using namespace arcticdb;
    // Each level uses the level below twice: evaluating per path would take 2^24 evaluations, slow but survivable
    constexpr size_t depth = 24;
    auto root = node(col("thing1"), val(int64_t{9}, DataType::INT64), OperationType::GT);
    for (size_t i = 0; i < depth; ++i) {
        root = node(root, root, i % 2 ? OperationType::AND : OperationType::OR);
        // Fail at once rather than build exponentially many bytes of labels
        ASSERT_LE(root->label_.size(), 4096);
    }

    const ankerl::unordered_dense::map<std::string, DataType> column_types{{"thing1", DataType::INT64}};
    ASSERT_TRUE(std::holds_alternative<BitSetTag>(root->compute(column_types)));

    auto proc = int_column_processing_unit(root);
    auto bitset = std::get<util::BitSet>(root->compute(proc));
    for (size_t idx = 0; idx < 20; ++idx) {
        ASSERT_EQ(bitset.get_bit(idx), idx > 9);
    }
}

TEST(ExpressionNode, StructurallyEqualSharedDagsAreMemoizedAcrossNodes) {
    using namespace arcticdb;
    // Column result names and labels would reach tens of MB at this depth if unbounded, so a regression fails cheaply.
    // Do not raise this depth: at 64 unbounded names exhaust the machine's memory.
    constexpr size_t depth = 20;
    auto build = [] {
        auto root = node(col("thing1"), val(int64_t{1}, DataType::INT64), OperationType::ADD);
        for (size_t i = 0; i < depth; ++i) {
            root = node(root, root, OperationType::ADD);
        }
        return root;
    };
    auto first = build();
    auto second = build();
    ASSERT_LE(first->label_.size(), 4096);
    auto proc = int_column_processing_unit(first);
    auto first_result = std::get<ColumnWithStrings>(first->compute(proc));
    ASSERT_LE(first_result.column_name_.size(), 4096);
    // second is a different DAG with the same structure, so it hits the first's memoized result
    auto second_result = std::get<ColumnWithStrings>(second->compute(proc));
    ASSERT_EQ(first_result.column_, second_result.column_);
}

TEST(ExpressionNode, TruncatedLabelsStillDistinguishExpressions) {
    using namespace arcticdb;
    auto build = [](int64_t innermost) {
        auto root = node(col("thing1"), val(innermost, DataType::INT64), OperationType::GT);
        for (size_t i = 0; i < 1'000; ++i) {
            root = node(std::move(root), OperationType::NOT);
        }
        return root;
    };
    auto a = build(1);
    ASSERT_LE(a->label_.size(), 4096);
    ASSERT_EQ(a->label_, build(1)->label_);
    ASSERT_NE(a->label_, build(2)->label_);
}

namespace {
// Counts evaluations and how many results are alive at once. Throws rather than evaluate without limit.
struct BottomUpProbe {
    static constexpr size_t max_evaluations = 100'000;
    size_t evaluations{0};
    std::shared_ptr<int> token{std::make_shared<int>(0)};
    size_t peak_alive{0};

    std::shared_ptr<int> result() {
        if (++evaluations > max_evaluations) {
            throw std::runtime_error("Too many evaluations");
        }
        peak_alive = std::max(peak_alive, static_cast<size_t>(token.use_count()));
        return token;
    }

    std::shared_ptr<int> run(const arcticdb::ExpressionNode& root) {
        using namespace arcticdb;
        return evaluate_bottom_up<std::shared_ptr<int>>(
                root,
                [this](const ExpressionNode::Leaf&) { return result(); },
                [](const ExpressionNode&) -> std::optional<std::shared_ptr<int>> { return std::nullopt; },
                [this](const ExpressionNode&, const ExpressionNode::Operation&, const auto&) { return result(); }
        );
    }
};
} // namespace

TEST(ExpressionNode, BottomUpEvaluationFreesIntermediateResultsOfAChain) {
    using namespace arcticdb;
    auto root = col("thing1");
    constexpr size_t depth = 1'000;
    for (size_t i = 0; i < depth; ++i) {
        root = node(std::move(root), OperationType::ABS);
    }
    BottomUpProbe probe;
    probe.run(*root);
    ASSERT_EQ(probe.evaluations, depth + 1);
    ASSERT_LE(probe.peak_alive, 3);
}

TEST(ExpressionNode, BottomUpEvaluationVisitsEachNodeOfASharedDagOnce) {
    using namespace arcticdb;
    auto root = col("thing1");
    constexpr size_t depth = 64;
    for (size_t i = 0; i < depth; ++i) {
        root = node(root, root, OperationType::ADD);
    }
    BottomUpProbe probe;
    probe.run(*root);
    ASSERT_EQ(probe.evaluations, depth + 1);
    ASSERT_LE(probe.peak_alive, 3);
}
