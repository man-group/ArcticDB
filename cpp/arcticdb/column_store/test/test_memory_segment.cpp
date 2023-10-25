/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <gtest/gtest.h>

#include <arcticdb/column_store/memory_segment.hpp>
#include <arcticdb/util/test/test_utils.hpp>
#include <arcticdb/stream/test/stream_test_common.hpp>
#include <arcticdb/util/test/generators.hpp>
#include <arcticdb/util/bitset.hpp>

#include <folly/container/Enumerate.h>

#include <cstdint>
#include <algorithm>

TEST(MemSegment, Empty) {
    using namespace arcticdb;

    SegmentInMemory s;
    ASSERT_EQ(s.row_count(), 0);
    ASSERT_EQ(s.num_columns(), 0);
    ASSERT_THROW(s.column(12), ArcticCategorizedException<ErrorCategory::INTERNAL>);
    //  ASSERT_THROW(s.scalar_at<uint16_t>(3, 6), std::invalid_argument);
    ASSERT_NO_THROW(s.clear());
    // Even though this index is out of bounds it will not throw as there are no columns to visit
//    ASSERT_NO_THROW(s.visit(5, [] (auto&& ) { std::cout << "testing" << std::endl;}));
}

template<typename TDT>
void test_segment_type(size_t num_values = 20, size_t num_tests = 50, size_t num_columns = 4) {
    using namespace arcticdb;
    using TypeDescriptorTag = TDT;
    using DTT = typename TypeDescriptorTag::DataTypeTag;
    using raw_type = typename DTT::raw_type;
    const Dimension dimensions = TDT::DimensionTag::value;

    TypeDescriptorTag typeDescriptorTag;
    const auto tsd = create_tsd<DTT, Dimension::Dim0>();
    SegmentInMemory s(StreamDescriptor{std::move(tsd)});
    for (size_t i = 0; i < num_tests; ++i) {
        auto ts = timestamp(i);
        TestRow<decltype(typeDescriptorTag)> test_row{ts, num_columns, raw_type(i), num_values};
        s.set_scalar(0, ts);
        for (size_t j = 1; j < num_columns; ++j) {
            if constexpr(dimensions == Dimension::Dim0) {
                auto v = test_row[j - 1].get_scalar();
                s.set_scalar(j, v);
            } else {
                auto t = test_row[j - 1].get_tensor();
                s.set_array(j, t);
            }
        }

        s.end_row();
        ASSERT_EQ(s.num_columns(), num_columns + 1); // i.e. including timestamp
    }

    for (size_t i = 0; i < num_tests; ++i) {
        auto ts = timestamp(i);
        TestRow<decltype(typeDescriptorTag)> test_row{ts, num_columns, raw_type(i)};
        ASSERT_EQ(s.scalar_at<timestamp>(i, 0), i);
        for (size_t j = 1; j < num_columns; ++j) {
            if constexpr  (dimensions == Dimension::Dim0) {
                auto v = s.scalar_at<raw_type>(i, j);
                ASSERT_FALSE(v == std::nullopt);
                ASSERT_EQ(v.value(), test_row[j - 1].get_scalar());
            } else {
                auto t = s.tensor_at<raw_type>(i, j);
                ASSERT_FALSE(t = std::nullopt);
                ASSERT_TRUE(test_row.check(j - 1, t.value()));
            }
        }
    }
}

TEST(MemSegment, Scalar) {
    test_segment_type<TypeDescriptorTag<DataTypeTag<DataType::UINT8>, DimensionTag<Dimension::Dim0>>>();
    test_segment_type<TypeDescriptorTag<DataTypeTag<DataType::UINT16>, DimensionTag<Dimension::Dim0>>>();
    test_segment_type<TypeDescriptorTag<DataTypeTag<DataType::UINT32>, DimensionTag<Dimension::Dim0>>>();
    test_segment_type<TypeDescriptorTag<DataTypeTag<DataType::UINT64>, DimensionTag<Dimension::Dim0>>>();
    test_segment_type<TypeDescriptorTag<DataTypeTag<DataType::INT8>, DimensionTag<Dimension::Dim0>>>();
    test_segment_type<TypeDescriptorTag<DataTypeTag<DataType::INT16>, DimensionTag<Dimension::Dim0>>>();
    test_segment_type<TypeDescriptorTag<DataTypeTag<DataType::INT32>, DimensionTag<Dimension::Dim0>>>();
    test_segment_type<TypeDescriptorTag<DataTypeTag<DataType::INT64>, DimensionTag<Dimension::Dim0>>>();
    test_segment_type<TypeDescriptorTag<DataTypeTag<DataType::FLOAT32>, DimensionTag<Dimension::Dim0>>>();
    test_segment_type<TypeDescriptorTag<DataTypeTag<DataType::FLOAT64>, DimensionTag<Dimension::Dim0>>>();
}

TEST(MemSegment, Iteration) {
    auto frame_wrapper = get_test_timeseries_frame("test_iteration", 100, 0);
    auto& segment = frame_wrapper.segment_;

    auto count ARCTICDB_UNUSED = 0u;
    for(auto it = segment.begin(); it != segment.end(); ++it) {
        ASSERT_EQ(it->row_id_, count++);
    }
    ASSERT_EQ(count, 100);
}

TEST(MemSegment, IterateAndGetValues) {
    auto frame_wrapper = get_test_timeseries_frame("test_get_values", 100, 0);
    auto& segment = frame_wrapper.segment_;

    for( auto row : folly::enumerate(segment)) {
        for(auto value : folly::enumerate(*row)) {
            value->visit([&] (const auto& val) {
                using ValType = std::decay_t<decltype(val)>;
                if( value.index == 0) {
                    ASSERT_EQ(static_cast<ValType>(row.index), val);
                }
                else {
                    if constexpr(std::is_integral_v<ValType>) {
                        ASSERT_EQ(val, get_integral_value_for_offset<ValType>(0, row.index));
                    }
                    if constexpr (std::is_floating_point_v<ValType>) {
                        ASSERT_EQ(val, get_floating_point_value_for_offset<ValType>(0, row.index));
                    }
                }
            });
        }
    }
}

TEST(MemSegment, IterateWithEmptyTypeColumn) {
    SegmentInMemory seg;
    size_t num_rows = 10;
    auto int_column = std::make_shared<Column>(generate_int_column(num_rows));
    seg.add_column(scalar_field(int_column->type().data_type(), "int_column"), int_column);

    auto empty_column = std::make_shared<Column>(generate_empty_column());
    seg.add_column(scalar_field(empty_column->type().data_type(), "empty_column"), empty_column);
    seg.set_row_id(num_rows - 1);
    for (auto&& [idx, row]: folly::enumerate(seg)) {
        ASSERT_EQ(static_cast<int64_t>(idx), row.scalar_at<int64_t>(0));
        // Exception should be thrown regardless of the type requested for empty type columns
        EXPECT_THROW(row.scalar_at<int64_t>(1).has_value(), InternalException);
        EXPECT_THROW(row.scalar_at<float>(1).has_value(), InternalException);
        EXPECT_THROW(row.scalar_at<uint8_t>(1).has_value(), InternalException);
    }
}

TEST(MemSegment, CopyViaIterator) {
    auto frame_wrapper = get_test_timeseries_frame("test_get_values", 100, 0);
    auto& source =frame_wrapper.segment_;
    auto target = get_test_empty_timeseries_segment("to_sort", 0u);
    std::copy(std::begin(source), std::end(source), std::back_inserter(target));

    for( auto row : folly::enumerate(target)) {
        for(auto value : folly::enumerate(*row)) {
            value->visit([&] (const auto& val) {
                using ValType = std::decay_t<decltype(val)>;
                if( value.index == 0) {
                    ASSERT_EQ(static_cast<ValType>(row.index), val);
                }
                else {
                    if constexpr(std::is_integral_v<ValType>) {
                        ASSERT_EQ(val, get_integral_value_for_offset<ValType>(0, row.index));
                    }
                    if constexpr (std::is_floating_point_v<ValType>) {
                        ASSERT_EQ(val, get_floating_point_value_for_offset<ValType>(0, row.index));
                    }
                }
            });
        }
    }
}

TEST(MemSegment, ModifyViaIterator) {
    auto num_rows = 100u;
    auto frame_wrapper = get_test_timeseries_frame("modify", num_rows, 0);
    auto &segment = frame_wrapper.segment_;
    for (auto &row : segment) {
        for (auto &value : row) {
            value.visit([](auto &v) { v += 1; });
        }
    }

    for (auto row : folly::enumerate(segment)) {
        for (auto value : folly::enumerate(*row)) {
            value->visit([&](const auto &val) {
                using ValType = std::decay_t<decltype(val)>;
                if (value.index == 0) {
                    ASSERT_EQ(static_cast<ValType>(row.index + 1), val);
                } else {
                    if constexpr(std::is_integral_v<ValType>) {
                        ASSERT_EQ(val, get_integral_value_for_offset<ValType>(0, row.index) + 1);
                    }
                    if constexpr (std::is_floating_point_v<ValType>) {
                        ASSERT_EQ(val, get_floating_point_value_for_offset<ValType>(0, row.index) + 1);
                    }
                }
            });
        }
    }
}

TEST(MemSegment, StdFindIf) {
    auto num_rows = 100u;
    auto frame_wrapper = get_test_timeseries_frame("modify", num_rows, 0);
    auto &segment = frame_wrapper.segment_;
    auto it = std::find_if(std::begin(segment), std::end(segment), [] (auto& row) { return row.template index<TimeseriesIndex>() == 50; });
    auto val_it = it->begin();
    ASSERT_EQ(it->index<TimeseriesIndex>(), 50);
    std::advance(val_it, 1);
    ASSERT_EQ(val_it->value<uint8_t>(), get_integral_value_for_offset<uint8_t>(0, 50));
}

TEST(MemSegment, LowerBound) {
    auto num_rows = 100u;
    auto frame_wrapper = get_test_timeseries_frame("modify", num_rows, 0);
    auto &segment = frame_wrapper.segment_;
    auto odds_segment = SegmentInMemory{segment.descriptor(), num_rows / 2};
    std::copy_if(std::begin(segment), std::end(segment), std::back_inserter(odds_segment), [](auto& row) {
        return row.template index<TimeseriesIndex>() & 1;
    });
    auto lb = std::lower_bound(std::begin(odds_segment), std::end(odds_segment), timestamp(50), [] (auto& row, timestamp t) {return row.template index<TimeseriesIndex>() < t; });
    ASSERT_EQ(lb->index<TimeseriesIndex>(), 51);
}

TEST(MemSegment, CloneAndCompare) {
    auto segment = get_standard_timeseries_segment("test_clone");
    auto copied = segment.clone();
    bool equal = segment == copied;
    ASSERT_EQ(equal, true);
}

TEST(MemSegment, CloneAndCompareWithEmptyTypeColumn) {
    SegmentInMemory seg;
    size_t num_rows = 10;
    auto int_column = std::make_shared<Column>(generate_int_column(num_rows));
    seg.add_column(scalar_field(int_column->type().data_type(), "int_column"), int_column);

    auto empty_column = std::make_shared<Column>(generate_empty_column());
    seg.add_column(scalar_field(empty_column->type().data_type(), "empty_column"), empty_column);
    seg.set_row_id(num_rows - 1);
    auto copied = seg.clone();
    bool equal = seg == copied;
    ASSERT_TRUE(equal);
}

TEST(MemSegment, SplitSegment) {
    auto segment = get_standard_timeseries_segment("test_clone", 100);
    auto split_segs = segment.split(10);

    for(const auto& split : split_segs)
        ASSERT_EQ(split.row_count(), 10);

    for(auto i = 0u; i < 100; ++i) {
        ASSERT_EQ(split_segs[i / 10].scalar_at<int8_t>(i % 10, 1), segment.scalar_at<int8_t>(i, 1));
        ASSERT_EQ(split_segs[i / 10].scalar_at<uint64_t>(i % 10, 2), segment.scalar_at<uint64_t>(i, 2));
        ASSERT_EQ(split_segs[i / 10].string_at(i % 10, 3), segment.string_at(i, 3));
    }
}

TEST(MemSegment, SplitSparseSegment) {
    using namespace arcticdb;
    using namespace arcticdb::stream;
    using DynamicAggregator =  Aggregator<TimeseriesIndex, DynamicSchema, stream::RowCountSegmentPolicy, stream::SparseColumnPolicy>;

    const std::string stream_id("test_sparse");

    const auto index = TimeseriesIndex::default_index();
    DynamicSchema schema{
        index.create_stream_descriptor(stream_id, {}), index
    };
    SegmentInMemory sparse_segment;
    DynamicAggregator aggregator(std::move(schema), [&](SegmentInMemory &&segment) {
            sparse_segment = std::move(segment);
        }, RowCountSegmentPolicy{});

    constexpr timestamp num_rows = 100;

    for(timestamp i = 0; i < num_rows; i ++) {
        aggregator.start_row(timestamp{i})([&](auto& rb) {
            rb.set_scalar_by_name("first", uint32_t(i * 2), DataType::UINT32);
            rb.set_scalar_by_name("third", uint64_t(i * 4), DataType::UINT64);
            if (i%4 == 0) {
                rb.set_scalar_by_name("second", uint64_t(i * 3), DataType::UINT64);
            }
            if (i%4 == 2) {
                rb.set_scalar_by_name("strings", std::string_view{"keep_me" + std::to_string(i)},
                                      DataType::ASCII_DYNAMIC64);
            }
        });
    }

    aggregator.commit();

    auto split_segs = sparse_segment.split(10);

    for(const auto& split : split_segs)
        ASSERT_EQ(split.row_count(), 10);

    for(auto i = 0u; i < 100; ++i) {
        ASSERT_EQ(split_segs[i / 10].scalar_at<uint32_t>(i % 10, 1), sparse_segment.scalar_at<uint32_t>(i, 1));
        ASSERT_EQ(split_segs[i / 10].scalar_at<uint64_t>(i % 10, 2), sparse_segment.scalar_at<uint64_t>(i, 2));
        if (i % 4 == 0) {
            ASSERT_EQ(split_segs[i / 10].scalar_at<uint64_t>(i % 10, 3), sparse_segment.scalar_at<uint64_t>(i, 3));
        } else {
            ASSERT_FALSE(static_cast<bool>(split_segs[i / 10].scalar_at<uint64_t>(i % 10, 3)));
        }
        if (i % 4 == 2) {
            ASSERT_EQ(split_segs[i / 10].string_at(i % 10, 4), sparse_segment.string_at(i, 4));
        } else {
            ASSERT_FALSE(static_cast<bool>(split_segs[i / 10].string_at(i % 10, 4)));
        }

    }
}

TEST(MemSegment, ShuffleAndSort) {
    auto segment = get_standard_timeseries_segment("test_clone");
    std::random_device rng;
    std::mt19937 urng(rng());
    auto copied = segment.clone();
    std::shuffle(segment.begin(), segment.end(), urng);
    ASSERT_EQ(segment.is_index_sorted(), false);
    segment.sort("time");
    bool equal = segment == copied;
    ASSERT_EQ(equal, true);
}

TEST(MemSegment, ShuffleAndSortStress) {
    auto segment = get_standard_timeseries_segment("test_clone", 100000);
    std::random_device rng;
    std::mt19937 urng(rng());
    auto copied = segment.clone();
    std::shuffle(segment.begin(), segment.end(), urng);
    ASSERT_EQ(segment.is_index_sorted(), false);
    segment.sort("time");
    bool equal = segment == copied;
    ASSERT_EQ(equal, true);
}

TEST(MemSegment, SortSorted) {
    auto segment = get_standard_timeseries_segment("test_clone");
    auto copied = segment.clone();
    segment.sort("time");
    bool equal = segment == copied;
    ASSERT_EQ(equal, true);
}

TEST(MemSegment, DoubleReverse) {
    auto segment = get_standard_timeseries_segment("test_clone");
    auto copied = segment.clone();
    ASSERT_EQ(segment.is_index_sorted(), true);
    std::reverse(segment.begin(), segment.end());
    ASSERT_EQ(segment.is_index_sorted(), false);
    std::reverse(segment.begin(), segment.end());
    bool equal = segment == copied;
    ASSERT_EQ(equal, true);
}

TEST(MemSegment, SortReversed) {
    auto segment = get_standard_timeseries_segment("test_clone");
    auto copied = segment.clone();
    std::reverse(segment.begin(), segment.end());
    ASSERT_EQ(segment.is_index_sorted(), false);
    segment.sort("time");
    bool equal = segment == copied;
    ASSERT_EQ(equal, true);
}

TEST(MemSegment, SparsifyUnsparsify) {
    auto segment = get_sparse_timeseries_segment_floats("test_shuffle_sparse");
    auto copied = segment.clone();
    segment.unsparsify();
    segment.sparsify();
    bool equals = segment == copied;
    ASSERT_EQ(equals, true);
}

TEST(MemSegment, ShuffleSparse) {
    auto segment = get_sparse_timeseries_segment_floats("test_shuffle_sparse");
    auto copied = segment.clone();
    std::random_device rng;
    std::mt19937 urng(rng());
    segment.unsparsify();
    std::shuffle(std::begin(segment), std::end(segment), urng);
    segment.sparsify();
    ASSERT_EQ(segment.is_index_sorted(), false);
}

TEST(MemSegment, ShuffleAndSortSparse) {
    auto segment = get_sparse_timeseries_segment_floats("test_shuffle_sparse");
    auto copied = segment.clone();
    std::random_device rng;
    std::mt19937 urng(rng());
    segment.unsparsify();
    std::shuffle(std::begin(segment), std::end(segment), urng);
    segment.sparsify();
    ASSERT_FALSE(segment.is_index_sorted());
    segment.sort("time");
    bool equal = segment == copied;
    ASSERT_EQ(equal, true);
}

TEST(MemSegment, Append) {
    StreamDescriptor descriptor{stream_descriptor(StreamId("test"), TimeseriesIndex::default_index(), {
        scalar_field(DataType::UINT8, "thing2"),
        scalar_field(DataType::UINT32, "thing3")
    })};
    SegmentInMemory s1(descriptor);
    for(int row = 0; row < 10; ++row) {
        for(int col = 0; col < 3; ++col) {
            switch(col) {
                case 0:
                    s1.set_scalar<timestamp>(col, row + col);
                    break;
                case 1:
                    s1.set_scalar<uint8_t>(col, row + col);
                    break;
                case 2:
                    s1.set_scalar<uint32_t>(col, row + col);
                    break;
                default:
                    break;
            }

        }
        s1.end_row();
    }

    SegmentInMemory s2(descriptor);
    for(int row = 10; row < 20; ++row) {
        for(int col = 0; col < 3; ++col) {
            switch(col) {
                case 0:
                    s2.set_scalar<timestamp>(col, row + col);
                    break;
                case 1:
                    s2.set_scalar<uint8_t>(col, row + col);
                    break;
                case 2:
                    s2.set_scalar<uint32_t>(col, row + col);
                    break;
                default:
                    break;
            }
        }
        s2.end_row();
    }

    s1.append(s2);
    for(int row = 0; row < 20; ++row) {
        ASSERT_EQ(s1.scalar_at<timestamp>(row, 0).value(), row);
    }
    for(int row = 0; row < 20; ++row) {
        ASSERT_EQ(s1.scalar_at<uint8_t>(row, 1).value(), row + 1);
    }
    for(int row = 0; row < 20; ++row) {
        ASSERT_EQ(s1.scalar_at<uint32_t>(row, 2).value(), row + 2);
    }
}

TEST(MemSegment, Filter) {
    SegmentInMemory seg;
    size_t num_rows = 10;
    auto int_column = std::make_shared<Column>(generate_int_column(num_rows));
    seg.add_column(scalar_field(int_column->type().data_type(), "int_column"), int_column);

    auto empty_column = std::make_shared<Column>(generate_empty_column());
    seg.add_column(scalar_field(empty_column->type().data_type(), "empty_column"), empty_column);
    seg.set_row_id(num_rows - 1);

    util::BitSet filter_bitset(num_rows);
    std::vector<size_t> retained_rows{0, 4, num_rows - 1};
    for (auto retained_row: retained_rows) {
        filter_bitset.set_bit(retained_row);
    }

    auto filtered_seg = seg.filter(filter_bitset);

    for (auto&& [idx, row]: folly::enumerate(filtered_seg)) {
        ASSERT_EQ(static_cast<int64_t>(retained_rows[idx]), row.scalar_at<int64_t>(0));
        // Exception should be thrown regardless of the type requested for empty type columns
        EXPECT_THROW(row.scalar_at<int64_t>(1).has_value(), InternalException);
        EXPECT_THROW(row.scalar_at<float>(1).has_value(), InternalException);
        EXPECT_THROW(row.scalar_at<uint8_t>(1).has_value(), InternalException);
    }
}

