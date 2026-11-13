/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <gtest/gtest.h>

#include <random>

#include <sparrow/record_batch.hpp>

#include <arcticdb/pipeline/column_mapping.hpp>
#include <arcticdb/stream/test/stream_test_common.hpp>
#include <arcticdb/arrow/test/arrow_test_utils.hpp>
#include <arcticdb/arrow/arrow_utils.hpp>
#include <arcticdb/arrow/arrow_handlers.hpp>
#include <arcticdb/util/allocator.hpp>
#include <arcticdb/util/constants.hpp>

using namespace arcticdb;

SegmentInMemory get_detachable_segment(
        std::span<const FieldRef> fields, size_t num_rows, size_t chunk_size, const ReadOptions& read_options
) {
    auto desc = get_test_descriptor<stream::TimeseriesIndex>("symbol", fields);
    auto& fields_with_index = desc.fields();
    auto num_columns = fields_with_index.size();
    auto handler = ArrowStringHandler();
    auto segment = SegmentInMemory();
    for (auto& field : fields_with_index) {
        DetachableBlockConfig block_config = detachable_block_config::Regular{0};
        if (is_sequence_type(field.type().data_type())) {
            auto [type, config] = handler.output_type_and_block_config(field.type(), field.name(), read_options);
            field.type_ = type;
            block_config = config;
        }
        segment.add_column(
                field,
                std::make_shared<Column>(
                        field.type(), 0, AllocationType::DETACHABLE, Sparsity::NOT_PERMITTED, block_config
                )
        );
    }

    for (auto i = 0u; i < num_columns; ++i) {
        auto& column = segment.column(i);
        details::visit_scalar(column.type(), [&column, &num_rows, &chunk_size](auto&& impl) {
            using TagType = std::decay_t<decltype(impl)>;
            using RawType = typename TagType::DataTypeTag::raw_type;
            allocate_and_fill_chunked_column<RawType>(column, num_rows, chunk_size);
        });
    }

    return segment;
}

namespace {
const auto kDynamicStringType = TypeDescriptor{DataType::UTF_DYNAMIC64, Dimension::Dim0};

// Converts a UTF_DYNAMIC64 source segment into dest's block starting at frame row first_row, as an Arrow read does
void convert_string_segment(
        const Column& source, Column& dest, std::string_view col_name, size_t first_row,
        const std::shared_ptr<StringPool>& string_pool, const ReadOptions& read_options, ColumnTruncation truncate = {}
) {
    const auto dest_size = data_type_size(dest.type());
    // Rows, not row_count(), which for a sparse column counts only the stored values
    const auto num_rows = static_cast<size_t>(source.last_row() + 1);
    auto field_wrapper = FieldWrapper(dest.type(), col_name);
    auto mapping = ColumnMapping(
            source.type(),
            dest.type(),
            field_wrapper.field(),
            dest_size,
            num_rows,
            first_row,
            dest_size * first_row,
            dest_size * num_rows,
            0
    );
    mapping.set_truncate(truncate);
    auto handler_data = std::any{};
    ArrowStringHandler().convert_type(source, dest, mapping, DecodePathData{}, handler_data, string_pool, read_options);
}

} // namespace

// Populates a column with strings as if using OutputFormat::ARROW. Assumes column is already allocated.
void fill_chunked_string_column(
        Column& column, std::string_view col_name, size_t num_rows, size_t chunk_size,
        std::shared_ptr<StringPool> string_pool, const std::vector<std::string>& values, const ReadOptions& read_options
) {
    for (size_t first_row = 0; first_row < num_rows; first_row += chunk_size) {
        auto source = Column(kDynamicStringType, 0, AllocationType::DYNAMIC, Sparsity::NOT_PERMITTED);
        for (auto row = first_row; row < std::min(first_row + chunk_size, num_rows); ++row)
            source.push_back(string_pool->get(values[row]).offset());
        convert_string_segment(source, column, col_name, first_row, string_pool, read_options);
    }
}

TEST(ColumnMetadata, NonArrow) {
    proto::descriptors::NormalizationMetadata norm_meta;
    // Accessing like this sets the oneof to df
    norm_meta.mutable_df();
    ASSERT_FALSE(column_metadata(norm_meta, "col").has_value());
}

TEST(ColumnMetadata, MissingColumn) {
    proto::descriptors::NormalizationMetadata norm_meta;
    (*norm_meta.mutable_arrow()->mutable_columns())["col"] =
            proto::descriptors::NormalizationMetadata::Arrow::ColumnMeta();
    ASSERT_FALSE(column_metadata(norm_meta, "missing col").has_value());
}

TEST(ColumnMetadata, PresentColumn) {
    std::string tz{"Europe/Brussels"};
    proto::descriptors::NormalizationMetadata norm_meta;
    proto::descriptors::NormalizationMetadata::Arrow::ColumnMeta col_meta;
    col_meta.set_timezone(tz);
    (*norm_meta.mutable_arrow()->mutable_columns())["col"] = col_meta;
    auto new_col_meta = column_metadata(norm_meta, "col");
    ASSERT_TRUE(new_col_meta.has_value());
    ASSERT_TRUE(new_col_meta->has_timezone());
    ASSERT_EQ(new_col_meta->timezone(), tz);
}

TEST(ArrowRead, ZeroCopy) {
    size_t num_rows{10};
    uint8_t* data_ptr = get_detachable_allocator().allocate(sizeof(uint64_t) * num_rows);
    auto typed_ptr = reinterpret_cast<uint64_t*>(data_ptr);
    for (size_t idx = 0; idx < num_rows; ++idx) {
        typed_ptr[idx] = idx;
    }
    sparrow::u8_buffer<uint64_t> u8_buffer(typed_ptr, num_rows, get_detachable_allocator());
    sparrow::primitive_array<uint64_t> primitive_array(std::move(u8_buffer), num_rows);
    sparrow::array array{std::move(primitive_array)};
    auto arrow_structures = sparrow::get_arrow_structures(array);
    auto arrow_array_buffers = sparrow::get_arrow_array_buffers(*arrow_structures.first, *arrow_structures.second);
    const auto* roundtripped_ptr = reinterpret_cast<uint64_t*>(arrow_array_buffers.at(1).data<uint8_t>());
    // Verify zero-copy: the roundtripped pointer should be the same as the original
    ASSERT_EQ(roundtripped_ptr, typed_ptr);
    // Verify data integrity through the roundtripped pointer
    for (size_t idx = 0; idx < num_rows; ++idx) {
        ASSERT_EQ(roundtripped_ptr[idx], idx);
    }
}

TEST(ArrowRead, ColumnBasic) {
    const size_t num_rows = 100;
    const size_t chunk_size = 5;
    const size_t num_chunks = num_rows / chunk_size;
    auto column = Column(
            TypeDescriptor{DataType::FLOAT32, Dimension::Dim0}, 0, AllocationType::DETACHABLE, Sparsity::NOT_PERMITTED
    );
    allocate_and_fill_chunked_column<float>(column, num_rows, chunk_size);
    auto arrow_arrays = arrow_arrays_from_column(column, "col");
    EXPECT_EQ(arrow_arrays.size(), num_chunks);
    for (const auto& arr : arrow_arrays) {
        EXPECT_EQ(arr.name(), "col");
    }
    for (auto row = 0u; row < num_rows; ++row) {
        auto chunk = row / chunk_size;
        auto pos = row % chunk_size;
        EXPECT_EQ(std::get<sparrow::nullable<const float&>>(arrow_arrays[chunk][pos]).get(), static_cast<float>(row));
    }
}

class ArrowStringColumnRead : public testing::TestWithParam<ArrowOutputStringFormat> {
  public:
    ArrowOutputStringFormat output_string_format() { return GetParam(); }
};

TEST_P(ArrowStringColumnRead, Basic) {
    const size_t num_rows = 100;
    const size_t chunk_size = 5;
    const size_t num_chunks = num_rows / chunk_size;
    const std::vector<std::string> strings = {"test", "strings", "available", "!"};

    std::vector<std::string> column_values;
    column_values.reserve(num_rows);
    for (auto i = 0u; i < num_rows; ++i) {
        column_values.push_back(strings[i % strings.size()]);
    }

    // Populate string pool and column using arrow string handler
    auto pool = std::make_shared<StringPool>();
    auto handler = ArrowStringHandler();
    auto column_name = "col";
    auto read_options = ReadOptions();
    read_options.set_output_config(ArrowOutputConfig{.default_string_format_ = output_string_format()});
    auto [type_desc, block_config] = handler.output_type_and_block_config(
            TypeDescriptor{DataType::UTF_DYNAMIC64, Dimension::Dim0}, column_name, read_options
    );
    auto column = Column(type_desc, 0, AllocationType::DETACHABLE, Sparsity::NOT_PERMITTED, block_config);
    allocate_chunked_column(column, num_rows, chunk_size);
    fill_chunked_string_column(column, column_name, num_rows, chunk_size, pool, column_values, read_options);

    // Verify applying the string handler sets the correct external buffers
    for (auto chunk = 0u; chunk < num_chunks; ++chunk) {
        auto dest_size = data_type_size(type_desc);
        if (output_string_format() == ArrowOutputStringFormat::CATEGORICAL) {
            EXPECT_EQ(dest_size, 4); // We should be using 32-bit integer keys for categorical arrow

            // We should have attached offsets and string buffers to the corresponding offsets.
            auto offset = chunk * chunk_size * dest_size;
            EXPECT_TRUE(column.has_extra_buffer(offset, ExtraBufferType::OFFSET));
            EXPECT_TRUE(column.has_extra_buffer(offset, ExtraBufferType::STRING));
            auto& column_buffer = column.buffer();
            auto& offset_buffer = column.get_extra_buffer(offset, ExtraBufferType::OFFSET);
            auto& string_buffer = column.get_extra_buffer(offset, ExtraBufferType::STRING);

            for (auto row_in_chunk = 0u; row_in_chunk < chunk_size; ++row_in_chunk) {
                auto global_row = chunk * chunk_size + row_in_chunk;
                auto id = column_buffer.cast<int32_t>(global_row);
                auto offset_begin = offset_buffer.cast<int64_t>(id);
                auto str_size = offset_buffer.cast<int64_t>(id + 1) - offset_begin;
                auto str_in_column = std::string_view(
                        reinterpret_cast<char*>(string_buffer.bytes_at(offset_begin, str_size)), str_size
                );
                EXPECT_EQ(str_in_column, column_values[global_row]);
            }
        } else {
            // We should be using 64-bit offsets for LARGE_STRING and 32-bit offsets for SMALL_STRING
            details::visit_scalar(column.type(), [&](auto&& impl) {
                using TagType = std::decay_t<decltype(impl)>;
                if constexpr (is_sequence_type(TagType::DataTypeTag::data_type)) {
                    using RawType = typename TagType::DataTypeTag::raw_type;
                    auto expected_dest_size = output_string_format() == ArrowOutputStringFormat::LARGE_STRING ? 8 : 4;
                    EXPECT_EQ(dest_size, expected_dest_size);
                    EXPECT_EQ(sizeof(RawType), expected_dest_size);

                    // We should have attached only a string buffer to the corresponding offset because we're storing
                    // the offsets in the column buffer.
                    auto offset = chunk * chunk_size * dest_size;
                    EXPECT_FALSE(column.has_extra_buffer(offset, ExtraBufferType::OFFSET));
                    EXPECT_TRUE(column.has_extra_buffer(offset, ExtraBufferType::STRING));
                    auto& column_buffer = column.buffer();
                    auto column_chunk_ptr = reinterpret_cast<RawType*>(
                            column_buffer.bytes_at(chunk * chunk_size * dest_size, chunk_size * dest_size)
                    );
                    auto& string_buffer = column.get_extra_buffer(offset, ExtraBufferType::STRING);

                    for (auto row_in_chunk = 0u; row_in_chunk < chunk_size; ++row_in_chunk) {
                        auto global_row = chunk * chunk_size + row_in_chunk;
                        auto offset_begin = column_chunk_ptr[row_in_chunk];
                        auto str_size = column_chunk_ptr[row_in_chunk + 1] - offset_begin;
                        auto str_in_column = std::string_view(
                                reinterpret_cast<char*>(string_buffer.bytes_at(offset_begin, str_size)), str_size
                        );
                        EXPECT_EQ(str_in_column, column_values[global_row]);
                    }
                } else {
                    util::raise_rte("Unexpected non-string type");
                }
            });
        }
    }

    // Convert to arrow arrays
    auto arrow_arrays = arrow_arrays_from_column(column, column_name);

    // Verify the dict arrays
    EXPECT_EQ(arrow_arrays.size(), num_chunks);
    for (const auto& arr : arrow_arrays) {
        EXPECT_EQ(arr.name(), column_name);
    }
    for (auto row = 0u; row < num_rows; ++row) {
        auto chunk = row / chunk_size;
        auto pos = row % chunk_size;
        auto value = arrow_arrays[chunk][pos];
        EXPECT_TRUE(value.has_value());
        EXPECT_EQ(std::get<sparrow::nullable<std::string_view>>(value).get(), column_values[row]);
    }
}

namespace {
constexpr size_t kBitMagicBlockBits = bm::gap_max_bits;
// Every this-many-th stored value is None rather than a string
constexpr size_t kNoneEvery = 37;
// Rows read back as "s<row % kDistinctStrings>", so the dictionary has repeats
constexpr size_t kDistinctStrings = 211;
constexpr size_t kMaxRunLength = 5'000;

// Decides row by row whether each row is stored, with probability `density`: independently (mostly BitMagic bit blocks
// at mid densities), or in runs of up to kMaxRunLength rows (GAP and full blocks)
class StoredRowPattern {
  public:
    StoredRowPattern(double density, bool runs, std::mt19937& rng) :
        stored_(density),
        run_length_(1, kMaxRunLength),
        runs_(runs),
        rng_(rng) {}

    bool next_row_is_stored() {
        if (!runs_)
            return stored_(rng_);
        if (run_left_-- == 0) {
            in_stored_run_ = stored_(rng_);
            run_left_ = run_length_(rng_);
        }
        return in_stored_run_;
    }

  private:
    std::bernoulli_distribution stored_;
    std::uniform_int_distribution<size_t> run_length_;
    bool runs_;
    std::mt19937& rng_;
    bool in_stored_run_ = false;
    // Rows left in the current run; the first row starts a new one
    size_t run_left_ = 0;
};

// A sparse dynamic-string column of num_rows rows, stored as StoredRowPattern decides, with every kNoneEvery-th stored
// value None. Returns the column and the value each row should read back as
std::pair<Column, std::vector<std::optional<std::string>>> sparse_string_column(
        size_t num_rows, double density, bool runs, StringPool& pool, std::mt19937& rng
) {
    auto column = Column(kDynamicStringType, 0, AllocationType::DYNAMIC, Sparsity::PERMITTED);
    std::vector<std::optional<std::string>> expected(num_rows);
    StoredRowPattern pattern(density, runs, rng);
    size_t stored_count = 0;
    for (size_t row = 0; row < num_rows; ++row) {
        if (!pattern.next_row_is_stored())
            continue;
        if (stored_count++ % kNoneEvery == kNoneEvery - 1) {
            column.set_scalar(static_cast<ssize_t>(row), not_a_string());
        } else {
            expected[row] = fmt::format("s{}", row % kDistinctStrings);
            column.set_scalar(static_cast<ssize_t>(row), pool.get(expected[row].value()).offset());
        }
    }
    // A segment with no stored values still decodes with a (empty) sparse map
    if (stored_count == 0)
        column.opt_sparse_map() = util::BitMagic();
    // At density 1 every row is stored, so the column has no sparse map and the dense path is tested
    if (column.opt_sparse_map().has_value())
        column.sparse_map().optimize();
    column.set_row_data(static_cast<ssize_t>(num_rows) - 1);
    return {std::move(column), std::move(expected)};
}

// The number of rows in [start, end) whose value in `array` (which starts at row start) differs from expected
size_t count_mismatches(
        const sparrow::array& array, const std::vector<std::optional<std::string>>& expected, size_t start, size_t end
) {
    size_t mismatches = 0;
    for (size_t row = start; row < end; ++row) {
        auto value = array[row - start];
        const bool matches = value.has_value() == expected[row].has_value() &&
                             (!value.has_value() ||
                              std::get<sparrow::nullable<std::string_view>>(value).get() == expected[row].value());
        mismatches += !matches;
    }
    return mismatches;
}

// Converts a dynamic-string column, the only segment of its frame, to Arrow
Column convert_to_arrow(
        const Column& source, const std::shared_ptr<StringPool>& pool, ArrowOutputStringFormat format,
        ColumnTruncation truncation = {}
) {
    const auto num_rows = static_cast<size_t>(source.last_row() + 1);
    auto read_options = ReadOptions();
    read_options.set_output_config(ArrowOutputConfig{.default_string_format_ = format});
    auto [dest_type, block_config] =
            ArrowStringHandler().output_type_and_block_config(kDynamicStringType, "col", read_options);
    auto dest = Column(dest_type, 0, AllocationType::DETACHABLE, Sparsity::NOT_PERMITTED, block_config);
    allocate_chunked_column(dest, num_rows, num_rows);
    convert_string_segment(source, dest, "col", 0, pool, read_options, truncation);
    return dest;
}
} // namespace

TEST_P(ArrowStringColumnRead, SparseColumn) {
    // Reads row ranges of sparse string columns of every density, with the stored rows independent or in runs, and
    // checks each row's value
    const size_t num_rows = 150'000; // Three BitMagic blocks, the last one partial
    std::mt19937 rng(17);
    const std::vector<std::pair<size_t, size_t>> row_ranges{
            {0, num_rows},                      // whole column, no truncation
            {1, num_rows},                      // starts just after the first row
            {63, kBitMagicBlockBits + 65},      // spans a block boundary, unaligned to words
            {64, 128},                          // one 64-bit word
            {70'000, 70'001},                   // one row
            {kBitMagicBlockBits, num_rows - 3}, // starts on a block boundary
            {num_rows - 1'000, num_rows}        // the tail of the partial last block
    };
    for (double density : {0.0, 0.001, 0.01, 0.05, 0.5, 0.95, 1.0}) {
        for (bool runs : {false, true}) {
            auto pool = std::make_shared<StringPool>();
            auto [source, expected] = sparse_string_column(num_rows, density, runs, *pool, rng);
            for (auto [start, end] : row_ranges) {
                SCOPED_TRACE(fmt::format("density {} runs {} rows [{}, {})", density, runs, start, end));
                const bool whole_column = start == 0 && end == num_rows;
                auto dest = convert_to_arrow(
                        source,
                        pool,
                        output_string_format(),
                        whole_column ? ColumnTruncation{} : ColumnTruncation{start, end}
                );
                auto arrays = arrow_arrays_from_column(dest, "col");
                ASSERT_EQ(arrays.size(), 1);
                ASSERT_EQ(arrays[0].size(), end - start);
                EXPECT_EQ(count_mismatches(arrays[0], expected, start, end), 0u);
            }
        }
    }
}

TEST_P(ArrowStringColumnRead, SparseMapWithMoreBitsThanValues) {
    // A corrupt segment: its sparse map claims more stored values than the column holds
    const size_t num_rows = 1'000;
    auto pool = std::make_shared<StringPool>();
    auto source = Column(kDynamicStringType, 0, AllocationType::DYNAMIC, Sparsity::PERMITTED);
    const size_t stored_every = 10;
    for (size_t row = 0; row < num_rows; row += stored_every)
        source.set_scalar(static_cast<ssize_t>(row), pool->get(fmt::format("s{}", row)).offset());
    source.set_row_data(static_cast<ssize_t>(num_rows) - 1);
    // 100 stored values, but the map now claims 550 stored rows
    source.sparse_map().set_range(num_rows / 2, num_rows - 1);
    // Release builds have no iterator bounds check, so the read itself must reject the map
    try {
        convert_to_arrow(source, pool, output_string_format());
        FAIL() << "Expected the read to reject the sparse map";
    } catch (const ArcticException& e) {
        EXPECT_NE(std::string(e.what()).find("more set bits"), std::string::npos) << e.what();
    }
}

namespace {
// Row `row` of a test string column; every third row is missing when sparse
std::optional<std::string> test_string(size_t row, bool sparse) {
    if (sparse && row % 3 == 1)
        return std::nullopt;
    return fmt::format("s{}", row % kDistinctStrings);
}

Column test_string_source_column(StringPool& pool, size_t num_rows, bool sparse) {
    auto column = Column(
            kDynamicStringType, 0, AllocationType::DYNAMIC, sparse ? Sparsity::PERMITTED : Sparsity::NOT_PERMITTED
    );
    for (size_t row = 0; row < num_rows; ++row) {
        if (auto value = test_string(row, sparse))
            column.set_scalar(static_cast<ssize_t>(row), pool.get(*value).offset());
    }
    column.set_row_data(static_cast<ssize_t>(num_rows) - 1);
    return column;
}

} // namespace

TEST_P(ArrowStringColumnRead, EndTruncatedSecondSegment) {
    // The read ends kRowsRead rows into the second segment. The truncation is in frame rows, but the second segment's
    // validity bitmap must cover only kRowsRead rows.
    constexpr size_t kFirstSegmentRows = 40'000;
    constexpr size_t kSecondSegmentRows = 30'000;
    // Not a multiple of 8, so a bitmap sized in frame rows has a different byte count from one sized in segment rows
    constexpr size_t kRowsRead = 10'003;
    const auto read_ends_in_second_segment = ColumnTruncation{std::nullopt, kFirstSegmentRows + kRowsRead};
    auto read_options = ReadOptions();
    read_options.set_output_config(ArrowOutputConfig{.default_string_format_ = output_string_format()});
    auto [dest_type, block_config] =
            ArrowStringHandler().output_type_and_block_config(kDynamicStringType, "col", read_options);
    for (bool sparse : {false, true}) {
        SCOPED_TRACE(fmt::format("sparse {}", sparse));
        auto pool = std::make_shared<StringPool>();
        auto dest = Column(dest_type, 0, AllocationType::DETACHABLE, Sparsity::NOT_PERMITTED, block_config);
        allocate_chunked_column(dest, kFirstSegmentRows + kSecondSegmentRows, kFirstSegmentRows);
        auto first = test_string_source_column(*pool, kFirstSegmentRows, false);
        auto second = test_string_source_column(*pool, kSecondSegmentRows, sparse);
        convert_string_segment(first, dest, "col", 0, pool, read_options);
        convert_string_segment(second, dest, "col", kFirstSegmentRows, pool, read_options, read_ends_in_second_segment);

        const auto bitmap_offset = kFirstSegmentRows * data_type_size(dest_type);
        if (sparse) {
            ASSERT_TRUE(dest.has_extra_buffer(bitmap_offset, ExtraBufferType::BITMAP));
            EXPECT_EQ(
                    dest.get_extra_buffer(bitmap_offset, ExtraBufferType::BITMAP).bytes(),
                    bitset_packed_size_bytes(kRowsRead)
            );
        } else {
            EXPECT_FALSE(dest.has_extra_buffer(bitmap_offset, ExtraBufferType::BITMAP));
        }
        auto arrays = arrow_arrays_from_column(dest, "col");
        ASSERT_EQ(arrays.size(), 2);
        ASSERT_EQ(arrays[1].size(), kRowsRead);
        for (size_t row = 0; row < kRowsRead; ++row) {
            const auto expected = test_string(row, sparse);
            const auto value = arrays[1][row];
            ASSERT_EQ(value.has_value(), expected.has_value()) << "row " << row;
            if (expected)
                ASSERT_EQ(std::get<sparrow::nullable<std::string_view>>(value).get(), *expected) << "row " << row;
        }
    }
}

INSTANTIATE_TEST_SUITE_P(
        AllStringFormats, ArrowStringColumnRead,
        testing::Values(
                ArrowOutputStringFormat::CATEGORICAL, ArrowOutputStringFormat::LARGE_STRING,
                ArrowOutputStringFormat::SMALL_STRING
        )
);

TEST(ArrowRead, ConvertSegmentBasic) {
    const auto num_rows = 100u;
    const auto chunk_size = 10u;
    const auto num_chunks = num_rows / chunk_size;
    const auto fields = std::array{
            scalar_field(DataType::UINT8, "smallints"),
            scalar_field(DataType::INT64, "bigints"),
            scalar_field(DataType::FLOAT64, "floats"),
    };
    auto segment = get_detachable_segment(fields, num_rows, chunk_size, ReadOptions{});
    // Verify the index column has the expected number of chunks
    EXPECT_EQ(segment.column(0).num_blocks(), num_chunks);

    proto::descriptors::NormalizationMetadata norm_meta;
    auto arrow_data = segment_to_arrow_data(segment, norm_meta);
    // We expect to see num_chunks record batches
    EXPECT_EQ(arrow_data.size(), num_chunks);
    for (const auto& record_batch : arrow_data) {
        auto names = record_batch.names();
        auto columns = record_batch.columns();
        // Each record batch should have all columns for the row range (including the index)
        EXPECT_EQ(names.size(), fields.size() + 1);
        EXPECT_EQ(columns.size(), fields.size() + 1);
        EXPECT_EQ(names[0], "time");
        EXPECT_EQ(columns[0].data_type(), sparrow::data_type::TIMESTAMP_NANOSECONDS);
        EXPECT_EQ(names[1], "smallints");
        EXPECT_EQ(columns[1].data_type(), sparrow::data_type::UINT8);
        EXPECT_EQ(names[2], "bigints");
        EXPECT_EQ(columns[2].data_type(), sparrow::data_type::INT64);
        EXPECT_EQ(names[3], "floats");
        EXPECT_EQ(columns[3].data_type(), sparrow::data_type::DOUBLE);
        for (const auto& col : columns) {
            EXPECT_EQ(col.size(), chunk_size);
        }
    }
}

TEST(ArrowRead, ConvertSegmentWithTimezones) {
    const auto num_rows = 100u;
    const auto chunk_size = 10u;
    const auto num_chunks = num_rows / chunk_size;
    const auto fields = std::array{
            scalar_field(DataType::NANOSECONDS_UTC64, "col_with_tz"),
            scalar_field(DataType::NANOSECONDS_UTC64, "col_without_tz"),
    };
    auto segment = get_detachable_segment(fields, num_rows, chunk_size, ReadOptions{});
    // Verify the index column has the expected number of chunks
    EXPECT_EQ(segment.column(0).num_blocks(), num_chunks);

    std::string tz_str{"Europe/Amsterdam"};
    proto::descriptors::NormalizationMetadata norm_meta;
    proto::descriptors::NormalizationMetadata::Arrow::ColumnMeta column_meta;
    column_meta.set_timezone(tz_str);
    (*norm_meta.mutable_arrow()->mutable_columns())["col_with_tz"] = column_meta;
    auto arrow_data = segment_to_arrow_data(segment, norm_meta);
    // We expect to see num_chunks record batches
    EXPECT_EQ(arrow_data.size(), num_chunks);
    for (const auto& record_batch : arrow_data) {
        auto names = record_batch.names();
        auto columns = record_batch.columns();
        // Each record batch should have all columns for the row range (including the index)
        EXPECT_EQ(names.size(), fields.size() + 1);
        EXPECT_EQ(columns.size(), fields.size() + 1);
        EXPECT_EQ(names[0], "time");
        EXPECT_EQ(columns[0].data_type(), sparrow::data_type::TIMESTAMP_NANOSECONDS);
        const date::time_zone* dtz = sparrow::get_timezone(sparrow::detail::array_access::get_arrow_proxy(columns[0]));
        ASSERT_TRUE(dtz == nullptr);
        EXPECT_EQ(names[1], "col_with_tz");
        EXPECT_EQ(columns[1].data_type(), sparrow::data_type::TIMESTAMP_NANOSECONDS);
        const auto& proxy = sparrow::detail::array_access::get_arrow_proxy(columns[1]);
        EXPECT_EQ(std::string{proxy.format()}, std::string{"tsn:"} + tz_str);
        dtz = sparrow::get_timezone(proxy);
        ASSERT_FALSE(dtz == nullptr);
        // The zone's name is OS-dependent for a link like Amsterdam, so check its offsets instead
        const date::sys_days summer{date::year{2025} / 7 / 15};
        EXPECT_EQ(dtz->get_info(summer).offset, std::chrono::hours{2});
        // conda-forge's date ignores the POSIX TZ footer of conda-forge's slim tzdata files, so offsets after the last
        // explicit transition (1996 for Amsterdam) are wrong. ArcticDB stores only the zone name, so Python output
        // read through pyarrow, polars or pandas is unaffected.
#ifndef ARCTICDB_USING_CONDA
        const date::sys_days winter{date::year{2025} / 1 / 15};
        EXPECT_EQ(dtz->get_info(winter).offset, std::chrono::hours{1});
#endif
        EXPECT_EQ(names[2], "col_without_tz");
        EXPECT_EQ(columns[2].data_type(), sparrow::data_type::TIMESTAMP_NANOSECONDS);
        dtz = sparrow::get_timezone(sparrow::detail::array_access::get_arrow_proxy(columns[2]));
        ASSERT_TRUE(dtz == nullptr);
        for (const auto& col : columns) {
            EXPECT_EQ(col.size(), chunk_size);
        }
    }
}

void assert_arrow_string_array_as_expected(const sparrow::array& arr, const std::span<std::string>& expected) {
    EXPECT_EQ(arr.size(), expected.size());
    for (auto i = 0u; i < arr.size(); i++) {
        const auto value = std::get<sparrow::nullable<std::string_view>>(arr[i]);
        EXPECT_TRUE(value.has_value());
        EXPECT_EQ(value.get(), std::string_view(expected[i]));
    }
}

TEST(ArrowRead, ConvertSegmentMultipleStringColumns) {
    const auto num_rows = 100u;
    const auto chunk_size = 19u;
    const auto num_chunks = num_rows / chunk_size + (num_rows % chunk_size != 0);
    const auto fields = std::array{
            scalar_field(DataType::FLOAT64, "floats"),
            scalar_field(DataType::UTF_DYNAMIC64, "str_1"),
            scalar_field(DataType::UTF_DYNAMIC64, "str_2"),
    };
    // We populate string columns so they have 30 different and 70 common strings.
    const auto str_id_offset = 30u;
    std::vector<std::vector<std::string>> string_values(2);
    for (auto row = 0u; row < num_rows; ++row) {
        string_values[0].emplace_back(fmt::format("string_{}", row));
        string_values[1].emplace_back(fmt::format("string_{}", str_id_offset + row));
    }
    auto read_options = ReadOptions();
    auto per_column_string_format =
            std::unordered_map<std::string, ArrowOutputStringFormat>{{"str_2", ArrowOutputStringFormat::LARGE_STRING}};
    read_options.set_output_config(ArrowOutputConfig{
            .default_string_format_ = ArrowOutputStringFormat::CATEGORICAL,
            .per_column_string_format_ = per_column_string_format
    });
    auto segment = get_detachable_segment(fields, num_rows, chunk_size, read_options);
    auto string_pool = std::make_shared<StringPool>();
    fill_chunked_string_column(
            segment.column(2), "str_1", num_rows, chunk_size, string_pool, string_values[0], read_options
    );
    fill_chunked_string_column(
            segment.column(3), "str_2", num_rows, chunk_size, string_pool, string_values[1], read_options
    );
    segment.set_string_pool(string_pool);

    // Convert to arrow
    proto::descriptors::NormalizationMetadata norm_meta;
    auto arrow_data = segment_to_arrow_data(segment, norm_meta);
    EXPECT_EQ(arrow_data.size(), num_chunks);
    for (auto i = 0u; i < num_chunks; ++i) {
        auto row_count = std::min(chunk_size, num_rows - i * chunk_size);
        const auto& record_batch = (arrow_data)[i];
        auto names = record_batch.names();
        auto columns = record_batch.columns();
        // Each record batch should have all columns for the row range (including the index)
        EXPECT_EQ(names.size(), fields.size() + 1);
        EXPECT_EQ(columns.size(), fields.size() + 1);
        EXPECT_EQ(names[0], "time");
        EXPECT_EQ(columns[0].data_type(), sparrow::data_type::TIMESTAMP_NANOSECONDS);
        EXPECT_EQ(names[1], "floats");
        EXPECT_EQ(columns[1].data_type(), sparrow::data_type::DOUBLE);
        EXPECT_EQ(names[2], "str_1");
        EXPECT_EQ(columns[2].data_type(), sparrow::data_type::INT32); // The dict array keys are INT32s
        assert_arrow_string_array_as_expected(
                columns[2], std::span(string_values[0]).subspan(i * chunk_size, row_count)
        );
        EXPECT_EQ(names[3], "str_2");
        EXPECT_EQ(columns[3].data_type(), sparrow::data_type::LARGE_STRING); // The offset values are INT64s
        assert_arrow_string_array_as_expected(
                columns[3], std::span(string_values[1]).subspan(i * chunk_size, row_count)
        );
        for (const auto& col : columns) {
            EXPECT_EQ(col.size(), row_count);
        }
    }
}

struct TimestampConvertParam {
    std::string name;
    bool sparse;
    bool has_nats;
    size_t source_chunk_size; // 0 = single block, >0 = multi-block with this chunk size
};

class ArrowTimestampConvert : public testing::TestWithParam<TimestampConvertParam> {
  public:
    static constexpr size_t num_rows = 100;
    ArrowTimestampHandler handler;
    TypeDescriptor source_type = make_scalar_type(DataType::NANOSECONDS_UTC64);
    TypeDescriptor dest_type;
    DetachableBlockConfig block_config;
    size_t dest_size;
    ReadOptions read_options;

    ArrowTimestampConvert() {
        read_options.set_output_config(ArrowOutputConfig{});
        std::tie(dest_type, block_config) = handler.output_type_and_block_config(source_type, "ts", read_options);
        dest_size = data_type_size(dest_type);
    }
};

TEST_P(ArrowTimestampConvert, NullPositions) {
    const auto& p = GetParam();
    auto sparsity = p.sparse ? Sparsity::PERMITTED : Sparsity::NOT_PERMITTED;
    bool multi_block = p.source_chunk_size > 0;

    // Build values and expected validity
    std::vector<timestamp> values(num_rows);
    std::vector<bool> expected_valid(num_rows, !p.sparse);
    for (size_t i = 0; i < num_rows; ++i) {
        if (p.sparse && i % 3 != 0)
            continue;
        bool is_nat = p.has_nats && (i % 10 == 0);
        values[i] = is_nat ? NaT : static_cast<timestamp>(i);
        expected_valid[i] = !is_nat;
    }

    // Build source column
    auto source = [&]() {
        if (multi_block) {
            auto col = Column(source_type, 0, AllocationType::DETACHABLE, Sparsity::NOT_PERMITTED);
            allocate_and_fill_chunked_column<timestamp>(
                    col, num_rows, p.source_chunk_size, std::span<timestamp>(values)
            );
            return col;
        }
        auto col = Column(source_type, num_rows, AllocationType::DYNAMIC, sparsity);
        for (size_t i = 0; i < num_rows; ++i) {
            if (p.sparse && i % 3 != 0)
                continue;
            col.set_scalar(i, values[i]);
        }
        return col;
    }();
    if (multi_block) {
        ASSERT_GT(source.num_blocks(), 1u);
    }

    // Run convert_type
    auto dest = Column(dest_type, 0, AllocationType::DETACHABLE, Sparsity::NOT_PERMITTED, block_config);
    allocate_chunked_column(dest, num_rows, num_rows);
    auto field_wrapper = FieldWrapper(dest_type, "ts");
    auto mapping = ColumnMapping(
            source_type, dest_type, field_wrapper.field(), dest_size, num_rows, 0, 0, dest_size * num_rows, 0
    );
    auto handler_data = std::any{};
    auto string_pool = std::make_shared<StringPool>();
    handler.convert_type(source, dest, mapping, DecodePathData{}, handler_data, string_pool, read_options);

    // Verify
    bool has_any_nulls = std::any_of(expected_valid.begin(), expected_valid.end(), [](bool v) { return !v; });
    EXPECT_EQ(dest.has_extra_buffer(0, ExtraBufferType::BITMAP), has_any_nulls);
    auto arrays = arrow_arrays_from_column(dest, "ts");
    ASSERT_EQ(arrays.size(), 1);
    for (size_t i = 0; i < num_rows; ++i) {
        EXPECT_EQ(arrays[0][i].has_value(), expected_valid[i]);
    }
}

INSTANTIATE_TEST_SUITE_P(
        AllCases, ArrowTimestampConvert,
        testing::Values(
                TimestampConvertParam{"DenseNoNats", false, false, 0},
                TimestampConvertParam{"DenseWithNats", false, true, 0},
                TimestampConvertParam{"SparseWithNats", true, true, 0},
                TimestampConvertParam{"MultiBlockDenseNoNats", false, false, 25},
                TimestampConvertParam{"MultiBlockDenseWithNats", false, true, 25}
        ),
        [](const auto& info) { return info.param.name; }
);

TEST(ModifyArrowOutputConfigFromNormMeta, NonArrowNormMeta) {
    proto::descriptors::NormalizationMetadata norm_meta;
    // Accessing like this sets the oneof to df
    norm_meta.mutable_df();

    // Unspecified -> large string
    ArrowOutputConfig arrow_output_config{.per_column_string_format_ = {{"col", ArrowOutputStringFormat::UNSPECIFIED}}};
    modify_arrow_output_config_from_norm_meta(norm_meta, arrow_output_config);
    ASSERT_EQ(arrow_output_config.default_string_format_, ArrowOutputStringFormat::LARGE_STRING);
    ASSERT_EQ(arrow_output_config.per_column_string_format_.at("col"), ArrowOutputStringFormat::LARGE_STRING);

    // Specified default/unspecified per-column makes per-column match the default
    arrow_output_config = ArrowOutputConfig{
            .default_string_format_ = ArrowOutputStringFormat::CATEGORICAL,
            .per_column_string_format_ = {{"col", ArrowOutputStringFormat::UNSPECIFIED}}
    };
    modify_arrow_output_config_from_norm_meta(norm_meta, arrow_output_config);
    ASSERT_EQ(arrow_output_config.default_string_format_, ArrowOutputStringFormat::CATEGORICAL);
    ASSERT_EQ(arrow_output_config.per_column_string_format_.at("col"), ArrowOutputStringFormat::CATEGORICAL);

    // Unspecified default/specified per-column makes the default large string, doesn't change the per-column
    arrow_output_config =
            ArrowOutputConfig{.per_column_string_format_ = {{"col", ArrowOutputStringFormat::CATEGORICAL}}};
    modify_arrow_output_config_from_norm_meta(norm_meta, arrow_output_config);
    ASSERT_EQ(arrow_output_config.default_string_format_, ArrowOutputStringFormat::LARGE_STRING);
    ASSERT_EQ(arrow_output_config.per_column_string_format_.at("col"), ArrowOutputStringFormat::CATEGORICAL);
}

TEST(ModifyArrowOutputConfigFromNormMeta, ArrowNormMeta) {
    proto::descriptors::NormalizationMetadata norm_meta;
    proto::descriptors::NormalizationMetadata::Arrow::ColumnMeta column_meta;
    column_meta.set_string_format(proto::descriptors::LARGE_STRING);
    (*norm_meta.mutable_arrow()->mutable_columns())["large string"] = column_meta;
    column_meta.set_string_format(proto::descriptors::SMALL_STRING);
    (*norm_meta.mutable_arrow()->mutable_columns())["small string"] = column_meta;
    column_meta.set_string_format(proto::descriptors::CATEGORICAL);
    (*norm_meta.mutable_arrow()->mutable_columns())["categorical"] = column_meta;

    // Default unspecified takes all values from norm meta and sets default to large string
    ArrowOutputConfig arrow_output_config{};
    modify_arrow_output_config_from_norm_meta(norm_meta, arrow_output_config);
    ASSERT_EQ(arrow_output_config.default_string_format_, ArrowOutputStringFormat::LARGE_STRING);
    ASSERT_EQ(arrow_output_config.per_column_string_format_.at("large string"), ArrowOutputStringFormat::LARGE_STRING);
    ASSERT_EQ(arrow_output_config.per_column_string_format_.at("small string"), ArrowOutputStringFormat::SMALL_STRING);
    ASSERT_EQ(arrow_output_config.per_column_string_format_.at("categorical"), ArrowOutputStringFormat::CATEGORICAL);

    // With Polars, default unspecified takes all values from norm meta but mapping small string to large string and
    // sets default to large string
    arrow_output_config = ArrowOutputConfig{.output_format = ArrowOutputFormat::POLARS};
    modify_arrow_output_config_from_norm_meta(norm_meta, arrow_output_config);
    ASSERT_EQ(arrow_output_config.default_string_format_, ArrowOutputStringFormat::LARGE_STRING);
    ASSERT_EQ(arrow_output_config.per_column_string_format_.at("large string"), ArrowOutputStringFormat::LARGE_STRING);
    ASSERT_EQ(arrow_output_config.per_column_string_format_.at("small string"), ArrowOutputStringFormat::LARGE_STRING);
    ASSERT_EQ(arrow_output_config.per_column_string_format_.at("categorical"), ArrowOutputStringFormat::CATEGORICAL);

    // Default unspecified and a per-column unspecified takes all values from norm meta and sets default to large string
    arrow_output_config =
            ArrowOutputConfig{.per_column_string_format_ = {{"small string", ArrowOutputStringFormat::UNSPECIFIED}}};
    modify_arrow_output_config_from_norm_meta(norm_meta, arrow_output_config);
    ASSERT_EQ(arrow_output_config.default_string_format_, ArrowOutputStringFormat::LARGE_STRING);
    ASSERT_EQ(arrow_output_config.per_column_string_format_.at("large string"), ArrowOutputStringFormat::LARGE_STRING);
    ASSERT_EQ(arrow_output_config.per_column_string_format_.at("small string"), ArrowOutputStringFormat::SMALL_STRING);
    ASSERT_EQ(arrow_output_config.per_column_string_format_.at("categorical"), ArrowOutputStringFormat::CATEGORICAL);

    // Default unspecified and a per-column override takes all values from norm meta except per-column overrides and
    // sets default to large string
    arrow_output_config =
            ArrowOutputConfig{.per_column_string_format_ = {{"small string", ArrowOutputStringFormat::CATEGORICAL}}};
    modify_arrow_output_config_from_norm_meta(norm_meta, arrow_output_config);
    ASSERT_EQ(arrow_output_config.default_string_format_, ArrowOutputStringFormat::LARGE_STRING);
    ASSERT_EQ(arrow_output_config.per_column_string_format_.at("large string"), ArrowOutputStringFormat::LARGE_STRING);
    ASSERT_EQ(arrow_output_config.per_column_string_format_.at("small string"), ArrowOutputStringFormat::CATEGORICAL);
    ASSERT_EQ(arrow_output_config.per_column_string_format_.at("categorical"), ArrowOutputStringFormat::CATEGORICAL);

    // Default specified and a per-column override leave arrow output config unchanged
    arrow_output_config = ArrowOutputConfig{
            .default_string_format_ = ArrowOutputStringFormat::SMALL_STRING,
            .per_column_string_format_ = {{"small string", ArrowOutputStringFormat::CATEGORICAL}}
    };
    modify_arrow_output_config_from_norm_meta(norm_meta, arrow_output_config);
    ASSERT_EQ(arrow_output_config.default_string_format_, ArrowOutputStringFormat::SMALL_STRING);
    ASSERT_EQ(arrow_output_config.per_column_string_format_.at("small string"), ArrowOutputStringFormat::CATEGORICAL);

    // Default specified and a per-column override to unspecified sets the per-column unspecified from the norm meta
    arrow_output_config = ArrowOutputConfig{
            .default_string_format_ = ArrowOutputStringFormat::CATEGORICAL,
            .per_column_string_format_ = {{"small string", ArrowOutputStringFormat::UNSPECIFIED}}
    };
    modify_arrow_output_config_from_norm_meta(norm_meta, arrow_output_config);
    ASSERT_EQ(arrow_output_config.default_string_format_, ArrowOutputStringFormat::CATEGORICAL);
    ASSERT_EQ(arrow_output_config.per_column_string_format_.at("small string"), ArrowOutputStringFormat::SMALL_STRING);

    // Default unspecified and a per-column override of a column not in norm meta maintains the override
    arrow_output_config =
            ArrowOutputConfig{.per_column_string_format_ = {{"new", ArrowOutputStringFormat::CATEGORICAL}}};
    modify_arrow_output_config_from_norm_meta(norm_meta, arrow_output_config);
    ASSERT_EQ(arrow_output_config.default_string_format_, ArrowOutputStringFormat::LARGE_STRING);
    ASSERT_EQ(arrow_output_config.per_column_string_format_.at("large string"), ArrowOutputStringFormat::LARGE_STRING);
    ASSERT_EQ(arrow_output_config.per_column_string_format_.at("small string"), ArrowOutputStringFormat::SMALL_STRING);
    ASSERT_EQ(arrow_output_config.per_column_string_format_.at("categorical"), ArrowOutputStringFormat::CATEGORICAL);
    ASSERT_EQ(arrow_output_config.per_column_string_format_.at("new"), ArrowOutputStringFormat::CATEGORICAL);

    // Default specified and a per-column override to unspecified of a column not in norm meta uses the default
    arrow_output_config = ArrowOutputConfig{
            .default_string_format_ = ArrowOutputStringFormat::CATEGORICAL,
            .per_column_string_format_ = {{"new", ArrowOutputStringFormat::UNSPECIFIED}}
    };
    modify_arrow_output_config_from_norm_meta(norm_meta, arrow_output_config);
    ASSERT_EQ(arrow_output_config.default_string_format_, ArrowOutputStringFormat::CATEGORICAL);
    ASSERT_EQ(arrow_output_config.per_column_string_format_.at("new"), ArrowOutputStringFormat::CATEGORICAL);
}
