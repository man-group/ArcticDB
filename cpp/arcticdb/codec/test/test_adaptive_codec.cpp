#include <gtest/gtest.h>

#include <arcticdb/codec/codec.hpp>
#include <arcticdb/column_store/column.hpp>
#include <arcticdb/column_store/column_data.hpp>
#include <arcticdb/codec/scanner.hpp>

namespace arcticdb {
/*
TEST(AdaptiveCodec, SimpleRoundtrip) {
    constexpr auto num_rows = 100 * 1024;
    Column column{make_scalar_type(DataType::UINT64), num_rows, AllocationType::PRESIZED, Sparsity::NOT_PERMITTED};

    auto ptr = reinterpret_cast<uint64_t*>(column.buffer().data());
    for(auto i = 0L; i < num_rows; ++i) {
        ptr[i] = static_cast<uint64_t>(i % 1000);
    }
    column.set_row_data(num_rows - 1);

    using TDT = ScalarTagType<DataTypeTag<DataType::UINT64>>;

    auto column_data = column.data();

    AdaptiveEncoder<TypedBlockData, TDT> encoder;
    auto scan_results = predicted_optimal_encodings(column_data);
    SizeResult size_result;
    select_encoding_for_column(column_data, scan_results, size_result);
    Buffer compressed{size_result.max_compressed_bytes_};
    std::ptrdiff_t pos = 0UL;
    EncodedBlock encoded_block;

    encoder.encode_data(column_data, compressed, pos, encoded_block, scan_results.first());

    Column output{make_scalar_type(DataType::UINT64), num_rows, AllocationType::PRESIZED, Sparsity::NOT_PERMITTED};
    AdaptiveDecoder decoder;
    decoder.decode_block(
        uint32_t{1},
        compressed.data(),
        encoded_block.out_bytes(),
        output.buffer().data(),
        column.bytes()
        );

    output.set_row_data(num_rows - 1);
    ASSERT_EQ(column, output);
}
*/
}