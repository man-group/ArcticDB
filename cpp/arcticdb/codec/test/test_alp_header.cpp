#include <gtest/gtest.h>

#include <arcticdb/codec/compression/alp_header.hpp>

namespace arcticdb {
// Test for the column header using double.
TEST(RealDoubleColumnHeaderTest, OffsetsAndDataPreservationDouble) {
    // Build a dummy state to initialize the column header.
    alp::state<double> dummy_state;
    // (Note: exceptions_count is not used by column header.)
    dummy_state.right_bit_width = 8;
    dummy_state.left_bit_width = 16;

    const size_t dict_entries = 5;
    std::vector<uint16_t> dummy_dict(dict_entries, 0x5555); // dictionary pattern

    using ColumnHeader = arcticdb::RealDoubleColumnHeader<double>;
    // Total size = fixed header size + dictionary bytes.
    size_t total_size = ColumnHeader::HeaderSize + (dict_entries * sizeof(uint16_t));

    std::vector<uint8_t> buffer(total_size, 0);
    auto header = new (buffer.data()) ColumnHeader(dummy_state);
    header->set_dict(dummy_dict.data(), dict_entries);

    // For a column header, dict() returns the area beginning at data_ (i.e. at offset 0 of data_).
    EXPECT_EQ(reinterpret_cast<uintptr_t>(header->dict()),
              reinterpret_cast<uintptr_t>(header->data_));

    // Now verify that writing into the dictionary works.
    for (size_t i = 0; i < dict_entries; i++) {
        EXPECT_EQ(header->dict()[i], 0x5555);
    }

    // Also verify that the header fields were stored correctly.
    EXPECT_EQ(header-> bit_widths_.right_, dummy_state.right_bit_width);
    EXPECT_EQ(header-> bit_widths_.left_, dummy_state.left_bit_width);
    EXPECT_EQ(header->dict_size_, dict_entries);
}

// Test for the block header using double.
TEST(RealDoubleBlockHeaderTest, OffsetsAndDataPreservationDouble) {
    using T = double;
    alp::state<T> dummy_state;
    dummy_state.exceptions_count = 3;  // for testing purposes
    // These values are not stored in the block header but may be used externally.
    dummy_state.right_bit_width = 8;
    dummy_state.left_bit_width = 16;
    RealDoubleColumnHeader<T> column_header{dummy_state};
    auto bit_widths = column_header.bit_widths_;
    using BlockHeader = arcticdb::RealDoubleBlockHeader<double>;
    // Calculate payload sizes.
    size_t left_size = BlockHeader{}.left_size(bit_widths);
    size_t right_size = BlockHeader{}.right_size(bit_widths);
    size_t exceptions_bytes = dummy_state.exceptions_count * sizeof(uint16_t);
    size_t excp_positions_bytes = dummy_state.exceptions_count * sizeof(uint16_t);
    size_t payload_size = left_size + right_size + exceptions_bytes + excp_positions_bytes;
    size_t total_size = BlockHeader::HeaderSize + payload_size;

    std::vector<uint8_t> buffer(total_size, 0);
    auto header = new (buffer.data()) BlockHeader();

    // For block header, the fixed header is at the beginning and then the dynamic data in data_.
    // Verify the various offsets using the at<> helpers.
    EXPECT_EQ(reinterpret_cast<uintptr_t>(header->left()),
              reinterpret_cast<uintptr_t>(header->at<uint16_t>(0UL)));
    EXPECT_EQ(reinterpret_cast<uintptr_t>(header->right(bit_widths)),
              reinterpret_cast<uintptr_t>(header->at<typename BlockHeader::RightType>(left_size)));
    EXPECT_EQ(reinterpret_cast<uintptr_t>(header->exceptions(bit_widths)),
              reinterpret_cast<uintptr_t>(header->at<uint16_t>(left_size + right_size)));
    EXPECT_EQ(reinterpret_cast<uintptr_t>(header->exception_positions(bit_widths)),
              reinterpret_cast<uintptr_t>(header->at<uint16_t>(left_size + right_size + exceptions_bytes)));

    // Fill in the arrays and verify preservation.
    uint16_t left_value = 0x1111;
    for (size_t i = 0; i < alp::config::VECTOR_SIZE; i++) {
        header->left()[i] = left_value;
    }
    using RightType = typename BlockHeader::RightType;
    RightType right_pattern = 0x2222222222222222ULL;
    for (size_t i = 0; i < alp::config::VECTOR_SIZE; i++) {
        header->right(bit_widths)[i] = right_pattern;
    }
    for (size_t i = 0; i < dummy_state.exceptions_count; i++) {
        header->exceptions(bit_widths)[i] = 0x3333;
    }
    for (size_t i = 0; i < dummy_state.exceptions_count; i++) {
        header->exception_positions(bit_widths)[i] = 0x4444;
    }

    for (size_t i = 0; i < alp::config::VECTOR_SIZE; i++) {
        EXPECT_EQ(header->left()[i], left_value);
    }
    EXPECT_EQ(header->right(bit_widths)[0], right_pattern);
    EXPECT_EQ(header->exceptions(bit_widths)[0], 0x3333);
    EXPECT_EQ(header->exception_positions(bit_widths)[0], 0x4444);

    // Also record the exception count.
    header->exception_count_ = dummy_state.exceptions_count;
    EXPECT_EQ(header->exception_count_, dummy_state.exceptions_count);
}

// Repeat similar tests for float.
TEST(RealDoubleColumnHeaderTest, OffsetsAndDataPreservationFloat) {
    alp::state<float> dummy_state;
    // Column header does not use exceptions_count.
    dummy_state.right_bit_width = 4;
    dummy_state.left_bit_width = 8;

    const size_t dict_entries = 4;
    std::vector<uint16_t> dummy_dict(dict_entries, 0x7777);

    using ColumnHeader = arcticdb::RealDoubleColumnHeader<float>;
    size_t total_size = ColumnHeader::HeaderSize + (dict_entries * sizeof(uint16_t));

    std::vector<uint8_t> buffer(total_size, 0);
    auto header = new (buffer.data()) ColumnHeader(dummy_state);
    header->set_dict(dummy_dict.data(), dict_entries);

    EXPECT_EQ(reinterpret_cast<uintptr_t>(header->dict()),
              reinterpret_cast<uintptr_t>(header->data_));

    for (size_t i = 0; i < dict_entries; i++) {
        EXPECT_EQ(header->dict()[i], 0x7777);
    }

    EXPECT_EQ(header-> bit_widths_.right_, dummy_state.right_bit_width);
    EXPECT_EQ(header-> bit_widths_.left_, dummy_state.left_bit_width);
    EXPECT_EQ(header->dict_size_, dict_entries);
}

TEST(RealDoubleBlockHeaderTest, OffsetsAndDataPreservationFloat) {
    using T = float;
    alp::state<T> dummy_state;
    dummy_state.exceptions_count = 2;
    dummy_state.right_bit_width = 4;
    dummy_state.left_bit_width = 8;

    RealDoubleColumnHeader<T> column_header{dummy_state};
    auto bit_widths = column_header.bit_widths_;
    using BlockHeader = arcticdb::RealDoubleBlockHeader<T>;
    size_t left_size = BlockHeader{}.left_size(bit_widths);
    size_t right_size = BlockHeader{}.right_size(bit_widths);
    size_t exceptions_bytes = dummy_state.exceptions_count * sizeof(uint16_t);
    size_t excp_positions_bytes = dummy_state.exceptions_count * sizeof(uint16_t);
    size_t payload_size = left_size + right_size + exceptions_bytes + excp_positions_bytes;
    size_t total_size = BlockHeader::HeaderSize + payload_size;

    std::vector<uint8_t> buffer(total_size, 0);
    auto header = new (buffer.data()) BlockHeader();

    EXPECT_EQ(reinterpret_cast<uintptr_t>(header->left()),
              reinterpret_cast<uintptr_t>(header->at<uint16_t>(0UL)));
    EXPECT_EQ(reinterpret_cast<uintptr_t>(header->right(bit_widths)),
              reinterpret_cast<uintptr_t>(header->at<typename BlockHeader::RightType>(left_size)));
    EXPECT_EQ(reinterpret_cast<uintptr_t>(header->exceptions(bit_widths)),
              reinterpret_cast<uintptr_t>(header->at<uint16_t>(left_size + right_size)));

    size_t expected_offset = left_size + right_size + exceptions_bytes;
    EXPECT_EQ(reinterpret_cast<uintptr_t>(header->exception_positions(bit_widths)),
              reinterpret_cast<uintptr_t>(header->at<uint16_t>(expected_offset)));

    uint16_t left_value = 0xAAAA;
    for (size_t i = 0; i < alp::config::VECTOR_SIZE; i++) {
        header->left()[i] = left_value;
    }

    using RightType = typename BlockHeader::RightType;
    RightType right_pattern = 0xBBBBBBBBU;
    for (size_t i = 0; i < alp::config::VECTOR_SIZE; i++) {
        header->right(bit_widths)[i] = right_pattern;
    }

    for (size_t i = 0; i < dummy_state.exceptions_count; i++) {
        header->exceptions(bit_widths)[i] = 0xCCCC;
    }
    for (size_t i = 0; i < dummy_state.exceptions_count; i++) {
        header->exception_positions(bit_widths)[i] = 0xDDDD;
    }

    for (size_t i = 0; i < alp::config::VECTOR_SIZE; i++) {
        EXPECT_EQ(header->left()[i], left_value);
    }
    EXPECT_EQ(header->right(bit_widths)[0], right_pattern);
    EXPECT_EQ(header->exceptions(bit_widths)[0], 0xCCCC);
    EXPECT_EQ(header->exception_positions(bit_widths)[0], 0xDDDD);

    header->exception_count_ = dummy_state.exceptions_count;
    EXPECT_EQ(header->exception_count_, dummy_state.exceptions_count);
}
TEST(ALPDecimalBlockHeaderTest, Float) {
    alp::state<float> state;
    state.exceptions_count = 5;
    state.bit_width = 8;
    state.exp = 2;
    state.fac = 3;

    using EncodedType = typename StorageType<float>::signed_type;
    size_t dataSize = alp::config::VECTOR_SIZE * sizeof(EncodedType);
    size_t exceptionsBytes = state.exceptions_count * sizeof(float);
    size_t exceptionPositionsSize = state.exceptions_count * sizeof(uint16_t);
    constexpr size_t headerSize = ALPDecimalBlockHeader<float>::HeaderSize;
    size_t expectedTotalSize = headerSize + dataSize + exceptionsBytes + exceptionPositionsSize;

    std::vector<uint8_t> buffer(expectedTotalSize, 0xFF);

    ALPDecimalBlockHeader<float>* header = new (buffer.data()) ALPDecimalBlockHeader<float>{};

    EXPECT_EQ(header->total_size(), expectedTotalSize);

    auto encoded = header->data();
    for (size_t i = 0; i < alp::config::VECTOR_SIZE; ++i) {
        encoded[i] = static_cast<EncodedType>(i);
    }

    float* exceptions = header->exceptions();
    for (uint16_t i = 0; i < state.exceptions_count; ++i) {
        exceptions[i] = static_cast<float>(i * 10);
    }

    uint16_t* positions = header->exception_positions();
    for (uint16_t i = 0; i < state.exceptions_count; ++i) {
        positions[i] = i + 100;
    }

    for (size_t i = 0; i < alp::config::VECTOR_SIZE; ++i) {
        EXPECT_EQ(encoded[i], static_cast<EncodedType>(i));
    }

    for (uint16_t i = 0; i < state.exceptions_count; ++i) {
        EXPECT_EQ(exceptions[i], static_cast<float>(i * 10));
    }

    for (uint16_t i = 0; i < state.exceptions_count; ++i) {
        EXPECT_EQ(positions[i], i + 100);
    }
}

TEST(ALPDecimalBlockHeaderTest, Double) {
    alp::state<double> state;
    state.exceptions_count = 5;
    state.bit_width = 16;
    state.exp = 4;
    state.fac = 6;

    using EncodedType = typename StorageType<double>::signed_type;
    size_t dataSize = alp::config::VECTOR_SIZE * sizeof(EncodedType);
    size_t exceptionsBytes = state.exceptions_count * sizeof(double);
    size_t exceptionPositionsSize = state.exceptions_count * sizeof(uint16_t);
    constexpr size_t headerSize = ALPDecimalBlockHeader<double>::HeaderSize;
    size_t expectedTotalSize = headerSize + dataSize + exceptionsBytes + exceptionPositionsSize;

    std::vector<uint8_t> buffer(expectedTotalSize, 0xFF);
    ALPDecimalBlockHeader<double>* header = new (buffer.data()) ALPDecimalBlockHeader<double>{};

    EXPECT_EQ(header->total_size(), expectedTotalSize);

    auto encoded = header->data();
    for (size_t i = 0; i < alp::config::VECTOR_SIZE; ++i) {
        encoded[i] = static_cast<EncodedType>(i);
    }

    double* exceptions = header->exceptions();
    for (uint16_t i = 0; i < state.exceptions_count; ++i) {
        exceptions[i] = static_cast<double>(i * 20);
    }

    uint16_t* positions = header->exception_positions();
    for (uint16_t i = 0; i < state.exceptions_count; ++i) {
        positions[i] = i + 200;
    }

    for (size_t i = 0; i < alp::config::VECTOR_SIZE; ++i) {
        EXPECT_EQ(encoded[i], static_cast<EncodedType>(i));
    }

    for (uint16_t i = 0; i < state.exceptions_count; ++i) {
        EXPECT_EQ(exceptions[i], static_cast<double>(i * 20));
    }

    for (uint16_t i = 0; i < state.exceptions_count; ++i) {
        EXPECT_EQ(positions[i], i + 200);
    }
}
}