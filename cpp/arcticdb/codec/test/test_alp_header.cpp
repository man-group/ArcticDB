#include <gtest/gtest.h>

#include <arcticdb/codec/compression/alp_header.hpp>

namespace arcticdb {
TEST(RealDoubleHeaderTest, OffsetsAndDataPreservationDouble) {
    alp::state<double> dummy_state;
    dummy_state.exceptions_count = 3;  // for testing purposes
    dummy_state.right_bit_width = 8;
    dummy_state.left_bit_width = 16;

    const size_t dict_entries = 5;
    std::vector<uint16_t> dummy_dict(dict_entries, 0x5555); // dictionary pattern

    using Header = arcticdb::RealDoubleHeader<double>;
    Header temp(dummy_state);
    size_t payload_size = temp.left_size() + temp.right_size() +
        (dummy_state.exceptions_count * sizeof(uint16_t)) * 2 +  // exceptions and exception positions
        (dict_entries * sizeof(uint16_t));
    size_t total_size = Header::HeaderSize + payload_size;

    std::vector<uint8_t> buffer(total_size, 0);
    Header* header = new (buffer.data()) Header(dummy_state);
    header->set_dict(dummy_dict.data(), dict_entries);

    EXPECT_EQ(reinterpret_cast<uintptr_t>(header->left()),
              reinterpret_cast<uintptr_t>(header->at<uint16_t>(0UL)));

    EXPECT_EQ(reinterpret_cast<uintptr_t>(header->right()),
              reinterpret_cast<uintptr_t>(header->at<typename Header::RightType>(header->left_size())));

    EXPECT_EQ(reinterpret_cast<uintptr_t>(header->exceptions()),
              reinterpret_cast<uintptr_t>(
                  header->at<uint16_t>(header->left_size() + header->right_size())));

    EXPECT_EQ(reinterpret_cast<uintptr_t>(header->exception_positions()),
              reinterpret_cast<uintptr_t>(header->at<uint16_t>(
                  header->left_size() + header->right_size() + header->exceptions_bytes())));

    size_t expected_dict_offset = header->left_size() + header->right_size() + header->exceptions_bytes() +
        header->exception_positions_size();
    EXPECT_EQ(reinterpret_cast<uintptr_t>(header->dict()),
              reinterpret_cast<uintptr_t>(header->at<uint16_t>(expected_dict_offset)));

    uint16_t left_value = 0x1111;
    for (size_t i = 0; i < alp::config::VECTOR_SIZE; i++) {
        header->left()[i] = left_value;
    }

    Header::RightType right_pattern = 0x2222222222222222ULL;
    for (size_t i = 0; i < alp::config::VECTOR_SIZE; i++) {
        header->right()[i] = right_pattern;
    }

    for (size_t i = 0; i < dummy_state.exceptions_count; i++) {
        header->exceptions()[i] = 0x3333;
    }

    for (size_t i = 0; i < dummy_state.exceptions_count; i++) {
        header->exception_positions()[i] = 0x4444;
    }

    for (size_t i = 0; i < dict_entries; i++) {
        EXPECT_EQ(header->dict()[i], 0x5555);
    }

    EXPECT_EQ(header->left()[0], left_value);
    EXPECT_EQ(header->right()[0], right_pattern);
    EXPECT_EQ(header->exceptions()[0], 0x3333);
    EXPECT_EQ(header->exception_positions()[0], 0x4444);
}

TEST(RealDoubleHeaderTest, OffsetsAndDataPreservationFloat) {
    alp::state<float> dummy_state;
    dummy_state.exceptions_count = 2;
    dummy_state.right_bit_width = 4;
    dummy_state.left_bit_width = 8;

    const size_t dict_entries = 4;
    std::vector<uint16_t> dummy_dict(dict_entries, 0x7777);

    using Header = arcticdb::RealDoubleHeader<float>;
    Header temp(dummy_state);
    size_t payload_size = temp.left_size() + temp.right_size() +
        (dummy_state.exceptions_count * sizeof(uint16_t)) * 2 +
        (dict_entries * sizeof(uint16_t));
    size_t total_size = Header::HeaderSize + payload_size;

    std::vector<uint8_t> buffer(total_size, 0);
    Header* header = new (buffer.data()) Header(dummy_state);
    header->set_dict(dummy_dict.data(), dict_entries);

    EXPECT_EQ(reinterpret_cast<uintptr_t>(header->left()), reinterpret_cast<uintptr_t>(header->at<uint16_t>(0UL)));
    EXPECT_EQ(reinterpret_cast<uintptr_t>(header->right()), reinterpret_cast<uintptr_t>(header->at<typename Header::RightType>(header->left_size())));
    EXPECT_EQ(reinterpret_cast<uintptr_t>(header->exceptions()), reinterpret_cast<uintptr_t>(header->at<uint16_t>(header->left_size() + header->right_size())));
    size_t expected_dict_offset = header->left_size() + header->right_size() + (dummy_state.exceptions_count * sizeof(uint16_t)) * 2;
    EXPECT_EQ(reinterpret_cast<uintptr_t>(header->dict()), reinterpret_cast<uintptr_t>(header->at<uint16_t>(expected_dict_offset)));

    uint16_t left_value = 0xAAAA;
    for (size_t i = 0; i < alp::config::VECTOR_SIZE; i++) {
        header->left()[i] = left_value;
    }

    typename Header::RightType right_pattern = 0xBBBBBBBBU;
    for (size_t i = 0; i < alp::config::VECTOR_SIZE; i++) {
        header->right()[i] = right_pattern;
    }

    for (size_t i = 0; i < dummy_state.exceptions_count; i++) {
        header->exceptions()[i] = 0xCCCC;
    }
    for (size_t i = 0; i < dummy_state.exceptions_count; i++) {
        header->exception_positions()[i] = 0xDDDD;
    }

    for (size_t i = 0; i < dict_entries; i++) {
        EXPECT_EQ(header->dict()[i], 0x7777);
    }

    EXPECT_EQ(header->left()[0], left_value);
    EXPECT_EQ(header->right()[0], right_pattern);
    EXPECT_EQ(header->exceptions()[0], 0xCCCC);
    EXPECT_EQ(header->exception_positions()[0], 0xDDDD);
}

TEST(ALPDecimalHeaderTest, Float) {
    alp::state<float> state;
    state.exceptions_count = 5;
    state.bit_width = 8;
    state.exp = 2;
    state.fac = 3;

    using EncodedType = StorageType<float>::signed_type;
    size_t dataSize = alp::config::VECTOR_SIZE * sizeof(EncodedType);
    size_t exceptionsBytes = state.exceptions_count * sizeof(float);
    size_t exceptionPositionsSize = state.exceptions_count * sizeof(uint16_t);
    constexpr size_t headerSize = ALPDecimalHeader<float>::HeaderSize;
    size_t expectedTotalSize = headerSize + dataSize + exceptionsBytes + exceptionPositionsSize;

    std::vector<uint8_t> buffer(expectedTotalSize, 0xFF);

    ALPDecimalHeader<float>* header = new (buffer.data()) ALPDecimalHeader<float>(state);

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

TEST(ALPDecimalHeaderTest, Double) {
    alp::state<double> state;
    state.exceptions_count = 5;
    state.bit_width = 16;
    state.exp = 4;
    state.fac = 6;

    using EncodedType = StorageType<double>::signed_type;
    size_t dataSize = alp::config::VECTOR_SIZE * sizeof(EncodedType);
    size_t exceptionsBytes = state.exceptions_count * sizeof(double);
    size_t exceptionPositionsSize = state.exceptions_count * sizeof(uint16_t);
    constexpr size_t headerSize = ALPDecimalHeader<double>::HeaderSize;
    size_t expectedTotalSize = headerSize + dataSize + exceptionsBytes + exceptionPositionsSize;

    std::vector<uint8_t> buffer(expectedTotalSize, 0xFF);
    ALPDecimalHeader<double>* header = new (buffer.data()) ALPDecimalHeader<double>(state);

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