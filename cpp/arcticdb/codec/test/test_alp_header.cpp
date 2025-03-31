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

    EXPECT_EQ(reinterpret_cast<uintptr_t>(header->left()),
              reinterpret_cast<uintptr_t>(header->at<uint16_t>(0UL)));
    EXPECT_EQ(reinterpret_cast<uintptr_t>(header->right()),
              reinterpret_cast<uintptr_t>(header->at<typename Header::RightType>(header->left_size())));
    EXPECT_EQ(reinterpret_cast<uintptr_t>(header->exceptions()),
              reinterpret_cast<uintptr_t>(
                  header->at<uint16_t>(header->left_size() + header->right_size())));
    size_t expected_dict_offset = header->left_size() + header->right_size() +
        (dummy_state.exceptions_count * sizeof(uint16_t)) * 2;
    EXPECT_EQ(reinterpret_cast<uintptr_t>(header->dict()),
              reinterpret_cast<uintptr_t>(header->at<uint16_t>(expected_dict_offset)));

    uint16_t left_value = 0xAAAA;
    for (size_t i = 0; i < alp::config::VECTOR_SIZE; i++) {
        header->left()[i] = left_value;
    }

    typename Header::RightType right_pattern = 0xBBBBBBBBBBBBBBBBULL;
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
}