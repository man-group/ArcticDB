#include <gtest/gtest.h>

#include <random>

#include <arcticdb/codec/compression/alp/alp.hpp>
#include <arcticdb/codec/compression/alp.hpp>
#include <arcticdb/log/log.hpp>
#include <arcticdb/codec/compression/alp_header.hpp>
#include <arcticdb/codec/test/encoding_test_common.hpp>

namespace arcticdb {

TEST(ALP, DetermineSchemeRealDouble) {
    auto data = random_doubles();
    alp::state<double> state;
    std::array<double, 1024> sample_buf;

    alp::encoder<double>::init(data.data(), 0, 1024, sample_buf.data(), state);
    log::codec().info("state: {}", state.exceptions_count);
    ASSERT_NE(state.scheme, alp::Scheme::INVALID);
    ASSERT_EQ(state.scheme, alp::Scheme::ALP_RD);
}

TEST(ALP, RoundtripRD) {
    auto data = random_doubles();
    alp::state<double> state;
    std::array<double, 1024> sample_buf;

    alp::encoder<double>::init(data.data(), 0, 1024, sample_buf.data(), state);
    log::codec().info("state: {}", state.exceptions_count);
    ASSERT_EQ(state.scheme, alp::Scheme::ALP_RD);

    std::array<uint16_t, BLOCK_SIZE> exceptions;
    std::array<uint16_t, BLOCK_SIZE> positions;
    std::array<uint16_t, BLOCK_SIZE> excp_count;
    std::array<uint64_t, BLOCK_SIZE> right;
    std::array<uint16_t, BLOCK_SIZE> left;

    alp::rd_encoder<double>::init(data.data(), 0, 1024, sample_buf.data(), state);
    alp::rd_encoder<double>::encode(data.data(), exceptions.data(), positions.data(), excp_count.data(), right.data(), left.data(), state);

    std::vector<double> output(1024);
    alp::rd_encoder<double>::decode(output.data(), right.data(), left.data(), exceptions.data(), positions.data(), excp_count.data(), state);
    ASSERT_EQ(data, output);
}

TEST(ALP, RoundtripRDWideRange) {
    auto data = random_doubles(1024, std::numeric_limits<double>::lowest(), std::numeric_limits<double>::max());
    alp::state<double> state;
    std::array<double, 1024> sample_buf;

    alp::encoder<double>::init(data.data(), 0, 1024, sample_buf.data(), state);
    log::codec().info("state: {}", state.exceptions_count);
    ASSERT_EQ(state.scheme, alp::Scheme::ALP_RD);

    std::array<uint16_t, BLOCK_SIZE> exceptions;
    std::array<uint16_t, BLOCK_SIZE> positions;
    std::array<uint16_t, BLOCK_SIZE> excp_count;
    std::array<uint64_t, BLOCK_SIZE> right;
    std::array<uint16_t, BLOCK_SIZE> left;

    alp::rd_encoder<double>::init(data.data(), 0, 1024, sample_buf.data(), state);
    alp::rd_encoder<double>::encode(data.data(), exceptions.data(), positions.data(), excp_count.data(), right.data(), left.data(), state);

    std::vector<double> output(1024);
    alp::rd_encoder<double>::decode(output.data(), right.data(), left.data(), exceptions.data(), positions.data(), excp_count.data(), state);
    ASSERT_EQ(data, output);
}

TEST(ALP, RoundtripLong) {
    constexpr size_t NUM_VALUES = BLOCK_SIZE * 100;
    auto data = random_doubles(NUM_VALUES);
    alp::state<double> state;
    std::array<double, 1024> sample_buf;

    alp::encoder<double>::init(data.data(), 0, NUM_VALUES, sample_buf.data(), state);
    log::codec().info("state: {}", state.exceptions_count);
    ASSERT_EQ(state.scheme, alp::Scheme::ALP_RD);

    std::array<uint16_t, NUM_VALUES> exceptions;
    std::array<uint16_t, NUM_VALUES> positions;
    std::array<uint16_t, NUM_VALUES> excp_count;
    std::array<uint64_t, NUM_VALUES> right;
    std::array<uint16_t, NUM_VALUES> left;

    auto num_blocks = NUM_VALUES / alp::config::VECTOR_SIZE;
    std::vector<alp::state<double>> states(num_blocks);
    for(auto i = 0UL; i < num_blocks; ++i) {
        const auto pos = i * alp::config::VECTOR_SIZE;
        alp::rd_encoder<double>::init(data.data(), pos, NUM_VALUES, sample_buf.data(), states[i]);
        alp::rd_encoder<double>::encode(
            data.data() + pos,
            exceptions.data() + pos,
            positions.data() + pos,
            excp_count.data() + pos,
            right.data() + pos,
            left.data() + pos,
            states[i]);
    }
    std::vector<double> output(NUM_VALUES);
    for(auto i = 0UL; i < num_blocks; ++i) {
        const auto pos = i * alp::config::VECTOR_SIZE;
        alp::rd_encoder<double>::decode(
            output.data() + pos,
            right.data() + pos,
            left.data() + pos,
            exceptions.data() + pos,
            positions.data() + pos,
            excp_count.data() + pos,
            states[i]);
    }
    for(auto i = 0UL; i < data.size(); ++i) {
        if(data[i] != output[i])
            log::codec().info("pants");
    }
    ASSERT_EQ(data, output);
}

template <typename T>
size_t max_block_size(const RealDoubleBitwidths& bit_widths) {
    RealDoubleBlockHeader<T> block_header_;
    block_header_.exception_count_ = BLOCK_SIZE;
    return block_header_.total_size(bit_widths);
}

TEST(ALP, RoundtripRealDoubleContiguous) {
    constexpr size_t NUM_VALUES = BLOCK_SIZE * 100;
    using T = double;
    auto data = random_doubles(NUM_VALUES);
    alp::state<T> state;
    std::array<T, 1024> sample_buf;

    alp::encoder<T>::init(data.data(), 0, NUM_VALUES, sample_buf.data(), state);
    ASSERT_EQ(state.scheme, alp::Scheme::ALP_RD);
    alp::rd_encoder<T>::init(data.data(), 0, NUM_VALUES, sample_buf.data(), state);
    RealDoubleColumnHeader<T> column_header{state};
    auto num_blocks = NUM_VALUES / alp::config::VECTOR_SIZE;
    auto total_size = num_blocks * max_block_size<T>(column_header.bit_widths()) + column_header.total_size();

    std::vector<uint8_t> compressed(total_size);
    memcpy(compressed.data(), &column_header, column_header.total_size());
    size_t write_pos = column_header.total_size();
    std::array<uint16_t, BLOCK_SIZE> exceptions;
    std::array<uint16_t, BLOCK_SIZE> exception_positions;
    for(auto i = 0UL; i < num_blocks; ++i) {
        const auto pos = i * alp::config::VECTOR_SIZE;
        auto header = new (compressed.data() + write_pos) RealDoubleBlockHeader<double>{};
        uint16_t exception_count;
        alp::rd_encoder<double>::encode(
            data.data() + pos,
            exceptions.data(),
            exception_positions.data(),
            &exception_count,
            header->right(column_header.bit_widths()),
            header->left(),
            state);

        header->exception_count_ = exception_count;
        if(exception_count > 0) {
            memcpy(header->exceptions(column_header.bit_widths()), exceptions.data(), exception_count * sizeof(uint16_t));
            memcpy(header->exception_positions(column_header.bit_widths()), exception_positions.data(), exception_count * sizeof(uint16_t));
        }
        write_pos += header->total_size(column_header.bit_widths());
    }

    std::vector<T> output(NUM_VALUES);
    alp::state<T> restored_state;
    auto restored_header = reinterpret_cast<RealDoubleColumnHeader<T>*>(compressed.data());
    auto read_pos = restored_header->total_size();
    set_real_double_state_from_column_header(restored_state, *restored_header);
    for(auto i = 0UL; i < num_blocks; ++i) {
        const auto pos = i * alp::config::VECTOR_SIZE;
        auto* header = reinterpret_cast<RealDoubleBlockHeader<double>*>(compressed.data() + read_pos);
        header->magic_.check();
        uint16_t exception_count = header->exceptions_count();
        update_read_double_state_from_block_header(restored_state, *header);
        alp::rd_encoder<double>::decode(
            output.data() + pos,
            header->right(restored_header->bit_widths()),
            header->left(),
            header->exceptions(restored_header->bit_widths()),
            header->exception_positions(restored_header->bit_widths()),
            &exception_count,
            restored_state);
        
        read_pos += header->total_size(restored_header->bit_widths());
    }
    for(auto i = 0UL; i < data.size(); ++i) {
        if(data[i] != output[i])
            log::codec().info("pants");
    }
    ASSERT_EQ(data, output);
}

TEST(ALP, DetermineSchemeALP) {
    auto data = random_decimal_floats(0, 1000, 2, 100 * 1024);
    alp::state<double> state;
    std::array<double, 1024> sample_buf;

    alp::encoder<double>::init(data.data(), 0, 1024, sample_buf.data(), state);
    log::codec().info("state: {}", state.exceptions_count);
    ASSERT_NE(state.scheme, alp::Scheme::INVALID);
    ASSERT_EQ(state.scheme, alp::Scheme::ALP);
}

TEST(ALP, RoundtripALP) {
    auto data = random_decimal_floats(0, 1000, 2, 1024);
    alp::state<double> state;
    std::array<double, 1024> sample_buf;

    alp::encoder<double>::init(data.data(), 0, 1024, sample_buf.data(), state);
    log::codec().info("state: {}", state.exceptions_count);
    ASSERT_EQ(state.scheme, alp::Scheme::ALP);

    std::array<double, BLOCK_SIZE> exceptions;
    std::array<uint16_t, BLOCK_SIZE> exception_positions;
    uint16_t exception_count = 0;
    std::array<int64_t, BLOCK_SIZE> encoded;

    alp::encoder<double>::init(data.data(), 0, 1024, sample_buf.data(), state);
    alp::encoder<double>::encode(data.data(), exceptions.data(), exception_positions.data(), &exception_count, encoded.data(), state);

    std::vector<double> output(1024);
    alp::decoder<double>::decode(encoded.data(), state.fac, state.exp, output.data());
    alp::decoder<double>::patch_exceptions(output.data(), exceptions.data(), exception_positions.data(), &exception_count);
    ASSERT_EQ(data, output);
}

template <typename T>
constexpr size_t max_block_size() {
    using EncodedType = typename StorageType<T>::signed_type;
    return ALPDecimalBlockHeader<T>::HeaderSize +
        alp::config::VECTOR_SIZE * (sizeof(EncodedType) + sizeof(T) + sizeof(uint16_t));
}

TEST(ALP, RoundtripALPDecimalContiguous) {
    constexpr size_t NUM_VALUES = BLOCK_SIZE * 100;

    auto data = random_decimal_floats(0, 1000, 2, NUM_VALUES);
    using T = double;
    alp::state<double> state;
    std::array<double, 1024> sample_buf;

    auto num_blocks = NUM_VALUES / alp::config::VECTOR_SIZE;
    alp::encoder<double>::init(data.data(), 0, NUM_VALUES, sample_buf.data(), state);
    ASSERT_EQ(state.scheme, alp::Scheme::ALP);

    auto total_size = num_blocks * max_block_size<T>() + sizeof(ALPDecimalColumnHeader<T>);
    std::vector<uint8_t> compressed(total_size);
    size_t write_pos = 0UL;
    std::array<T, alp::config::VECTOR_SIZE> exceptions;
    using EncodedType = StorageType<T>::signed_type;
    std::array<EncodedType, alp::config::VECTOR_SIZE> encoded;
    std::array<uint16_t, alp::config::VECTOR_SIZE> exception_positions;
    for(auto i = 0UL; i < num_blocks; ++i) {
        const auto pos = i * alp::config::VECTOR_SIZE;
        auto header = new (compressed.data() + write_pos) ALPDecimalBlockHeader<double>{};
        uint16_t exception_count;
        alp::encoder<double>::encode(
            data.data() + pos,
            exceptions.data(),
            exception_positions.data(),
            &exception_count,
            encoded.data(),
            state);

        header->exception_count_ = exception_count;
        header->exp_ = state.exp;
        header->fac_ = state.fac;
        if(exception_count > 0) {
            memcpy(header->exceptions(), exceptions.data(), exception_count * sizeof(uint16_t));
            memcpy(header->exception_positions(), exception_positions.data(), exception_count * sizeof(uint16_t));
        }

        alp::encoder<T>::analyze_ffor(encoded.data(), state.bit_width, header->bases());
        ffor::ffor(encoded.data(), header->data(), state.bit_width, header->bases());
        header->bit_width_ = state.bit_width;
        write_pos += header->total_size();
    }

    std::vector<double> output(NUM_VALUES);
    std::array<EncodedType, alp::config::VECTOR_SIZE> unffor;
    alp::state<T> restored_state;
    auto read_pos = 0UL;
    for(auto i = 0UL; i < num_blocks; ++i) {
        const auto pos = i * alp::config::VECTOR_SIZE;
        auto* header = reinterpret_cast<const ALPDecimalBlockHeader<T>*>(compressed.data() + read_pos);
        header->magic_.check();
        update_state_from_alp_decimal_block_header(restored_state, *header);
        unffor::unffor(header->data(), unffor.data(), restored_state.bit_width, header->bases());

        alp::decoder<T>::decode(
            unffor.data(),
            restored_state.fac,
            restored_state.exp,
            output.data() + pos);

        alp::decoder<T>::patch_exceptions(
            output.data() + pos,
            header->exceptions(),
            header->exception_positions(),
            &header->exception_count_);

        read_pos += header->total_size();
    }
    for(auto i = 0UL; i < data.size(); ++i) {
        if(data[i] != output[i])
            log::codec().info("pants");
    }
    ASSERT_EQ(data, output);
}

} //namespace arcticdb