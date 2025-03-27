#include <gtest/gtest.h>

#include <random>

#include <arcticdb/codec/compression/alp/alp.hpp>
#include <arcticdb/log/log.hpp>

namespace arcticdb {

struct ALPCompressData {

};

struct ALPCompressor {

};


struct VectorMetadata {
    uint8_t                                    bit_width {0};
    uint16_t                                   exceptions_count {0};
    uint64_t                                   unq_c {0};
    uint16_t                                   freq {0};
    double                                     size {0};
    uint64_t                                   right_bit_width {0};
    uint64_t                                   left_bit_width {0};
    std::vector<std::pair<uint16_t, uint64_t>> repetition_vec;
    alp::Scheme                                scheme;
};


template <typename T>
VectorMetadata rd_state_to_metadata(alp::state<T> state) {
    VectorMetadata vector_metadata;
    vector_metadata.right_bit_width  = state.right_bit_width;
    vector_metadata.left_bit_width   = state.left_bit_width;
    vector_metadata.exceptions_count = state.exceptions_count;
    vector_metadata.scheme           = alp::Scheme::ALP_RD;
}

template <typename T>
VectorMetadata alp_state_to_metadata(alp::state<T> state, size_t exceptions_count) {
    VectorMetadata vector_metadata;
    vector_metadata.bit_width        = state.bit_width;
    vector_metadata.exceptions_count = exceptions_count;
    vector_metadata.scheme           = alp::Scheme::ALP;
}

template <typename T>
double calculate_alp_pde_compression_total_bytes(VectorMetadata& vector_metadata) {
    // Total bits for non-exception values
    double non_exception_bits = (alp::config::VECTOR_SIZE - vector_metadata.exceptions_count) * vector_metadata.bit_width;

    // Total bits for exceptions
    double exception_bits = static_cast<double>(vector_metadata.exceptions_count) *
        (alp::Constants<double>::EXCEPTION_SIZE + alp::EXCEPTION_POSITION_SIZE);

    // Overhead bits is assumed to be specified per value; multiply by VECTOR_SIZE.
    double overhead_bits = get_overhead_per_vector<T>() * alp::config::VECTOR_SIZE;

    // Total bits for the vector
    double total_bits = non_exception_bits + exception_bits + overhead_bits;

    // Convert to bytes.
    return total_bits / 8.0;
}
double alprd_overhead_per_vector {static_cast<double>(alp::config::MAX_RD_DICTIONARY_SIZE * 16) / alp::config::ROWGROUP_SIZE};

double calculate_alprd_compression_total_bytes(VectorMetadata& vector_metadata) {
    // For the ALP_RD scheme the base encoding uses right_bit_width and left_bit_width for every value.
    double base_bits = alp::config::VECTOR_SIZE * (vector_metadata.right_bit_width + vector_metadata.left_bit_width);

    // Exception bits for ALP_RD are added separately.
    double exception_bits = static_cast<double>(vector_metadata.exceptions_count) *
        (alp::RD_EXCEPTION_SIZE + alp::RD_EXCEPTION_POSITION_SIZE);

    // Similarly, overhead specified per value.
    double overhead_bits = alprd_overhead_per_vector;

    double total_bits = base_bits + exception_bits + overhead_bits;
    return total_bits / 8.0;
}

template <typename T>
double calculate_alp_compression_total_bytes(std::vector<VectorMetadata>& vector_metadatas) {
    double total_bytes = 0.0;
    for (auto& vector_metadata : vector_metadatas) {
        if (vector_metadata.scheme == alp::Scheme::ALP_RD) {
            total_bytes += calculate_alprd_compression_total_bytes(vector_metadata);
        } else if (vector_metadata.scheme == alp::Scheme::ALP) {
            total_bytes += calculate_alp_pde_compression_total_bytes<T>(vector_metadata);
        } else {
            throw std::runtime_error("No valid scheme is chosen");
        }
    }
    return total_bytes;
}

std::vector<double> random_doubles(size_t num_rows = 1024, double min = 0.0, double max = 1000.0) {
    using namespace arcticdb;
    auto rd = std::random_device();
    std::default_random_engine generator(rd());
    auto dist = std::uniform_real_distribution<double>(min, max);
    std::vector<double> data;
    data.reserve(num_rows);
    for(auto i = 0UL; i < num_rows; ++i) {
        data.push_back(dist(generator));
    }
    return data;
}

TEST(ALP, DetermineScheme) {
    auto data = random_doubles();
    alp::state<double> state;
    std::array<double, 1024> sample_buf;

    alp::encoder<double>::init(data.data(), 0, 1024, sample_buf.data(), state);
    log::codec().info("state: {}", state.exceptions_count);
    ASSERT_NE(state.scheme, alp::Scheme::INVALID);
    ASSERT_EQ(state.scheme, alp::Scheme::ALP_RD);
}

constexpr size_t BLOCK_SIZE = 1024;

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
    auto data = random_doubles(std::numeric_limits<double>::lowest(), std::numeric_limits<double>::max());
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

    alp::rd_encoder<double>::init(data.data(), 0, NUM_VALUES, sample_buf.data(), state);
    alp::rd_encoder<double>::encode(data.data(), exceptions.data(), positions.data(), excp_count.data(), right.data(), left.data(), state);

    std::vector<double> output(NUM_VALUES);
    alp::rd_encoder<double>::decode(output.data(), right.data(), left.data(), exceptions.data(), positions.data(), excp_count.data(), state);
    for(auto i = 0UL; i < data.size(); ++i) {
        if(data[i] != output[i])
            log::codec().info("pants");
    }
    ASSERT_EQ(data, output);
}

} //namespace arcticdb