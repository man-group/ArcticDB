#include <gtest/gtest.h>

#include <random>

#include <arcticdb/codec/compression/alp/alp.hpp>
#include <arcticdb/log/log.hpp>
#include <arcticdb/codec/compression/alp_header.hpp>

namespace arcticdb {

struct ALPCompressData {

};

struct ALPCompressor {

};

struct VectorMetadata {
    uint8_t bit_width{0};
    uint16_t exceptions_count{0};

    double size{0};
    uint64_t right_bit_width{0};
    uint64_t left_bit_width{0};

    std::vector<std::pair<uint16_t, uint64_t>> repetition_vec;
    alp::Scheme scheme;
    uint16_t left_parts_dict_[alp::config::MAX_RD_DICTIONARY_SIZE];
    uint32_t dict_size_ = 0;
};


template <typename T>
VectorMetadata rd_state_to_metadata(alp::state<T> state) {
    VectorMetadata vector_metadata;
    vector_metadata.right_bit_width  = state.right_bit_width;
    vector_metadata.left_bit_width   = state.left_bit_width;
    vector_metadata.exceptions_count = state.exceptions_count;
    vector_metadata.scheme           = alp::Scheme::ALP_RD;
    memcpy(vector_metadata.left_parts_dict_, state.left_parts_dict, state.actual_dictionary_size_bytes);
    vector_metadata.dict_size_ = state.actual_dictionary_size_bytes;
    //log::codec().info("Dict size: {}", vector_metadata.dict_size_);
    return vector_metadata;
}

template <typename T>
VectorMetadata alp_state_to_metadata(alp::state<T> state, size_t exceptions_count) {
    VectorMetadata vector_metadata;
    vector_metadata.bit_width        = state.bit_width;
    vector_metadata.exceptions_count = exceptions_count;
    vector_metadata.scheme           = alp::Scheme::ALP;
    return vector_metadata;
}

template <typename T>
alp::state<T> metadata_to_rd_state(const VectorMetadata &metadata) {
    alp::state<T> state;
    state.right_bit_width  = metadata.right_bit_width;
    state.left_bit_width   = metadata.left_bit_width;
    state.exceptions_count = metadata.exceptions_count;
    memcpy(state.left_parts_dict, metadata.left_parts_dict_, metadata.dict_size_);
    return state;
}

template <typename T>
alp::state<T> metadata_to_alp_state(const VectorMetadata &metadata) {
    alp::state<T> state;
    state.bit_width        = metadata.bit_width;
    state.exceptions_count = metadata.exceptions_count;
    return state;
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
    //auto rd = std::random_device();
    //std::default_random_engine generator(rd());
    std::default_random_engine generator(43);
    auto dist = std::uniform_real_distribution<double>(min, max);
    std::vector<double> data;
    data.reserve(num_rows);
    for(auto i = 0UL; i < num_rows; ++i) {
        data.push_back(dist(generator));
    }
    return data;
}

TEST(ALP, DetermineSchemeRealDouble) {
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


TEST(ALP, RoundtripVectorMetadata) {
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
    std::vector<VectorMetadata> states(num_blocks);
    for(auto i = 0UL; i < num_blocks; ++i) {
        const auto pos = i * alp::config::VECTOR_SIZE;
        alp::rd_encoder<double>::init(data.data(), pos, NUM_VALUES, sample_buf.data(), state);
        alp::rd_encoder<double>::encode(
            data.data() + pos,
            exceptions.data() + pos,
            positions.data() + pos,
            excp_count.data() + pos,
            right.data() + pos,
            left.data() + pos,
            state);
        states[i] = rd_state_to_metadata(state);
    }
    std::vector<double> output(NUM_VALUES);
    for(auto i = 0UL; i < num_blocks; ++i) {
        const auto pos = i * alp::config::VECTOR_SIZE;
        auto restored_state = metadata_to_rd_state<double>(states[i]);
        alp::rd_encoder<double>::decode(
            output.data() + pos,
            right.data() + pos,
            left.data() + pos,
            exceptions.data() + pos,
            positions.data() + pos,
            excp_count.data() + pos,
            restored_state);
    }
    for(auto i = 0UL; i < data.size(); ++i) {
        if(data[i] != output[i])
            log::codec().info("pants");
    }
    ASSERT_EQ(data, output);
}

template <typename T>
size_t real_double_size_from_state(const alp::state<T>&) {
    using RightType = StorageType<T>::unsigned_type;
    size_t size = RealDoubleHeader<T>::HeaderSize;
    size += alp::config::VECTOR_SIZE * sizeof(RightType); // TODO times bitwidth + round upwards to byte
    size += alp::config::VECTOR_SIZE * sizeof (uint16_t);
    size += alp::config::VECTOR_SIZE * sizeof(uint16_t); // exceptions
    size += alp::config::VECTOR_SIZE * sizeof(uint16_t); // exception positions
    size += alp::config::MAX_RD_DICTIONARY_SIZE * sizeof(uint16_t);
    return size;
}

template <typename T>
void set_state_from_header(alp::state<T>& state, const RealDoubleHeader<T>& header) {
    state.exceptions_count = header.exceptions_count();
    memcpy(state.left_parts_dict, header.dict(), header.dict_size());
    state.actual_dictionary_size_bytes = header.dict_size();
    state.actual_dictionary_size = header.dict_size() / sizeof(uint16_t);
    state.right_bit_width = header.right_bit_width_;
    state.left_bit_width = header.left_bit_width_;
}


TEST(ALP, RoundtripRealDoubleContiguous) {
    constexpr size_t NUM_VALUES = BLOCK_SIZE * 100;
    auto data = random_doubles(NUM_VALUES);
    alp::state<double> state;
    std::array<double, 1024> sample_buf;

    alp::encoder<double>::init(data.data(), 0, NUM_VALUES, sample_buf.data(), state);
    ASSERT_EQ(state.scheme, alp::Scheme::ALP_RD);

    auto num_blocks = NUM_VALUES / alp::config::VECTOR_SIZE;
    std::vector<alp::state<double>> states(num_blocks);
    auto total_size = 0UL;
    for(auto i = 0UL; i < num_blocks; ++i) {
        const auto pos = i * alp::config::VECTOR_SIZE;
        alp::rd_encoder<double>::init(data.data(), pos, NUM_VALUES, sample_buf.data(), states[i]);
        total_size += real_double_size_from_state(states[i]);
        log::codec().info("state: {}", state.exceptions_count);
    }

    std::vector<uint8_t> compressed(total_size);
    size_t write_pos = 0;
    std::array<uint16_t, BLOCK_SIZE> exceptions;
    std::array<uint16_t, BLOCK_SIZE> exception_positions;
    for(auto i = 0UL; i < 1; ++i) {
        const auto pos = i * alp::config::VECTOR_SIZE;
        auto header = new (compressed.data() + write_pos) RealDoubleHeader<double>{states[i]};
        alp::rd_encoder<double>::init(data.data(), pos, NUM_VALUES, sample_buf.data(), state);
        uint16_t exception_count;
        alp::rd_encoder<double>::encode(
            data.data() + pos,
            exceptions.data(),
            exception_positions.data(),
            &exception_count,
            header->right(),
            header->left(),
            state);

        header->exception_count_ = exception_count;
        if(exception_count > 0) {
            memcpy(header->exceptions(), exceptions.data(), exception_count * sizeof(uint16_t));
            memcpy(header->exception_positions(), exception_positions.data(), exception_count * sizeof(uint16_t));
        }
        header->set_dict(state.left_parts_dict, state.actual_dictionary_size);
       // header->exception_count_ = exception_count;
        write_pos += header->total_size();
    }

    std::vector<double> output(NUM_VALUES);
    auto read_pos = 0UL;
    alp::state<double> restored_state;
    for(auto i = 0UL; i < 1; ++i) {
        const auto pos = i * alp::config::VECTOR_SIZE;
        auto* header = reinterpret_cast<RealDoubleHeader<double>*>(compressed.data() + read_pos);
        header->magic_.check();
        uint16_t exception_count = header->exceptions_count();
        set_state_from_header(restored_state, *header);
        alp::rd_encoder<double>::decode(
            output.data() + pos,
            header->right(),
            header->left(),
            header->exceptions(),
            header->exception_positions(),
            &exception_count,
            restored_state);
        
        read_pos += header->total_size();
    }
    for(auto i = 0UL; i < data.size(); ++i) {
        if(data[i] != output[i])
            log::codec().info("pants");
    }
    ASSERT_EQ(data, output);
}
std::vector<double> random_decimal_floats(int64_t lower, int64_t upper, size_t decimal_places, size_t count) {
    std::vector<double> result;
    result.reserve(count);

    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> dis(lower, upper);

    for (auto i = 0UL; i < count; ++i) {
        auto value = dis(gen);
        result.push_back(static_cast<double>(value) / pow(10, decimal_places));
    }

    return result;
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
size_t decimal_size_from_state(const alp::state<T>&) {
    using DataType = StorageType<T>::signed_type;
    size_t size = ALPDecimalHeader<T>::HeaderSize;
    size += alp::config::VECTOR_SIZE * sizeof(DataType); // TODO times bitwidth + round upwards to byte
    size += alp::config::VECTOR_SIZE * sizeof(uint16_t); // exceptions
    size += alp::config::VECTOR_SIZE * sizeof(uint16_t); // exception positions
    return size;
}

TEST(ALP, RoundtripALPDecimalContiguous) {
    constexpr size_t NUM_VALUES = BLOCK_SIZE * 100;

    auto data = random_decimal_floats(0, 1000, 2, NUM_VALUES);

    alp::state<double> state;
    std::array<double, 1024> sample_buf;

    alp::encoder<double>::init(data.data(), 0, NUM_VALUES, sample_buf.data(), state);
    ASSERT_EQ(state.scheme, alp::Scheme::ALP);

    auto num_blocks = NUM_VALUES / alp::config::VECTOR_SIZE;
    std::vector<alp::state<double>> states(num_blocks);
    auto total_size = 0UL;
    for(auto i = 0UL; i < num_blocks; ++i) {
        const auto pos = i * alp::config::VECTOR_SIZE;
        alp::encoder<double>::init(data.data(), pos, NUM_VALUES, sample_buf.data(), states[i]);
        total_size += decimal_size_from_state(states[i]);
        log::codec().info("state: {}", state.exceptions_count);
    }

    std::vector<uint8_t> compressed(total_size);
    size_t write_pos = 0;
    std::array<double, BLOCK_SIZE> exceptions;
    std::array<uint16_t, BLOCK_SIZE> exception_positions;
    for(auto i = 0UL; i < 1; ++i) {
        const auto pos = i * alp::config::VECTOR_SIZE;
        auto header = new (compressed.data() + write_pos) ALPDecimalHeader<double>{states[i]};
        alp::rd_encoder<double>::init(data.data(), pos, NUM_VALUES, sample_buf.data(), state);
        uint16_t exception_count;
        alp::encoder<double>::encode(
            data.data() + pos,
            exceptions.data(),
            exception_positions.data(),
            &exception_count,
            header->data(),
            state);

        header->exception_count_ = exception_count;
        if(exception_count > 0) {
            memcpy(header->exceptions(), exceptions.data(), exception_count * sizeof(uint16_t));
            memcpy(header->exception_positions(), exception_positions.data(), exception_count * sizeof(uint16_t));
        }
        // header->exception_count_ = exception_count;
        write_pos += header->total_size();
    }

    std::vector<double> output(NUM_VALUES);
    auto read_pos = 0UL;
    for(auto i = 0UL; i < 1; ++i) {
        const auto pos = i * alp::config::VECTOR_SIZE;
        auto* header = reinterpret_cast<ALPDecimalHeader<double>*>(compressed.data() + read_pos);
        header->magic_.check();
        uint16_t exception_count = header->exceptions_count();
        alp::decoder<double>::decode(
            reinterpret_cast<const StorageType<double>::signed_type*>(compressed.data() + read_pos),
            header->fac_,
            header->exp_,
            output.data() + pos);

        alp::decoder<double>::patch_exceptions(
            output.data() + pos,
            header->exceptions(),
            header->exception_positions(),
            &exception_count);

        read_pos += header->total_size();
    }
    for(auto i = 0UL; i < data.size(); ++i) {
        if(data[i] != output[i])
            log::codec().info("pants");
    }
    ASSERT_EQ(data, output);
}



} //namespace arcticdb