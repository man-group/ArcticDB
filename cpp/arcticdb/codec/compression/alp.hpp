#pragma once

#include <cstddef>
#include <cstdint>

#include <arcticdb/column_store/column_data.hpp>
#include <arcticdb/codec/compression/encoder_data.hpp>
#include <arcticdb/codec/compression/alp/rd.hpp>
#include <arcticdb/codec/compression/alp_header.hpp>

namespace arcticdb {

template<typename T>
struct __attribute__((packed)) ALPHeader {
    uint32_t num_rows_;
    alp::Scheme scheme_;
};

constexpr std::size_t round_up_bits(std::size_t bits) noexcept {
    return (bits + (CHAR_BIT - 1)) / CHAR_BIT;
}

template <typename T>
size_t required_alp_size(const ALPCompressData<T>& data) {
    return ALPDecimalHeader<T>::HeaderSize +
        (data.max_exceptions_ * alp::RD_EXCEPTION_SIZE) +
        (data.max_exceptions_ * alp::EXCEPTION_POSITION_SIZE) +
        round_up_bits(data.max_bit_width_ * alp::config::VECTOR_SIZE);
}

template <typename T>
size_t required_rd_size(const ALPCompressData<T>& data) {
    return ALPDecimalHeader<T>::HeaderSize +
        (data.max_exceptions_ * alp::RD_EXCEPTION_SIZE) +
        (data.max_exceptions_ * alp::EXCEPTION_POSITION_SIZE) +
        round_up_bits(data.max_bit_width_ * alp::config::VECTOR_SIZE) +
        round_up_bits(data.max_bit_width_ * alp::config::VECTOR_SIZE) +
        alp::config::MAX_RD_DICTIONARY_SIZE * sizeof(uint16_t);
}

template<typename T>
struct ALPCompressor {
    size_t full_blocks_ = 0UL;
    size_t compressed_rows_ = 0UL;
    size_t remainder_ = 0UL;
    ALPCompressData<T> data_;

    explicit ALPCompressor(ALPCompressData<T>&& compress_data) :
        data_(std::move(compress_data)) {
    }

    size_t calculate_full_blocks(size_t rows) {
        if(rows < alp::config::VECTOR_SIZE)
            return 0;

        return rows / BLOCK_SIZE;
    }

    [[nodiscard]] size_t remainder_offset() const {
        return full_blocks_ * BLOCK_SIZE;
    }

    size_t scan(const ColumnData input, size_t rows) {
        full_blocks_ = calculate_full_blocks(rows);
        compressed_rows_ = rows;
        remainder_ = compressed_rows_ % BLOCK_SIZE;
        const auto remainder_blocks = remainder_ > 0 ? 1 : 0;
        data_.states_.resize(full_blocks_ + remainder_blocks);
        ARCTICDB_DEBUG(log::codec(), "Compressing {} rows, {} total blocks with remainder of {}", rows, full_blocks_, remainder_);
        std::array<T, alp::config::VECTOR_SIZE> sample_buf;
        size_t total_size = sizeof(ALPHeader<T>);
        if (full_blocks_ > 0) {
            ARCTICDB_DEBUG(log::codec(), "Total size including header: {}", total_size);

            T max_delta = std::numeric_limits<T>::lowest();
            if(input.num_blocks() == 1) {
                auto ptr = reinterpret_cast<const T*>(input.buffer().data());
                switch(data_.scheme_) {
                case alp::Scheme::ALP: {
                    for (auto i = 1UL; i < full_blocks_; ++i) {
                        //TODO check if init is required
                        alp::encoder<T>::init(ptr, 0, alp::config::VECTOR_SIZE, sample_buf.data(), data_.states_[i]);
                        total_size += required_alp_size(data_);
                    }
                }
                case alp::Scheme::ALP_RD: {
                    for (auto i = 1UL; i < full_blocks_; ++i) {
                        alp::rd_encoder<double>::init(ptr, 0, 1024, sample_buf.data(), data_.states_[i]);
                        total_size += required_rd_size(data_);
                    }
                }
                default:
                    util::raise_rte("Unhandled ALP scheme type");
                }
            } else {
                ContiguousRangeForwardAdaptor<T, BLOCK_SIZE> adaptor(input);
                auto current = *input.buffer().data();

                switch(data_.scheme_) {
                case alp::Scheme::ALP: {
                    for(auto i = 0UL; i < full_blocks_; ++i) {
                        auto ptr = adaptor.next();
                        alp::encoder<T>::init(ptr, 0, alp::config::VECTOR_SIZE, sample_buf.data(), data_.states_[i]);
                        total_size += required_alp_size(data_);
                    }
                }
                case alp::Scheme::ALP_RD: {
                     for(auto i = 0UL; i < full_blocks_; ++i) {
                        auto ptr = adaptor.next();
                        alp::rd_encoder<double>::init(ptr, 0, 1024, sample_buf.data(), data_.states_[i]);
                        total_size += required_rd_size(data_);
                    }
                }
                default:
                    util::raise_rte("Unhandled ALP scheme type");
                }
            }
        }

        ARCTICDB_DEBUG(log::codec(), "Total size including full blocks: {}", total_size);
        if (remainder_ > 0) {
            std::array<T, alp::config::VECTOR_SIZE> remainder_data;
            DynamicRangeRandomAccessAdaptor<T> adaptor{input};
            const T* remainder_ptr = adaptor.at(remainder_offset(), remainder_);
            std::fill(std::begin(remainder_data), std::end(remainder_data), 0.0);
            std::copy(remainder_data, remainder_ptr, remainder_);
            switch(data_.scheme_) {
            case alp::Scheme::ALP: {
                for(auto i = 0UL; i < full_blocks_; ++i) {
                    alp::encoder<T>::init(remainder_data.data(), 0, alp::config::VECTOR_SIZE, sample_buf.data(), data_.states_[i]);
                    total_size += required_alp_size(data_);
                }
            }
            case alp::Scheme::ALP_RD: {
                for(auto i = 0UL; i < full_blocks_; ++i) {
                    alp::rd_encoder<double>::init(remainder_data.data(), 0, 1024, sample_buf.data(), data_.states_[i]);
                    total_size += required_rd_size(data_);
                }
            }
            default:
                util::raise_rte("Unhandled ALP scheme type");
            }

        }
        ARCTICDB_DEBUG(log::codec(), "Total size including remainder: {}", total_size);
        return total_size;
    }

    template <typename T>
    void write_real_double_data(
        const T* data,
        std::array<T, alp::config::VECTOR_SIZE>& exceptions,
        std::array<T, alp::config::VECTOR_SIZE>& exception_positions,
        alp::state<T> state,
        uint8_t* out,
        size_t& write_pos
        ) {

        auto header = new (out + write_pos) RealDoubleHeader<double>{state};
        uint16_t exception_count;
        alp::rd_encoder<double>::encode(
            data,
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

    void write_decimal_data(
        const T* data,
        std::array<T, alp::config::VECTOR_SIZE>& exceptions,
        std::array<T, alp::config::VECTOR_SIZE>& exception_positions,
        alp::state<T> state,
        uint8_t* out,
        size_t& write_pos
    ) {

       auto header = new (out + write_pos) ALPDecimalHeader<double>{state};
        uint16_t exception_count;
        alp::encoder<double>::encode(
            data,
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
        header->exp_ = state.exp;
        header->fac_ = state.fac;
        write_pos += header->total_size();
    }

    size_t compress(ColumnData input, T *__restrict out, size_t rows) {
        full_blocks_ = calculate_full_blocks(rows);
        compressed_rows_ = rows;
        remainder_ = compressed_rows_ % BLOCK_SIZE;
        const auto remainder_blocks = remainder_ > 0 ? 1 : 0;
        data_.states_.resize(full_blocks_ + remainder_blocks);
        ARCTICDB_DEBUG(log::codec(), "Compressing {} rows, {} total blocks with remainder of {}", count, full_blocks_, remainder_);
        std::array<T, alp::config::VECTOR_SIZE> sample_buf;
        size_t total_size = sizeof(ALPHeader<T>);

        std::array<T, BLOCK_SIZE> exceptions;
        std::array<uint16_t, BLOCK_SIZE> exception_positions;
        if (full_blocks_ > 0) {
            ARCTICDB_DEBUG(log::codec(), "Total size including header: {}", total_size);

            T max_delta = std::numeric_limits<T>::lowest();
            if(input.num_blocks() == 1) {
                auto ptr = reinterpret_cast<const T*>(input.buffer().data());
                switch(data_.scheme_) {
                case alp::Scheme::ALP: {
                    for (auto i = 1UL; i < full_blocks_; ++i) {
                        write_decimal_data(ptr, exceptions, exception_positions, data_.states_[i], out, total_size);
                    }
                }
                case alp::Scheme::ALP_RD: {
                    for (auto i = 1UL; i < full_blocks_; ++i) {
                        write_real_double_data(ptr, exceptions, exception_positions, data_.states_[i], out, total_size);
                    }
                }
                default:
                    util::raise_rte("Unhandled ALP scheme type");
                }
            } else {
                ContiguousRangeForwardAdaptor<T, BLOCK_SIZE> adaptor(input);
                auto current = *input.buffer().data();

                switch(data_.scheme_) {
                case alp::Scheme::ALP: {
                    for(auto i = 0UL; i < full_blocks_; ++i) {
                        auto ptr = adaptor.next();
                        write_decimal_data(ptr, exceptions, exception_positions, data_.states_[i], out, total_size);
                    }
                }
                case alp::Scheme::ALP_RD: {
                     for(auto i = 0UL; i < full_blocks_; ++i) {
                        auto ptr = adaptor.next();
                         write_real_double_data(ptr, exceptions, exception_positions, data_.states_[i], out, total_size);
                    }
                }
                default:
                    util::raise_rte("Unhandled ALP scheme type");
                }
            }
        }

        ARCTICDB_DEBUG(log::codec(), "Total size including full blocks: {}", total_size);
        if (remainder_ > 0) {
            std::array<T, alp::config::VECTOR_SIZE> remainder_data;
            DynamicRangeRandomAccessAdaptor<T> adaptor{input};
            const T* remainder_ptr = adaptor.at(remainder_offset(), remainder_);
            std::fill(std::begin(remainder_data), std::end(remainder_data), 0.0);
            std::copy(remainder_data, remainder_ptr, remainder_);
            switch(data_.scheme_) {
            case alp::Scheme::ALP: {
                for(auto i = 0UL; i < full_blocks_; ++i) {
                    write_decimal_data(remainder_data.data(), exceptions, exception_positions, data_.states_[i], out, total_size);
                }
            }
            case alp::Scheme::ALP_RD: {
                for(auto i = 0UL; i < full_blocks_; ++i) {
                    write_real_double_data(remainder_data.data(), exceptions, exception_positions, data_.states_[i], out, total_size);
                }
            }
            default:
                util::raise_rte("Unhandled ALP scheme type");
            }
        }
        ARCTICDB_DEBUG(log::codec(), "Total size including remainder: {}", total_size);
        return total_size;
    }
};

template<typename T>
struct ALPDecompressor {
    static size_t decompress(const T *__restrict /*in*/, T *__restrict /*out*/, size_t expected_count) {
        return expected_count * sizeof(T);
    }
};

} // namespace arcticdb