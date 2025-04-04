#pragma once

#include <cstddef>
#include <cstdint>

#include <arcticdb/column_store/column_data.hpp>
#include <arcticdb/codec/compression/encoder_data.hpp>
#include <arcticdb/codec/compression/alp/rd.hpp>
#include <arcticdb/codec/compression/alp_header.hpp>
#include <arcticdb/codec/compression/compressor.hpp>
#include <arcticdb/codec/compression/compression_utils.hpp>

namespace arcticdb {

template<typename T>
struct __attribute__((packed)) ALPHeader {
    uint32_t num_rows_;
    alp::Scheme scheme_;
};

template<typename T>
size_t required_alp_size(const ALPCompressData<T> &data) {
    return ALPDecimalHeader<T>::HeaderSize +
        (data.max_exceptions_ * alp::RD_EXCEPTION_SIZE) +
        (data.max_exceptions_ * alp::EXCEPTION_POSITION_SIZE) +
        round_up_bits(data.max_bit_width_ * alp::config::VECTOR_SIZE);
}

template<typename T>
size_t required_rd_size(const ALPCompressData<T> &data) {
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

    explicit ALPCompressor(ALPCompressData<T> &&compress_data) :
        data_(std::move(compress_data)) {
    }


   /* explicit ALPCompressor(const ALPCompressData<T> &compress_data) :
        data_(compress_data) {
        util::check(data_.scheme_.empty(), "Copying states array");
    } */

    size_t calculate_full_blocks(size_t rows) {
        if (rows < alp::config::VECTOR_SIZE)
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
        ARCTICDB_DEBUG(log::codec(),
                       "Compressing {} rows, {} total blocks with remainder of {}",
                       rows,
                       full_blocks_,
                       remainder_);
        std::array<T, alp::config::VECTOR_SIZE> sample_buf;
        size_t total_size = sizeof(ALPHeader<T>);
        if (full_blocks_ > 0) {
            ARCTICDB_DEBUG(log::codec(), "Total size including header: {}", total_size);

            T max_delta = std::numeric_limits<T>::lowest();
            if (input.num_blocks() == 1) {
                auto ptr = reinterpret_cast<const T *>(input.buffer().data());
                switch (data_.scheme_) {
                case alp::Scheme::ALP: {
                    for (auto i = 1UL; i < full_blocks_; ++i) {
                        //TODO check if init is required
                        alp::encoder<T>::init(ptr, 0, alp::config::VECTOR_SIZE, sample_buf.data(), data_.states_[i]);
                        total_size += required_alp_size(data_);
                    }
                }
                case alp::Scheme::ALP_RD: {
                    for (auto i = 1UL; i < full_blocks_; ++i) {
                        alp::rd_encoder<T>::init(ptr, 0, 1024, sample_buf.data(), data_.states_[i]);
                        total_size += required_rd_size(data_);
                    }
                }
                default:util::raise_rte("Unhandled ALP scheme type");
                }
            } else {
                ContiguousRangeForwardAdaptor<T, BLOCK_SIZE> adaptor(input);
                switch (data_.scheme_) {
                case alp::Scheme::ALP: {
                    for (auto i = 0UL; i < full_blocks_; ++i) {
                        auto ptr = adaptor.next();
                        alp::encoder<T>::init(ptr, 0, alp::config::VECTOR_SIZE, sample_buf.data(), data_.states_[i]);
                        total_size += required_alp_size(data_);
                    }
                }
                case alp::Scheme::ALP_RD: {
                    for (auto i = 0UL; i < full_blocks_; ++i) {
                        auto ptr = adaptor.next();
                        alp::rd_encoder<T>::init(ptr, 0, 1024, sample_buf.data(), data_.states_[i]);
                        total_size += required_rd_size(data_);
                    }
                }
                default:util::raise_rte("Unhandled ALP scheme type");
                }
            }
        }

        ARCTICDB_DEBUG(log::codec(), "Total size including full blocks: {}", total_size);
        if (remainder_ > 0) {
            std::array<T, alp::config::VECTOR_SIZE> remainder_data;
            DynamicRangeRandomAccessAdaptor<T> adaptor{input};
            const T *remainder_ptr = adaptor.at(remainder_offset(), remainder_);
            std::fill(std::begin(remainder_data), std::end(remainder_data), 0.0);
            std::copy(remainder_data, remainder_ptr, remainder_);
            switch (data_.scheme_) {
            case alp::Scheme::ALP: {
                for (auto i = 0UL; i < full_blocks_; ++i) {
                    alp::encoder<T>::init(
                        remainder_data.data(),
                        0,
                        alp::config::VECTOR_SIZE,
                        sample_buf.data(),
                        data_.states_[i]);
                    total_size += required_alp_size(data_);
                }
            }
            case alp::Scheme::ALP_RD: {
                for (auto i = 0UL; i < full_blocks_; ++i) {
                    alp::rd_encoder<T>::init(remainder_data.data(), 0, 1024, sample_buf.data(), data_.states_[i]);
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

    void write_real_double_data(
            const T *data,
            std::array<uint16_t, alp::config::VECTOR_SIZE> &exceptions,
            std::array<uint16_t, alp::config::VECTOR_SIZE> &exception_positions,
            alp::state<T> state,
            uint8_t *out,
            size_t &write_pos) {
        auto header = new(out + write_pos) RealDoubleHeader<T>{state};
        uint16_t exception_count;
        alp::rd_encoder<T>::encode(
            data,
            exceptions.data(),
            exception_positions.data(),
            &exception_count,
            header->right(),
            header->left(),
            state);

        header->exception_count_ = exception_count;
        if (exception_count > 0) {
            memcpy(header->exceptions(), exceptions.data(), exception_count * sizeof(uint16_t));
            memcpy(header->exception_positions(), exception_positions.data(), exception_count * sizeof(uint16_t));
        }
        header->set_dict(state.left_parts_dict, state.actual_dictionary_size);
        // header->exception_count_ = exception_count;
        write_pos += header->total_size();
    }

    void write_decimal_data(
        const T *data,
        std::array<T, alp::config::VECTOR_SIZE> &exceptions,
        std::array<uint16_t, alp::config::VECTOR_SIZE> &exception_positions,
        alp::state<T> state,
        uint8_t *out,
        size_t &write_pos) {
        auto header = new(out + write_pos) ALPDecimalHeader<T>{state};
        uint16_t exception_count;
        alp::encoder<T>::encode(
            data,
            exceptions.data(),
            exception_positions.data(),
            &exception_count,
            header->data(),
            state);

        header->exception_count_ = exception_count;
        if (exception_count > 0) {
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
        ARCTICDB_DEBUG(log::codec(),
                       "Compressing {} rows, {} total blocks with remainder of {}",
                       rows,
                       full_blocks_,
                       remainder_);
        size_t total_size = sizeof(ALPHeader<T>);
        auto header [[maybe_unused]] = new(out) ALPHeader<T>{.num_rows_ = static_cast<uint32_t>(rows), .scheme_ = data_.scheme_};
        std::array<uint16_t, BLOCK_SIZE> exception_positions;
        auto out_ptr = reinterpret_cast<uint8_t*>(out);
        if (full_blocks_ > 0) {
            ARCTICDB_DEBUG(log::codec(), "Total size including header: {}", total_size);

            if (input.num_blocks() == 1) {
                auto ptr = reinterpret_cast<const T *>(input.buffer().data());
                switch (data_.scheme_) {
                case alp::Scheme::ALP: {
                    std::array<T, BLOCK_SIZE> exceptions;
                    for (auto i = 1UL; i < full_blocks_; ++i) {
                        write_decimal_data(ptr, exceptions, exception_positions, data_.states_[i], out_ptr, total_size);
                    }
                }
                case alp::Scheme::ALP_RD: {
                    std::array<uint16_t, BLOCK_SIZE> exceptions;
                    for (auto i = 1UL; i < full_blocks_; ++i) {
                        write_real_double_data(ptr, exceptions, exception_positions, data_.states_[i], out_ptr, total_size);
                    }
                }
                default:util::raise_rte("Unhandled ALP scheme type");
                }
            } else {
                ContiguousRangeForwardAdaptor<T, BLOCK_SIZE> adaptor(input);
                switch (data_.scheme_) {
                case alp::Scheme::ALP: {
                    std::array<T, BLOCK_SIZE> exceptions;
                    for (auto i = 0UL; i < full_blocks_; ++i) {
                        auto ptr = adaptor.next();
                        write_decimal_data(ptr, exceptions, exception_positions, data_.states_[i], out_ptr, total_size);
                    }
                }
                case alp::Scheme::ALP_RD: {
                    std::array<uint16_t, BLOCK_SIZE> exceptions;
                    for (auto i = 0UL; i < full_blocks_; ++i) {
                        auto ptr = adaptor.next();
                        write_real_double_data(ptr, exceptions, exception_positions, data_.states_[i], out_ptr, total_size);
                    }
                }
                default:util::raise_rte("Unhandled ALP scheme type");
                }
            }
        }

        ARCTICDB_DEBUG(log::codec(), "Total size including full blocks: {}", total_size);
        if (remainder_ > 0) {
            std::array<T, alp::config::VECTOR_SIZE> remainder_data;
            DynamicRangeRandomAccessAdaptor<T> adaptor{input};
            const T *remainder_ptr = adaptor.at(remainder_offset(), remainder_);
            std::fill(std::begin(remainder_data), std::end(remainder_data), 0.0);
            mempcpy(remainder_data.data(), remainder_ptr, remainder_ * sizeof(T));
            switch (data_.scheme_) {
            case alp::Scheme::ALP: {
                std::array<T, BLOCK_SIZE> exceptions;
                for (auto i = 0UL; i < full_blocks_; ++i) {
                    write_decimal_data(remainder_data.data(),
                                       exceptions,
                                       exception_positions,
                                       data_.states_[i],
                                       out_ptr,
                                       total_size);
                }
            }
            case alp::Scheme::ALP_RD: {
                std::array<uint16_t, BLOCK_SIZE> exceptions;
                for (auto i = 0UL; i < full_blocks_; ++i) {
                    write_real_double_data(remainder_data.data(),
                                           exceptions,
                                           exception_positions,
                                           data_.states_[i],
                                           out_ptr,
                                           total_size);
                }
            }
            default:util::raise_rte("Unhandled ALP scheme type");
            }
        }
        ARCTICDB_DEBUG(log::codec(), "Total size including remainder: {}", total_size);
        return total_size;
    }
};

template<typename T>
void set_state_from_header(alp::state<T> &state, const RealDoubleHeader<T>& header) {
    state.exceptions_count = header.exceptions_count();
    memcpy(state.left_parts_dict, header.dict(), header.dict_size());
    state.actual_dictionary_size_bytes = header.dict_size();
    state.actual_dictionary_size = header.dict_size() / sizeof(uint16_t);
    state.right_bit_width = header.right_bit_width_;
    state.left_bit_width = header.left_bit_width_;
}

template<typename T>
struct ALPDecompressor {
    using Header = ALPHeader<T>;

private:
    const Header *header_;
    size_t full_blocks_;
    size_t remainder_;

    [[nodiscard]] size_t calculate_input_offset() const {
        return sizeof(DeltaHeader<T>) / sizeof(T);
    }

public:
    void init(const T *input) {
        header_ = reinterpret_cast<const Header *>(input);
        full_blocks_ = header_->num_rows_ / BLOCK_SIZE;
        remainder_ = header_->num_rows_ % BLOCK_SIZE;
    }

    [[nodiscard]] size_t num_rows() const { return header_->num_rows_; }

    DecompressResult decompress(const T *input, T *output) {
        size_t read_pos = calculate_input_offset();
        auto write_pos = 0UL;
        if (full_blocks_ > 0) {
            for (size_t block = 0; block < full_blocks_; ++block) {
                switch (header_->scheme_) {
                case alp::Scheme::ALP: {
                    for (auto i = 1UL; i < full_blocks_; ++i) {
                        auto *header = reinterpret_cast<const ALPDecimalHeader<T>*>(input + read_pos);
                        header->magic_.check();
                        uint16_t exception_count = header->exceptions_count();
                        alp::decoder<T>::decode(
                            reinterpret_cast<const StorageType<T>::signed_type *>(header->data()),
                            header->fac_,
                            header->exp_,
                            output + write_pos);

                        alp::decoder<T>::patch_exceptions(
                            output + write_pos,
                            header->exceptions(),
                            header->exception_positions(),
                            &exception_count);

                        read_pos += header->total_size();
                        write_pos += alp::config::VECTOR_SIZE;
                    }
                }
                case alp::Scheme::ALP_RD: {
                    alp::state<T> restored_state;
                    for (auto i = 1UL; i < full_blocks_; ++i) {
                        auto *header = reinterpret_cast<RealDoubleHeader<T>*>(const_cast<T*>(input) + read_pos);
                        header->magic_.check();
                        uint16_t exception_count = header->exceptions_count();
                        set_state_from_header(restored_state, *header);
                        alp::rd_encoder<T>::decode(
                            output + write_pos,
                            header->right(),
                            header->left(),
                            header->exceptions(),
                            header->exception_positions(),
                            &exception_count,
                            restored_state);

                        read_pos += header->total_size();
                    }
                }
                default:util::raise_rte("Unhandled ALP scheme type");
                }
            }
        }

        if (remainder_ > 0) {
            ARCTICDB_DEBUG(log::codec(), " ALP decompressing remainder at offset {}", write_pos);
            std::array<T, alp::config::VECTOR_SIZE> remainder_data;
            switch (header_->scheme_) {
            case alp::Scheme::ALP: {
                auto *header = reinterpret_cast<ALPDecimalHeader<T>*>(const_cast<T*>(input) + read_pos);
                header->magic_.check();
                uint16_t exception_count = header->exceptions_count();
                alp::decoder<T>::decode(
                    reinterpret_cast<const StorageType<T>::signed_type*>(header->data()),
                    header->fac_,
                    header->exp_,
                    remainder_data.data());

                alp::decoder<T>::patch_exceptions(
                    output + write_pos,
                    header->exceptions(),
                    header->exception_positions(),
                    &exception_count);

                read_pos += header->total_size();
            }
            case alp::Scheme::ALP_RD: {
                alp::state<T> restored_state;
                auto *header = reinterpret_cast<RealDoubleHeader<T>*>(const_cast<T*>(input) + read_pos);
                header->magic_.check();
                uint16_t exception_count = header->exceptions_count();
                set_state_from_header(restored_state, *header);
                alp::rd_encoder<T>::decode(
                    remainder_data.data(),
                    header->right(),
                    header->left(),
                    header->exceptions(),
                    header->exception_positions(),
                    &exception_count,
                    restored_state);

                read_pos += header->total_size();
            }
            default:util::raise_rte("Unhandled ALP scheme type");
            }
            memcpy(output + write_pos, remainder_data.data(), remainder_ * sizeof(T));
            write_pos += remainder_;
        }
        return {.compressed_ = read_pos, .uncompressed_ = header_->num_rows_ * sizeof(T)};
    }

    static size_t compressed_size(const T *) {

        size_t input_offset = 0;

        return input_offset;
    }
};

} // namespace arcticdb