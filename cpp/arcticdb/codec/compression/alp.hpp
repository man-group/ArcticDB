#pragma once

#include <cstddef>
#include <cstdint>

#include <arcticdb/column_store/column_data.hpp>
#include <arcticdb/codec/compression/encoder_data.hpp>
#include <arcticdb/codec/compression/alp/rd.hpp>
#include <arcticdb/codec/compression/alp_header.hpp>
#include <arcticdb/codec/compression/compressor.hpp>
#include <arcticdb/codec/compression/ffor.hpp>
#include <arcticdb/codec/compression/bitpack.hpp>
#include <arcticdb/codec/compression/compression_utils.hpp>
#include <arcticdb/codec/compression/contiguous_range_adaptor.hpp>
#include <arcticdb/util/configs_map.hpp>

namespace arcticdb {

template<typename T>
struct ARCTICDB_PACKED ALPHeader {
    uint32_t num_rows_;
    alp::Scheme scheme_;
};

template<typename T>
size_t worst_case_required_alp_size() {
    constexpr size_t max_exceptions = alp::config::VECTOR_SIZE;
    return ALPDecimalBlockHeader<T>::HeaderSize +
        (max_exceptions * alp::RD_EXCEPTION_SIZE) +
        (max_exceptions * alp::EXCEPTION_POSITION_SIZE) +
        (sizeof(typename StorageType<T>::signed_type) * alp::config::VECTOR_SIZE);
}

template<typename T>
size_t worst_case_required_rd_size() {
    constexpr size_t max_exceptions = alp::config::VECTOR_SIZE;
    return ALPDecimalBlockHeader<T>::HeaderSize +
        (max_exceptions * alp::RD_EXCEPTION_SIZE) +
        (max_exceptions * alp::EXCEPTION_POSITION_SIZE) +
        (sizeof(typename StorageType<T>::unsigned_type) * alp::config::VECTOR_SIZE) +
        (sizeof(uint16_t) * alp::config::VECTOR_SIZE) +
        alp::config::MAX_RD_DICTIONARY_SIZE * sizeof(uint16_t);
}


template<typename T>
struct ALPCompressor {
    size_t full_blocks_ = 0UL;
    size_t compressed_rows_ = 0UL;
    size_t remainder_ = 0UL;
    ALPCompressData<T> data_;

    using EncodedType = StorageType<T>::signed_type;

    explicit ALPCompressor(ALPCompressData<T> &&compress_data) :
        data_(std::move(compress_data)) {
    }

    size_t calculate_full_blocks(size_t rows) {
        if (rows < alp::config::VECTOR_SIZE)
            return 0;

        return rows / alp::config::VECTOR_SIZE;
    }

    [[nodiscard]] size_t remainder_offset() const {
        return full_blocks_ * alp::config::VECTOR_SIZE;
    }

    void write_real_double_data(
        const T *data,
        std::array<uint16_t, alp::config::VECTOR_SIZE> &left,
        std::array<typename StorageType<T>::unsigned_type , alp::config::VECTOR_SIZE> &right,
        std::array<uint16_t, alp::config::VECTOR_SIZE> &exceptions,
        std::array<uint16_t, alp::config::VECTOR_SIZE> &exception_positions,
        alp::state<T> state,
        uint8_t *out,
        size_t &write_pos,
        const RealDoubleBitwidths &bit_widths) {
        auto header = new(out + write_pos) RealDoubleBlockHeader<T>{};
        uint16_t exception_count;
        alp::rd_encoder<T>::encode(
            data,
            exceptions.data(),
            exception_positions.data(),
            &exception_count,
            right.data(),
            left.data(),
            state);

        ARCTICDB_DEBUG(log::codec(), "Left: {} Right: {}", left[0], right[0]);

        BitPackCompressKernel<uint16_t> left_kernel;
        dispatch_bitwidth_fused<uint16_t, BitPackFused>(
            left.data(),
            header->left(),
            bit_widths.left_,
            left_kernel
        );


        using RightType = StorageType<T>::unsigned_type;
        BitPackCompressKernel<RightType> right_kernel;
        dispatch_bitwidth_fused<RightType, BitPackFused>(
            right.data(),
            header->right(bit_widths),
            bit_widths.right_,
            right_kernel
        );

        ARCTICDB_DEBUG(log::codec(), "Left bitpack: {} Right bitpack: {}", *header->left(), *header->right(bit_widths));

        header->exception_count_ = exception_count;
        if (exception_count > 0) {
            memcpy(header->exceptions(bit_widths), exceptions.data(), exception_count * sizeof(uint16_t));
            memcpy(header->exception_positions(bit_widths), exception_positions.data(), exception_count * sizeof(uint16_t));
        }
        write_pos += header->total_size(bit_widths);
    }

    void write_decimal_data(
        const T *data,
        std::array<T, alp::config::VECTOR_SIZE> &exceptions,
        std::array<uint16_t, alp::config::VECTOR_SIZE> &exception_positions,
        std::array<EncodedType, alp::config::VECTOR_SIZE> encoded,
        alp::state<T> state,
        uint8_t *out,
        size_t &write_pos) {
        auto header = new(out + write_pos) ALPDecimalBlockHeader<T>{};
        uint16_t exception_count;
        alp::encoder<T>::encode(
            data,
            exceptions.data(),
            exception_positions.data(),
            &exception_count,
            encoded.data(),
            state);

        header->exception_count_ = exception_count;
        if (exception_count > 0) {
            memcpy(header->exceptions(), exceptions.data(), exception_count * sizeof(T));
            memcpy(header->exception_positions(), exception_positions.data(), exception_count * sizeof(uint16_t));
        }
        header->exp_ = state.exp;
        header->fac_ = state.fac;
        alp::encoder<T>::analyze_ffor(encoded.data(), state.bit_width, header->bases());
        FForCompressKernel<EncodedType> kernel(*header->bases());
        ARCTICDB_DEBUG(log::codec(), "First byte of encoded: {}", encoded[0]);
        dispatch_bitwidth_fused<EncodedType, BitPackFused>(
            encoded.data(),
            header->data(),
            state.bit_width,
            kernel
        );
        ARCTICDB_DEBUG(log::codec(), "First byte of ffor: {}", reinterpret_cast<EncodedType*>(header->data())[0]);
        header->bit_width_ = state.bit_width;
        write_pos += header->total_size();
    }

    void compress_alp(
            const ColumnData input,
            alp::state<T> &state,
            uint8_t *out_ptr,
            size_t &total_size,
            std::array<uint16_t, alp::config::VECTOR_SIZE> &exception_positions) {
        const auto max_comp_ratio = ConfigsMap::instance()->get_double("Alp.MaxCompressRatio", 0.8);
        const auto compress_limit = double(input.buffer().bytes()) * max_comp_ratio;
        if (input.num_blocks() == 1) {
            auto ptr = reinterpret_cast<const T *>(input.buffer().data());
            for (auto i = 0UL; i < full_blocks_; ++i) {
                std::array<T, alp::config::VECTOR_SIZE> exceptions;
                std::array<EncodedType, alp::config::VECTOR_SIZE> encoded;
                write_decimal_data(ptr, exceptions, exception_positions, encoded, state, out_ptr, total_size);
                codec::check<ErrorCode::E_ENCODING_OVERFLOW>(static_cast<double>(total_size) < compress_limit, "Exceeded encoding limit");
                ptr += alp::config::VECTOR_SIZE;
            }
        } else {
            ContiguousRangeForwardAdaptor<T, alp::config::VECTOR_SIZE> adaptor(input);
            for (auto i = 0UL; i < full_blocks_; ++i) {
                auto ptr = adaptor.next();
                std::array<T, alp::config::VECTOR_SIZE> exceptions;
                std::array<EncodedType, alp::config::VECTOR_SIZE> encoded;
                write_decimal_data(ptr, exceptions, exception_positions, encoded, state, out_ptr, total_size);
                codec::check<ErrorCode::E_ENCODING_OVERFLOW>(static_cast<double>(total_size) < compress_limit, "Exceeded encoding limit");
            }
        }
        if (remainder_ > 0) {
            std::array<T, alp::config::VECTOR_SIZE> remainder_data;
            fill_remainder_array(remainder_data, input, remainder_offset(), remainder_);
            std::array<T, alp::config::VECTOR_SIZE> exceptions;
            std::array<EncodedType, alp::config::VECTOR_SIZE> encoded;
            write_decimal_data(
                remainder_data.data(),
                exceptions,
                exception_positions,
                encoded,
                state,
                out_ptr,
                total_size);
        }
    }

    void compress_rd(
            const ColumnData input,
            alp::state<T> &state,
            uint8_t *out_ptr,
            size_t &total_size,
            std::array<uint16_t, alp::config::VECTOR_SIZE> &exception_positions) {
        auto column_header = new (out_ptr + total_size) RealDoubleColumnHeader<T>(state);
        total_size += column_header->total_size();
        const auto max_comp_ratio = ConfigsMap::instance()->get_double("Alp.MaxCompressRatio", 0.8);
        const auto compress_limit = double(input.buffer().bytes()) * max_comp_ratio;
        auto bit_widths = column_header->bit_widths();
        std::array<uint16_t, alp::config::VECTOR_SIZE> left{};
        std::array<typename StorageType<T>::unsigned_type, alp::config::VECTOR_SIZE> right;
        std::array<uint16_t, alp::config::VECTOR_SIZE> exceptions{};
        if (input.num_blocks() == 1) {
            auto ptr = reinterpret_cast<const T *>(input.buffer().data());
            for (auto i = 0UL; i < full_blocks_; ++i) {
                write_real_double_data(ptr, left, right, exceptions, exception_positions, state, out_ptr, total_size, bit_widths);
                codec::check<ErrorCode::E_ENCODING_OVERFLOW>(static_cast<double>(total_size) < compress_limit, "Exceeded encoding limit");
                ptr += alp::config::VECTOR_SIZE;
            }
        } else {
            ContiguousRangeForwardAdaptor<T, alp::config::VECTOR_SIZE> adaptor(input);
            for (auto i = 0UL; i < full_blocks_; ++i) {
                auto ptr = adaptor.next();
                write_real_double_data(ptr, left, right, exceptions, exception_positions, state, out_ptr, total_size, bit_widths);
                codec::check<ErrorCode::E_ENCODING_OVERFLOW>(static_cast<double>(total_size) < compress_limit, "Exceeded encoding limit");
            }
        }
        if (remainder_ > 0) {
            std::array<T, alp::config::VECTOR_SIZE> remainder_data;
            fill_remainder_array(remainder_data, input, remainder_offset(), remainder_);
            write_real_double_data(remainder_data.data(), left, right, exceptions, exception_positions, state, out_ptr, total_size, bit_widths);
        }
    }

    size_t compress(ColumnData input, T *__restrict out, size_t expected_bytes) {
        const auto rows = input.row_count();
        full_blocks_ = calculate_full_blocks(rows);
        compressed_rows_ = rows;
        remainder_ = compressed_rows_ % alp::config::VECTOR_SIZE;
        ARCTICDB_DEBUG(log::codec(), "ALP compressing {} rows, {} total blocks with remainder of {}", rows, full_blocks_, remainder_);
        size_t total_size = sizeof(ALPHeader<T>);
        auto header [[maybe_unused]] = new(out) ALPHeader<T>{.num_rows_ = static_cast<uint32_t>(rows), .scheme_ = data_.state_.scheme};
        std::array<uint16_t, alp::config::VECTOR_SIZE> exception_positions{};
        auto out_ptr = reinterpret_cast<uint8_t *>(out);
        switch (data_.state_.scheme) {
        case alp::Scheme::ALP: {
            auto col_header = new(out_ptr + total_size) ALPDecimalColumnHeader<T>{};
            total_size += col_header->total_size();
            compress_alp(input, data_.state_, out_ptr, total_size, exception_positions);
            break;
        }
        case alp::Scheme::ALP_RD: {
            compress_rd(input, data_.state_, out_ptr, total_size, exception_positions);
            break;
        }
        default:util::raise_rte("Unhandled ALP scheme type");
        }
        util::check(total_size <= expected_bytes, "ALP compression overflow: {} > {}", total_size, expected_bytes);
        return total_size;
    }
};

template<typename T>
void set_real_double_state_from_column_header(
        alp::state<T> &state,
        const RealDoubleColumnHeader<T>& col_header) {
    memcpy(state.left_parts_dict, col_header.dict(), col_header.dict_size());
    state.actual_dictionary_size_bytes = col_header.dict_size();
    state.actual_dictionary_size = col_header.dict_size() / sizeof(uint16_t);
    state.right_bit_width = col_header.bit_widths_.right_;
    state.left_bit_width = col_header.bit_widths_.left_;
}

template<typename T>
void update_real_double_state_from_block_header(
        alp::state<T> &state,
        const RealDoubleBlockHeader<T>& block_header) {
    state.exceptions_count = block_header.exception_count_;
}

template<typename T>
void update_state_from_alp_decimal_block_header(alp::state<T>& state,
                                                const ALPDecimalBlockHeader<T>& block_header) {
    state.exceptions_count = block_header.exception_count_;
    state.bit_width = block_header.bit_width_;
    state.exp = block_header.exp_;
    state.fac = block_header.fac_;
}

template<typename T>
struct ALPDecompressor {
    using Header = ALPHeader<T>;
    using EncodedType = StorageType<T>::signed_type;
private:
    const Header *header_;
    size_t full_blocks_;
    size_t remainder_;

public:
    void init(const T *input) {
        header_ = reinterpret_cast<const Header *>(input);
        full_blocks_ = header_->num_rows_ / alp::config::VECTOR_SIZE;
        remainder_ = header_->num_rows_ % alp::config::VECTOR_SIZE;
    }

    [[nodiscard]] size_t num_rows() const { return header_->num_rows_; }

    size_t decompress_alp(const uint8_t* input, T *output) {
        auto column_header = reinterpret_cast<const ALPDecimalColumnHeader<T>*>(input);
        column_header->magic_.check();
        auto read_pos = column_header->total_size();
        alp::state<T> state;
        std::array<typename StorageType<T>::signed_type, alp::config::VECTOR_SIZE> unffor;
        auto write_pos = 0UL;
        for (size_t block = 0; block < full_blocks_; ++block) {
            auto *header = reinterpret_cast<const ALPDecimalBlockHeader<T>*>(input + read_pos);
            header->magic_.check();
            update_state_from_alp_decimal_block_header(state, *header);
            uint16_t exception_count = header->exceptions_count();

            const auto data = reinterpret_cast<const StorageType<T>::signed_type *>(header->data());
            ARCTICDB_DEBUG(log::codec(), "First value in data to decompress: {}", data[0]);
            if(state.bit_width == 0) {
                std::fill(std::begin(unffor), std::end(unffor), *header->bases());
            } else {
                FForUncompressKernel<EncodedType> kernel(*header->bases());
                dispatch_bitwidth_fused<EncodedType, BitUnpackFused>(
                    data,
                    unffor.data(),
                    state.bit_width,
                    kernel
                );
            }

            ARCTICDB_DEBUG(log::codec(), "First byte of unffor: {}", unffor[0]);
            alp::decoder<T>::decode(
                unffor.data(),
                state.fac,
                state.exp,
                output + write_pos);

            alp::decoder<T>::patch_exceptions(
                output + write_pos,
                header->exceptions(),
                header->exception_positions(),
                &exception_count);
            read_pos += header->total_size();
            write_pos += alp::config::VECTOR_SIZE;
        }

        if (remainder_ > 0) {
            std::array<T, alp::config::VECTOR_SIZE> remainder_data;
            auto *header = reinterpret_cast<ALPDecimalBlockHeader<T>*>(const_cast<uint8_t *>(input) + read_pos);
            header->magic_.check();
            update_state_from_alp_decimal_block_header(state, *header);
            uint16_t exception_count = header->exceptions_count();
            ARCTICDB_DEBUG(log::codec(), "First value in data to decompress: {}", reinterpret_cast<const EncodedType*>(header->data())[0]);
            if(state.bit_width == 0) {
                std::fill(std::begin(unffor), std::end(unffor), *header->bases());
            } else {
                FForUncompressKernel<EncodedType> kernel(*header->bases());
                dispatch_bitwidth_fused<EncodedType, BitUnpackFused>(
                    header->data(),
                    unffor.data(),
                    state.bit_width,
                    kernel
                );
            }

            ARCTICDB_DEBUG(log::codec(), "First byte of unffor: {}", unffor[0]);
            alp::decoder<T>::decode(
                unffor.data(),
                state.fac,
                state.exp,
                remainder_data.data());

            alp::decoder<T>::patch_exceptions(
                remainder_data.data(),
                header->exceptions(),
                header->exception_positions(),
                &exception_count);
            read_pos += header->total_size();
            memcpy(output + write_pos, remainder_data.data(), remainder_ * sizeof(T));
        }
        return read_pos;
    }

    size_t decompress_alp_rd(const uint8_t *input, T *output) {
        alp::state<T> restored_state;
        size_t read_pos = 0UL;
        size_t write_pos = 0UL;
        auto column_header = reinterpret_cast<const RealDoubleColumnHeader<T>*>(input);
        column_header->magic_.check();
        set_real_double_state_from_column_header(restored_state, *column_header);
        auto bit_widths = column_header->bit_widths();
        read_pos += column_header->total_size();

        using RightType = StorageType<T>::unsigned_type;
        std::array<uint16_t, alp::config::VECTOR_SIZE> left{};
        std::array<RightType, alp::config::VECTOR_SIZE> right;
        for (size_t block = 0; block < full_blocks_; ++block) {
            auto *header = reinterpret_cast<RealDoubleBlockHeader<T>*>(const_cast<uint8_t*>(input) + read_pos);
            header->magic_.check();
            uint16_t exception_count = header->exceptions_count();
            update_real_double_state_from_block_header(restored_state, *header);

            ARCTICDB_DEBUG(log::codec(), "Left unbitpack: {} Right unbitpack: {}", *header->left(), *header->right(bit_widths));
            BitUnpackCompressKernel<uint16_t> left_kernel;
            dispatch_bitwidth_fused<uint16_t, BitUnpackFused>(
                header->left(),
                left.data(),
                bit_widths.left_,
                left_kernel
                );

            BitUnpackCompressKernel<RightType> right_kernel;
            dispatch_bitwidth_fused<RightType, BitUnpackFused>(
                header->right(bit_widths),
                right.data(),
                bit_widths.right_,
                right_kernel
            );

            ARCTICDB_DEBUG(log::codec(), "Uncompress left: {} right: {}", left[0], right[0]);
            alp::rd_encoder<T>::decode(
                output + write_pos,
                right.data(),
                left.data(),
                header->exceptions(bit_widths),
                header->exception_positions(bit_widths),
                &exception_count,
                restored_state);

            read_pos += header->total_size(bit_widths);
            write_pos += alp::config::VECTOR_SIZE;
        }
        if (remainder_ > 0) {
            std::array<T, alp::config::VECTOR_SIZE> remainder_data;
            auto *header = reinterpret_cast<RealDoubleBlockHeader<T>*>(const_cast<uint8_t *>(input) + read_pos);
            header->magic_.check();
            uint16_t exception_count = header->exceptions_count();
            update_real_double_state_from_block_header(restored_state, *header);

            BitUnpackCompressKernel<uint16_t> left_kernel;
            dispatch_bitwidth_fused<uint16_t, BitUnpackFused>(
                header->left(),
                left.data(),
                bit_widths.left_,
                left_kernel
            );

            BitUnpackCompressKernel<RightType> right_kernel;
            dispatch_bitwidth_fused<RightType, BitUnpackFused>(
                header->right(bit_widths),
                right.data(),
                bit_widths.right_,
                right_kernel
            );

            alp::rd_encoder<T>::decode(
                remainder_data.data(),
                right.data(),
                left.data(),
                header->exceptions(bit_widths),
                header->exception_positions(bit_widths),
                &exception_count,
                restored_state);
            read_pos += header->total_size(bit_widths);
            memcpy(output + write_pos, remainder_data.data(), remainder_ * sizeof(T));
        }
        return read_pos;
    }

    DecompressResult decompress(const T *input, T *output) {
        size_t compressed_bytes;
        auto in_ptr = reinterpret_cast<const uint8_t*>(input);
        in_ptr += sizeof(ALPHeader<T>);
        switch (header_->scheme_) {
        case alp::Scheme::ALP:
            compressed_bytes = decompress_alp(in_ptr, output);
            break;
        case alp::Scheme::ALP_RD:
            compressed_bytes = decompress_alp_rd(in_ptr, output);
            break;
        default:
            util::raise_rte("Unhandled ALP scheme type");
        }
        return { .compressed_ = compressed_bytes, .uncompressed_ = header_->num_rows_ * sizeof(T) };
    }

};

} // namespace arcticdb