#pragma once

#include <arcticdb/codec/default_codecs.hpp>
#include <arcticdb/codec/encoded_field.hpp>
#include <arcticdb/column_store/column_data.hpp>
#include <arcticdb/column_store/statistics.hpp>
#include <arcticdb/storage/memory_layout.hpp>
#include <arcticdb/codec/compression/encoding_types.hpp>
#include <arcticdb/codec/compression/encoder_data.hpp>
#include <arcticdb/codec/compression/encoding_scan_result.hpp>
#include <arcticdb/util/configs_map.hpp>
#include <arcticdb/codec/encode_common.hpp>

namespace arcticdb {

static constexpr int TOO_SMALL_TO_COMPRESS = 32;

class EncodingsList {
    uint64_t data_;

    static constexpr uint64_t to_pos(EncodingType encoding) {
        return 1UL << (static_cast<uint64_t>(encoding));
    }

public:
    constexpr EncodingsList() : data_(0) {}

    constexpr EncodingsList(std::initializer_list<EncodingType> encodings) : data_(0) {
        for (const auto& encoding : encodings) {
            set(encoding);
        }
    }

    constexpr void set(EncodingType encoding) {
        data_ |= to_pos(encoding);
    }

    constexpr void unset(EncodingType encoding) {
        data_ &= ~to_pos(encoding);
    }

    [[nodiscard]] constexpr bool is_set(EncodingType encoding) const {
        return data_ & to_pos(encoding);
    }
};

constexpr EncodingsList IntegerEncodings {
    EncodingType::BITPACK,
    EncodingType::CONSTANT,
    EncodingType::DELTA,
    EncodingType::FFOR,
    EncodingType::FREQUENCY
    //EncodingType::RLE
};

constexpr EncodingsList BoolEncodings {
    EncodingType::BITPACK
};

constexpr EncodingsList FloatEncodings {
    EncodingType::CONSTANT,
    EncodingType::ALP
};

constexpr EncodingsList StringEncodings {
    EncodingType::CONSTANT,
    EncodingType::DELTA,
    EncodingType::FREQUENCY,
    EncodingType::FFOR
    //EncodingType::RLE
};

inline EncodingsList possible_encodings(DataType data_type) {
    if(is_sequence_type(data_type)) {
        return StringEncodings;
    } else if(is_integer_type(data_type) || is_time_type(data_type)) {
        return IntegerEncodings;
    } else if(is_floating_point_type(data_type)) {
        return FloatEncodings;
    } else if(is_bool_type(data_type)) {
        return BoolEncodings;
    } else {
       util::raise_rte("Cannot select encoding for type {}", data_type);
    }
}

inline EncodingsList viable_encodings(
        EncodingsList input,
        FieldStatsImpl field_stats,
        DataType data_type,
        size_t row_count) {
    EncodingsList output;

    if(output.is_set(EncodingType::CONSTANT) && is_viable(EncodingType::CONSTANT, field_stats, data_type, row_count)) {
        output.set(EncodingType::CONSTANT);
        return output;
    }

    if(get_type_size(data_type) * row_count < TOO_SMALL_TO_COMPRESS) {
        ARCTICDB_DEBUG(log::codec(), "Data is too small for compression, using plain codec");
        output.set(EncodingType::PLAIN);
        return output;
    }

    for(auto i = 0U; i < static_cast<uint8_t>(EncodingType::COUNT); ++i) {
        const auto type = EncodingType(i);
        if(input.is_set(type) && is_viable(type, field_stats, data_type, row_count))
            output.set(type);
    }
    return output;
}

inline EncodingScanResultSet predicted_optimal_encodings(ColumnData column_data) {
    if(column_data.row_count() == 0)
        return {};

    const auto data_type = column_data.type().data_type();
    util::check(column_data.has_field_stats(), "Expected column statistics in adaptive encoding");
    const auto field_stats = column_data.field_stats();
    const auto row_count = column_data.row_count();
    const auto original_size = column_data.buffer().bytes();
    auto encodings_for_type = possible_encodings(data_type);
    auto filtered_encodings = viable_encodings(encodings_for_type, field_stats, data_type, row_count);
    EncodingScanResultSet results;
    
    if(is_viable(EncodingType::CONSTANT, field_stats, data_type, row_count)) {
        auto size = deterministic_size(EncodingType::CONSTANT, field_stats, data_type, row_count);
        results.try_emplace(create_scan_result(EncodingType::CONSTANT, size->first, Constant::speed_factor(), original_size, true, std::move(size->second)));
        return results;
    }

    for(auto i = 0U; i < static_cast<uint8_t>(EncodingType::COUNT); ++i) {
        const auto type = EncodingType(i);
        if(!filtered_encodings.is_set(type))
            continue;

        // Could potentially introduce a scaling factor for deterministic results vs estimated results, i.e. try
        // encodings with a guaranteed size unless the estimated size indicates it will be substantially smaller.
        if(auto exact_size = deterministic_size(type, field_stats, data_type, row_count)) {
            results.try_emplace(create_scan_result(type, exact_size->first, speed_factor(type), original_size, true, std::move(exact_size->second)));
        } else {
            auto scan_result = estimated_size(
                type,
                field_stats,
                data_type,
                column_data,
                row_count,
                original_size);

            results.try_emplace(std::move(scan_result));
        }
    }

    results.sort();
    return results;
}

// For the time being an acceptable result is just one that achieves some degree of compression, however this could be
// made more sophisticated if multiple results were to be evaluated together (see below)
inline bool is_acceptable_result(const EncodingScanResult& result) {
    return result.estimated_size_ <= plain_result_bytes(result.original_size_);
}

inline EncodingScanResult plain_result(size_t bytes) {
    EncodingScanResult result;
    result.original_size_ = bytes;
    result.estimated_size_ = bytes;
    result.is_deterministic_ = true;
    result.type_ = EncodingType::PLAIN;
    return result;
}

inline EncodingScanResult shape_encoding(const ColumnData& col) {
    const auto& shapes = *col.shapes();
    if(shapes.bytes() <= TOO_SMALL_TO_COMPRESS) {
       return plain_result(shapes.bytes());
    } else {
        const auto num_shapes = shapes.bytes() / sizeof(shape_t);
        auto shape_ptr = shapes.ptr_cast<shape_t>(0, shapes.bytes());
        auto [min, max] = std::minmax_element(shape_ptr, shape_ptr + num_shapes); 
        FieldStatsImpl stats{*min, *max, static_cast<uint32_t>(num_shapes), UniqueCountType::PRECISE, SortedValue::UNKNOWN};
        if(Ffor::is_viable(stats, DataType::INT64, num_shapes)) {
            EncoderData encoder_data;
            return Ffor::max_compressed_size(stats, DataType::INT64, num_shapes, col, encoder_data);
        } else {
            return plain_result(shapes.bytes());
        }
    }
}

inline EncodingScanResultSet shapes_encoding_for_column(const ColumnData& column_data, SizeResult& result) {
    EncodingScanResultSet shape_set;
    shape_set.try_emplace(shape_encoding(column_data));
    result.max_compressed_bytes_ += shape_set[0].estimated_size_;
    result.uncompressed_bytes_ += shape_set[0].original_size_;
    return shape_set;
}

inline void select_encoding_for_column(const ColumnData& column_data, EncodingScanResultSet& encodings_set, SizeResult& result) {
    // We could potentially keep an array of results that on a full scan were worse than the partial scan predicted, but
    // might still be better than subsequent trials, then compare them all together at the end
    if(column_data.row_count() == 0)
        return;

    bool found_encoding = false;
    for(auto i = 0UL; i < encodings_set.size(); ++i) {
        if(encodings_set[i].is_deterministic_) {
            encodings_set.select(i);
            result.max_compressed_bytes_ += encodings_set[0].estimated_size_;
            result.uncompressed_bytes_ += encodings_set[0].original_size_;
            ARCTICDB_DEBUG(log::codec(), "Selected encoding {} with deterministic size {}, ratio {}", encodings_set[0].type_, encodings_set[0].estimated_size_, double(encodings_set[0].estimated_size_) / encodings_set[0].original_size_);
            found_encoding = true;
            break;
        }

        auto scanned_result = max_compressed_size(encodings_set[i].type_, column_data.field_stats(), column_data.type().data_type(), column_data.row_count(), column_data, encodings_set[i].data_);
        if(is_acceptable_result(scanned_result)) {
            encodings_set.select(i, std::move(scanned_result));
            result.max_compressed_bytes_ += encodings_set[0].estimated_size_;
            result.uncompressed_bytes_ += encodings_set[0].original_size_;
            ARCTICDB_DEBUG(log::codec(), "Selected encoding {} with estimated size {}, ratio {}", encodings_set[0].type_, encodings_set[0].estimated_size_, double(encodings_set[0].estimated_size_) / encodings_set[0].original_size_);
            found_encoding = true;
            break;
        }
    }

    if(!found_encoding) {
        ARCTICDB_DEBUG(log::codec(), "Failed to find a successful encoding for column, using plain encoding");
        encodings_set.set_first(create_plain_result(column_data));
        result.max_compressed_bytes_ += encodings_set[0].estimated_size_;
        result.uncompressed_bytes_ += encodings_set[0].original_size_;
    }
}

inline void resolve_encodings_for_column(const ColumnData& column_data, position_t column_index, SegmentScanResults& column_encodings, SizeResult& result) {
    if(column_data.type().dimension() != Dimension::Dim0) {
        auto shape_set = shapes_encoding_for_column(column_data, result);
        column_encodings.add_shape(column_index, shape_set);
    }

    auto& encodings_set = column_encodings.value(column_index);
    select_encoding_for_column(column_data, encodings_set, result);
}

inline void resolve_adaptive_encodings_size(const SegmentInMemory& seg, SegmentScanResults& column_encodings, SizeResult& result) {
    util::check(column_encodings.values_size() == seg.num_columns(), "Expected equality of encoding scan results to columns: {} != {}", column_encodings.values_size(), seg.num_columns());
    for (position_t column_index = 0; column_index < position_t(seg.num_columns()); ++column_index) {
        const auto& column_data = seg.column(column_index).data();
        resolve_encodings_for_column(column_data, column_index, column_encodings, result);
    }
}

inline SegmentScanResults get_encodings(const SegmentInMemory& seg) {
    SegmentScanResults output;
    output.reserve_values(seg.num_columns());
    for (position_t column_index = 0; column_index < position_t(seg.num_columns()); ++column_index)
        output.add_value(column_index, predicted_optimal_encodings(seg.column(position_t(column_index)).data()));

    return output;
}


} // namespace arcticdb