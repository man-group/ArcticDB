#pragma once

#include <arcticdb/column_store/chunked_buffer.hpp>
#include <arcticdb/column_store/string_pool.hpp>
#include <arcticdb/util/memory_tracing.hpp>

#include <arcticdb/pipeline/string_pool_utils.hpp>

namespace arcticdb {
inline void merge_string_column(
        ChunkedBuffer& src_buffer, const std::shared_ptr<StringPool>& src_pool,
        const std::shared_ptr<StringPool>& merged_pool, CursoredBuffer<ChunkedBuffer>& output, bool verify
) {
    using OffsetType = entity::position_t;
    constexpr auto offset_size = sizeof(OffsetType);
    auto num_strings = src_buffer.bytes() / offset_size;
    for (auto row = 0ULL; row < num_strings; ++row) {
        auto offset = get_offset_string_at(row, src_buffer);
        entity::position_t new_value;
        if (offset != not_a_string() && offset != nan_placeholder()) {
            auto sv = get_string_from_pool(offset, *src_pool);
            new_value = merged_pool->get(sv).offset();
        } else {
            new_value = offset;
        }
        output.ensure<OffsetType>(1);
        util::check(new_value >= 0, "Unexpected negative number {} in merge string column", new_value);
        output.typed_cursor<OffsetType>() = new_value;
        output.commit();
    }
    if (verify) {
        const auto& out_buffer = output.buffer();
        auto num_out = out_buffer.bytes() / offset_size;
        util::check(num_strings == num_out, "Mismatch in input/output size {} != {}", num_strings, num_out);
        for (auto row = size_t(0); row < num_out; ++row) {
            auto offset = get_offset_string_at(row, out_buffer);
            if (offset != not_a_string() && offset != nan_placeholder()) {
                auto sv ARCTICDB_UNUSED = get_string_from_pool(offset, *merged_pool);
                ARCTICDB_DEBUG(log::version(), "Accessed {} from string pool", sv);
            }
        }
    }
}

inline void merge_string_columns(
        const SegmentInMemory& segment, const std::shared_ptr<StringPool>& merged_pool, bool verify
) {
    for (size_t c = 0; c < segment.descriptor().field_count(); ++c) {
        auto& frame_field = segment.field(c);
        const auto& field_type = frame_field.type();

        if (!is_sequence_type(field_type.data_type_))
            continue;

        auto& src = const_cast<ChunkedBuffer&>(segment.column(static_cast<position_t>(c)).buffer());
        CursoredBuffer<ChunkedBuffer> cursor{src.bytes(), AllocationType::DYNAMIC};
        merge_string_column(src, segment.string_pool_ptr(), merged_pool, cursor, verify);
        std::swap(src, cursor.buffer());
    }
}

inline void merge_segments(std::vector<SegmentInMemory>& segments, SegmentInMemory& merged, Sparsity is_sparse) {
    ARCTICDB_DEBUG(log::version(), "Appending {} segments", segments.size());
    timestamp min_idx = std::numeric_limits<timestamp>::max();
    timestamp max_idx = std::numeric_limits<timestamp>::min();
    for (auto& segment : segments) {
        ARCTICDB_DEBUG(log::version(), "Appending segment with {} rows", segment.row_count());
        for (const auto& field : segment.descriptor().fields()) {
            if (!merged.column_index(field.name())) { // TODO: Bottleneck for wide segments
                auto pos = merged.add_column(field, 0, AllocationType::DYNAMIC);
                if (is_sparse == Sparsity::NOT_PERMITTED) {
                    merged.column(pos).mark_absent_rows(merged.row_count());
                }
            }
        }

        if (segment.row_count() && segment.descriptor().index().type() == IndexDescriptorImpl::Type::TIMESTAMP) {
            min_idx = std::min(min_idx, segment.begin()->begin()->value<timestamp>());
            max_idx = std::max(max_idx, (segment.end() - 1)->begin()->value<timestamp>());
        }

        merge_string_columns(segment, merged.string_pool_ptr(), false);
        merged.append(segment);
        merged.set_compacted(true);
        util::print_total_mem_usage(__FILE__, __LINE__, __FUNCTION__);
    }
}

inline pipelines::FrameSlice merge_slices(std::vector<pipelines::FrameSlice>& slices, const StreamDescriptor& desc) {
    util::check(!slices.empty(), "Expected to merge non-empty slices_vector");

    pipelines::FrameSlice output{slices[0]};
    for (const auto& slice : slices) {
        output.row_range.first = std::min(output.row_range.first, slice.row_range.first);
        output.row_range.second = std::max(output.row_range.second, slice.row_range.second);
    }

    output.col_range.first = desc.index().field_count();
    output.col_range.second = desc.field_count();
    return output;
}
} // namespace arcticdb
