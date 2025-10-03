/* Copyright 2025 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/arrow/arrow_output_frame.hpp>

#include <vector>

#include <sparrow/record_batch.hpp>

namespace arcticdb {

ArrowOutputFrame::ArrowOutputFrame(std::shared_ptr<std::vector<sparrow::record_batch>>&& data) :
    data_(std::move(data)) {}

size_t ArrowOutputFrame::num_blocks() const {
    if (!data_ || data_->empty())
        return 0;

    return data_->size();
}

std::vector<RecordBatchData> ArrowOutputFrame::extract_record_batches() {
    std::vector<RecordBatchData> output;
    if (!data_) {
        return output;
    }
    output.reserve(data_->size());

    for (auto& batch : *data_) {
        auto struct_array = sparrow::array{batch.extract_struct_array()};
        auto [arr, schema] = sparrow::extract_arrow_structures(std::move(struct_array));

        output.emplace_back(arr, schema);
    }

    return output;
}

} // namespace arcticdb