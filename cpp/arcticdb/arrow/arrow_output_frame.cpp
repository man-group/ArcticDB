/* Copyright 2023 Man Group Operations Limited
*
* Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
*
* As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
*/


#include <arcticdb/arrow/arrow_utils.hpp>
#include <arcticdb/arrow/arrow_data.hpp>
#include <arcticdb/arrow/arrow_output_frame.hpp>

#include <vector>

namespace arcticdb {

ArrowOutputFrame::ArrowOutputFrame(
    std::shared_ptr<std::vector<sparrow::record_batch>>&& data,
    std::vector<std::string>&& names) :
    data_(std::move(data)),
    names_(std::move(names)) {
}

size_t ArrowOutputFrame::num_blocks() const {
    if(data_->empty())
        return 0;

    return data_->size();
}

std::vector<RecordBatchData> ArrowOutputFrame::record_batches() {
    std::vector<RecordBatchData> output;
    output.reserve(data_->size());

    // Store views to keep the Arrow structures alive
    std::vector<sparrow::struct_array> views;
    views.reserve(data_->size());

    for(auto& batch : *data_) {
        auto struct_array = batch.extract_struct_array();
        views.push_back(struct_array.slice_view(0, struct_array.size()));

        if (auto wrapper = views.back().raw_child(0)) {
            const auto& proxy = wrapper->get_arrow_proxy();

            output.push_back(RecordBatchData{
                .array_ = reinterpret_cast<uintptr_t>(&proxy.array()),
                .schema_ = reinterpret_cast<uintptr_t>(&proxy.schema())
            });
        }
    }

    return output;
}

std::vector<std::string> ArrowOutputFrame::names() const {
    return names_;
}

}  // namespace arcticdb