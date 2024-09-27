/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */


#include <arcticdb/arrow/arrow_utils.hpp>
#include <arcticdb/arrow/arrow_data.hpp>

#include <vector>

namespace arcticdb {

    ArrowOutputFrame::ArrowOutputFrame(
        std::vector<ArrowData>&& data,
        std::vector<std::string>&& names) :
        data_(std::make_shared<std::vector<ArrowData>>(std::move(data))),
        names_(std::move(names)) {
    }

    std::vector<uintptr_t> ArrowOutputFrame::arrays() {
        std::vector<uintptr_t> output;
        output.reserve(data_->size());
        for(auto& data : *data_)
            output.emplace_back(reinterpret_cast<uintptr_t>(data.data_.get()));

        return output;
    }

    std::vector<uintptr_t> ArrowOutputFrame::schemas() {
        std::vector<uintptr_t> output;
        output.reserve(data_->size());
        for(auto& data : *data_)
            output.emplace_back(reinterpret_cast<uintptr_t>(data.schema_.get()));

        return output;
    }

    std::vector<std::string> ArrowOutputFrame::names() const {
        return names_;
    }

}  // namespace arcticdb