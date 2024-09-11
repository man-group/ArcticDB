/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */


#include <arcticdb/arrow/arrow_output_frame.hpp>

namespace arcticdb {

ArrowOutputFrame::ArrowOutputFrame(const SegmentInMemory &frame, std::shared_ptr <BufferHolder> buffers) :
    module_data_(ModuleData::instance()),
    frame_(frame),
    names_(frame.fields().size() - frame.descriptor().index().field_count()),
    index_columns_(frame.descriptor().index().field_count()),
    buffers_(std::move(buffers)) {

}

} // namespace arcticdb