/*
* Copyright 2023 Man Group Operations Ltd.
* NO WARRANTY, EXPRESSED OR IMPLIED
*/

#include <arcticdb/pipeline/index_utils.hpp>
#include <arcticdb/pipeline/frame_slice.hpp>
#include <arcticdb/pipeline/index_fields.hpp>

namespace arcticdb {
using namespace arcticdb::stream;
using namespace arcticdb::pipelines::index;

struct TestSegment {
    TestSegment() :
        start_(TypeDescriptor{DataType::UINT64, Dimension::Dim0}, false),
        end_(TypeDescriptor{DataType::UINT64, Dimension::Dim0}, false) {

    }
    Column start_;
    Column end_;
    position_t row_ = 0;

    const Column& column(position_t pos) const {
        switch(pos) {
        case int(pipelines::index::Fields::start_index):
            return start_;
            case int(pipelines::index::Fields::end_index):
                return end_;
                default:
                    util::raise_rte("Unknown index");
        }
    }

    void set_range(uint64_t start, uint64_t end) {
        start_.set_scalar(row_, start);
        end_.set_scalar(row_, end);
        ++row_;
    }
};

struct TestContainer {
    mutable TestSegment seg_;

    TestSegment& seg() const {
        return seg_;
    }

    size_t size() const {
        return seg_.end_.row_count();
    }

    bool empty() const {
        return seg_.end_.row_count() == 0;
    }
};
} //namespace arcticdb

