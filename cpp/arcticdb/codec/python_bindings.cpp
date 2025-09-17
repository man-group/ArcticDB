/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/codec/python_bindings.hpp>
#include <arcticdb/codec/codec.hpp>
#include <arcticdb/column_store/chunked_buffer.hpp>
#include <arcticdb/column_store/column_data.hpp>
#include <arcticdb/column_store/memory_segment.hpp>
#include <arcticdb/python/python_utils.hpp>
#include <arcticdb/util/cursored_buffer.hpp>
#include <arcticdb/util/preconditions.hpp>
#include <arcticdb/util/pybind_mutex.hpp>

#include <memory>
#include <algorithm>

namespace py = pybind11;
using namespace arcticdb::python_util;

namespace arcticdb {

class DynamicFieldBuffer {
  public:
    DynamicFieldBuffer(const TypeDescriptor& td, const py::buffer& data, const py::buffer& shapes) : td_(td), field_() {
        auto d_info_ = data.request();
        auto s_info_ = shapes.request();
        util::check_arg(d_info_.ndim == 1, "only support dimension 1 (flattened) data. actual {}", d_info_.ndim);
        util::check_arg(s_info_.ndim == 1, "only support dimension 1 (flattened) shapes. actual {}", s_info_.ndim);
        util::check_arg(
                sizeof(shape_t) == s_info_.itemsize,
                "expected shape itemsize={:d}, actual={:d}",
                sizeof(shape_t),
                s_info_.itemsize
        );
        util::check_arg(s_info_.size > 0, "does not support empty size.");
        util::check_arg(d_info_.size > 0, "does not support empty data.");
        std::size_t item_count = 0;
        auto s = reinterpret_cast<shape_t*>(s_info_.ptr);
        std::size_t shape_count = s_info_.size;
        std::size_t dim = std::max(static_cast<std::size_t>(1), static_cast<std::size_t>(td.dimension()));
        for (std::size_t i = 0; i < shape_count / dim; ++i) {
            std::size_t v = 1;
            for (std::size_t j = 0; j < dim; ++j, ++s) {
                v *= *s;
            }
            item_count += v;
        }
        util::check_arg(
                item_count == std::size_t(d_info_.size),
                "number of elements e={} and sum of shapes s={} do not match",
                item_count,
                d_info_.size
        );

        auto data_bytes = d_info_.size * d_info_.itemsize;
        auto shape_bytes = s_info_.size * s_info_.itemsize;

        if (td.dimension() == Dimension::Dim0) {
            data_.ensure_bytes(data_bytes);
            memcpy(data_.cursor(), d_info_.ptr, data_bytes);
            field_ = std::make_shared<ColumnData>(&data_.buffer(), &shapes_.buffer(), td_, nullptr);
        } else {
            data_.ensure_bytes(data_bytes);
            shapes_.ensure_bytes(shape_bytes);
            memcpy(shapes_.cursor(), s_info_.ptr, shape_bytes);
            memcpy(data_.cursor(), d_info_.ptr, data_bytes);
            data_.commit();
            shapes_.commit();
            field_ = std::make_shared<ColumnData>(&data_.buffer(), &shapes_.buffer(), td_, nullptr);
        }
    }

    std::shared_ptr<ColumnData> as_field() { return field_; }

  private:
    CursoredBuffer<Buffer> shapes_;
    CursoredBuffer<ChunkedBuffer> data_;
    TypeDescriptor td_;
    std::shared_ptr<ColumnData> field_;
};

struct FieldEncodingResult {
    FieldEncodingResult() = default;
    FieldEncodingResult(std::shared_ptr<Buffer> buffer, proto::encoding::EncodedField encoded_field) :
        buffer_(std::move(buffer)),
        encoded_field_(encoded_field) {}
    std::shared_ptr<Buffer> buffer_;
    proto::encoding::EncodedField encoded_field_;
};

Segment encode_segment(SegmentInMemory segment_in_memory, const py::object& opts, EncodingVersion encoding_version) {
    proto::encoding::VariantCodec opts_cpp;
    python_util::pb_from_python(opts, opts_cpp);
    return encode_dispatch(std::move(segment_in_memory), opts_cpp, encoding_version);
}

SegmentInMemory decode_python_segment(Segment& segment) { return decode_segment(segment, AllocationType::DETACHABLE); }

class BufferPairDataSink {
  public:
    BufferPairDataSink() : values_(std::make_shared<Buffer>()), shapes_(std::make_shared<Buffer>()) {}

    void* allocate_data(std::size_t size) {
        values_->ensure(size);
        return values_->data();
    }

    shape_t* allocate_shapes(std::size_t size) {
        shapes_->ensure(size);
        return reinterpret_cast<shape_t*>(shapes_->data());
    }

    void advance_shapes(std::size_t) {}

    void advance_data(std::size_t) {}

    void set_allow_sparse(Sparsity) {}

    std::shared_ptr<Buffer> values() { return values_; }

    std::shared_ptr<Buffer> shapes() { return shapes_; }

  private:
    std::shared_ptr<Buffer> values_;
    std::shared_ptr<Buffer> shapes_;
};

struct FieldDecodingResult {
    FieldDecodingResult() = default;
    FieldDecodingResult(std::shared_ptr<Buffer> shape_buffer, std::shared_ptr<Buffer> values_buffer) :
        shape_buffer_(std::move(shape_buffer)),
        values_buffer_(std::move(values_buffer)) {}
    std::shared_ptr<Buffer> shape_buffer_;
    std::shared_ptr<Buffer> values_buffer_;
};

void register_codec(py::module& m) {
    py::class_<DynamicFieldBuffer>(m, "DynamicFieldBuffer")
            .def(py::init<TypeDescriptor, py::buffer, py::buffer>())
            .def("as_field", &DynamicFieldBuffer::as_field, py::call_guard<SingleThreadMutexHolder>());

    py::class_<FieldEncodingResult, std::shared_ptr<FieldEncodingResult>>(m, "FieldEncodingResult")
            .def(py::init<>())
            .def_property_readonly("buffer", [](const FieldEncodingResult& self) { return self.buffer_; })
            .def_property_readonly("encoded_field", [](const FieldEncodingResult& self) {
                return python_util::pb_to_python(self.encoded_field_);
            });

    py::class_<FieldDecodingResult, std::shared_ptr<FieldDecodingResult>>(m, "FieldDecodingResult")
            .def(py::init<>())
            .def_property_readonly("shape_buffer", [](const FieldDecodingResult& self) { return self.shape_buffer_; })
            .def_property_readonly("values_buffer", [](const FieldDecodingResult& self) {
                return self.values_buffer_;
            });

    py::class_<Buffer, std::shared_ptr<Buffer>>(m, "Buffer", py::buffer_protocol())
            .def(py::init(), py::call_guard<SingleThreadMutexHolder>())
            .def("size", &Buffer::bytes)
            .def_buffer([](Buffer& buffer) {
                return py::buffer_info{
                        buffer.data(), 1, py::format_descriptor<std::uint8_t>::format(), 1, {buffer.bytes()}, {1}
                };
            });

    py::class_<Segment>(m, "Segment")
            .def(py::init<>())
            .def("fields_size", &Segment::fields_size)
            .def("fields", &Segment::fields_vector)
            .def_property_readonly(
                    "header", [](const Segment& self) { return self.header().clone(); }, py::return_value_policy::move
            )
            .def_property_readonly("bytes", [](const Segment& self) {
                return py::bytes(reinterpret_cast<char*>(self.buffer().data()), self.buffer().bytes());
            });

    m.def("encode_segment", &encode_segment);
    m.def("decode_segment", &decode_python_segment, py::return_value_policy::move);
}

} // namespace arcticdb
