#pragma once

#include <core/types.hpp>
#include <util/type_handler.hpp>

namespace arcticdb {
namespace py = pybind11;
struct DecimalHandler {

    DecimalHandler(std::shared_ptr<std::mutex> mutex) :
        mutex_(std::move(mutex)) {

    }

    void handle_type(
        const uint8_t*& data,
        uint8_t* dest,
        const pb2::encoding_pb2::EncodedField& encoded_field_info,
        const entity::TypeDescriptor& type_descriptor,
        size_t dest_bytes,
        std::shared_ptr<BufferHolder> buffers
        );

    std::shared_ptr<std::mutex> mutex_ = std::make_shared<std::mutex>();
    std::shared_ptr<py::object> decimal_ = std::make_shared<py::object>(py::module_::import("decimal").attr("Decimal"));
};

struct ArrayHandler {
    std::shared_ptr<std::mutex> mutex_ = std::make_shared<std::mutex>();

    ArrayHandler(std::shared_ptr<std::mutex> mutex) :
        mutex_(std::move(mutex)) {
    }

    void handle_type(
        const uint8_t*& data,
        uint8_t* dest,
        const pb2::encoding_pb2::EncodedField& encoded_field_info,
        const entity::TypeDescriptor& type_descriptor,
        size_t dest_bytes,
        std::shared_ptr<BufferHolder> buffers
        );
};

struct ArrayHandlerOld {
    std::shared_ptr<std::mutex> mutex_ = std::make_shared<std::mutex>();

    ArrayHandlerOld(std::shared_ptr<std::mutex> mutex) :
        mutex_(std::move(mutex)) {
    }

    void handle_type(
        const uint8_t*& data,
        uint8_t* dest,
        const pb2::encoding_pb2::EncodedField& encoded_field_info,
        const entity::TypeDescriptor& type_descriptor,
        size_t dest_bytes,
        std::shared_ptr<BufferHolder> buffers
    );
};

struct EmptyHandler {
    std::shared_ptr<std::mutex> mutex_ = std::make_shared<std::mutex>();

    EmptyHandler(std::shared_ptr<std::mutex> mutex) :
        mutex_(std::move(mutex)) {
    }

    void handle_type(
        const uint8_t*& data,
        uint8_t* dest,
        const pb2::encoding_pb2::EncodedField& encoded_field_info,
        const entity::TypeDescriptor& type_descriptor,
        size_t dest_bytes,
        std::shared_ptr<BufferHolder> buffers
    );
};

struct BoolHandler {
    std::shared_ptr<std::mutex> mutex_ = std::make_shared<std::mutex>();

    BoolHandler(std::shared_ptr<std::mutex> mutex) :
        mutex_(std::move(mutex)) {
    }

    void handle_type(
        const uint8_t*& data,
        uint8_t* dest,
        const pb2::encoding_pb2::EncodedField& encoded_field_info,
        const entity::TypeDescriptor& type_descriptor,
        size_t dest_bytes,
        std::shared_ptr<BufferHolder> buffers
    );
};

} //namespace arcticdb
