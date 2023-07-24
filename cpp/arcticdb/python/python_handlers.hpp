#pragma once

#include <entity/types.hpp>
#include <util/type_handler.hpp>

namespace arcticdb {
    namespace py = pybind11;
    struct EmptyHandler {

        EmptyHandler() = default;

        void handle_type(
                const uint8_t*& data,
                uint8_t* dest,
                size_t dest_bytes,
                const arcticdb::proto::encoding::EncodedField& encoded_field_info,
                const entity::TypeDescriptor& type_descriptor,
                std::shared_ptr<BufferHolder> buffers
        );
    };
} //namespace arcticc