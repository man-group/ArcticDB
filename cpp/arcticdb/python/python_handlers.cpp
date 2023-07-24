#include <python/python_handlers.hpp>
#include <python/gil_lock.hpp>

namespace arcticdb {
    void EmptyHandler::handle_type(
            const uint8_t*& data ARCTICDB_UNUSED,
            uint8_t* dest,
            size_t dest_bytes,
            const arcticdb::proto::encoding::EncodedField& encoded_field_info ARCTICDB_UNUSED,
            const entity::TypeDescriptor& type_descriptor ARCTICDB_UNUSED,
            std::shared_ptr<BufferHolder> buffers ARCTICDB_UNUSED
    ) {
        ARCTICDB_SAMPLE(HandleEmpty, 0)
        util::check(dest != nullptr, "Got null destination pointer");
        py::none none = {};
        const size_t num_rows = dest_bytes / get_type_size(DataType::EMPTYVAL);
        const PyObject** ptr_dest = reinterpret_cast<const PyObject**>(dest);
        ScopedGILLock lock;
        for(auto row = 0u; row < num_rows; ++row) {
            none.inc_ref();
            *ptr_dest++ = none.ptr();
        }
    }
}