#pragma once

#include <arcticdb/arrow/arrow_c_data_interface.hpp>
#include <arcticdb/arrow/arrow_wrappers.h>
#include <arcticdb/util/preconditions.hpp>

namespace arcticdb {

class Error {
public:
    ArrowError c_error;

    Error() {
        std::memset(&c_error, 0, sizeof(ArrowError));
    }

    void raise_message(const std::string& what, int code) {
        log::inmem().info("{}: {} '{}'", code, what, std::string(c_error.message));  //TODO maybe arrow logger?
    }

    static void raise_error(const std::string& what, int code) {
        arrow::raise<ErrorCode::E_ARROW_INVALID>("{}: {}", code, what);
    }
};
}