#pragma once

#include <arcticdb/util/type_handler.hpp>

namespace arcticdb {
struct NativeHandlerData {

};

struct NativeHandlerDataFactory  : public TypeHandlerDataFactory {
    std::shared_ptr<std::any> get_data() const override {
        return std::make_shared<std::any>(std::make_any<NativeHandlerData>());
    }
};

inline void register_native_handler_data_factory() {
    TypeHandlerRegistry::instance()->set_handler_data(OutputFormat::NATIVE, std::make_unique<NativeHandlerDataFactory>());
}
}