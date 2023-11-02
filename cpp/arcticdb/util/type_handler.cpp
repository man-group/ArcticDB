/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/util/type_handler.hpp>

namespace arcticdb {

std::shared_ptr<TypeHandlerRegistry> TypeHandlerRegistry::instance(){
    std::call_once(TypeHandlerRegistry::init_flag_, &TypeHandlerRegistry::init);
    return TypeHandlerRegistry::instance_;
}

void TypeHandlerRegistry::init() {
    TypeHandlerRegistry::instance_ = std::make_shared<TypeHandlerRegistry>();
}

std::shared_ptr<TypeHandlerRegistry> TypeHandlerRegistry::instance_;
std::once_flag TypeHandlerRegistry::init_flag_;

std::shared_ptr<TypeHandler> TypeHandlerRegistry::get_handler(const util::TypeDescriptor& type_descriptor) const {
    auto it = handlers_.find(type_descriptor);
    return it == std::end(handlers_) ? std::shared_ptr<TypeHandler>{} : it->second;
}

void TypeHandlerRegistry::register_handler(const util::TypeDescriptor& type_descriptor, TypeHandler&& handler) {
     handlers_.try_emplace(type_descriptor, std::make_shared<TypeHandler>(std::move(handler)));
}

size_t TypeHandlerRegistry::Hasher::operator()(const util::TypeDescriptor val) const {
    static_assert(sizeof(val) == sizeof(uint16_t), "Cannot compute util::TypeDescriptor's hash. The size is wrong.");
    const std::hash<uint16_t> hasher;
    return hasher(*reinterpret_cast<const uint16_t*>(&val));
}

} //namespace arcticdb
