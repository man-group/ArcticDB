/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/util/type_handler.hpp>

namespace arcticdb {

std::shared_ptr<TypeHandlerRegistry> TypeHandlerRegistry::instance() {
    std::call_once(TypeHandlerRegistry::init_flag_, &TypeHandlerRegistry::init);
    return TypeHandlerRegistry::instance_;
}

void TypeHandlerRegistry::init() { TypeHandlerRegistry::instance_ = std::make_shared<TypeHandlerRegistry>(); }

std::shared_ptr<TypeHandlerRegistry> TypeHandlerRegistry::instance_;
std::once_flag TypeHandlerRegistry::init_flag_;

std::shared_ptr<TypeHandler> TypeHandlerRegistry::get_handler(
        OutputFormat output_format, const entity::TypeDescriptor& type_descriptor
) const {
    const auto& map = handler_map(output_format);
    auto it = map.find(type_descriptor);
    return it == std::end(map) ? std::shared_ptr<TypeHandler>{} : it->second;
}

void TypeHandlerRegistry::destroy_instance() { TypeHandlerRegistry::instance_.reset(); }

void TypeHandlerRegistry::register_handler(
        OutputFormat output_format, const entity::TypeDescriptor& type_descriptor, TypeHandler&& handler
) {
    handler_map(output_format).try_emplace(type_descriptor, std::make_shared<TypeHandler>(std::move(handler)));
}

size_t TypeHandlerRegistry::Hasher::operator()(const entity::TypeDescriptor descriptor) const {
    static_assert(
            sizeof(descriptor) == sizeof(uint16_t), "Cannot compute util::TypeDescriptor's hash. The size is wrong."
    );
    static_assert(sizeof(decltype(descriptor.data_type())) == 1);
    static_assert(sizeof(decltype(descriptor.dimension())) == 1);
    const std::hash<uint16_t> hasher;
    return hasher(uint16_t(descriptor.data_type()) << 8 | uint16_t(descriptor.dimension()));
}

} // namespace arcticdb
