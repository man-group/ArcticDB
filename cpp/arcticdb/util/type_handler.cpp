/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/util/type_handler.hpp>

namespace arcticdb {

std::shared_ptr<TypeHandlerRegistry> TypeHandlerRegistry::instance()
{
    std::call_once(TypeHandlerRegistry::init_flag_, &TypeHandlerRegistry::init);
    return TypeHandlerRegistry::instance_;
}

void TypeHandlerRegistry::init()
{
    TypeHandlerRegistry::instance_ = std::make_shared<TypeHandlerRegistry>();
}

std::shared_ptr<TypeHandlerRegistry> TypeHandlerRegistry::instance_;
std::once_flag TypeHandlerRegistry::init_flag_;

std::shared_ptr<TypeHandler> TypeHandlerRegistry::get_handler(entity::DataType data_type) const
{
    auto it = handlers_.find(data_type);
    return it == std::end(handlers_) ? std::shared_ptr<TypeHandler>{} : it->second;
}

void TypeHandlerRegistry::register_handler(entity::DataType data_type, TypeHandler&& handler)
{
    handlers_.try_emplace(data_type, std::make_shared<TypeHandler>(std::move(handler)));
}

} // namespace arcticdb
