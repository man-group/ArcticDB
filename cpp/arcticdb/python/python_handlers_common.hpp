/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/python/python_handlers.hpp>
#include <arcticdb/arrow/arrow_handlers.hpp>

namespace arcticdb {
/// Register handling of non-trivial types. For more information @see arcticdb::TypeHandlerRegistry and
/// @see arcticdb::ITypeHandler
inline void register_type_handlers() {
    using namespace arcticdb;
    TypeHandlerRegistry::instance()->register_handler(
            OutputFormat::PANDAS, make_scalar_type(entity::DataType::EMPTYVAL), arcticdb::PythonEmptyHandler()
    );
    TypeHandlerRegistry::instance()->register_handler(
            OutputFormat::PANDAS, make_scalar_type(entity::DataType::BOOL_OBJECT8), arcticdb::PythonBoolHandler()
    );

    register_python_array_types();
    register_python_string_types();

    register_arrow_string_types();

    register_python_handler_data_factory();
    register_arrow_handler_data_factory();
}
} // namespace arcticdb