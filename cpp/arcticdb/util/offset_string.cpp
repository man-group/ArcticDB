/*
* Copyright 2023 Man Group Operations Ltd.
* NO WARRANTY, EXPRESSED OR IMPLIED
*/

#include <arcticdb/util/offset_string.hpp>
#include <arcticdb/column_store/string_pool.hpp>

namespace arcticdb {
OffsetString::operator std::string_view() const { return pool_->get_view(offset()); }
} //namespace arcticdb