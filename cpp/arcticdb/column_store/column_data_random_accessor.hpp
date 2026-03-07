/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <folly/Poly.h>

#include <arcticdb/column_store/column_data.hpp>
#include <arcticdb/util/type_traits.hpp>

namespace arcticdb {

template<typename TDT>
struct IColumnDataRandomAccessor {
    template<class Base>
    struct Interface : Base {
        typename TDT::DataTypeTag::raw_type at(size_t idx) const { return folly::poly_call<0>(*this, idx); };
    };

    template<class T>
    using Members = folly::PolyMembers<&T::at>;
};

template<typename TDT>
using ColumnDataRandomAccessor = folly::Poly<IColumnDataRandomAccessor<TDT>>;

template<typename TDT>
requires(util::instantiation_of<TDT, TypeDescriptorTag> && (TDT::dimension() == Dimension::Dim0))
ColumnDataRandomAccessor<TDT> random_accessor(ColumnData* parent);
} // namespace arcticdb