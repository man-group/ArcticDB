/*
 * Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/processing/operation_dispatch_binary.hpp>

namespace arcticdb {

template VariantData visit_binary_operator<PlusOperator>(const VariantData&, const VariantData&, PlusOperator&&);
template VariantData visit_binary_operator<MinusOperator>(const VariantData&, const VariantData&, MinusOperator&&);
template VariantData visit_binary_operator<TimesOperator>(const VariantData&, const VariantData&, TimesOperator&&);
template VariantData visit_binary_operator<DivideOperator>(const VariantData&, const VariantData&, DivideOperator&&);

}