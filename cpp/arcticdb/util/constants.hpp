/*
* Copyright 2023 Man Group Operations Ltd.
* NO WARRANTY, EXPRESSED OR IMPLIED
*/

#pragma once

namespace arcticdb {

using timestamp = int64_t;

static constexpr decltype(timestamp(0) - timestamp(0)) ONE_SECOND = 1'000'000'000;

static constexpr decltype(timestamp(0) - timestamp(0)) ONE_MINUTE = 60 * ONE_SECOND;

} //namespace arcticdb
