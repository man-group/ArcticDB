/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

namespace arcticdb {

using timestamp = int64_t;

static constexpr decltype(timestamp(0) - timestamp(0)) ONE_SECOND = 1'000'000'000;

static constexpr decltype(timestamp(0) - timestamp(0)) ONE_MINUTE = 60 * ONE_SECOND;

} //namespace arcticdb
