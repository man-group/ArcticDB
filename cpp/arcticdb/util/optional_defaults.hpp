/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <optional>

namespace arcticdb {

// TODO remove
inline bool opt_false(const std::optional<bool>& opt) { return opt && *opt; }
} // namespace arcticdb