/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/stream/test/stream_test_common.hpp>
#include <arcticdb/stream/piloted_clock.hpp>
namespace arcticdb {

std::atomic<timestamp> PilotedClock::time_{0};

} //namespace arcticdb