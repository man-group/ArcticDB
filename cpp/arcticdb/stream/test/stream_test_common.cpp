/*
* Copyright 2023 Man Group Operations Ltd.
* NO WARRANTY, EXPRESSED OR IMPLIED
*/

#include <arcticdb/stream/test/stream_test_common.hpp>

namespace arcticdb {

std::atomic<timestamp> PilotedClock::time_{0};

} //namespace arcticdb