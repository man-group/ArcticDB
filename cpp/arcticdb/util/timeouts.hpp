/*
* Copyright 2023 Man Group Operations Ltd.
* NO WARRANTY, EXPRESSED OR IMPLIED
*/

#pragma once

#include <folly/futures/Future.h>

namespace arcticdb::util::timeout {

inline folly::Duration get_default() {
    static folly::Duration duration(40 * 60 * 1000);
    return duration;
}

}
