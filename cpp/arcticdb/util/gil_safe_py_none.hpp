/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */


/* The class is introduced purely to selectively bypass the GIL check introduced in pybind11 v2.11.0. 
 * None in python is a global static. Therefore GIL is not needed. 
 * For other object, it's fine as long as GIL is held during allocation and deallocation, e.g. nan
 * To bypass the check, we could define PYBIND11_NO_ASSERT_GIL_HELD_INCREF_DECREF to globally disable the check
 */

#pragma once

#include <memory>
#include <mutex>
#include <pybind11/pybind11.h>

namespace arcticdb {
class GilSafePyNone {
private:
    static std::shared_ptr<pybind11::none> instance_;
    static std::once_flag init_flag_;
    static void init();
public:
    static std::shared_ptr<pybind11::none> instance();
};

} //namespace arcticdb