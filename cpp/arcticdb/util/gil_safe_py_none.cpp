/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */


#include <util/gil_safe_py_none.hpp>
#include <arcticdb/python/gil_lock.hpp>

using namespace arcticdb;

std::shared_ptr<pybind11::none> GilSafePyNone::instance_;
std::once_flag GilSafePyNone::init_flag_;


void GilSafePyNone::init(){
    pybind11::gil_scoped_acquire gil_lock;
    instance_ = std::make_shared<pybind11::none>();
};

std::shared_ptr<pybind11::none> GilSafePyNone::instance(){
    std::call_once(init_flag_, &init);
    return instance_;
};