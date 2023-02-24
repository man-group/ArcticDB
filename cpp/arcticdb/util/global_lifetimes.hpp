/*
* Copyright 2023 Man Group Operations Ltd.
* NO WARRANTY, EXPRESSED OR IMPLIED
*/

#pragma once

#include <memory>
#include <mutex>

namespace arcticdb {

struct ModuleData{
    ~ModuleData();

    static std::shared_ptr<ModuleData> instance_;
    static std::once_flag init_flag_;

    static void init();
    static std::shared_ptr<ModuleData> instance();
    static void destroy_instance();
};


void shutdown_globals();

} //namespace arcticdb