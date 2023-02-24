/*
* Copyright 2023 Man Group Operations Ltd.
* NO WARRANTY, EXPRESSED OR IMPLIED
*/

#pragma once

#include <memory>
#include <arcticdb/log/log.hpp>


namespace arcticdb {
    struct ModuleData {
        std::shared_ptr<log::Loggers> loggers_;

    };
}