/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <memory>
#include <mutex>

namespace arcticdb {

struct ModuleData {
    ~ModuleData();

    static std::shared_ptr<ModuleData> instance_;
    static std::once_flag init_flag_;

    static void init();
    static std::shared_ptr<ModuleData> instance();
    static void destroy_instance();
};

void shutdown_globals();

} // namespace arcticdb