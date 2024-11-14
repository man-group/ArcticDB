/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/util/global_lifetimes.hpp>
#include <arcticdb/async/task_scheduler.hpp>
#include <arcticdb/util/allocator.hpp>
#include <arcticdb/storage/s3/s3_api.hpp>
#include <arcticdb/storage/mongo/mongo_instance.hpp>
#include <arcticdb/log/log.hpp>
#include <arcticdb/entity/metrics.hpp>
#include <arcticdb/util/buffer_pool.hpp>
#include <arcticdb/util/type_handler.hpp>
#include <arcticdb/util/allocation_tracing.hpp>

#if defined(_MSC_VER) && defined(_DEBUG)
#include <crtdbg.h>
#include <util/configs_map.hpp>
#endif

namespace arcticdb {

ModuleData::~ModuleData() {
#ifdef ARCTICDB_COUNT_ALLOCATIONS
    AllocationTracker::destroy_instance();
#endif
    BufferPool::destroy_instance();
    TracingData::destroy_instance();
    Allocator::destroy_instance();
    PrometheusInstance::destroy_instance();
#if defined(USE_REMOTERY)
    RemoteryInstance::destroy_instance();
#endif
    TypeHandlerRegistry::destroy_instance();
    ARCTICDB_DEBUG(log::version(), "Destroying AWS instance");
    storage::s3::S3ApiInstance::destroy_instance();
    log::Loggers::destroy_instance();
}

std::shared_ptr<ModuleData> ModuleData::instance() {
    std::call_once(ModuleData::init_flag_, &ModuleData::init);
    return instance_;
}

void ModuleData::destroy_instance() {
    ModuleData::instance_.reset();
}

void ModuleData::init() {
    ModuleData::instance_ = std::make_shared<ModuleData>();

#if defined(_MSC_VER) && defined(_DEBUG)
    if (auto config = ConfigsMap::instance()->get_int("CrtDbgDialog"); !config || config.value() == 0) {
        log::root().info("_CrtSetReportMode(..., _CRTDBG_MODE_DEBUG)");
        _CrtSetReportMode(_CRT_ERROR, _CRTDBG_MODE_DEBUG);
        _CrtSetReportMode(_CRT_ASSERT, _CRTDBG_MODE_DEBUG);
    }
#endif
}

std::shared_ptr<ModuleData> ModuleData::instance_;
std::once_flag ModuleData::init_flag_;

void shutdown_globals() {
    async::TaskScheduler::destroy_instance();
    storage::mongo::MongoInstance::destroy_instance();
    ModuleData::destroy_instance();
}

} //namespace arcticdb
