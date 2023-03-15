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

namespace arcticdb {

ModuleData::~ModuleData() {
    BufferPool::destroy_instance();
    TracingData::destroy_instance();
    SharedMemoryAllocator::destroy_instance();
    Allocator::destroy_instance();
    PrometheusInstance::destroy_instance();
    RemoteryInstance::destroy_instance();
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
}

std::shared_ptr<ModuleData> ModuleData::instance_;
std::once_flag ModuleData::init_flag_;

void shutdown_globals() {
    async::TaskScheduler::destroy_instance();
    storage::s3::S3ApiInstance::destroy_instance();
    storage::mongo::MongoInstance::destroy_instance();
    ModuleData::destroy_instance();
}

} //namespace arcticdb