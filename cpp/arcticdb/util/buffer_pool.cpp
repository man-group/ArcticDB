/*
* Copyright 2023 Man Group Operations Ltd.
* NO WARRANTY, EXPRESSED OR IMPLIED
*/

#include <arcticdb/util/buffer_pool.hpp>
#include <memory>

namespace arcticdb {

std::shared_ptr<BufferPool> BufferPool::instance(){
    std::call_once(BufferPool::init_flag_, &BufferPool::init);
    return BufferPool::instance_;
}

void BufferPool::destroy_instance() {
    if(instance_)
        instance_->clear();
    instance_.reset();
}

void BufferPool::init() {
    instance_ = std::make_shared<BufferPool>();
}

BufferPool::BufferPool() : pool_(
            [] () { return std::make_shared<Buffer>(); },
            [] (std::shared_ptr<Buffer> buf) { buf->reset(); }
    ){
}

std::shared_ptr<BufferPool> BufferPool::instance_;
std::once_flag BufferPool::init_flag_;

}  //namespace arcticdb