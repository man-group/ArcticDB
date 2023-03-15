/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/entity/protobufs.hpp>
#include <arcticdb/util/random.h>
#include <arcticdb/util/constructors.hpp>

namespace arcticdb {

enum class FailureType : int {
    WRITE = 0,
    READ
};

static const char* failure_names[] = {
        "WRITE",
        "READ"
};

struct FailureCategory {
    double prob_;
    bool thrown_;

    explicit FailureCategory(double prob) :  prob_(prob), thrown_(false) {}
};

class StorageFailureSimulator {
public:
    static std::shared_ptr<StorageFailureSimulator> instance();
    static std::shared_ptr<StorageFailureSimulator> instance_;
    static std::once_flag init_flag_;
    static void init(){
        instance_ = std::make_shared<StorageFailureSimulator>();
    }
    static void destroy_instance(){instance_.reset();}

    StorageFailureSimulator() :
        configured_(false) {
    }

    void configure(const arcticdb::proto::storage::VersionStoreConfig::StorageFailureSimulator& cfg)  {
        log::storage().info("Initializing storage failure simulator");
        configured_ = true;
        categories_.insert(std::make_pair(FailureType::WRITE, FailureCategory{cfg.write_failure_prob()}));
        categories_.insert(std::make_pair(FailureType::READ, FailureCategory{cfg.read_failure_prob()}));
    }

    FailureCategory& find(FailureType failure_type) {
        auto item = categories_.find(failure_type);
        if(item == categories_.end())
            util::raise_rte("Unknown failure type {}", failure_type);

        return item->second;
    }

    bool configured() const {
        return configured_;
    }

    ARCTICDB_NO_MOVE_OR_COPY(StorageFailureSimulator)

    void go(FailureType failure_type) {
        util::check(configured_, "Attempted failure simulation in unconfigured class");
        thread_local std::once_flag flag;
        std::atomic<uint64_t> counter{42};
        std::call_once(flag, [&counter]() { init_random(counter++); });
        auto& category = find(failure_type);
        category.thrown_ = true;
        throw std::runtime_error(fmt::format("Simulating storage failure {}", failure_type));
    }

    bool was_thrown(FailureType failure_type)  {
        auto category = find(failure_type);
        return category.thrown_;
    }

private:
    std::unordered_map<FailureType, FailureCategory> categories_;
    bool configured_;
};

} //namespace arcticdb

namespace fmt {
template<>
struct formatter<arcticdb::FailureType> {
    template<typename ParseContext>
    constexpr auto parse(ParseContext &ctx) { return ctx.begin(); }

    template<typename FormatContext>
    auto format(arcticdb::FailureType failure_type, FormatContext &ctx) const {
        return format_to(ctx.out(), arcticdb::failure_names[int(failure_type)]);
    }
};
}
