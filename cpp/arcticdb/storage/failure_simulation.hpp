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
#include <arcticdb/util/preconditions.hpp>
#include <arcticdb/util/variant.hpp>
#include <folly/Function.h>

namespace arcticdb {

#ifdef _WIN32
#undef DELETE
#endif

enum class FailureType : int {
    WRITE = 0,
    READ,
    ITERATE,
    DELETE,
};

static const char* failure_names[] = {
        "WRITE",
        "READ",
        "ITERATE",
        "DELETE",
};

}

// Formatters are defined here since they are used in implementations bellow.
namespace fmt {
template<>
struct formatter<arcticdb::FailureType> {
    template<typename ParseContext>
    constexpr auto parse(ParseContext &ctx) { return ctx.begin(); }

    template<typename FormatContext>
    auto format(const arcticdb::FailureType failure_type, FormatContext &ctx) const {
        return fmt::format_to(ctx.out(), fmt::runtime(arcticdb::failure_names[int(failure_type)]));
    }
};
}

namespace arcticdb {

/** Function holder with a description. */
struct FailureAction {
    using Description = std::variant<const char*, std::string>;
    using FunctionWrapper = folly::Function<void(FailureType)>;

    Description description_;
    FunctionWrapper::SharedProxy proxy_;

    FailureAction(Description description, FunctionWrapper::SharedProxy proxy) :
            description_(std::move(description)), proxy_(std::move(proxy)) {}

    template<typename Func>
    FailureAction(Description description, Func func):
            FailureAction(std::move(description), FunctionWrapper{func}.asSharedProxy()) {}

    inline void operator()(FailureType type) const {
        proxy_(type);
    }
};

inline std::ostream& operator<<(std::ostream& out, const FailureAction& action) {
    std::visit([&out](const auto& desc) { out << desc; }, action.description_);
    return out;
}

namespace action_factories { // To allow `using namespace`
static inline const FailureAction no_op("no_op", [](FailureType){});

/** Raises the given exception with the given probability. */
template<class Exception = StorageException>
static inline FailureAction fault(double probability = 1.0) {
    util::check_arg(probability >= 0, "Bad probability: {}", probability);

    if (probability >= 1.0) {
        return {"raise",  [](FailureType failure_type) {
            throw Exception(fmt::format("Simulating {} storage failure", failure_type));
        }};
    } else {
        return {fmt::format("fault({})", probability),
                [prob=probability](FailureType failure_type) {
                    thread_local std::once_flag flag;
                    std::call_once(flag, [seed = uint64_t(&failure_type)]() { init_random(seed); });
                    if (random_probability() < prob) {
                        throw Exception(fmt::format("Simulating {} storage failure", failure_type));
                    }
                }};
    }
}
}

/** Independent state for each FailureType. Thread-safe except for the c'tors. */
class FailureTypeState {
public:
    using ActionSequence = std::vector<FailureAction>;
    static_assert(std::is_copy_assignable_v<ActionSequence>);

private:
    friend class StorageFailureSimulator;

    const ActionSequence sequence_;
    std::atomic<size_t> cursor_ {0}; // Index into sequence

public:
    explicit FailureTypeState(ActionSequence sequence) :
            sequence_(sequence.empty() ? ActionSequence{action_factories::no_op} : std::move(sequence)) {}

    const ActionSequence::value_type& pick_action() {
        if (cursor_ < sequence_.size()) {
            if (auto local = cursor_.fetch_add(1); local < sequence_.size()) {
                return sequence_[local];
            }
        }
        return sequence_.back();
    }
};

// Note that StorageFailureSimulator currently is only used for the following storages:
// - Mongo storage
// - InMemoryStore (only in cpp tests)
class StorageFailureSimulator {
public:
    using ParamActionSequence = FailureTypeState::ActionSequence;
    /**
     * Easy-to-copy parameters that can be used to configure this class. Useful in parameterized tests.
     * The string is a sequence of "action indicators" selecting the action to perform for each call.
     * After this sequence is exhausted, the last action is used for all subsequent calls.
     */
    using Params = std::unordered_map<FailureType, ParamActionSequence>;

    static std::shared_ptr<StorageFailureSimulator> instance();
    static void reset() {
        instance_ = std::make_shared<StorageFailureSimulator>();
    }
    static void destroy_instance(){instance_.reset();}

    StorageFailureSimulator() : configured_(false) {}

    void configure(const arcticdb::proto::storage::VersionStoreConfig::StorageFailureSimulator& cfg)  {
        configure({{FailureType::WRITE, {action_factories::fault(cfg.write_failure_prob())}},
                   {FailureType::READ, {action_factories::fault(cfg.read_failure_prob())}}});
    }

    void configure(const Params& params) {
        log::storage().info("Initializing storage failure simulator");
        for (const auto& [type, sequence]: params) {
            // Due to the atomic in FailureTypeState, it cannot be moved, so has to be constructed in-place:
            categories_.emplace(std::piecewise_construct,
                    std::forward_as_tuple(type),
                    std::forward_as_tuple(sequence));
        }
        configured_ = true;
    }

    bool configured() const {
        return configured_;
    }

    ARCTICDB_NO_MOVE_OR_COPY(StorageFailureSimulator)

    void go(FailureType failure_type) {
        if (ARCTICDB_LIKELY(!configured_)) return;
        util::check(configured_, "Attempted failure simulation in unconfigured class");
        if (auto itr = categories_.find(failure_type); itr != categories_.end()) {
            auto& state = itr->second;
            auto& action = state.pick_action();
            action(failure_type);
        }
    }

private:
    static std::shared_ptr<StorageFailureSimulator> instance_;
    static std::once_flag init_flag_;

    std::unordered_map<FailureType, FailureTypeState> categories_;
    bool configured_;
};

} //namespace arcticdb

