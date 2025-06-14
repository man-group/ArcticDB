#include <arcticdb/entity/atom_key.hpp>
#include <string>
#include <vector>

namespace arcticdb {
struct StageResult {
    explicit StageResult(std::vector<entity::AtomKey>&& staged_segments) :
        staged_segments(std::move(staged_segments)) {}

    std::vector<entity::AtomKey> staged_segments;
    uint64_t version{ 0 };
};
}