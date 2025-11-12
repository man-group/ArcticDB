#include <arcticdb/entity/stage_result.hpp>

namespace arcticdb {
std::string StageResult::view() const { 
    return fmt::format("{}", *this); 
}
} // namespace arcticdb
