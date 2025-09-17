#pragma once

#include <atomic>

#include <arcticdb/util/constructors.hpp>

namespace arcticdb::entity {
using timestamp = int64_t;
}

namespace arcticdb::async {

class BitRateStats {
  public:
    BitRateStats();
    void add_stat(std::size_t bytes, double time_ms);

    ARCTICDB_NO_MOVE_OR_COPY(BitRateStats)
  private:
    uint64_t data_to_stat(std::size_t bytes, double time_ms) const;
    void log_stats(uint64_t stats) const;

    // Use an 8 byte atomic for lock free implementation
    // Upper 5 bytes represent the number of bytes of data transferred (giving max representable value of 1TB)
    // Lower 3 bytes represent the total time in milliseconds (giving max representable value of 4.5 hours)
    std::atomic_uint64_t stats_{0};

    entity::timestamp last_log_time_ns_;
};

} // namespace arcticdb::async
