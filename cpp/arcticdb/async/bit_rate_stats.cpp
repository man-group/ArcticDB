#include "bit_rate_stats.hpp"

#include <folly/Likely.h>

#include <arcticdb/log/log.hpp>
#include <arcticdb/util/format_bytes.hpp>
#include <arcticdb/entity/performance_tracing.hpp>
#include <arcticdb/util/clock.hpp>

constexpr uint64_t max_bytes{0xFFFFFFFFFF};
constexpr uint64_t max_time_ms{0xFFFFFF};
constexpr arcticdb::entity::timestamp log_frequency_ns{60LL * 1000L * 1000L * 1000L};

namespace arcticdb::async {

BitRateStats::BitRateStats() : last_log_time_ns_(util::SysClock::coarse_nanos_since_epoch()) {}

void BitRateStats::add_stat(std::size_t bytes, double time_ms) {
    auto now = util::SysClock::coarse_nanos_since_epoch();
    uint64_t stat = data_to_stat(bytes, time_ms);
    auto previous_stats = stats_.fetch_add(stat);
    auto current_stats = previous_stats + stat;
    if (now - last_log_time_ns_ > log_frequency_ns && stats_.compare_exchange_strong(current_stats, 0)) {
        last_log_time_ns_ = now;
        log_stats(current_stats);
    }
}

uint64_t BitRateStats::data_to_stat(std::size_t bytes, double time_ms) const {
    if (UNLIKELY(bytes > max_bytes || time_ms > max_time_ms)) {
        log::storage().warn(
                "Bit rate stats provided too large to represent, ignoring: {} in {}ms", format_bytes(bytes), time_ms
        );
        return 0;
    }
    uint64_t stat{(bytes << 24) + static_cast<uint64_t>(time_ms)};
    return stat;
}

void BitRateStats::log_stats(uint64_t stats) const {
    double time_s = static_cast<double>(stats & max_time_ms) / 1000;
    double bytes = static_cast<double>(stats >> 24);
    double bandwidth = bytes / time_s;
    log::storage().info("Byte rate {}/s", format_bytes(bandwidth));
    std::string log_msg = "Current BW is " + format_bytes(bandwidth) + "/s";
    ARCTICDB_SAMPLE_LOG(log_msg.c_str());
}

} // namespace arcticdb::async
