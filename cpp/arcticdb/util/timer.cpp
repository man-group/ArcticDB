#include <arcticdb/util/timer.hpp>
#include <boost/date_time/posix_time/time_formatters.hpp>
#include <boost/date_time/posix_time/posix_time_duration.hpp>

namespace arcticdb {
    std::string date_and_time(int64_t ts) {
        const std::time_t seconds_since_epoch = ts / BILLION;
        return boost::posix_time::to_iso_string(boost::posix_time::seconds(seconds_since_epoch));
    }
}
