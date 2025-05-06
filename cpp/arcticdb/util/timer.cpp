#include <arcticdb/util/timer.hpp>
#include <boost/date_time/posix_time/time_formatters.hpp>
#include <boost/date_time/posix_time/posix_time_duration.hpp>
#include <boost/date_time/posix_time/conversion.hpp>

namespace arcticdb {
    std::string date_and_time(int64_t ts) {
        const auto epoch = boost::posix_time::from_time_t(0);
#ifndef BOOST_DATE_TIME_POSIX_TIME_STD_CONFIG
        const auto offest = boost::posix_time::microseconds(ts / 1000);
#else
        const auto offset = boost::posix_time::nanoseconds(ts);
#endif
        return boost::posix_time::to_simple_string(epoch + offset);
    }
}
