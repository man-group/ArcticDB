#define BOOST_DATE_TIME_POSIX_TIME_STD_CONFIG // Allows using nanoseconds in boost.
#include <arcticdb/util/format_date.hpp>
#include <boost/date_time/posix_time/time_formatters.hpp>
#include <boost/date_time/posix_time/posix_time_duration.hpp>
#include <boost/date_time/posix_time/conversion.hpp>
#include <arcticdb/util/constants.hpp>

namespace arcticdb::util {

    std::string format_timestamp(const entity::timestamp ts) {
        if (ts == NaT) {
            return "NaT";
        }
        // Use boost as it can handle nanoseconds both on all OS's.
        // std::std::chrono::time_point<std::chrono::system_clock> does not handle nanoseconds on Windows and Mac
        const boost::posix_time::ptime epoch = boost::posix_time::from_time_t(0);
        const boost::posix_time::ptime time = epoch + boost::posix_time::nanoseconds{ts};
        // Custom formatting seems to work best compared to other options.
        // * using std::put_time(std::gmtime(...)) throws on Windows when pre-epoch dates are used
        // * using boosts time_facet requires the facet used for formatting to be allocated on the heap for each
        //   formatting call (because it requires calling std::stringstream::imbue which takes onwership of the passed
        //   pointer
        return fmt::format(
            "{}-{:02}-{:02} {:02}:{:02}:{:02}.{:09}",
            int{time.date().year()},
            int{time.date().month()},
            int{time.date().day()},
            time.time_of_day().hours(),
            time.time_of_day().minutes(),
            time.time_of_day().seconds(),
            time.time_of_day().fractional_seconds()
        );
    }
}
