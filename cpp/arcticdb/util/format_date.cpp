#define BOOST_DATE_TIME_POSIX_TIME_STD_CONFIG // Allows using nanoseconds in boost.
#include <arcticdb/util/format_date.hpp>
#include <boost/date_time/posix_time/posix_time_duration.hpp>
#include <boost/date_time/posix_time/conversion.hpp>
#include <arcticdb/util/constants.hpp>

namespace arcticdb::util {

    std::string format_timestamp(const entity::timestamp ts) {
        if (ts == NaT) {
            return "NaT";
        }
        // Use boost as it can handle nanoseconds on all OS's.
        // std::std::chrono::time_point<std::chrono::system_clock> does not handle nanoseconds on Windows and Mac.
        const auto div = std::lldiv(ts, 1'000'000'000);
        const timestamp seconds = div.quot;
        const timestamp ns_remainder = div.rem;
        const boost::posix_time::ptime epoch = boost::posix_time::from_time_t(0);
        // Split into seconds and nanoseconds fractions because using epoch + boost::posix_time::nanoseconds fails when
        // std::numeric_limits<int64_t>::max() is used due to overflow
        const boost::posix_time::ptime dt = epoch + boost::posix_time::seconds(seconds) + boost::posix_time::nanoseconds(ns_remainder);

        // Custom formatting seems to work best compared to other options.
        // * using std::put_time(std::gmtime(...)) throws on Windows when pre-epoch dates are used (pre-epoch is UB)
        // * using Boost's time_facet requires the facet used for formatting to be allocated on the heap for each
        //   formatting call (because it requires calling std::stringstream::imbue which takes onwership of the passed
        //   pointer)
        return fmt::format(
            "{}-{:02}-{:02} {:02}:{:02}:{:02}.{:09}",
            int{dt.date().year()},
            int{dt.date().month()},
            int{dt.date().day()},
            dt.time_of_day().hours(),
            dt.time_of_day().minutes(),
            dt.time_of_day().seconds(),
            dt.time_of_day().fractional_seconds()
        );
    }
}
