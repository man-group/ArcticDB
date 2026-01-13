/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/util/preconditions.hpp>
// This exports CLOCK_REALTIME, used below
#ifdef _WIN32
#include <folly/portability/Time.h>
#endif

#include <folly/Function.h>
#include <fmt/format.h>

#include <string>
#include <map>
#include <ctime>
#include <iomanip>

namespace arcticdb {

struct interval;

typedef interval* interval_ptr;
typedef std::basic_string<char> name_type;
typedef std::basic_string<char> result_type;
typedef std::map<name_type, interval_ptr> interval_map;
typedef std::pair<name_type, interval_ptr> interval_type;
typedef interval_map::iterator interval_iterator;
typedef std::vector<double> total_list;
// Use an ordered map to control the order that timings are logged in
typedef std::map<name_type, double> total_map;

struct interval_results {
    double total;
    int64_t count;
    double mean;
};

struct interval {
  private:
    double total_;
    int64_t count_;
    timespec timer_;
    bool running_;

  public:
    interval() : total_(0), count_(0), timer_{0, 0}, running_(false) {}

    void start() {
        if (!running_) {
            running_ = true;
            get_time(timer_);
            count_++;
        }
    }

    void end() {
        if (!running_)
            return;

        timespec tm{0, 0};
        get_time(tm);
        total_ += time_diff(timer_, tm);
        running_ = false;
    }

    void get_results(interval_results& results) const {
        results.count = count_;
        results.total = total_;
        results.mean = total_ / count_;
    }

    interval_results get_results() const {
        interval_results results;
        results.count = count_;
        results.total = total_;
        results.mean = total_ / count_;
        return results;
    }

    double get_results_total() const { return total_; }

  private:
    void get_time(timespec& tm) {
#ifdef _WIN32
        int rc = clock_gettime(CLOCK_REALTIME, &tm);
#else
        int rc = clock_gettime(CLOCK_MONOTONIC, &tm);
#endif
        if (rc == -1) {
            throw std::runtime_error("Failed to get time");
        }
    }

#define BILLION 1000000000LL
    static double time_diff(timespec& start, timespec& stop) {
        double secs = stop.tv_sec - start.tv_sec;
        double nsecs = double(stop.tv_nsec - start.tv_nsec) / BILLION;
        return secs + nsecs;
    }
};

class interval_timer {

  public:
    interval_timer() = default;

    explicit interval_timer(const name_type& name) { start_timer(name); }

    ~interval_timer() {
        for (const auto& current : intervals_) {
            delete (current.second);
        }
    }

    void start_timer(const name_type& name = "default") {
        auto it = intervals_.find(name);
        if (it == intervals_.end()) {
            auto created = new interval();
            intervals_.try_emplace(name, created);
            created->start();
        } else
            (*it).second->start();
    }

    void stop_timer(const name_type& name = "default") {
        auto it = intervals_.find(name);
        if (it != intervals_.end())
            (*it).second->end();
    }

    result_type display_timer(const name_type& name = "default") {
        result_type ret;
        auto it = intervals_.find(name);
        if (it == intervals_.end())
            return ret;

        it->second->end();
        interval_results results{};
        (*it).second->get_results(results);
        if (results.count > 1) {
            auto buffer = fmt::format(
                    "{}:\truns: {}\ttotal time: {:.6f}\tmean: {:.6f}",
                    (*it).first.c_str(),
                    results.count,
                    results.total,
                    results.mean
            );
            ret.assign(buffer);
        } else {
            auto buffer = fmt::format("{}\t{:.6f}", (*it).first.c_str(), results.total);
            ret.assign(buffer);
        }
        return ret;
    }

    result_type display_all() {
        result_type ret;
        for (auto& current : intervals_) {
            ret.append(display_timer(current.first) + "\n");
        }
        return ret;
    }

    total_list get_total_all() {
        total_list ret;
        for (auto& current : intervals_) {
            current.second->end();
            interval_results results{};
            current.second->get_results(results);
            ret.push_back(results.total);
        }
        return ret;
    }

    total_map get_total_map() const {
        total_map ret;
        for (const auto& [name, timer] : intervals_) {
            timer->end();
            ret.try_emplace(name, timer->get_results_total());
        }
        return ret;
    }

    const interval& get_timer(const name_type& name = "default") {
        auto it = intervals_.find(name);
        util::check(it != intervals_.end(), "Timer {} not found, name");
        return *it->second;
    }

  private:
    interval_map intervals_;
};

/* Timer helper, use like so:
 *
   ScopedTimer timer{"read_partial", [](auto msg) {
        ARCTICDB_DEBUG(log::version(), msg);
    }};
*/
class ScopedTimer {
    std::string name_;
    interval_timer timer_;
    using FuncType = folly::Function<void(std::string)>;
    FuncType func_;
    bool started_ = false;

  public:
    ScopedTimer() = default;

    ScopedTimer(const std::string& name, FuncType&& func) : name_(name), func_(std::move(func)) {
        timer_.start_timer(name_);
        started_ = true;
    }

    ScopedTimer(arcticdb::ScopedTimer& other) = delete; // Folly Functions are non-copyable

    ScopedTimer(arcticdb::ScopedTimer&& other) : name_(std::move(other.name_)), func_(std::move(other.func_)) {
        if (other.started_) {
            timer_.start_timer(name_);
            started_ = true;
            other.started_ = false;
        }
    }

    ScopedTimer& operator=(arcticdb::ScopedTimer& other) = delete; // Folly Functions are non-copyable

    ScopedTimer& operator=(arcticdb::ScopedTimer&& other) {
        if (this == &other) {
            return *this;
        }
        name_ = std::move(other.name_);
        func_ = std::move(other.func_);
        if (other.started_) {
            timer_.start_timer(name_);
            started_ = true;
            other.started_ = false;
        }
        return *this;
    }

    ~ScopedTimer() {
        if (started_) {
            timer_.stop_timer(name_);
            func_(timer_.display_all());
        }
    }
};

/* Timer helper, use like so:
  ScopedTimer timer{"read_partial", [](auto time) {
        ARCTICDB_DEBUG(log::version(), time);
    }};
*/

class ScopedTimerTotal {
    std::string name_;
    interval_timer timer_;
    using FuncType = folly::Function<void(std::vector<double>)>;
    FuncType func_;

  public:
    ScopedTimerTotal(const std::string& name, FuncType&& func) : name_(name), func_(std::move(func)) {
        timer_.start_timer(name_);
    }

    ~ScopedTimerTotal() {
        timer_.stop_timer(name_);
        func_(timer_.get_total_all());
    }
};

#define SCOPED_TIMER(name, data)                                                                                       \
    arcticdb::ScopedTimerTotal timer1{#name, [&data](auto totals) {                                                    \
                                          std::copy(std::begin(totals), std::end(totals), std::back_inserter(data));   \
                                      }};
#define SUBSCOPED_TIMER(name, data)                                                                                    \
    arcticdb::ScopedTimerTotal timer2{#name, [&data](auto totals) {                                                    \
                                          std::copy(std::begin(totals), std::end(totals), std::back_inserter(data));   \
                                      }};

} // namespace arcticdb
