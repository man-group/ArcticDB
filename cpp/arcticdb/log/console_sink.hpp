/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <spdlog/details/console_globals.h>
#include <spdlog/pattern_formatter.h>
#include <spdlog/sinks/sink.h>

#include <cstdio>
#include <memory>
#include <mutex>
#include <string>

namespace arcticdb::log {

/// Writes to the stdio stream so that output follows fd redirections (dup2) done after the sink was created.
/// On Windows, if a process-shared CRT (ucrtbase.dll, the one Python uses) is loaded, the fd's current handle is
/// looked up there on every write; arcticdb_ext links the CRT statically so its own fd table does not see redirections
/// made through Python's CRT. Otherwise, or on other platforms, this is fwrite + fflush.
void write_to_console(FILE* file, const char* data, size_t size);

/// spdlog's own stdout_sink_base with one thing changed: where it caches the Win32 HANDLE of fd 1/2 at construction
/// and WriteFile()s to it for the rest of the process, this resolves the destination on every write. Once something
/// dup2()s over that fd (pytest's capture does) the cached handle has been closed and its value recycled by the next
/// CreateFile - LMDB's data.mdb in these tests - so the cached-handle version writes log lines into whatever file
/// happens to hold that handle now. The same applies to a static CRT's own fd table when another CRT in the process
/// does the dup2.
///
/// Everything else is deliberately spdlog's: the same ConsoleMutex shared by every console sink in the process, so
/// stdout and stderr sinks still serialise against each other, and the same lock scope around the formatter.
template<typename ConsoleMutex>
class ConsoleSink final : public spdlog::sinks::sink {
  public:
    using mutex_t = typename ConsoleMutex::mutex_t;

    explicit ConsoleSink(FILE* file) :
        mutex_(ConsoleMutex::mutex()),
        file_(file),
        formatter_(std::make_unique<spdlog::pattern_formatter>()) {}

    ConsoleSink(const ConsoleSink&) = delete;
    ConsoleSink& operator=(const ConsoleSink&) = delete;

    void log(const spdlog::details::log_msg& msg) override {
        std::lock_guard<mutex_t> lock(mutex_);
        spdlog::memory_buf_t formatted;
        formatter_->format(msg, formatted);
        write_to_console(file_, formatted.data(), formatted.size());
    }

    void flush() override {
        std::lock_guard<mutex_t> lock(mutex_);
        std::fflush(file_);
    }

    void set_pattern(const std::string& pattern) override {
        std::lock_guard<mutex_t> lock(mutex_);
        formatter_ = std::make_unique<spdlog::pattern_formatter>(pattern);
    }

    void set_formatter(std::unique_ptr<spdlog::formatter> sink_formatter) override {
        std::lock_guard<mutex_t> lock(mutex_);
        formatter_ = std::move(sink_formatter);
    }

  private:
    mutex_t& mutex_;
    FILE* file_;
    std::unique_ptr<spdlog::formatter> formatter_;
};

using ConsoleSinkMt = ConsoleSink<spdlog::details::console_mutex>;

/// Console sink for stdout/stderr: ConsoleSink on Windows, spdlog's own stdout/stderr sinks everywhere else, where
/// they already write through the FILE* and so already follow a dup2 of fd 1/2.
std::shared_ptr<spdlog::sinks::sink> make_console_sink(bool std_err, bool color);

} // namespace arcticdb::log
