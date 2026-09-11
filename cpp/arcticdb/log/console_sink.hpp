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

/// Writes one formatted line to wherever the stream's fd points at the time of the write, so that output follows fd
/// redirections (dup2) done after the sink was created.
///
/// Off Windows this is fwrite + fflush. On Windows it never touches stdio: the fd's current HANDLE is resolved on
/// every write with _get_osfhandle and written with WriteFile, and a line whose write fails is dropped. Which CRT's
/// _get_osfhandle: the process-shared one (ucrtbase.dll, Python's) whenever it is loaded, else this binary's own,
/// statically linked one. arcticdb_ext is built /MT, so it has its own fd table, and a dup2 made through Python's
/// CRT (pytest's capture) is invisible to it: its fd 2 keeps naming a HANDLE that Python's CRT has closed and
/// Windows has since reused for another file.
///
/// "ucrtbase loaded means its table is the one to trust" rather than a check for being hosted by Python, because
/// both hold: the two CRTs initialise fd 0-2 from the same GetStdHandle values, so their tables only disagree once
/// something dup2s through one of them, and nothing in this codebase dup2s through the static CRT - Python and
/// pytest go through ucrtbase, and so does the C++ regression test. A Python-hosted check would need a per-target
/// compile definition (console_sink.cpp is compiled once into arcticdb_core_static and linked into the extension
/// and the test binaries alike) or a call into the Python runtime from the logging layer, for no gain.
void write_to_console(FILE* file, const char* data, size_t size);

/// spdlog's own stdout_sink_base with one thing changed: where it caches the Win32 HANDLE of fd 1/2 at construction
/// and WriteFile()s to it for the rest of the process, this resolves the destination on every write. Once something
/// dup2()s over that fd (pytest's capture does) the cached handle has been closed and its value recycled by the next
/// CreateFile - LMDB's data.mdb in these tests - so the cached-handle version writes log lines into whatever file
/// happens to hold that handle now. The same applies to a static CRT's own fd table when another CRT in the process
/// does the dup2.
///
/// Everything else is deliberately spdlog's: the same ConsoleMutex shared by every console sink in the process, so
/// stdout and stderr sinks still serialise against each other, and the same lock scope around the formatter. The one
/// other difference is flush(): write_to_console() leaves nothing behind to flush, and on Windows an fflush() of the
/// FILE* would push any bytes pending in the static CRT's stream through the fd this sink exists to bypass.
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

    // Nothing to do: every line is written out in full by log(). Not fflush(file_) - see the class comment.
    void flush() override {}

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
