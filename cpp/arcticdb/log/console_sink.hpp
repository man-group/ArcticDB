/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <spdlog/sinks/base_sink.h>
#include <spdlog/sinks/sink.h>

#include <cstdio>
#include <memory>
#include <mutex>

namespace arcticdb::log {

/// Console sink that writes through the C stdio FILE* on every call. spdlog's stdout/stderr sinks cache the Win32
/// HANDLE of fd 1/2 at construction and WriteFile to it; once something dup2()s over that fd (pytest capture) the
/// cached handle is closed and its value is recycled by the next CreateFile (e.g. LMDB's data.mdb), so log lines end
/// up written into that file. fwrite() resolves the handle from the fd on each write, so it follows redirections.
template<typename Mutex>
class ConsoleSink final : public spdlog::sinks::base_sink<Mutex> {
  public:
    explicit ConsoleSink(FILE* file) : file_(file) {}

  protected:
    void sink_it_(const spdlog::details::log_msg& msg) override {
        spdlog::memory_buf_t formatted;
        this->formatter_->format(msg, formatted);
        std::fwrite(formatted.data(), 1, formatted.size(), file_);
        std::fflush(file_);
    }

    void flush_() override { std::fflush(file_); }

  private:
    FILE* file_;
};

using ConsoleSinkMt = ConsoleSink<std::mutex>;

/// Console sink for stdout/stderr. Colour output is only honoured off Windows, where spdlog's ANSI colour sink also
/// writes via fwrite; the Windows colour sink has the same cached-HANDLE problem as the plain one.
std::shared_ptr<spdlog::sinks::sink> make_console_sink(bool std_err, bool color);

} // namespace arcticdb::log
