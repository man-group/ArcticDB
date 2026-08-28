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

/// Writes to the stdio stream so that output follows fd redirections (dup2) done after the sink was created.
/// On Windows, if a process-shared CRT (ucrtbase.dll, the one Python uses) is loaded, the fd's current handle is
/// looked up there on every write; arcticdb_ext links the CRT statically so its own fd table does not see redirections
/// made through Python's CRT. Otherwise, or on other platforms, this is fwrite + fflush.
void write_to_console(FILE* file, const char* data, size_t size);

/// Console sink that goes through write_to_console(). spdlog's stdout/stderr sinks cache the Win32 HANDLE of fd 1/2
/// at construction and WriteFile to it; once something dup2()s over that fd (pytest capture) the cached handle is
/// closed and its value is recycled by the next CreateFile (e.g. LMDB's data.mdb), so log lines end up written into
/// that file. The same happens to a static CRT's own fd table when another CRT in the process does the dup2.
template<typename Mutex>
class ConsoleSink final : public spdlog::sinks::base_sink<Mutex> {
  public:
    explicit ConsoleSink(FILE* file) : file_(file) {}

  protected:
    void sink_it_(const spdlog::details::log_msg& msg) override {
        spdlog::memory_buf_t formatted;
        this->formatter_->format(msg, formatted);
        write_to_console(file_, formatted.data(), formatted.size());
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
