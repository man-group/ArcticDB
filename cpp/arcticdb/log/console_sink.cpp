/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/log/console_sink.hpp>

#include <cstdio>

#ifdef _WIN32
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#ifndef NOMINMAX
#define NOMINMAX
#endif
#include <windows.h>
#include <io.h>
#include <stdlib.h>

#include <algorithm>
#include <cstdint>
#include <limits>
#include <optional>
#endif

namespace arcticdb::log {

#ifdef _WIN32
namespace {

using GetOsfHandleFn = intptr_t(__cdecl*)(int);
using SetInvalidParameterHandlerFn = _invalid_parameter_handler(__cdecl*)(_invalid_parameter_handler);

// The two entry points of a CRT's fd table the sink needs, bound to one CRT: the process-shared ucrtbase.dll when
// it is loaded, else this binary's own statically linked CRT.
struct CrtFdTable {
    GetOsfHandleFn get_osfhandle = nullptr;
    SetInvalidParameterHandlerFn set_thread_local_invalid_parameter_handler = nullptr;
};

CrtFdTable static_crt() { return CrtFdTable{&::_get_osfhandle, &::_set_thread_local_invalid_parameter_handler}; }

// The release and debug flavours of the universal CRT are separate DLLs with separate fd tables; a debug host
// (python_d.exe) loads only ucrtbased.dll. The module is pinned so the resolved pointers stay valid for the life of
// the process however many times it is FreeLibrary'd. Both entry points have to resolve: resolve_handle() needs the
// handler setter to keep a closed fd from terminating the process, so a CRT that exports only _get_osfhandle is not
// one to trust, and the binary's own is used instead.
std::optional<CrtFdTable> shared_crt() {
    for (const wchar_t* name : {L"ucrtbase.dll", L"ucrtbased.dll"}) {
        HMODULE module = nullptr;
        if (!::GetModuleHandleExW(GET_MODULE_HANDLE_EX_FLAG_PIN, name, &module) || module == nullptr)
            continue;
        CrtFdTable crt;
        crt.get_osfhandle = reinterpret_cast<GetOsfHandleFn>(::GetProcAddress(module, "_get_osfhandle"));
        crt.set_thread_local_invalid_parameter_handler = reinterpret_cast<SetInvalidParameterHandlerFn>(
                ::GetProcAddress(module, "_set_thread_local_invalid_parameter_handler")
        );
        if (crt.get_osfhandle != nullptr && crt.set_thread_local_invalid_parameter_handler != nullptr)
            return crt;
    }
    return std::nullopt;
}

// Resolved once: GetModuleHandleEx/GetProcAddress take the loader lock, which would serialise logging threads if
// done per line. In every binary this repository builds, ucrtbase.dll is an import-time dependency (through
// python3X.dll, which arcticdb_core_static links), so it is loaded before the first log line and cannot appear later.
const CrtFdTable& console_crt() {
    static const CrtFdTable crt = shared_crt().value_or(static_crt());
    return crt;
}

void ignore_invalid_parameter(const wchar_t*, const wchar_t*, const wchar_t*, unsigned int, uintptr_t) {}

// _get_osfhandle validates the fd and, when it is not open, reports an invalid parameter; with no handler installed
// the CRT terminates the process (__fastfail via _invoke_watson). fd 2 is closed by whoever owns the table - the
// host, in the shared CRT's case (os.close(2)) - and a log line must not take the host down for it, so a no-op
// handler is installed for the duration of the call, the way CPython's _Py_BEGIN_SUPPRESS_IPH does around its own
// fd calls. The handler is thread-local, so nothing outside this thread and this call sees it. It has to be the
// same CRT's setter: the static CRT's handler is invisible to ucrtbase's validation. On the debug CRTs the
// validation also raises a _CrtDbgReport assertion before consulting the handler; that follows the host's report
// mode and is not touched here, since _CrtSetReportMode is process-wide.
HANDLE resolve_handle(const CrtFdTable& crt, int fd) {
    _invalid_parameter_handler previous = nullptr;
    if (crt.set_thread_local_invalid_parameter_handler != nullptr)
        previous = crt.set_thread_local_invalid_parameter_handler(&ignore_invalid_parameter);
    const auto handle = reinterpret_cast<HANDLE>(crt.get_osfhandle(fd));
    if (crt.set_thread_local_invalid_parameter_handler != nullptr)
        crt.set_thread_local_invalid_parameter_handler(previous);
    return handle;
}

// -1 (INVALID_HANDLE_VALUE) is what _get_osfhandle returns for a closed or out-of-range fd; -2 (_NO_CONSOLE_FILENO)
// is what fd 0-2 hold in a process started without standard handles (pythonw.exe), and also happens to be the
// current-thread pseudo-handle, which WriteFile would accept.
bool is_writable_handle(HANDLE handle) {
    return handle != nullptr && handle != INVALID_HANDLE_VALUE &&
           handle != reinterpret_cast<HANDLE>(static_cast<intptr_t>(-2));
}

} // namespace
#endif

void write_to_console(FILE* file, const char* data, size_t size) {
#ifdef _WIN32
    // Resolved on every write: the fd's handle changes when something dup2s over it, which is the whole point.
    // No stdio on this path at all: fwrite through the static CRT's text-mode stream would go to the fd this
    // lookup exists to bypass, and would turn spdlog's "\r\n" into "\r\r\n" (spdlog issue #1675) on the way.
    const HANDLE handle = resolve_handle(console_crt(), ::_fileno(file));
    if (!is_writable_handle(handle))
        return;
    while (size > 0) {
        const auto chunk = static_cast<DWORD>(std::min<size_t>(size, std::numeric_limits<DWORD>::max()));
        DWORD written = 0;
        // A failed or short write drops the rest of the line. spdlog's sink threw here, which its logger reports
        // through fputs(stderr) - the static CRT's stderr, the one stream this path cannot trust - so there is no
        // safe place to report the failure from a console sink, and a log line is not worth an exception.
        if (!::WriteFile(handle, data, chunk, &written, nullptr) || written == 0)
            return;
        data += written;
        size -= written;
    }
#else
    std::fwrite(data, 1, size, file);
    std::fflush(file);
#endif
}

} // namespace arcticdb::log
