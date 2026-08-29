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
#endif

namespace arcticdb::log {

#ifdef _WIN32
namespace {

using GetOsfHandleFn = intptr_t(__cdecl*)(int);

// _get_osfhandle from the process-shared CRT, resolved once: GetModuleHandle/GetProcAddress take the loader lock,
// which would serialise logging threads if done per line. nullptr when no shared CRT is loaded at that point (a
// static-CRT executable such as the C++ test binary), where nothing else can dup2 our fds anyway and fwrite is
// correct.
GetOsfHandleFn shared_crt_get_osfhandle() {
    static const GetOsfHandleFn fn = [] {
        HMODULE ucrt = ::GetModuleHandleW(L"ucrtbase.dll");
        return ucrt == nullptr ? nullptr : reinterpret_cast<GetOsfHandleFn>(::GetProcAddress(ucrt, "_get_osfhandle"));
    }();
    return fn;
}

bool write_via_shared_crt(int fd, const char* data, size_t size) {
    auto get_osfhandle = shared_crt_get_osfhandle();
    if (get_osfhandle == nullptr)
        return false;
    // Looked up per write: the fd's handle changes when something dup2s over it, which is the whole point
    auto handle = reinterpret_cast<HANDLE>(get_osfhandle(fd));
    if (handle == INVALID_HANDLE_VALUE || handle == nullptr)
        return false;
    while (size > 0) {
        DWORD written = 0;
        if (!::WriteFile(handle, data, static_cast<DWORD>(size), &written, nullptr) || written == 0)
            return false;
        data += written;
        size -= written;
    }
    return true;
}

} // namespace
#endif

void write_to_console(FILE* file, const char* data, size_t size) {
#ifdef _WIN32
    std::fflush(file);
    if (write_via_shared_crt(::_fileno(file), data, size))
        return;
#endif
    std::fwrite(data, 1, size, file);
    std::fflush(file);
}

} // namespace arcticdb::log
