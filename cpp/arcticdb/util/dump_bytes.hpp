/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <sstream>
#include <iomanip>

// based on this: https://codereview.stackexchange.com/questions/165120/printing-hex-dumps-for-diagnostics
namespace arcticdb {
inline std::ostream& hex_dump(
        std::ostream& os, const void* buffer, std::size_t buf_size, bool show_printable_chars = true
) {
    if (buffer == nullptr)
        return os;

    auto old_format = os.flags();
    auto old_fill_char = os.fill();
    constexpr std::size_t max_line{8};

    // create a place to store text version of string
    char render_string[max_line + 1];
    char* rsptr{render_string};

    // convenience cast
    const unsigned char* buf{reinterpret_cast<const unsigned char*>(buffer)};
    for (std::size_t line_count = max_line; buf_size; --buf_size, ++buf) {
        os << std::setw(2) << std::setfill('0') << std::hex << static_cast<unsigned>(*buf) << ' ';
        *rsptr++ = std::isprint(*buf) ? *buf : '.';
        if (--line_count == 0) {
            *rsptr++ = '\0'; // terminate string
            if (show_printable_chars) {
                os << " | " << render_string;
            }
            os << '\n';
            rsptr = render_string;
            line_count = std::min(max_line, buf_size);
        }
    }

    // emit newline if we haven't already
    if (rsptr != render_string) {
        if (show_printable_chars) {
            for (*rsptr++ = '\0'; rsptr != &render_string[max_line + 1]; ++rsptr) {
                os << "   ";
            }
            os << " | " << render_string;
        }
        os << '\n';
    }

    os.fill(old_fill_char);
    os.flags(old_format);
    return os;
}

[[nodiscard]] inline std::string dump_bytes(const void* data, size_t size, size_t max_size = 20u) {
    const auto sz = std::min(max_size, size);
    std::ostringstream strm;
    hex_dump(strm, data, sz);
    return strm.str();
}

} // namespace arcticdb