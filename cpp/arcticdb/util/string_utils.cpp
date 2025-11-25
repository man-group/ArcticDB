/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/util/string_utils.hpp>

#include <cctype>
#include <iomanip>
#include <sstream>

#include <boost/locale.hpp>

namespace arcticdb::util {

char from_hex(char c) { return std::isdigit(c) != 0 ? c - '0' : c - 'A' + 10; }

char decode_char(char a, char b) { return from_hex(a) << 4 | from_hex(b); }

std::string safe_encode(const std::string& value) {
    std::ostringstream escaped;
    escaped.fill('0');
    escaped << std::hex;

    for (auto i = value.begin(); i != value.end(); ++i) {
        std::string::value_type c = (*i);

        if (std::isalnum(c) || c == '-' || c == '_' || c == '.' || c == '~') {
            escaped << c;
            continue;
        }

        escaped << std::uppercase;
        escaped << escape_char << std::setw(0) << int((unsigned char)c);
        escaped << std::nouppercase;
    }

    return escaped.str();
}

std::string safe_decode(const std::string& value) {
    std::ostringstream unescaped;
    auto pos = 0u;
    const auto len = value.size();
    while (true) {
        auto curr = value.find(escape_char, pos);
        if (curr == std::string::npos) {
            unescaped << strv_from_pos(value, pos, len - pos);
            break;
        }

        unescaped << strv_from_pos(value, pos, curr - pos);

        auto is_escaped = len - curr > 2 && std::isxdigit(value[curr + 1]) != 0 && std::isxdigit(value[curr + 2]) != 0;
        if (is_escaped) {
            unescaped << decode_char(value[curr + 1], value[curr + 2]);
            pos = curr + 3;
        } else {
            unescaped << escape_char;
            pos = curr + 1;
        }
    }
    return unescaped.str();
}

std::string utf32_to_u8(std::string_view strv) {
    std::u32string_view strv32{reinterpret_cast<const char32_t*>(strv.data()), strv.size() / 4};
    // Strip trailing null characters
    strv32 = strv32.substr(0, strv32.find_first_of(char32_t(0)));
    return boost::locale::conv::utf_to_utf<char>(strv32.data(), strv32.data() + strv32.size());
}

} // namespace arcticdb::util