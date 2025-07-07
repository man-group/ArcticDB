/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#define PCRE2_CODE_UNIT_WIDTH 0 //Disable default api so both UTF-8 and UTF-32 can be supported
#include <pcre2.h>
#include <boost/locale.hpp>
#include <arcticdb/util/constructors.hpp>
#include <arcticdb/util/preconditions.hpp>

namespace arcticdb::util {

template<typename T>
[[nodiscard]] auto convert_to_utf8_if_needed(const T& input) {
    if constexpr (std::is_same_v<T, std::string> || std::is_same_v<T, std::string_view>) {
        return input; // Already UTF-8
    } else if constexpr (std::is_same_v<T, std::u32string_view>) {
        return boost::locale::conv::utf_to_utf<char>(std::u32string(input));
    } else if constexpr (std::is_same_v<T, std::u32string>) {
        return boost::locale::conv::utf_to_utf<char>(input);
    }
}

class PcreRegexUTF8 {
protected:
    using HandleType = ::pcre2_code_8*;
    using MatchDataType = ::pcre2_match_data_8*;
    using StringType = std::string;
    using StringViewType = std::string_view;
    using StringCastingType = const unsigned char*;
    constexpr static auto& pcre_compile_ = ::pcre2_compile_8;
    constexpr static auto& pcre_match_ = ::pcre2_match_8;
    constexpr static auto& pcre_pattern_info_ = ::pcre2_pattern_info_8;
    constexpr static auto& pcre_match_data_create_from_pattern = ::pcre2_match_data_create_from_pattern_8;
    constexpr static auto& pcre_code_free_ = ::pcre2_code_free_8;
    constexpr static auto& pcre_match_data_free_ = ::pcre2_match_data_free_8;
};

class PcreRegexUTF32 {
protected:
    using HandleType = ::pcre2_code_32*;
    using MatchDataType = ::pcre2_match_data_32*;
    using StringType = std::u32string;
    using StringViewType = std::u32string_view;
    using StringCastingType = const unsigned int*;
    constexpr static auto& pcre_compile_ = ::pcre2_compile_32;
    constexpr static auto& pcre_match_ = ::pcre2_match_32;
    constexpr static auto& pcre_pattern_info_ = ::pcre2_pattern_info_32;
    constexpr static auto& pcre_match_data_create_from_pattern = ::pcre2_match_data_create_from_pattern_32;
    constexpr static auto& pcre_code_free_ = ::pcre2_code_free_32;
    constexpr static auto& pcre_match_data_free_ = ::pcre2_match_data_free_32;
};

template<typename PcreRegexEncode>
class RegexPattern : protected PcreRegexEncode {
    typename PcreRegexEncode::StringType text_;
    typename PcreRegexEncode::HandleType handle_ = nullptr;
    uint32_t options_ = 0;
    uint32_t capturing_groups_ = 0;
public:
    explicit RegexPattern(const typename PcreRegexEncode::StringType& pattern) :
    text_(pattern) {
        compile_regex();
    }

    ~RegexPattern() {
        if (handle_ != nullptr) {
            this->pcre_code_free_(handle_);
        }
    }

    [[nodiscard]] bool valid() const {
        return handle_ != nullptr;
    }

    [[nodiscard]] typename PcreRegexEncode::HandleType handle() const {
        return handle_;
    }

    [[nodiscard]] const typename PcreRegexEncode::StringType& text() const {
        return text_;
    }

    [[nodiscard]] size_t capturing_groups() const {
        return static_cast<size_t>(capturing_groups_);
    }

    ARCTICDB_NO_MOVE_OR_COPY(RegexPattern)
private:
    void compile_regex() {
        PCRE2_SIZE erroroffset;
        int error = 0;
        handle_ = this->pcre_compile_(
            reinterpret_cast<typename PcreRegexEncode::StringCastingType>(text_.data()),
            PCRE2_ZERO_TERMINATED,
            options_,
            &error,
            &erroroffset,
            nullptr
        );
        util::check(
            handle_ != nullptr, 
            "Error {} compiling regex {} at position {}", 
            error, 
            convert_to_utf8_if_needed(text_), 
            erroroffset
        );
        auto result = get_capturing_groups();
        if(result != 0) {
            handle_ = nullptr;
            util::raise_rte("Failed to get capturing groups for regex {}: {}", convert_to_utf8_if_needed(text_), result);
        }
    }

    int get_capturing_groups() {
        return this->pcre_pattern_info_(handle_, PCRE2_INFO_CAPTURECOUNT, &capturing_groups_);
    }
};

template<typename PcreRegexEncode>
class Regex : private PcreRegexEncode {
    const RegexPattern<PcreRegexEncode>& pattern_;
    typename PcreRegexEncode::MatchDataType match_data_ = nullptr;
    uint32_t options_ = 0;
    
public:
    ARCTICDB_NO_MOVE_OR_COPY(Regex);

    explicit Regex(const RegexPattern<PcreRegexEncode>& pattern) : pattern_(pattern) {
        match_data_ = this->pcre_match_data_create_from_pattern(pattern_.handle(), nullptr); // Size = 1 for match string + N for N capturing substrings
    }

    ~Regex() {
        if (match_data_ != nullptr) {
            this->pcre_match_data_free_(match_data_);
        }
    }

    bool match(typename PcreRegexEncode::StringViewType text) const { // Not thread safe
        auto res = this->pcre_match_(
            pattern_.handle(),
            reinterpret_cast<typename PcreRegexEncode::StringCastingType>(text.data()),
            static_cast<PCRE2_SIZE>(text.size()),
            0,
            options_,
            match_data_,
            nullptr
        );
        util::check(
            res >= 0 || res == PCRE2_ERROR_NOMATCH,
            "Invalid result in regex compile with pattern {} and text {}: {}", 
            convert_to_utf8_if_needed(pattern_.text()), 
            convert_to_utf8_if_needed(text), 
            res
        );
        return res > 0;
    }
};

using RegexUTF8 = Regex<PcreRegexUTF8>;
using RegexUTF32 = Regex<PcreRegexUTF32>;
using RegexPatternUTF8 = RegexPattern<PcreRegexUTF8>;
using RegexPatternUTF32 = RegexPattern<PcreRegexUTF32>;

class RegexGeneric {
private:
    RegexPatternUTF8 pattern_utf8_;
    RegexPatternUTF32 pattern_utf32_;
public:
    RegexGeneric(const std::string& pattern) :
        pattern_utf8_(pattern),
        pattern_utf32_(boost::locale::conv::utf_to_utf<char32_t>(pattern)) {
    }
    [[nodiscard]] RegexUTF8 get_utf8_match_object() const {
        return RegexUTF8(pattern_utf8_);
    }
    [[nodiscard]] RegexUTF32 get_utf32_match_object() const {
        return RegexUTF32(pattern_utf32_);
    }
    std::string text() const {
        return pattern_utf8_.text();
    }
};

}