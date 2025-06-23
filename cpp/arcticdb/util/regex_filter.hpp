/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <pcre.h>
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
    using ExtraPtr = ::pcre_extra*;
    using HandleType = ::pcre*;
    using StringType = std::string;
    using StringViewType = std::string_view;
    using StringCastingType = const char*;
    constexpr static auto& pcre_exec_ = ::pcre_exec;
    constexpr static auto& pcre_compile2_ = ::pcre_compile2;
    constexpr static auto& pcre_fullinfo_ = ::pcre_fullinfo;
};

class PcreRegexUTF32 {
protected:
    using ExtraPtr = ::pcre32_extra*;
    using HandleType = ::pcre32*;
    using StringType = std::u32string;
    using StringViewType = std::u32string_view;
    using StringCastingType = const unsigned int*;
    constexpr static auto& pcre_exec_ = ::pcre32_exec;
    constexpr static auto& pcre_compile2_ = ::pcre32_compile2;
    constexpr static auto& pcre_fullinfo_ = ::pcre32_fullinfo;
};

template<typename PcreRegexEncode>
class RegexPattern : protected PcreRegexEncode {
    typename PcreRegexEncode::StringType text_;
    typename PcreRegexEncode::HandleType handle_ = nullptr;
    typename PcreRegexEncode::ExtraPtr extra_ = nullptr;
    const char* help_  = "";
    int offset_ = 0;
    const unsigned char * table_ = nullptr;
    int error_ = 0;
    int options_ = 0;
    int capturing_groups_ = 0;

public:
    explicit RegexPattern(const typename PcreRegexEncode::StringType& pattern) :
    text_(pattern) {
        compile_regex();
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
        handle_ = this->pcre_compile2_(reinterpret_cast<typename PcreRegexEncode::StringCastingType>(text_.data()), options_, &error_, &help_, &offset_, table_);
        util::check(
            handle_ != nullptr, 
            "Error {} compiling regex {}: {}", 
            error_, 
            convert_to_utf8_if_needed(text_), 
            help_
        );
        auto result = get_capturing_groups();
        if(result != 0) {
            handle_ = nullptr;
            util::raise_rte("Failed to get capturing groups for regex {}: {}", convert_to_utf8_if_needed(text_), result);
        }
    }

    int get_capturing_groups() {
        return this->pcre_fullinfo_(handle_, extra_, PCRE_INFO_CAPTURECOUNT, &capturing_groups_);
    }
};

template<typename PcreRegexEncode>
class Regex : private PcreRegexEncode {
    const RegexPattern<PcreRegexEncode>& pattern_;
    typename PcreRegexEncode::ExtraPtr extra_ = nullptr;
    int options_ = 0;
    mutable std::vector<int> results_;
public:
    ARCTICDB_NO_MOVE_OR_COPY(Regex);

    explicit Regex(const RegexPattern<PcreRegexEncode>& pattern) :
        pattern_(pattern),
        results_((pattern_.capturing_groups() + 1) * 3, 0) {
    }

    bool match(typename PcreRegexEncode::StringViewType text) const {
        auto res = this->pcre_exec_(
            pattern_.handle(), 
            extra_, 
            reinterpret_cast<typename PcreRegexEncode::StringCastingType>(text.data()), 
            static_cast<int>(text.size()), 
            0, 
            options_, 
            &results_[0], 
            static_cast<int>(results_.size())
        );
        util::check(
            res >= 0 || res == PCRE_ERROR_NOMATCH, 
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
    RegexUTF8 regex_utf8_;
    RegexUTF32 regex_utf32_;
public:
    RegexGeneric(const std::string& pattern) :
        pattern_utf8_(pattern),
        pattern_utf32_(boost::locale::conv::utf_to_utf<char32_t>(pattern)),
        regex_utf8_(pattern_utf8_),
        regex_utf32_(pattern_utf32_){
    }
    bool match(std::string_view text) const {
        return regex_utf8_.match(text);
    }
    bool match(std::u32string_view text) const {
        return regex_utf32_.match(text);
    }
    std::string text() const {
        return pattern_utf8_.text();
    }
};

}
