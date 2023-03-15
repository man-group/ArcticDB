/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <pcre.h>
#include <arcticdb/util/constructors.hpp>
#include <arcticdb/util/preconditions.hpp>

namespace arcticdb::util {

class PcreRegex {
protected:
    using ExtraPtr = ::pcre_extra*;
    using ErrorType = int;
    using HandleType = ::pcre*;
    using HelpType = const char*;
    using Offset = int;
    using TableType = const unsigned char*;
    using OptionsType = int;
    using ResultsType = int;
};

class RegexPattern : private PcreRegex {
    const std::string& text_;
    HandleType handle_ = nullptr;
    HelpType help_  = "";
    Offset offset_ = 0;
    TableType table_ = nullptr;
    ErrorType error_ = 0;
    ExtraPtr extra_ = nullptr;
    OptionsType options_ = 0;
    int capturing_groups_ = 0;

public:
    explicit RegexPattern(const std::string& pattern) :
    text_(pattern) {
        compile_regex();
    }

    [[nodiscard]] bool valid() const {
        return handle_ != nullptr;
    }

    [[nodiscard]] HandleType handle() const {
        return handle_;
    }

    [[nodiscard]] const std::string& text() const {
        return text_;
    }

    [[nodiscard]] size_t capturing_groups() const {
        return static_cast<size_t>(capturing_groups_);
    }

    ARCTICDB_NO_MOVE_OR_COPY(RegexPattern)
private:
    void compile_regex() {
        handle_ = ::pcre_compile2(text_.data(), options_, &error_, &help_, &offset_, table_);
        util::check(handle_ != nullptr, "Error {} compiling regex {}: {}", error_, text_, help_);
        auto result = get_capturing_groups();
        if(result != 0) {
            handle_ = nullptr;
            util::raise_rte("Failed to get capturing groups for regex {}: {}", text_, result);
        }
    }

    ResultsType get_capturing_groups() {
        return ::pcre_fullinfo(handle_, extra_, PCRE_INFO_CAPTURECOUNT, &capturing_groups_);
    }
};

class Regex : private PcreRegex {
    const RegexPattern& pattern_;
    ExtraPtr extra_ = nullptr;
    OptionsType options_ = 0;
    std::vector<int> results_;
public:
    ARCTICDB_NO_MOVE_OR_COPY(Regex);

    explicit Regex(const RegexPattern& pattern) :
    pattern_(pattern),
    results_((pattern_.capturing_groups() + 1) * 3, 0) {
    }

    bool match(const std::string& text) {
        ResultsType res = ::pcre_exec(pattern_.handle(), extra_, text.data(), static_cast<int>(text.size()), 0, options_, &results_[0], static_cast<int>(results_.size()));
        util::check(res >= 0 || res == PCRE_ERROR_NOMATCH, "Invalid result in regex compile with pattern {} and text {}: {}", pattern_.text(), text, res);
        return res > 0;
    }
};

}