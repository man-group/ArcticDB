/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/entity/output_format.hpp>
#include <arcticdb/util/optional_defaults.hpp>
#include <arcticdb/util/variant.hpp>
#include <arcticdb/arrow/arrow_output_options.hpp>

namespace arcticdb {

struct ReadOptionsData {
    std::optional<bool> force_strings_to_fixed_;
    std::optional<bool> force_strings_to_object_;
    std::optional<bool> incompletes_;
    std::optional<bool> dynamic_schema_;
    std::optional<bool> allow_sparse_;
    std::optional<bool> set_tz_;
    std::optional<bool> optimise_string_memory_;
    OutputFormat output_format_ = OutputFormat::PANDAS;
    ArrowOutputConfig arrow_output_config_ = ArrowOutputConfig{};
};

struct ReadOptions {
    std::shared_ptr<ReadOptionsData> data_ = std::make_shared<ReadOptionsData>();

    void set_force_strings_to_fixed(const std::optional<bool>& force_strings_to_fixed) {
        data_->force_strings_to_fixed_ = force_strings_to_fixed;
    }

    void set_force_strings_to_object(const std::optional<bool>& force_strings_to_object) {
        data_->force_strings_to_object_ = force_strings_to_object;
    }

    void set_incompletes(const std::optional<bool>& incompletes) { data_->incompletes_ = incompletes; }

    [[nodiscard]] bool get_incompletes() const { return opt_false(data_->incompletes_); }

    void set_dynamic_schema(const std::optional<bool>& dynamic_schema) { data_->dynamic_schema_ = dynamic_schema; }

    void set_allow_sparse(const std::optional<bool>& allow_sparse) { data_->allow_sparse_ = allow_sparse; }

    void set_set_tz(const std::optional<bool>& set_tz) { data_->set_tz_ = set_tz; }

    void set_optimise_string_memory(const std::optional<bool>& optimise_string_memory) {
        data_->optimise_string_memory_ = optimise_string_memory;
    }

    [[nodiscard]] const std::optional<bool>& dynamic_schema() const { return data_->dynamic_schema_; }

    [[nodiscard]] const std::optional<bool>& force_strings_to_object() const { return data_->force_strings_to_object_; }

    [[nodiscard]] const std::optional<bool>& force_strings_to_fixed() const { return data_->force_strings_to_fixed_; }

    [[nodiscard]] const std::optional<bool>& incompletes() const { return data_->incompletes_; }

    void set_output_format(OutputFormat output_format) { data_->output_format_ = output_format; }

    [[nodiscard]] OutputFormat output_format() const { return data_->output_format_; }

    void set_arrow_output_default_string_format(ArrowOutputStringFormat arrow_output_string_format) {
        util::check(
                output_format() == OutputFormat::ARROW,
                "Setting arrow string format should only happen for ARROW output format but got {}",
                static_cast<uint8_t>(output_format())
        );
        data_->arrow_output_config_.default_string_format_ = arrow_output_string_format;
    }

    void set_arrow_output_per_column_string_format(
            std::unordered_map<std::string, ArrowOutputStringFormat>& per_column_string_format
    ) {
        util::check(
                output_format() == OutputFormat::ARROW,
                "Setting arrow string format should only happen for ARROW output format but got {}",
                static_cast<uint8_t>(output_format())
        );
        data_->arrow_output_config_.per_column_string_format_ = per_column_string_format;
    }

    [[nodiscard]] const ArrowOutputConfig& arrow_output_config() const {
        util::check(
                output_format() == OutputFormat::ARROW,
                "Getting arrow string format should only happen for ARROW output format but got {}",
                static_cast<uint8_t>(output_format())
        );
        return data_->arrow_output_config_;
    }

    [[nodiscard]] ReadOptions clone() const { return ReadOptions(std::make_shared<ReadOptionsData>(*data_)); }
};

using ReadOptionsPerSymbol = std::variant<ReadOptions, std::vector<ReadOptions>>;

struct BatchReadOptionsData {
    ReadOptionsPerSymbol read_options_per_symbol_;
    std::optional<bool> batch_throw_on_error_;
    OutputFormat output_format_ = OutputFormat::PANDAS;
};

struct BatchReadOptions {
    std::shared_ptr<BatchReadOptionsData> data_ = std::make_shared<BatchReadOptionsData>();

    void set_read_options(const ReadOptions& read_options) { data_->read_options_per_symbol_ = read_options; }

    void set_read_options_per_symbol(const std::vector<ReadOptions>& read_options_per_symbol) {
        data_->read_options_per_symbol_ = read_options_per_symbol;
    }

    [[nodiscard]] ReadOptions at(size_t idx) const {
        return util::variant_match(
                data_->read_options_per_symbol_,
                [&](const std::vector<ReadOptions>& read_options) { return read_options.at(idx); },
                [&](ReadOptions read_options) { return read_options; }
        );
    }

    void set_batch_throw_on_error(bool batch_throw_on_error) { data_->batch_throw_on_error_ = batch_throw_on_error; }

    [[nodiscard]] const std::optional<bool>& batch_throw_on_error() const { return data_->batch_throw_on_error_; }

    void set_output_format(OutputFormat output_format) { data_->output_format_ = output_format; }

    [[nodiscard]] OutputFormat output_format() const { return data_->output_format_; }
};

} // namespace arcticdb