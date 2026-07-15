/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <util/preconditions.hpp>
#include <arcticdb/entity/output_format.hpp>
#include <arcticdb/entity/types.hpp>
#include <arcticdb/pipeline/output_config.hpp>
#include <arcticdb/util/optional_defaults.hpp>
#include <arcticdb/util/variant.hpp>
#include <string_view>

namespace arcticdb::stream {
std::optional<std::string_view> demangled_name(std::string_view name);
}

namespace arcticdb {

struct ReadOptionsData {
    std::optional<bool> force_strings_to_fixed_;
    std::optional<bool> force_strings_to_object_;
    std::optional<bool> incompletes_;
    std::optional<bool> dynamic_schema_;
    std::optional<bool> allow_sparse_;
    std::optional<bool> set_tz_;
    std::optional<bool> optimise_string_memory_;
    OutputConfig output_config_ = PandasOutputConfig{};
};

class ReadOptions {
  public:
    ReadOptions() = default;

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

    void set_output_config(OutputConfig&& output_config) { data_->output_config_ = std::move(output_config); }

    [[nodiscard]] OutputFormat output_format_for_frame() const {
        return util::variant_match(
                data_->output_config_,
                [](const PandasOutputConfig&) { return OutputFormat::PANDAS; },
                [](const ArrowOutputConfig&) { return OutputFormat::ARROW; }
        );
    }

    [[nodiscard]] OutputFormat output_format_for_column_type(const entity::TypeDescriptor& type) const {
        return util::variant_match(
                data_->output_config_,
                [](const ArrowOutputConfig&) { return OutputFormat::ARROW; },
                [&](const PandasOutputConfig& pandas) {
                    if (entity::is_sequence_type(type.data_type()) &&
                        pandas.default_string_format_ == PandasStringFormat::ARROW_LARGE_STRING) {
                        return OutputFormat::ARROW;
                    }
                    return OutputFormat::PANDAS;
                }
        );
    }

    [[nodiscard]] ArrowOutputStringFormat arrow_string_format_for_column(std::string_view name) const {
        return util::variant_match(
                data_->output_config_,
                [&](const ArrowOutputConfig& arrow) -> ArrowOutputStringFormat {
                    auto try_find = [&](std::string_view n) -> std::optional<ArrowOutputStringFormat> {
                        auto it = arrow.per_column_string_format_.find(std::string{n});
                        return it != arrow.per_column_string_format_.end() ? std::optional{it->second} : std::nullopt;
                    };
                    if (auto fmt = try_find(name))
                        return *fmt;
                    // This could give the incorrect return type if:
                    //  - The user has explicitly named a column "__idx__blah"
                    //  - The user has NOT specified a string return type for column "__idx__blah"
                    //  - The user HAS specified a string return type for column "blah"
                    // This seems vanishingly unlikely, and to fix would require pushing knowledge of Pandas multiindex
                    // field counts and fake field positions deep into the decoding pipeline, which is architecturally
                    // undesirable if it can be avoided. See test_explicit_string_format__idx__prefix and issue
                    // 10679807500
                    if (auto demangled = stream::demangled_name(name)) {
                        if (auto fmt = try_find(*demangled))
                            return *fmt;
                    }
                    return arrow.default_string_format_;
                },
                [](const PandasOutputConfig& pandas) -> ArrowOutputStringFormat {
                    util::check(
                            pandas.default_string_format_ == PandasStringFormat::ARROW_LARGE_STRING,
                            "Arrow string format requested for a non-arrow column"
                    );
                    return ArrowOutputStringFormat::LARGE_STRING; 
                }
        );
    }

    [[nodiscard]] const ArrowOutputConfig& arrow_output_config() const {
        const auto& variant_config = data_->output_config_;
        util::check(
                std::holds_alternative<ArrowOutputConfig>(variant_config),
                "ReadOptions::arrow_output_config called for non-Arrow config."
        );
        return std::get<ArrowOutputConfig>(variant_config);
    }

    [[nodiscard]] ReadOptions clone() const { return ReadOptions(std::make_shared<ReadOptionsData>(*data_)); }

  private:
    explicit ReadOptions(std::shared_ptr<ReadOptionsData> data) : data_(std::move(data)) {}
    std::shared_ptr<ReadOptionsData> data_ = std::make_shared<ReadOptionsData>();
};

using ReadOptionsPerSymbol = std::variant<ReadOptions, std::vector<ReadOptions>>;

struct BatchReadOptionsData {
    ReadOptionsPerSymbol read_options_per_symbol_;
    bool batch_throw_on_error_;
    OutputFormat output_format_ = OutputFormat::PANDAS;

    BatchReadOptionsData(bool batch_throw_on_error) : batch_throw_on_error_(batch_throw_on_error) {}
};

class BatchReadOptions {
  public:
    explicit BatchReadOptions(bool batch_throw_on_error) {
        data_ = std::make_shared<BatchReadOptionsData>(batch_throw_on_error);
    }

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

    [[nodiscard]] bool batch_throw_on_error() const { return data_->batch_throw_on_error_; }

    void set_output_format(OutputFormat output_format) { data_->output_format_ = output_format; }

    [[nodiscard]] OutputFormat output_format() const { return data_->output_format_; }

  private:
    std::shared_ptr<BatchReadOptionsData> data_;
};

} // namespace arcticdb
