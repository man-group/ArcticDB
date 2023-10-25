/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/entity/protobufs.hpp>
#include <arcticdb/util/optional_defaults.hpp>

namespace arcticdb {
struct ReadOptions {
    std::optional<bool> force_strings_to_fixed_;
    std::optional<bool> force_strings_to_object_;
    std::optional<bool> incompletes_;
    std::optional<bool> dynamic_schema_;
    std::optional<bool> allow_sparse_;
    std::optional<bool> set_tz_;
    std::optional<bool> optimise_string_memory_;
    std::optional<bool> batch_throw_on_error_;
    std::optional<bool> read_previous_on_failure_;

    void set_force_strings_to_fixed(const std::optional<bool>& force_strings_to_fixed) {
        force_strings_to_fixed_ = force_strings_to_fixed;
    }

    void set_force_strings_to_object(const std::optional<bool>& force_strings_to_object) {
        force_strings_to_object_ = force_strings_to_object;
    }

    void set_incompletes(const std::optional<bool>& incompletes) {
        incompletes_ = incompletes;
    }

    bool get_incompletes() const {
        return opt_false(incompletes_);
    }

    void set_dynamic_schema(const std::optional<bool>& dynamic_schema) {
        dynamic_schema_ = dynamic_schema;
    }

    void set_allow_sparse(const std::optional<bool>& allow_sparse) {
        allow_sparse_ = allow_sparse;
    }

    void set_set_tz(const std::optional<bool>& set_tz) {
        set_tz_ = set_tz;
    }

    void set_optimise_string_memory(const std::optional<bool>& optimise_string_memory) {
        optimise_string_memory_ = optimise_string_memory;
    }

    void set_batch_throw_on_error(bool batch_throw_on_error) {
        batch_throw_on_error_ = batch_throw_on_error;
    }
};
} //namespace arcticdb