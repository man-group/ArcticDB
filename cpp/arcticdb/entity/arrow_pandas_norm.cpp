/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/entity/arrow_pandas_norm.hpp>

#include <arcticdb/entity/types.hpp>
#include <arcticdb/util/preconditions.hpp>

namespace arcticdb {

using namespace proto::descriptors;
using entity::IndexDescriptorImpl;
using entity::StreamDescriptor;
using ExperimentalArrow = NormalizationMetadata_ExperimentalArrow;
using ArrowColumnMeta = NormalizationMetadata_ExperimentalArrow_ColumnMeta;
using Pandas = NormalizationMetadata_Pandas;

namespace {

// The name a Series was written with, if it had one. A client older than the has_name field wrote the name without
// setting it and denormalization still honours such a name, so a non-empty name counts as one either way.
std::optional<std::string> series_name(const Pandas& common) {
    if (common.has_name() || !common.name().empty()) {
        return common.name();
    }
    return std::nullopt;
}

// The timezone of every index level, by the position of the column holding it. Pandas records level 0's timezone on the
// index message itself and the rest in a map keyed by level, so iterating that map covers every level that has one.
void mirror_index_timezones(const Pandas& common, const StreamDescriptor& desc, ExperimentalArrow& arrow) {
    auto& columns = *arrow.mutable_columns();
    const auto set_timezone = [&](size_t level, const std::string& timezone) {
        if (!timezone.empty() && level < desc.field_count()) {
            columns[std::string{desc.field(level).name()}].set_timezone(timezone);
        }
    };
    if (common.has_multi_index()) {
        set_timezone(0, common.multi_index().tz());
        for (const auto& [level, timezone] : common.multi_index().timezone()) {
            set_timezone(level, timezone);
        }
    } else if (common.index().is_physically_stored()) {
        set_timezone(0, common.index().tz());
    }
}

} // namespace

bool is_embeddable_pandas(const NormalizationMetadata& norm_meta) {
    return norm_meta.has_df() || norm_meta.has_series() || norm_meta.has_ts();
}

void embed_pandas(const NormalizationMetadata& pandas, ExperimentalArrow& arrow) {
    switch (pandas.input_type_case()) {
    case NormalizationMetadata::kDf:
        arrow.mutable_df()->CopyFrom(pandas.df());
        return;
    case NormalizationMetadata::kSeries:
        arrow.mutable_series()->CopyFrom(pandas.series());
        return;
    case NormalizationMetadata::kTs:
        arrow.mutable_ts()->CopyFrom(pandas.ts());
        return;
    default:
        internal::raise<ErrorCode::E_ASSERTION_FAILURE>(
                "Cannot describe normalization metadata of type {} in Arrow terms",
                static_cast<int>(pandas.input_type_case())
        );
    }
}

NormalizationMetadata arrow_norm_from_pandas(const NormalizationMetadata& norm_meta, const StreamDescriptor& desc) {
    NormalizationMetadata res;
    // A custom normalizer's metadata sits beside the input type rather than inside it, and outlives the conversion
    if (norm_meta.has_custom()) {
        res.mutable_custom()->CopyFrom(norm_meta.custom());
    }
    auto& arrow = *res.mutable_experimental_arrow();
    embed_pandas(norm_meta, arrow);
    const auto* common = pandas_common(norm_meta);
    internal::check<ErrorCode::E_ASSERTION_FAILURE>(common != nullptr, "Embeddable pandas metadata without a common");

    // Arrow's index is the symbol's timeseries index. A physically stored pandas index that is not one - a string
    // index, or a multi-index whose first level is not a timeseries - is a leading column instead, which is why this
    // comes from the index descriptor rather than from is_physically_stored.
    arrow.set_has_index(desc.index().type() == IndexDescriptorImpl::Type::TIMESTAMP);

    if (norm_meta.has_series() && !common->has_multi_index() && !common->index().is_physically_stored()) {
        // A Series with no index column is one-dimensional, as a pa.[Chunked]Array or a pl.Series is
        arrow.set_one_dimensional(true);
        if (const auto name = series_name(*common)) {
            arrow.set_polars_series_name(*name);
        }
    }

    // Arrow records column metadata for every timestamp column, timezone-naive ones included, so a column with no
    // metadata is a column the schema does not have. Pandas data has to say the same, or when the two are combined a
    // naive pandas column would be taken for an absent one and inherit the other schema's timezone.
    auto& columns = *arrow.mutable_columns();
    for (const auto& field : desc.fields()) {
        if (is_time_type(field.type().data_type())) {
            columns[std::string{field.name()}] = ArrowColumnMeta{};
        }
    }
    mirror_index_timezones(*common, desc, arrow);
    return res;
}

} // namespace arcticdb
