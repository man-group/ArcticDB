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

void check_embedded_pandas_agrees(const ExperimentalArrow& arrow, const StreamDescriptor& desc) {
    const auto* common = embedded_pandas_common(arrow);
    if (common == nullptr) {
        return;
    }
    if (arrow.one_dimensional()) {
        // Arrow cannot tell a None name from an empty one - polars stores both as "" - so neither does this
        const auto name = series_name(*common).value_or("");
        internal::check<ErrorCode::E_ASSERTION_FAILURE>(
                name == arrow.polars_series_name(),
                "Series name disagrees between Arrow ('{}') and embedded pandas ('{}') metadata",
                arrow.polars_series_name(),
                name
        );
    }
    // By position, as only the descriptor names the levels of a multi-index beyond the first
    const auto check_timezone = [&](size_t level, const std::string& pandas_timezone) {
        internal::check<ErrorCode::E_ASSERTION_FAILURE>(
                level < desc.field_count(),
                "Index level {} is beyond the descriptor's {} fields",
                level,
                desc.field_count()
        );
        const auto column_name = std::string{desc.field(level).name()};
        const auto it = arrow.columns().find(column_name);
        const auto arrow_timezone =
                it != arrow.columns().end() && it->second.has_timezone() ? it->second.timezone() : std::string{};
        internal::check<ErrorCode::E_ASSERTION_FAILURE>(
                arrow_timezone == pandas_timezone,
                "Timezone for '{}' disagrees between Arrow ('{}') and embedded pandas ('{}') metadata",
                column_name,
                arrow_timezone,
                pandas_timezone
        );
    };
    if (common->has_multi_index()) {
        check_timezone(0, common->multi_index().tz());
        for (const auto& [level, timezone] : common->multi_index().timezone()) {
            check_timezone(level, timezone);
        }
    } else if (common->index().is_physically_stored()) {
        check_timezone(0, common->index().tz());
    }
}

bool is_pandas_convertible_to_arrow(const NormalizationMetadata& norm_meta) {
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

std::optional<NormalizationMetadata> embedded_pandas(const ExperimentalArrow& arrow) {
    NormalizationMetadata res;
    switch (arrow.pandas_input_type_case()) {
    case ExperimentalArrow::kDf:
        res.mutable_df()->CopyFrom(arrow.df());
        return res;
    case ExperimentalArrow::kSeries:
        res.mutable_series()->CopyFrom(arrow.series());
        return res;
    case ExperimentalArrow::kTs:
        res.mutable_ts()->CopyFrom(arrow.ts());
        return res;
    case ExperimentalArrow::PANDAS_INPUT_TYPE_NOT_SET:
        return std::nullopt;
    }
    return std::nullopt;
}

NormalizationMetadata derive_pandas_from_arrow(
        const ExperimentalArrow& arrow, const StreamDescriptor& desc, const NormalizationMetadata& shape
) {
    const auto* shape_common = pandas_common(shape);
    internal::check<ErrorCode::E_ASSERTION_FAILURE>(shape_common != nullptr, "Pandas shape without a common");
    NormalizationMetadata res;
    switch (shape.input_type_case()) {
    case NormalizationMetadata::kDf:
        res.mutable_df();
        break;
    case NormalizationMetadata::kSeries:
        res.mutable_series();
        break;
    case NormalizationMetadata::kTs:
        res.mutable_ts();
        break;
    default:
        internal::raise<ErrorCode::E_ASSERTION_FAILURE>(
                "Cannot derive pandas metadata shaped like type {}", static_cast<int>(shape.input_type_case())
        );
    }
    auto& common = *mutable_pandas_common(res);
    common.set_mark(true);
    common.mutable_columns()->CopyFrom(shape_common->columns());

    const auto name_of = [&](size_t pos) {
        return pos < desc.field_count() ? std::string{desc.field(pos).name()} : std::string{};
    };
    const auto timezone_of = [&](size_t pos) {
        const auto it = arrow.columns().find(name_of(pos));
        return it != arrow.columns().end() && it->second.has_timezone() ? it->second.timezone() : std::string{};
    };
    size_t required_fields = 0;
    if (shape_common->has_multi_index()) {
        const auto field_count = shape_common->multi_index().field_count();
        auto& multi_index = *common.mutable_multi_index();
        multi_index.set_field_count(field_count);
        multi_index.set_name(name_of(0));
        multi_index.set_tz(timezone_of(0));
        for (uint32_t level = 1; level <= field_count; ++level) {
            (*multi_index.mutable_timezone())[level] = timezone_of(level);
        }
        required_fields = field_count + 1;
    } else if (shape_common->index().is_physically_stored()) {
        auto& index = *common.mutable_index();
        index.set_is_physically_stored(true);
        index.set_name(name_of(0));
        index.set_tz(timezone_of(0));
        required_fields = 1;
    } else {
        // A RangeIndex is not stored, so Arrow has nothing to say about it
        common.mutable_index()->CopyFrom(shape_common->index());
    }
    if (shape.has_series()) {
        const auto& name = arrow.one_dimensional() ? arrow.polars_series_name() : name_of(required_fields);
        if (!name.empty()) {
            common.set_name(name);
            common.set_has_name(true);
        }
        ++required_fields;
    }
    // Arrow data stores the columns of a frame with synthetic columns as "0".."n-1"; any other names mean it has none
    bool synthetic = shape.has_df()       ? shape.df().has_synthetic_columns()
                     : shape.has_series() ? shape.series().has_synthetic_columns()
                                          : shape.ts().has_synthetic_columns();
    for (size_t pos = required_fields; synthetic && pos < desc.field_count(); ++pos) {
        synthetic = name_of(pos) == std::to_string(pos - required_fields);
    }
    if (res.has_df()) {
        res.mutable_df()->set_has_synthetic_columns(synthetic);
    } else if (res.has_series()) {
        res.mutable_series()->set_has_synthetic_columns(synthetic);
    } else {
        res.mutable_ts()->set_has_synthetic_columns(synthetic);
    }
    return res;
}

NormalizationMetadata arrow_norm_from_pandas(const NormalizationMetadata& norm_meta, const StreamDescriptor& desc) {
    NormalizationMetadata res;
    // A custom normalizer's metadata sits beside the input type rather than inside it, and outlives the conversion
    if (norm_meta.has_custom()) {
        res.mutable_custom()->CopyFrom(norm_meta.custom());
    }
    auto& arrow = *res.mutable_experimental_arrow();
    const auto* common = pandas_common(norm_meta);
    internal::check<ErrorCode::E_ASSERTION_FAILURE>(common != nullptr, "Convertible pandas metadata without a common");

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

    embed_pandas(norm_meta, arrow);
    check_embedded_pandas_agrees(arrow, desc);
    return res;
}

} // namespace arcticdb
