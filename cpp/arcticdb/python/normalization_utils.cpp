/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <google/protobuf/util/message_differencer.h>

#include <arcticdb/python/normalization_utils.hpp>
#include <arcticdb/log/log.hpp>
#include <arcticdb/util/preconditions.hpp>
#include <arcticdb/util/pb_util.hpp>
#include <arcticdb/pipeline/input_frame.hpp>
#include <arcticdb/pipeline/index_segment_reader.hpp>
#undef GetMessage // defined as GetMessageA on Windows

namespace arcticdb {
using namespace proto::descriptors;

template<typename InnerFunction, typename FieldType = google::protobuf::FieldDescriptor*>
auto get_pandas_common_via_reflection(
        proto::descriptors::NormalizationMetadata norm_meta, InnerFunction&& inner_function
) -> decltype(inner_function(norm_meta, std::declval<FieldType>(), std::declval<FieldType>())) {
    try {
        if (norm_meta.input_type_case() != proto::descriptors::NormalizationMetadata::INPUT_TYPE_NOT_SET) {
            if (auto one_of =
                        proto::descriptors::NormalizationMetadata::descriptor()->field(norm_meta.input_type_case());
                one_of) {
                log::storage().info(
                        "Inefficient NormalizationMetadata.input_type.{} access via reflection", one_of->name()
                );
                if (auto msg_type = one_of->message_type(); msg_type) {
                    if (auto common_field = msg_type->FindFieldByName("common"); common_field) {
                        normalization::check<ErrorCode::E_UNIMPLEMENTED_INPUT_TYPE>(
                                common_field->message_type() ==
                                        proto::descriptors::NormalizationMetadata::Pandas::descriptor(),
                                "{}.common must be Pandas",
                                one_of->name()
                        );
                        return inner_function(norm_meta, one_of, common_field);
                    }
                }
            }
        }
    } catch (const std::exception& e) {
        log::storage().info("pandas_common() reflection exception: {}", e.what());
    }
    log::storage().warn("New NormalizationMetadata.input_type access failure. Cannot check.");
    return nullptr;
}

const NormalizationMetadata_Pandas* pandas_common(const proto::descriptors::NormalizationMetadata& norm_meta) {
    using Pandas = const arcticdb::proto::descriptors::NormalizationMetadata_Pandas;
    switch (norm_meta.input_type_case()) {
    case proto::descriptors::NormalizationMetadata::kDf:
        return &norm_meta.df().common();
    case proto::descriptors::NormalizationMetadata::kSeries:
        return &norm_meta.series().common();
    case proto::descriptors::NormalizationMetadata::kTs:
        return &norm_meta.ts().common();
    case proto::descriptors::NormalizationMetadata::INPUT_TYPE_NOT_SET:
    case proto::descriptors::NormalizationMetadata::kMsgPackFrame:
    case proto::descriptors::NormalizationMetadata::kNp:
    case proto::descriptors::NormalizationMetadata::kExperimentalArrow:
        return nullptr;
    default:
        return get_pandas_common_via_reflection(norm_meta, [](auto& norm_meta, auto one_of, auto common_field) {
            auto& one_of_msg = norm_meta.GetReflection()->GetMessage(norm_meta, one_of);
            auto& common_msg = one_of_msg.GetReflection()->GetMessage(one_of_msg, common_field);
            return reinterpret_cast<Pandas*>(const_cast<::google::protobuf::Message*>(&common_msg));
        });
    }
}

NormalizationMetadata_Pandas* mutable_pandas_common(proto::descriptors::NormalizationMetadata& norm_meta) {
    using Pandas = arcticdb::proto::descriptors::NormalizationMetadata_Pandas;
    switch (norm_meta.input_type_case()) {
    case proto::descriptors::NormalizationMetadata::kDf:
        return norm_meta.mutable_df()->mutable_common();
    case proto::descriptors::NormalizationMetadata::kSeries:
        return norm_meta.mutable_series()->mutable_common();
    case proto::descriptors::NormalizationMetadata::kTs:
        return norm_meta.mutable_ts()->mutable_common();
    case proto::descriptors::NormalizationMetadata::INPUT_TYPE_NOT_SET:
    case proto::descriptors::NormalizationMetadata::kMsgPackFrame:
    case proto::descriptors::NormalizationMetadata::kNp:
    case proto::descriptors::NormalizationMetadata::kExperimentalArrow:
        return nullptr;
    default:
        return get_pandas_common_via_reflection(norm_meta, [](auto& norm_meta, auto one_of, auto common_field) {
            auto& one_of_msg = norm_meta.GetReflection()->GetMessage(norm_meta, one_of);
            auto& common_msg = one_of_msg.GetReflection()->GetMessage(one_of_msg, common_field);
            return reinterpret_cast<Pandas*>(const_cast<::google::protobuf::Message*>(&common_msg));
        });
    }
}

void update_rowrange_norm_for_append(
        const proto::descriptors::NormalizationMetadata& old_norm, proto::descriptors::NormalizationMetadata& new_norm,
        size_t old_length
) {
    const auto* old_pandas = pandas_common(old_norm);
    auto* new_pandas = mutable_pandas_common(new_norm);
    if (old_pandas == nullptr || new_pandas == nullptr) {
        return;
    }
    const auto* old_index = old_pandas->has_index() ? &old_pandas->index() : nullptr;
    const auto* new_index = new_pandas->has_index() ? &new_pandas->index() : nullptr;
    if (old_index) {
        constexpr auto error_suffix =
                " the existing version. Please convert both to use Int64Index if you need this to work.";

        util::check(new_index != nullptr, "New index is null in normalization checks");
        normalization::check<ErrorCode::E_INCOMPATIBLE_INDEX>(
                old_index->is_physically_stored() == new_index->is_physically_stored(),
                "The argument uses a {} index which is incompatible with {}",
                new_index->is_physically_stored() ? "non-range" : "range-style",
                error_suffix
        );

        if (!old_index->is_physically_stored()) {
            normalization::check<ErrorCode::E_INCOMPATIBLE_INDEX>(
                    old_index->step() == new_index->step(),
                    "The new argument has a different RangeIndex step from {}",
                    error_suffix
            );

            size_t new_start = new_index->start();
            auto stop = old_index->start() + old_length * old_index->step();
            normalization::check<ErrorCode::E_INCOMPATIBLE_INDEX>(
                    new_start == stop || (new_start == 0 && new_index->step() == 1),
                    "The appending data has a RangeIndex.start={} that is not contiguous with the "
                    "stop ({}) of {}",
                    new_start,
                    stop,
                    error_suffix
            );

            new_pandas->mutable_index()->set_start(old_index->start());
        }
    }
}

} // namespace arcticdb
