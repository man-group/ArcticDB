/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/python/normalization_checks.hpp>
#include <arcticdb/log/log.hpp>
#include <arcticdb/util/preconditions.hpp>
#include <arcticdb/util/pb_util.hpp>
#include <arcticdb/pipeline/input_tensor_frame.hpp>
#include <arcticdb/pipeline/index_segment_reader.hpp>
#undef GetMessage  // defined as GetMessageA on Windows

namespace arcticdb {

template<typename InnerFunction, typename FieldType=google::protobuf::FieldDescriptor *>
    auto get_pandas_common_via_reflection(
        proto::descriptors::NormalizationMetadata norm_meta,
        InnerFunction&& inner_function
    ) -> decltype(inner_function(norm_meta, std::declval<FieldType>(), std::declval<FieldType>())) {
    try {
        if (norm_meta.input_type_case() != proto::descriptors::NormalizationMetadata::INPUT_TYPE_NOT_SET) {
            if (auto one_of = proto::descriptors::NormalizationMetadata::descriptor()->field(norm_meta.input_type_case()); one_of) {
                log::storage().info("Inefficient NormalizationMetadata.input_type.{} access via reflection",
                                    one_of->name());
                if (auto msg_type = one_of->message_type(); msg_type) {
                    if (auto common_field = msg_type->FindFieldByName("common"); common_field) {
                        normalization::check<ErrorCode::E_UNIMPLEMENTED_INPUT_TYPE>(
                            common_field->message_type() ==
                                proto::descriptors::NormalizationMetadata::Pandas::descriptor(),
                                    "{}.common must be Pandas", one_of->name());
                        return inner_function(norm_meta, one_of, common_field);
                    }
                }
            }
        }
    } catch (const std::exception &e) {
        log::storage().info("get_common_pandas() reflection exception: {}", e.what());
    }
    log::storage().warn("New NormalizationMetadata.input_type access failure. Cannot check.");
    return std::nullopt;
}

std::optional<std::decay_t<std::reference_wrapper<const arcticdb::proto::descriptors::NormalizationMetadata_Pandas>>>
get_common_pandas(const proto::descriptors::NormalizationMetadata& norm_meta) {
    using Pandas = const arcticdb::proto::descriptors::NormalizationMetadata_Pandas;
    switch (norm_meta.input_type_case()) {
    case proto::descriptors::NormalizationMetadata::kDf:
        return std::make_optional(std::reference_wrapper<Pandas>(norm_meta.df().common()));
    case proto::descriptors::NormalizationMetadata::kSeries:
        return std::make_optional(std::reference_wrapper<Pandas>(norm_meta.series().common()));
    case proto::descriptors::NormalizationMetadata::kTs:
        return std::make_optional(std::reference_wrapper<Pandas>(norm_meta.ts().common()));
    case proto::descriptors::NormalizationMetadata::kMsgPackFrame:
    case proto::descriptors::NormalizationMetadata::kNp: return std::nullopt;
    default:
        return get_pandas_common_via_reflection(norm_meta, [](auto& norm_meta, auto one_of, auto common_field) {
            auto& one_of_msg = norm_meta.GetReflection()->GetMessage(norm_meta, one_of);
            auto& common_msg = one_of_msg.GetReflection()->GetMessage(one_of_msg, common_field);
            return std::make_optional(std::reference_wrapper<Pandas>(
                *reinterpret_cast<Pandas*>(const_cast<::google::protobuf::Message*>(&common_msg))
            ));
        });
    }
}

std::optional<std::decay_t<std::reference_wrapper<arcticdb::proto::descriptors::NormalizationMetadata_Pandas>>>
get_common_pandas(proto::descriptors::NormalizationMetadata& norm_meta) {
    using Pandas = arcticdb::proto::descriptors::NormalizationMetadata_Pandas;
    switch (norm_meta.input_type_case()) {
    case proto::descriptors::NormalizationMetadata::kDf:
        return std::make_optional(std::reference_wrapper<Pandas>(*norm_meta.mutable_df()->mutable_common()));
    case proto::descriptors::NormalizationMetadata::kSeries:
        return std::make_optional(std::reference_wrapper<Pandas>(*norm_meta.mutable_series()->mutable_common()));
    case proto::descriptors::NormalizationMetadata::kTs:
        return std::make_optional(std::reference_wrapper<Pandas>(*norm_meta.mutable_ts()->mutable_common()));
    case proto::descriptors::NormalizationMetadata::kMsgPackFrame:
    case proto::descriptors::NormalizationMetadata::kNp: return std::nullopt;
    default:
        return get_pandas_common_via_reflection(norm_meta, [](auto& norm_meta, auto one_of, auto common_field) {
            auto& one_of_msg = norm_meta.GetReflection()->GetMessage(norm_meta, one_of);
            auto& common_msg = one_of_msg.GetReflection()->GetMessage(one_of_msg, common_field);
            return std::make_optional(std::reference_wrapper<Pandas>(
                *reinterpret_cast<Pandas*>(const_cast<::google::protobuf::Message*>(&common_msg))
            ));
        });
    }
}

/// In case both indexes are row-ranged sanity checks will be performed:
/// * Both indexes must have the same step
/// * The new index must start at the point where the old one ends
/// If the checks above pass update the new normalization index so that it spans the whole index (old + new)
/// @throws In case the row-ranged indexes are incompatible
void update_rowcount_normalization_data(
    const proto::descriptors::NormalizationMetadata& old_norm,
    proto::descriptors::NormalizationMetadata& new_norm,
    size_t old_length
) {
    const auto old_pandas = get_common_pandas(old_norm);
    const auto new_pandas = get_common_pandas(new_norm);
    const auto* old_index = old_pandas->get().has_index() ? &old_pandas->get().index() : nullptr;
    const auto* new_index = new_pandas->get().has_index() ? &new_pandas->get().index() : nullptr;
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

            new_pandas->get().mutable_index()->set_start(old_index->start());
        }
    }
}

bool check_pandas_like(
    const proto::descriptors::NormalizationMetadata& old_norm,
    proto::descriptors::NormalizationMetadata& new_norm
) {
    auto old_pandas = get_common_pandas(old_norm);
    auto new_pandas = get_common_pandas(new_norm);
    if (old_pandas || new_pandas) {
        normalization::check<ErrorCode::E_UPDATE_NOT_SUPPORTED>(old_pandas && new_pandas,
                        "Currently only supports modifying existing Pandas data with Pandas.\nexisting={}\nargument={}",
                        util::newlines_to_spaces(old_norm),
                        util::newlines_to_spaces(new_norm));

        const auto *old_index = old_pandas->get().has_index() ? &old_pandas->get().index() : nullptr;
        const auto *new_index = new_pandas->get().has_index() ? &new_pandas->get().index() : nullptr;
        normalization::check<ErrorCode::E_INCOMPATIBLE_INDEX>(static_cast<bool>(old_index) == static_cast<bool>(new_index),
                        "The argument has an index type incompatible with the existing version:\nexisting={}\nargument={}",
                        util::newlines_to_spaces(old_norm),
                        util::newlines_to_spaces(new_norm));
        // FUTURE: check PandasMultiIndex and many other descriptor types. Might be more efficiently implemented using
        // some structural comparison lib or do it via Python
        return true;
    }
    return false;
}

template<class NormalizationMetadata>
bool check_ndarray_append(const NormalizationMetadata &old_norm, NormalizationMetadata &new_norm) {
    if (old_norm.has_np() || new_norm.has_np()) {
        normalization::check<ErrorCode::E_INCOMPATIBLE_OBJECTS>(old_norm.has_np() && new_norm.has_np(),
                        "Currently, can only append numpy.ndarray to each other.");

        const auto &old_shape = old_norm.np().shape();
        auto *new_shape = new_norm.mutable_np()->mutable_shape();
        normalization::check<ErrorCode::E_WRONG_SHAPE>(!new_shape->empty(), "Append input has invalid normalization metadata (empty shape)");
        normalization::check<ErrorCode::E_WRONG_SHAPE>(std::equal(old_shape.begin() + 1, old_shape.end(), new_shape->begin() + 1, new_shape->end()),
                        "The appending NDArray must have the same shape as the existing (excl. the first dimension)");
        (*new_shape)[0] += old_shape[0];
        return true;
    }
    return false;
}

void fix_normalization_or_throw(
    bool is_append,
    const pipelines::index::IndexSegmentReader &existing_isr,
    const pipelines::InputTensorFrame &new_frame) {
    auto &old_norm = existing_isr.tsd().proto().normalization();
    auto &new_norm = new_frame.norm_meta;
    normalization::check<ErrorCode::E_INCOMPATIBLE_OBJECTS>(
        old_norm.input_type_case() == new_frame.norm_meta.input_type_case(),
        "{} can be performed only on objects of the same type. Existing type is {} new type is {}.",
        is_append ? "Append" : "Update", old_norm.input_type_case(), new_frame.norm_meta.input_type_case()
    );
    if (check_pandas_like(old_norm, new_norm)) {
        const IndexDescriptor::Type old_index_type = existing_isr.tsd().index().type();
        const IndexDescriptor::Type new_index_type = new_frame.desc.index().type();
        if (old_index_type == new_index_type && old_index_type == IndexDescriptor::Type::ROWCOUNT) {
            update_rowcount_normalization_data(old_norm, new_norm, existing_isr.tsd().total_rows());
        }
        return;
    }
    if (is_append) {
        if (check_ndarray_append(old_norm, new_norm))
            return;
    } else {
        // ndarray normalizes to a ROWCOUNT frame and we don't support update on those
        normalization::check<ErrorCode::E_UPDATE_NOT_SUPPORTED>(!old_norm.has_np() && !new_norm.has_np(), "current normalization scheme doesn't allow update of ndarray");
    }
}

} //namespace arcticdb
