/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/entity/field_collection.hpp>
#include <arcticdb/entity/stream_descriptor.hpp>

namespace arcticdb {

struct FrameDescriptorImpl : public FrameDescriptor {
    FrameDescriptorImpl() = default;

    ARCTICDB_MOVE_COPY_DEFAULT(FrameDescriptorImpl)

    [[nodiscard]] FrameDescriptorImpl clone() const { return *this; }
};

struct TimeseriesDescriptor {
    using Proto = arcticdb::proto::descriptors::FrameMetadata;

    std::shared_ptr<FrameDescriptorImpl> frame_data_ = std::make_shared<FrameDescriptorImpl>();
    std::shared_ptr<SegmentDescriptorImpl> segment_desc_ = std::make_shared<SegmentDescriptorImpl>();
    std::shared_ptr<Proto> proto_ = std::make_shared<Proto>();
    std::shared_ptr<FieldCollection> fields_ = std::make_shared<FieldCollection>();
    StreamId stream_id_;

    TimeseriesDescriptor() = default;

    TimeseriesDescriptor(
            std::shared_ptr<FrameDescriptorImpl> frame_desc, std::shared_ptr<SegmentDescriptorImpl> segment_desc,
            std::shared_ptr<Proto> proto, std::shared_ptr<FieldCollection> fields, StreamId stream_id
    ) :
        frame_data_(std::move(frame_desc)),
        segment_desc_(segment_desc),
        proto_(std::move(proto)),
        fields_(std::move(fields)),
        stream_id_(stream_id) {}

    [[nodiscard]] const FrameDescriptorImpl& frame_descriptor() const { return *frame_data_; }

    [[nodiscard]] IndexDescriptorImpl index() const { return segment_desc_->index_; }

    void set_stream_descriptor(const StreamDescriptor& desc) {
        segment_desc_ = desc.data_ptr();
        fields_ = desc.fields_ptr();
        stream_id_ = desc.stream_id_;
    }

    void set_total_rows(uint64_t rows) { frame_data_->total_rows_ = rows; }

    [[nodiscard]] uint64_t total_rows() const { return frame_data_->total_rows_; }

    [[nodiscard]] SortedValue sorted() const { return segment_desc_->sorted_; }

    void set_sorted(SortedValue sorted) { segment_desc_->sorted_ = sorted; }

    const arcticdb::proto::descriptors::UserDefinedMetadata& user_metadata() const { return proto_->user_meta(); }

    const arcticdb::proto::descriptors::NormalizationMetadata& normalization() const { return proto_->normalization(); }

    void set_user_metadata(arcticdb::proto::descriptors::UserDefinedMetadata&& user_meta) {
        *proto_->mutable_user_meta() = std::move(user_meta);
    }

    void set_normalization_metadata(arcticdb::proto::descriptors::NormalizationMetadata&& norm_meta) {
        *proto_->mutable_normalization() = std::move(norm_meta);
    }

    void set_multi_key_metadata(arcticdb::proto::descriptors::UserDefinedMetadata&& multi_key_meta) {
        *proto_->mutable_multi_key_meta() = std::move(multi_key_meta);
    }

    [[nodiscard]] std::shared_ptr<FieldCollection> fields_ptr() const { return fields_; }

    [[nodiscard]] std::shared_ptr<Proto> proto_ptr() const { return proto_; }

    [[nodiscard]] bool proto_is_null() const { return !proto_; }

    [[nodiscard]] const FieldCollection& fields() const { return *fields_; }

    [[nodiscard]] FieldCollection& mutable_fields() { return *fields_; }

    [[nodiscard]] Proto& mutable_proto() { return *proto_; }

    [[nodiscard]] const Proto& proto() const { return *proto_; }

    [[nodiscard]] TimeseriesDescriptor clone() const {
        auto proto = std::make_shared<Proto>();
        proto->CopyFrom(*proto_);
        auto frame_desc = std::make_shared<FrameDescriptorImpl>(frame_data_->clone());
        auto segment_desc = std::make_shared<SegmentDescriptorImpl>(segment_desc_->clone());
        return {std::move(frame_desc),
                std::move(segment_desc),
                std::move(proto),
                std::make_shared<FieldCollection>(fields_->clone()),
                stream_id_};
    }

    [[nodiscard]] bool column_groups() const { return frame_data_->column_groups_; }

    [[nodiscard]] StreamDescriptor as_stream_descriptor() const { return {segment_desc_, fields_, stream_id_}; }
};

} // namespace arcticdb

namespace fmt {
template<>
struct formatter<arcticdb::TimeseriesDescriptor> {
    template<typename ParseContext>
    constexpr auto parse(ParseContext& ctx) {
        return ctx.begin();
    }

    template<typename FormatContext>
    auto format(const arcticdb::TimeseriesDescriptor& tsd, FormatContext& ctx) const {
        if (!tsd.fields_ptr())
            return fmt::format_to(ctx.out(), "TimeseriesDescriptor<fields=empty, proto={}>", tsd.proto());

        return fmt::format_to(ctx.out(), "TimeseriesDescriptor<fields={}, proto={}>", tsd.fields(), tsd.proto());
    }
};

template<>
struct formatter<arcticdb::TimeseriesDescriptor::Proto> {
    template<typename ParseContext>
    constexpr auto parse(ParseContext& ctx) {
        return ctx.begin();
    }

    template<typename FormatContext>
    auto format(const arcticdb::TimeseriesDescriptor::Proto& tsd, FormatContext& ctx) const {
        return fmt::format_to(ctx.out(), "{}", tsd.ShortDebugString());
    }
};

} // namespace fmt
