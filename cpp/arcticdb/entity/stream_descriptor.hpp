/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/entity/field_collection.hpp>
#include <arcticdb/storage/memory_layout.hpp>
#include <arcticdb/entity/field_collection_proto.hpp>
#include <arcticdb/entity/types_proto.hpp>
#include <arcticdb/pipeline/value.hpp>

#include <ankerl/unordered_dense.h>

namespace arcticdb::entity {

struct SegmentDescriptorImpl : public SegmentDescriptor {
    SegmentDescriptorImpl() = default;

    ARCTICDB_MOVE_COPY_DEFAULT(SegmentDescriptorImpl)

    [[nodiscard]] const IndexDescriptorImpl& index() const {
        return static_cast<const IndexDescriptorImpl&>(index_);
    }

    IndexDescriptorImpl& index() {
        return static_cast<IndexDescriptorImpl&>(index_);
    }

    [[nodiscard]] SegmentDescriptorImpl clone() const {
        return *this;
    }
};

inline bool operator==(const SegmentDescriptorImpl& l, const SegmentDescriptorImpl& r) {
    return l.sorted_ == r.sorted_ &&
        l.index() == r.index() &&
        l.compressed_bytes_ == r.compressed_bytes_ &&
        l.uncompressed_bytes_ == r.uncompressed_bytes_;
}

inline bool operator!=(const SegmentDescriptorImpl& l, const SegmentDescriptorImpl& r) {
    return !(l == r);
}

struct StreamDescriptor {
    std::shared_ptr<SegmentDescriptorImpl> segment_desc_ = std::make_shared<SegmentDescriptorImpl>();
    std::shared_ptr<FieldCollection> fields_ = std::make_shared<FieldCollection>();

    StreamId stream_id_;
    StreamDescriptor() = default;
    ~StreamDescriptor() = default;

    StreamDescriptor(std::shared_ptr<SegmentDescriptorImpl> data, std::shared_ptr<FieldCollection> fields) :
        segment_desc_(std::move(data)),
        fields_(std::move(fields)) {
    }

    StreamDescriptor(std::shared_ptr<SegmentDescriptorImpl> data, std::shared_ptr<FieldCollection> fields, StreamId stream_id) :
        segment_desc_(std::move(data)),
        fields_(std::move(fields)),
        stream_id_(std::move(stream_id)) {
    }

    [[nodiscard]] const SegmentDescriptorImpl& data() const  {
        return *segment_desc_;
    }

    void set_id(const StreamId& id) {
        stream_id_ = id;
    }

    [[nodiscard]] StreamId id() const {
        return stream_id_;
    }

    [[nodiscard]] uint64_t uncompressed_bytes() const {
        return segment_desc_->uncompressed_bytes_;
    }

    [[nodiscard]] uint64_t compressed_bytes() const {
        return segment_desc_->compressed_bytes_;
    }

    [[nodiscard]] SortedValue sorted() const {
        return segment_desc_->sorted_;
    }

    [[nodiscard]] IndexDescriptorImpl index() const {
        return static_cast<IndexDescriptorImpl&>(segment_desc_->index_);
    }

    void set_sorted(SortedValue sorted) {
       segment_desc_->sorted_ = sorted;
    }

    void set_index(const IndexDescriptorImpl& idx) {
        segment_desc_->index_ = idx;
    }

    IndexDescriptorImpl& index() {
        return static_cast<IndexDescriptorImpl&>(segment_desc_->index_);
    }

    void set_index_type(const IndexDescriptorImpl::Type type) {
       index().set_type(type);
    }

    void set_index_field_count(size_t size) {
        index().set_field_count(size);
    }

    void set_row_count(size_t row_count) {
        segment_desc_->row_count_ = row_count;
    }

    size_t row_count() const {
        return segment_desc_->row_count_;
    }

    explicit StreamDescriptor(const StreamId& id) {
        set_id(id);
        set_index({IndexDescriptor::Type::ROWCOUNT, 0});
    }

    void add_scalar_field(DataType data_type, std::string_view name) {
        fields_->add_field(TypeDescriptor{data_type, Dimension::Dim0}, name);
    }

    StreamDescriptor(const StreamId& id, const IndexDescriptorImpl &idx, std::shared_ptr<FieldCollection> fields) {
        set_id(id);
        set_index(idx);
        util::check(static_cast<bool>(fields), "Creating field collection with null pointer");
        fields_ = std::move(fields);
    }

    StreamDescriptor(const StreamId& id, const IndexDescriptorImpl &idx) {
        set_id(id);
        set_index(idx);
    }

    StreamDescriptor(const StreamDescriptor& other) = default;
    StreamDescriptor& operator=(const StreamDescriptor& other) = default;

    friend void swap(StreamDescriptor& left, StreamDescriptor& right) noexcept {
        using std::swap;

        if(&left == &right)
            return;

        swap(left.stream_id_, right.stream_id_);
        swap(left.segment_desc_, right.segment_desc_);
        swap(left.fields_, right.fields_);
    }

    StreamDescriptor& operator=(StreamDescriptor&& other) noexcept {
        swap(*this, other);
        return *this;
    }

    StreamDescriptor(StreamDescriptor&& other) noexcept
    : StreamDescriptor() {
        swap(*this, other);
    }

    [[nodiscard]] StreamDescriptor clone() const {
        return StreamDescriptor{std::make_shared<SegmentDescriptorImpl>(segment_desc_->clone()), std::make_shared<FieldCollection>(fields_->clone()), stream_id_};
    };

    [[nodiscard]] const FieldCollection& fields() const {
        return *fields_;
    }

    [[nodiscard]] FieldCollection& fields() {
        return *fields_;
    }

    [[nodiscard]] const Field& field(size_t pos) const {
        util::check(pos < fields().size(), "Field index {} out of range", pos);
        return fields_->at(pos);
    }

    [[nodiscard]] Field& mutable_field(size_t pos) {
        util::check(pos < fields().size(), "Field index {} out of range", pos);
        return fields_->at(pos);
    }

    const Field& operator[](std::size_t pos) const {
        return field(pos);
    }

    std::string_view add_field(const Field& field) {
        return fields_->add(FieldRef{field.type(), field.name()});
    }

    std::string_view add_field(FieldRef field) {
        return fields_->add(field);
    }

    [[nodiscard]] std::shared_ptr<FieldCollection> fields_ptr() const {
        return fields_;
    }

    [[nodiscard]] std::shared_ptr<SegmentDescriptorImpl> data_ptr() const {
        return segment_desc_;
    }

    decltype(auto) begin() {
        return fields().begin();
    }

    decltype(auto) end() {
        return fields().end();
    }

    [[nodiscard]] decltype(auto) begin() const {
        return fields().begin();
    }

    [[nodiscard]] decltype(auto) end() const {
        return fields().end();
    }

    [[nodiscard]] size_t field_count() const {
        return fields().size();
    }

    [[nodiscard]] bool empty() const {
        return fields().empty();
    }

    [[nodiscard]] std::optional<std::size_t> find_field(std::string_view view) const {
        auto it = std::find_if(begin(), end(), [&](const auto& field) {
            return field.name() == view;
        });

        if (it == end())
            return std::nullopt;

        return std::distance(begin(), it);
    }

    friend bool operator==(const StreamDescriptor& left, const StreamDescriptor& right) {
        if(*left.segment_desc_ != *right.segment_desc_)
            return false;

        return *left.fields_ == *right.fields_;
    }

    friend bool operator !=(const StreamDescriptor& left, const StreamDescriptor& right) {
        return !(left == right);
    }

    void erase_field(position_t field) {
        util::check(field < position_t(fields().size()), "Column index out of range in drop_column");
        fields_->erase_field(field);
    }

    FieldCollection& mutable_fields() {
        return *fields_;
    }

    [[nodiscard]] const Field& fields(size_t pos) const {
        return fields_->at(pos);
    }

    const Field& field(size_t pos) {
        return fields_->at(pos);
    }
};

struct OutputSchema {
    proto::descriptors::NormalizationMetadata norm_metadata_;

    OutputSchema() = default;

    OutputSchema(StreamDescriptor stream_descriptor,
                 proto::descriptors::NormalizationMetadata norm_metadata):
            norm_metadata_(std::move(norm_metadata)),
            stream_descriptor_(std::move(stream_descriptor)) {};

    const StreamDescriptor& stream_descriptor() const {
        return stream_descriptor_;
    }

    void set_stream_descriptor(StreamDescriptor&& stream_descriptor) {
        stream_descriptor_ = std::move(stream_descriptor);
        column_types_ = std::nullopt;
    }

    ankerl::unordered_dense::map<std::string, DataType>& column_types() {
        if (!column_types_.has_value()) {
            column_types_ = ankerl::unordered_dense::map<std::string, DataType>();
            column_types_->reserve(stream_descriptor_.field_count());
            for (const auto& field: stream_descriptor_.fields()) {
                column_types_->emplace(field.name(), field.type().data_type());
            }
        }
        return *column_types_;
    }

    void add_field(std::string_view name, DataType data_type) {
        stream_descriptor_.add_scalar_field(data_type, name);
        column_types().emplace(name, data_type);
    }

    auto release() {
        column_types_.reset();
        return std::tuple{std::move(stream_descriptor_), std::move(norm_metadata_), std::move(default_values_)};
    }

    void clear_default_values() {
        default_values_.clear();
    }

    void set_default_value_for_column(const std::string& name, const Value& value) {
        default_values_.emplace(name, value);
    }

private:
    StreamDescriptor stream_descriptor_;
    std::optional<ankerl::unordered_dense::map<std::string, DataType>> column_types_;
    ankerl::unordered_dense::map<std::string, Value> default_values_;
};

template <class IndexType>
void set_index(StreamDescriptor &stream_desc) {
    stream_desc.set_index_field_count(std::uint32_t(IndexType::field_count()));
    stream_desc.set_index_type(IndexType::type());
}

template <typename IndexType, typename RangeType>
StreamDescriptor index_descriptor_from_range(const StreamId& stream_id, IndexType, const RangeType& fields) {
    StreamDescriptor desc;
    desc.set_id(stream_id);
    set_index<IndexType>(desc);

    auto out_fields = desc.fields_ptr();
    for(const auto& field : fields)
        out_fields->add({field.type(), field.name()});

    return desc;
}

template <typename IndexType>
StreamDescriptor index_descriptor(StreamId stream_id, IndexType index_type, std::initializer_list<FieldRef> fields) {
    return index_descriptor_from_range(stream_id, index_type, fields);
}

template <typename IndexType, typename RangeType>
StreamDescriptor stream_descriptor_from_range(const StreamId& stream_id, IndexType idx, RangeType fields) {
    StreamDescriptor output;
    output.set_id(stream_id);
    set_index<IndexType>(output);

    for(auto i = 0u; i < IndexType::field_count(); ++i) {
        const auto& field = idx.field(i);
        output.add_field(FieldRef{field.type(), field.name()});
    }

    for(const auto& field : fields)
        output.add_field(FieldRef{field.type(), field.name()});

    return output;
}

template <typename IndexType>
StreamDescriptor stream_descriptor(StreamId stream_id, IndexType index_type, std::initializer_list<FieldRef> fields) {
    return stream_descriptor_from_range(stream_id, index_type, fields);
}

inline DataType stream_id_data_type(const StreamId &stream_id) {
    return std::holds_alternative<NumericId>(stream_id) ? DataType::UINT64 : DataType::ASCII_DYNAMIC64;
}

inline FieldCollection field_collection_from_proto(const google::protobuf::RepeatedPtrField<arcticdb::proto::descriptors::StreamDescriptor_FieldDescriptor>& fields) {
    FieldCollection output;
    for(const auto& field : fields)
        output.add_field(type_desc_from_proto(field.type_desc()), field.name());

    return output;
}

} //namespace arcticdb::entity

namespace fmt {
template<>
struct formatter<arcticdb::entity::StreamDescriptor> {
    template<typename ParseContext>
    constexpr auto parse(ParseContext &ctx) { return ctx.begin(); }

    template<typename FormatContext>
    auto format(const arcticdb::entity::StreamDescriptor &sd, FormatContext &ctx) const {
        if(!sd.fields_ptr())
            return fmt::format_to(ctx.out(), "TSD<tsid={}, idx={}, fields=empty>", sd.id(), sd.index());

        return fmt::format_to(ctx.out(), "TSD<tsid={}, idx={}, fields={}>", sd.id(), sd.index(), sd.fields());
    }
};

template<>
struct formatter<arcticdb::proto::descriptors::StreamDescriptor> {
    template<typename ParseContext>
    constexpr auto parse(ParseContext &ctx) { return ctx.begin(); }

    template<typename FormatContext>
    auto format(const arcticdb::proto::descriptors::StreamDescriptor &sd, FormatContext &ctx) const {
        return format_to(ctx.out(), "{}", sd.DebugString());
    }
};

} //namespace fmt
