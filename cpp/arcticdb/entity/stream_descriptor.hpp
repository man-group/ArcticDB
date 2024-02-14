/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/entity/field_collection.hpp>
#include <arcticdb/memory_layout.hpp>
#include <folly/gen/Base.h>

namespace arcticdb::entity {


struct StreamDescriptorDataImpl : public StreamDescriptorData {
    StreamDescriptorDataImpl() = default;

    ARCTICDB_MOVE_COPY_DEFAULT(StreamDescriptorDataImpl)

    [[nodiscard]] StreamDescriptorDataImpl clone() const {
        return *this;
    }
};

bool operator==(const StreamDescriptorDataImpl& left, const StreamDescriptorDataImpl& right) {
    return left.stream_id_ == right.stream_id_ && left.index_ == right.index_;
}

bool operator!=(const StreamDescriptorDataImpl& left, const StreamDescriptorDataImpl& right) {
    return !(left == right);
}

struct StreamDescriptor {
    std::shared_ptr<StreamDescriptorDataImpl> data_ = std::make_shared<StreamDescriptorDataImpl>();
    std::shared_ptr<FieldCollection> fields_ = std::make_shared<FieldCollection>();

    StreamDescriptor() = default;
    ~StreamDescriptor() = default;

    StreamDescriptor(std::shared_ptr<StreamDescriptorDataImpl> data, std::shared_ptr<FieldCollection> fields) :
            data_(std::move(data)),
            fields_(std::move(fields)) {

    }

    [[nodiscard]] const StreamDescriptorDataImpl& data() const  {
        return *data_;
    }

    void set_id(const StreamId& id) {
        data_->stream_id_ = id;
    }

    [[nodiscard]] StreamId id() const {
        return data_->stream_id_;
    }

    [[nodiscard]] uint64_t uncompressed_bytes() const {
        return data_->uncompressed_bytes_;
    }

    [[nodiscard]] uint64_t compressed_bytes() const {
        return data_->compressed_bytes_;
    }

    [[nodiscard]] SortedValue sorted() const {
        return data_->sorted_;
    }

    [[nodiscard]] IndexDescriptor index() const {
        return data_->index_;
    }

    void set_sorted(SortedValue sorted) {
       data_->sorted_ = sorted;
    }

    SortedValue get_sorted() {
        return data_->sorted_;
    }

    void set_index(const IndexDescriptor& idx) {
        data_->index_ = idx;
    }

    void set_index_type(const IndexDescriptor::Type type) {
        data_->index_.set_type(type);
    }

    void set_index_field_count(size_t size) {
        data_->index_.set_field_count(size);
    }

    explicit StreamDescriptor(const StreamId& id) {
        set_id(id);
    }

    void add_scalar_field(DataType data_type, std::string_view name) {
        fields_->add_field(TypeDescriptor{data_type, Dimension::Dim0}, name);
    }

    StreamDescriptor(const StreamId& id, const IndexDescriptor &idx, std::shared_ptr<FieldCollection> fields) {
        set_id(id);
        set_index(idx);
        util::check(static_cast<bool>(fields), "Creating field collection with null pointer");
        fields_ = std::move(fields);
    }

    StreamDescriptor(const StreamId& id, const IndexDescriptor &idx) {
        set_id(id);
        set_index(idx);
    }

    StreamDescriptor(const StreamDescriptor& other) = default;
    StreamDescriptor& operator=(const StreamDescriptor& other) = default;

    friend void swap(StreamDescriptor& left, StreamDescriptor& right) noexcept {
        using std::swap;

        if(&left == &right)
            return;

        swap(left.data_, right.data_);
        swap(left.fields_, right.fields_);
    }

    StreamDescriptor& operator=(StreamDescriptor&& other) {
        swap(*this, other);
        return *this;
    }

    StreamDescriptor(StreamDescriptor&& other) noexcept
    : StreamDescriptor() {
        swap(*this, other);
    }

    [[nodiscard]] StreamDescriptor clone() const {
        return StreamDescriptor{std::make_shared<StreamDescriptorDataImpl>(data_->clone()), std::make_shared<FieldCollection>(fields_->clone())};
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

    std::shared_ptr<FieldCollection> fields_ptr() const {
        return fields_;
    }

    decltype(auto) begin() {
        return fields().begin();
    }

    decltype(auto) end() {
        return fields().end();
    }

    decltype(auto) begin() const {
        return fields().begin();
    }

    decltype(auto) end() const {
        return fields().end();
    }

    [[nodiscard]] size_t field_count() const {
        return fields().size();
    }

    bool empty() const {
        return fields().empty();
    }

    std::optional<std::size_t> find_field(std::string_view view) const {
        auto it = std::find_if(begin(), end(), [&](const auto& field) {
            return field.name() == view;
        });

        if (it == end()) return std::nullopt;
        return std::distance(begin(), it);
    }

    friend bool operator==(const StreamDescriptor& left, const StreamDescriptor& right) {
        google::protobuf::util::MessageDifferencer diff;
        if(*left.data_ != *right.data_)
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

template <class IndexType>
inline void set_index(arcticdb::proto::descriptors::StreamDescriptor &stream_desc) {
    auto& pb_desc = *stream_desc.mutable_index();
    pb_desc.set_field_count(std::uint32_t(IndexType::field_count()));
    pb_desc.set_kind(static_cast<arcticdb::proto::descriptors::IndexDescriptor_Type>(
        static_cast<int>(IndexType::type())));
}

template <typename IndexType, typename RangeType>
StreamDescriptor index_descriptor(const StreamId& stream_id, IndexType, const RangeType& fields) {
    StreamDescriptor desc;
    desc.set_id(stream_id);
    desc.set_index(IndexType{});
    auto out_fields = std::make_shared<FieldCollection>();
    for(const auto& field : fields) {
        out_fields->add({field.type(), field.name()});
    }

    return desc;
}

template <typename IndexType>
StreamDescriptor index_descriptor(StreamId stream_id, IndexType index_type,
                                  std::initializer_list<FieldRef> fields) {
    return index_descriptor(stream_id, index_type, folly::gen::from(fields) | folly::gen::as<std::vector>());
}

template <typename IndexType, typename RangeType>
StreamDescriptor stream_descriptor(const StreamId& stream_id, IndexType idx, RangeType fields) {
    StreamDescriptor output;

    output.set_id(stream_id);
    set_index<IndexType>(*output.data_);
    for(auto i = 0u; i < IndexType::field_count(); ++i) {
        const auto& field = idx.field(i);
        output.add_field(FieldRef{field.type(), field.name()});
    }

    for(const auto& field : fields) {
        output.add_field(FieldRef{field.type(), field.name()});
    }

    return output;
}

template <typename IndexType>
StreamDescriptor stream_descriptor(StreamId stream_id, IndexType index_type,
                                          std::initializer_list<FieldRef> fields) {
    std::vector<FieldRef> vec{fields};
    return stream_descriptor(stream_id, index_type, folly::range(vec));
}

inline TypeDescriptor stream_id_descriptor(const StreamId &stream_id) {
    return std::holds_alternative<NumericId>(stream_id) ?
    TypeDescriptor(DataType::UINT64, 0) :
    TypeDescriptor(DataType::ASCII_DYNAMIC64, 0);
}

inline DataType stream_id_data_type(const StreamId &stream_id) {
    return std::holds_alternative<NumericId>(stream_id) ? DataType::UINT64 : DataType::ASCII_DYNAMIC64;
}

inline FieldCollection field_collection_from_proto(google::protobuf::RepeatedPtrField<arcticdb::proto::descriptors::StreamDescriptor_FieldDescriptor>&& fields) {
    FieldCollection output;
    for(const auto& field : fields) {
        output.add_field(type_desc_from_proto(field.type_desc()), field.name());
    }
    return output;
}

} //namespace arcticdb

namespace fmt {
template<>
struct formatter<arcticdb::entity::StreamDescriptor> {
    template<typename ParseContext>
    constexpr auto parse(ParseContext &ctx) { return ctx.begin(); }

    template<typename FormatContext>
    auto format(const arcticdb::entity::StreamDescriptor &sd, FormatContext &ctx) const {
        if(!sd.fields_ptr())
            return format_to(ctx.out(), "TSD<tsid={}, idx={}, fields=empty>", sd.id(), sd.index());

        return format_to(ctx.out(), "TSD<tsid={}, idx={}, fields={}>", sd.id(), sd.index(), sd.fields());
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
