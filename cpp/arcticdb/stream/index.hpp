/*
* Copyright 2023 Man Group Operations Ltd.
* NO WARRANTY, EXPRESSED OR IMPLIED
*/

#pragma once

#include <arcticdb/entity/types.hpp>
#include <arcticdb/util/preconditions.hpp>
#include <arcticdb/util/clock.hpp>
#include <arcticdb/entity/atom_key.hpp>
#include <arcticdb/entity/index_range.hpp>
#include <arcticdb/pipeline/index_fields.hpp>

#include <folly/Range.h>

namespace arcticdb::stream {

using namespace arcticdb::entity;

inline IndexDescriptor::Type get_index_value_type(const AtomKey &key) {
    return std::holds_alternative<timestamp>(key.start_index()) ? IndexDescriptor::TIMESTAMP : IndexDescriptor::STRING;
}

template<typename Derived>
class BaseIndex {
  public:
    template<class RangeType>
    StreamDescriptor create_stream_descriptor(StreamId stream_id, RangeType fields) const {
        return StreamDescriptor{stream_descriptor(stream_id, *derived(), fields)};
    }

    StreamDescriptor create_stream_descriptor(StreamId stream_id,
                                                     std::initializer_list<FieldDescriptor::Proto> fields) const {
        std::vector<FieldDescriptor::Proto> fds{fields};
        return create_stream_descriptor(stream_id, folly::range(fds));
    }

    [[nodiscard]] const Derived* derived() const {
        return static_cast<const Derived*>(this);
    }

    explicit operator IndexDescriptor() const {
        return {Derived::field_count(), Derived::type()};
    }

    [[nodiscard]] FieldDescriptor field_proto(size_t) const {
        return FieldDescriptor{scalar_field_proto(typename Derived::TypeDescTag::DataTypeTag().data_type, std::string(derived()->name()))};;
    }
};

//TODO make this into just a numeric index, of which timestamp is a special case
class TimeseriesIndex : public BaseIndex<TimeseriesIndex> {
    static constexpr const char* DefaultName = "time" ;

  public:
    explicit TimeseriesIndex(const std::string name) :
        name_(name) {
    }

    static TimeseriesIndex default_index() {
        return TimeseriesIndex(DefaultName);
    }

    using TypeDescTag = TypeDescriptorTag<
        DataTypeTag<DataType::MICROS_UTC64>,
        DimensionTag<Dimension::Dim0>>;

    static constexpr size_t field_count() {
        return 1;
    }

    static constexpr IndexDescriptor::Type type() {
        return IndexDescriptor::TIMESTAMP;
    }

    void check(const StreamDescriptor::FieldsCollection &fields) const {
        util::check_arg(fields.size() >= int(field_count()), "expected at least {} fields, actual {}",
                        field_count(), fields.size());
        util::check_arg(type_desc_from_proto(fields[0].type_desc()) == this->field_proto(0).type_desc(), "expected field[0]={}, actual {}",
                        this->field_proto(0), fields[0]);
    }

    template<typename SegmentType>
    static IndexValue start_value_for_segment(const SegmentType &segment) {
        if (segment.row_count() == 0)
            return {0};
        auto first_ts = segment.template scalar_at<timestamp>(0, 0).value();
        return {first_ts};
    }

    template<typename SegmentType>
    static IndexValue end_value_for_segment(const SegmentType &segment) {
        auto row_count = segment.row_count();
        if (row_count == 0)
            return {0};
        auto last_ts = segment.template scalar_at<timestamp>(row_count - 1, 0).value();
        return {last_ts};
    }

    template<typename SegmentType>
    static IndexValue start_value_for_keys_segment(const SegmentType &segment) {
        if (segment.row_count() == 0)
            return {0};
        auto start_index_id = int(pipelines::index::Fields::start_index);
        auto first_ts = segment.template scalar_at<timestamp>(0, start_index_id).value();
        return {first_ts};
    }

    template<typename SegmentType>
    static IndexValue end_value_for_keys_segment(const SegmentType &segment) {
        auto row_count = segment.row_count();
        if (row_count == 0)
            return {0};
        auto end_index_id = int(pipelines::index::Fields::end_index);
        auto last_ts = segment.template scalar_at<timestamp>(row_count - 1, end_index_id).value();
        return {last_ts};
    }

    template<class RowCellSetter>
    void set(RowCellSetter setter, const IndexValue &index_value) {
        if (std::holds_alternative<timestamp>(index_value)) {
            auto ts = std::get<timestamp>(index_value);
            util::check_arg(ts >= ts_, "timestamp decreasing, current val={}, candidate={}", ts_, ts);
            ts_ = ts;
            setter(0, ts);
        } else
            util::raise_rte("Cannot set this type, expecting timestamp");
    }

    [[nodiscard]] const char *name() const { return name_.c_str(); }

    static TimeseriesIndex make_from_descriptor(const StreamDescriptor::Proto& desc) {
        if(desc.fields_size() > 0)
            return TimeseriesIndex(desc.fields(0).name());

        return TimeseriesIndex(DefaultName);
    }

  private:
    std::string name_;
    timestamp ts_ = 0;
};

class TableIndex : public BaseIndex<TableIndex> {
    static constexpr const char* DefaultName = "Key";

  public:
    explicit TableIndex(const std::string& name) :
        name_(name) {
    }

    static TableIndex default_index() {
        return TableIndex(DefaultName);
    }

    using TypeDescTag = TypeDescriptorTag<
        DataTypeTag<DataType::ASCII_DYNAMIC64>,
        DimensionTag<Dimension::Dim0>>;

    static constexpr size_t field_count() {
        return 1;
    }

    static constexpr IndexDescriptor::Type type() {
        return IndexDescriptor::STRING;
    }

    void check(const StreamDescriptor::FieldsCollection &fields) const {
        util::check_arg(fields.size() >= int(field_count()), "expected at least {} fields, actual {}",
                        field_count(), fields.size());

        google::protobuf::util::MessageDifferencer diff;
        util::check(diff.Compare(fields[0], static_cast<const FieldDescriptor::Proto&>(field_proto(0))),
            "Field descriptor mismatch {} != {}", fields[0], field_proto(0));
    }

    template<typename SegmentType>
    static IndexValue start_value_for_segment(const SegmentType &segment) {
        auto string_index = segment.string_at(0, 0).value();
        return {std::string{string_index}};
    }

    template<typename SegmentType>
    static IndexValue end_value_for_segment(const SegmentType &segment) {
        auto last_rowid = segment.row_count() - 1;
        auto string_index = segment.string_at(last_rowid, 0).value();
        return {std::string{string_index}};
    }

    template<typename SegmentType>
    static IndexValue start_value_for_keys_segment(const SegmentType &segment) {
        if (segment.row_count() == 0)
            return {0};
        auto start_index_id = int(pipelines::index::Fields::start_index);
        auto string_index = segment.string_at(0, start_index_id).value();
        return {std::string{string_index}};
    }

    template<typename SegmentType>
    static IndexValue end_value_for_keys_segment(const SegmentType &segment) {
        auto row_count = segment.row_count();
        if (row_count == 0)
            return {0};
        auto end_index_id = int(pipelines::index::Fields::end_index);
        auto string_index = segment.string_at(row_count - 1, end_index_id).value();
        return {std::string{string_index}};
    }

    template<class RowCellSetter>
    void set(RowCellSetter setter, const IndexValue &index_value) const {
        if (std::holds_alternative<std::string>(index_value))
            setter(0, std::get<std::string>(index_value));
        else
            util::raise_rte("Cannot set this type. Expecting std::string");
    }

    static TableIndex make_from_descriptor(const StreamDescriptor::Proto& desc) {
        if(desc.fields_size() > 0)
            return TableIndex(desc.fields(0).name());

        return TableIndex(DefaultName);
    }

    const char *name() const { return name_.c_str(); }

private:
    std::string name_;
};

class RowCountIndex : public BaseIndex<RowCountIndex> {
  public:
    using TypeDescTag = TypeDescriptorTag<
        DataTypeTag<DataType::MICROS_UTC64>,
        DimensionTag<Dimension::Dim0>>;

    RowCountIndex() = default;

    static RowCountIndex default_index() {
        return RowCountIndex{};
    }

    static constexpr size_t field_count() { return 0; }

    static constexpr IndexDescriptor::Type type() { return IndexDescriptor::ROWCOUNT; }

    void check(const StreamDescriptor::FieldsCollection& ) const {
        // No index defined
    }

    template<typename SegmentType>
    static IndexValue start_value_for_segment(const SegmentType &segment) {
        return static_cast<timestamp>(segment.offset());
    }

    template<typename SegmentType>
    static IndexValue end_value_for_segment(const SegmentType &segment) {
        return static_cast<timestamp>(segment.offset() + (segment.row_count() - 1));
    }

    template<typename SegmentType>
    static IndexValue start_value_for_keys_segment(const SegmentType &segment) {
        return static_cast<timestamp>(segment.offset());
    }

    template<typename SegmentType>
    static IndexValue end_value_for_keys_segment(const SegmentType &segment) {
        return static_cast<timestamp>(segment.offset() + (segment.row_count() - 1));
    }

    template<class RowCellSetter>
    void set(RowCellSetter, const IndexValue & = {timestamp(0)}) {
        // No index value
    }

    RowCountIndex make_from_descriptor(const StreamDescriptor&) const {
        return RowCountIndex::default_index();
    }

    static constexpr const char *name() { return "row_count"; }
};

using Index = std::variant<stream::TimeseriesIndex, stream::RowCountIndex, stream::TableIndex>;

inline Index index_type_from_descriptor(const StreamDescriptor::Proto &desc) {
    switch (desc.index().kind()) {
    case IndexDescriptor::TIMESTAMP:
        return TimeseriesIndex::make_from_descriptor(desc);
    case IndexDescriptor::STRING:
        return TableIndex::make_from_descriptor(desc);
    case IndexDescriptor::ROWCOUNT:
        return RowCountIndex{};
    default:util::raise_rte("Data obtained from storage refers to an index type that this build of ArcticDB doesn't understandi ({}).", desc.index().kind());
    }
}

inline Index default_index_type_from_descriptor(const IndexDescriptor::Proto &desc) {
    switch (desc.kind()) {
    case IndexDescriptor::TIMESTAMP:
        return TimeseriesIndex::default_index();
    case IndexDescriptor::STRING:
        return TableIndex::default_index();
    case IndexDescriptor::ROWCOUNT:
        return RowCountIndex::default_index();
    default:
        util::raise_rte("Unknown index type {} trying to generate index type", desc.kind());
    }
}

inline Index index_type_from_descriptor(const StreamDescriptor& desc) {
    return index_type_from_descriptor(desc.proto());
}

inline IndexDescriptor get_descriptor_from_index(const Index& index) {
    return util::variant_match(index, [] (const auto& idx) {
        return static_cast<IndexDescriptor>(idx);
    });
}

inline Index empty_index() {
    return RowCountIndex::default_index();
}

}



