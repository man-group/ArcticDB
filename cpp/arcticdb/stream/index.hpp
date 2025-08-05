/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/entity/types.hpp>
#include <arcticdb/util/preconditions.hpp>
#include <arcticdb/entity/atom_key.hpp>
#include <arcticdb/entity/index_range.hpp>
#include <arcticdb/entity/stream_descriptor.hpp>


namespace arcticdb {
    class SegmentInMemory;
}

namespace arcticdb::stream {

using namespace arcticdb::entity;

IndexDescriptor::Type get_index_value_type(const AtomKey& key);

template <typename Derived>
class BaseIndex {
public:
    template <class RangeType> StreamDescriptor create_stream_descriptor(StreamId stream_id, RangeType&& fields) const {
        return stream_descriptor_from_range(stream_id, *derived(), std::move(fields));
    }

    [[nodiscard]] StreamDescriptor create_stream_descriptor(StreamId stream_id, std::initializer_list<FieldRef> fields) const;
    [[nodiscard]] const Derived* derived() const;
    explicit operator IndexDescriptorImpl() const;
    [[nodiscard]] FieldRef field(size_t) const;
};

class TimeseriesIndex : public BaseIndex<TimeseriesIndex> {
public:
    static constexpr const char* DefaultName = "time" ;

    using TypeDescTag = TypeDescriptorTag<
        DataTypeTag<DataType::NANOSECONDS_UTC64>,
        DimensionTag<Dimension::Dim0>>;

    static constexpr size_t field_count() {
        return 1;
    }

    static constexpr IndexDescriptorImpl::Type type() {
        return IndexDescriptorImpl::Type::TIMESTAMP;
    }

    static constexpr timestamp min_index_value() {
        // std::numeric_limits<timestamp>::min() is reserved for NaT
        return std::numeric_limits<timestamp>::min() + 1;
    }

    explicit TimeseriesIndex(const std::string& name);
    static TimeseriesIndex default_index();
    void check(const FieldCollection& fields) const;
    static IndexValue start_value_for_segment(const SegmentInMemory& segment);
    static IndexValue end_value_for_segment(const SegmentInMemory& segment);
    static IndexValue start_value_for_keys_segment(const SegmentInMemory& segment);
    static IndexValue end_value_for_keys_segment(const SegmentInMemory& segment);

    [[nodiscard]] const char* name() const;
    static TimeseriesIndex make_from_descriptor(const StreamDescriptor& desc);

    template <class RowCellSetter>
    void set(RowCellSetter setter, const IndexValue& index_value) {
        if (std::holds_alternative<timestamp>(index_value)) {
            auto ts = std::get<timestamp>(index_value);
            util::check_arg(ts >= ts_, "timestamp decreasing, current val={}, candidate={}", ts_, ts);
            ts_ = ts;
            setter(0, ts);
        } else
            util::raise_rte("Cannot set this type, expecting timestamp");
    }

  private:
    std::string name_;
    timestamp ts_ = min_index_value();
};

class TableIndex : public BaseIndex<TableIndex> {
public:
    static constexpr const char* DefaultName = "Key";

    explicit TableIndex(const std::string& name);

    static TableIndex default_index();

    using TypeDescTag = TypeDescriptorTag<
        DataTypeTag<DataType::ASCII_DYNAMIC64>,
        DimensionTag<Dimension::Dim0>>;

    static constexpr size_t field_count() {
        return 1;
    }

    static constexpr IndexDescriptorImpl::Type type() {
        return IndexDescriptorImpl::Type::STRING;
    }

    void check(const FieldCollection& fields) const;

    static IndexValue start_value_for_segment(const SegmentInMemory& segment);

    static IndexValue end_value_for_segment(const SegmentInMemory& segment);

    static IndexValue start_value_for_keys_segment(const SegmentInMemory& segment);

    static IndexValue end_value_for_keys_segment(const SegmentInMemory& segment);

    template<class RowCellSetter>
    void set(RowCellSetter setter, const IndexValue &index_value) const {
        if (std::holds_alternative<std::string>(index_value))
            setter(0, std::get<std::string>(index_value));
        else
            util::raise_rte("Cannot set this type. Expecting std::string");
    }

    static TableIndex make_from_descriptor(const StreamDescriptor& desc);

    const char* name() const;

private:
    std::string name_;
};

class RowCountIndex : public BaseIndex<RowCountIndex> {
  public:
    using TypeDescTag = TypeDescriptorTag<
        DataTypeTag<DataType::NANOSECONDS_UTC64>,
        DimensionTag<Dimension::Dim0>>;

    RowCountIndex() = default;

    static RowCountIndex default_index();

    static constexpr size_t field_count() { return 0; }

    static constexpr IndexDescriptorImpl::Type type() { return IndexDescriptorImpl::Type::ROWCOUNT; }

    static IndexValue start_value_for_segment(const SegmentInMemory& segment);

    static IndexValue end_value_for_segment(const SegmentInMemory& segment);

    static IndexValue start_value_for_keys_segment(const SegmentInMemory& segment);

    static IndexValue end_value_for_keys_segment(const SegmentInMemory& segment);

    template<class RowCellSetter>
    void set(RowCellSetter, const IndexValue & = {timestamp(0)}) {
        // No index value
    }

    RowCountIndex make_from_descriptor(const StreamDescriptor&) const;

    static constexpr const char *name() { return "row_count"; }
};

class EmptyIndex : public BaseIndex<EmptyIndex> {
public:
    using TypeDescTag = TypeDescriptorTag<DataTypeTag<DataType::EMPTYVAL>, DimensionTag<Dimension::Dim0>>;
    static constexpr size_t field_count() {
        return 0;
    }

    static constexpr IndexDescriptor::Type type() {
        return IndexDescriptor::Type::EMPTY;
    }

    static constexpr const char* name() {
        return "empty";
    }

    static constexpr EmptyIndex default_index() {
        return {};
    }

    [[nodiscard]] static IndexValue start_value_for_segment(const SegmentInMemory& segment);
    [[nodiscard]] static IndexValue end_value_for_segment(const SegmentInMemory& segment);
    [[nodiscard]] static IndexValue start_value_for_keys_segment(const SegmentInMemory& segment);
    [[nodiscard]] static IndexValue end_value_for_keys_segment(const SegmentInMemory& segment);
};

using Index = std::variant<stream::TimeseriesIndex, stream::RowCountIndex, stream::TableIndex, stream::EmptyIndex>;

std::string mangled_name(std::string_view name);

Index index_type_from_descriptor(const StreamDescriptor& desc);
Index default_index_type_from_descriptor(const IndexDescriptorImpl& desc);

// Only to be used for visitation to get field count etc as the name is not set

Index variant_index_from_type(IndexDescriptor::Type type);
Index default_index_type_from_descriptor(const IndexDescriptor& desc);
IndexDescriptor get_descriptor_from_index(const Index& index);
Index empty_index();

}
