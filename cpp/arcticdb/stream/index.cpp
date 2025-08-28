/* Copyright 2024 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/stream/index.hpp>
#include <arcticdb/column_store/memory_segment.hpp>
#include <arcticdb/pipeline/index_fields.hpp>
#include <arcticdb/entity/type_utils.hpp>
#include <ranges>

namespace arcticdb::stream {

template <typename Derived>
StreamDescriptor BaseIndex<Derived>::create_stream_descriptor(
    StreamId stream_id,
    std::initializer_list<FieldRef> fields
) const {
    std::vector<FieldRef> fds{fields};
    return create_stream_descriptor(stream_id, std::views::all(fds));
}

[[nodiscard]] IndexDescriptor::Type get_index_value_type(const AtomKey& key) {
    return std::holds_alternative<timestamp>(key.start_index()) ? IndexDescriptor::Type::TIMESTAMP : IndexDescriptor::Type::STRING;
}

template <typename Derived> const Derived* BaseIndex<Derived>::derived() const {
    return static_cast<const Derived*>(this);
}

template <typename Derived> BaseIndex<Derived>::operator IndexDescriptorImpl() const {
    return {Derived::type(), Derived::field_count()};
}

template <typename Derived> FieldRef BaseIndex<Derived>::field(size_t) const {
    return {static_cast<TypeDescriptor>(typename Derived::TypeDescTag{}), std::string_view(derived()->name())};
}

TimeseriesIndex::TimeseriesIndex(const std::string& name) : name_(name) {}

TimeseriesIndex TimeseriesIndex::default_index() {
    return TimeseriesIndex(DefaultName);
}

void TimeseriesIndex::check(const FieldCollection& fields) const {
    const size_t fields_size = fields.size();
    constexpr int current_fields_size = int(field_count());

    const TypeDescriptor& first_field_type = fields[0].type();
    const TypeDescriptor& current_first_field_type = this->field(0).type();

    const bool valid_type_promotion = is_valid_type_promotion_to_target(first_field_type, current_first_field_type);
    const bool trivial_type_compatibility = trivially_compatible_types(first_field_type, current_first_field_type);

    const bool compatible_types = valid_type_promotion || trivial_type_compatibility;

    util::check_arg(
        fields_size >= current_fields_size,
        "expected at least {} fields, actual {}",
        current_fields_size,
        fields_size
    );
    util::check_arg(compatible_types, "expected field[0]={}, actual {}", this->field(0), fields[0]);
}

IndexValue TimeseriesIndex::start_value_for_segment(const SegmentInMemory& segment) {
    if (segment.row_count() == 0)
        return {NumericIndex{min_index_value()}};
    auto first_ts = segment.template scalar_at<timestamp>(0, 0).value();
    return {first_ts};
}

IndexValue TimeseriesIndex::end_value_for_segment(const SegmentInMemory& segment) {
    auto row_count = segment.row_count();
    if (row_count == 0)
        return {NumericIndex{min_index_value()}};
    auto last_ts = segment.template scalar_at<timestamp>(row_count - 1, 0).value();
    return {last_ts};
}

IndexValue TimeseriesIndex::start_value_for_keys_segment(const SegmentInMemory& segment) {
    if (segment.row_count() == 0)
        return {NumericIndex{min_index_value()}};
    auto start_index_id = int(pipelines::index::Fields::start_index);
    auto first_ts = segment.template scalar_at<timestamp>(0, start_index_id).value();
    return {first_ts};
}

IndexValue TimeseriesIndex::end_value_for_keys_segment(const SegmentInMemory& segment) {
    auto row_count = segment.row_count();
    if (row_count == 0)
        return {NumericIndex{min_index_value()}};
    auto end_index_id = int(pipelines::index::Fields::end_index);
    auto last_ts = segment.template scalar_at<timestamp>(row_count - 1, end_index_id).value();
    return {last_ts};
}

const char* TimeseriesIndex::name() const {
    return name_.c_str();
}

TimeseriesIndex TimeseriesIndex::make_from_descriptor(const StreamDescriptor& desc) {
    if (desc.field_count() > 0)
        return TimeseriesIndex(std::string(desc.fields(0).name()));

    return TimeseriesIndex(DefaultName);
}


TableIndex::TableIndex(const std::string& name) : name_(name) {
}

TableIndex TableIndex::default_index() {
    return TableIndex(DefaultName);
}

void TableIndex::check(const FieldCollection& fields) const {
    util::check_arg(
        fields.size() >= int(field_count()),
        "expected at least {} fields, actual {}",
        field_count(),
        fields.size()
    );

    util::check(fields.ref_at(0) == field(0), "Field descriptor mismatch {} != {}", fields.ref_at(0), field(0));
}

IndexValue TableIndex::start_value_for_segment(const SegmentInMemory& segment) {
    auto string_index = segment.string_at(0, 0).value();
    return {std::string{string_index}};
}

IndexValue TableIndex::end_value_for_segment(const SegmentInMemory& segment) {
    auto last_rowid = segment.row_count() - 1;
    auto string_index = segment.string_at(last_rowid, 0).value();
    return {std::string{string_index}};
}

IndexValue TableIndex::start_value_for_keys_segment(const SegmentInMemory& segment) {
    if (segment.row_count() == 0)
        return {NumericIndex{0}};
    auto start_index_id = int(pipelines::index::Fields::start_index);
    auto string_index = segment.string_at(0, start_index_id).value();
    return {std::string{string_index}};
}

IndexValue TableIndex::end_value_for_keys_segment(const SegmentInMemory& segment) {
    auto row_count = segment.row_count();
    if (row_count == 0)
        return {NumericIndex{0}};
    auto end_index_id = int(pipelines::index::Fields::end_index);
    auto string_index = segment.string_at(row_count - 1, end_index_id).value();
    return {std::string{string_index}};
}

TableIndex TableIndex::make_from_descriptor(const StreamDescriptor& desc) {
    if (desc.field_count() > 0)
        return TableIndex(std::string(desc.field(0).name()));

    return TableIndex(DefaultName);
}

const char* TableIndex::name() const {
    return name_.c_str();
}

RowCountIndex RowCountIndex::default_index() {
    return RowCountIndex{};
}

IndexValue RowCountIndex::start_value_for_segment(const SegmentInMemory& segment) {
    return static_cast<timestamp>(segment.offset());
}

IndexValue RowCountIndex::end_value_for_segment(const SegmentInMemory& segment) {
    return static_cast<timestamp>(segment.offset() + (segment.row_count() - 1));
}

IndexValue RowCountIndex::start_value_for_keys_segment(const SegmentInMemory& segment) {
    return static_cast<timestamp>(segment.offset());
}

IndexValue RowCountIndex::end_value_for_keys_segment(const SegmentInMemory& segment) {
    return static_cast<timestamp>(segment.offset() + (segment.row_count() - 1));
}

RowCountIndex RowCountIndex::make_from_descriptor(const StreamDescriptor&) const {
    return RowCountIndex::default_index();
}

IndexValue EmptyIndex::start_value_for_segment(const SegmentInMemory& segment) {
    return static_cast<NumericIndex>(segment.offset());
}

IndexValue EmptyIndex::end_value_for_segment(const SegmentInMemory& segment) {
    return static_cast<NumericIndex>(segment.offset());
}

IndexValue EmptyIndex::start_value_for_keys_segment(const SegmentInMemory& segment) {
    return static_cast<NumericIndex>(segment.offset());
}

IndexValue EmptyIndex::end_value_for_keys_segment(const SegmentInMemory& segment) {
    return static_cast<NumericIndex>(segment.offset());
}

Index index_type_from_descriptor(const StreamDescriptor& desc) {
    switch (desc.index().type()) {
    case IndexDescriptor::Type::EMPTY:
        return EmptyIndex{};
    case IndexDescriptor::Type::TIMESTAMP:
        return TimeseriesIndex::make_from_descriptor(desc);
    case IndexDescriptor::Type::STRING:
        return TableIndex::make_from_descriptor(desc);
    case IndexDescriptor::Type::ROWCOUNT:
        return RowCountIndex{};
    default:
        util::raise_rte(
            "Data obtained from storage refers to an index type that this build of ArcticDB doesn't understand ({}).",
            int(desc.index().type())
        );
    }
}

Index default_index_type_from_descriptor(const IndexDescriptorImpl& desc) {
    switch (desc.type()) {
    case IndexDescriptor::Type::EMPTY:
        return EmptyIndex{};
    case IndexDescriptor::Type::TIMESTAMP:
        return TimeseriesIndex::default_index();
    case IndexDescriptor::Type::STRING:
        return TableIndex::default_index();
    case IndexDescriptor::Type::ROWCOUNT:
        return RowCountIndex::default_index();
    default:
        util::raise_rte("Unknown index type {} trying to generate index type", int(desc.type()));
    }
}

IndexDescriptor get_descriptor_from_index(const Index& index) {
    return util::variant_match(index, [](const auto& idx) { return static_cast<IndexDescriptorImpl>(idx); });
}

Index empty_index() {
    return RowCountIndex::default_index();
}

template class BaseIndex<TimeseriesIndex>;
template class BaseIndex<TableIndex>;
template class BaseIndex<RowCountIndex>;
template class BaseIndex<EmptyIndex>;

std::string mangled_name(std::string_view name) {
    return fmt::format("__idx__{}", name);
}


}