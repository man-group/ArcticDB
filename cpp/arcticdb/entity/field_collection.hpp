/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/column_store/chunked_buffer.hpp>
#include <util/cursored_buffer.hpp>
#include <util/buffer.hpp>
#include <entity/types.hpp>
#include <arcticdb/column_store/column_data.hpp>

namespace arcticdb {

class FieldCollection {
    CursoredBuffer<ChunkedBuffer> buffer_;
    CursoredBuffer<Buffer> offsets_;
    CursoredBuffer<Buffer> shapes_;
    static constexpr TypeDescriptor type_ = TypeDescriptor{DataType::UINT8, Dimension::Dim1};

    FieldCollection(
            CursoredBuffer<ChunkedBuffer>&& buffer, CursoredBuffer<Buffer>&& offsets, CursoredBuffer<Buffer> shapes
    ) :
        buffer_(std::move(buffer)),
        offsets_(std::move(offsets)),
        shapes_(std::move(shapes)) {}

  public:
    ARCTICDB_MOVE_ONLY_DEFAULT(FieldCollection)

    static TypeDescriptor type() { return type_; }

    template<class ValueType>
    class FieldCollectionIterator
        : public boost::iterator_facade<FieldCollectionIterator<ValueType>, ValueType, boost::forward_traversal_tag> {
      public:
        explicit FieldCollectionIterator(ChunkedBuffer* buffer) : buffer_(buffer) {}

        explicit FieldCollectionIterator(ChunkedBuffer* buffer, size_t pos) : buffer_(buffer), pos_(pos) {}

        template<class OtherValue>
        explicit FieldCollectionIterator(const FieldCollectionIterator<OtherValue>& other) :
            buffer_(other.buffer_),
            pos_(other.pos_) {}

        FieldCollectionIterator() = default;

        FieldCollectionIterator& operator=(const FieldCollectionIterator& other) {
            if (&other != this) {
                pos_ = other.pos_;
                buffer_ = other.buffer_;
            }

            return *this;
        }

        FieldCollectionIterator(const FieldCollectionIterator& other) : buffer_(other.buffer_), pos_(other.pos_) {}

        template<class OtherValue>
        bool equal(const FieldCollectionIterator<OtherValue>& other) const {
            return pos_ == other.pos_ && buffer_ == other.buffer_;
        }

        void increment() { pos_ += std::max(sizeof(ValueType), Field::calc_size(dereference().name())); }

        [[nodiscard]] ValueType& dereference() const { return *buffer_->ptr_cast<ValueType>(pos_, sizeof(ValueType)); }

        ChunkedBuffer* buffer_ = nullptr;
        size_t pos_ = 0;
    };

    [[nodiscard]] auto begin() { return FieldCollectionIterator<Field>(&buffer_.buffer()); }

    [[nodiscard]] auto end() { return FieldCollectionIterator<Field>(&buffer_.buffer(), buffer_.bytes()); }

    [[nodiscard]] auto begin() const {
        return FieldCollectionIterator<const Field>(const_cast<ChunkedBuffer*>(&buffer_.buffer()));
    }

    [[nodiscard]] auto end() const {
        return FieldCollectionIterator<const Field>(const_cast<ChunkedBuffer*>(&buffer_.buffer()), buffer_.bytes());
    }

    [[nodiscard]] bool empty() const { return buffer_.empty(); }

    FieldCollection() = default;

    std::string_view add_field(const FieldRef& field) { return add_field(field.type_, field.name_); }

    std::string_view add_field(const TypeDescriptor& type, std::string_view name);

    // Note that this is expensive and is primarily intended for testing, where drop_column
    // is a good way of creating dynamic schema from a large dataframe
    void erase_field(position_t pos) {
        util::check(
                static_cast<size_t>(pos) < size(), "Position {} out of range in drop field with {} fields", pos, size()
        );
        FieldCollection result;
        position_t count = 0;
        for (const auto& field : *this) {
            if (count++ != pos)
                result.add_field(field.ref());
        }
        std::swap(*this, result);
    }

    inline shape_t* allocate_shapes(std::size_t bytes) {
        util::check(bytes != 0, "Allocate data called with zero size");
        shapes_.ensure_bytes(bytes);
        return reinterpret_cast<shape_t*>(shapes_.cursor());
    }

    inline uint8_t* allocate_data(std::size_t bytes) {
        util::check(bytes != 0, "Allocate data called with zero size");
        buffer_.ensure_bytes(bytes);
        return buffer_.cursor();
    }

    inline void advance_data(std::size_t size) { buffer_.advance(position_t(size)); }

    inline void advance_shapes(std::size_t size) { shapes_.advance(position_t(size)); }

    std::string_view add(const FieldRef& field) { return add_field(field.type(), field.name()); }

    void set_allow_sparse(Sparsity) {
        // Not used
    }

    [[nodiscard]] size_t get_offset(size_t pos) const {
        if (pos == 0)
            return 0;

        return *offsets_.buffer().ptr_cast<shape_t>((pos - 1) * sizeof(shape_t), sizeof(shape_t));
    }

    [[nodiscard]] const Field& at(size_t pos) const {
        return *(buffer_.buffer().ptr_cast<Field>(get_offset(pos), sizeof(shape_t)));
    }

    [[nodiscard]] FieldRef ref_at(size_t pos) const {
        const auto* field = buffer_.buffer().ptr_cast<Field>(get_offset(pos), sizeof(shape_t));
        return {field->type(), field->name()};
    }

    [[nodiscard]] Field& at(size_t pos) {
        return *(buffer_.buffer().ptr_cast<Field>(get_offset(pos), sizeof(shape_t)));
    }

    [[nodiscard]] size_t size() const { return (offsets_.bytes() / sizeof(shape_t)); }

    [[nodiscard]] ColumnData column_data() const { return {&buffer_.buffer(), &shapes_.buffer(), type_, nullptr}; }

    size_t num_blocks() const { return buffer_.buffer().num_blocks(); }

    const Field& operator[](size_t pos) const { return at(pos); }

    const ChunkedBuffer& buffer() const { return buffer_.buffer(); }

    friend bool operator==(const FieldCollection& left, const FieldCollection& right);

    void regenerate_offsets();

    [[nodiscard]] FieldCollection clone() const { return {buffer_.clone(), offsets_.clone(), shapes_.clone()}; }
};

template<typename RangeType>
FieldCollection fields_from_range(const RangeType& fields) {
    FieldCollection output;
    for (const auto& field : fields) {
        output.add({field.type(), field.name()});
    }
    return output;
}

} // namespace arcticdb

namespace fmt {
template<>
struct formatter<arcticdb::FieldCollection> {
    template<typename ParseContext>
    constexpr auto parse(ParseContext& ctx) {
        return ctx.begin();
    }
    template<typename FormatContext>
    auto format(const arcticdb::FieldCollection& fc, FormatContext& ctx) const {
        for (size_t i = 0; i < fc.size(); ++i) {
            if (i == fc.size() - 1) {
                fmt::format_to(ctx.out(), "FD<name={}, type={}, idx={}>", fc[i].name(), fc[i].type(), i);
            } else {
                fmt::format_to(ctx.out(), "FD<name={}, type={}, idx={}>, ", fc[i].name(), fc[i].type(), i);
            }
        }

        return ctx.out();
    }
};
} // namespace fmt
