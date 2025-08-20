/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/util/cursor.hpp>
#include <arcticdb/util/constructors.hpp>

namespace arcticdb {

template<typename BufferType>
struct CursoredBuffer {
private:
    Cursor cursor_;
    BufferType buffer_;

public:
    CursoredBuffer() = default;

    CursoredBuffer(size_t size, AllocationType allocation_type) :
        cursor_(allocation_type == AllocationType::PRESIZED || allocation_type == AllocationType::DETACHABLE ? static_cast<int64_t>(size) : 0),
        buffer_(allocation_type == AllocationType::PRESIZED ? BufferType::presized(size) : BufferType{size, allocation_type}) { }

    explicit CursoredBuffer(BufferType&& buffer) :
        cursor_(0),
        buffer_(std::move(buffer)) {}

    ARCTICDB_MOVE_ONLY_DEFAULT(CursoredBuffer)

    CursoredBuffer clone() const {
        CursoredBuffer output;
        output.cursor_ = cursor_.clone();
        output.buffer_ = buffer_.clone();
        return output;
    }

    friend void swap(CursoredBuffer& left, CursoredBuffer& right) {
        using std::swap;
        swap(left.buffer_, right.buffer_);
        swap(left.cursor_, right.cursor_);
    }

    [[nodiscard]] position_t cursor_pos() const {
        return cursor_.pos();
    }

    template<typename T>
    void ensure(size_t num = 1) {
        buffer_.ensure((num * sizeof(T)) + cursor_.pos());
    }

    void ensure_bytes(size_t bytes) {
        buffer_.ensure(cursor_.pos() + bytes);
    }

    uint8_t* ensure_aligned_bytes(size_t bytes) {
        return buffer_.ensure(cursor_.pos() + bytes, true);
    }

    void commit() {
        cursor_.commit(buffer_.bytes());
    }

    void advance(std::size_t size) {
        cursor_.advance(position_t(size), buffer_.bytes());
    }

    template<typename T>
    [[nodiscard]] size_t size() const {
        return buffer_.bytes() / sizeof(T);
    }

    [[nodiscard]] const uint8_t* data() const {
        return buffer_.data();
    }

    uint8_t* data() {
        return buffer_.data();
    }

    [[nodiscard]] size_t bytes() const {
        return buffer_.bytes();
    }

    const BufferType& buffer() const {
        return buffer_;
    }

    BufferType& buffer() {
        return buffer_;
    }

    void compact_blocks() {
        if(buffer_.blocks().size() <=1)
            return;

        CursoredBuffer tmp{buffer_.bytes(), entity::AllocationType::DYNAMIC};
        for(const auto& block : buffer_.blocks()) {
            tmp.ensure_bytes(block->bytes());
            memcpy(tmp.cursor(), block->data(), block->bytes());
            tmp.commit();
        }
        util::check(cursor_ == tmp.cursor_, "Cursor mismatch on compact_blocks, {} != {}", cursor_, tmp.cursor_);
        std::swap(tmp.buffer_, buffer_);
    }

    template<class T>
    T* pos_cast(size_t required_bytes) {
        return buffer_.template ptr_cast<T>(cursor_.pos(), required_bytes);
    }

    uint8_t *cursor() {
        return &buffer_[cursor_.pos()];
    }

    template <typename T>
    T& typed_cursor() {
        return *(reinterpret_cast<T*>(cursor()));
    }

    template<typename T>
    const T *ptr_cast(position_t t_pos, size_t required_bytes) const {
        return reinterpret_cast<const T *>(buffer_.template ptr_cast<T>(t_pos * sizeof(T), required_bytes));
    }

    template<typename T>
    T *ptr_cast(position_t pos, size_t required_bytes) {
        return const_cast<T*>(const_cast<const CursoredBuffer*>(this)->ptr_cast<T>(pos, required_bytes));
    }

    [[nodiscard]] bool empty() const {
        return buffer_.empty();
    }

    void reset() {
        cursor_.reset();
    }

    uint8_t* bytes_at(size_t bytes, size_t required) {
        return buffer_.bytes_at(bytes, required);
    }

    const uint8_t* bytes_at(size_t bytes, size_t required) const {
        return buffer_.bytes_at(bytes, required);
    }

    void clear() {
        buffer_.clear();
        cursor_.reset();
    }
};

} //namespace arcticdb
