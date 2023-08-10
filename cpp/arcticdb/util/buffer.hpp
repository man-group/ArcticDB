/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <util/allocator.hpp>
#include <util/constructors.hpp>

#include <cstdlib>
#include <cstdint>
#include <memory>
#include <algorithm>
#include <cassert>
#include <cstring>
#include <variant>

namespace arcticdb {

struct Buffer;

template<class T, bool owning>
struct BaseBuffer {
    template<class B, std::enable_if_t<std::is_same_v<std::decay_t<B>, Buffer>, int> = 0>
    void copy_to(B &dest) const {
        dest.ensure(derived().bytes());
        std::memcpy(dest.data(), derived().data(), derived().bytes());
    }

    [[nodiscard]] const T &derived() const {
        return *(static_cast<const T *>(this));
    }
};

struct BufferView : public BaseBuffer<BufferView, false> {
    BufferView() = default;
    BufferView(uint8_t *data, size_t size) : data_(data), bytes_(size) {}

    friend void swap(BufferView &a, BufferView &b) noexcept {
        using std::swap;
        swap(a.data_, b.data_);
        swap(a.bytes_, b.bytes_);
    }

    [[nodiscard]] uint8_t *data() { return data_; }
    [[nodiscard]] const uint8_t *data() const { return data_; }
    [[nodiscard]] size_t bytes() const { return bytes_; }

  private:
    uint8_t* data_ = nullptr;
    size_t bytes_ = 0;
};

struct Buffer : public BaseBuffer<Buffer, true> {
    void init(size_t size, std::optional<size_t> preamble = std::nullopt) {
        preamble_bytes_ = preamble.value_or(0);
        ensure(size);
        check_invariants();
    }

    explicit Buffer(size_t size, std::optional<size_t> preamble = std::nullopt) {
        init(size, preamble);
    }

    Buffer() = default;

    Buffer(Buffer &&other) noexcept {
        *this = std::move(other);
        check_invariants();
    }

    Buffer &operator=(Buffer &&b) noexcept {
        deallocate();
        using std::swap;
        swap(*this, b);
        check_invariants();
        return *this;
    }

    ARCTICDB_NO_COPY(Buffer)

    ~Buffer() {
        deallocate();
    }

    void set_preamble(size_t pos) {
        util::check(pos <= capacity_, "Can't set preamble past the end of the buffer");
        preamble_bytes_ = pos;
        ptr_ += pos;
        body_bytes_ -= pos;
        check_invariants();
    }

    void deallocate() {
        if(data_ != nullptr)
            Allocator::free(std::make_pair(data_, ts_));

        data_ = nullptr;
        ptr_ = nullptr;
        capacity_ = 0;
        preamble_bytes_ = 0;
        ts_ = 0;
        body_bytes_ = 0;
        check_invariants();
    }

    void reset() {
        preamble_bytes_ = 0;
        ts_ = 0;
        body_bytes_ = 0;
        ptr_ = data_;
        memset(data_, 0, capacity_);
        check_invariants();
    }

    [[nodiscard]] bool empty() const { return bytes() == 0; }
    [[nodiscard]] uint8_t *data() { return ptr_; }
    [[nodiscard]] const uint8_t *data() const { return ptr_; }
    [[nodiscard]] size_t bytes() const { return body_bytes_; }

    friend void swap(Buffer &a, Buffer &b) noexcept {
        ARCTICDB_TRACE(log::version(), "Buffer {} swap {}", uintptr_t(&a), uintptr_t(&b));
        using std::swap;
        a.check_invariants();
        b.check_invariants();

        swap(a.data_, b.data_);
        swap(a.ptr_, b.ptr_);
        swap(a.capacity_, b.capacity_);
        swap(a.body_bytes_, b.body_bytes_);
        swap(a.preamble_bytes_, b.preamble_bytes_);
        swap(a.ts_, b.ts_);

        a.check_invariants();
        b.check_invariants();
    }

    [[nodiscard]] Buffer clone() const {
        Buffer output;
        if(total_bytes() > 0) {
            output.init(body_bytes_, preamble_bytes_);
            util::check(data_ != nullptr && output.data_ != nullptr, "Error in buffer allocation of size {} + {}", body_bytes_, preamble_bytes_);
            memcpy(output.data_, data_, total_bytes());
        }
        return output;
    }

    template<typename T>
    [[nodiscard]] T *ptr_cast(size_t bytes_offset, size_t required_bytes) {
        check_invariants();
        if (bytes_offset  + required_bytes > bytes()) {
            std::string err = fmt::format("Cursor overflow in reallocating buffer ptr_cast, cannot read {} bytes from a buffer of size {} with cursor "
                                          "at {}, as it would required {} bytes. ",
                                          required_bytes,
                                          bytes(),
                                          bytes_offset,
                                          bytes_offset + required_bytes
            );
            ARCTICDB_TRACE(log::memory(), err);
            throw ArcticCategorizedException<ErrorCategory::INTERNAL>(err);
        }

        return reinterpret_cast<T*>(ptr_ + bytes_offset);
    }

    template<typename T>
    const T *ptr_cast(size_t bytes_offset, size_t required_bytes) const {
        return const_cast<Buffer*>(this)->ptr_cast<T>(bytes_offset, required_bytes);
    }

    inline void ensure(size_t bytes) {
        const size_t total_size = bytes + preamble_bytes_;
        if(total_size > capacity_) {
            resize(total_size);
        } else {
            ARCTICDB_TRACE(log::version(), "Buffer {} has sufficient bytes for {}, ptr {} data {}, capacity {}",
                                uintptr_t(this), bytes, uintptr_t(ptr_), uintptr_t(data_), capacity_, body_bytes_);
        }

        body_bytes_ = bytes;
        check_invariants();
    }

    inline void set_bytes(size_t bytes) {
        util::check(bytes <= available(), "Can't set bytes to larger than the buffer: {} > {}", bytes, available());
        body_bytes_ = bytes;
        check_invariants();
    }

    inline void assert_size(size_t bytes) const {
        util::check(bytes <= body_bytes_, "Expected allocation size {} smaller than actual allocation {}", bytes, body_bytes_);
    }

    [[nodiscard]] BufferView view() const {
        return {ptr_, body_bytes_};
    }

    [[nodiscard]] uint8_t &operator[](size_t bytes_offset) {
        return ptr_[bytes_offset];
    }

    [[nodiscard]] const uint8_t &operator[](size_t bytes_offset) const {
        return ptr_[bytes_offset];
    }

    [[nodiscard]] size_t total_bytes() const {
        return preamble_bytes_ + body_bytes_;
    }

    [[nodiscard]] size_t preamble_bytes() const {
        return preamble_bytes_;
    }

    [[nodiscard]] uint8_t* preamble() {
        return data_;
    }
    
    [[nodiscard]] size_t available() const {
        return capacity_ >= preamble_bytes_ ? capacity_ - preamble_bytes_ : 0;
    }

  private:
    inline void resize(size_t alloc_bytes) {
        const size_t bytes = alloc_bytes - preamble_bytes_;
        util::check(alloc_bytes >= preamble_bytes_, "The requested size of a resizes call is less than the preamble bytes");
        auto [mem_ptr, ts] = ptr_ ?
                             Allocator::realloc(std::make_pair(data_, ts_), alloc_bytes)
                                  :
                             Allocator::aligned_alloc(alloc_bytes);

        ARCTICDB_TRACE(log::codec(), "Allocating {} bytes ({} + {} bytes preamble)", alloc_bytes, bytes, preamble_bytes_);
        if (mem_ptr) {
            data_ = mem_ptr;
            ptr_ = data_ + preamble_bytes_;
            ts_ = ts;
            body_bytes_ = bytes;
            capacity_ = body_bytes_ + preamble_bytes_;
            ARCTICDB_TRACE(log::version(), "Buffer {} did realloc for {}, ptr {} data {}, capacity {}",
                                uintptr_t(this), bytes,  uintptr_t(ptr_), uintptr_t(data_), capacity_, body_bytes_);
        } else {
            throw std::bad_alloc();
        }
        check_invariants();
    }

    void check_invariants() const  {
#ifdef DEBUG_BUILD
        util::check(preamble_bytes_ + body_bytes_ <= capacity_, "total_bytes exceeds capacity {} + {} > {}", preamble_bytes_, body_bytes_, capacity_);
        util::check(total_bytes() == preamble_bytes_ + body_bytes_, "Total bytes calculation is incorrect {} != {} + {}", total_bytes(), preamble_bytes_, body_bytes_);
        util::check(data_ + preamble_bytes_ == ptr_, "Buffer pointer is in the wrong place {} + {} != {}", uintptr_t(data_), preamble_bytes_, uintptr_t(ptr_));
#endif
    }

    uint8_t *data_ = nullptr;
    uint8_t* ptr_ = nullptr;
    size_t capacity_ = 0;
    size_t body_bytes_ = 0;
    size_t preamble_bytes_ = 0;
    entity::timestamp ts_ = 0;
};

using VariantBuffer = std::variant<std::monostate, std::shared_ptr<Buffer>, BufferView>;


} // namespace arcticdb
