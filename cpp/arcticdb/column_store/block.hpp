/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/util/preconditions.hpp>
#include <arcticdb/util/magic_num.hpp>

#include <cstdint>

namespace arcticdb {

struct MemBlock {
    static const size_t MinSize = 64;
    using magic_t = arcticdb::util::MagicNum<'M', 'e', 'm', 'b'>;
    magic_t magic_;

    template<size_t DefaultBlockSize> friend
    class ChunkedBufferImpl;

    explicit MemBlock(size_t capacity, size_t offset, entity::timestamp ts) :
            bytes_(0),
            capacity_(capacity),
            external_data_(nullptr),
            offset_(offset),
            timestamp_(ts) {
#ifdef DEBUG_BUILD
        memset(data_, 'c', capacity_); // For identifying unwritten-to block portions
#endif
    }

    MemBlock(const uint8_t *data, size_t size, size_t offset, entity::timestamp ts, bool owning) :
            bytes_(size),
            capacity_(size),
            external_data_(const_cast<uint8_t*>(data)),
            offset_(offset),
            timestamp_(ts),
            owns_external_data_(owning) {
    }

    MemBlock(uint8_t *data, size_t size, size_t offset, entity::timestamp ts, bool owning) :
        bytes_(size),
        capacity_(size),
        external_data_(data),
        offset_(offset),
        timestamp_(ts),
        owns_external_data_(owning) {
    }

    [[nodiscard]] bool is_external() const {
        // external_data_ can be nullptr when owns_external_data_ is true
        return external_data_ != nullptr || owns_external_data_;
    }

    ~MemBlock() {
        magic_.check(true);
        if (owns_external_data_) {
            // Previously warn level, but would then show up in a read_batch when some of the returned values are
            // DataError objects if the read is racing with a delete
            log::version().debug("Unexpected release of detachable block memory");
            free_detachable_memory(external_data_, bytes_);
        }
    }

    static constexpr size_t alloc_size(size_t requested_size) noexcept {
        return HeaderSize + requested_size;
    }

    static constexpr size_t raw_size(size_t total_size) noexcept {
        return total_size - HeaderSize;
    }

    void resize(size_t size) {
        arcticdb::util::check_arg(size <= capacity_, "Buffer overflow, size {} is greater than capacity {}", size,
                                 capacity_);
        bytes_ = size;
    }

    [[nodiscard]] size_t bytes() const {
        return bytes_;
    }

    [[nodiscard]] size_t capacity() const {
        return capacity_;
    }

    [[nodiscard]] const uint8_t& operator[](size_t pos) const {
        return data()[pos];
    }

    [[nodiscard]] const uint8_t* internal_ptr(size_t pos) const {
        return &data_[pos];
    }

    void copy_to(uint8_t *target) const {
        memcpy(target, data(), bytes_);
    }

    void copy_from(const uint8_t *src, size_t bytes, size_t pos) {
        arcticdb::util::check_arg(pos + bytes <= capacity_, "Copying more bytes: {} is greater than capacity {}", bytes,
                                 capacity_);
        memcpy(data() + pos, src, bytes);
    }

    uint8_t &operator[](size_t pos) {
        return const_cast<uint8_t *>(data())[pos];
    }

    [[nodiscard]] bool empty() const { return bytes_ == 0; }

    [[nodiscard]] const uint8_t *data() const { return is_external() ? external_data_ : data_; }

    [[nodiscard]] uint8_t *data() { return is_external() ? external_data_ : data_; }

    [[nodiscard]] uint8_t* release() {
        util::check(is_external(), "Cannot release inlined or external data pointer");
        auto* tmp = external_data_;
        external_data_ = nullptr;
        owns_external_data_ = false;
        return tmp;
    }

    void abandon() {
        util::check(is_external(), "Cannot abandon inlined or external data pointer");
        free_detachable_memory(external_data_, bytes_);
        external_data_ = nullptr;
        owns_external_data_ = false;
    }

    [[nodiscard]] uint8_t *end() const { return const_cast<uint8_t*>(&data()[bytes_]); }

    [[nodiscard]] size_t free_space() const {
        arcticdb::util::check(bytes_ <= capacity_, "Block overflow: {} > {}", bytes_, capacity_);
        return capacity_ - bytes_;
    }

    size_t bytes_ = 0UL;
    size_t capacity_= 0UL;
    uint8_t *external_data_ = nullptr;
    size_t offset_ = 0UL;
    entity::timestamp timestamp_ = 0L;
    bool owns_external_data_ = false;

    static const size_t HeaderDataSize =
        sizeof(magic_) +
            sizeof(bytes_) +
            sizeof(capacity_) +
            sizeof(external_data_) +
            sizeof(offset_) +
            sizeof(timestamp_) +
            sizeof(owns_external_data_);

    static const size_t DataAlignment = 64;
    static const size_t PadSize = (DataAlignment - (HeaderDataSize % DataAlignment)) % DataAlignment;

    uint8_t pad[PadSize];
    static const size_t HeaderSize = HeaderDataSize + PadSize;
    static_assert(HeaderSize % DataAlignment == 0, "Header size must be aligned to 64 bytes");

    alignas(DataAlignment) uint8_t data_[MinSize];
};
}
