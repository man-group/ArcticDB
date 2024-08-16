/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the
 * file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source
 * License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/util/magic_num.hpp>
#include <arcticdb/util/preconditions.hpp>

#include <cstdint>

namespace arcticdb {

struct MemBlock {
  static const size_t Align = 128;
  static const size_t DataAlignment = 64;
  static const size_t MinSize = 64;
  using magic_t = arcticdb::util::MagicNum<'M', 'e', 'm', 'b'>;
  magic_t magic_;

  template <size_t DefaultBlockSize> friend class ChunkedBufferImpl;

  explicit MemBlock(size_t capacity, size_t offset, entity::timestamp ts)
      : bytes_(0), capacity_(capacity), external_data_(nullptr), offset_(offset),
        ts_(ts) {
#ifdef DEBUG_BUILD
    memset(data_, 'c', capacity_); // For identifying unwritten-to block portions
#endif
  }

  MemBlock(const uint8_t* data, size_t size, size_t offset, entity::timestamp ts)
      : bytes_(size), capacity_(size), external_data_(data), offset_(offset), ts_(ts) {}

  bool is_external() const { return external_data_ != nullptr; }

  static constexpr size_t alloc_size(size_t requested_size) noexcept {
    return HeaderSize + requested_size;
  }

  static constexpr size_t raw_size(size_t total_size) noexcept {
    return total_size - HeaderSize;
  }

  void resize(size_t size) {
    arcticdb::util::check_arg(size <= capacity_,
                              "Buffer overflow, size {} is greater than capacity {}",
                              size, capacity_);
    bytes_ = size;
  }

  size_t bytes() const { return bytes_; }

  size_t capacity() const { return capacity_; }

  const uint8_t& operator[](size_t pos) const { return data()[pos]; }

  const uint8_t* internal_ptr(size_t pos) const { return &data_[pos]; }

  void copy_to(uint8_t* target) { memcpy(target, data(), bytes_); }

  void copy_from(const uint8_t* src, size_t bytes, size_t pos) {
    arcticdb::util::check_arg(pos + bytes <= capacity_,
                              "Copying more bytes: {} is greater than capacity {}",
                              bytes, capacity_);
    memcpy(data_ + pos, src, bytes);
  }

  uint8_t& operator[](size_t pos) { return const_cast<uint8_t*>(data())[pos]; }

  bool empty() { return bytes_ == 0; }

  const uint8_t* data() const { return is_external() ? external_data_ : data_; }

  uint8_t* end() const { return const_cast<uint8_t*>(&data()[bytes_]); }

  size_t free_space() const {
    arcticdb::util::check(bytes_ <= capacity_, "Block overflow: {} > {}", bytes_,
                          capacity_);
    return capacity_ - bytes_;
  }

  size_t bytes_;
  size_t capacity_;
  const uint8_t* external_data_ = nullptr;
  size_t offset_;
  entity::timestamp ts_;

  static const size_t HeaderDataSize = sizeof(magic_) +    // 8 bytes
                                       sizeof(bytes_) +    // 8 bytes
                                       sizeof(capacity_) + // 8 bytes
                                       sizeof(external_data_) + sizeof(offset_) +
                                       sizeof(ts_); // 8 bytes

  uint8_t pad[Align - HeaderDataSize];
  static const size_t HeaderSize = HeaderDataSize + sizeof(pad);
  static_assert(HeaderSize == Align);
  uint8_t data_[MinSize];
};
} // namespace arcticdb
