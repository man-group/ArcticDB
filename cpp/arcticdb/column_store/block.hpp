/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/util/preconditions.hpp>
#include <arcticdb/util/magic_num.hpp>
#include <entity/types.hpp>

#include <cstdint>

namespace arcticdb {

enum class MemBlockType { DYNAMIC, EXTERNAL_WITH_EXTRA_BYTES, EXTERNAL_PACKED };

bool is_external(MemBlockType type);

class IMemBlock {
  public:
    // Abstract methods
    [[nodiscard]] virtual MemBlockType get_type() const = 0;
    [[nodiscard]] virtual size_t physical_bytes() const = 0;
    [[nodiscard]] virtual size_t logical_size() const = 0;
    [[nodiscard]] virtual size_t capacity() const = 0;
    [[nodiscard]] virtual size_t offset() const = 0;
    [[nodiscard]] virtual entity::timestamp timestamp() const = 0;
    [[nodiscard]] virtual const uint8_t* data() const = 0;
    [[nodiscard]] virtual uint8_t* data() = 0;

    virtual ~IMemBlock() = default;

    // The below methods make sense only for specific memory block types. Decided to put them in the interface for
    // performance reasons (i.e. calling them would be a single vtable lookup vs using `get_type` + `dynamic_cast`).
    // Dynamic block specific methods
    virtual void resize(size_t bytes) = 0;
    virtual void check_magic() const = 0;
    // External block specific methods
    [[nodiscard]] virtual uint8_t* release() = 0;
    virtual void abandon() = 0;

    // Implemented methods
    [[nodiscard]] bool empty() const;
    [[nodiscard]] size_t free_space() const;
    void add_bytes(size_t bytes);
    void copy_to(uint8_t* target) const;
    void copy_from(const uint8_t* src, size_t bytes, size_t pos);
    [[nodiscard]] virtual uint8_t& operator[](size_t pos);
    [[nodiscard]] const uint8_t* ptr(size_t pos) const;
    [[nodiscard]] uint8_t* ptr(size_t pos);
    [[nodiscard]] uint8_t* end() const;
};

// DynamicMemBlock stores inline memory of a certain capacity. It allows resizing within the predefined capacity.
class DynamicMemBlock : public IMemBlock {
  public:
    static const size_t Align = 128;
    static const size_t MinSize = 64;
    using magic_t = arcticdb::util::MagicNum<'M', 'e', 'm', 'b'>;
    magic_t magic_;

    template<size_t DefaultBlockSize>
    friend class ChunkedBufferImpl;

    explicit DynamicMemBlock(size_t capacity, size_t offset, entity::timestamp ts);
    ~DynamicMemBlock();

    MemBlockType get_type() const override;

    static constexpr size_t alloc_size(size_t requested_size) noexcept { return HeaderSize + requested_size; }

    static constexpr size_t raw_size(size_t total_size) noexcept { return total_size - HeaderSize; }

    [[nodiscard]] size_t physical_bytes() const override;
    [[nodiscard]] size_t logical_size() const override;
    [[nodiscard]] size_t capacity() const override;
    [[nodiscard]] size_t offset() const override;
    [[nodiscard]] entity::timestamp timestamp() const override;
    [[nodiscard]] const uint8_t* data() const override;
    [[nodiscard]] uint8_t* data() override;
    void resize(size_t size) override;
    void check_magic() const override;
    [[nodiscard]] uint8_t* release() override;
    void abandon() override;

    size_t bytes_ = 0UL;
    size_t capacity_ = 0UL;
    size_t offset_ = 0UL;
    entity::timestamp timestamp_ = 0L;

    static const size_t HeaderDataSize = sizeof(void*) +     // 8 bytes for vptr
                                         sizeof(magic_) +    // 8 bytes
                                         sizeof(bytes_) +    // 8 bytes
                                         sizeof(capacity_) + // 8 bytes
                                         sizeof(offset_) + sizeof(timestamp_);

    uint8_t pad[Align - HeaderDataSize];
    static const size_t HeaderSize = HeaderDataSize + sizeof(pad);
    static_assert(HeaderSize == Align);
    uint8_t data_[MinSize];
};

// ExternalMemBlock stores external memory of predefined size. It does not allow resizing.
// It allows storing `extra_bytes` for arrow string types.
class ExternalMemBlock : public IMemBlock {
  public:
    ExternalMemBlock(
            const uint8_t* data, size_t size, size_t offset, entity::timestamp ts, bool owning, size_t extra_bytes = 0
    );
    ExternalMemBlock(
            uint8_t* data, size_t size, size_t offset, entity::timestamp ts, bool owning, size_t extra_bytes = 0
    );
    ~ExternalMemBlock();

    MemBlockType get_type() const override;
    [[nodiscard]] uint8_t* release() override;
    void abandon() override;
    [[nodiscard]] size_t physical_bytes() const override;
    [[nodiscard]] size_t logical_size() const override;
    [[nodiscard]] size_t capacity() const override;
    [[nodiscard]] size_t offset() const override;
    [[nodiscard]] entity::timestamp timestamp() const override;
    [[nodiscard]] const uint8_t* data() const override;
    [[nodiscard]] uint8_t* data() override;
    void resize(size_t bytes) override;
    void check_magic() const override;

  private:
    size_t bytes_ = 0UL;
    size_t offset_ = 0UL;
    entity::timestamp timestamp_ = 0UL;
    uint8_t* external_data_ = nullptr;
    bool owns_external_data_ = false;
    size_t extra_bytes_ = 0UL;
};

// ExternalMemBlock stores external memory of predefined size. It does not allow resizing.
// It stores packed bits and does not allow any slicing. It is used for arrow packed bool types.
class ExternalPackedMemBlock : public ExternalMemBlock {
  public:
    ExternalPackedMemBlock(
            const uint8_t* data, size_t size, size_t shift, size_t offset, entity::timestamp ts, bool owning
    );
    ExternalPackedMemBlock(uint8_t* data, size_t size, size_t shift, size_t offset, entity::timestamp ts, bool owning);

    MemBlockType get_type() const override;
    [[nodiscard]] size_t logical_size() const override;
    [[nodiscard]] uint8_t& operator[](size_t pos) override;
    [[nodiscard]] size_t shift() const;

  private:
    size_t logical_size_ = 0UL;
    size_t shift_ = 0UL;
};
} // namespace arcticdb
