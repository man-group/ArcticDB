/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/util/preconditions.hpp>
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
    // External block specific methods
    [[nodiscard]] virtual uint8_t* release() = 0;
    virtual void abandon() = 0;

    // Implemented methods
    [[nodiscard]] bool empty() const;
    [[nodiscard]] size_t free_space() const;
    void add_bytes(size_t bytes);
    void copy_to(uint8_t* target) const;
    void copy_from(const uint8_t* src, size_t bytes, size_t pos);
    // virtual to allow ExternalPackedBuffer to override to throw
    [[nodiscard]] virtual const uint8_t* ptr(size_t pos) const;
    [[nodiscard]] virtual uint8_t* ptr(size_t pos);
    [[nodiscard]] uint8_t* end() const;
};

// DynamicMemBlock stores inline memory of a certain capacity. It allows resizing within the predefined capacity.
class DynamicMemBlock : public IMemBlock {
  public:
    static const size_t Align = 128;
    static const size_t MinSize = 64;

    template<size_t DefaultBlockSize>
    friend class ChunkedBufferImpl;

    explicit DynamicMemBlock(size_t capacity, size_t offset, entity::timestamp ts);
    ~DynamicMemBlock() = default;

    MemBlockType get_type() const final;

    static constexpr size_t alloc_size(size_t requested_size) noexcept { return HeaderSize + requested_size; }

    static constexpr size_t raw_size(size_t total_size) noexcept { return total_size - HeaderSize; }

    [[nodiscard]] size_t physical_bytes() const final;
    [[nodiscard]] size_t logical_size() const final;
    [[nodiscard]] size_t capacity() const final;
    [[nodiscard]] size_t offset() const final;
    [[nodiscard]] entity::timestamp timestamp() const final;
    [[nodiscard]] const uint8_t* data() const final;
    [[nodiscard]] uint8_t* data() final;
    void resize(size_t size) final;
    [[nodiscard]] uint8_t* release() final;
    void abandon() final;

    size_t bytes_ = 0UL;
    size_t capacity_ = 0UL;
    size_t offset_ = 0UL;
    entity::timestamp timestamp_ = 0L;

    static const size_t HeaderDataSize = sizeof(void*) +     // 8 bytes for vptr
                                         sizeof(bytes_) +    // 8 bytes
                                         sizeof(capacity_) + // 8 bytes
                                         sizeof(offset_) + sizeof(timestamp_);

    uint8_t pad[Align - HeaderDataSize];
    static const size_t HeaderSize = HeaderDataSize + sizeof(pad);
    static_assert(HeaderSize == Align);
    uint8_t data_[MinSize];
};
static_assert(sizeof(DynamicMemBlock) == DynamicMemBlock::Align + DynamicMemBlock::MinSize);

// ExternalMemBlock stores external memory of predefined size. It does not allow resizing.
// It allows storing `extra_bytes` for arrow string types.
class ExternalMemBlock : public IMemBlock {
  public:
    ExternalMemBlock(
            const uint8_t* data, size_t logical_size, size_t offset, entity::timestamp ts, bool owning,
            size_t extra_bytes = 0
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
            const uint8_t* data, size_t logical_size, size_t shift, size_t offset, entity::timestamp ts, bool owning
    );

    MemBlockType get_type() const final;
    [[nodiscard]] size_t logical_size() const final;
    [[nodiscard]] const uint8_t* ptr(size_t pos) const final;
    [[nodiscard]] uint8_t* ptr(size_t pos) final;
    [[nodiscard]] size_t shift() const;

  private:
    size_t logical_size_ = 0UL;
    // shift_ refers to the bit shift to the first bit in memory.
    // Arrow spec allows having a packed bit buffer which does not start at a byte boundary. E.g. We could have 10 bits
    // stored in 2 bytes with a shift_=2. So the 2 bytes would look like:
    // xx012345 6789xxxx
    // where a number i represents the i-th bit in the buffer and x marks unused bits.
    size_t shift_ = 0UL;
};
} // namespace arcticdb

namespace fmt {
template<>
struct formatter<arcticdb::MemBlockType> {
    template<typename ParseContext>
    constexpr auto parse(ParseContext& ctx) {
        return ctx.begin();
    }

    template<typename FormatContext>
    auto format(arcticdb::MemBlockType t, FormatContext& ctx) const {
        switch (t) {
        case arcticdb::MemBlockType::DYNAMIC:
            return fmt::format_to(ctx.out(), "DYNAMIC");
        case arcticdb::MemBlockType::EXTERNAL_WITH_EXTRA_BYTES:
            return fmt::format_to(ctx.out(), "EXTERNAL_WITH_EXTRA_BYTES");
        case arcticdb::MemBlockType::EXTERNAL_PACKED:
            return fmt::format_to(ctx.out(), "EXTERNAL_PACKED");
        default:
            arcticdb::util::raise_rte("Unrecognized to load {}", static_cast<int8_t>(t));
        }
    }
};
} // namespace fmt
