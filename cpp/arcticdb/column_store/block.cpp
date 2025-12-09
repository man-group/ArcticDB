/* Copyright 2025 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/column_store/block.hpp>
#include <arcticdb/util/allocator.hpp>

namespace arcticdb {

bool is_external(MemBlockType type) { return type != MemBlockType::DYNAMIC; }

// IMemBlock implementation
bool IMemBlock::empty() const { return physical_bytes() == 0; }

size_t IMemBlock::free_space() const {
    util::check(physical_bytes() <= capacity(), "Block overflow: {} > {}", physical_bytes(), capacity());
    return capacity() - physical_bytes();
}

void IMemBlock::add_bytes(size_t bytes) { resize(physical_bytes() + bytes); }

void IMemBlock::copy_to(uint8_t* target) const { memcpy(target, data(), physical_bytes()); }

void IMemBlock::copy_from(const uint8_t* src, size_t bytes, size_t pos) {
    util::check_arg(
            pos + bytes <= physical_bytes(),
            "Copying more bytes: {} is greater than available bytes {}",
            bytes,
            physical_bytes()
    );
    memcpy(ptr(pos), src, bytes);
}

const uint8_t* IMemBlock::ptr(size_t pos) const { return data() + pos; }

uint8_t* IMemBlock::ptr(size_t pos) { return data() + pos; }

uint8_t* IMemBlock::end() const { return const_cast<uint8_t*>(ptr(physical_bytes())); }

// DynamicMemBlock implementation
DynamicMemBlock::DynamicMemBlock(size_t capacity, size_t offset, entity::timestamp ts) :
    bytes_(0),
    capacity_(capacity),
    offset_(offset),
    timestamp_(ts) {
#ifdef DEBUG_BUILD
    memset(data_, 'c', capacity_); // For identifying unwritten-to block portions
#endif
}

MemBlockType DynamicMemBlock::get_type() const { return MemBlockType::DYNAMIC; }

size_t DynamicMemBlock::physical_bytes() const { return bytes_; }

size_t DynamicMemBlock::logical_size() const { return bytes_; }

size_t DynamicMemBlock::capacity() const { return capacity_; }

size_t DynamicMemBlock::offset() const { return offset_; }

entity::timestamp DynamicMemBlock::timestamp() const { return timestamp_; }

const uint8_t* DynamicMemBlock::data() const { return data_; }

uint8_t* DynamicMemBlock::data() { return data_; }

void DynamicMemBlock::resize(size_t size) {
    arcticdb::util::check_arg(
            size <= capacity_, "Buffer overflow, size {} is greater than capacity {}", size, capacity_
    );
    bytes_ = size;
}

uint8_t* DynamicMemBlock::release() { util::raise_rte("Can't release a dynamic block"); }

void DynamicMemBlock::abandon() { util::raise_rte("Can't abandon a dynamic block"); }

// ExternalMemBlock implementation
ExternalMemBlock::ExternalMemBlock(
        const uint8_t* data, size_t logical_size, size_t offset, entity::timestamp ts, bool owning, size_t extra_bytes
) :
    bytes_(logical_size),
    offset_(offset),
    timestamp_(ts),
    external_data_(const_cast<uint8_t*>(data)),
    owns_external_data_(owning),
    extra_bytes_(extra_bytes) {}

ExternalMemBlock::~ExternalMemBlock() {
    if (owns_external_data_) {
        // Previously warn level, but would then show up in a read_batch when some of the returned values are
        // DataError objects if the read is racing with a delete
        log::version().debug("Unexpected release of detachable block memory");
        free_detachable_memory(external_data_, physical_bytes());
    }
}

MemBlockType ExternalMemBlock::get_type() const { return MemBlockType::EXTERNAL_WITH_EXTRA_BYTES; }

uint8_t* ExternalMemBlock::release() {
    auto* tmp = external_data_;
    external_data_ = nullptr;
    owns_external_data_ = false;
    return tmp;
}

void ExternalMemBlock::abandon() {
    if (owns_external_data_) {
        free_detachable_memory(external_data_, physical_bytes());
        external_data_ = nullptr;
        owns_external_data_ = false;
    }
}

size_t ExternalMemBlock::physical_bytes() const { return bytes_ + extra_bytes_; }

size_t ExternalMemBlock::logical_size() const { return bytes_; }

size_t ExternalMemBlock::capacity() const { return physical_bytes(); }

size_t ExternalMemBlock::offset() const { return offset_; }

entity::timestamp ExternalMemBlock::timestamp() const { return timestamp_; }

const uint8_t* ExternalMemBlock::data() const { return external_data_; }

uint8_t* ExternalMemBlock::data() { return external_data_; }

void ExternalMemBlock::resize(size_t bytes) { util::raise_rte("Can't resize a non dynamic block. Bytes: {}", bytes); }

// ExternalPackedMemBlock implementation
ExternalPackedMemBlock::ExternalPackedMemBlock(
        const uint8_t* data, size_t logical_size, size_t shift, size_t offset, entity::timestamp ts, bool owning
) :
    ExternalMemBlock(data, (logical_size + shift - 1) / 8 + 1, offset, ts, owning),
    logical_size_(logical_size),
    shift_(shift) {}

MemBlockType ExternalPackedMemBlock::get_type() const { return MemBlockType::EXTERNAL_PACKED; }

size_t ExternalPackedMemBlock::logical_size() const { return logical_size_; }

uint8_t* ExternalPackedMemBlock::ptr(size_t pos) {
    util::raise_rte("Accessing position {} for a packed mem block is not supported", pos);
}
const uint8_t* ExternalPackedMemBlock::ptr(size_t pos) const {
    util::raise_rte("Accessing position {} for a packed mem block is not supported", pos);
}

size_t ExternalPackedMemBlock::shift() const { return shift_; }

} // namespace arcticdb
