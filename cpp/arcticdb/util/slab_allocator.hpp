/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/util/preconditions.hpp>
#include <arcticdb/util/configs_map.hpp>

#include <folly/Function.h>

#include <atomic>

namespace arcticdb {

static const double slab_activate_cb_cutoff =
        ConfigsMap::instance()->get_double("Allocator.SlabActivateCallbackCutoff", 0.1);
static const double slab_deactivate_cb_cutoff =
        ConfigsMap::instance()->get_double("Allocator.SlabDeactivateCallbackCutoff", 0.2);

template<typename T, std::size_t cache_line_size = 128>
class SlabAllocator {
  private:
    template<typename Value, typename Tag>
    struct tagged_value {
        // We have tag for every value to counter https://en.wikipedia.org/wiki/ABA_problem of lock free implementation
        Value value;
        Tag tag;
    };

  public:
    using value_type = T;
    using pointer = value_type*;
    using size_type = std::size_t;

  private:
    // We store the offset of the next free block inside the blocks itself
    // Block size should be max of the value_type (used for actual storing the value) and size_type (storing offset)
    using block_t = std::conditional_t<sizeof(value_type) < sizeof(size_type), size_type, value_type>;

    using tagged_value_t = tagged_value<size_type, size_type>;

  public:
    explicit SlabAllocator(size_type capacity) :
        capacity_(capacity),
        main_memory_(new block_t[capacity_]),
        num_free_blocks_(capacity),
        next_free_offset_({0, 0}),
        cb_activated_(false) {
        size_type i = 0;
        for (block_t* p = main_memory_; i < capacity; ++p) {
            // We init each block with the value pointing to next block as the free block
            *reinterpret_cast<size_type*>(p) = ++i;
        }
    };
    ~SlabAllocator() { delete[] main_memory_; };

    pointer allocate() noexcept {
        manage_slab_capacity();
        pointer p_block = nullptr;
        tagged_value_t curr_next_free_offset = next_free_offset_.load();
        tagged_value_t new_next_free_offset;
#ifdef LOG_SLAB_ALLOC_INTERNALS
        // The LOG_SLAB_ALLOC_INTERNALS can be used to check for contention in getting the next free block by many
        // threads.
        auto allocation_attempts = 0u;
#endif
        do {
#ifdef LOG_SLAB_ALLOC_INTERNALS
            ++allocation_attempts;
#endif
            // increase the id tag to prevent the ABA problem
            new_next_free_offset.tag = curr_next_free_offset.tag + 1;

            p_block = reinterpret_cast<pointer>(main_memory_ + curr_next_free_offset.value);

            // the next free block is written in the first size_type bytes of p block
            new_next_free_offset.value = *reinterpret_cast<size_type*>(p_block);
        } while (!next_free_offset_.compare_exchange_strong(curr_next_free_offset, new_next_free_offset));
#ifdef LOG_SLAB_ALLOC_INTERNALS
        if (allocation_attempts > 10) {
            // We only print when we encounter a lot of allocation_attempts because otherwise we remove the contention
            // effect by effectively pausing every time to print.
            std::cout << "Many allocation attempts: " << allocation_attempts << "\n";
        }
#endif
        return p_block;
    };

    void deallocate(pointer p) noexcept {
        util::check(p != nullptr, "Received nullptr in SlabAllocator::deallocate");
        if (!is_addr_in_slab(p))
            return;
        tagged_value_t curr_next_free_offset = next_free_offset_.load();
        tagged_value_t new_next_free_offset;
        do {
            // calculate the offset of this based on argument p
            new_next_free_offset.value = static_cast<size_type>(reinterpret_cast<block_t*>(p) - main_memory_);
            // increase the id tag to prevent the ABA problem
            new_next_free_offset.tag = curr_next_free_offset.tag + 1;

            // set the next free offset inside p from the current next_free_offset_
            *reinterpret_cast<size_type*>(p) = curr_next_free_offset.value;
        } while (!next_free_offset_.compare_exchange_strong(curr_next_free_offset, new_next_free_offset));

        num_free_blocks_.fetch_add(1);
    };

    bool is_addr_in_slab(pointer p) noexcept {
        auto min_ptr = reinterpret_cast<pointer>(main_memory_);
        auto max_ptr = reinterpret_cast<pointer>(main_memory_ + capacity_ - 1);
        return min_ptr <= p && p <= max_ptr;
    }

    size_t add_cb_when_full(folly::Function<void()>&& func) {
        std::scoped_lock<std::mutex> lock(mutex_);
        memory_full_cbs_.emplace_back(std::move(func), true);
        return memory_full_cbs_.size() - 1;
    }

    void remove_cb_when_full(size_t id) {
        std::scoped_lock<std::mutex> lock(mutex_);
        memory_full_cbs_[id].second = false;
    }

    size_t get_approx_free_blocks() { return num_free_blocks_.load(); }

    bool _get_cb_activated() { return cb_activated_.load(); }

  private:
    size_type try_decrease_available_blocks() noexcept {
        size_type n = num_free_blocks_.load();
        do {
            if (n == 0) {
                // true: there are no available blocks -> return false
                return 0;
            }
        } while (!num_free_blocks_.compare_exchange_strong(n, n - 1));

        return n;
    }

    void manage_slab_capacity() {
        size_type n = try_decrease_available_blocks();
        if (!n) {
            util::raise_rte("Out of memory in slab allocator, callbacks not freeing memory?");
        }
        if (n / (float)capacity_ <= slab_activate_cb_cutoff) {
            // trigger callbacks to free space
            if (try_changing_cb(true)) {
                ARCTICDB_TRACE(
                        log::inmem(), "Memory reached cutoff, calling callbacks in slab allocator to free up memory"
                );
                std::scoped_lock<std::mutex> lock(mutex_);
                for (auto& cb : memory_full_cbs_) {
                    if (cb.second)
                        (cb.first)();
                }
            }
        }
        if (n / (float)capacity_ >= slab_deactivate_cb_cutoff)
            try_changing_cb(false);
    }

    bool try_changing_cb(bool activate) {
        bool curr = cb_activated_.load();
        do {
            if (curr == activate) {
                // this value already changed by someone else
                return false;
            }

        } while (!cb_activated_.compare_exchange_strong(curr, activate));
        return true;
    }

  private:
    size_type capacity_;
    block_t* main_memory_;
    // The allocator is lock-free, this mutex is just used to add callbacks when full
    std::mutex mutex_;
    std::vector<std::pair<folly::Function<void()>, bool>> memory_full_cbs_;
    alignas(cache_line_size) std::atomic<size_type> num_free_blocks_;
    alignas(cache_line_size) std::atomic<tagged_value_t> next_free_offset_;
    alignas(cache_line_size) std::atomic<bool> cb_activated_;
};
} // namespace arcticdb