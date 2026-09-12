/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/util/buffer.hpp>
#include <arcticdb/util/variant.hpp>

#include <memory>
#include <variant>

namespace arcticdb {

class VariantBuffer {
    using VariantType = std::variant<std::monostate, std::shared_ptr<Buffer>, BufferView>;

    VariantType buffer_;

  public:
    VariantBuffer() = default;

    template<typename BufferType>
    VariantBuffer(BufferType&& buf) : buffer_(std::forward<decltype(buf)>(buf)) {}

    [[nodiscard]] VariantBuffer clone() const {
        return util::variant_match(
                buffer_,
                [](const BufferView& bv) {
                    auto b = std::make_shared<Buffer>();
                    bv.copy_to(*b);
                    return VariantBuffer{std::move(b)};
                },
                [](const std::shared_ptr<Buffer>& buf) {
                    return VariantBuffer{std::make_shared<Buffer>(buf->clone())};
                },
                [](const std::monostate) -> VariantBuffer { util::raise_rte("Uninitialized buffer"); }
        );
    }

    template<typename BufferType>
    VariantBuffer& operator=(BufferType&& buf) {
        buffer_ = std::forward<decltype(buf)>(buf);
        return *this;
    }

    [[nodiscard]] const std::shared_ptr<Buffer>& get_owning_buffer() const {
        return std::get<std::shared_ptr<Buffer>>(buffer_);
    }

    uint8_t* data() {
        return util::variant_match(
                buffer_,
                [](BufferView& bv) { return bv.data(); },
                [](const std::shared_ptr<Buffer>& buf) { return buf->data(); },
                [](const std::monostate) -> uint8_t* { util::raise_rte("Uninitialized buffer"); }
        );
    }

    const uint8_t* data() const {
        return util::variant_match(
                buffer_,
                [](const BufferView& bv) { return bv.data(); },
                [](const std::shared_ptr<Buffer>& buf) -> const uint8_t* { return buf->data(); },
                [](const std::monostate) -> const uint8_t* { util::raise_rte("Uninitialized buffer"); }
        );
    }

    [[nodiscard]] size_t preamble_bytes() const {
        if (std::holds_alternative<std::shared_ptr<Buffer>>(buffer_)) {
            return std::get<std::shared_ptr<Buffer>>(buffer_)->preamble_bytes();
        } else {
            return 0U;
        }
    }

    [[nodiscard]] BufferView view() const {
        return util::variant_match(
                buffer_,
                [](const std::monostate&) -> BufferView {
                    util::raise_rte("Underlying buffer in view() is unexpectedly monostate");
                },
                [](const BufferView& b) { return b; },
                [](const std::shared_ptr<Buffer> b) { return b->view(); }
        );
    }

    [[nodiscard]] std::size_t bytes() const {
        std::size_t s = 0;
        util::variant_match(
                buffer_,
                [](const std::monostate&) { /* Uninitialized buffer */ },
                [&s](const BufferView& b) { s = b.bytes(); },
                [&s](const std::shared_ptr<Buffer>& b) { s = b->bytes(); }
        );

        return s;
    }

    [[nodiscard]] bool is_uninitialized() const { return std::holds_alternative<std::monostate>(buffer_); }

    void move_buffer(VariantBuffer&& that) {
        if (is_uninitialized() || that.is_uninitialized()) {
            std::swap(buffer_, that.buffer_);
        } else if (!(is_owning_buffer() ^ that.is_owning_buffer())) {
            if (is_owning_buffer()) {
                swap(*std::get<std::shared_ptr<Buffer>>(buffer_), *std::get<std::shared_ptr<Buffer>>(that.buffer_));
            } else {
                swap(std::get<BufferView>(buffer_), std::get<BufferView>(that.buffer_));
            }
        } else if (is_owning_buffer()) {
            log::storage().info("Copying segment");
            // data of segment being moved is not owned, moving it is dangerous, copying instead
            std::get<BufferView>(that.buffer_).copy_to(*std::get<std::shared_ptr<Buffer>>(buffer_));
        } else {
            // data of this segment is a view, but the move data is moved
            buffer_ = std::move(std::get<std::shared_ptr<Buffer>>(that.buffer_));
        }
    }

    [[nodiscard]] bool is_owning_buffer() const { return std::holds_alternative<std::shared_ptr<Buffer>>(buffer_); }

    void force_own_buffer() {
        if (!is_owning_buffer()) {
            auto b = std::make_shared<Buffer>();
            std::get<BufferView>(buffer_).copy_to(*b);
            buffer_ = std::move(b);
        }
    }
};

}
