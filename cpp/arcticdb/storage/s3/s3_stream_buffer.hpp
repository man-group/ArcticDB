/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/log/log.hpp>
#include <arcticdb/util/buffer.hpp>
#include <arcticdb/util/constructors.hpp>

#ifdef USE_BUFFER_POOL
#include <arcticdb/util/buffer_pool.hpp>
#endif

#include <cstring>
#include <istream>
#include <memory>
#include <streambuf>

namespace arcticdb::storage::s3 {

// TODO Use buffer pool once memory profile and lifetime is well understood
struct S3StreamBuffer : public std::streambuf {
    ARCTICDB_NO_MOVE_OR_COPY(S3StreamBuffer)

    S3StreamBuffer() :
#ifdef USE_BUFFER_POOL
        buffer_(BufferPool::instance()->allocate()) {
#else
        buffer_(std::make_shared<Buffer>()) {
#endif
    }

    std::shared_ptr<Buffer> buffer_;
    size_t pos_ = 0;

    std::shared_ptr<Buffer> get_buffer() {
        buffer_->set_bytes(pos_);
        return buffer_;
    }

  protected:
    std::streamsize xsputn(const char_type* s, std::streamsize n) override {
        ARCTICDB_TRACE(log::version(), "xsputn {} pos at {}, {} bytes", uintptr_t(buffer_.get()), pos_, n);
        if (buffer_->bytes() < pos_ + n) {
            ARCTICDB_TRACE(log::version(), "{} Calling ensure for {}", uintptr_t(buffer_.get()), (pos_ + n) * 2);
            buffer_->ensure((pos_ + n) * 2);
        }

        auto target = buffer_->ptr_cast<char_type>(pos_, n);
        ARCTICDB_TRACE(log::version(), "Putting {} bytes at {}", n, uintptr_t(target));
        memcpy(target, s, n);
        pos_ += n;
        ARCTICDB_TRACE(log::version(), "{} pos is now {}, returning {}", uintptr_t(buffer_.get()), pos_, n);
        return n;
    }

    int_type overflow(int_type ch) override { return xsputn(reinterpret_cast<char*>(&ch), 1); }
};

struct S3IOStream : public std::iostream {
    S3StreamBuffer stream_buf_;

    S3IOStream() : std::iostream(&stream_buf_) {}

    std::shared_ptr<Buffer> get_buffer() { return stream_buf_.get_buffer(); }
};

} // namespace arcticdb::storage::s3
