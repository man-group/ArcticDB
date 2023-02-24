/*
* Copyright 2023 Man Group Operations Ltd.
* NO WARRANTY, EXPRESSED OR IMPLIED
*/

#include <gtest/gtest.h>
#include <arcticdb/column_store/chunked_buffer.hpp>
#include <arcticdb/util/cursored_buffer.hpp>

TEST(ChunkedBuffer, Iterator) {
    using namespace arcticdb;

    auto buff = CursoredBuffer<ChunkedBufferImpl<64>>{};
    for (auto i = 0; i < 10000; ++i) {
        buff.ensure<uint64_t>();
        buff.typed_cursor<uint64_t>() = i;
        buff.commit();
    }

    auto it = buff.buffer().iterator(8);
    uint64_t count = 0;
    while (!it.finished()) {
        ASSERT_EQ(*reinterpret_cast<uint64_t *>(it.value()), count++);
        it.next();
    }

    ASSERT_EQ(count, 10000);
}
TEST(ChunkedBuffer, Split) {
    using namespace arcticdb;
    std::vector<uint8_t> input{1, 0, 0};
    auto chunk_size = 1;
    auto split_size = 2;
    CursoredBuffer<ChunkedBufferImpl<64>> cb;
    auto n = input.size();
    auto top = n - (n % chunk_size);
    for (size_t i = 0; i < top; i += chunk_size) {
        cb.ensure_bytes(chunk_size);
        memcpy(cb.cursor(), &input[i], chunk_size);
        cb.commit();
    }

    auto buffers = ::arcticdb::split(cb.buffer(), split_size);
    auto buf = buffers.begin();
    for (size_t i = 0; i < top; i += chunk_size) {
        const auto where = i % split_size;
        ASSERT_NE(buf, buffers.end());
        ASSERT_EQ(buf->cast<uint8_t>(where), input[i]);
        if ((i + 1) % split_size == 0)
            ++buf;
    }
}

TEST(ChunkedBuffer, RapidCheckRepro) {
    std::vector<uint8_t> input(64);
    using namespace arcticdb;
    auto chunk_size = 5u;
    auto split_size = 13;
    CursoredBuffer<ChunkedBufferImpl<64>> cb;
    auto n = input.size();
    auto top = n - (n % chunk_size);
    for (size_t i = 0; i < top; i += chunk_size) {
        cb.ensure_bytes(chunk_size);
        memcpy(cb.cursor(), &input[i], chunk_size);
        cb.commit();
    }

    auto buffers = ::arcticdb::split(cb.buffer(), split_size);
    auto buf = buffers.begin();
    for (size_t i = 0; i < top; ++i) {
        const auto where = i % split_size;
        ASSERT_NE(buf, buffers.end());
        auto left = buf->cast<uint8_t>(where);
        auto right = input[i];
        auto& buf_obj = *buf;
        if(buf_obj.cast<uint8_t>(where) != input[i])
            std::cout << "Blarf";
        ASSERT_EQ(left, right);
        if(((i + 1) % split_size) == 0)
            ++buf;
    }
}