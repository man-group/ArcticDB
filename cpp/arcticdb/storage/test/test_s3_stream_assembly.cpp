/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

// Stress test for the S3 async-read buffer-assembly + decode path (Idea A).
//
// Production sees intermittent lz4 decode failures on large index segments, only on the async S3 read
// path, with the on-disk bytes intact on re-read -> transient, length-preserving, in-memory corruption
// of the compressed input buffer. This test feeds a known large lz4-encoded segment through the REAL
// S3IOStream / S3StreamBuffer assembler (the code the AWS SDK writes the HTTP body into) in libcurl-
// sized chunks, then byte-compares the assembled buffer to the source and decodes it, under heavy
// concurrency and with malloc_trim(0) firing concurrently. It supplies correct bytes, so it cannot
// reproduce an SDK/curl transfer bug -- it isolates whether arcticdb's own assembly / Buffer / Allocator
// can corrupt under load. A pass exonerates that layer; a failure is a deterministic arcticdb-side repro.

#include <gtest/gtest.h>

#include <arcticdb/storage/s3/s3_stream_buffer.hpp>

#include <arcticdb/codec/codec.hpp>
#include <arcticdb/codec/default_codecs.hpp>
#include <arcticdb/codec/segment.hpp>
#include <arcticdb/column_store/memory_segment.hpp>
#include <arcticdb/entity/stream_descriptor.hpp>
#include <arcticdb/entity/types.hpp>
#include <arcticdb/stream/index.hpp>
#include <arcticdb/util/buffer.hpp>

#include <fmt/format.h>

#include <atomic>
#include <cstdlib>
#include <mutex>
#include <random>
#include <string>
#include <thread>
#include <vector>

#if defined(__linux__) && defined(__GLIBC__)
#include <malloc.h>
#endif

using namespace arcticdb;
using arcticdb::storage::s3::S3IOStream;

namespace {

size_t env_size(const char* name, size_t def) {
    const char* v = std::getenv(name);
    return v ? static_cast<size_t>(std::strtoull(v, nullptr, 10)) : def;
}

constexpr size_t kNumDataCols = 11; // mimic the real index schema (~11 mostly-int64 columns)

struct RefObject {
    std::vector<uint8_t> bytes; // exact on-disk serialized segment bytes
};

// Build a large lz4-encoded segment resembling the production index: a timestamp index plus int64
// columns, half sequential (compressible, like version_id) and half random (incompressible, like
// content_hash), so lz4 produces realistic multi-block output of roughly target_mb.
RefObject build_reference(size_t target_mb) {
    const size_t approx_raw = target_mb * 2 * 1024 * 1024;
    const size_t rows = std::max<size_t>(1024, approx_raw / ((kNumDataCols + 1) * sizeof(int64_t)));

    std::vector<FieldRef> fields;
    for (size_t c = 0; c < kNumDataCols; ++c)
        fields.push_back(scalar_field(DataType::INT64, fmt::format("col_{}", c)));
    const auto desc =
            stream_descriptor_from_range(StreamId{"repro"}, stream::TimeseriesIndex::default_index(), fields);

    SegmentInMemory seg(desc);
    std::mt19937_64 rng(12345);
    for (size_t r = 0; r < rows; ++r) {
        seg.set_scalar(0, static_cast<int64_t>(r)); // timestamp index column
        for (size_t c = 0; c < kNumDataCols; ++c) {
            const int64_t v = (c % 2 == 0) ? static_cast<int64_t>(r) : static_cast<int64_t>(rng());
            seg.set_scalar(static_cast<position_t>(c + 1), v);
        }
        seg.end_row();
    }

    Segment enc = encode_dispatch(seg.clone(), codec::default_lz4_codec(), EncodingVersion::V2);
    const size_t total = enc.calculate_size();
    RefObject ref;
    ref.bytes.resize(total);
    enc.write_to(ref.bytes.data());
    fmt::print(stderr, "[repro] reference object: {} rows, {} data cols, encoded {} bytes ({:.1f} MB)\n", rows,
            kNumDataCols, total, double(total) / (1024.0 * 1024.0));
    return ref;
}

// One simulated async GET: feed ref bytes through the real S3IOStream in libcurl-sized chunks (with
// jitter to hit growth/boundary cases), then verify. Returns empty on success, else a failure string.
std::string assemble_and_check(const RefObject& ref, std::mt19937_64& rng, bool do_decode) {
    storage::s3::S3IOStream stream;
    const uint8_t* src = ref.bytes.data();
    const size_t n = ref.bytes.size();
    std::uniform_int_distribution<size_t> jitter(1, 4096);
    size_t off = 0;
    while (off < n) {
        size_t chunk = std::min(16u * 1024u + jitter(rng), n - off);
        stream.write(reinterpret_cast<const char*>(src + off), static_cast<std::streamsize>(chunk));
        off += chunk;
    }
    auto buf = stream.get_buffer();

    // Check 1: assembly fidelity. Length-preserving corruption of the compressed bytes shows up here.
    if (buf->bytes() != n)
        return fmt::format("SIZE mismatch: assembled {} expected {}", buf->bytes(), n);
    const uint8_t* a = buf->data();
    for (size_t i = 0; i < n; ++i) {
        if (a[i] != src[i])
            return fmt::format("BYTE MISMATCH at {}/{}: got 0x{:02x} expected 0x{:02x}", i, n, a[i], src[i]);
    }

    if (do_decode) {
        // Check 2: decode fidelity. The production symptom is an lz4 failure thrown right here.
        // (Byte identity above already guarantees the decoded values match a clean read; this catches
        // any decode-side concurrency fault and reproduces the exact lz4 throw if bytes were mangled.)
        try {
            Segment seg = Segment::from_buffer(buf);
            SegmentInMemory decoded = decode_segment(seg);
            if (decoded.num_columns() != kNumDataCols + 1)
                return fmt::format("COL mismatch: {} vs {}", decoded.num_columns(), kNumDataCols + 1);
        } catch (const std::exception& e) {
            return fmt::format("DECODE THREW: {}", e.what());
        }
    }
    return {};
}

void run_concurrent(const RefObject& ref, bool do_decode, size_t& out_failures, std::string& out_first) {
    const size_t threads = env_size("ADB_REPRO_THREADS", std::max<size_t>(2, std::thread::hardware_concurrency() * 2));
    const size_t iters = env_size("ADB_REPRO_ITERS", 100);
    std::atomic<size_t> failures{0};
    std::mutex msg_mtx;
    std::string first_msg;
    std::vector<std::thread> pool;
    for (size_t t = 0; t < threads; ++t) {
        pool.emplace_back([&, t]() {
            std::mt19937_64 rng(1000 + t);
            for (size_t i = 0; i < iters; ++i) {
                const auto err = assemble_and_check(ref, rng, do_decode);
                if (!err.empty() && failures.fetch_add(1) == 0) {
                    std::lock_guard<std::mutex> g(msg_mtx);
                    first_msg = fmt::format("thread {} iter {}: {}", t, i, err);
                }
            }
        });
    }
    for (auto& th : pool)
        th.join();
    out_failures = failures.load();
    out_first = first_msg;
}

} // namespace

class S3StreamAssemblyTest : public ::testing::Test {
  protected:
    static RefObject ref_;
    static void SetUpTestSuite() { ref_ = build_reference(env_size("ADB_REPRO_MB", 16)); }
};
RefObject S3StreamAssemblyTest::ref_;

// Single-threaded, many chunk-size patterns: catches deterministic growth/boundary bugs in xsputn.
TEST_F(S3StreamAssemblyTest, SingleThreadManyChunkPatterns) {
    std::mt19937_64 rng(1);
    const size_t iters = env_size("ADB_REPRO_ITERS", 50);
    for (size_t i = 0; i < iters; ++i) {
        const auto err = assemble_and_check(ref_, rng, /*do_decode=*/true);
        ASSERT_TRUE(err.empty()) << "iter " << i << ": " << err;
    }
}

// Many threads each assembling+decoding their own copy concurrently: catches Buffer/Allocator
// interference under the parallel large-allocation load the prune/read workload generates.
TEST_F(S3StreamAssemblyTest, ConcurrentAssembly) {
    size_t failures = 0;
    std::string first;
    run_concurrent(ref_, /*do_decode=*/true, failures, first);
    ASSERT_EQ(failures, 0u) << "first failure: " << first;
}

// Same, but with malloc_trim(0) hammered from a background thread throughout -- the untested hypothesis
// that trimming the arena concurrently with large reallocs corrupts an in-flight buffer.
TEST_F(S3StreamAssemblyTest, ConcurrentAssemblyWithConcurrentTrim) {
#if defined(__linux__) && defined(__GLIBC__)
    std::atomic<bool> stop{false};
    std::thread trimmer([&]() {
        while (!stop.load(std::memory_order_relaxed))
            malloc_trim(0);
    });
    size_t failures = 0;
    std::string first;
    run_concurrent(ref_, /*do_decode=*/true, failures, first);
    stop.store(true, std::memory_order_relaxed);
    trimmer.join();
    ASSERT_EQ(failures, 0u) << "first failure: " << first;
#else
    GTEST_SKIP() << "malloc_trim only available on glibc/Linux";
#endif
}
