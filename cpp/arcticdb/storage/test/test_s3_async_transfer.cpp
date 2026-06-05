/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

// Idea B: stress the REAL SDK async transfer (get_object_async) against a real S3 endpoint.
//
// Idea A proved arcticdb's own buffer assembly + decode never corrupt correct bytes, even under heavy
// concurrency. The production lz4 failures are therefore in the SDK/libcurl async GET delivering wrong
// bytes, and the read-only repros never fired -- the missing ingredient is a concurrent large PUT (in
// prod every prune-read is immediately preceded by a large index PUT). This test PUTs one large object,
// then hammers get_object_async on it from many threads, byte-comparing each result to the known-good
// in-memory bytes (and decoding it to reproduce the exact lz4 throw), while other threads do concurrent
// large PUTs. It is gated on env vars and skips unless a real endpoint is configured.
//
// Configure via env (creds passed in, never compiled in):
//   ARCTICDB_REPRO_S3_ENDPOINT  host (no scheme), e.g. arctic-data.vast.gdc.storage.pre.m
//   ARCTICDB_REPRO_S3_BUCKET    bucket name
//   ARCTICDB_REPRO_S3_ACCESS    access key id
//   ARCTICDB_REPRO_S3_SECRET    secret access key
//   ARCTICDB_REPRO_S3_PREFIX    optional key prefix (default "lz4_repro")
//   ARCTICDB_REPRO_S3_HTTPS     "1" for https (default http)
//   ADB_REPRO_MB                target object size in MB (default 64)
//   ADB_REPRO_READERS           reader threads (default 32)
//   ADB_REPRO_WRITERS           concurrent-PUT threads (default 4)
//   ADB_REPRO_SECONDS           run duration (default 120)

#include <gtest/gtest.h>

#include <arcticdb/storage/s3/s3_api.hpp>
#include <arcticdb/storage/s3/s3_client_impl.hpp>
#include <arcticdb/storage/s3/s3_client_interface.hpp>

#include <arcticdb/codec/codec.hpp>
#include <arcticdb/codec/default_codecs.hpp>
#include <arcticdb/codec/segment.hpp>
#include <arcticdb/column_store/memory_segment.hpp>
#include <arcticdb/entity/stream_descriptor.hpp>
#include <arcticdb/entity/types.hpp>
#include <arcticdb/stream/index.hpp>

#include <aws/core/auth/AWSCredentials.h>
#include <aws/core/client/ClientConfiguration.h>
#include <aws/core/http/Scheme.h>
#include <aws/core/http/HttpResponse.h>
#include <aws/s3/S3Errors.h>

#include <folly/futures/Future.h>
#include <fmt/format.h>

#include <atomic>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <mutex>
#include <random>
#include <string>
#include <thread>
#include <vector>

using namespace arcticdb;
using namespace arcticdb::storage::s3;

namespace {

std::string env_str(const char* name, const std::string& def = "") {
    const char* v = std::getenv(name);
    return v ? std::string(v) : def;
}
size_t env_size(const char* name, size_t def) {
    const char* v = std::getenv(name);
    return v ? static_cast<size_t>(std::strtoull(v, nullptr, 10)) : def;
}

constexpr size_t kNumDataCols = 11;

// Build a large lz4-encoded index-like Segment (timestamp + int64 cols, half sequential / half random).
Segment make_large_segment(size_t target_mb, uint64_t seed) {
    const size_t approx_raw = target_mb * 2 * 1024 * 1024;
    const size_t rows = std::max<size_t>(1024, approx_raw / ((kNumDataCols + 1) * sizeof(int64_t)));
    std::vector<FieldRef> fields;
    for (size_t c = 0; c < kNumDataCols; ++c)
        fields.push_back(scalar_field(DataType::INT64, fmt::format("col_{}", c)));
    const auto desc =
            stream_descriptor_from_range(StreamId{"repro"}, stream::TimeseriesIndex::default_index(), fields);
    SegmentInMemory seg(desc);
    std::mt19937_64 rng(seed);
    for (size_t r = 0; r < rows; ++r) {
        seg.set_scalar(0, static_cast<int64_t>(r));
        for (size_t c = 0; c < kNumDataCols; ++c)
            seg.set_scalar(static_cast<position_t>(c + 1), (c % 2 == 0) ? int64_t(r) : int64_t(rng()));
        seg.end_row();
    }
    return encode_dispatch(seg.clone(), codec::default_lz4_codec(), EncodingVersion::V2);
}

} // namespace

TEST(S3AsyncTransfer, ConcurrentGetWithConcurrentPut) {
    const std::string endpoint = env_str("ARCTICDB_REPRO_S3_ENDPOINT");
    const std::string bucket = env_str("ARCTICDB_REPRO_S3_BUCKET");
    const std::string access = env_str("ARCTICDB_REPRO_S3_ACCESS");
    const std::string secret = env_str("ARCTICDB_REPRO_S3_SECRET");
    if (endpoint.empty() || bucket.empty() || access.empty() || secret.empty())
        GTEST_SKIP() << "set ARCTICDB_REPRO_S3_{ENDPOINT,BUCKET,ACCESS,SECRET} to run";

    const std::string prefix = env_str("ARCTICDB_REPRO_S3_PREFIX", "lz4_repro");
    const bool https = env_size("ARCTICDB_REPRO_S3_HTTPS", 0) != 0;
    const size_t mb = env_size("ADB_REPRO_MB", 64);
    const size_t n_readers = env_size("ADB_REPRO_READERS", 32);
    const size_t n_writers = env_size("ADB_REPRO_WRITERS", 4);
    const size_t seconds = env_size("ADB_REPRO_SECONDS", 120);

    S3ApiInstance::instance(); // Aws::InitAPI

    Aws::Auth::AWSCredentials creds(access.c_str(), secret.c_str());
    Aws::Client::ClientConfiguration cfg;
    cfg.scheme = https ? Aws::Http::Scheme::HTTPS : Aws::Http::Scheme::HTTP;
    cfg.endpointOverride = endpoint;
    cfg.region = ""; // VAST configs use empty region
    cfg.verifySSL = false;
    cfg.maxConnections = static_cast<long>(n_readers + n_writers + 8); // don't bottleneck concurrency
    S3ClientImpl client(
            creds, cfg, Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never, /*useVirtualAddressing=*/false
    );

    const std::string golden_key = prefix + "/golden";

    // Golden object + its known-good in-memory body bytes (never read from storage).
    Segment gold = make_large_segment(mb, 1);
    std::vector<uint8_t> golden_body(gold.buffer().data(), gold.buffer().data() + gold.buffer().bytes());
    {
        auto put = client.put_object(golden_key, gold, bucket);
        ASSERT_TRUE(put.is_success()) << "PUT golden failed: " << put.get_error().GetMessage();
    }
    fmt::print(stderr, "[repro] golden uploaded: {} body bytes ({:.1f} MB), {} readers, {} writers, {}s\n",
            golden_body.size(), double(golden_body.size()) / (1024.0 * 1024.0), n_readers, n_writers, seconds);

    std::atomic<bool> stop{false};
    std::atomic<size_t> reads{0}, get_errors{0}, size_mismatch{0}, byte_mismatch{0}, decode_throws{0};
    std::atomic<size_t> first_logged{0};
    std::mutex log_mtx;

    auto reader = [&]() {
        while (!stop.load(std::memory_order_relaxed)) {
            auto res = client.get_object_async(golden_key, bucket).get();
            reads.fetch_add(1, std::memory_order_relaxed);
            if (!res.is_success()) {
                get_errors.fetch_add(1, std::memory_order_relaxed);
                continue;
            }
            Segment seg = std::move(res.get_output());
            auto bv = seg.buffer();
            bool bad = false;
            if (bv.bytes() != golden_body.size()) {
                size_mismatch.fetch_add(1, std::memory_order_relaxed);
                bad = true;
            } else if (std::memcmp(bv.data(), golden_body.data(), golden_body.size()) != 0) {
                byte_mismatch.fetch_add(1, std::memory_order_relaxed);
                bad = true;
            }
            // Decode too -- reproduces the exact production lz4 throw if bytes were mangled.
            try {
                SegmentInMemory decoded = decode_segment(seg);
                (void)decoded;
            } catch (const std::exception& e) {
                decode_throws.fetch_add(1, std::memory_order_relaxed);
                bad = true;
                if (first_logged.fetch_add(1) == 0) {
                    std::lock_guard<std::mutex> g(log_mtx);
                    fmt::print(stderr, "[repro] *** DECODE THREW on async GET: {}\n", e.what());
                }
            }
            if (bad && first_logged.load() <= 1) {
                std::lock_guard<std::mutex> g(log_mtx);
                fmt::print(stderr, "[repro] *** CORRUPTION: size_mm={} byte_mm={} decode_throw={}\n",
                        size_mismatch.load(), byte_mismatch.load(), decode_throws.load());
            }
        }
    };

    // Each writer owns its own Segment (put_object mutates the header preamble -> no sharing).
    std::vector<Segment> junk;
    junk.reserve(n_writers);
    for (size_t w = 0; w < n_writers; ++w)
        junk.push_back(make_large_segment(mb, 100 + w));
    std::atomic<size_t> puts{0}, put_errors{0};
    auto writer = [&](size_t wid) {
        const std::string key = fmt::format("{}/junk_{}", prefix, wid);
        while (!stop.load(std::memory_order_relaxed)) {
            auto put = client.put_object(key, junk[wid], bucket);
            puts.fetch_add(1, std::memory_order_relaxed);
            if (!put.is_success())
                put_errors.fetch_add(1, std::memory_order_relaxed);
        }
    };

    std::vector<std::thread> threads;
    for (size_t i = 0; i < n_readers; ++i)
        threads.emplace_back(reader);
    for (size_t w = 0; w < n_writers; ++w)
        threads.emplace_back(writer, w);

    const auto t0 = std::chrono::steady_clock::now();
    while (std::chrono::steady_clock::now() - t0 < std::chrono::seconds(seconds) &&
           size_mismatch.load() == 0 && byte_mismatch.load() == 0 && decode_throws.load() == 0) {
        std::this_thread::sleep_for(std::chrono::seconds(2));
        fmt::print(stderr, "[repro] reads={} puts={} get_err={} put_err={} | size_mm={} byte_mm={} decode_throw={}\n",
                reads.load(), puts.load(), get_errors.load(), put_errors.load(), size_mismatch.load(),
                byte_mismatch.load(), decode_throws.load());
    }
    stop.store(true);
    for (auto& t : threads)
        t.join();

    // Cleanup (best effort).
    client.delete_object(golden_key, bucket).get();
    for (size_t w = 0; w < n_writers; ++w)
        client.delete_object(fmt::format("{}/junk_{}", prefix, w), bucket).get();

    fmt::print(stderr, "[repro] DONE reads={} puts={} get_err={} put_err={} | size_mm={} byte_mm={} decode_throw={}\n",
            reads.load(), puts.load(), get_errors.load(), put_errors.load(), size_mismatch.load(),
            byte_mismatch.load(), decode_throws.load());

    EXPECT_EQ(size_mismatch.load(), 0u);
    EXPECT_EQ(byte_mismatch.load(), 0u) << "ASYNC GET returned length-preserving-corrupted bytes";
    EXPECT_EQ(decode_throws.load(), 0u) << "ASYNC GET bytes failed to lz4-decode (the production symptom)";
}

// v2: model the prune profile -- continuous interleaving of PUT, async GET and DELETE on the same
// client/connection pool. A pool of large keys all hold identical golden bytes; writers re-PUT golden
// (recreating deleted keys), readers async-GET random keys and verify-or-tolerate-404, deleters DELETE
// random keys. A GET racing a DELETE may legitimately 404 (counted as "missing", ignored); the bug is a
// GET that succeeds with wrong bytes (byte_mm) or fails to decode (decode_throw).
TEST(S3AsyncTransfer, ConcurrentGetPutDelete) {
    const std::string endpoint = env_str("ARCTICDB_REPRO_S3_ENDPOINT");
    const std::string bucket = env_str("ARCTICDB_REPRO_S3_BUCKET");
    const std::string access = env_str("ARCTICDB_REPRO_S3_ACCESS");
    const std::string secret = env_str("ARCTICDB_REPRO_S3_SECRET");
    if (endpoint.empty() || bucket.empty() || access.empty() || secret.empty())
        GTEST_SKIP() << "set ARCTICDB_REPRO_S3_{ENDPOINT,BUCKET,ACCESS,SECRET} to run";

    const std::string prefix = env_str("ARCTICDB_REPRO_S3_PREFIX", "lz4_repro") + "/pool";
    const bool https = env_size("ARCTICDB_REPRO_S3_HTTPS", 0) != 0;
    const size_t mb = env_size("ADB_REPRO_MB", 64);
    const size_t n_keys = env_size("ADB_REPRO_KEYS", 16);
    const size_t n_readers = env_size("ADB_REPRO_READERS", 24);
    const size_t n_writers = env_size("ADB_REPRO_WRITERS", 4);
    const size_t n_deleters = env_size("ADB_REPRO_DELETERS", 4);
    // A DELETE is ~instant but a large PUT is ~0.5s, so unthrottled deleters empty the pool and almost
    // every GET 404s. Throttle deletes so writers keep the pool populated and GETs hit live objects that
    // a DELETE is concurrently racing. Tune so the steady-state miss rate is well below 100%.
    const size_t del_sleep_ms = env_size("ADB_REPRO_DEL_SLEEP_MS", 200);
    const size_t seconds = env_size("ADB_REPRO_SECONDS", 120);

    S3ApiInstance::instance();
    Aws::Auth::AWSCredentials creds(access.c_str(), secret.c_str());
    Aws::Client::ClientConfiguration cfg;
    cfg.scheme = https ? Aws::Http::Scheme::HTTPS : Aws::Http::Scheme::HTTP;
    cfg.endpointOverride = endpoint;
    cfg.region = "";
    cfg.verifySSL = false;
    cfg.maxConnections = static_cast<long>(n_readers + n_writers + n_deleters + 8);
    S3ClientImpl client(creds, cfg, Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never, false);

    auto key_for = [&](size_t i) { return fmt::format("{}/{}", prefix, i); };

    // Distinct valid segments (different content/seed) -- objects vary like real index versions. We do
    // NOT byte-compare; the production symptom is an lz4 decode failure, so a successful GET is verified
    // simply by decoding it. Each writer owns a Segment (put_object mutates the header preamble).
    const size_t n_segs = std::max<size_t>(n_writers, 1);
    std::vector<Segment> writer_segs;
    writer_segs.reserve(n_segs);
    for (size_t w = 0; w < n_segs; ++w)
        writer_segs.push_back(make_large_segment(mb, 10 + w));

    // Initial fill with varying content.
    for (size_t i = 0; i < n_keys; ++i) {
        auto put = client.put_object(key_for(i), writer_segs[i % n_segs], bucket);
        ASSERT_TRUE(put.is_success()) << "initial PUT failed: " << put.get_error().GetMessage();
    }
    fmt::print(stderr, "[repro] pool filled: {} keys ~{:.1f} MB; {} readers {} writers {} deleters, {}s\n",
            n_keys, double(writer_segs[0].buffer().bytes()) / (1024.0 * 1024.0), n_readers, n_writers, n_deleters,
            seconds);

    std::atomic<bool> stop{false};
    std::atomic<size_t> reads{0}, missing{0}, get_errors{0}, decode_throws{0}, puts{0}, deletes{0};
    std::atomic<size_t> first_logged{0};
    std::mutex log_mtx;

    auto rand_key = [&](std::mt19937_64& r) { return key_for(std::uniform_int_distribution<size_t>(0, n_keys - 1)(r)); };

    auto reader = [&](size_t id) {
        std::mt19937_64 r(2000 + id);
        while (!stop.load(std::memory_order_relaxed)) {
            auto res = client.get_object_async(rand_key(r), bucket).get();
            reads.fetch_add(1, std::memory_order_relaxed);
            if (!res.is_success()) {
                const auto& err = res.get_error();
                if (err.GetErrorType() == Aws::S3::S3Errors::NO_SUCH_KEY ||
                    err.GetResponseCode() == Aws::Http::HttpResponseCode::NOT_FOUND)
                    missing.fetch_add(1, std::memory_order_relaxed); // expected: lost the race with a DELETE
                else
                    get_errors.fetch_add(1, std::memory_order_relaxed);
                continue;
            }
            Segment seg = std::move(res.get_output());
            try {
                SegmentInMemory decoded = decode_segment(seg); // production symptom is a throw here
                (void)decoded;
            } catch (const std::exception& e) {
                if (first_logged.fetch_add(1) == 0) {
                    std::lock_guard<std::mutex> g(log_mtx);
                    fmt::print(stderr, "[repro] *** DECODE THREW on async GET ({} bytes): {}\n",
                            seg.buffer().bytes(), e.what());
                }
                decode_throws.fetch_add(1, std::memory_order_relaxed);
            }
        }
    };

    auto writer = [&](size_t id) {
        std::mt19937_64 r(3000 + id);
        while (!stop.load(std::memory_order_relaxed)) {
            auto put = client.put_object(rand_key(r), writer_segs[id], bucket);
            puts.fetch_add(1, std::memory_order_relaxed);
            (void)put;
        }
    };

    auto deleter = [&](size_t id) {
        std::mt19937_64 r(4000 + id);
        while (!stop.load(std::memory_order_relaxed)) {
            client.delete_object(rand_key(r), bucket).get();
            deletes.fetch_add(1, std::memory_order_relaxed);
            if (del_sleep_ms)
                std::this_thread::sleep_for(std::chrono::milliseconds(del_sleep_ms));
        }
    };

    std::vector<std::thread> threads;
    for (size_t i = 0; i < n_readers; ++i)
        threads.emplace_back(reader, i);
    for (size_t w = 0; w < n_writers; ++w)
        threads.emplace_back(writer, w);
    for (size_t d = 0; d < n_deleters; ++d)
        threads.emplace_back(deleter, d);

    const auto t0 = std::chrono::steady_clock::now();
    while (std::chrono::steady_clock::now() - t0 < std::chrono::seconds(seconds) && decode_throws.load() == 0) {
        std::this_thread::sleep_for(std::chrono::seconds(2));
        fmt::print(stderr, "[repro] reads={} miss={} puts={} dels={} get_err={} decode_throw={}\n",
                reads.load(), missing.load(), puts.load(), deletes.load(), get_errors.load(), decode_throws.load());
    }
    stop.store(true);
    for (auto& t : threads)
        t.join();

    for (size_t i = 0; i < n_keys; ++i)
        client.delete_object(key_for(i), bucket).get();

    fmt::print(stderr, "[repro] DONE reads={} miss={} puts={} dels={} get_err={} decode_throw={}\n",
            reads.load(), missing.load(), puts.load(), deletes.load(), get_errors.load(), decode_throws.load());

    EXPECT_EQ(decode_throws.load(), 0u) << "GET bytes failed to lz4-decode (the production symptom)";
}

// v3: model the real single-writer append+prune sequence (lz4.md: prune does NOT run concurrently
// with the new-index write in a single writer; the discriminator is a large PUT immediately followed
// by an async GET of a recently-written large object on the shared S3 client / io_executor). Each
// writer is one "symbol" that loops: PUT a fresh large key v (the new index), then async-GET + DECODE
// the key it wrote one iteration earlier (v-1, the "previous index" a prune reads) and then DELETE
// that same v-1 key (prune reads the pruned index to find its data keys, then deletes it). Multiple
// writers run in parallel on a shared client. This supplies the read-your-recent-write ingredient the
// settled-object tests (v1/v2) lacked.
// Detector = decode_throw on the prune read.
TEST(S3AsyncTransfer, AppendPruneProfile) {
    const std::string endpoint = env_str("ARCTICDB_REPRO_S3_ENDPOINT");
    const std::string bucket = env_str("ARCTICDB_REPRO_S3_BUCKET");
    const std::string access = env_str("ARCTICDB_REPRO_S3_ACCESS");
    const std::string secret = env_str("ARCTICDB_REPRO_S3_SECRET");
    if (endpoint.empty() || bucket.empty() || access.empty() || secret.empty())
        GTEST_SKIP() << "set ARCTICDB_REPRO_S3_{ENDPOINT,BUCKET,ACCESS,SECRET} to run";

    const std::string prefix = env_str("ARCTICDB_REPRO_S3_PREFIX", "lz4_repro") + "/append_prune";
    const bool https = env_size("ARCTICDB_REPRO_S3_HTTPS", 0) != 0;
    const size_t mb = env_size("ADB_REPRO_MB", 64);
    const size_t n_writers = env_size("ADB_REPRO_WRITERS", 8);
    const size_t seconds = env_size("ADB_REPRO_SECONDS", 120);

    S3ApiInstance::instance();
    Aws::Auth::AWSCredentials creds(access.c_str(), secret.c_str());
    Aws::Client::ClientConfiguration cfg;
    cfg.scheme = https ? Aws::Http::Scheme::HTTPS : Aws::Http::Scheme::HTTP;
    cfg.endpointOverride = endpoint;
    cfg.region = "";
    cfg.verifySSL = false;
    cfg.maxConnections = static_cast<long>(2 * n_writers + 8);
    S3ClientImpl client(creds, cfg, Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never, false);

    // Each writer owns its own Segment (put_object mutates the header preamble).
    std::vector<Segment> segs;
    segs.reserve(n_writers);
    for (size_t w = 0; w < n_writers; ++w)
        segs.push_back(make_large_segment(mb, 10 + w));
    fmt::print(stderr, "[repro] append+prune: {} writers, ~{:.1f} MB index, {}s\n",
            n_writers, double(segs[0].buffer().bytes()) / (1024.0 * 1024.0), seconds);

    std::atomic<bool> stop{false};
    std::atomic<size_t> appends{0}, prune_reads{0}, decode_throws{0}, get_errors{0}, deletes{0}, put_errors{0};
    std::atomic<size_t> first_logged{0};
    std::mutex log_mtx;

    auto writer = [&](size_t id) {
        std::string k_prev;
        size_t v = 0;
        while (!stop.load(std::memory_order_relaxed)) {
            const std::string k_cur = fmt::format("{}/sym_{}/v_{}", prefix, id, v++);
            // Append: write the new (large) index.
            auto put = client.put_object(k_cur, segs[id], bucket);
            appends.fetch_add(1, std::memory_order_relaxed);
            if (!put.is_success())
                put_errors.fetch_add(1, std::memory_order_relaxed);
            // Prune the previous version: async-read + decode its index (to find the data keys to
            // delete), then delete that same index key. A failed decode is exactly the prod case that
            // leaks -- the cleanup can't proceed, so the index is left undeleted.
            if (!k_prev.empty()) {
                auto res = client.get_object_async(k_prev, bucket).get();
                prune_reads.fetch_add(1, std::memory_order_relaxed);
                bool decoded_ok = false;
                if (!res.is_success()) {
                    get_errors.fetch_add(1, std::memory_order_relaxed);
                } else {
                    try {
                        SegmentInMemory decoded = decode_segment(res.get_output());
                        (void)decoded;
                        decoded_ok = true;
                    } catch (const std::exception& e) {
                        if (first_logged.fetch_add(1) == 0) {
                            std::lock_guard<std::mutex> g(log_mtx);
                            fmt::print(stderr, "[repro] *** DECODE THREW on prune read of {}: {}\n", k_prev, e.what());
                        }
                        decode_throws.fetch_add(1, std::memory_order_relaxed);
                    }
                }
                if (decoded_ok) {
                    client.delete_object(k_prev, bucket).get();
                    deletes.fetch_add(1, std::memory_order_relaxed);
                }
            }
            k_prev = k_cur;
        }
        // Best-effort cleanup of the key still live for this writer.
        if (!k_prev.empty())
            client.delete_object(k_prev, bucket).get();
    };

    std::vector<std::thread> threads;
    for (size_t w = 0; w < n_writers; ++w)
        threads.emplace_back(writer, w);

    const auto t0 = std::chrono::steady_clock::now();
    while (std::chrono::steady_clock::now() - t0 < std::chrono::seconds(seconds) && decode_throws.load() == 0) {
        std::this_thread::sleep_for(std::chrono::seconds(2));
        fmt::print(stderr, "[repro] appends={} prune_reads={} dels={} get_err={} put_err={} decode_throw={}\n",
                appends.load(), prune_reads.load(), deletes.load(), get_errors.load(), put_errors.load(),
                decode_throws.load());
    }
    stop.store(true);
    for (auto& t : threads)
        t.join();

    fmt::print(stderr, "[repro] DONE appends={} prune_reads={} dels={} get_err={} put_err={} decode_throw={}\n",
            appends.load(), prune_reads.load(), deletes.load(), get_errors.load(), put_errors.load(),
            decode_throws.load());

    EXPECT_EQ(decode_throws.load(), 0u) << "prune read failed to lz4-decode (the production symptom)";
}
