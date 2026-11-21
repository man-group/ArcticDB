/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/async/async_store.hpp>
#include <arcticdb/storage/azure/azure_client_impl.hpp>
#include <arcticdb/storage/azure/azure_storage.hpp>
#include <arcticdb/storage/config_cache.hpp>
#include <arcticdb/storage/library.hpp>
#include <arcticdb/storage/lmdb/lmdb_storage.hpp>
#include <arcticdb/storage/memory/memory_storage.hpp>
#include <arcticdb/storage/s3/s3_api.hpp>
#include <arcticdb/storage/s3/s3_client_wrapper.hpp>
#include <arcticdb/storage/s3/s3_storage.hpp>
#include <arcticdb/storage/storages.hpp>
#include <arcticdb/storage/test/common.hpp>
#include <arcticdb/toolbox/query_stats.hpp>
#include <arcticdb/util/configs_map.hpp>

#include <folly/synchronization/NativeSemaphore.h>

#include <aws/core/auth/AWSCredentials.h>
#include <aws/core/client/DefaultRetryStrategy.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/sts/STSClient.h>
#include <aws/sts/model/GetCallerIdentityRequest.h>

#include <azure/core/http/http.hpp>
#include <azure/core/http/policies/policy.hpp>
#include <azure/core/http/transport.hpp>
#include <azure/core/internal/http/pipeline.hpp>

#include <gtest/gtest.h>

#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstring>
#include <filesystem>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#ifndef WIN32
#include <arpa/inet.h>
#include <netinet/in.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>
#endif

namespace arcticdb::storage::test {
namespace {

using namespace arcticdb::query_stats;

s3::S3Settings make_mock_s3_settings() {
    arcticdb::proto::s3_storage::Config cfg;
    cfg.set_bucket_name("test-bucket");
    cfg.set_use_mock_storage_for_testing(true);
    return s3::S3Settings(cfg);
}

TEST(StorageQueryStats, S3MockOutcomesConflictErrorAndVisitorExclusion) {
    s3::S3Storage store(LibraryPath("my_s3_lib", '.'), OpenMode::DELETE, make_mock_s3_settings());

    CollectorSession session(CollectorOptions{.by_library = true, .by_symbol = true});

    // Normal write and read
    write_in_store(store, "AAPL", KeyType::TABLE_DATA);
    ASSERT_EQ(read_in_store(store, "AAPL", KeyType::TABLE_DATA), "AAPL");

    // Missing read -> NotFound (NOT Error)
    ASSERT_THROW(read_in_store(store, "MISSING_SYM", KeyType::TABLE_DATA), KeyNotFoundException);

    // key_exists true -> Ok, false -> NotFound
    ASSERT_TRUE(exists_in_store(store, "AAPL", KeyType::TABLE_DATA));
    ASSERT_FALSE(exists_in_store(store, "MISSING_SYM", KeyType::TABLE_DATA));

    // write_if_none: first succeeds (Ok), second hits 412 PRECONDITION_FAILED -> Conflict (NOT Error!)
    auto lock_key = RefKey("sym_lock", KeyType::ATOMIC_LOCK);
    store.write_if_none(KeySegmentPair(lock_key, get_test_segment()));
    ASSERT_THROW(store.write_if_none(KeySegmentPair(lock_key, get_test_segment())), AtomicOperationFailedException);

    // Unexpected failure -> Error (NOT Ok/Conflict)
    const std::string fail_sym = s3::S3ClientTestWrapper::get_failure_trigger(
            "FAIL_SYM", StorageOperation::WRITE, Aws::S3::S3Errors::INTERNAL_FAILURE, false
    );
    ASSERT_THROW(write_in_store(store, fail_sym, KeyType::TABLE_DATA), UnexpectedS3ErrorException);

    // iterate_type & visit_object_sizes exclude visitor callback time
    uint64_t visited_keys = 0;
    store.iterate_type(KeyType::TABLE_DATA, [&](VariantKey&&) {
        ++visited_keys;
        std::this_thread::sleep_for(std::chrono::milliseconds(80));
    });
    ASSERT_EQ(visited_keys, 1U);

    uint64_t visited_sizes = 0;
    store.visit_object_sizes(KeyType::TABLE_DATA, "", [&](const VariantKey&, CompressedSize) {
        ++visited_sizes;
        std::this_thread::sleep_for(std::chrono::milliseconds(80));
    });
    ASSERT_EQ(visited_sizes, 1U);

    const auto snap = session.snapshot();
    ASSERT_TRUE(snap.storage.contains(StorageBackend::S3));
    const auto& s3_ops = snap.storage.at(StorageBackend::S3);

    // Write: 1 ok (AAPL), 1 error (FAIL_SYM)
    const auto& wr = s3_ops.at(StorageOp::Write).at("TABLE_DATA");
    EXPECT_EQ(wr.ok, 1U);
    EXPECT_EQ(wr.error, 1U);
    EXPECT_EQ(wr.error_latency.count, 1U);
    EXPECT_GT(wr.bytes, 0U);

    // Read: 1 ok (AAPL), 1 not_found (MISSING_SYM), 0 error
    const auto& rd = s3_ops.at(StorageOp::Read).at("TABLE_DATA");
    EXPECT_EQ(rd.ok, 1U);
    EXPECT_EQ(rd.not_found, 1U);
    EXPECT_EQ(rd.error, 0U);
    EXPECT_GT(rd.bytes, 0U);

    // Exists: 1 ok, 1 not_found, 0 error
    const auto& ex = s3_ops.at(StorageOp::Exists).at("TABLE_DATA");
    EXPECT_EQ(ex.ok, 1U);
    EXPECT_EQ(ex.not_found, 1U);
    EXPECT_EQ(ex.error, 0U);

    // WriteIfNone: 1 ok, 1 conflict, 0 error (the PR #3541 regression!)
    const auto& win = s3_ops.at(StorageOp::WriteIfNone).at("ATOMIC_LOCK");
    EXPECT_EQ(win.ok, 1U);
    EXPECT_EQ(win.conflict, 1U);
    EXPECT_EQ(win.error, 0U);

    // List & VisitSizes: pages == 1, items == 1, storage latency excludes 80ms visitor sleep
    const auto& ls = s3_ops.at(StorageOp::List).at("TABLE_DATA");
    EXPECT_EQ(ls.ok, 1U);
    EXPECT_EQ(ls.pages, 1U);
    EXPECT_EQ(ls.items, 1U);
    EXPECT_LT(ls.latency.total_ms(), 50.0);

    const auto& vs = s3_ops.at(StorageOp::VisitSizes).at("TABLE_DATA");
    EXPECT_EQ(vs.ok, 1U);
    EXPECT_EQ(vs.pages, 1U);
    EXPECT_EQ(vs.items, 1U);
    EXPECT_LT(vs.latency.total_ms(), 50.0);

    // Library & symbol attribution
    ASSERT_TRUE(snap.by_library.contains("my_s3_lib"));
    ASSERT_TRUE(snap.by_symbol.contains("AAPL"));
    EXPECT_EQ(snap.by_symbol.at("AAPL").writes, 1U);
    EXPECT_EQ(snap.by_symbol.at("AAPL").reads, 1U);
    EXPECT_EQ(snap.by_symbol.at("AAPL").exists, 1U);
    // ATOMIC_LOCK is NOT symbol-scoped, so "sym_lock" must NOT appear in by_symbol!
    EXPECT_FALSE(snap.by_symbol.contains("sym_lock"));
}

TEST(StorageQueryStats, S3MixedBatchDeleteAndPartialRetry) {
    s3::S3Storage store(LibraryPath("batch_del_lib", '.'), OpenMode::DELETE, make_mock_s3_settings());
    write_in_store(store, "SYM_A", KeyType::TABLE_DATA);
    write_in_store(store, "SYM_A", KeyType::TABLE_INDEX);
    write_in_store(store, "SYM_B", KeyType::COLUMN_STATS);

    // Inject 1 transient partial delete retry for SYM_B
    auto& wrapper = dynamic_cast<s3::S3ClientTestWrapper&>(store.client());
    wrapper.add_delete_objects_failure("SYM_B", "InternalError", 1);

    auto legacy_qs = QueryStats::instance();
    struct LegacyQueryStatsCleanup {
        std::shared_ptr<QueryStats> qs;
        ~LegacyQueryStatsCleanup() {
            qs->disable();
            qs->reset_stats();
        }
    } legacy_cleanup{legacy_qs};
    legacy_qs->reset_stats();
    legacy_qs->enable();
    CollectorSession session(CollectorOptions{.by_library = true, .by_symbol = true});

    std::vector<VariantKey> batch{
            get_test_key("SYM_A", KeyType::TABLE_DATA),
            get_test_key("SYM_A", KeyType::TABLE_INDEX),
            get_test_key("SYM_B", KeyType::COLUMN_STATS),
    };
    store.remove(std::span<VariantKey>(batch), RemoveOpts{});

    const auto snap = session.snapshot();
    const auto legacy = legacy_qs->get_stats();
    legacy_qs->disable();
    legacy_qs->reset_stats();

    // Structured stats record ONE honest DeleteBatch under "MIXED" with 3 items and 1 partial retry.
    const auto& db = snap.storage.at(StorageBackend::S3).at(StorageOp::DeleteBatch).at("MIXED");
    EXPECT_EQ(db.ok, 1U);
    EXPECT_EQ(db.items, 3U);
    EXPECT_EQ(db.partial_delete_retries, 1U);

    // Both symbols attributed
    EXPECT_EQ(snap.by_symbol.at("SYM_A").deletes, 1U);
    EXPECT_EQ(snap.by_symbol.at("SYM_B").deletes, 1U);

    // Legacy view preserves per-distinct-key-type S3_DeleteObjects counts for test_bulk_delete.py compat
    EXPECT_EQ(legacy.at("storage_operations").at("S3_DeleteObjects").at("TABLE_DATA").at("count"), 1U);
    EXPECT_EQ(legacy.at("storage_operations").at("S3_DeleteObjects").at("TABLE_INDEX").at("count"), 1U);
    EXPECT_EQ(legacy.at("storage_operations").at("S3_DeleteObjects").at("COLUMN_STATS").at("count"), 1U);
}

TEST(StorageQueryStats, S3SingleKeyDeletePartialRetryAttribution) {
    s3::S3Storage store(LibraryPath("single_del_lib", '.'), OpenMode::DELETE, make_mock_s3_settings());
    write_in_store(store, "SYM_SINGLE", KeyType::TABLE_DATA);

    auto& wrapper = dynamic_cast<s3::S3ClientTestWrapper&>(store.client());
    wrapper.add_delete_objects_failure("SYM_SINGLE", "InternalError", 1);

    CollectorSession session(CollectorOptions{.by_library = true, .by_symbol = true});
    store.remove(get_test_key("SYM_SINGLE", KeyType::TABLE_DATA), RemoveOpts{});

    const auto snap = session.snapshot();
    ASSERT_TRUE(snap.storage.contains(StorageBackend::S3));
    const auto& s3_ops = snap.storage.at(StorageBackend::S3);
    ASSERT_TRUE(s3_ops.contains(StorageOp::Delete));
    const auto& del = s3_ops.at(StorageOp::Delete).at("TABLE_DATA");
    EXPECT_EQ(del.ok, 1U);
    EXPECT_EQ(del.items, 1U);
    EXPECT_EQ(del.partial_delete_retries, 1U);
    EXPECT_FALSE(s3_ops.contains(StorageOp::DeleteBatch));

    ASSERT_TRUE(snap.by_library.contains("single_del_lib"));
    const auto& lib_ops = snap.by_library.at("single_del_lib").storage.at(StorageBackend::S3);
    ASSERT_TRUE(lib_ops.contains(StorageOp::Delete));
    EXPECT_EQ(lib_ops.at(StorageOp::Delete).at("TABLE_DATA").partial_delete_retries, 1U);
    EXPECT_FALSE(lib_ops.contains(StorageOp::DeleteBatch));

    ASSERT_TRUE(snap.by_symbol.contains("SYM_SINGLE"));
    EXPECT_EQ(snap.by_symbol.at("SYM_SINGLE").deletes, 1U);
}

TEST(StorageQueryStats, MultiBackendMemoryLmdbAndAzureMock) {
    // MemoryStorage
    {
        memory::MemoryStorage mem(LibraryPath("mem_lib", '.'), OpenMode::DELETE, memory::MemoryStorage::Config{});
        CollectorSession session;
        write_in_store(mem, "S1", KeyType::TABLE_DATA);
        ASSERT_EQ(read_in_store(mem, "S1", KeyType::TABLE_DATA), "S1");
        ASSERT_THROW(read_in_store(mem, "NOPE", KeyType::TABLE_DATA), KeyNotFoundException);
        ASSERT_THROW(
                mem.write_if_none(KeySegmentPair(get_test_key("S1", KeyType::TABLE_DATA), get_test_segment())),
                UnsupportedAtomicOperationException
        );
        std::vector<VariantKey> batch{get_test_key("S1", KeyType::TABLE_DATA)};
        mem.remove(std::span<VariantKey>(batch), RemoveOpts{});

        const auto snap = session.snapshot();
        const auto& ops = snap.storage.at(StorageBackend::Memory);
        EXPECT_EQ(ops.at(StorageOp::Write).at("TABLE_DATA").ok, 1U);
        EXPECT_EQ(ops.at(StorageOp::Read).at("TABLE_DATA").ok, 1U);
        EXPECT_EQ(ops.at(StorageOp::Read).at("TABLE_DATA").not_found, 1U);
        EXPECT_EQ(ops.at(StorageOp::WriteIfNone).at("TABLE_DATA").unsupported, 1U);
        EXPECT_EQ(ops.at(StorageOp::DeleteBatch).at("TABLE_DATA").ok, 1U);
        EXPECT_GT(ops.at(StorageOp::DeleteBatch).at("TABLE_DATA").bytes, 0U);
    }

    // LmdbStorage
    {
        const auto tmp_dir = std::filesystem::temp_directory_path() / "arcticdb_qs_lmdb_test";
        std::filesystem::remove_all(tmp_dir);
        std::filesystem::create_directories(tmp_dir);
        lmdb::LmdbStorage::Config lcfg;
        lcfg.set_path(tmp_dir.string());
        lcfg.set_map_size(1024 * 1024 * 16);
        {
            lmdb::LmdbStorage lmdb_store(LibraryPath("lmdb_lib", '.'), OpenMode::DELETE, lcfg);
            CollectorSession session(CollectorOptions{.by_library = true, .by_symbol = true});
            write_in_store(lmdb_store, "NVDA", KeyType::TABLE_DATA);
            ASSERT_TRUE(exists_in_store(lmdb_store, "NVDA", KeyType::TABLE_DATA));
            ASSERT_EQ(read_in_store(lmdb_store, "NVDA", KeyType::TABLE_DATA), "NVDA");
            ASSERT_THROW(read_in_store(lmdb_store, "NONE", KeyType::TABLE_DATA), KeyNotFoundException);
            ASSERT_THROW(
                    lmdb_store.write_if_none(
                            KeySegmentPair(get_test_key("NVDA", KeyType::TABLE_DATA), get_test_segment())
                    ),
                    UnsupportedAtomicOperationException
            );

            const auto snap = session.snapshot();
            const auto& ops = snap.storage.at(StorageBackend::Lmdb);
            EXPECT_EQ(ops.at(StorageOp::Write).at("TABLE_DATA").ok, 1U);
            EXPECT_EQ(ops.at(StorageOp::Exists).at("TABLE_DATA").ok, 1U);
            EXPECT_EQ(ops.at(StorageOp::Read).at("TABLE_DATA").ok, 1U);
            EXPECT_EQ(ops.at(StorageOp::Read).at("TABLE_DATA").not_found, 1U);
            EXPECT_EQ(ops.at(StorageOp::WriteIfNone).at("TABLE_DATA").unsupported, 1U);
            ASSERT_TRUE(snap.by_library.contains("lmdb_lib"));
            ASSERT_TRUE(snap.by_symbol.contains("NVDA"));
        }
        std::filesystem::remove_all(tmp_dir);
    }

    // AzureStorage (mock)
    {
        arcticdb::proto::azure_storage::Config acfg;
        acfg.set_use_mock_storage_for_testing(true);
        azure::AzureStorage az(LibraryPath("az_lib", '.'), OpenMode::DELETE, acfg);
        CollectorSession session;
        write_in_store(az, "AZ1", KeyType::TABLE_DATA);
        ASSERT_TRUE(exists_in_store(az, "AZ1", KeyType::TABLE_DATA));
        ASSERT_EQ(read_in_store(az, "AZ1", KeyType::TABLE_DATA), "AZ1");
        ASSERT_THROW(read_in_store(az, "AZ_MISS", KeyType::TABLE_DATA), KeyNotFoundException);
        ASSERT_EQ(list_in_store(az, KeyType::TABLE_DATA).size(), 1U);

        const auto snap = session.snapshot();
        const auto& ops = snap.storage.at(StorageBackend::Azure);
        EXPECT_EQ(ops.at(StorageOp::Write).at("TABLE_DATA").ok, 1U);
        EXPECT_EQ(ops.at(StorageOp::Exists).at("TABLE_DATA").ok, 1U);
        EXPECT_EQ(ops.at(StorageOp::Read).at("TABLE_DATA").ok, 1U);
        EXPECT_EQ(ops.at(StorageOp::Read).at("TABLE_DATA").not_found, 1U);
        EXPECT_EQ(ops.at(StorageOp::List).at("TABLE_DATA").ok, 1U);
        EXPECT_GE(ops.at(StorageOp::List).at("TABLE_DATA").pages, 1U);
    }
}

class FakeAzureTransport final : public Azure::Core::Http::HttpTransport {
  public:
    explicit FakeAzureTransport(std::vector<int> status_sequence) : statuses_(std::move(status_sequence)) {}

    std::unique_ptr<Azure::Core::Http::RawResponse> Send(Azure::Core::Http::Request&, Azure::Core::Context const&)
            override {
        const size_t i = call_index_++;
        const int code = i < statuses_.size() ? statuses_[i] : 200;
        if (code == -2) {
            throw Azure::Core::OperationCancelledException("simulated timeout/cancel");
        }
        if (code < 0) {
            throw Azure::Core::Http::TransportException("simulated network failure");
        }
        auto resp = std::make_unique<Azure::Core::Http::RawResponse>(
                1, 1, static_cast<Azure::Core::Http::HttpStatusCode>(code), "OK"
        );
        resp->SetBodyStream(std::make_unique<Azure::Core::IO::MemoryBodyStream>(nullptr, 0));
        return resp;
    }

  private:
    std::vector<int> statuses_;
    std::atomic<size_t> call_index_{0};
};

TEST(StorageQueryStats, AzureHttpPoliciesTrackRetriesStatusAndNetworkErrors) {
    // 503 -> 200 retry through Azure HttpPipeline
    {
        Azure::Storage::Blobs::BlobClientOptions opts;
        opts.Retry.MaxRetries = 2;
        opts.Retry.RetryDelay = std::chrono::milliseconds(5);
        opts.Retry.MaxRetryDelay = std::chrono::milliseconds(10);
        opts.Transport.Transport = std::make_shared<FakeAzureTransport>(std::vector<int>{503, 200});
        azure::install_query_stats_azure_policies(opts);

        Azure::Core::Http::_internal::HttpPipeline pipeline(opts, "AzureTest", "1.0", {}, {});

        CollectorSession session;
        ContextTagScope tag("az_http_lib", "SYM_AZ");
        Azure::Core::Http::Request req(
                Azure::Core::Http::HttpMethod::Get, Azure::Core::Url("https://example.invalid/container/blob")
        );
        auto resp = pipeline.Send(req, Azure::Core::Context{});
        ASSERT_EQ(resp->GetStatusCode(), Azure::Core::Http::HttpStatusCode::Ok);

        const auto snap = session.snapshot();
        ASSERT_TRUE(snap.http.contains("azure_blob"));
        const auto& st = snap.http.at("azure_blob").at("GetBlob");
        EXPECT_EQ(st.requests, 1U);
        EXPECT_EQ(st.attempts, 2U);
        EXPECT_EQ(st.retries, 1U);
        EXPECT_EQ(st.status_5xx, 1U);
        EXPECT_EQ(st.throttled, 1U);
        EXPECT_EQ(st.status_2xx, 1U);
        EXPECT_GT(st.backoff_time_ns, 0U);
    }

    // 429 Too Many Requests -> throttled + status_4xx
    {
        Azure::Storage::Blobs::BlobClientOptions opts;
        opts.Retry.MaxRetries = 2;
        opts.Retry.RetryDelay = std::chrono::milliseconds(5);
        opts.Retry.MaxRetryDelay = std::chrono::milliseconds(10);
        opts.Retry.StatusCodes.insert(Azure::Core::Http::HttpStatusCode::TooManyRequests);
        opts.Transport.Transport = std::make_shared<FakeAzureTransport>(std::vector<int>{429, 200});
        azure::install_query_stats_azure_policies(opts);

        Azure::Core::Http::_internal::HttpPipeline pipeline(opts, "AzureTest", "1.0", {}, {});
        CollectorSession session;
        Azure::Core::Http::Request req(
                Azure::Core::Http::HttpMethod::Get, Azure::Core::Url("https://example.invalid/container/blob")
        );
        auto resp = pipeline.Send(req, Azure::Core::Context{});
        ASSERT_EQ(resp->GetStatusCode(), Azure::Core::Http::HttpStatusCode::Ok);

        const auto snap = session.snapshot();
        const auto& st = snap.http.at("azure_blob").at("GetBlob");
        EXPECT_EQ(st.status_4xx, 1U);
        EXPECT_EQ(st.throttled, 1U);
        EXPECT_EQ(st.status_2xx, 1U);
    }

    // TransportException -> network_errors
    {
        Azure::Storage::Blobs::BlobClientOptions opts;
        opts.Retry.MaxRetries = 1;
        opts.Retry.RetryDelay = std::chrono::milliseconds(1);
        opts.Transport.Transport = std::make_shared<FakeAzureTransport>(std::vector<int>{-1, -1});
        azure::install_query_stats_azure_policies(opts);

        Azure::Core::Http::_internal::HttpPipeline pipeline(opts, "AzureTest", "1.0", {}, {});

        CollectorSession session;
        Azure::Core::Http::Request req(
                Azure::Core::Http::HttpMethod::Put, Azure::Core::Url("https://example.invalid/container/blob")
        );
        ASSERT_THROW(pipeline.Send(req, Azure::Core::Context{}), Azure::Core::Http::TransportException);

        const auto snap = session.snapshot();
        const auto& st = snap.http.at("azure_blob").at("PutBlob");
        EXPECT_EQ(st.requests, 1U);
        EXPECT_EQ(st.attempts, 2U);
        EXPECT_EQ(st.retries, 1U);
        EXPECT_EQ(st.network_errors, 2U);
    }

    // OperationCancelledException (e.g. request timeout/cancellation) -> network_errors
    {
        Azure::Storage::Blobs::BlobClientOptions opts;
        opts.Retry.MaxRetries = 0;
        opts.Transport.Transport = std::make_shared<FakeAzureTransport>(std::vector<int>{-2});
        azure::install_query_stats_azure_policies(opts);

        Azure::Core::Http::_internal::HttpPipeline pipeline(opts, "AzureTest", "1.0", {}, {});

        CollectorSession session;
        Azure::Core::Http::Request req(
                Azure::Core::Http::HttpMethod::Get, Azure::Core::Url("https://example.invalid/container/blob")
        );
        ASSERT_THROW(pipeline.Send(req, Azure::Core::Context{}), Azure::Core::OperationCancelledException);

        const auto snap = session.snapshot();
        const auto& st = snap.http.at("azure_blob").at("GetBlob");
        EXPECT_EQ(st.requests, 1U);
        EXPECT_EQ(st.attempts, 1U);
        EXPECT_EQ(st.network_errors, 1U);
    }
}

#ifndef WIN32
// Tiny single-connection-capable loopback HTTP/1.1 server that serves a scripted sequence of responses
// with keep-alive support so libcurl connection reuse & retries are exercised deterministically.
class CannedHttpServer {
  public:
    struct ResponseSpec {
        int status_code;
        std::string status_text;
        std::string body;
        std::string content_type{"application/xml"};
        uint32_t delay_ms{0};
    };

    explicit CannedHttpServer(std::vector<ResponseSpec> responses) : responses_(std::move(responses)) {
        listen_fd_ = ::socket(AF_INET, SOCK_STREAM, 0);
        EXPECT_GE(listen_fd_, 0);
        int opt = 1;
        ::setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        addr.sin_port = 0;
        EXPECT_EQ(::bind(listen_fd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)), 0);
        EXPECT_EQ(::listen(listen_fd_, 8), 0);

        socklen_t len = sizeof(addr);
        EXPECT_EQ(::getsockname(listen_fd_, reinterpret_cast<sockaddr*>(&addr), &len), 0);
        port_ = ntohs(addr.sin_port);

        thread_ = std::thread([this]() { serve(); });
    }

    ~CannedHttpServer() {
        stop_.store(true);
        if (thread_.joinable()) {
            thread_.join();
        }
        if (listen_fd_ >= 0) {
            ::close(listen_fd_);
            listen_fd_ = -1;
        }
    }

    std::string endpoint() const { return "http://127.0.0.1:" + std::to_string(port_); }
    uint16_t port() const { return port_; }

  private:
    bool wait_readable(int fd) const {
        while (!stop_.load()) {
            pollfd pfd{};
            pfd.fd = fd;
            pfd.events = POLLIN;
            const int rc = ::poll(&pfd, 1, 50);
            if (rc > 0) {
                return (pfd.revents & (POLLIN | POLLHUP | POLLERR)) != 0;
            }
            if (rc < 0 && errno != EINTR) {
                return false;
            }
        }
        return false;
    }

    void serve() {
        size_t idx = 0;
        while (!stop_.load() && idx < responses_.size()) {
            if (!wait_readable(listen_fd_)) {
                break;
            }
            sockaddr_in cli{};
            socklen_t cli_len = sizeof(cli);
            int fd = ::accept(listen_fd_, reinterpret_cast<sockaddr*>(&cli), &cli_len);
            if (fd < 0) {
                break;
            }
            // Serve multiple requests on the same TCP connection to test HTTP keep-alive reuse!
            while (!stop_.load() && idx < responses_.size()) {
                std::string req;
                char buf[4096];
                bool got_headers = false;
                while (!got_headers && wait_readable(fd)) {
                    const ssize_t n = ::recv(fd, buf, sizeof(buf), 0);
                    if (n <= 0) {
                        break;
                    }
                    req.append(buf, static_cast<size_t>(n));
                    if (req.find("\r\n\r\n") != std::string::npos) {
                        got_headers = true;
                    }
                }
                if (!got_headers) {
                    break;
                }
                // Drain Content-Length body if present
                if (const auto cl_pos = req.find("Content-Length:"); cl_pos != std::string::npos) {
                    const auto line_end = req.find("\r\n", cl_pos);
                    const size_t content_len = std::stoull(req.substr(cl_pos + 15, line_end - (cl_pos + 15)));
                    const auto header_end = req.find("\r\n\r\n") + 4;
                    while (req.size() < header_end + content_len && wait_readable(fd)) {
                        const ssize_t n = ::recv(fd, buf, sizeof(buf), 0);
                        if (n <= 0) {
                            break;
                        }
                        req.append(buf, static_cast<size_t>(n));
                    }
                }

                const auto& r = responses_[idx++];
                if (r.delay_ms > 0) {
                    std::this_thread::sleep_for(std::chrono::milliseconds(r.delay_ms));
                }
                const std::string raw = "HTTP/1.1 " + std::to_string(r.status_code) + " " + r.status_text + "\r\n" +
                                        "Content-Type: " + r.content_type + "\r\n" +
                                        "Content-Length: " + std::to_string(r.body.size()) + "\r\n" +
                                        "Connection: keep-alive\r\n\r\n" + r.body;
                size_t sent = 0;
                while (sent < raw.size()) {
                    const ssize_t n = ::send(fd, raw.data() + sent, raw.size() - sent, 0);
                    if (n <= 0) {
                        break;
                    }
                    sent += static_cast<size_t>(n);
                }
            }
            ::close(fd);
        }
    }

    std::vector<ResponseSpec> responses_;
    int listen_fd_{-1};
    uint16_t port_{0};
    std::atomic<bool> stop_{false};
    std::thread thread_;
};

Aws::Client::ClientConfiguration make_local_aws_config(const std::string& endpoint, long max_retries = 2) {
    Aws::Client::ClientConfiguration cfg;
    cfg.endpointOverride = endpoint;
    cfg.scheme = Aws::Http::Scheme::HTTP;
    cfg.region = "us-east-1";
    cfg.verifySSL = false;
    cfg.connectTimeoutMs = 2000;
    cfg.requestTimeoutMs = 5000;
    cfg.retryStrategy = std::make_shared<Aws::Client::DefaultRetryStrategy>(max_retries, 5);
    cfg.executor = Aws::MakeShared<s3::ContextPropagatingAwsExecutor>("TestAwsExecutor");
    return cfg;
}

TEST(StorageQueryStats, AwsMonitorRetriesKeepAliveBackoffAsyncAndStsIsolation) {
    auto api = s3::S3ApiInstance::instance();
    const Aws::Auth::AWSCredentials creds("test-key", "test-secret");

    // 1) S3 503 -> 200 retry over keep-alive loopback server
    {
        CannedHttpServer server({
                {503,
                 "Service Unavailable",
                 "<Error><Code>ServiceUnavailable</Code><Message>Slow down</Message></Error>",
                 "application/xml",
                 0},
                {200, "OK", "hello-world", "application/octet-stream", 15},
        });
        auto cfg = make_local_aws_config(server.endpoint(), 2);
        Aws::S3::S3Client client(
                creds, cfg, Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never, /*useVirtualAddressing=*/false
        );

        CollectorSession session(CollectorOptions{.by_library = true, .by_symbol = true});
        ContextTagScope tag("s3_http_lib", "AAPL");

        Aws::S3::Model::GetObjectRequest req;
        req.WithBucket("bkt").WithKey("obj1");
        auto outcome = client.GetObject(req);
        ASSERT_TRUE(outcome.IsSuccess()) << outcome.GetError().GetMessage();

        const auto snap = session.snapshot();
        ASSERT_TRUE(snap.http.contains("s3"));
        const auto& st = snap.http.at("s3").at("GetObject");
        EXPECT_EQ(st.requests, 1U);
        EXPECT_EQ(st.attempts, 2U);
        EXPECT_EQ(st.retries, 1U);
        EXPECT_EQ(st.status_5xx, 1U);
        EXPECT_EQ(st.throttled, 1U);
        EXPECT_EQ(st.status_2xx, 1U);
        EXPECT_EQ(st.network_errors, 0U);
        EXPECT_EQ(st.new_connections, 1U); // 1 TCP connect reused on retry
        EXPECT_GT(st.backoff_time_ns, 0U);
        EXPECT_EQ(st.attempt_latency.count, 2U);
        EXPECT_EQ(st.request_latency.count, 1U);
        EXPECT_GE(st.ttfb_latency.count, 1U);
        EXPECT_GT(st.ttfb_latency.min_ns, 0U);
        EXPECT_LE(st.tcp_connect_latency.count, 1U);
        EXPECT_GE(st.ttfb_latency.max_ms(), 10.0);
        EXPECT_LT(st.ttfb_latency.max_ms(), 5000.0);
        ASSERT_TRUE(snap.by_library.contains("s3_http_lib"));
        EXPECT_EQ(snap.by_library.at("s3_http_lib").http.at("s3").at("GetObject").retries, 1U);
        EXPECT_EQ(snap.by_symbol.at("AAPL").http_requests, 1U);
        EXPECT_EQ(snap.by_symbol.at("AAPL").http_retries, 1U);
    }

    // 2) Connection refused -> network_errors
    {
        // Bind an ephemeral port without calling ::listen() and keep the fd open across the
        // request so connect gets ECONNREFUSED without port-reuse TOCTOU.
        int fd = ::socket(AF_INET, SOCK_STREAM, 0);
        ASSERT_GE(fd, 0);
        struct FdGuard {
            int fd;
            ~FdGuard() {
                if (fd >= 0) {
                    ::close(fd);
                }
            }
        } fd_guard{fd};
        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        addr.sin_port = 0;
        ASSERT_EQ(::bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)), 0);
        socklen_t len = sizeof(addr);
        ASSERT_EQ(::getsockname(fd, reinterpret_cast<sockaddr*>(&addr), &len), 0);
        const uint16_t dead_port = ntohs(addr.sin_port);

        auto cfg = make_local_aws_config("http://127.0.0.1:" + std::to_string(dead_port), 1);
        Aws::S3::S3Client client(creds, cfg, Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never, false);

        CollectorSession session;
        Aws::S3::Model::GetObjectRequest req;
        req.WithBucket("bkt").WithKey("obj_fail");
        auto outcome = client.GetObject(req);
        ASSERT_FALSE(outcome.IsSuccess());

        const auto snap = session.snapshot();
        ASSERT_TRUE(snap.http.contains("s3"));
        const auto& st = snap.http.at("s3").at("GetObject");
        EXPECT_EQ(st.requests, 1U);
        EXPECT_EQ(st.attempts, 2U);
        EXPECT_EQ(st.retries, 1U);
        EXPECT_EQ(st.network_errors, 2U);
        EXPECT_EQ(st.ttfb_latency.count, 0U);
        EXPECT_EQ(st.transfer_latency.count, 0U);
    }

    // 3) Async GetObjectAsync propagates RequestContext through ContextPropagatingAwsExecutor
    {
        CannedHttpServer server({{200, "OK", "async-payload", "application/octet-stream"}});
        auto cfg = make_local_aws_config(server.endpoint(), 0);
        Aws::S3::S3Client client(creds, cfg, Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never, false);

        CollectorSession session(CollectorOptions{.by_library = true, .by_symbol = true});
        ContextTagScope tag("async_s3_lib", "TSLA");

        Aws::S3::Model::GetObjectRequest req;
        req.WithBucket("bkt").WithKey("obj_async");
        auto outcome_future = client.GetObjectCallable(req);
        auto outcome = outcome_future.get();
        ASSERT_TRUE(outcome.IsSuccess()) << outcome.GetError().GetMessage();

        const auto snap = session.snapshot();
        ASSERT_TRUE(snap.http.contains("s3"));
        EXPECT_EQ(snap.http.at("s3").at("GetObject").requests, 1U);
        EXPECT_EQ(snap.http.at("s3").at("GetObject").status_2xx, 1U);
        ASSERT_TRUE(snap.by_library.contains("async_s3_lib"));
        EXPECT_EQ(snap.by_symbol.at("TSLA").http_requests, 1U);
    }

    // 4) STS requests are isolated under "sts", never polluting "s3"
    {
        const std::string sts_xml =
                "<GetCallerIdentityResponse xmlns=\"https://sts.amazonaws.com/doc/2011-06-15/\">"
                "<GetCallerIdentityResult><UserId>u</UserId><Account>123456789012</Account>"
                "<Arn>arn:aws:iam::123456789012:user/u</Arn></GetCallerIdentityResult></GetCallerIdentityResponse>";
        CannedHttpServer server({{200, "OK", sts_xml}});
        auto cfg = make_local_aws_config(server.endpoint(), 0);
        Aws::STS::STSClient sts_client(creds, cfg);

        CollectorSession session;
        Aws::STS::Model::GetCallerIdentityRequest req;
        auto outcome = sts_client.GetCallerIdentity(req);
        ASSERT_TRUE(outcome.IsSuccess()) << outcome.GetError().GetMessage();

        const auto snap = session.snapshot();
        EXPECT_FALSE(snap.http.contains("s3"));
        ASSERT_TRUE(snap.http.contains("sts"));
        EXPECT_EQ(snap.http.at("sts").at("GetCallerIdentity").requests, 1U);
        EXPECT_EQ(snap.http.at("sts").at("GetCallerIdentity").status_2xx, 1U);
    }

    // 5) GCPXMLStorage do_remove_no_batching_impl instrumented under GcpXml + DeleteObject HTTP tracked
    {
        CannedHttpServer server({
                {200, "OK", ""},
                {204, "No Content", ""},
        });
        s3::GCPXMLSettings gcp_settings;
        gcp_settings.set_bucket("gcp-bucket");
        gcp_settings.set_endpoint(server.endpoint());
        gcp_settings.set_access("test-key");
        gcp_settings.set_secret("test-secret");
        gcp_settings.set_https(false);
        gcp_settings.set_ssl(false);

        s3::GCPXMLStorage store(LibraryPath("gcp_lib", '.'), OpenMode::DELETE, gcp_settings);

        CollectorSession session(CollectorOptions{.by_library = true, .by_symbol = true});
        // Use a >32-char heap-allocated symbol name to exercise non-SSO ownership across remove(span).
        const std::string long_sym = "GOOG_LONG_HEAP_ALLOCATED_SYMBOL_NAME_FOR_UAF_REGRESSION_TEST";
        write_in_store(store, long_sym, KeyType::TABLE_DATA);
        std::vector<VariantKey> keys{get_test_key(long_sym, KeyType::TABLE_DATA)};
        store.remove(std::span<VariantKey>(keys), RemoveOpts{});

        const auto snap = session.snapshot();
        ASSERT_TRUE(snap.storage.contains(StorageBackend::GcpXml));
        EXPECT_EQ(snap.storage.at(StorageBackend::GcpXml).at(StorageOp::Write).at("TABLE_DATA").ok, 1U);
        EXPECT_EQ(snap.storage.at(StorageBackend::GcpXml).at(StorageOp::DeleteBatch).at("TABLE_DATA").ok, 1U);
        EXPECT_EQ(snap.storage.at(StorageBackend::GcpXml).at(StorageOp::DeleteBatch).at("TABLE_DATA").items, 1U);
        ASSERT_TRUE(snap.by_symbol.contains(long_sym));
        EXPECT_EQ(snap.by_symbol.at(long_sym).deletes, 1U);
        ASSERT_TRUE(snap.http.contains("s3"));
        const auto& put_st = snap.http.at("s3").at("PutObject");
        EXPECT_EQ(put_st.requests, 1U);
        if (put_st.ttfb_latency.count > 0) {
            EXPECT_GT(put_st.ttfb_latency.min_ns, 0U);
        }
        if (put_st.transfer_latency.count > 0) {
            EXPECT_GT(put_st.transfer_latency.min_ns, 0U);
            EXPECT_GT(put_st.ttfb_latency.count, 0U);
            EXPECT_LT(put_st.transfer_latency.max_ns, put_st.attempt_latency.max_ns);
        }
        EXPECT_EQ(snap.http.at("s3").at("DeleteObject").requests, 1U);
    }
}
#endif // !WIN32

TEST(StorageQueryStats, V1SegmentSizeOrCalculateAvoidsProtoRegeneration) {
    memory::MemoryStorage store(LibraryPath("v1_size_lib", '.'), OpenMode::DELETE, memory::MemoryStorage::Config{});
    auto sim = get_test_frame<arcticdb::stream::TimeseriesIndex>("v1_sym", {}, 16, 0).segment_;
    auto codec_opts = proto::encoding::VariantCodec();
    Segment v1_seg = encode_dispatch(std::move(sim), codec_opts, EncodingVersion::V1);
    const size_t expected_size = v1_seg.size_or_calculate();
    ASSERT_GT(expected_size, 0U);

    auto key = get_test_key("v1_sym", KeyType::TABLE_DATA);
    store.write(KeySegmentPair(VariantKey{key}, std::move(v1_seg)));

    CollectorSession session;
    // Direct KeySegmentPair read overload must reuse cached size_ without regenerating V1 proto_.
    KeySegmentPair read_res = store.read(VariantKey{key}, ReadKeyOpts{});
    ASSERT_TRUE(read_res.segment_ptr() != nullptr);
    auto& read_seg = *read_res.segment_ptr();
    EXPECT_TRUE(read_seg.has_size());
    EXPECT_EQ(read_seg.size_or_calculate(), expected_size);
    EXPECT_ANY_THROW((void)read_seg.proto_size());

    // Clone also preserves cached size_ without regenerating proto_.
    Segment cloned = read_seg.clone();
    EXPECT_TRUE(cloned.has_size());
    EXPECT_EQ(cloned.size_or_calculate(), expected_size);
    EXPECT_ANY_THROW((void)cloned.proto_size());

    // Visitor read overload also avoids proto_ regeneration.
    bool visited = false;
    store.read(
            VariantKey{key},
            [&](const VariantKey&, Segment&& seg) {
                visited = true;
                EXPECT_TRUE(seg.has_size());
                EXPECT_EQ(seg.size_or_calculate(), expected_size);
                EXPECT_ANY_THROW((void)seg.proto_size());
            },
            ReadKeyOpts{}
    );
    EXPECT_TRUE(visited);

    const auto snap = session.snapshot();
    const auto& rd = snap.storage.at(StorageBackend::Memory).at(StorageOp::Read).at("TABLE_DATA");
    EXPECT_EQ(rd.ok, 2U);
    EXPECT_EQ(rd.bytes, 2U * expected_size);
}

TEST(StorageQueryStats, LmdbActiveReadersAndMaxMapUtilizationRatio) {
    const auto root = std::filesystem::temp_directory_path() / "arcticdb_qs_lmdb_gauges_test";
    std::filesystem::remove_all(root);
    const auto large_dir = root / "large";
    const auto small_dir = root / "small";
    std::filesystem::create_directories(large_dir);
    std::filesystem::create_directories(small_dir);

    {
        lmdb::LmdbStorage::Config large_cfg;
        large_cfg.set_path(large_dir.string());
        large_cfg.set_map_size(64 * 1024 * 1024);
        lmdb::LmdbStorage large_store(LibraryPath("large_lmdb_lib", '.'), OpenMode::DELETE, large_cfg);
        write_in_store(large_store, "SPARSE_SYM", KeyType::TABLE_DATA);

        lmdb::LmdbStorage::Config small_cfg;
        small_cfg.set_path(small_dir.string());
        small_cfg.set_map_size(512 * 1024);
        lmdb::LmdbStorage small_store(LibraryPath("small_lmdb_lib", '.'), OpenMode::DELETE, small_cfg);
        for (int i = 0; i < 16; ++i) {
            write_in_store(small_store, "SMALL_SYM_" + std::to_string(i), KeyType::TABLE_DATA);
        }

        // Complete a read and drop the returned KeySegmentPair: active_readers must return to 0 (not HWM).
        ASSERT_EQ(read_in_store(small_store, "SMALL_SYM_0", KeyType::TABLE_DATA), "SMALL_SYM_0");
        EXPECT_EQ(sample_live_resources().lmdb.active_readers, 0U);

        // Hold a returned KeySegmentPair (whose LmdbKeepalive keeps MDB_RDONLY txn open): active_readers == 1.
        {
            auto held = small_store.read(get_test_key("SMALL_SYM_0", KeyType::TABLE_DATA), ReadKeyOpts{});
            ASSERT_TRUE(held.has_segment());
            EXPECT_EQ(sample_live_resources().lmdb.active_readers, 1U);
        }
        EXPECT_EQ(sample_live_resources().lmdb.active_readers, 0U);

        const auto res = sample_live_resources();
        EXPECT_GE(res.lmdb.open_envs, 2U);
        const double agg_ratio =
                static_cast<double>(res.lmdb.used_bytes) / static_cast<double>(res.lmdb.map_size_bytes);
        EXPECT_GT(res.lmdb.max_map_utilization_ratio, agg_ratio);
        EXPECT_LE(res.lmdb.max_map_utilization_ratio, 1.0);
    }

    std::filesystem::remove_all(root);
}

TEST(StorageQueryStats, AsyncStoreSemaphoreSkipsUncontendedAndTracksContendedWait) {
    auto mem = std::make_shared<memory::MemoryStorage>(
            LibraryPath("sem_lib", '.'), OpenMode::DELETE, memory::MemoryStorage::Config{}
    );
    auto storages = std::make_shared<Storages>(Storages::StorageVector{mem}, OpenMode::DELETE);
    auto lib = std::make_shared<Library>(LibraryPath("sem_lib", '.'), std::move(storages));
    async::AsyncStore<> async_store(lib, proto::encoding::VariantCodec{}, EncodingVersion::V2);

    auto sem = std::make_shared<folly::NativeSemaphore>(1);
    CollectorSession session;

    // 1) Uncontended write must not record a semaphore wait.
    auto sim1 = get_test_frame<arcticdb::stream::TimeseriesIndex>("sem_sym_1", {}, 10, 0).segment_;
    stream::PartialKey pk1{
            KeyType::TABLE_DATA, VersionId{0}, StreamId{"sem_sym_1"}, entity::NumericIndex{0}, entity::NumericIndex{10}
    };
    async_store.write_maybe_blocking(pk1, std::move(sim1), sem).get();

    const auto after_uncontended = session.snapshot();
    EXPECT_EQ(after_uncontended.thread_pools.semaphore_waits, 0U);
    EXPECT_EQ(after_uncontended.thread_pools.semaphore_wait_latency.count, 0U);
    EXPECT_EQ(after_uncontended.stream.data_segments_written, 1U);

    // 2) Contended write must record exactly 1 semaphore wait with positive latency.
    auto sim2 = get_test_frame<arcticdb::stream::TimeseriesIndex>("sem_sym_2", {}, 10, 0).segment_;
    stream::PartialKey pk2{
            KeyType::TABLE_DATA, VersionId{0}, StreamId{"sem_sym_2"}, entity::NumericIndex{0}, entity::NumericIndex{10}
    };
    sem->wait();
    std::thread releaser([sem]() {
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
        sem->post();
    });
    async_store.write_maybe_blocking(pk2, std::move(sim2), sem).get();
    releaser.join();

    const auto after_contended = session.snapshot();
    EXPECT_EQ(after_contended.thread_pools.semaphore_waits, 1U);
    EXPECT_EQ(after_contended.thread_pools.semaphore_wait_latency.count, 1U);
    EXPECT_GT(after_contended.thread_pools.semaphore_wait_latency.sum_ns, 0U);
    EXPECT_EQ(after_contended.stream.data_segments_written, 2U);
}

TEST(StorageQueryStats, VisitorExceptionPreservesOkOutcome) {
    s3::S3Storage store(LibraryPath("visitor_exc_lib", '.'), OpenMode::DELETE, make_mock_s3_settings());
    write_in_store(store, "AAPL", KeyType::TABLE_DATA);

    CollectorSession session(CollectorOptions{.by_library = true, .by_symbol = false});
    ASSERT_THROW(
            store.iterate_type(
                    KeyType::TABLE_DATA,
                    [](VariantKey&&) { throw KeyNotFoundException("user visitor threw key not found"); }
            ),
            KeyNotFoundException
    );

    const auto snap = session.snapshot();
    const auto& ls = snap.storage.at(StorageBackend::S3).at(StorageOp::List).at("TABLE_DATA");
    EXPECT_EQ(ls.ok, 1U);
    EXPECT_EQ(ls.not_found, 0U);
    EXPECT_EQ(ls.error, 0U);
    EXPECT_EQ(ls.items, 1U);
}

class ThrowingStoragesConfigResolver final : public ConfigResolver {
  public:
    std::vector<std::pair<LibraryPath, arcticdb::proto::storage::LibraryDescriptor>>
    get_libraries(const EnvironmentName&) const override {
        arcticdb::proto::storage::LibraryDescriptor d1;
        d1.set_name("lib_one");
        arcticdb::proto::storage::LibraryDescriptor d2;
        d2.set_name("lib_two");
        return {
                {LibraryPath("lib_one", '.'), std::move(d1)},
                {LibraryPath("lib_two", '.'), std::move(d2)},
        };
    }

    std::vector<std::pair<StorageName, arcticdb::proto::storage::VariantStorage>> get_storages(const EnvironmentName&)
            const override {
        throw std::runtime_error("simulated get_storages failure");
    }

    void add_library(const EnvironmentName&, const arcticdb::proto::storage::LibraryDescriptor&) override {}
    void add_storage(const EnvironmentName&, const StorageName&, const arcticdb::proto::storage::VariantStorage&)
            override {}
    void initialize_environment(const EnvironmentName&) override {}
    std::string_view resolver_type() const override { return "throwing_test"; }
};

TEST(StorageQueryStats, ConfigCacheCtorFailureDoesNotDriftGauges) {
    const auto before = sample_live_resources();
    auto resolver = std::make_shared<ThrowingStoragesConfigResolver>();
    EXPECT_THROW({ storage::ConfigCache cache(EnvironmentName{"ctor_fail_env"}, resolver); }, std::runtime_error);
    const auto after = sample_live_resources();
    EXPECT_EQ(after.config_cache_libraries, before.config_cache_libraries);
    EXPECT_EQ(after.config_cache_storages, before.config_cache_storages);
}

} // namespace
} // namespace arcticdb::storage::test
