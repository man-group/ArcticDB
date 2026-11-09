/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <gtest/gtest.h>
#include <arcticdb/toolbox/query_stats.hpp>
#include <arcticdb/storage/s3/s3_api.hpp>
#include <arcticdb/storage/s3/s3_storage.hpp>
#include <arcticdb/storage/s3/s3_client_impl.hpp>
#include <arcticdb/storage/s3/s3_client_wrapper.hpp>
#include <arcticdb/storage/s3/detail-inl.hpp>
#include <arcticdb/storage/test/common.hpp>
#include <arcticdb/util/configs_map.hpp>

#include <aws/core/client/DefaultRetryStrategy.h>

#include <atomic>
#include <stdexcept>
#include <string>
#include <thread>

#ifndef WIN32
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>
#endif

using namespace arcticdb;
using namespace arcticdb::storage;
using namespace arcticdb::query_stats;

namespace {

class QueryStatsFixture : public testing::Test {
  protected:
    void SetUp() override {
        QueryStats::instance()->reset_stats();
        QueryStats::instance()->enable();
    }

    void TearDown() override {
        QueryStats::instance()->disable();
        QueryStats::instance()->reset_stats();
    }

    static QueryStats::OperationStatsOutput stats_for(const std::string& operation, const std::string& key_type) {
        auto stats = QueryStats::instance()->get_stats();
        return stats["storage_operations"][operation][key_type];
    }
};

proto::s3_storage::Config mock_s3_config() {
    auto config = proto::s3_storage::Config();
    config.set_use_mock_storage_for_testing(true);
    return config;
}

std::string failure_trigger(const std::string& symbol, StorageOperation operation, Aws::S3::S3Errors error) {
    return s3::S3ClientTestWrapper::get_failure_trigger(symbol, operation, error, false);
}

} // namespace

TEST_F(QueryStatsFixture, TimerRecordsSuccessAndFailureSeparately) {
    add_task_count_and_time(TaskType::S3_PutObject, entity::KeyType::TABLE_DATA).finish(true);
    {
        auto timer = add_task_count_and_time(TaskType::S3_PutObject, entity::KeyType::TABLE_DATA);
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
        timer.finish(false);
        // Finishing again, or the destructor, must not record it again
        timer.finish(true);
    }

    auto stats = stats_for("S3_PutObject", "TABLE_DATA");
    ASSERT_EQ(stats["count"], 1);
    ASSERT_EQ(stats["failure_count"], 1);
    ASSERT_GE(stats["failure_time_ms"], 20);
    ASSERT_LT(stats["total_time_ms"], 20);
}

TEST_F(QueryStatsFixture, TimerRecordsAThrowingCallAsFailed) {
    ASSERT_THROW(
            {
                auto timer = add_task_count_and_time(TaskType::S3_GetObject, entity::KeyType::VERSION_REF);
                throw std::runtime_error("SDK threw");
            },
            std::runtime_error
    );
    {
        // Not finished, and no exception: succeeded
        auto timer = add_task_count_and_time(TaskType::S3_GetObject, entity::KeyType::VERSION_REF);
    }

    auto stats = stats_for("S3_GetObject", "VERSION_REF");
    ASSERT_EQ(stats["count"], 1);
    ASSERT_EQ(stats["failure_count"], 1);
}

TEST_F(QueryStatsFixture, TimerCreatedDuringStackUnwindingIsNotAFailure) {
    struct TimesInDestructor {
        ~TimesInDestructor() { auto timer = add_task_count_and_time(TaskType::S3_HeadObject, entity::KeyType::LOCK); }
    };
    try {
        TimesInDestructor times_in_destructor;
        throw std::runtime_error("unrelated");
    } catch (const std::runtime_error&) {
    }

    auto stats = stats_for("S3_HeadObject", "LOCK");
    ASSERT_EQ(stats["count"], 1);
    ASSERT_EQ(stats["failure_count"], 0);
}

TEST_F(QueryStatsFixture, OutputHasEveryStatWhenOnlyFailuresWereRecorded) {
    add_task_count_and_time(TaskType::S3_ListObjectsV2, entity::KeyType::SYMBOL_LIST).finish(false);

    auto stats = stats_for("S3_ListObjectsV2", "SYMBOL_LIST");
    ASSERT_EQ(stats.size(), static_cast<size_t>(StatType::END));
    ASSERT_EQ(stats["count"], 0);
    ASSERT_EQ(stats["total_time_ms"], 0);
    ASSERT_EQ(stats["failure_count"], 1);
    ASSERT_EQ(stats["attempts"], 0);
}

TEST_F(QueryStatsFixture, AttemptStatsGoToTheCallsTimedOnTheSameThread) {
    add_to_current_calls(StatType::ATTEMPTS, 1); // No call being timed: dropped
    {
        auto timer = add_task_count_and_time(TaskType::S3_PutObject, entity::KeyType::TABLE_DATA);
        add_to_current_calls(StatType::ATTEMPTS, 1);
        std::thread([] { add_to_current_calls(StatType::ATTEMPTS, 100); }).join();
        add_to_current_calls(StatType::TTFB_MS, 7);
        timer.finish(true);
        add_to_current_calls(StatType::ATTEMPTS, 1000); // After finishing: dropped
    }
    {
        auto data_timer = add_task_count_and_time(TaskType::S3_DeleteObjects, entity::KeyType::TABLE_DATA);
        auto index_timer = add_task_count_and_time(TaskType::S3_DeleteObjects, entity::KeyType::TABLE_INDEX);
        add_to_current_calls(StatType::ATTEMPTS, 2);
    }

    auto put_stats = stats_for("S3_PutObject", "TABLE_DATA");
    ASSERT_EQ(put_stats["attempts"], 1);
    ASSERT_EQ(put_stats["ttfb_ms"], 7);
    ASSERT_EQ(stats_for("S3_DeleteObjects", "TABLE_DATA")["attempts"], 2);
    ASSERT_EQ(stats_for("S3_DeleteObjects", "TABLE_INDEX")["attempts"], 2);
}

TEST_F(QueryStatsFixture, S3StorageRecordsFailedCallsAsFailures) {
    s3::S3Storage store(LibraryPath("lib", '.'), OpenMode::DELETE, s3::S3Settings(mock_s3_config()));

    write_in_store(store, "symbol");
    ASSERT_THROW(
            write_in_store(
                    store, failure_trigger("symbol", StorageOperation::WRITE, Aws::S3::S3Errors::NETWORK_CONNECTION)
            ),
            UnexpectedS3ErrorException
    );
    auto put_stats = stats_for("S3_PutObject", "TABLE_DATA");
    ASSERT_EQ(put_stats["count"], 1);
    ASSERT_GT(put_stats["size_bytes"], 0);
    ASSERT_EQ(put_stats["failure_count"], 1);

    read_in_store(store, "symbol");
    // A missing key is an answer from S3, so it is not a failure
    ASSERT_THROW(read_in_store(store, "missing"), KeyNotFoundException);
    ASSERT_THROW(
            read_in_store(
                    store, failure_trigger("symbol", StorageOperation::READ, Aws::S3::S3Errors::INTERNAL_FAILURE)
            ),
            UnexpectedS3ErrorException
    );
    auto get_stats = stats_for("S3_GetObject", "TABLE_DATA");
    ASSERT_EQ(get_stats["count"], 2);
    ASSERT_EQ(get_stats["failure_count"], 1);

    ASSERT_TRUE(exists_in_store(store, "symbol"));
    ASSERT_FALSE(exists_in_store(store, "missing"));
    ASSERT_THROW(
            exists_in_store(
                    store, failure_trigger("symbol", StorageOperation::EXISTS, Aws::S3::S3Errors::ACCESS_DENIED)
            ),
            PermissionException
    );
    auto head_stats = stats_for("S3_HeadObject", "TABLE_DATA");
    ASSERT_EQ(head_stats["count"], 2);
    ASSERT_EQ(head_stats["failure_count"], 1);
}

TEST_F(QueryStatsFixture, S3StorageRecordsAFailedListAsAFailure) {
    s3::S3Storage store(LibraryPath("lib", '.'), OpenMode::DELETE, s3::S3Settings(mock_s3_config()));
    write_in_store(store, "symbol");
    ASSERT_EQ(list_in_store(store).size(), 1);

    dynamic_cast<s3::S3ClientTestWrapper&>(store.client())
            .add_list_objects_failure_unretryable(Aws::S3::S3Errors::NETWORK_CONNECTION);
    ASSERT_THROW(list_in_store(store), UnexpectedS3ErrorException);

    auto list_stats = stats_for("S3_ListObjectsV2", "TABLE_DATA");
    ASSERT_EQ(list_stats["count"], 1);
    ASSERT_EQ(list_stats["failure_count"], 1);
}

TEST_F(QueryStatsFixture, S3StorageRecordsADeleteBatchOnceForEachKeyType) {
    s3::S3Storage store(LibraryPath("lib", '.'), OpenMode::DELETE, s3::S3Settings(mock_s3_config()));
    auto& client = dynamic_cast<s3::S3ClientTestWrapper&>(store.client());
    for (int i = 0; i < 3; ++i) {
        write_in_store(store, fmt::format("symbol_{}", i));
    }

    // A rejected object that is deleted on retry: one successful call
    client.add_delete_objects_failure("symbol_0", "InternalError", 1);
    remove_in_store(store, {"symbol_0"});
    // A rejected object that is never deleted, while the others are: one failed call
    client.add_delete_objects_failure("symbol_1", "AccessDenied", 1);
    ASSERT_THROW(remove_in_store(store, {"symbol_1", "symbol_2"}), UnexpectedS3ErrorException);

    auto delete_stats = stats_for("S3_DeleteObjects", "TABLE_DATA");
    ASSERT_EQ(delete_stats["count"], 1);
    ASSERT_EQ(delete_stats["failure_count"], 1);
}

#ifndef WIN32
namespace {

// Serves one canned HTTP response per request on a loopback port, keeping connections open, so the real SDK and
// ArcticCurlHttpClient can be tested without S3
class CannedHttpServer {
  public:
    explicit CannedHttpServer(std::vector<std::string> responses) : responses_(std::move(responses)) {
        listen_fd_ = ::socket(AF_INET, SOCK_STREAM, 0);
        int reuse = 1;
        ::setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));
        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        addr.sin_port = 0;
        util::check(::bind(listen_fd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) == 0, "bind failed");
        util::check(::listen(listen_fd_, 8) == 0, "listen failed");
        socklen_t len = sizeof(addr);
        ::getsockname(listen_fd_, reinterpret_cast<sockaddr*>(&addr), &len);
        port_ = ntohs(addr.sin_port);
        thread_ = std::thread([this] { serve(); });
    }

    ~CannedHttpServer() {
        stop_ = true;
        ::shutdown(listen_fd_, SHUT_RDWR);
        ::close(listen_fd_);
        thread_.join();
    }

    uint16_t port() const { return port_; }
    int connections() const { return connections_; }

  private:
    void serve() {
        size_t next_response = 0;
        while (!stop_ && next_response < responses_.size()) {
            int fd = ::accept(listen_fd_, nullptr, nullptr);
            if (fd < 0) {
                return;
            }
            ++connections_;
            std::string received;
            char buf[4096];
            while (next_response < responses_.size()) {
                auto header_end = received.find("\r\n\r\n");
                if (header_end == std::string::npos) {
                    auto n = ::recv(fd, buf, sizeof(buf), 0);
                    if (n <= 0) {
                        break;
                    }
                    received.append(buf, n);
                    continue;
                }
                // Skip the request body, if any
                size_t content_length = 0;
                auto lower = received.substr(0, header_end);
                std::transform(lower.begin(), lower.end(), lower.begin(), ::tolower);
                if (auto pos = lower.find("content-length:"); pos != std::string::npos) {
                    content_length = std::stoul(lower.substr(pos + 15));
                }
                while (received.size() < header_end + 4 + content_length) {
                    auto n = ::recv(fd, buf, sizeof(buf), 0);
                    if (n <= 0) {
                        break;
                    }
                    received.append(buf, n);
                }
                received.erase(0, header_end + 4 + content_length);
                const auto& response = responses_[next_response++];
                ::send(fd, response.data(), response.size(), MSG_NOSIGNAL);
            }
            ::close(fd);
        }
    }

    std::vector<std::string> responses_;
    int listen_fd_ = -1;
    uint16_t port_ = 0;
    std::atomic<bool> stop_ = false;
    std::atomic<int> connections_ = 0;
    std::thread thread_;
};

std::string http_response(int code, const std::string& reason) {
    return fmt::format("HTTP/1.1 {} {}\r\nContent-Length: 0\r\nConnection: keep-alive\r\n\r\n", code, reason);
}

std::unique_ptr<s3::S3ClientImpl> client_for_endpoint(const std::string& endpoint) {
    s3::S3ApiInstance::instance();
    Aws::Client::ClientConfiguration config;
    config.endpointOverride = endpoint;
    config.scheme = Aws::Http::Scheme::HTTP;
    config.region = "us-east-1";
    config.connectTimeoutMs = 1000;
    config.requestTimeoutMs = 5000;
    // The server never sends "100 Continue", so curl would wait 1s before sending each PUT body
    config.disableExpectHeader = true;
    // Two retries, with 0 and 2 ms of backoff
    config.retryStrategy = std::make_shared<Aws::Client::DefaultRetryStrategy>(2, 1);
    config.checksumConfig.requestChecksumCalculation = Aws::Client::RequestChecksumCalculation::WHEN_REQUIRED;
    return std::make_unique<s3::S3ClientImpl>(
            Aws::Auth::AWSCredentials("key", "secret"),
            config,
            Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never,
            false
    );
}

} // namespace

TEST_F(QueryStatsFixture, CurlClientRecordsEachAttempt) {
    // 503 is retried by the SDK, on the same connection
    CannedHttpServer server({http_response(503, "Service Unavailable"), http_response(200, "OK")});
    auto client = client_for_endpoint(fmt::format("127.0.0.1:{}", server.port()));

    auto segment = get_test_segment();
    {
        auto timer = add_task_count_and_time(TaskType::S3_PutObject, entity::KeyType::TABLE_DATA);
        auto result = client->put_object("lib/tdata/symbol", segment, "bucket");
        ASSERT_TRUE(result.is_success());
        timer.finish(result.is_success());
    }

    auto stats = stats_for("S3_PutObject", "TABLE_DATA");
    ASSERT_EQ(stats["count"], 1);
    ASSERT_EQ(stats["attempts"], 2);
    ASSERT_EQ(stats["server_errors"], 1);
    ASSERT_EQ(stats["network_errors"], 0);
    ASSERT_EQ(stats["new_connections"], 1);
    ASSERT_EQ(server.connections(), 1);
    ASSERT_LE(stats["attempt_time_ms"], stats["total_time_ms"]);
    ASSERT_LE(stats["connect_ms"], stats["ttfb_ms"]);
    ASSERT_LT(stats["total_time_ms"], 1000);
}

TEST_F(QueryStatsFixture, CurlClientRecordsNetworkErrors) {
    // Nothing listens on this port once the socket is closed, so every attempt is refused
    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    ASSERT_EQ(::bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)), 0);
    socklen_t len = sizeof(addr);
    ::getsockname(fd, reinterpret_cast<sockaddr*>(&addr), &len);
    ::close(fd);
    auto client = client_for_endpoint(fmt::format("127.0.0.1:{}", ntohs(addr.sin_port)));

    {
        auto timer = add_task_count_and_time(TaskType::S3_HeadObject, entity::KeyType::VERSION_REF);
        auto result = client->head_object("lib/vref/symbol", "bucket");
        ASSERT_FALSE(result.is_success());
        timer.finish(result.is_success());
    }

    auto stats = stats_for("S3_HeadObject", "VERSION_REF");
    ASSERT_EQ(stats["count"], 0);
    ASSERT_EQ(stats["failure_count"], 1);
    ASSERT_EQ(stats["attempts"], 3);
    ASSERT_EQ(stats["network_errors"], 3);
    ASSERT_EQ(stats["server_errors"], 0);
    ASSERT_EQ(stats["ttfb_ms"], 0);
}

TEST_F(QueryStatsFixture, CurlClientRecordsNothingWhenQueryStatsAreDisabled) {
    CannedHttpServer server({http_response(200, "OK")});
    auto client = client_for_endpoint(fmt::format("127.0.0.1:{}", server.port()));
    QueryStats::instance()->disable();

    auto timer = add_task_count_and_time(TaskType::S3_HeadObject, entity::KeyType::VERSION_REF);
    ASSERT_TRUE(client->head_object("lib/vref/symbol", "bucket").is_success());
    timer.finish(true);

    QueryStats::instance()->enable();
    ASSERT_TRUE(QueryStats::instance()->get_stats().empty());
}
#endif // WIN32
