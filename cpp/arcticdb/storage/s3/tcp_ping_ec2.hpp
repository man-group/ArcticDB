#include <folly/net/NetOps.h>
#include <arcticdb/log/log.hpp>

namespace arcticdb::storage::s3 {

namespace {
using namespace folly::netops;

struct RaiiSocket {
    static constexpr folly::NetworkSocket invalid{};
    folly::NetworkSocket fd;

    RaiiSocket() : fd(folly::netops::socket(AF_INET, SOCK_STREAM, 0)) {}

    template<typename Fun>
    int attempt([[maybe_unused]] const char* step, Fun&& fun) {
        if (fd != invalid) {
            int out = fun(fd);
            if (out < 0) {
                close(fd);
                fd = invalid;
                ARCTICDB_DEBUG(arcticdb::log::Loggers::instance().storage(),
                    "ec2_metadata step {} failed with {}. errno={}", step, out, errno);
            }
            return out;
        }
        return -1;
    }

    ~RaiiSocket() {
        if (fd != invalid) {
            close(fd);
        }
    }
};
}

/* Quick check without incurring the default, hard-coded long wait time in the AWS SDK. */
bool ec2_metadata_endpoint_reachable() {
    using namespace folly::netops;

    RaiiSocket s;
    s.attempt("set_socket_non_blocking", &set_socket_non_blocking);

    s.attempt("connect", [](auto& fd) {
        sockaddr_in addr;
        addr.sin_family = AF_INET;
        addr.sin_port = htons(80);
        addr.sin_addr.s_addr = inet_addr("169.254.169.254"); // EC2 Metadata endpoint

        connect(fd, (const sockaddr*) &addr, sizeof addr);
        return errno == EINPROGRESS ? 0 : -1;
    });

    int num_ready = s.attempt("select", [](auto& fd) {
        fd_set write_fd;
        FD_ZERO(&write_fd);
        FD_SET(fd.data, &write_fd);
        timeval timeout;
        timeout.tv_sec = 0;
        timeout.tv_usec = 100000;

        return select(fd.data, NULL, &write_fd, NULL, &timeout);
    });

    return num_ready > 0;
}
}
