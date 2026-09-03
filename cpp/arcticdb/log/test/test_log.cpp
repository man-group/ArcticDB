/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/log/log.hpp>

#include <logger.pb.h>

#include <gtest/gtest.h>
#include <google/protobuf/text_format.h>
#include <arcticdb/util/format_bytes.hpp>

TEST(TestLog, SmokeTest) { arcticdb::log::root().info("Some msg"); }

TEST(TestLog, ConfigureSingleton) {
    std::string txt_conf = R"pb(
sink_by_id {
    key: "console"
    value {
        console {
            has_color: true
            std_err: true
        }
    }
}
logger_by_id {
    key: "root"
    value {
        pattern: "*** [%H:%M:%S %z] [thread %t] %v ***"
        sink_ids: "console"
    }
}
    )pb";
    arcticdb::proto::logger::LoggersConfig cfg;
    google::protobuf::TextFormat::ParseFromString(txt_conf, &cfg);
    arcticdb::log::Loggers::instance().configure(cfg);
    arcticdb::log::root().info("Some msg");
}

TEST(TestLog, TestFormatBytes) {
    auto s = arcticdb::format_bytes(12345678);
    ASSERT_EQ(s, "12.35MB");
}

#ifdef _WIN32
#include <io.h>
#define ARCTICDB_DUP _dup
#define ARCTICDB_DUP2 _dup2
#define ARCTICDB_CLOSE _close
#else
#include <unistd.h>
#define ARCTICDB_DUP dup
#define ARCTICDB_DUP2 dup2
#define ARCTICDB_CLOSE close
#endif

#include <arcticdb/log/console_sink.hpp>
#include <filesystem>
#include <fstream>
#include <sstream>

// Regression test: a console sink created before fd 2 is redirected (as pytest's capture does) must write to the
// redirected target, not to whatever the original stderr handle was. With spdlog's stderr_sink_mt on Windows the line
// went to the cached HANDLE, whose value had by then been reused by another file.
TEST(TestLog, ConsoleSinkFollowsStderrRedirection) {
    auto sink = arcticdb::log::make_console_sink(true, false);
    spdlog::logger logger("redirect_test", sink);
    logger.set_pattern("%v");

    const auto capture_path = std::filesystem::temp_directory_path() / "arcticdb_test_log_stderr_capture.txt";
    std::filesystem::remove(capture_path);

    int saved_stderr = ARCTICDB_DUP(2);
    ASSERT_GE(saved_stderr, 0);
    std::fflush(stderr);
    {
        FILE* capture = std::fopen(capture_path.string().c_str(), "w");
        ASSERT_NE(capture, nullptr);
#ifdef _WIN32
        ASSERT_EQ(ARCTICDB_DUP2(_fileno(capture), 2), 0);
#else
        ASSERT_EQ(ARCTICDB_DUP2(fileno(capture), 2), 2);
#endif
        std::fclose(capture);
    }
    logger.warn("captured-line-42");
    logger.flush();
    std::fflush(stderr);
    ARCTICDB_DUP2(saved_stderr, 2);
    ARCTICDB_CLOSE(saved_stderr);

    std::ifstream in(capture_path);
    std::stringstream contents;
    contents << in.rdbuf();
    in.close();
    std::filesystem::remove(capture_path);
    ASSERT_NE(contents.str().find("captured-line-42"), std::string::npos) << "got: " << contents.str();
}
