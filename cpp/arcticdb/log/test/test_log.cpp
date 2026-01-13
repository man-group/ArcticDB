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
