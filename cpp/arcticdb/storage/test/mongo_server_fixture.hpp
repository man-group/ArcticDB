/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <boost/process.hpp>
#include <gtest/gtest.h>

static const char* TestMongod = "/opt/mongo/bin/mongod";

class TestMongoStorage : public ::testing::Test {
  protected:
    TestMongoServer() : mongod_(TestMongod) {}

    ~TestMongoServer() { terminate(); }

  private:
    void terminate() { mongod_.terminate(); }

    boost::process::child mongod_;
};
