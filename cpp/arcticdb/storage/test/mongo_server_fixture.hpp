/*
* Copyright 2023 Man Group Operations Ltd.
* NO WARRANTY, EXPRESSED OR IMPLIED
*/

#pragma once

#include <boost/process.hpp>
#include <gtest/gtest.h>

static const char *TestMongod = "/opt/mongo/bin/mongod";

class TestMongoStorage : public ::testing::Test {
  protected:
    TestMongoServer() :
        mongod_(TestMongod) {
    }

    ~TestMongoServer() {
        terminate();
    }
  private:
    void terminate() {
        mongod_.terminate();
    }

    boost::process::child mongod_;
};
