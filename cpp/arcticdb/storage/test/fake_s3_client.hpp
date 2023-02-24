/*
* Copyright 2023 Man Group Operations Ltd.
* NO WARRANTY, EXPRESSED OR IMPLIED
*/

#pragma once

#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/PutObjectRequest.h>
#include <aws/s3/model/DeleteObjectRequest.h>
#include <aws/s3/model/ListObjectsRequest.h>
#include <aws/s3/model/Object.h>
#include <aws/s3/S3Client.h>
#include <arcticdb/storage/common.hpp>
#include <arcticdb/util/string_utils.hpp>

namespace arcticdb::storage::fake {

struct FakeResult
{
    std::vector<char> data_;
    FakeResult(std::vector<char> data) : data_(data) {}
    std::vector<char>& GetBody() { return data_; }
};

struct FakeError{
    std::string GetExceptionName() const { return "fake"; }
    std::string GetMessage() const { return "hello"; }
};

struct FakeOutcome {
    FakeResult result_;
    FakeError error_;
    FakeOutcome(FakeResult&& result) : result_(std::move(result)) {}
    bool IsSuccess() { return true; }
    FakeResult& GetResult() { return result_; }
    FakeError& GetError() { return error_; }
};

class S3Client {
  public:
    virtual Aws::S3::Model::PutObjectOutcome PutObject(const Aws::S3::Model::PutObjectRequest &request) const {
        auto &bucket = storage_[std::string(request.GetBucket())];
        const auto &key = std::string(request.GetKey());
        const auto &body = request.GetBody();
        bucket[key] = arcticdb::storage::stream_to_vector(*body);
        return Aws::S3::Model::PutObjectOutcome(Aws::S3::Model::PutObjectResult());
    }

    virtual FakeOutcome GetObject(const Aws::S3::Model::GetObjectRequest &request) const {
        auto& bucket = storage_[std::string(request.GetBucket())];
        const auto &key = std::string(request.GetKey());
        FakeResult result(bucket[key]);
        return FakeOutcome(std::move(result));
    }

    virtual Aws::S3::Model::ListObjectsOutcome ListObjects(const Aws::S3::Model::ListObjectsRequest &request) const {
        auto &bucket = storage_[std::string(request.GetBucket())];
        const auto &prefix = std::string(request.GetPrefix());
        Aws::Vector<Aws::S3::Model::Object> output;

        for (const auto &kv : bucket) {
            if (arcticdb::util::string_starts_with(prefix, kv.first)) {
                Aws::S3::Model::Object obj;
                obj.SetKey(Aws::String(kv.first));
                output.push_back(obj);
            }
        }
        Aws::S3::Model::ListObjectsResult result;
        result.SetContents(output);
        return Aws::S3::Model::ListObjectsOutcome(result);
    }

    typedef std::unordered_map<std::string, std::vector<char>> BucketType;
    mutable std::unordered_map<std::string, BucketType> storage_;
};

}