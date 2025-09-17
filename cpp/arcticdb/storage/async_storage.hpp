#pragma once

#include <arcticdb/entity/variant_key.hpp>
#include <arcticdb/storage/common.hpp>
#include <arcticdb/storage/storage_options.hpp>
#include <arcticdb/storage/key_segment_pair.hpp>

#include <folly/futures/Future.h>

namespace arcticdb::storage {
class AsyncStorage {
  public:
    folly::Future<folly::Unit> async_read(
            entity::VariantKey&& variant_key, const ReadVisitor& visitor, ReadKeyOpts opts
    ) {
        return do_async_read(std::move(variant_key), visitor, opts);
    }

    folly::Future<KeySegmentPair> async_read(entity::VariantKey&& variant_key, ReadKeyOpts opts) {
        return do_async_read(std::move(variant_key), opts);
    }

  private:
    virtual folly::Future<folly::Unit> do_async_read(
            entity::VariantKey&& variant_key, const ReadVisitor& visitor, ReadKeyOpts opts
    ) = 0;

    virtual folly::Future<KeySegmentPair> do_async_read(entity::VariantKey&& variant_key, ReadKeyOpts opts) = 0;
};
} // namespace arcticdb::storage
