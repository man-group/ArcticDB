/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/entity/variant_key.hpp>
#include <arcticdb/storage/common.hpp>
#include <arcticdb/storage/key_segment_pair.hpp>
#include <fmt/ranges.h>
#include <arcticdb/storage/storage_exceptions.hpp>
#include <arcticdb/storage/storage_options.hpp>
#include <arcticdb/toolbox/query_stats.hpp>

#include <folly/futures/Future.h>

#include <chrono>
#include <memory>
#include <string>
#include <string_view>

namespace arcticdb::storage {

namespace detail {

inline std::string_view extract_symbol_from_variant_key(const entity::VariantKey& vk) noexcept {
    if (!query_stats::is_symbol_scoped_key_type(entity::variant_key_type(vk))) {
        return {};
    }
    if (const auto* s = std::get_if<arcticdb::StringId>(&entity::variant_key_id(vk))) {
        return *s;
    }
    return {};
}

inline query_stats::StorageOutcome classify_exception_wrapper(const folly::exception_wrapper& ew) noexcept {
    if (ew.is_compatible_with<KeyNotFoundException>()) {
        return query_stats::StorageOutcome::NotFound;
    }
    if (ew.is_compatible_with<DuplicateKeyException>() || ew.is_compatible_with<AtomicOperationFailedException>()) {
        return query_stats::StorageOutcome::Conflict;
    }
    if (ew.is_compatible_with<NotImplementedException>() ||
        ew.is_compatible_with<UnsupportedAtomicOperationException>()) {
        return query_stats::StorageOutcome::Unsupported;
    }
    return query_stats::StorageOutcome::Error;
}

} // namespace detail

class AsyncStorage {
  public:
    virtual ~AsyncStorage() = default;

    [[nodiscard]] virtual query_stats::StorageBackend async_backend_type() const noexcept {
        return query_stats::StorageBackend::Custom;
    }

    [[nodiscard]] virtual std::string_view async_library_name() const noexcept { return {}; }

    folly::Future<folly::Unit> async_read(
            entity::VariantKey&& variant_key, const ReadVisitor& visitor, ReadKeyOpts opts
    ) {
        if (!query_stats::is_active()) {
            return do_async_read(std::move(variant_key), visitor, opts);
        }

        const auto key_type = entity::variant_key_type(variant_key);
        const auto backend = async_backend_type();
        std::string lib(async_library_name());
        std::string sym(detail::extract_symbol_from_variant_key(variant_key));
        const auto start = std::chrono::steady_clock::now();

        query_stats::ContextTagScope tag(lib, sym, query_stats::StorageOp::AsyncRead, key_type);
        auto ctx = query_stats::capture_context();

        struct AsyncVisitorState {
            query_stats::CapturedContext ctx;
            query_stats::StorageBackend backend;
            entity::KeyType key_type;
            std::string lib;
            std::string sym;
            query_stats::TimePoint start;
            ReadVisitor user_visitor;
            bool recorded{false};
        };

        auto state = std::make_shared<AsyncVisitorState>(AsyncVisitorState{
                std::move(ctx), backend, key_type, std::move(lib), std::move(sym), start, visitor, false
        });
        auto wrapped_visitor = std::make_shared<ReadVisitor>([state](const VariantKey& k, Segment&& seg) {
            const auto elapsed_ns = static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(
                                                                  std::chrono::steady_clock::now() - state->start
            )
                                                                  .count());
            const uint64_t bytes = seg.calculate_size();
            {
                query_stats::RestoredContextGuard guard(state->ctx);
                query_stats::record_storage_op(
                        state->backend,
                        query_stats::StorageOp::AsyncRead,
                        state->key_type,
                        query_stats::StorageOutcome::Ok,
                        elapsed_ns,
                        bytes,
                        1,
                        state->lib,
                        state->sym
                );
            }
            state->recorded = true;
            state->user_visitor(k, std::move(seg));
        });

        return do_async_read(std::move(variant_key), *wrapped_visitor, opts)
                .thenTryInline(
                        [state, wrapped_visitor](folly::Try<folly::Unit>&& t) mutable -> folly::Try<folly::Unit> {
                            if (!state->recorded) {
                                const auto elapsed_ns =
                                        static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(
                                                                      std::chrono::steady_clock::now() - state->start
                                        )
                                                                      .count());
                                const auto outcome = t.hasException()
                                                             ? detail::classify_exception_wrapper(t.exception())
                                                             : query_stats::StorageOutcome::Ok;
                                query_stats::RestoredContextGuard guard(state->ctx);
                                query_stats::record_storage_op(
                                        state->backend,
                                        query_stats::StorageOp::AsyncRead,
                                        state->key_type,
                                        outcome,
                                        elapsed_ns,
                                        0,
                                        1,
                                        state->lib,
                                        state->sym
                                );
                                state->recorded = true;
                            }
                            return std::move(t);
                        }
                );
    }

    folly::Future<KeySegmentPair> async_read(entity::VariantKey&& variant_key, ReadKeyOpts opts) {
        if (!query_stats::is_active()) {
            return do_async_read(std::move(variant_key), opts);
        }

        const auto key_type = entity::variant_key_type(variant_key);
        const auto backend = async_backend_type();
        std::string lib(async_library_name());
        std::string sym(detail::extract_symbol_from_variant_key(variant_key));
        const auto start = std::chrono::steady_clock::now();

        query_stats::ContextTagScope tag(lib, sym, query_stats::StorageOp::AsyncRead, key_type);
        auto ctx = query_stats::capture_context();

        return do_async_read(std::move(variant_key), opts)
                .thenTryInline(
                        [ctx = std::move(ctx), backend, key_type, lib = std::move(lib), sym = std::move(sym), start](
                                folly::Try<KeySegmentPair>&& t
                        ) mutable -> folly::Try<KeySegmentPair> {
                            const auto elapsed_ns =
                                    static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(
                                                                  std::chrono::steady_clock::now() - start
                                    )
                                                                  .count());
                            uint64_t bytes = 0;
                            query_stats::StorageOutcome outcome = query_stats::StorageOutcome::Ok;
                            if (t.hasValue()) {
                                if (const auto& seg = t.value().segment_ptr()) {
                                    bytes = seg->calculate_size();
                                }
                            } else {
                                outcome = detail::classify_exception_wrapper(t.exception());
                            }
                            query_stats::RestoredContextGuard guard(ctx);
                            query_stats::record_storage_op(
                                    backend,
                                    query_stats::StorageOp::AsyncRead,
                                    key_type,
                                    outcome,
                                    elapsed_ns,
                                    bytes,
                                    1,
                                    lib,
                                    sym
                            );
                            return std::move(t);
                        }
                );
    }

  private:
    virtual folly::Future<folly::Unit> do_async_read(
            entity::VariantKey&& variant_key, const ReadVisitor& visitor, ReadKeyOpts opts
    ) = 0;

    virtual folly::Future<KeySegmentPair> do_async_read(entity::VariantKey&& variant_key, ReadKeyOpts opts) = 0;
};
} // namespace arcticdb::storage
