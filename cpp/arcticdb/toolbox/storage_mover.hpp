#pragma once

#include <arcticdb/version/version_store_api.hpp>

#include "codec/default_codecs.hpp"
#include "column_store/column_utils.hpp"

#include "stream/test/stream_test_common.hpp"
#include "util/variant.hpp"
#include "fmt/format.h"
#include <pybind11/pybind11.h>
#include "async/async_store.hpp"
#include "version/version_map.hpp"
#include <util/timer.hpp>
#include <storage/storage.hpp>

#include <pipeline/query.hpp>

#include <Python.h>
#include <pipeline/index_writer.hpp>
#include <util/format_bytes.hpp>
#include <numeric>

#include <arcticdb/stream/stream_source.hpp>
#include <arcticdb/entity/metrics.hpp>
#include <ranges>
#include <functional>

#include "version/version_functions.hpp"

namespace rng = std::ranges;

namespace arcticdb {

constexpr std::size_t NumThreads = 50;

struct BatchCopier {
    std::atomic<uint64_t> count_ = 0;
    std::atomic<uint64_t> objects_moved_ = 0;
    std::atomic<uint64_t> bytes_moved_ = 0;
    std::atomic<uint64_t> skipped_ = 0;
    interval_timer timers_;
    std::vector<VariantKey> keys_;
    std::shared_ptr<Store> source_store_;
    std::shared_ptr<Store> target_store_;
    size_t batch_size_;
    size_t thread_count_;

    BatchCopier(
            std::shared_ptr<Store> source_store, std::shared_ptr<Store> target_store, size_t batch_size,
            size_t thread_count = 32
    ) :
        source_store_(std::move(source_store)),
        target_store_(std::move(target_store)),
        batch_size_(batch_size),
        thread_count_{thread_count} {
        timers_.start_timer();
    }

    void add_key(const VariantKey& key, bool check_target = true, bool check_source = true) {
        if (check_target && !is_ref_key_class(variant_key_type(key)) && target_store_->key_exists(key).get()) {
            ++skipped_;
            return;
        }

        if (check_source && !source_store_->key_exists(key).get()) {
            log::storage().warn("Found an unreadable key {}", key);
            return;
        }

        keys_.push_back(key);
        if (keys_.size() == batch_size_) {
            copy_keys();
            keys_ = std::vector<VariantKey>();

            if (++count_ % 10 == 0) {
                timers_.stop_timer();
                auto bps = bytes_moved_ / timers_.get_timer().get_results().total;
                log::storage().info(
                        "Moved {}, {} objects ({} skipped), {} per second",
                        format_bytes(bytes_moved_),
                        objects_moved_,
                        skipped_,
                        format_bytes(bps)
                );
                timers_.start_timer();
            }
        }
    }

    void go(std::unordered_map<KeyType, std::vector<VariantKey>>&& keys, bool perform_checks) {
        size_t batch_size_per_thread = std::max(batch_size_ / thread_count_, size_t{1});
        // Log approximately every 10000 objects
        uint64_t logging_frequency = 10000 / batch_size_per_thread;
        folly::FutureExecutor<folly::IOThreadPoolExecutor> exec{thread_count_};
        std::vector<folly::Future<folly::Unit>> futures;

        foreach_key_type_write_precedence([&](auto key_type) {
            bool check_target = perform_checks && !is_ref_key_class(key_type);
            bool check_source = perform_checks;
            if (auto it = keys.find(key_type); it != keys.end()) {
                while (it->second.size() > 0) {
                    const auto start = it->second.size() >= batch_size_per_thread
                                               ? it->second.end() - batch_size_per_thread
                                               : it->second.begin();
                    const auto end = it->second.end();
                    const size_t size = std::distance(start, end);
                    std::vector<std::pair<VariantKey, StreamSource::ReadContinuation>> keys_to_copy;
                    keys_to_copy.reserve(size);
                    auto segments_ptr = std::make_unique<std::vector<storage::KeySegmentPair>>(size);
                    std::transform(
                            std::make_move_iterator(start),
                            std::make_move_iterator(end),
                            std::back_inserter(keys_to_copy),
                            [segments = segments_ptr.get(), pos = 0](VariantKey&& key) mutable {
                                return std::pair{
                                        std::move(key),
                                        [segments, pos = pos++](storage::KeySegmentPair&& segment) {
                                            segments->at(pos) = std::move(segment);
                                            return segments->at(pos).variant_key();
                                        }
                                };
                            }
                    );
                    it->second.erase(start, end);
                    futures.emplace_back(exec.addFuture([this,
                                                         keys_to_copy = std::move(keys_to_copy),
                                                         &logging_frequency,
                                                         check_target,
                                                         check_source,
                                                         segments_ptr = std::move(segments_ptr)]() mutable {
                        for (const auto& key : keys_to_copy) {
                            if (check_source && !source_store_->key_exists(key.first).get()) {
                                log::storage().warn("Found an unreadable key {}", key.first);
                            }
                            if (check_target && target_store_->key_exists(key.first).get()) {
                                ++skipped_;
                            }
                        }

                        size_t n_keys = keys_to_copy.size();
                        auto collected_kvs =
                                folly::collect(
                                        source_store_->batch_read_compressed(std::move(keys_to_copy), BatchReadArgs{})
                                )
                                        .via(&async::io_executor())
                                        .get();
                        if (n_keys > 0) {
                            const size_t bytes_being_copied = std::accumulate(
                                    segments_ptr->begin(),
                                    segments_ptr->end(),
                                    size_t{0},
                                    [](size_t a, const storage::KeySegmentPair& ks) { return a + ks.segment().size(); }
                            );
                            target_store_->batch_write_compressed(*segments_ptr.release()).get();
                            bytes_moved_.fetch_add(bytes_being_copied, std::memory_order_relaxed);
                            objects_moved_.fetch_add(n_keys, std::memory_order_relaxed);
                        }
                        ++count_;
                        if (count_.compare_exchange_strong(logging_frequency, 0)) {
                            timers_.stop_timer();
                            auto bps = bytes_moved_.load() / timers_.get_timer().get_results().total;
                            log::storage().info(
                                    "Moved {}, {} objects ({} skipped), {} per second",
                                    format_bytes(bytes_moved_.load()),
                                    objects_moved_.load(),
                                    skipped_.load(),
                                    format_bytes(bps)
                            );
                            timers_.start_timer();
                        }
                        // count_ could be incremented to a value greater than logging_frequency, just reset it in this
                        // case
                        if (count_.load() > logging_frequency) {
                            count_.store(0);
                        }
                        return makeFuture(folly::Unit{});
                    }));
                }
            }
        });
        collect(futures).get();
        timers_.stop_timer();
        auto bps = bytes_moved_.load() / timers_.get_timer().get_results().total;
        log::storage().info(
                "Moved {}, {} objects ({} skipped), {} per second",
                format_bytes(bytes_moved_.load()),
                objects_moved_.load(),
                skipped_.load(),
                format_bytes(bps)
        );
    }

    void copy_keys() {
        std::vector<storage::KeySegmentPair> segments(keys_.size());
        std::vector<std::pair<VariantKey, StreamSource::ReadContinuation>> keys_to_copy;
        keys_to_copy.reserve(keys_.size());
        std::transform(
                std::make_move_iterator(keys_.begin()),
                std::make_move_iterator(keys_.end()),
                std::back_inserter(keys_to_copy),
                [&segments, i = 0](VariantKey&& key) mutable {
                    return std::pair{std::move(key), [&segments, i = i++](storage::KeySegmentPair&& ks) {
                                         segments.at(i) = std::move(ks);
                                         return segments.at(i).variant_key();
                                     }};
                }
        );
        keys_.clear();
        size_t n_keys = keys_to_copy.size();
        auto collected_kvs =
                folly::collect(source_store_->batch_read_compressed(std::move(keys_to_copy), BatchReadArgs{}))
                        .via(&async::io_executor())
                        .get();
        if (n_keys > 0) {
            bytes_moved_ += std::accumulate(
                    segments.begin(),
                    segments.end(),
                    size_t{0},
                    [](size_t a, const storage::KeySegmentPair& ks) { return a + ks.segment().size(); }
            );
            target_store_->batch_write_compressed(std::move(segments)).get();
        }
        objects_moved_ += keys_.size();
    }

    void finalize() {
        if (!keys_.empty()) {
            copy_keys();
        }
        timers_.stop_timer();
        auto total = timers_.get_timer().get_results().total;
        auto bps = bytes_moved_ / total;
        log::storage().info(
                "Moved {} {} objects  in {} - {} bps ",
                format_bytes(bytes_moved_),
                objects_moved_,
                total,
                format_bytes(bps)
        );
    }
};

struct BatchDeleter {
    uint64_t count = 0;
    uint64_t objects_moved = 0;
    uint64_t skipped = 0;
    interval_timer timers;
    std::vector<VariantKey> keys;
    std::shared_ptr<Store> source_store_;
    std::shared_ptr<Store> target_store_;
    size_t batch_size_;

    BatchDeleter(std::shared_ptr<Store> source_store, std::shared_ptr<Store> target_store, size_t batch_size) :
        source_store_(std::move(source_store)),
        target_store_(std::move(target_store)),
        batch_size_(batch_size) {
        timers.start_timer();
    }

    void delete_keys() {
        target_store_->remove_keys(keys).get();
        objects_moved += keys.size();
    }

    void add_key(const VariantKey& key, bool check_target = true) {
        if (check_target && !target_store_->key_exists(key).get()) {
            skipped++;
            log::storage().warn("Found an unreadable key {}", key);
            return;
        }
        keys.push_back(key);
        if (keys.size() == batch_size_) {
            delete_keys();
            keys = std::vector<VariantKey>();

            if (++count % 10 == 0) {
                timers.stop_timer();
                auto bps = objects_moved / timers.get_timer().get_results().total;
                log::storage().info("Moved {} objects ({} skipped), {} per second", objects_moved, skipped, bps);
                timers.start_timer();
            }
        }
    }

    void finalize() {
        if (!keys.empty()) {
            delete_keys();
        }
        timers.stop_timer();
        auto total = timers.get_timer().get_results().total;
        auto bps = objects_moved / timers.get_timer().get_results().total;
        log::storage().info("Moved {} objects  in {} - {} per second ", objects_moved, total, bps);
    }
};

inline MetricsConfig::Model get_model_from_proto_config(const proto::utils::PrometheusConfig& cfg) {
    switch (cfg.prometheus_model()) {
    case proto::utils::PrometheusConfig_PrometheusModel_NO_INIT:
        return MetricsConfig::Model::NO_INIT;
    case proto::utils::PrometheusConfig_PrometheusModel_PUSH:
        return MetricsConfig::Model::PUSH;
    case proto::utils::PrometheusConfig_PrometheusModel_WEB:
        return MetricsConfig::Model::PULL;
    default:
        internal::raise<ErrorCode::E_ASSERTION_FAILURE>(
                "Unknown Prometheus proto model {}", int{cfg.prometheus_model()}
        );
    }
}

class ARCTICDB_VISIBILITY_HIDDEN StorageMover {
  public:
    StorageMover(std::shared_ptr<storage::Library> source_library, std::shared_ptr<storage::Library> target_library) :
        source_store_(std::make_shared<async::AsyncStore<util::SysClock>>(
                source_library, codec::default_lz4_codec(), encoding_version(source_library->config())
        )),
        target_store_(std::make_shared<async::AsyncStore<util::SysClock>>(
                target_library, codec::default_lz4_codec(), encoding_version(target_library->config())
        )),
        cfg_() {
        codec::check<ErrorCode::E_ENCODING_VERSION_MISMATCH>(
                encoding_version(source_library->config()) == encoding_version(target_library->config()),
                "The encoding version of the source library {} is {} which is different than the encoding version {} "
                "of the target library {}",
                source_library->name(),
                encoding_version(source_library->config()),
                encoding_version(target_library->config()),
                target_library->name()
        );
        auto const& src_cfg = source_library->config();
        util::variant_match(
                src_cfg,
                [](std::monostate) { util::raise_rte("Invalid source library cfg"); },
                [&](const proto::storage::VersionStoreConfig& conf) {
                    if (conf.has_prometheus_config()) {
                        MetricsConfig prometheus_config(
                                conf.prometheus_config().host(),
                                conf.prometheus_config().port(),
                                conf.prometheus_config().job_name(),
                                conf.prometheus_config().instance(),
                                conf.prometheus_config().prometheus_env(),
                                get_model_from_proto_config(conf.prometheus_config())
                        );
                        PrometheusInstance::instance()->configure(prometheus_config);
                    }
                    source_symbol_list_ = conf.symbol_list();
                }
        );

        auto const& target_cfg = target_library->config();
        util::variant_match(
                target_cfg,
                [](std::monostate) { util::raise_rte("Invalid source library cfg"); },
                [&](const proto::storage::VersionStoreConfig& conf) { target_symbol_list_ = conf.symbol_list(); }
        );
    }

    void go(size_t batch_size = 1000) {
        BatchCopier copier{source_store_, target_store_, batch_size};
        foreach_key_type([&](KeyType key_type) {
            source_store_->iterate_type(key_type, [&](const VariantKey&& key) { copier.add_key(key); });
        });
        copier.finalize();
    }

    py::list get_all_source_keys() {
        py::list res;
        size_t count = 0;
        foreach_key_type([&](KeyType key_type) {
            source_store_->iterate_type(key_type, [&](const VariantKey& key) {
                res.append(key);
                if (++count % 10000 == 0)
                    log::storage().info("Got {} keys", count);
            });
        });
        return res;
    }

    struct MissingKeysData {
        std::atomic<uint64_t> scanned_keys_;
        std::atomic<uint64_t> missing_keys_;
        std::mutex mutex_;
        interval_timer timer_;

        MissingKeysData() : scanned_keys_(0), missing_keys_(0) { timer_.start_timer(); }

        void report() {
            std::lock_guard lock{mutex_};
            timer_.stop_timer();
            auto keys_per_sec = scanned_keys_ / timer_.get_timer().get_results().total;
            log::version().info(
                    "Scanned {} keys of all types and found {} missing : {} keys/sec",
                    scanned_keys_.load(),
                    missing_keys_.load(),
                    keys_per_sec
            );
            timer_.start_timer();
        }
    };

    struct FindMissingKeysTask : async::BaseTask {
        KeyType key_type_;
        std::shared_ptr<Store> source_store_;
        std::shared_ptr<Store> target_store_;
        std::shared_ptr<MissingKeysData> global_data_;
        uint64_t keys_of_type_;
        uint64_t missing_keys_of_type_;
        size_t batch_size_;
        bool skip_target_check_ref_;
        bool skip_source_check_;

        FindMissingKeysTask(
                KeyType key_type, std::shared_ptr<Store> source_store, std::shared_ptr<Store> target_store,
                std::shared_ptr<MissingKeysData> global_data, size_t batch_size = 100,
                bool skip_target_check_ref = false, bool skip_source_check = false
        ) :
            key_type_(key_type),
            source_store_(std::move(source_store)),
            target_store_(std::move(target_store)),
            global_data_(std::move(global_data)),
            keys_of_type_(0),
            missing_keys_of_type_(0),
            batch_size_(batch_size),
            skip_target_check_ref_(skip_target_check_ref),
            skip_source_check_(skip_source_check) {}

        std::vector<VariantKey> operator()() {
            interval_timer timers;
            timers.start_timer();
            std::vector<VariantKey> res;
            std::vector<VariantKey> all_keys;
            source_store_->iterate_type(key_type_, [&](const VariantKey&& key) {
                ++keys_of_type_;
                ++global_data_->scanned_keys_;
                all_keys.emplace_back(key);
                if (all_keys.size() == batch_size_) {
                    auto key_exists = folly::collect(target_store_->batch_key_exists(all_keys)).get();
                    for (size_t idx = 0; idx != all_keys.size(); idx++) {
                        if ((skip_target_check_ref_ && is_ref_key_class(key_type_)) || !key_exists[idx]) {
                            if (skip_source_check_ || source_store_->key_exists(all_keys[idx]).get()) {
                                res.push_back(all_keys[idx]);
                                ++missing_keys_of_type_;
                                ++global_data_->missing_keys_;
                            } else {
                                log::storage().warn("Storage contains an unreadable key {}", all_keys[idx]);
                            }
                        }
                    }
                    all_keys.clear();
                }
                if (keys_of_type_ % 10000 == 0) {
                    timers.stop_timer();
                    auto keys_per_sec = keys_of_type_ / timers.get_timer().get_results().total;
                    log::version().info(
                            "Scanned {} {} keys and found {} missing : {} keys/sec",
                            keys_of_type_,
                            get_key_description(key_type_),
                            missing_keys_of_type_,
                            keys_per_sec
                    );
                    global_data_->report();
                    timers.start_timer();
                }
            });

            if (!all_keys.empty()) {
                auto key_exists = folly::collect(target_store_->batch_key_exists(all_keys)).get();
                for (size_t idx = 0; idx != all_keys.size(); idx++) {
                    if ((skip_target_check_ref_ && is_ref_key_class(key_type_)) || !key_exists[idx]) {
                        if (skip_source_check_ || source_store_->key_exists(all_keys[idx]).get()) {
                            res.push_back(all_keys[idx]);
                            ++missing_keys_of_type_;
                            ++global_data_->missing_keys_;
                        } else {
                            log::storage().warn("Storage contains an unreadable key {}", all_keys[idx]);
                        }
                    }
                }
            }

            log::storage().info(
                    "{} missing keys of type {}, scanned {}", res.size(), get_key_description(key_type_), keys_of_type_
            );
            return res;
        }
    };

    std::unordered_map<KeyType, std::vector<VariantKey>> get_missing_keys(
            size_t batch_size, bool reverse, bool skip_target_check_ref
    ) {
        auto shared_data = std::make_shared<MissingKeysData>();
        std::unordered_map<KeyType, std::vector<VariantKey>> results;
        auto prim = reverse ? target_store_ : source_store_;
        auto second = reverse ? source_store_ : target_store_;
        foreach_key_type_read_precedence([&](KeyType key_type) {
            auto task =
                    FindMissingKeysTask{key_type, prim, second, shared_data, batch_size, skip_target_check_ref, true};
            results.emplace(key_type, task());
        });

        log::storage().info("Finished scan, collating results");
        shared_data->report();
        return results;
    }

    void incremental_copy(
            size_t batch_size = 1000, size_t thread_count = 32, bool delete_keys = false, bool perform_checks = true
    ) {
        auto missing_keys = get_missing_keys(batch_size * 100, false, true);
        log::storage().info("Copying {} missing key types", missing_keys.size());
        BatchCopier copier{source_store_, target_store_, batch_size, thread_count};
        copier.go(std::move(missing_keys), perform_checks);

        if (delete_keys) {
            auto deleting_keys = get_missing_keys(batch_size * 100, true, false);
            log::storage().info("Deleting {} key types", deleting_keys.size());
            BatchDeleter deleter{source_store_, target_store_, batch_size};
            foreach_key_type_read_precedence([&](auto key_type) {
                if (auto it = deleting_keys.find(key_type); it != deleting_keys.end()) {
                    for (auto& key : it->second)
                        deleter.add_key(key, perform_checks);
                }
            });
            deleter.finalize();
        }
    }

    py::list get_keys_in_source_only() {
        auto all_missing = get_missing_keys(100, false, false);

        py::list res;
        for (const auto& missing_of_type : all_missing) {
            for (const auto& key : missing_of_type.second)
                res.append(key);
        }
        return res;
    }

    size_t clone_all_keys_for_symbol(const StreamId& stream_id, size_t batch_size) {
        std::vector<VariantKey> vkeys;
        foreach_key_type([&](KeyType key_type) {
            source_store_->iterate_type(
                    key_type, [&](const VariantKey& key) { vkeys.push_back(key); }, std::get<StringId>(stream_id)
            );
        });
        return write_variant_keys_from_source_to_target(std::move(vkeys), batch_size);
    }

    size_t clone_all_keys_for_symbol_for_type(const StreamId& stream_id, size_t batch_size, KeyType key_type) {
        std::vector<VariantKey> vkeys;
        source_store_->iterate_type(
                key_type, [&](const VariantKey& key) { vkeys.push_back(key); }, std::get<StringId>(stream_id)
        );
        return write_variant_keys_from_source_to_target(std::move(vkeys), batch_size);
    }

    size_t write_variant_keys_from_source_to_target(std::vector<VariantKey>&& vkeys, size_t batch_size) {
        std::vector<folly::Future<folly::Unit>> write_futs;

        size_t total_copied = 0;
        for (size_t start = 0; start < vkeys.size(); start += batch_size) {
            const size_t end = std::min(start + batch_size, vkeys.size());
            const size_t copy_max_size = end - start;
            std::vector<std::pair<VariantKey, StreamSource::ReadContinuation>> keys_to_copy(copy_max_size);
            std::vector<storage::KeySegmentPair> segments(copy_max_size);
            size_t copied = 0;
            for (size_t offset = start; offset < end; ++offset) {
                if (VariantKey& key = vkeys[offset];
                    source_store_->key_exists(key).get() && !target_store_->key_exists(key).get()) {
                    util::check(variant_key_type(key) != KeyType::UNDEFINED, "Key type is undefined");
                    keys_to_copy[copied++] =
                            std::pair{std::move(key), [copied, &segments](storage::KeySegmentPair&& ks) {
                                          segments[copied] = std::move(ks);
                                          return segments[copied].variant_key();
                                      }};
                } else {
                    log::storage().warn("Key {} not found in source or already exists in target", key);
                }
            }
            // check that there are no undefined keys due to failed key_exists calls
            std::erase_if(keys_to_copy, [](const auto& key) {
                return variant_key_type(key.first) == KeyType::UNDEFINED;
            });
            if (keys_to_copy.empty()) {
                continue;
            }

            total_copied += copied;
            [[maybe_unused]] auto keys =
                    folly::collect(source_store_->batch_read_compressed(std::move(keys_to_copy), BatchReadArgs{}))
                            .via(&async::io_executor())
                            .get();
            std::erase_if(segments, [](const auto& segment) {
                return variant_key_type(segment.variant_key()) == KeyType::UNDEFINED;
            });
            util::check(
                    keys.size() == segments.size(), "Keys and segments size mismatch, maybe due to parallel deletes"
            );
            write_futs.push_back(target_store_->batch_write_compressed(std::move(segments)));
        }
        folly::collect(write_futs).get();
        return total_copied;
    }

    size_t write_keys_from_source_to_target(const std::vector<py::object>& py_keys, size_t batch_size) {
        std::vector<VariantKey> vkeys;
        rng::transform(py_keys, std::back_inserter(vkeys), [](const auto& py_key) -> VariantKey {
            if (py::isinstance<RefKey>(py_key)) {
                return py_key.template cast<RefKey>();
            } else if (py::isinstance<AtomKey>(py_key)) {
                return py_key.template cast<AtomKey>();
            }
            internal::raise<ErrorCode::E_ASSERTION_FAILURE>("Invalid key type");
        });
        return write_variant_keys_from_source_to_target(std::move(vkeys), batch_size);
    }

    py::dict write_symbol_trees_from_source_to_target(
            const std::vector<py::object>& py_partial_keys, bool append_versions
    ) {
        std::shared_ptr<VersionMap> source_map(std::make_shared<VersionMap>());
        std::shared_ptr<VersionMap> target_map(std::make_shared<VersionMap>());
        std::optional<SymbolList> symbol_list;
        if (target_symbol_list_)
            symbol_list.emplace(target_map);
        // res is a dict with key sym and value a dict showing results of the versions
        py::dict res;
        target_map->set_log_changes(true);
        for (const auto& py_pkey : py_partial_keys) {
            // For each version, outputs the version_id which was written in the dest if no error otherwise error string
            py::dict sym_data;
            std::unordered_map<VersionId, std::vector<StringId>> version_to_snapshot_map;
            auto sym = py_pkey.attr("id").cast<StreamId>();
            // Can be either numeric(version id) or string(snapshot_id)
            auto ids = py_pkey.attr("versions").cast<std::vector<std::variant<VersionId, StringId>>>();
            std::vector<AtomKey> index_keys;
            for (const auto& id : ids) {
                util::variant_match(
                        id,
                        [&](const VersionId& numeric_id) {
                            auto index_key = get_specific_version(source_store_, source_map, sym, numeric_id);
                            if (!index_key) {
                                sym_data[py::int_(numeric_id)] =
                                        fmt::format("Sym:{},Version:{},Ex:{}", sym, numeric_id, "Numeric Id not found");
                            } else {
                                index_keys.emplace_back(index_key.value());
                            }
                        },
                        [&](const StringId& snap_name) {
                            auto opt_snapshot = get_snapshot(source_store_, snap_name);
                            if (!opt_snapshot) {
                                sym_data[py::str(snap_name)] = fmt::format(
                                        "Sym:{},SnapId:{},Ex:{}", sym, snap_name, "Snapshot not found in source"
                                );
                                return;
                            }
                            // A snapshot will normally be in a ref key, but for old libraries it still needs to fall
                            // back to iteration of atom keys.
                            auto variant_snap_key = opt_snapshot.value().first;
                            auto snapshot_segment = opt_snapshot.value().second;
                            auto opt_idx_for_stream_id = row_id_for_stream_in_snapshot_segment(
                                    snapshot_segment, variant_key_type(variant_snap_key) == KeyType::SNAPSHOT_REF, sym
                            );
                            if (opt_idx_for_stream_id) {
                                auto stream_idx = opt_idx_for_stream_id.value();
                                auto index_key = read_key_row(snapshot_segment, stream_idx);
                                version_to_snapshot_map[index_key.version_id()].push_back(snap_name);
                                index_keys.emplace_back(std::move(index_key));
                            } else {
                                sym_data[py::str(snap_name)] = fmt::format(
                                        "Sym:{},SnapId:{},Ex:{}", sym, snap_name, "Symbol not found in source snapshot"
                                );
                            }
                        }
                );
            }
            // Remove duplicate keys
            rng::sort(index_keys, [&](const auto& k1, const auto& k2) { return k1.version_id() < k2.version_id(); });
            auto to_erase =
                    rng::unique(index_keys, std::equal_to<VersionId>{}, [](const auto& k) { return k.version_id(); });
            index_keys.erase(to_erase.begin(), to_erase.end());
            for (const auto& index_key : index_keys) {
                VersionId v_id = index_key.version_id();
                try {
                    std::optional<VersionId> new_version_id;
                    std::optional<AtomKey> previous_key;
                    if (append_versions) {
                        auto [maybe_prev, _] = get_latest_version(target_store_, target_map, sym);
                        if (maybe_prev) {
                            new_version_id = std::make_optional(maybe_prev.value().version_id() + 1);
                            previous_key = std::move(maybe_prev);
                        }
                    } else {
                        if (auto target_index_key = get_specific_version(target_store_, target_map, sym, v_id)) {
                            throw storage::DuplicateKeyException(target_index_key.value());
                        }
                    }
                    const auto new_index_key =
                            copy_index_key_recursively(source_store_, target_store_, index_key, new_version_id);
                    target_map->write_version(target_store_, new_index_key, previous_key);
                    if (symbol_list)
                        symbol_list->add_symbol(target_store_, new_index_key.id(), new_version_id.value_or(0));

                    // Change the version in the result map
                    sym_data[py::int_(v_id)] = new_version_id ? new_version_id.value() : v_id;
                    // Give the new version id to the snapshots
                    if (version_to_snapshot_map.contains(v_id)) {
                        for (const auto& snap_name : version_to_snapshot_map[v_id]) {
                            sym_data[py::str(snap_name)] = sym_data[py::int_(v_id)];
                        }
                    }
                } catch (std::exception& e) {
                    auto key = py::int_(v_id);
                    auto error = fmt::format("Sym:{},Version:{},Ex:{}", sym, v_id, e.what());
                    sym_data[key] = error;
                    // Give the error to snapshots which also had the same version_id
                    if (version_to_snapshot_map.contains(v_id)) {
                        for (const auto& snap_name : version_to_snapshot_map[v_id]) {
                            sym_data[py::str(snap_name)] = error;
                        }
                    }
                }
            }
            util::variant_match(
                    sym,
                    [&sym_data, &res](const NumericId& numeric_id) { res[py::int_(numeric_id)] = sym_data; },
                    [&sym_data, &res](const StringId& string_id) { res[py::str(string_id)] = sym_data; }
            );
        }
        return res;
    }

  private:
    std::shared_ptr<Store> source_store_;
    std::shared_ptr<Store> target_store_;
    proto::storage::VersionStoreConfig cfg_;
    bool target_symbol_list_;
    bool source_symbol_list_;
};

} // namespace arcticdb
