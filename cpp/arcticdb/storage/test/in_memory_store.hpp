/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/storage/store.hpp>
#include <arcticdb/storage/storage_utils.hpp>
#include <arcticdb/entity/key.hpp>
#include <arcticdb/util/preconditions.hpp>
#include <arcticdb/util/variant.hpp>
#include <arcticdb/stream/test/stream_test_common.hpp>
#include <arcticdb/storage/storage.hpp>

#include <folly/futures/Future.h>

#include <vector>

namespace arcticdb {
    class InMemoryStore : public Store {

    public:
        InMemoryStore() = default;

        folly::Future<folly::Unit> batch_write_compressed(std::vector<storage::KeySegmentPair>) override {
            util::raise_rte("Not implemented");
        }

        bool batch_all_keys_exist_sync(const std::unordered_set<entity::VariantKey>&) override {
            util::raise_rte("Not implemented");
        }

        bool supports_prefix_matching() const override {
            return false;
        }

        bool fast_delete() override {
            return false;
        }

        std::vector<Composite<ProcessingUnit>> batch_read_uncompressed(
                std::vector<Composite<pipelines::SliceAndKey>> &&,
                const std::vector<std::shared_ptr<Clause>>&,
                const std::shared_ptr<std::unordered_set<std::string>>&,
                const BatchReadArgs &) override {
            throw std::runtime_error("Not implemented for tests");
        }

        folly::Future<VariantKey> write(
                KeyType key_type,
                VersionId gen_id,
                const StreamId& stream_id,
                IndexValue start_index,
                IndexValue end_index,
                SegmentInMemory &&segment
        ) override {
            auto key = atom_key_builder().gen_id(gen_id).content_hash(content_hash_).creation_ts(PilotedClock::nanos_since_epoch())
                    .start_index(start_index).end_index(end_index).build(stream_id, key_type);
            add_segment(key, std::move(segment));
            ARCTICDB_DEBUG(log::storage(), "Mock store adding atom key {}", key);
            return folly::makeFuture(key);
        }

        folly::Future<entity::VariantKey> write(
                stream::KeyType key_type,
                VersionId gen_id,
                const StreamId& stream_id,
                timestamp creation_ts,
                IndexValue start_index,
                IndexValue end_index,
                SegmentInMemory &&segment
        ) override {
            auto key = atom_key_builder().gen_id(gen_id).content_hash(content_hash_).creation_ts(creation_ts)
                    .start_index(start_index).end_index(end_index).build(stream_id, key_type);
            add_segment(key, std::move(segment));
            ARCTICDB_DEBUG(log::storage(), "Mock store adding atom key {}", key);
            return folly::makeFuture(key);
        }

        folly::Future<VariantKey>
        update(const VariantKey& key,
                SegmentInMemory &&segment,
                storage::UpdateOpts opts
        ) override {
            if (!opts.upsert_) {
                util::check_rte(key_exists(key).get(), "update called with upsert=false but key does not exist");
            }
            if (std::holds_alternative<AtomKey>(key)) {
                add_segment(to_atom(key), std::move(segment));
            } else {
                add_segment(to_ref(key), std::move(segment));
            }
            ARCTICDB_DEBUG(log::storage(), "Mock store adding atom key {}", key);
            return folly::makeFuture(key);
        }

        folly::Future<VariantKey>
        write(
                PartialKey pk,
                SegmentInMemory &&segment
        ) override {
            return write(pk.key_type, pk.version_id, pk.stream_id, pk.start_index, pk.end_index,
                         std::move(segment));
        }

        entity::VariantKey write_sync(
                stream::KeyType key_type,
                VersionId version_id,
                const StreamId& stream_id,
                IndexValue start_index,
                IndexValue end_index,
                SegmentInMemory &&segment) override {
            return write(key_type, version_id, stream_id, start_index, end_index, std::move(segment)).get();
        }

        entity::VariantKey write_sync(PartialKey pk, SegmentInMemory &&segment) override {
            return write(pk, std::move(segment)).get();
        }

        entity::VariantKey write_sync(
                KeyType key_type,
                const StreamId& stream_id,
                SegmentInMemory &&segment) override {
            return write(key_type, stream_id, std::move(segment)).get();
        }

        folly::Future<entity::VariantKey>
        write(stream::KeyType key_type, const StreamId& stream_id, SegmentInMemory &&segment) override {
            util::check(is_ref_key_class(key_type), "Cannot write ref key with atom key type {}", key_type);
            auto key = entity::RefKey{stream_id, key_type};
            add_segment(key, std::move(segment));
            ARCTICDB_DEBUG(log::storage(), "Mock store adding ref key {}", key);
            return folly::makeFuture(key);
        }

        folly::Future<VariantKey> copy(arcticdb::entity::KeyType, const StreamId&, arcticdb::entity::VersionId, const VariantKey&) override {
            util::raise_rte("Not implemented");
        }

        VariantKey copy_sync(arcticdb::entity::KeyType, const StreamId&, arcticdb::entity::VersionId, const VariantKey&) override {
            util::raise_rte("Not implemented");
        }

        bool key_exists_sync(const entity::VariantKey &key) override {
            std::lock_guard lock{mutex_};
            return util::variant_match(key,
                                       [&](const RefKey &key) {
                                           auto it = seg_by_ref_key_.find(key);
                                           return it != seg_by_ref_key_.end();
                                       },
                                       [&](const AtomKey &key) {
                                           auto it = seg_by_atom_key_.find(key);
                                           return it != seg_by_atom_key_.end();
                                       }
            );
        }

        folly::Future<bool> key_exists(const entity::VariantKey &key) override {
            return folly::makeFuture(key_exists_sync(key));
        }

        std::pair<VariantKey, SegmentInMemory> read_sync(const VariantKey& key, storage::ReadKeyOpts) override {
            StorageFailureSimulator::instance()->go(FailureType::READ);
            std::lock_guard lock{mutex_};
            return util::variant_match(key,
                                       [&] (const RefKey& ref_key) {
                                           auto it = seg_by_ref_key_.find(ref_key);
                                           if (it == seg_by_ref_key_.end())
                                               throw storage::KeyNotFoundException(Composite<VariantKey>(ref_key));
                                           ARCTICDB_DEBUG(log::storage(), "Mock store returning ref key {}", ref_key);
                                           std::pair<VariantKey, arcticdb::SegmentInMemory> res = {it->first, it->second->clone()};
                                           return res;
                                       },
                                       [&] (const AtomKey& atom_key) {
                                           auto it = seg_by_atom_key_.find(atom_key);
                                           if (it == seg_by_atom_key_.end())
                                               throw storage::KeyNotFoundException(Composite<VariantKey>(atom_key));
                                           ARCTICDB_DEBUG(log::storage(), "Mock store returning atom key {}", atom_key);
                                           std::pair<VariantKey, arcticdb::SegmentInMemory> res = {it->first, it->second->clone()};
                                           //seg_by_atom_key_.erase(it);
                                           return res;
                                       });
        }

        folly::Future<storage::KeySegmentPair> read_compressed(const entity::VariantKey&, storage::ReadKeyOpts) override {
            throw std::runtime_error("Not implemented");
        }

        folly::Future<std::pair<VariantKey, SegmentInMemory>> read(const VariantKey& key, storage::ReadKeyOpts opts) override {
            // Anything read_sync() throws should be returned inside the Future, so:
            return folly::makeFutureWith([&](){ return read_sync(key, opts); });
        }

        folly::Future<folly::Unit> write_compressed(storage::KeySegmentPair&&) override {
            util::raise_rte("Not implemented");
        }

        void write_compressed_sync(storage::KeySegmentPair&&) override {
            util::raise_rte("Not implemented");
        }

        RemoveKeyResultType remove_key_sync(const entity::VariantKey &key, storage::RemoveOpts opts) override {
            StorageFailureSimulator::instance()->go(FailureType::DELETE);
            std::lock_guard lock{mutex_};
            size_t removed = util::variant_match(key,
                [&](const AtomKey &ak) { return seg_by_atom_key_.erase(ak); },
                [&](const RefKey &rk) { return seg_by_ref_key_.erase(rk); });
            ARCTICDB_DEBUG(log::storage(), "Mock store removed {} {}", removed, key);
            if (removed == 0 && !opts.ignores_missing_key_) {
                throw storage::KeyNotFoundException(Composite(VariantKey(key)));
            }
            return {};
        }

        folly::Future<RemoveKeyResultType> remove_key(const VariantKey &key, storage::RemoveOpts opts) override {
            return folly::makeFuture(remove_key_sync(key, opts));
        }

        timestamp current_timestamp() override {
            return PilotedClock::nanos_since_epoch();
        }


        void iterate_type(KeyType kt, entity::IterateTypeVisitor func, const std::string& prefix = "")
        override {
            auto prefix_matcher = stream_id_prefix_matcher(prefix);
            auto failure_sim = StorageFailureSimulator::instance();

            std::lock_guard lock{mutex_};
            for (auto it = seg_by_atom_key_.cbegin(), next_it = it; it != seg_by_atom_key_.cend(); it = next_it) {
                ++next_it;
                const auto& key = it->first;
                if (key.type() == kt && prefix_matcher(key.id())) {
                    ARCTICDB_DEBUG(log::version(), "Iterate type {}", key);
                    failure_sim->go(FailureType::ITERATE);
                    func(VariantKey{key});
                }
            }

            for (auto it = seg_by_ref_key_.cbegin(), next_it = it; it != seg_by_ref_key_.cend(); it = next_it) {
                ++next_it;
                const auto& key = it->first;
                if (key.type() == kt && prefix_matcher(key.id())) {
                    failure_sim->go(FailureType::ITERATE);
                    func(VariantKey{key});
                }
            }
        }

        folly::Future<std::vector<VariantKey>> batch_write(
                std::vector<std::pair<PartialKey, SegmentInMemory>> &&key_segments,
                const std::shared_ptr<DeDupMap> &,
                const BatchWriteArgs &
        ) override {
            std::vector<VariantKey> output;
            for (auto &pair : key_segments) {
                output.emplace_back(std::get<AtomKey>(write(pair.first, std::move(pair.second)).value()));
            }
            return folly::makeFuture(std::move(output));
        }

        std::vector<folly::Future<bool>> batch_key_exists(const std::vector<entity::VariantKey> &keys) override {
            std::vector<folly::Future<bool>> output;
            for(const auto& key : keys) {
                util::variant_match(key,
                                    [&output, &refs=seg_by_ref_key_] (const RefKey& ref) {
                    output.push_back(folly::makeFuture<bool>(refs.find(ref) != refs.end()));
                },
                [&output, &atoms=seg_by_atom_key_] (const AtomKey& atom) {
                    output.push_back(folly::makeFuture<bool>(atoms.find(atom) != atoms.end()));
                });
            }
            return output;
        }

        std::vector<storage::KeySegmentPair>
        batch_read_compressed(std::vector<entity::VariantKey> &&, const BatchReadArgs &, bool) override {
            throw std::runtime_error("Not implemented");
        }

        folly::Future<std::vector<VariantKey>> batch_read_compressed(
                std::vector<entity::VariantKey>&& keys,
                std::vector<ReadContinuation>&&,
                const BatchReadArgs &
        ) override {
            std::lock_guard lock{mutex_};
            std::vector<VariantKey> output;
            for (auto &key : keys)
                output.push_back(key);

            return output;
        }

        folly::Future<std::vector<RemoveKeyResultType>>
        remove_keys(const std::vector<entity::VariantKey> &keys, storage::RemoveOpts opts) override {
            std::vector<RemoveKeyResultType> output;
            for (const auto &key: keys) {
                output.emplace_back(remove_key_sync(key, opts));
            }

            return output;
        }

        folly::Future<std::vector<RemoveKeyResultType>>
        remove_keys(std::vector<entity::VariantKey> &&keys, storage::RemoveOpts opts) override {
            std::vector<RemoveKeyResultType> output;
            for (const auto &key: keys) {
                output.emplace_back(remove_key_sync(key, opts));
            }

            return output;
        }

        void insert_atom_key(const AtomKey &key) {
            seg_by_atom_key_.insert(std::make_pair(key, std::make_unique<SegmentInMemory>()));
        }

        size_t num_atom_keys() const { return seg_by_atom_key_.size(); }

        size_t num_ref_keys() const { return seg_by_ref_key_.size(); }

        size_t num_atom_keys_of_type(KeyType key_type) const {
            util::check(!is_ref_key_class(key_type), "Num atom keys of type for ref key doesn't make sense");
            return std::count_if(seg_by_atom_key_.cbegin(), seg_by_atom_key_.cend(),
                                 [=](auto &entry) { return entry.first.type() == key_type; });
        }

        void move_storage(KeyType , timestamp , size_t ) override {
        }

        size_t num_ref_keys_of_type(KeyType key_type) const {
            util::check(is_ref_key_class(key_type), "Num ref keys of type for non-ref key doesn't make sense");
            return std::count_if(seg_by_ref_key_.cbegin(), seg_by_ref_key_.cend(),
                                 [=](auto &entry) { return entry.first.type() == key_type; });
        }

        HashedValue content_hash_ = 0x42;

        folly::Future<std::pair<std::optional<VariantKey>, std::optional<google::protobuf::Any>>>

        read_metadata(const entity::VariantKey &key, storage::ReadKeyOpts) override {
            return util::variant_match(key,
                                       [&](const AtomKey &ak) {
                                           auto it = seg_by_atom_key_.find(ak);
                                           // util::check_rte(it != seg_by_atom_key_.end(), "atom key {} not found in remove", ak);
                                           if (it == seg_by_atom_key_.end())
                                               throw storage::KeyNotFoundException(Composite<VariantKey>(ak));
                                           ARCTICDB_DEBUG(log::storage(), "Mock store removing data for atom key {}", ak);
                                           return std::make_pair(std::make_optional<VariantKey>(key), std::make_optional<google::protobuf::Any>(*it->second->metadata()));
                                       },
                                       [&](const RefKey &rk) {
                                           auto it = seg_by_ref_key_.find(rk);
                                           // util::check_rte(it != seg_by_ref_key_.end(), "ref key {} not found in remove", rk);
                                           if (it == seg_by_ref_key_.end())
                                               throw storage::KeyNotFoundException(Composite<VariantKey>(rk));
                                           ARCTICDB_DEBUG(log::storage(), "Mock store removing data for ref key {}", rk);
                                           return std::make_pair(std::make_optional<VariantKey>(key), std::make_optional<google::protobuf::Any>(*it->second->metadata()));
                                       });
        }

        folly::Future<std::tuple<VariantKey, std::optional<google::protobuf::Any>, StreamDescriptor>>
        read_metadata_and_descriptor(
            const entity::VariantKey& key, storage::ReadKeyOpts) override {
            auto components =  util::variant_match(key,
                                       [&](const AtomKey &ak) {
                auto it = seg_by_atom_key_.find(ak);
                // util::check_rte(it != seg_by_atom_key_.end(), "atom key {} not found in remove", ak);
                if (it == seg_by_atom_key_.end())
                    throw storage::KeyNotFoundException(Composite<VariantKey>(ak));
                ARCTICDB_DEBUG(log::storage(), "Mock store removing data for atom key {}", ak);
                return std::make_tuple(key, std::make_optional<google::protobuf::Any>(*it->second->metadata()), it->second->descriptor());
                },
                [&](const RefKey &rk) {
                auto it = seg_by_ref_key_.find(rk);
                // util::check_rte(it != seg_by_ref_key_.end(), "ref key {} not found in remove", rk);
                if (it == seg_by_ref_key_.end())
                    throw storage::KeyNotFoundException(Composite<VariantKey>(rk));
                ARCTICDB_DEBUG(log::storage(), "Mock store removing data for ref key {}", rk);
                return std::make_tuple(key, std::make_optional<google::protobuf::Any>(*it->second->metadata()), it->second->descriptor());
            });
            return folly::makeFuture(std::move(components));
        }


        folly::Future<std::pair<std::variant<arcticdb::entity::AtomKeyImpl, arcticdb::entity::RefKey>, arcticdb::TimeseriesDescriptor>>
        read_timeseries_descriptor(const entity::VariantKey& key) override {
            return util::variant_match(key, [&](const AtomKey &ak) {
                auto it = seg_by_atom_key_.find(ak);
                if (it == seg_by_atom_key_.end())
                    throw storage::KeyNotFoundException(Composite<VariantKey>(ak));
                ARCTICDB_DEBUG(log::storage(), "Mock store removing data for atom key {}", ak);
                return std::make_pair(key, it->second->index_descriptor());
                },
                [&](const RefKey &rk) {
                auto it = seg_by_ref_key_.find(rk);
                if (it == seg_by_ref_key_.end())
                    throw storage::KeyNotFoundException(Composite<VariantKey>(rk));
                ARCTICDB_DEBUG(log::storage(), "Mock store removing data for ref key {}", rk);
                return std::make_pair(key, it->second->index_descriptor());
            });
        }


        void set_failure_sim(const arcticdb::proto::storage::VersionStoreConfig::StorageFailureSimulator &) override {}

        void add_segment(const AtomKey &key, SegmentInMemory &&seg) {
            StorageFailureSimulator::instance()->go(FailureType::WRITE);
            std::lock_guard lock{mutex_};
            ARCTICDB_DEBUG(log::storage(), "Adding segment with key {}", key);
            seg_by_atom_key_[key] = std::make_unique<SegmentInMemory>(std::move(seg));
        }

        void add_segment(const RefKey &key, SegmentInMemory &&seg) {
            StorageFailureSimulator::instance()->go(FailureType::WRITE);
            std::lock_guard lock{mutex_};
            ARCTICDB_DEBUG(log::storage(), "Adding segment with key {}", key);
            seg_by_ref_key_[key] = std::make_unique<SegmentInMemory>(std::move(seg));
        }


    protected:
        std::recursive_mutex mutex_; // Allow iterate_type() to be re-entrant
        std::unordered_map<AtomKey, std::unique_ptr<SegmentInMemory>> seg_by_atom_key_;
        std::unordered_map<RefKey, std::unique_ptr<SegmentInMemory>> seg_by_ref_key_;
    };

} //namespace arcticdb