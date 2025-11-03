/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
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
/*
 * In-memory implementation of Store for testing purposes.
 *
 * InMemoryStore only implements a fraction of all the methods from Store.
 *
 */
class InMemoryStore : public Store {

  public:
    InMemoryStore() = default;

    folly::Future<folly::Unit> batch_write_compressed(std::vector<storage::KeySegmentPair>) override {
        util::raise_rte("Not implemented");
    }

    bool supports_prefix_matching() const override { return false; }

    bool supports_atomic_writes() const override { return true; }

    bool fast_delete() override { return false; }

    std::vector<folly::Future<pipelines::SegmentAndSlice>>
    batch_read_uncompressed(std::vector<pipelines::RangesAndKey>&&, std::shared_ptr<std::unordered_set<std::string>>)
            override {
        throw std::runtime_error("Not implemented for tests");
    }

    std::vector<folly::Future<VariantKey>>
    batch_read_compressed(std::vector<std::pair<entity::VariantKey, ReadContinuation>>&&, const BatchReadArgs&)
            override {
        throw std::runtime_error("Not implemented for tests");
    }

    AtomKey get_key(
            stream::KeyType key_type, VersionId gen_id, const StreamId& stream_id, const IndexValue& start_index,
            const IndexValue& end_index, std::optional<timestamp> creation_ts = std::nullopt
    ) const {
        return atom_key_builder()
                .gen_id(gen_id)
                .content_hash(content_hash_)
                .creation_ts(creation_ts.value_or(PilotedClock::nanos_since_epoch()))
                .start_index(start_index)
                .end_index(end_index)
                .build(stream_id, key_type);
    }

    folly::Future<VariantKey> write(
            KeyType key_type, VersionId gen_id, const StreamId& stream_id, IndexValue start_index, IndexValue end_index,
            SegmentInMemory&& segment
    ) override {
        auto key = get_key(key_type, gen_id, stream_id, start_index, end_index);
        add_segment(key, std::move(segment));
        ARCTICDB_DEBUG(log::storage(), "Mock store adding atom key {}", key);
        return folly::makeFuture(key);
    }

    folly::Future<entity::VariantKey> write(
            stream::KeyType key_type, VersionId gen_id, const StreamId& stream_id, timestamp creation_ts,
            IndexValue start_index, IndexValue end_index, SegmentInMemory&& segment
    ) override {
        auto key = get_key(key_type, gen_id, stream_id, start_index, end_index, creation_ts);
        add_segment(key, std::move(segment));
        ARCTICDB_DEBUG(log::storage(), "Mock store adding atom key {}", key);
        return folly::makeFuture(key);
    }

    folly::Future<VariantKey> update(const VariantKey& key, SegmentInMemory&& segment, storage::UpdateOpts opts)
            override {
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

    folly::Future<VariantKey> write(PartialKey pk, SegmentInMemory&& segment) override {
        return write(pk.key_type, pk.version_id, pk.stream_id, pk.start_index, pk.end_index, std::move(segment));
    }

    entity::VariantKey write_sync(
            stream::KeyType key_type, VersionId version_id, const StreamId& stream_id, IndexValue start_index,
            IndexValue end_index, SegmentInMemory&& segment
    ) override {
        return write(key_type, version_id, stream_id, start_index, end_index, std::move(segment)).get();
    }

    entity::VariantKey write_sync(PartialKey pk, SegmentInMemory&& segment) override {
        return write(pk, std::move(segment)).get();
    }

    entity::VariantKey write_sync(KeyType key_type, const StreamId& stream_id, SegmentInMemory&& segment) override {
        return write(key_type, stream_id, std::move(segment)).get();
    }

    entity::VariantKey write_if_none_sync(KeyType key_type, const StreamId& stream_id, SegmentInMemory&& segment)
            override {
        auto key = entity::RefKey{stream_id, key_type};
        add_segment(key, std::move(segment), true);
        return key;
    }

    bool is_path_valid(const std::string_view) const override { return true; }

    folly::Future<entity::VariantKey> write(
            stream::KeyType key_type, const StreamId& stream_id, SegmentInMemory&& segment
    ) override {
        util::check(is_ref_key_class(key_type), "Cannot write ref key with atom key type {}", key_type);
        auto key = entity::RefKey{stream_id, key_type};
        add_segment(key, std::move(segment));
        ARCTICDB_DEBUG(log::storage(), "Mock store adding ref key {}", key);
        return folly::makeFuture(key);
    }

    folly::Future<VariantKey> write_maybe_blocking(
            PartialKey pk, SegmentInMemory&& segment, std::shared_ptr<folly::NativeSemaphore> semaphore
    ) override {
        semaphore->wait();
        return write(pk.key_type, pk.version_id, pk.stream_id, pk.start_index, pk.end_index, std::move(segment))
                .thenTryInline([semaphore](folly::Try<VariantKey> keyTry) {
                    semaphore->post();
                    keyTry.throwUnlessValue();
                    return keyTry.value();
                });
    }

    folly::Future<VariantKey>
    copy(arcticdb::entity::KeyType, const StreamId&, arcticdb::entity::VersionId, const VariantKey&) override {
        util::raise_rte("Not implemented");
    }

    VariantKey copy_sync(arcticdb::entity::KeyType, const StreamId&, arcticdb::entity::VersionId, const VariantKey&)
            override {
        util::raise_rte("Not implemented");
    }

    bool key_exists_sync(const entity::VariantKey& key) override {
        StorageFailureSimulator::instance()->go(FailureType::READ);
        std::lock_guard lock{mutex_};
        return util::variant_match(
                key,
                [&](const RefKey& key) {
                    auto it = seg_by_ref_key_.find(key);
                    return it != seg_by_ref_key_.end();
                },
                [&](const AtomKey& key) {
                    auto it = seg_by_atom_key_.find(key);
                    return it != seg_by_atom_key_.end();
                }
        );
    }

    folly::Future<bool> key_exists(const entity::VariantKey& key) override {
        return folly::makeFuture(key_exists_sync(key));
    }

    std::pair<VariantKey, SegmentInMemory> read_sync(const VariantKey& key, storage::ReadKeyOpts) override {
        StorageFailureSimulator::instance()->go(FailureType::READ);
        std::lock_guard lock{mutex_};

        return util::variant_match(
                key,
                [&](const RefKey& ref_key) {
                    auto it = seg_by_ref_key_.find(ref_key);
                    if (it == seg_by_ref_key_.end())
                        throw storage::KeyNotFoundException(ref_key);
                    ARCTICDB_DEBUG(log::storage(), "Mock store returning ref key {}", ref_key);
                    std::pair<VariantKey, arcticdb::SegmentInMemory> res = {it->first, it->second->clone()};
                    return res;
                },
                [&](const AtomKey& atom_key) {
                    auto it = seg_by_atom_key_.find(atom_key);
                    if (it == seg_by_atom_key_.end())
                        throw storage::KeyNotFoundException(atom_key);
                    ARCTICDB_DEBUG(log::storage(), "Mock store returning atom key {}", atom_key);
                    std::pair<VariantKey, arcticdb::SegmentInMemory> res = {it->first, it->second->clone()};
                    return res;
                }
        );
    }

    folly::Future<storage::KeySegmentPair> read_compressed(const entity::VariantKey& key, storage::ReadKeyOpts opts)
            override {
        return folly::makeFutureWith([&]() { return read_compressed_sync(key, opts); });
    }

    storage::KeySegmentPair read_compressed_sync(const entity::VariantKey& key, storage::ReadKeyOpts) override {
        StorageFailureSimulator::instance()->go(FailureType::READ);
        std::lock_guard lock{mutex_};
        auto segment_in_memory = util::variant_match(
                key,
                [&](const RefKey& ref_key) {
                    auto it = seg_by_ref_key_.find(ref_key);
                    if (it == seg_by_ref_key_.end())
                        throw storage::KeyNotFoundException(ref_key);
                    ARCTICDB_DEBUG(log::storage(), "Mock store returning compressed ref key {}", ref_key);
                    return it->second->clone();
                },
                [&](const AtomKey& atom_key) {
                    auto it = seg_by_atom_key_.find(atom_key);
                    if (it == seg_by_atom_key_.end())
                        throw storage::KeyNotFoundException(atom_key);
                    ARCTICDB_DEBUG(log::storage(), "Mock store returning compressed atom key {}", atom_key);
                    return it->second->clone();
                }
        );

        Segment segment = encode_dispatch(std::move(segment_in_memory), codec_, EncodingVersion::V1);
        (void)segment.calculate_size();
        return {VariantKey{key}, std::move(segment)};
    }

    folly::Future<std::pair<VariantKey, SegmentInMemory>> read(const VariantKey& key, storage::ReadKeyOpts opts)
            override {
        return folly::makeFutureWith([&]() { return read_sync(key, opts); });
    }

    folly::Future<folly::Unit> write_compressed(storage::KeySegmentPair key_segment) override {
        return folly::makeFutureWith([&]() { return write_compressed_sync(key_segment); });
    }

    void write_compressed_sync(storage::KeySegmentPair key_segment) override {
        auto key = key_segment.variant_key();
        if (std::holds_alternative<RefKey>(key)) {
            auto ref_key = std::get<RefKey>(key);
            add_segment(ref_key, decode_segment(*key_segment.segment_ptr()));
        } else {
            auto atom_key = key_segment.atom_key();
            add_segment(atom_key, decode_segment(*key_segment.segment_ptr()));
        }
    }

    void update_compressed_sync(storage::KeySegmentPair key_segment, storage::UpdateOpts opts) override {
        auto key = key_segment.variant_key();
        if (!opts.upsert_) {
            util::check_rte(key_exists(key).get(), "update called with upsert=false but key does not exist");
        }
        if (std::holds_alternative<RefKey>(key)) {
            auto ref_key = std::get<RefKey>(key);
            add_segment(ref_key, decode_segment(*key_segment.segment_ptr()));
        } else {
            auto atom_key = key_segment.atom_key();
            add_segment(atom_key, decode_segment(*key_segment.segment_ptr()));
        }
    }

    RemoveKeyResultType remove_key_sync(const entity::VariantKey& key, storage::RemoveOpts opts) override {
        StorageFailureSimulator::instance()->go(FailureType::DELETE);
        std::lock_guard lock{mutex_};
        size_t removed = util::variant_match(
                key,
                [&](const AtomKey& atom_key) { return seg_by_atom_key_.erase(atom_key); },
                [&](const RefKey& ref_key) { return seg_by_ref_key_.erase(ref_key); }
        );
        ARCTICDB_DEBUG(log::storage(), "Mock store removed {} {}", removed, key);
        if (removed == 0 && !opts.ignores_missing_key_) {
            throw storage::KeyNotFoundException(VariantKey(key));
        }
        return {};
    }

    folly::Future<RemoveKeyResultType> remove_key(const VariantKey& key, storage::RemoveOpts opts) override {
        return folly::makeFuture(remove_key_sync(key, opts));
    }

    timestamp current_timestamp() override { return PilotedClock::nanos_since_epoch(); }

    void iterate_type(KeyType kt, const entity::IterateTypeVisitor& func, const std::string& prefix = "") override {
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

    [[nodiscard]] folly::Future<std::shared_ptr<storage::ObjectSizes>>
    get_object_sizes(KeyType, const std::optional<StreamId>&) override {
        util::raise_rte("get_object_sizes not implemented for InMemoryStore");
    }

    [[nodiscard]] folly::Future<folly::Unit> visit_object_sizes(
            KeyType, const std::optional<StreamId>&, storage::ObjectSizesVisitor
    ) override {
        util::raise_rte("visit_object_sizes not implemented for InMemoryStore");
    }

    bool scan_for_matching_key(KeyType kt, const IterateTypePredicate& predicate) override {
        auto failure_sim = StorageFailureSimulator::instance();

        std::lock_guard lock{mutex_};
        for (const auto& it : seg_by_atom_key_) {
            const auto& key = it.first;
            if (key.type() == kt && predicate(key)) {
                ARCTICDB_DEBUG(log::version(), "Scan for matching key {}", key);
                failure_sim->go(FailureType::ITERATE);
                return true;
            }
        }

        for (const auto& it : seg_by_ref_key_) {
            const auto& key = it.first;
            if (key.type() == kt && predicate(key)) {
                ARCTICDB_DEBUG(log::version(), "Scan for matching key {}", key);
                failure_sim->go(FailureType::ITERATE);
                return true;
            }
        }

        return false;
    }

    folly::Future<pipelines::SliceAndKey>
    async_write(folly::Future<std::tuple<PartialKey, SegmentInMemory, pipelines::FrameSlice>>&& input_fut, const std::shared_ptr<DeDupMap>&)
            override {
        return std::move(input_fut).thenValue([this](auto&& input) {
            auto [pk, seg, slice] = std::move(input);
            auto key = get_key(pk.key_type, 0, pk.stream_id, pk.start_index, pk.end_index);
            add_segment(key, std::move(seg));
            return SliceAndKey{std::move(slice), std::move(key)};
        });
    }

    folly::Future<pipelines::SliceAndKey>
    async_write(std::tuple<PartialKey, SegmentInMemory, pipelines::FrameSlice>&& input, const std::shared_ptr<DeDupMap>&)
            override {
        auto [pk, seg, slice] = std::move(input);
        auto key = get_key(pk.key_type, 0, pk.stream_id, pk.start_index, pk.end_index);
        add_segment(key, std::move(seg));
        return SliceAndKey{std::move(slice), std::move(key)};
    }

    std::vector<folly::Future<bool>> batch_key_exists(const std::vector<entity::VariantKey>& keys) override {
        auto failure_sim = StorageFailureSimulator::instance();
        failure_sim->go(FailureType::READ);
        std::vector<folly::Future<bool>> output;
        for (const auto& key : keys) {
            util::variant_match(
                    key,
                    [&output, &refs = seg_by_ref_key_](const RefKey& ref) {
                        output.emplace_back(folly::makeFuture<bool>(refs.find(ref) != refs.end()));
                    },
                    [&output, &atoms = seg_by_atom_key_](const AtomKey& atom) {
                        output.emplace_back(folly::makeFuture<bool>(atoms.find(atom) != atoms.end()));
                    }
            );
        }
        return output;
    }

    folly::Future<std::vector<RemoveKeyResultType>> remove_keys(
            const std::vector<entity::VariantKey>& keys, storage::RemoveOpts opts
    ) override {
        std::vector<RemoveKeyResultType> output;
        for (const auto& key : keys) {
            output.emplace_back(remove_key_sync(key, opts));
        }

        return output;
    }

    folly::Future<std::vector<RemoveKeyResultType>> remove_keys(
            std::vector<entity::VariantKey>&& keys, storage::RemoveOpts opts
    ) override {
        std::vector<RemoveKeyResultType> output;
        for (const auto& key : keys) {
            output.emplace_back(remove_key_sync(key, opts));
        }

        return output;
    }

    std::vector<RemoveKeyResultType> remove_keys_sync(
            const std::vector<entity::VariantKey>& keys, storage::RemoveOpts opts
    ) override {
        std::vector<RemoveKeyResultType> output;
        for (const auto& key : keys) {
            output.emplace_back(remove_key_sync(key, opts));
        }

        return output;
    }

    std::vector<RemoveKeyResultType> remove_keys_sync(std::vector<entity::VariantKey>&& keys, storage::RemoveOpts opts)
            override {
        std::vector<RemoveKeyResultType> output;
        for (const auto& key : keys) {
            output.emplace_back(remove_key_sync(key, opts));
        }

        return output;
    }

    size_t num_atom_keys() const { return seg_by_atom_key_.size(); }

    size_t num_ref_keys() const { return seg_by_ref_key_.size(); }

    size_t num_atom_keys_of_type(KeyType key_type) const {
        util::check(!is_ref_key_class(key_type), "Num atom keys of type for ref key doesn't make sense");
        return std::count_if(seg_by_atom_key_.cbegin(), seg_by_atom_key_.cend(), [=](auto& entry) {
            return entry.first.type() == key_type;
        });
    }

    void move_storage(KeyType, timestamp, size_t) override {
        // Nothing
    }

    HashedValue content_hash_ = 0x42;

    folly::Future<std::pair<std::optional<VariantKey>, std::optional<google::protobuf::Any>>> read_metadata(
            const entity::VariantKey& key, storage::ReadKeyOpts
    ) override {
        auto failure_sim = StorageFailureSimulator::instance();
        failure_sim->go(FailureType::READ);
        return util::variant_match(
                key,
                [&](const AtomKey& atom_key) {
                    auto it = seg_by_atom_key_.find(atom_key);
                    // util::check_rte(it != seg_by_atom_key_.end(), "atom key {} not found in remove", atom_key);
                    if (it == seg_by_atom_key_.end())
                        throw storage::KeyNotFoundException(atom_key);
                    ARCTICDB_DEBUG(log::storage(), "Mock store removing data for atom key {}", atom_key);
                    return std::make_pair(
                            std::make_optional<VariantKey>(key),
                            std::make_optional<google::protobuf::Any>(*it->second->metadata())
                    );
                },
                [&](const RefKey& ref_key) {
                    auto it = seg_by_ref_key_.find(ref_key);
                    // util::check_rte(it != seg_by_ref_key_.end(), "ref key {} not found in remove", ref_key);
                    if (it == seg_by_ref_key_.end())
                        throw storage::KeyNotFoundException(ref_key);
                    ARCTICDB_DEBUG(log::storage(), "Mock store removing data for ref key {}", ref_key);
                    return std::make_pair(
                            std::make_optional<VariantKey>(key),
                            std::make_optional<google::protobuf::Any>(*it->second->metadata())
                    );
                }
        );
    }

    folly::Future<std::tuple<VariantKey, std::optional<google::protobuf::Any>, StreamDescriptor>>
    read_metadata_and_descriptor(const entity::VariantKey& key, storage::ReadKeyOpts) override {
        auto failure_sim = StorageFailureSimulator::instance();
        failure_sim->go(FailureType::READ);
        auto components = util::variant_match(
                key,
                [&](const AtomKey& atom_key) {
                    auto it = seg_by_atom_key_.find(atom_key);
                    // util::check_rte(it != seg_by_atom_key_.end(), "atom key {} not found in remove", atom_key);
                    if (it == seg_by_atom_key_.end())
                        throw storage::KeyNotFoundException(atom_key);
                    ARCTICDB_DEBUG(log::storage(), "Mock store removing data for atom key {}", atom_key);
                    return std::make_tuple(
                            key,
                            std::make_optional<google::protobuf::Any>(*it->second->metadata()),
                            it->second->descriptor()
                    );
                },
                [&](const RefKey& ref_key) {
                    auto it = seg_by_ref_key_.find(ref_key);
                    // util::check_rte(it != seg_by_ref_key_.end(), "ref key {} not found in remove", ref_key);
                    if (it == seg_by_ref_key_.end())
                        throw storage::KeyNotFoundException(ref_key);
                    ARCTICDB_DEBUG(log::storage(), "Mock store removing data for ref key {}", ref_key);
                    return std::make_tuple(
                            key,
                            std::make_optional<google::protobuf::Any>(*it->second->metadata()),
                            it->second->descriptor()
                    );
                }
        );
        return folly::makeFuture(std::move(components));
    }

    folly::Future<std::pair<VariantKey, arcticdb::TimeseriesDescriptor>>
    read_timeseries_descriptor(const entity::VariantKey& key, storage::ReadKeyOpts /*opts*/) override {
        auto failure_sim = StorageFailureSimulator::instance();
        failure_sim->go(FailureType::READ);
        return util::variant_match(
                key,
                [&](const AtomKey& atom_key) {
                    auto it = seg_by_atom_key_.find(atom_key);
                    if (it == seg_by_atom_key_.end())
                        throw storage::KeyNotFoundException(atom_key);
                    ARCTICDB_DEBUG(log::storage(), "Mock store removing data for atom key {}", atom_key);
                    return std::make_pair(key, it->second->index_descriptor());
                },
                [&](const RefKey& ref_key) {
                    auto it = seg_by_ref_key_.find(ref_key);
                    if (it == seg_by_ref_key_.end())
                        throw storage::KeyNotFoundException(ref_key);
                    ARCTICDB_DEBUG(log::storage(), "Mock store removing data for ref key {}", ref_key);
                    return std::make_pair(key, it->second->index_descriptor());
                }
        );
    }

    void set_failure_sim(const arcticdb::proto::storage::VersionStoreConfig::StorageFailureSimulator&) override {}

    std::string name() const override { return "InMemoryStore"; }

    void add_segment(const AtomKey& key, SegmentInMemory&& seg) {
        StorageFailureSimulator::instance()->go(FailureType::WRITE);
        std::lock_guard lock{mutex_};
        ARCTICDB_DEBUG(log::storage(), "Adding segment with key {}", key);
        seg_by_atom_key_[key] = std::make_unique<SegmentInMemory>(std::move(seg));
    }

    void add_segment(const RefKey& key, SegmentInMemory&& seg, bool if_none_match = false) {
        StorageFailureSimulator::instance()->go(FailureType::WRITE);
        std::lock_guard lock{mutex_};
        ARCTICDB_DEBUG(log::storage(), "Adding segment with key {}", key);
        if (if_none_match) {
            if (seg_by_ref_key_.find(key) != seg_by_ref_key_.end()) {
                storage::raise<ErrorCode::E_ATOMIC_OPERATION_FAILED>("Precondition failed. Object is already present.");
            }
        }
        seg_by_ref_key_[key] = std::make_unique<SegmentInMemory>(std::move(seg));
    }

  protected:
    std::recursive_mutex mutex_; // Allow iterate_type() to be re-entrant
    std::unordered_map<AtomKey, std::unique_ptr<SegmentInMemory>> seg_by_atom_key_;
    std::unordered_map<RefKey, std::unique_ptr<SegmentInMemory>> seg_by_ref_key_;
    arcticdb::proto::encoding::VariantCodec codec_;
};

} // namespace arcticdb