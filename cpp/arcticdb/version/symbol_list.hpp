/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/entity/types.hpp>
#include <arcticdb/log/log.hpp>
#include <arcticdb/storage/store.hpp>
#include <arcticdb/stream/stream_writer.hpp>
#include <arcticdb/stream/stream_reader.hpp>
#include <arcticdb/entity/atom_key.hpp>
#include <arcticdb/entity/ref_key.hpp>
#include <arcticdb/stream/index_aggregator.hpp>
#include <arcticdb/util/variant.hpp>
#include <arcticdb/util/configs_map.hpp>
#include <arcticdb/util/storage_lock.hpp>
#include <arcticdb/util/key_utils.hpp>
#include <arcticdb/storage/storage_exceptions.hpp>
#include <arcticdb/version/version_functions.hpp>
#include <arcticdb/version/version_map_batch_methods.hpp>

#include <folly/Range.h>

#include <cstdlib>
#include <set>
#include <mutex>

#define SYMBOL_LIST_RUNTIME_LOG(...) ARCTICDB_RUNTIME_DEBUG(log::version(), "Symbol List: {}: ", __func__, __VA_ARGS__)

namespace arcticdb {

using namespace arcticdb::stream;

static const char* const CompactionId = "__symbols__";
static const char* const AddSymbol = "__add__";
static const char* const DeleteSymbol = "__delete__";
static const char* const CompactionLockName = "SymbolListCompactionLock";

class SymbolList {
    using StreamId = entity::StreamId;
    using CollectionType = std::set<StreamId>;
    using KeyVector = std::vector<entity::AtomKey>;

    StreamId type_holder_;
    uint64_t max_delta_ = 0;
    std::shared_ptr<VersionMap> version_map_;

public:
    explicit SymbolList(std::shared_ptr<VersionMap> version_map, StreamId type_indicator = StringId())
        : type_holder_(std::move(type_indicator)),
          max_delta_(ConfigsMap::instance()->get_int("SymbolList.MaxDelta", 500)),
          version_map_(std::move(version_map))
    {
    }

    CollectionType load(const std::shared_ptr<Store>& store, bool no_compaction)
    {
        return ExponentialBackoff<std::runtime_error>(100, 2000).go([&]() {
            SYMBOL_LIST_RUNTIME_LOG("Symbol list load attempt");
            return load_symbols(store, no_compaction);
        });
    }

    std::vector<StreamId> get_symbols(const std::shared_ptr<Store>& store, bool no_compaction = false)
    {
        SYMBOL_LIST_RUNTIME_LOG("no_compaction={}", no_compaction); // function name logged in macro
        auto symbols = load(store, no_compaction);
        return {std::make_move_iterator(symbols.begin()), std::make_move_iterator(symbols.end())};
    }

    std::set<StreamId> get_symbol_set(const std::shared_ptr<Store>& store)
    {
        SYMBOL_LIST_RUNTIME_LOG("called");
        return load(store, false);
    }

    void add_symbol(const std::shared_ptr<Store>& store, const StreamId& symbol)
    {
        SYMBOL_LIST_RUNTIME_LOG("{}", symbol);
        write_symbol(store, symbol);
    }

    void remove_symbol(const std::shared_ptr<Store>& store, const StreamId& symbol)
    {
        SYMBOL_LIST_RUNTIME_LOG("{}", symbol);
        delete_symbol(store, symbol);
    }

    void clear(const std::shared_ptr<Store>& store)
    {
        delete_all_keys_of_type(KeyType::SYMBOL_LIST, store, true);
    }

    void reload(const std::shared_ptr<Store>& store)
    {
        clear(store);
        load(store, false);
    }

private:
    // Making each function return the collection instead of using a shared field removes the possibility of the field
    // being left blank by mistake
    CollectionType load_symbols(const std::shared_ptr<Store>& store, bool no_compaction);

    CollectionType compact(std::shared_ptr<Store> store, const std::vector<AtomKey>& symbol_keys);

    void write_symbol(const std::shared_ptr<Store>& store, const StreamId& symbol)
    {
        write_journal(store, symbol, AddSymbol);
    }

    void delete_symbol(const std::shared_ptr<Store>& store, const StreamId& symbol)
    {
        write_journal(store, symbol, DeleteSymbol);
    }

    void write_journal(const std::shared_ptr<Store>& store, const StreamId& symbol, std::string action);

    [[nodiscard]] CollectionType load_from_version_keys(const std::shared_ptr<Store>& store);

    [[nodiscard]] folly::Future<VariantKey> write_symbols(const std::shared_ptr<Store>& store,
        const CollectionType& symbols,
        const StreamId& stream_id,
        timestamp creation_ts);

    [[nodiscard]] CollectionType load_from_storage(const std::shared_ptr<StreamSource>& store,
        const std::vector<AtomKey>& keys);

    void
    read_list_from_storage(const std::shared_ptr<StreamSource>& store, const AtomKey& key, CollectionType& symbols);

    [[nodiscard]] KeyVector get_all_symbol_list_keys(const std::shared_ptr<StreamSource>& store) const;

    void delete_keys(std::shared_ptr<Store> store, const KeyVector& lists);

    std::optional<KeyVector::difference_type> last_compaction(const KeyVector& keys);

    inline StreamDescriptor symbol_stream_descriptor(const StreamId& stream_id)
    {
        auto data_type = std::holds_alternative<StringId>(type_holder_) ? DataType::ASCII_DYNAMIC64 : DataType::UINT64;
        return StreamDescriptor{
            stream_descriptor(stream_id, RowCountIndex(), {scalar_field_proto(data_type, "symbol")})};
    };

    inline StreamDescriptor journal_stream_descriptor(const StreamId& action, const StreamId& id)
    {
        return util::variant_match(
            id,
            [&action](const NumericId&) {
                return StreamDescriptor{
                    stream_descriptor(action, RowCountIndex(), {scalar_field_proto(DataType::UINT64, "symbol")})};
            },
            [&action](const StringId&) {
                return StreamDescriptor{stream_descriptor(action,
                    RowCountIndex(),
                    {scalar_field_proto(DataType::UTF_DYNAMIC64, "symbol")})};
            });
    };
};

struct WriteSymbolTask : async::BaseTask {
    const std::shared_ptr<Store> store_;
    std::shared_ptr<SymbolList> symbol_list_;
    const StreamId stream_id_;

    WriteSymbolTask(std::shared_ptr<Store> store, std::shared_ptr<SymbolList> symbol_list, StreamId stream_id)
        : store_(store),
          symbol_list_(symbol_list),
          stream_id_(stream_id)
    {
    }

    folly::Future<folly::Unit> operator()()
    {
        symbol_list_->add_symbol(store_, stream_id_);
        return folly::Unit{};
    }
};

} //namespace arcticdb
