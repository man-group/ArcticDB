/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/version/version_log.hpp>

#include <util/variant.hpp>

namespace arcticdb {
    StreamDescriptor log_compacted_stream_descriptor(const StreamId& stream_id,
                                                     DataType symbol_datatype) {
        return StreamDescriptor{stream_descriptor(stream_id, RowCountIndex(), {
            scalar_field(symbol_datatype, "symbol"),
            scalar_field(DataType::UINT64, "version_id"),
            scalar_field(DataType::ASCII_DYNAMIC64, "action"),
            scalar_field(DataType::NANOSECONDS_UTC64, "creation_ts"),
            } )};
    }

    void delete_op_logs(const std::shared_ptr<Store>& store,
                        std::variant<std::vector<AtomKey>, std::vector<OpLog>>&& op_logs) {
        std::vector<VariantKey> vars;
        util::variant_match(op_logs,
        [&vars](std::vector<AtomKey>& keys) {
            vars.reserve(keys.size());
            for (auto& key: keys) {
                vars.emplace_back(std::move(key));
            }
        },
        [&vars](std::vector<OpLog>& logs) {
            vars.reserve(logs.size());
            for (auto& log: logs) {
                vars.emplace_back(log.extract_key());
            }
        });
        store->remove_keys(vars).get();
    }

    std::vector<OpLog> get_op_logs(std::shared_ptr<Store> store) {
        std::vector<OpLog> op_logs;
        store->iterate_type(KeyType::LOG,
                            [&op_logs](auto&& vk) {
            auto key = std::forward<VariantKey>(vk);
            if (is_recognized_log_action(variant_key_id(key))) {
                op_logs.emplace_back(std::move(to_atom(key)));
            }
        });
        std::sort(std::begin(op_logs), std::end(op_logs),
                  [](const OpLog &l, const OpLog &r) {
            return l.creation_ts() < r.creation_ts();
        });
        return op_logs;
    }

    std::vector<AtomKey> get_compacted_op_log_keys(std::shared_ptr<Store> store) {
        std::vector<AtomKey> res;
        store->iterate_type(KeyType::LOG_COMPACTED,
                            [&res](auto&& vk) {
            auto key = std::forward<VariantKey>(vk);
            if (variant_key_id(key) == StreamId{StorageLogId}) {
                res.emplace_back(std::move(to_atom(key)));
            }
        });
        std::sort(std::begin(res), std::end(res),
                  [](const AtomKey &l, const AtomKey &r) {
            return l.creation_ts() < r.creation_ts();
        });
        return res;
    }

    std::vector<OpLog> get_op_logs_from_compacted_op_log_keys(std::shared_ptr<Store> store,
                                                              const std::vector<AtomKey>& compacted_op_log_keys) {
        std::vector<OpLog> op_logs;
        for (const auto& compacted_op_log_key: compacted_op_log_keys) {
            auto seg = store->read_sync(compacted_op_log_key).second;
            for (auto it = seg.begin(); it != seg.end(); it++) {
                std::string id{it->string_at(0).value()};
                VersionId version_id{it->scalar_at<VersionId>(1).value()};
                std::string action{it->string_at(2).value()};
                timestamp creation_ts{it->scalar_at<timestamp>(3).value()};

                op_logs.emplace_back(id, version_id, action, creation_ts);
            }
        }
        return op_logs;
    }
}