/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/entity/atom_key.hpp>
#include <fmt/format.h>
#include <string>

namespace arcticdb {
struct VersionedItem {
    VersionedItem(entity::AtomKey&& key) : key_(std::move(key)) {}

    VersionedItem(const entity::AtomKey& key) : key_(key) {}

    VersionedItem() = default;

    entity::AtomKey key_;

    std::string symbol() const { return fmt::format("{}", key_.id()); }
    uint64_t version() const { return key_.version_id(); }
    int64_t timestamp() const { return key_.creation_ts(); }
};
} // namespace arcticdb